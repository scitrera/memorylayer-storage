// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package pgindex is the Postgres-backed persistence for blobgw: it implements
// both casstore's snapshot.DedupStore (the chunk→pack dedup index, table
// pack_manifest) and the gateway's RefStore (the logical object index, table
// blob_ref). It uses only database/sql so the caller supplies the driver and
// connection — enterprise points it at the PostgreSQL it already runs, adding
// no new datastore.
package pgindex

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// Store is the Postgres-backed DedupStore + RefStore. Safe for concurrent use
// (it delegates concurrency to *sql.DB).
type Store struct {
	db *sql.DB
	// dp records per-op RED metrics (blobgw.pg.op.duration / errors). nil-safe:
	// a nil *metrics.DataPath makes every record call a no-op, so the un-wired
	// (tests, dev without metrics) path has zero overhead and no branches here.
	dp *metrics.DataPath
}

// Option configures a Store.
type Option func(*Store)

// WithMetrics wires the data-path RED recorder so each PG index operation records
// blobgw.pg.op.duration {operation, outcome} + blobgw.pg.errors {operation,
// error.kind}. Omit (or pass nil) to leave PG metrics dark.
func WithMetrics(dp *metrics.DataPath) Option {
	return func(s *Store) { s.dp = dp }
}

// Compile-time proof the Store satisfies both seams it backs, plus the optional
// PackSizeRecorder capability (per-pack physical-size accounting) casstore's
// write + compaction paths detect and call.
var (
	_ snapshot.DedupStore       = (*Store)(nil)
	_ gateway.RefStore          = (*Store)(nil)
	_ snapshot.PackSizeRecorder = (*Store)(nil)
)

// New wraps an existing *sql.DB. The caller owns the pool's lifecycle and
// chooses the driver (e.g. github.com/jackc/pgx/v5/stdlib). Optional Options wire
// cross-cutting concerns such as WithMetrics.
func New(db *sql.DB, opts ...Option) *Store {
	s := &Store{db: db}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Schema is the DDL for the two tables, idempotent. Migrate applies it.
//
// The ADD COLUMN IF NOT EXISTS statements below are forward-safe migrations for
// columns added after the table's initial shape: they no-op on a table that
// already has them and backfill the nullable column on an existing table
// (NULL = unset), so existing blob_ref rows keep working unchanged.
//   - user_meta:     caller metadata captured at mint/put time (A3), so it
//     survives a restart between MintRefWithMeta and Finalize.
//   - expected_hash / expected_size: the optional finalize integrity gate (A1);
//     NULL/0/"" mean "no expectation, accept whatever is staged".
const Schema = `
CREATE TABLE IF NOT EXISTS pack_manifest (
    domain     TEXT   NOT NULL,
    chunk_hash TEXT   NOT NULL,
    pack_hash  TEXT   NOT NULL,
    offset_b   BIGINT NOT NULL,
    length_b   BIGINT NOT NULL,
    PRIMARY KEY (domain, chunk_hash)
);
CREATE INDEX IF NOT EXISTS pack_manifest_by_pack ON pack_manifest (domain, pack_hash);

CREATE TABLE IF NOT EXISTS packs (
    domain                TEXT        NOT NULL,
    pack_hash             TEXT        NOT NULL,
    compressed_pack_bytes BIGINT      NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (domain, pack_hash)
);

CREATE TABLE IF NOT EXISTS blob_ref (
    domain       TEXT        NOT NULL,
    ref          TEXT        NOT NULL,
    content_hash TEXT        NOT NULL DEFAULT '',
    size_b       BIGINT      NOT NULL DEFAULT 0,
    content_type TEXT        NOT NULL DEFAULT '',
    version      TEXT        NOT NULL DEFAULT '',
    pending      BOOLEAN     NOT NULL DEFAULT FALSE,
    staging_key  TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (domain, ref)
);
CREATE INDEX IF NOT EXISTS blob_ref_by_prefix ON blob_ref (domain, ref text_pattern_ops);

ALTER TABLE blob_ref ADD COLUMN IF NOT EXISTS user_meta     JSONB;
ALTER TABLE blob_ref ADD COLUMN IF NOT EXISTS expected_hash TEXT;
ALTER TABLE blob_ref ADD COLUMN IF NOT EXISTS expected_size BIGINT;

-- Pack reclamation lifecycle (snapshot.PackState). A pack with no packs row is
-- treated as 'stored', so this is a pure forward migration: existing packs keep
-- behaving exactly as before until a reclaimer marks one.
ALTER TABLE packs ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'stored';

-- pack_assoc records which owner CONTEXT references which pack, so GC liveness
-- is a query rather than a walk over every manifest, and so reclamation can be
-- scoped to one owner. A pack is live while any row names it.
CREATE TABLE IF NOT EXISTS pack_assoc (
    domain     TEXT        NOT NULL,
    pack_hash  TEXT        NOT NULL,
    context    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (domain, pack_hash, context)
);
-- Drops are by (domain, context); the PK already serves lookups by pack.
CREATE INDEX IF NOT EXISTS pack_assoc_by_context ON pack_assoc (domain, context);

-- pack_assoc_backfill marks a domain whose associations have been populated from
-- pre-existing manifests. Until a domain is marked, the association-derived live
-- set is refused (it would judge legacy data garbage).
CREATE TABLE IF NOT EXISTS pack_assoc_backfill (
    domain       TEXT        NOT NULL PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Migrate creates the tables if they don't exist.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("pgindex: migrate: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// snapshot.DedupStore — chunk → pack dedup index
// ---------------------------------------------------------------------------

func (s *Store) Lookup(ctx context.Context, domain, chunkHash string) (_ snapshot.PackRef, _ bool, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "lookup", start, rerr) }()
	var ref snapshot.PackRef
	var off, length int64
	err := s.db.QueryRowContext(ctx,
		`SELECT pm.pack_hash, pm.offset_b, pm.length_b
		   FROM pack_manifest pm
		   LEFT JOIN packs p ON p.domain = pm.domain AND p.pack_hash = pm.pack_hash
		  WHERE pm.domain=$1 AND pm.chunk_hash=$2
		    AND COALESCE(p.state, 'stored') = 'stored'`,
		domain, chunkHash).Scan(&ref.PackHash, &off, &length)
	if err == sql.ErrNoRows {
		return snapshot.PackRef{}, false, nil
	}
	if err != nil {
		rerr = fmt.Errorf("pgindex: lookup: %w", err)
		return snapshot.PackRef{}, false, rerr
	}
	ref.Offset = int(off)
	ref.Size = int(length)
	return ref, true, nil
}

func (s *Store) LookupBatch(ctx context.Context, domain string, chunkHashes []string) (_ map[string]snapshot.PackRef, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "lookup", start, rerr) }()
	out := make(map[string]snapshot.PackRef, len(chunkHashes))
	if len(chunkHashes) == 0 {
		return out, nil
	}
	placeholders, args := inClause(domain, chunkHashes)
	q := `SELECT pm.chunk_hash, pm.pack_hash, pm.offset_b, pm.length_b
	        FROM pack_manifest pm
	        LEFT JOIN packs p ON p.domain = pm.domain AND p.pack_hash = pm.pack_hash
	       WHERE pm.domain=$1 AND COALESCE(p.state, 'stored') = 'stored'
	         AND pm.chunk_hash IN (` + placeholders + `)`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("pgindex: lookup batch: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chunkHash string
		var ref snapshot.PackRef
		var off, length int64
		if err := rows.Scan(&chunkHash, &ref.PackHash, &off, &length); err != nil {
			return nil, fmt.Errorf("pgindex: lookup batch scan: %w", err)
		}
		ref.Offset = int(off)
		ref.Size = int(length)
		out[chunkHash] = ref
	}
	return out, rows.Err()
}

func (s *Store) Record(ctx context.Context, domain string, locs []snapshot.ChunkLocation) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "record", start, rerr) }()
	if len(locs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgindex: record begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO pack_manifest (domain, chunk_hash, pack_hash, offset_b, length_b)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (domain, chunk_hash) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("pgindex: record prepare: %w", err)
	}
	defer stmt.Close()
	for _, loc := range locs {
		if _, err := stmt.ExecContext(ctx, domain, loc.ChunkHash, loc.PackHash, int64(loc.Offset), int64(loc.Size)); err != nil {
			return fmt.Errorf("pgindex: record exec: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pgindex: record commit: %w", err)
	}
	return nil
}

func (s *Store) PurgePacks(ctx context.Context, domain string, packHashes []string) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "gc.purge", start, rerr) }()
	if len(packHashes) == 0 {
		return nil
	}
	placeholders, args := inClause(domain, packHashes)
	// Drop both the per-chunk dedup entries AND the per-pack physical-size rows
	// for the reclaimed packs, so a purged pack leaves neither a dangling dedup
	// reference nor a phantom byte count in the domain's physical SUM. The two
	// deletes share the same (domain, packHashes) predicate/args.
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM pack_manifest WHERE domain=$1 AND pack_hash IN (`+placeholders+`)`, args...); err != nil {
		rerr = fmt.Errorf("pgindex: purge packs: %w", err)
		return rerr
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM packs WHERE domain=$1 AND pack_hash IN (`+placeholders+`)`, args...); err != nil {
		rerr = fmt.Errorf("pgindex: purge packs (packs table): %w", err)
		return rerr
	}
	return nil
}

// RecordPackSizes upserts the physical (compressed, on-disk) size of one or
// more packs in domain, backing snapshot.PackSizeRecorder. It is called by
// casstore's pack-write and compaction paths immediately after a pack blob is
// stored, so the recorded size always refers to bytes that physically exist. On
// CONFLICT it overwrites the recorded size (a compaction that rewrites a pack at
// the same hash — content-addressed, so the bytes are identical — is idempotent;
// an upsert also self-heals a partially-written prior row). Batched in one tx,
// mirroring Record. A purged pack's row is dropped by PurgePacks.
func (s *Store) RecordPackSizes(ctx context.Context, domain string, sizes []snapshot.PackSize) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "record.packsize", start, rerr) }()
	if len(sizes) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgindex: record pack sizes begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO packs (domain, pack_hash, compressed_pack_bytes)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (domain, pack_hash) DO UPDATE SET compressed_pack_bytes = EXCLUDED.compressed_pack_bytes`)
	if err != nil {
		return fmt.Errorf("pgindex: record pack sizes prepare: %w", err)
	}
	defer stmt.Close()
	for _, ps := range sizes {
		if _, err := stmt.ExecContext(ctx, domain, ps.PackHash, ps.CompressedBytes); err != nil {
			return fmt.Errorf("pgindex: record pack sizes exec: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pgindex: record pack sizes commit: %w", err)
	}
	return nil
}

// UsageStats is the per-domain storage rollup DomainUsage returns.
type UsageStats struct {
	// PhysicalBytes is the effective on-disk footprint: SUM of every pack's
	// compressed size (dedup × compression). Complete — mlfs and blobgw share the
	// pack substrate, so this covers both.
	PhysicalBytes int64
	// LogicalDedupedBytes is the unique, uncompressed content size: SUM of
	// length_b over DISTINCT chunk hashes (each unique chunk counted once).
	LogicalDedupedBytes int64
	// ObjectApparentBytes is the pre-dedup size of blobgw OBJECTS only:
	// SUM(blob_ref.size_b WHERE domain). mlfs slice refs live in the tenant's
	// separate mlfs meta DB and are combined downstream by data-connectors.
	ObjectApparentBytes int64
}

// DomainUsage computes the per-domain storage rollup: PhysicalBytes (SUM of the
// packs' compressed sizes — the per-PACK physical truth, never per-chunk),
// LogicalDedupedBytes (SUM of length_b over DISTINCT chunk hashes), and
// ObjectApparentBytes (pre-dedup size of blobgw OBJECTS only = SUM(blob_ref.size_b
// WHERE domain)). mlfs slice refs live in the tenant's separate mlfs meta DB and
// are combined downstream by data-connectors. Both pack queries are indexed range
// scans over the domain's rows.
func (s *Store) DomainUsage(ctx context.Context, domain string) (_ UsageStats, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "usage", start, rerr) }()
	var us UsageStats
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(compressed_pack_bytes),0) FROM packs WHERE domain=$1`,
		domain).Scan(&us.PhysicalBytes); err != nil {
		rerr = fmt.Errorf("pgindex: domain usage physical: %w", err)
		return UsageStats{}, rerr
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(length_b),0) FROM (SELECT DISTINCT chunk_hash, length_b FROM pack_manifest WHERE domain=$1) t`,
		domain).Scan(&us.LogicalDedupedBytes); err != nil {
		rerr = fmt.Errorf("pgindex: domain usage logical: %w", err)
		return UsageStats{}, rerr
	}
	// ObjectApparentBytes: pre-dedup size of blobgw objects only. blob_ref is
	// blobgw's own domain-scoped table. mlfs slice refs live in the tenant's
	// separate mlfs meta DB; data-connectors combines both sides downstream.
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(size_b),0) FROM blob_ref WHERE domain=$1`,
		domain).Scan(&us.ObjectApparentBytes); err != nil {
		rerr = fmt.Errorf("pgindex: domain usage apparent (blob_ref): %w", err)
		return UsageStats{}, rerr
	}
	return us, nil
}

// ---------------------------------------------------------------------------
// gateway.RefStore — logical object index
// ---------------------------------------------------------------------------

func (s *Store) Put(ctx context.Context, info gateway.ObjectInfo) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "put", start, rerr) }()
	if info.Ref == "" {
		return gateway.ErrInvalidRef
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO blob_ref (domain, ref, content_hash, size_b, content_type, version, pending, staging_key, created_at, user_meta, expected_hash, expected_size)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (domain, ref) DO UPDATE SET
		   content_hash=EXCLUDED.content_hash, size_b=EXCLUDED.size_b, content_type=EXCLUDED.content_type,
		   version=EXCLUDED.version, pending=EXCLUDED.pending, staging_key=EXCLUDED.staging_key, created_at=EXCLUDED.created_at,
		   user_meta=EXCLUDED.user_meta, expected_hash=EXCLUDED.expected_hash, expected_size=EXCLUDED.expected_size`,
		info.Domain, info.Ref, info.ContentHash, info.Size, info.ContentType, info.Version, info.Pending, info.StagingKey, info.CreatedAt.UTC(),
		encodeUserMeta(info.UserMeta), nullableHash(info.ExpectedHash), nullableSize(info.ExpectedSize))
	if err != nil {
		return fmt.Errorf("pgindex: ref put: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, domain, ref string) (_ gateway.ObjectInfo, _ bool, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "get", start, rerr) }()
	row := s.db.QueryRowContext(ctx,
		`SELECT `+blobRefColumns+`
		 FROM blob_ref WHERE domain=$1 AND ref=$2`,
		domain, ref)
	info, err := scanObjectInfo(row)
	if err == sql.ErrNoRows {
		return gateway.ObjectInfo{}, false, nil
	}
	if err != nil {
		rerr = fmt.Errorf("pgindex: ref get: %w", err)
		return gateway.ObjectInfo{}, false, rerr
	}
	return info, true, nil
}

func (s *Store) Delete(ctx context.Context, domain, ref string) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "delete", start, rerr) }()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM blob_ref WHERE domain=$1 AND ref=$2`, domain, ref); err != nil {
		rerr = fmt.Errorf("pgindex: ref delete: %w", err)
		return rerr
	}
	return nil
}

func (s *Store) List(ctx context.Context, domain, prefix string, limit int) (_ []gateway.ObjectInfo, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "list", start, rerr) }()
	lim := limit
	if lim <= 0 {
		lim = 1 << 30 // effectively unbounded
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+blobRefColumns+`
		 FROM blob_ref WHERE domain=$1 AND ref LIKE $2 ESCAPE '\'
		 ORDER BY created_at DESC, ref ASC LIMIT $3`,
		domain, escapeLike(prefix)+"%", lim)
	if err != nil {
		return nil, fmt.Errorf("pgindex: ref list: %w", err)
	}
	defer rows.Close()
	var out []gateway.ObjectInfo
	for rows.Next() {
		info, err := scanObjectInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("pgindex: ref list scan: %w", err)
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// ListPage is the keyset-paginated form of List. It returns objects under
// prefix in the same (created_at DESC, ref ASC) order, starting strictly after
// the opaque cursor `after`, at most limit rows (limit<=0 → an internal default
// page), plus the cursor of the last row when more remain (next=="" = the page
// is exhausted). The keyset predicate matches the List order: a row qualifies if
// its created_at is older than the cursor's, or equal with a strictly larger ref.
func (s *Store) ListPage(ctx context.Context, domain, prefix, after string, limit int) (_ []gateway.ObjectInfo, _ string, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "list", start, rerr) }()
	afterTime, afterRef, err := gateway.DecodeListCursor(after)
	if err != nil {
		return nil, "", err
	}
	lim := limit
	if lim <= 0 {
		lim = 1000 // default page when the caller leaves it unbounded
	}
	// Build the query: the keyset predicate is only added when a cursor is set,
	// so the first page (after=="") is identical to List's filter.
	args := []any{domain, escapeLike(prefix) + "%"}
	where := `domain=$1 AND ref LIKE $2 ESCAPE '\'`
	if after != "" {
		args = append(args, afterTime.UTC(), afterRef)
		where += ` AND (created_at < $3 OR (created_at = $3 AND ref > $4))`
	}
	// Fetch one extra row to detect whether more pages remain without a count.
	args = append(args, lim+1)
	q := `SELECT ` + blobRefColumns + `
		 FROM blob_ref WHERE ` + where + `
		 ORDER BY created_at DESC, ref ASC LIMIT $` + fmt.Sprintf("%d", len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("pgindex: ref list page: %w", err)
	}
	defer rows.Close()
	var out []gateway.ObjectInfo
	for rows.Next() {
		info, err := scanObjectInfo(rows)
		if err != nil {
			return nil, "", fmt.Errorf("pgindex: ref list page scan: %w", err)
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("pgindex: ref list page: %w", err)
	}
	next := ""
	if len(out) > lim {
		last := out[lim-1]
		next = gateway.EncodeListCursor(last.CreatedAt, last.Ref)
		out = out[:lim]
	}
	return out, next, nil
}

func (s *Store) ListStalePending(ctx context.Context, domain string, olderThan time.Time, limit int) (_ []gateway.ObjectInfo, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "list", start, rerr) }()
	lim := limit
	if lim <= 0 {
		lim = 1 << 30 // effectively unbounded
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+blobRefColumns+`
		 FROM blob_ref WHERE domain=$1 AND pending=TRUE AND created_at<=$2
		 ORDER BY created_at ASC, ref ASC LIMIT $3`,
		domain, olderThan.UTC(), lim)
	if err != nil {
		return nil, fmt.Errorf("pgindex: ref list stale pending: %w", err)
	}
	defer rows.Close()
	var out []gateway.ObjectInfo
	for rows.Next() {
		info, err := scanObjectInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("pgindex: ref list stale pending scan: %w", err)
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// blobRefColumns is the canonical SELECT column list for blob_ref, in the order
// scanObjectInfo reads them. Centralizing it keeps every read path (Get / List /
// ListPage / ListStalePending) and the scan in lockstep, so a column added in
// one place can't drift out of sync with the scan in another.
const blobRefColumns = `domain, ref, content_hash, size_b, content_type, version, pending, staging_key, created_at, user_meta, expected_hash, expected_size`

// rowScanner is the subset of *sql.Row / *sql.Rows that scanObjectInfo needs, so
// the single-row (QueryRowContext) and multi-row (rows.Next loop) read paths
// share one scan helper.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanObjectInfo scans one blob_ref row (the blobRefColumns set, in order) into
// a gateway.ObjectInfo, decoding the post-initial columns via applyExtraCols. It
// returns the underlying Scan error verbatim (including sql.ErrNoRows for the
// single-row path) so callers keep their existing error wrapping. It replaces
// the byte-identical scan-then-applyExtraCols block duplicated across Get, List,
// ListPage, and ListStalePending.
func scanObjectInfo(sc rowScanner) (gateway.ObjectInfo, error) {
	var info gateway.ObjectInfo
	var um []byte
	var eh sql.NullString
	var es sql.NullInt64
	if err := sc.Scan(&info.Domain, &info.Ref, &info.ContentHash, &info.Size,
		&info.ContentType, &info.Version, &info.Pending, &info.StagingKey, &info.CreatedAt, &um, &eh, &es); err != nil {
		return gateway.ObjectInfo{}, err
	}
	if err := applyExtraCols(&info, um, eh, es); err != nil {
		return gateway.ObjectInfo{}, err
	}
	return info, nil
}

// inClause builds the comma-separated positional placeholder list ($2,$3,…) for
// a SQL IN (...) over vals, with $1 reserved for the leading domain argument. It
// returns the placeholder string and the full args slice (domain followed by the
// vals), so LookupBatch and PurgePacks share one builder.
func inClause(domain string, vals []string) (placeholders string, args []any) {
	args = make([]any, 0, len(vals)+1)
	args = append(args, domain)
	ph := make([]string, len(vals))
	for i, v := range vals {
		ph[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, v)
	}
	return strings.Join(ph, ","), args
}

// escapeLike escapes the LIKE metacharacters in a literal prefix so it matches
// exactly (with '\' as the escape char, per the queries' ESCAPE clause).
func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// encodeUserMeta marshals the caller's user metadata to a JSONB value for the
// blob_ref.user_meta column. nil/empty in → SQL NULL out (no JSON written), so
// rows without metadata read back as a nil map (parity with the manifest path).
func encodeUserMeta(m map[string]string) any {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		// map[string]string always marshals; treat a failure as "no metadata"
		// rather than failing the whole write.
		return nil
	}
	return b
}

// nullableHash maps an unset ("") expected hash to SQL NULL so the integrity
// gate's "no expectation" state is stored as NULL, not an empty string.
func nullableHash(h string) any {
	if h == "" {
		return nil
	}
	return h
}

// nullableSize maps an unset (<=0) expected size to SQL NULL (0/negative =
// "no expectation").
func nullableSize(n int64) any {
	if n <= 0 {
		return nil
	}
	return n
}

// applyExtraCols decodes the three columns added after blob_ref's initial shape
// (user_meta, expected_hash, expected_size) into info. A NULL/absent column
// leaves the zero value (nil map / "" / 0), so old rows scan cleanly.
func applyExtraCols(info *gateway.ObjectInfo, userMeta []byte, expectedHash sql.NullString, expectedSize sql.NullInt64) error {
	if len(userMeta) > 0 {
		m := make(map[string]string)
		if err := json.Unmarshal(userMeta, &m); err != nil {
			return fmt.Errorf("decode user_meta: %w", err)
		}
		if len(m) > 0 {
			info.UserMeta = m
		}
	}
	if expectedHash.Valid {
		info.ExpectedHash = expectedHash.String
	}
	if expectedSize.Valid {
		info.ExpectedSize = expectedSize.Int64
	}
	return nil
}
