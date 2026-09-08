// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package manifeststore provides a PostgreSQL-backed snapshot.SnapshotStore for
// mlfs slice manifests. It stores the manifest payload bytes alongside the
// SnapshotMetadata fields in a dedicated table, avoiding per-read S3 round-trips
// and keeping slice manifest metadata consistent with the rest of the mlfs PG
// metadata.
//
// The store is stateless over a *sql.DB pool and is safe for concurrent use.
// Schema migration follows the same pattern as internal/meta: a single
// idempotent DDL string applied via Migrate, registered separately from the
// meta engine so that the manifeststore can be wired independently.
package manifeststore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver
)

// Schema is the manifeststore DDL, idempotent. The table uses (tenant,
// workspace, user_field, owner_key, version) as a natural composite key.
// version is the RFC3339Nano+hex string produced by snapshot.NewVersion, which
// sorts lexicographically newest-last, so "MAX(version)" gives GetLatest
// without a separate index scan — ORDER BY version DESC LIMIT 1 is the query
// shape.
//
// Columns mirror SnapshotMetadata exactly plus the raw payload blob.
const Schema = `
CREATE TABLE IF NOT EXISTS slice_manifest (
    tenant      TEXT        NOT NULL DEFAULT '',
    workspace   TEXT        NOT NULL DEFAULT '',
    user_field  TEXT        NOT NULL DEFAULT '',
    owner_key   TEXT        NOT NULL,
    version     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    size_bytes  BIGINT      NOT NULL DEFAULT 0,
    runtime     TEXT        NOT NULL DEFAULT '',
    format      TEXT        NOT NULL DEFAULT '',
    tags        JSONB,
    payload     BYTEA       NOT NULL,
    PRIMARY KEY (tenant, workspace, user_field, owner_key, version)
);
CREATE INDEX IF NOT EXISTS slice_manifest_latest
    ON slice_manifest (tenant, workspace, user_field, owner_key, version DESC);
`

// Migrate applies the schema against db. Safe to call repeatedly (all
// statements are idempotent). Follows the same pattern as (*meta.Engine).Migrate.
func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("manifeststore: migrate schema: %w", err)
	}
	return nil
}

// Store is a PostgreSQL-backed snapshot.SnapshotStore. It is stateless and
// concurrency-safe.
type Store struct {
	db  *sql.DB
	log *slog.Logger
}

// compile-time interface assertions
var (
	_ snapshot.SnapshotStore     = (*Store)(nil)
	_ snapshot.BatchLatestGetter = (*Store)(nil)
	_ snapshot.PayloadWalker     = (*Store)(nil)
)

// New wraps db as a Store. db must already be migrated (call Migrate first).
// log may be nil; in that case slog.Default() is used.
func New(db *sql.DB, log *slog.Logger) *Store {
	if log == nil {
		log = slog.Default()
	}
	return &Store{db: db, log: log}
}

// Put writes a new snapshot version for key. The store generates the version
// via snapshot.NewVersion, reads the payload fully from r, stores both, then
// returns the stamped metadata. SizeBytes in the input meta is ignored; the
// store computes it from the stream.
func (s *Store) Put(ctx context.Context, key snapshot.SnapshotKey, meta snapshot.SnapshotMetadata, r io.Reader) (snapshot.SnapshotMetadata, error) {
	payload, err := io.ReadAll(r)
	if err != nil {
		return snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: read payload: %w", err)
	}

	version := snapshot.NewVersion()
	createdAt := time.Now().UTC()
	if !meta.CreatedAt.IsZero() {
		createdAt = meta.CreatedAt.UTC()
	}

	meta.Key = key
	meta.Version = version
	meta.SizeBytes = int64(len(payload))
	meta.CreatedAt = createdAt

	tagsJSON, err := marshalTags(meta.Tags)
	if err != nil {
		return snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: marshal tags: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO slice_manifest
		    (tenant, workspace, user_field, owner_key, version, created_at,
		     size_bytes, runtime, format, tags, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey,
		version, createdAt,
		meta.SizeBytes, meta.Runtime, meta.Format, tagsJSON, payload,
	)
	if err != nil {
		return snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: put %v version %s: %w", key, version, err)
	}

	s.log.DebugContext(ctx, "manifeststore: put complete",
		"key", key, "version", version, "size", meta.SizeBytes)
	return meta, nil
}

// Get retrieves the payload and metadata for a specific version. Caller must
// Close the returned reader. Returns ErrNoSnapshot if the version is absent.
func (s *Store) Get(ctx context.Context, key snapshot.SnapshotKey, version string) (io.ReadCloser, snapshot.SnapshotMetadata, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT created_at, size_bytes, runtime, format, tags, payload
		 FROM slice_manifest
		 WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key=$4 AND version=$5`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey, version,
	)
	meta, payload, err := scanRow(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, snapshot.SnapshotMetadata{}, snapshot.ErrNoSnapshot
		}
		return nil, snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: get %v version %s: %w", key, version, err)
	}
	meta.Key = key
	meta.Version = version
	s.log.DebugContext(ctx, "manifeststore: get", "key", key, "version", version)
	return io.NopCloser(bytes.NewReader(payload)), meta, nil
}

// GetLatest returns the blob and metadata for the highest-version snapshot of
// key. Returns ErrNoSnapshot when none exist.
func (s *Store) GetLatest(ctx context.Context, key snapshot.SnapshotKey) (io.ReadCloser, snapshot.SnapshotMetadata, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, created_at, size_bytes, runtime, format, tags, payload
		 FROM slice_manifest
		 WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key=$4
		 ORDER BY version DESC
		 LIMIT 1`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey,
	)
	meta, payload, err := scanRowWithVersion(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, snapshot.SnapshotMetadata{}, snapshot.ErrNoSnapshot
		}
		return nil, snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: get-latest %v: %w", key, err)
	}
	meta.Key = key
	s.log.DebugContext(ctx, "manifeststore: get-latest", "key", key, "version", meta.Version)
	return io.NopCloser(bytes.NewReader(payload)), meta, nil
}

// GetLatestBatch resolves the latest version of each key in keys, returning each
// key's manifest payload + metadata. Keys with no manifest are absent from the
// result (not an error). It runs ONE query per distinct (tenant, workspace,
// user_field) partition; mlfs slice keys set only OwnerKey, so that is a single
// query. The shape is DISTINCT ON (owner_key) ... ORDER BY owner_key, version
// DESC — the slice_manifest_latest index serves it directly — which removes the
// ~N−1 round trips a per-key GetLatest loop costs a prefetch window (and relieves
// the metadata-pool pressure that bounds -readahead-concurrency). Implements
// snapshot.BatchLatestGetter.
func (s *Store) GetLatestBatch(ctx context.Context, keys []snapshot.SnapshotKey) (map[snapshot.SnapshotKey]snapshot.BatchLatestEntry, error) {
	out := make(map[snapshot.SnapshotKey]snapshot.BatchLatestEntry, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	// Group owner_keys by their (tenant, workspace, user) partition, de-duplicated.
	type part struct{ tenant, workspace, user string }
	groups := make(map[part][]string)
	seen := make(map[snapshot.SnapshotKey]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		p := part{k.Tenant, k.Workspace, k.User}
		groups[p] = append(groups[p], k.OwnerKey)
	}
	for p, owners := range groups {
		if err := s.getLatestPartition(ctx, p.tenant, p.workspace, p.user, owners, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// getLatestPartition runs the DISTINCT ON batch query for one (tenant, workspace,
// user) partition and merges the rows into out.
func (s *Store) getLatestPartition(ctx context.Context, tenant, workspace, user string, owners []string, out map[snapshot.SnapshotKey]snapshot.BatchLatestEntry) error {
	// Build "owner_key IN ($4,$5,...)" — placeholders after the 3 partition args.
	args := make([]any, 0, 3+len(owners))
	args = append(args, tenant, workspace, user)
	var in strings.Builder
	for i, ok := range owners {
		if i > 0 {
			in.WriteByte(',')
		}
		fmt.Fprintf(&in, "$%d", i+4)
		args = append(args, ok)
	}
	q := `SELECT DISTINCT ON (owner_key)
	          owner_key, version, created_at, size_bytes, runtime, format, tags, payload
	      FROM slice_manifest
	      WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key IN (` + in.String() + `)
	      ORDER BY owner_key, version DESC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("manifeststore: get-latest-batch (%d keys): %w", len(owners), err)
	}
	defer rows.Close()
	for rows.Next() {
		var ownerKey, version, runtime, format string
		var createdAt time.Time
		var sizeBytes int64
		var tagsJSON, payload []byte
		if err := rows.Scan(&ownerKey, &version, &createdAt, &sizeBytes, &runtime, &format, &tagsJSON, &payload); err != nil {
			return fmt.Errorf("manifeststore: get-latest-batch scan: %w", err)
		}
		tags, err := unmarshalTags(tagsJSON)
		if err != nil {
			return fmt.Errorf("manifeststore: get-latest-batch tags: %w", err)
		}
		key := snapshot.SnapshotKey{Tenant: tenant, Workspace: workspace, User: user, OwnerKey: ownerKey}
		out[key] = snapshot.BatchLatestEntry{
			Meta: snapshot.SnapshotMetadata{
				Key: key, Version: version, CreatedAt: createdAt.UTC(),
				SizeBytes: sizeBytes, Runtime: runtime, Format: format, Tags: tags,
			},
			Payload: payload,
		}
	}
	return rows.Err()
}

// GetLatestMetadata returns only the metadata for the highest-version snapshot
// without fetching the payload. Returns ErrNoSnapshot when none exist.
func (s *Store) GetLatestMetadata(ctx context.Context, key snapshot.SnapshotKey) (snapshot.SnapshotMetadata, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, created_at, size_bytes, runtime, format, tags
		 FROM slice_manifest
		 WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key=$4
		 ORDER BY version DESC
		 LIMIT 1`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey,
	)
	meta, err := scanMetaOnly(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snapshot.SnapshotMetadata{}, snapshot.ErrNoSnapshot
		}
		return snapshot.SnapshotMetadata{}, fmt.Errorf("manifeststore: get-latest-metadata %v: %w", key, err)
	}
	meta.Key = key
	s.log.DebugContext(ctx, "manifeststore: get-latest-metadata", "key", key, "version", meta.Version)
	return meta, nil
}

// List returns metadata for all versions of key, newest first (descending
// version order). Returns an empty slice (not ErrNoSnapshot) when none exist.
func (s *Store) List(ctx context.Context, key snapshot.SnapshotKey) ([]snapshot.SnapshotMetadata, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT version, created_at, size_bytes, runtime, format, tags
		 FROM slice_manifest
		 WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key=$4
		 ORDER BY version DESC`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey,
	)
	if err != nil {
		return nil, fmt.Errorf("manifeststore: list %v: %w", key, err)
	}
	defer rows.Close()

	var out []snapshot.SnapshotMetadata
	for rows.Next() {
		meta, err := scanMetaOnly(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("manifeststore: list scan %v: %w", key, err)
		}
		meta.Key = key
		out = append(out, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifeststore: list rows %v: %w", key, err)
	}
	return out, nil
}

// Delete removes a specific version. Returns nil if already absent (idempotent).
func (s *Store) Delete(ctx context.Context, key snapshot.SnapshotKey, version string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM slice_manifest
		 WHERE tenant=$1 AND workspace=$2 AND user_field=$3 AND owner_key=$4 AND version=$5`,
		key.Tenant, key.Workspace, key.User, key.OwnerKey, version,
	)
	if err != nil {
		return fmt.Errorf("manifeststore: delete %v version %s: %w", key, version, err)
	}
	s.log.DebugContext(ctx, "manifeststore: deleted", "key", key, "version", version)
	return nil
}

// Walk yields every (key, version, meta) tuple in the store, in unspecified
// order. If the yield function returns an error, Walk stops and returns that
// error. Used by GC to compute the live set across all stored manifests.
// Payload is NOT fetched by Walk to bound memory on large stores.
//
// Conservative GC behaviour: if a row's tags JSON fails to unmarshal, Walk
// does NOT skip the row. Instead it yields the manifest with nil Tags and
// logs a WARN. Skipping a manifest would allow GC to treat the corresponding
// blobs as unreferenced and delete them — a silent data-loss risk. Callers
// that need valid tags should handle nil Tags defensively.
func (s *Store) Walk(ctx context.Context, yield func(snapshot.SnapshotKey, string, snapshot.SnapshotMetadata) error) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant, workspace, user_field, owner_key, version,
		        created_at, size_bytes, runtime, format, tags
		 FROM slice_manifest`,
	)
	if err != nil {
		return fmt.Errorf("manifeststore: walk: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key snapshot.SnapshotKey
		var version string
		var createdAt time.Time
		var sizeBytes int64
		var runtime, format string
		var tagsJSON []byte

		if err := rows.Scan(
			&key.Tenant, &key.Workspace, &key.User, &key.OwnerKey, &version,
			&createdAt, &sizeBytes, &runtime, &format, &tagsJSON,
		); err != nil {
			return fmt.Errorf("manifeststore: walk scan: %w", err)
		}

		tags, err := unmarshalTags(tagsJSON)
		if err != nil {
			// Conservative: do NOT skip — yield with nil Tags so GC keeps the
			// manifest in its live set and does not delete referenced blobs.
			s.log.WarnContext(ctx, "manifeststore: walk: corrupt tags, yielding with nil tags",
				"key", key, "version", version, "err", err)
			tags = nil
		}

		meta := snapshot.SnapshotMetadata{
			Key:       key,
			Version:   version,
			CreatedAt: createdAt.UTC(),
			SizeBytes: sizeBytes,
			Runtime:   runtime,
			Format:    format,
			Tags:      tags,
		}
		if err := yield(key, version, meta); err != nil {
			return err
		}
	}
	return rows.Err()
}

// WalkPayloads streams every manifest's payload ALONGSIDE its metadata in one
// query, so the casstore GC can parse chunk refs inline instead of issuing a
// Get per manifest (the M-individual-round-trips cost, TECH_DEBT #28). Rows are
// streamed (the payload is handed to the callback, never accumulated), so memory
// stays bounded on large stores. Same conservative tag handling as Walk: a corrupt
// tags blob yields nil Tags + a WARN rather than skipping the row (skipping would
// let GC delete referenced blobs). Implements snapshot.PayloadWalker.
func (s *Store) WalkPayloads(ctx context.Context, yield func(key snapshot.SnapshotKey, version string, meta snapshot.SnapshotMetadata, payload []byte) error) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant, workspace, user_field, owner_key, version,
		        created_at, size_bytes, runtime, format, tags, payload
		 FROM slice_manifest`,
	)
	if err != nil {
		return fmt.Errorf("manifeststore: walk-payloads: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key snapshot.SnapshotKey
		var version string
		var createdAt time.Time
		var sizeBytes int64
		var runtime, format string
		var tagsJSON, payload []byte

		if err := rows.Scan(
			&key.Tenant, &key.Workspace, &key.User, &key.OwnerKey, &version,
			&createdAt, &sizeBytes, &runtime, &format, &tagsJSON, &payload,
		); err != nil {
			return fmt.Errorf("manifeststore: walk-payloads scan: %w", err)
		}
		tags, err := unmarshalTags(tagsJSON)
		if err != nil {
			s.log.WarnContext(ctx, "manifeststore: walk-payloads: corrupt tags, yielding with nil tags",
				"key", key, "version", version, "err", err)
			tags = nil
		}
		meta := snapshot.SnapshotMetadata{
			Key:       key,
			Version:   version,
			CreatedAt: createdAt.UTC(),
			SizeBytes: sizeBytes,
			Runtime:   runtime,
			Format:    format,
			Tags:      tags,
		}
		if err := yield(key, version, meta, payload); err != nil {
			return err
		}
	}
	return rows.Err()
}

// --- scan helpers ------------------------------------------------------------

// scanRow scans created_at, size_bytes, runtime, format, tags, payload (no
// version column — used by Get where version is already known).
func scanRow(scan func(...any) error) (snapshot.SnapshotMetadata, []byte, error) {
	var createdAt time.Time
	var sizeBytes int64
	var runtime, format string
	var tagsJSON, payload []byte

	if err := scan(&createdAt, &sizeBytes, &runtime, &format, &tagsJSON, &payload); err != nil {
		return snapshot.SnapshotMetadata{}, nil, err
	}
	tags, err := unmarshalTags(tagsJSON)
	if err != nil {
		return snapshot.SnapshotMetadata{}, nil, err
	}
	return snapshot.SnapshotMetadata{
		CreatedAt: createdAt.UTC(),
		SizeBytes: sizeBytes,
		Runtime:   runtime,
		Format:    format,
		Tags:      tags,
	}, payload, nil
}

// scanRowWithVersion scans version, created_at, size_bytes, runtime, format,
// tags, payload. Used by GetLatest.
func scanRowWithVersion(scan func(...any) error) (snapshot.SnapshotMetadata, []byte, error) {
	var version string
	var createdAt time.Time
	var sizeBytes int64
	var runtime, format string
	var tagsJSON, payload []byte

	if err := scan(&version, &createdAt, &sizeBytes, &runtime, &format, &tagsJSON, &payload); err != nil {
		return snapshot.SnapshotMetadata{}, nil, err
	}
	tags, err := unmarshalTags(tagsJSON)
	if err != nil {
		return snapshot.SnapshotMetadata{}, nil, err
	}
	return snapshot.SnapshotMetadata{
		Version:   version,
		CreatedAt: createdAt.UTC(),
		SizeBytes: sizeBytes,
		Runtime:   runtime,
		Format:    format,
		Tags:      tags,
	}, payload, nil
}

// scanMetaOnly scans version, created_at, size_bytes, runtime, format, tags
// (no payload). Used by GetLatestMetadata and List.
func scanMetaOnly(scan func(...any) error) (snapshot.SnapshotMetadata, error) {
	var version string
	var createdAt time.Time
	var sizeBytes int64
	var runtime, format string
	var tagsJSON []byte

	if err := scan(&version, &createdAt, &sizeBytes, &runtime, &format, &tagsJSON); err != nil {
		return snapshot.SnapshotMetadata{}, err
	}
	tags, err := unmarshalTags(tagsJSON)
	if err != nil {
		return snapshot.SnapshotMetadata{}, err
	}
	return snapshot.SnapshotMetadata{
		Version:   version,
		CreatedAt: createdAt.UTC(),
		SizeBytes: sizeBytes,
		Runtime:   runtime,
		Format:    format,
		Tags:      tags,
	}, nil
}

// --- JSON tag helpers --------------------------------------------------------

func marshalTags(tags map[string]string) ([]byte, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	return json.Marshal(tags)
}

func unmarshalTags(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var tags map[string]string
	if err := json.Unmarshal(data, &tags); err != nil {
		return nil, fmt.Errorf("unmarshal tags: %w", err)
	}
	return tags, nil
}
