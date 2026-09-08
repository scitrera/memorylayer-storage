// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/fsstat"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// runStats is the read-only filesystem inspector: a point-in-time view of what
// the metadata DB and the local node's on-disk cache/casstore hold, for
// operators sizing capacity, spotting a write-back backlog, or confirming dedup
// is working. Unlike gc/fsck it is SAFE to run against a live mount — it only
// issues read queries and walks directories, so there is no quiesce/lock guard.
//
// A single invocation prints a point-in-time snapshot. With -watch it re-samples
// every -interval and prints a compact delta line — write activity is derived
// from the monotonic fs_changelog LSN (≈ write ops/s) plus slice-row and
// logical-byte growth. That gives a live activity feed by polling alone: the
// daemon exposes no metrics endpoint, so there is nothing else to scrape.
func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	metaDSN := fs.String("meta-dsn", os.Getenv("MLFS_META_DSN"), "PostgreSQL DSN for the metadata engine (pgx)")
	dataDir := fs.String("data-dir", "/var/lib/mlfs/data", "local casstore backend directory (pack occupancy; ignored/empty in -remote mode)")
	cacheDir := fs.String("cache-dir", "/var/lib/mlfs/cache", "local disk cache + write-back staging directory (occupancy + backlog)")
	watch := fs.Bool("watch", false, "re-sample every -interval and print live deltas (Ctrl-C to stop)")
	interval := fs.Duration("interval", 2*time.Second, "sampling interval for -watch")
	asJSON := fs.Bool("json", false, "emit the snapshot as JSON (single sample; ignores -watch)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *metaDSN == "" {
		return fmt.Errorf("-meta-dsn (or $MLFS_META_DSN) is required")
	}
	ctx := context.Background()

	db, err := sql.Open("pgx", *metaDSN)
	if err != nil {
		return fmt.Errorf("open meta db: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping meta db: %w", err)
	}
	// Idempotent CREATE TABLE IF NOT EXISTS, matching gc/fsck — guarantees the
	// queries below resolve even against a never-mounted DB. A no-op on a live one.
	engine := meta.Open(db, 0)
	if err := engine.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate meta schema: %w", err)
	}

	dirs := diskPaths{data: *dataDir, cache: filepath.Join(*cacheDir, "cache"),
		staging: filepath.Join(*cacheDir, "staging"), cacheRoot: *cacheDir}

	first, err := collectStats(ctx, db, dirs)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(first)
	}
	printStats(os.Stdout, first)
	if !*watch {
		return nil
	}

	fmt.Fprintf(os.Stdout, "\nwatching (interval=%s); deltas are per-second:\n", *interval)
	prev := first
	prevAt := first.SampledAt
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for range tick.C {
		cur, err := collectStats(ctx, db, dirs)
		if err != nil {
			return err
		}
		printStatsDelta(os.Stdout, prev, cur, cur.SampledAt.Sub(prevAt).Seconds())
		prev, prevAt = cur, cur.SampledAt
	}
	return nil
}

// statsSnapshot is a point-in-time view of the filesystem, assembled from the
// metadata DB and the on-disk cache/casstore. Fields are exported for -json.
type statsSnapshot struct {
	SampledAt time.Time `json:"sampled_at"`

	// Metadata (PostgreSQL).
	Files       int64 `json:"files"`
	Dirs        int64 `json:"dirs"`
	Symlinks    int64 `json:"symlinks"`
	OtherNodes  int64 `json:"other_nodes"`
	LogicalSize int64 `json:"logical_size_bytes"` // SUM(node.length) over files
	Edges       int64 `json:"edges"`              // directory entries (incl. hardlinks)
	XAttrs      int64 `json:"xattrs"`

	SliceRows    int64 `json:"slice_rows"`         // rows in slice_ref (physical refs)
	UniqueSlices int64 `json:"unique_slices"`      // DISTINCT slice_id (shared by CoW/dedup)
	SliceLive    int64 `json:"slice_live_bytes"`   // SUM(slen): bytes actually referenced
	SliceSource  int64 `json:"slice_source_bytes"` // SUM(size): source slice bytes

	ChangelogLSN    int64 `json:"changelog_lsn"` // MAX(lsn): monotonic write counter
	ChangelogRows   int64 `json:"changelog_rows"`
	ChangelogOldest int64 `json:"changelog_oldest_ts"` // unix sec; 0 if empty
	Flocks          int64 `json:"flocks"`
	Plocks          int64 `json:"plocks"`
	Scopes          int64 `json:"ownership_scopes"`
	ActiveLeases    int64 `json:"active_leases"`

	// On-disk (local node), 0 when the dir is absent (e.g. -remote has no data-dir).
	CacheFiles   int64 `json:"cache_files"`
	CacheBytes   int64 `json:"cache_bytes"` // clean read cache occupancy
	StagingFiles int64 `json:"staging_files"`
	StagingBytes int64 `json:"staging_bytes"` // write-back backlog (un-uploaded)
	DataFiles    int64 `json:"data_files"`
	DataBytes    int64 `json:"data_bytes"` // local casstore packs (local mode)

	// Filesystem capacity of the cache disk (statfs).
	CacheFSAvail int64 `json:"cache_fs_avail_bytes"`
	CacheFSTotal int64 `json:"cache_fs_total_bytes"`
}

type diskPaths struct{ data, cache, staging, cacheRoot string }

func collectStats(ctx context.Context, db *sql.DB, dirs diskPaths) (statsSnapshot, error) {
	var s statsSnapshot
	s.SampledAt = time.Now()

	// Node counts by type + logical size, in one grouped scan.
	rows, err := db.QueryContext(ctx, `SELECT type, COUNT(*), COALESCE(SUM(length),0) FROM node GROUP BY type`)
	if err != nil {
		return s, fmt.Errorf("query node: %w", err)
	}
	for rows.Next() {
		var typ int16
		var cnt, length int64
		if err := rows.Scan(&typ, &cnt, &length); err != nil {
			rows.Close()
			return s, fmt.Errorf("scan node: %w", err)
		}
		switch uint8(typ) {
		case meta.TypeFile:
			s.Files += cnt
			s.LogicalSize += length
		case meta.TypeDirectory:
			s.Dirs += cnt
		case meta.TypeSymlink:
			s.Symlinks += cnt
		default:
			s.OtherNodes += cnt
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return s, fmt.Errorf("iterate node: %w", err)
	}
	rows.Close()

	type scalar struct {
		label string
		row   *sql.Row
		dest  []any
	}
	now := s.SampledAt.UnixMilli()
	scalars := []scalar{
		{"edge", db.QueryRowContext(ctx, `SELECT COUNT(*) FROM edge`), []any{&s.Edges}},
		{"xattr", db.QueryRowContext(ctx, `SELECT COUNT(*) FROM xattr`), []any{&s.XAttrs}},
		{"slice_ref", db.QueryRowContext(ctx,
			`SELECT COUNT(*), COUNT(DISTINCT slice_id), COALESCE(SUM(slen),0), COALESCE(SUM(size),0) FROM slice_ref`),
			[]any{&s.SliceRows, &s.UniqueSlices, &s.SliceLive, &s.SliceSource}},
		{"fs_changelog", db.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(lsn),0), COUNT(*), COALESCE(MIN(ts),0) FROM fs_changelog`),
			[]any{&s.ChangelogLSN, &s.ChangelogRows, &s.ChangelogOldest}},
		{"flock", db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flock`), []any{&s.Flocks}},
		{"plock", db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plock`), []any{&s.Plocks}},
		{"ownership", db.QueryRowContext(ctx,
			`SELECT COUNT(*), COUNT(*) FILTER (WHERE lease_expires_unix_ms > $1) FROM ownership`, now),
			[]any{&s.Scopes, &s.ActiveLeases}},
	}
	for _, q := range scalars {
		if err := q.row.Scan(q.dest...); err != nil {
			return s, fmt.Errorf("query %s: %w", q.label, err)
		}
	}

	s.CacheFiles, s.CacheBytes = fsstat.DirUsage(dirs.cache)
	s.StagingFiles, s.StagingBytes = fsstat.DirUsage(dirs.staging)
	s.DataFiles, s.DataBytes = fsstat.DirUsage(dirs.data)
	s.CacheFSAvail, s.CacheFSTotal = fsstat.StatfsBytes(dirs.cacheRoot)
	return s, nil
}

// printStats renders a point-in-time snapshot as an aligned, sectioned report.
func printStats(w *os.File, s statsSnapshot) {
	dedup := "—"
	if s.SliceLive > 0 {
		dedup = fmt.Sprintf("%.2fx", float64(s.SliceSource)/float64(s.SliceLive))
	}
	usedPct := "—"
	if s.CacheFSTotal > 0 {
		usedPct = fmt.Sprintf("%.0f%%", 100*float64(s.CacheFSTotal-s.CacheFSAvail)/float64(s.CacheFSTotal))
	}
	fmt.Fprintf(w, "mlfs stats @ %s\n", s.SampledAt.Format(time.RFC3339))
	fmt.Fprintln(w, "  metadata")
	fmt.Fprintf(w, "    files=%d dirs=%d symlinks=%d other=%d  logical=%s\n",
		s.Files, s.Dirs, s.Symlinks, s.OtherNodes, fsstat.HumanBytes(s.LogicalSize))
	fmt.Fprintf(w, "    edges=%d xattrs=%d  flocks=%d plocks=%d\n", s.Edges, s.XAttrs, s.Flocks, s.Plocks)
	fmt.Fprintf(w, "    slices: rows=%d unique=%d live=%s source=%s dedup=%s\n",
		s.SliceRows, s.UniqueSlices, fsstat.HumanBytes(s.SliceLive), fsstat.HumanBytes(s.SliceSource), dedup)
	fmt.Fprintf(w, "    changelog: lsn=%d rows=%d%s\n", s.ChangelogLSN, s.ChangelogRows, oldestSuffix(s.ChangelogOldest))
	fmt.Fprintf(w, "    ownership: scopes=%d active_leases=%d\n", s.Scopes, s.ActiveLeases)
	fmt.Fprintln(w, "  on-disk (local node)")
	fmt.Fprintf(w, "    cache:   files=%d size=%s\n", s.CacheFiles, fsstat.HumanBytes(s.CacheBytes))
	fmt.Fprintf(w, "    staging: files=%d size=%s  (write-back backlog)\n", s.StagingFiles, fsstat.HumanBytes(s.StagingBytes))
	fmt.Fprintf(w, "    data:    files=%d size=%s  (local casstore packs)\n", s.DataFiles, fsstat.HumanBytes(s.DataBytes))
	fmt.Fprintf(w, "    cache fs: %s free of %s (%s used)\n",
		fsstat.HumanBytes(s.CacheFSAvail), fsstat.HumanBytes(s.CacheFSTotal), usedPct)
}

func oldestSuffix(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return fmt.Sprintf(" oldest=%s", time.Unix(ts, 0).Format(time.RFC3339))
}

// printStatsDelta prints one watch-mode line: monotonic counters as per-second
// rates, occupancy as signed deltas. secs is the wall time between samples.
func printStatsDelta(w *os.File, prev, cur statsSnapshot, secs float64) {
	if secs <= 0 {
		secs = 1
	}
	writeOps := float64(cur.ChangelogLSN-prev.ChangelogLSN) / secs
	sliceRows := float64(cur.SliceRows-prev.SliceRows) / secs
	logical := float64(cur.LogicalSize-prev.LogicalSize) / secs
	fmt.Fprintf(w, "  %s  writes=%.0f/s slices=%+.0f/s logical=%s/s | files=%d staging=%s backlog=%+d cache=%s\n",
		cur.SampledAt.Format("15:04:05"), writeOps, sliceRows, fsstat.HumanBytes(int64(logical)),
		cur.Files, fsstat.HumanBytes(cur.StagingBytes), cur.StagingFiles-prev.StagingFiles, fsstat.HumanBytes(cur.CacheBytes))
}
