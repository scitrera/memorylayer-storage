// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// Postgres-backed policy state for blobgw-edge.
//
// The in-memory QuotaStore / RateLimiter / Revocation (policy.go) lose all
// state on restart: a tenant's used-bytes resets to zero, every rate-limit
// bucket refills, and the jti denylist empties so revoked-but-unexpired tokens
// work again. These implementations back the same three interfaces with the
// Postgres the edge can already reach, so policy survives restarts and is
// shared across edge replicas. They use only database/sql; the caller supplies
// the driver and pool (e.g. github.com/jackc/pgx/v5/stdlib), matching pgindex.
//
// Select them by config/flag (see cmd/blobgw-edge); the Memory* impls remain
// the default for dev and tests.

// PgSchema is the idempotent DDL for the edge policy tables. PgMigrate applies
// it.
const PgSchema = `
CREATE TABLE IF NOT EXISTS edge_quota (
    tenant       TEXT   NOT NULL PRIMARY KEY,
    bytes_used   BIGINT NOT NULL DEFAULT 0,
    object_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS edge_rate_bucket (
    bucket_key TEXT             NOT NULL PRIMARY KEY,
    tokens     DOUBLE PRECISION NOT NULL,
    last_ts    TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS edge_revocation (
    jti        TEXT        NOT NULL PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS edge_revocation_by_expiry ON edge_revocation (expires_at);
`

// PgMigrate creates the edge policy tables if they don't exist.
func PgMigrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, PgSchema); err != nil {
		return fmt.Errorf("edge: pg policy migrate: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// QuotaStore — Postgres-backed per-tenant accounting
// ---------------------------------------------------------------------------

// PgQuotaStore is a Postgres-backed QuotaStore. One shared limit applies to
// every tenant (mirroring MemoryQuotaStore); 0 means unlimited. Safe for
// concurrent use and across replicas: accounting is a single atomic UPSERT.
type PgQuotaStore struct {
	db         *sql.DB
	maxBytes   int64
	maxObjects int64
	logger     *slog.Logger
}

var _ QuotaStore = (*PgQuotaStore)(nil)

// NewPgQuotaStore wraps db with the given per-tenant ceilings (0 = unlimited).
// logger may be nil (falls back to slog.Default); it records the rare write
// failures the QuotaStore interface cannot return.
func NewPgQuotaStore(db *sql.DB, maxBytes, maxObjects int64, logger *slog.Logger) *PgQuotaStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &PgQuotaStore{db: db, maxBytes: maxBytes, maxObjects: maxObjects, logger: logger}
}

func (q *PgQuotaStore) Authorize(ctx context.Context, tenant string, addBytes int64) error {
	if q.maxBytes <= 0 && q.maxObjects <= 0 {
		return nil // unlimited
	}
	var usedBytes, count int64
	err := q.db.QueryRowContext(ctx,
		`SELECT bytes_used, object_count FROM edge_quota WHERE tenant=$1`, tenant).
		Scan(&usedBytes, &count)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("edge: quota authorize: %w", err)
	}
	if q.maxObjects > 0 && count+1 > q.maxObjects {
		return ErrQuota
	}
	if q.maxBytes > 0 && usedBytes+addBytes > q.maxBytes {
		return ErrQuota
	}
	return nil
}

func (q *PgQuotaStore) RecordPut(ctx context.Context, tenant string, bytes int64) {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO edge_quota (tenant, bytes_used, object_count)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (tenant) DO UPDATE SET
		   bytes_used = edge_quota.bytes_used + EXCLUDED.bytes_used,
		   object_count = edge_quota.object_count + 1`,
		tenant, bytes)
	if err != nil {
		q.logger.ErrorContext(ctx, "edge: quota record put", "tenant", tenant, "bytes", bytes, "err", err)
	}
}

// Tenants returns every tenant with a quota row, satisfying TenantLister so the
// leader-elected GC runner sweeps exactly the tenants with data (the edge has no
// static tenant list). It is the "tenants with data" source: a tenant gets a row
// on its first RecordPut and keeps it while accounted. Rows clamp at zero on
// delete but are not removed, so a fully-released tenant may still appear — GC
// over an empty tenant is a cheap no-op (RunOnceForDomain finds no live/dead
// chunks), so this is safe and keeps the query a single indexed scan. It honors
// ctx so a cancelled sweep does not hang.
func (q *PgQuotaStore) Tenants(ctx context.Context) ([]string, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT tenant FROM edge_quota`)
	if err != nil {
		return nil, fmt.Errorf("edge: quota list tenants: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("edge: quota list tenants scan: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edge: quota list tenants iterate: %w", err)
	}
	return out, nil
}

var _ TenantLister = (*PgQuotaStore)(nil)

func (q *PgQuotaStore) RecordDelete(ctx context.Context, tenant string, bytes int64) {
	// Clamp at zero so a double-delete or stale size never drives the counters
	// negative (matching MemoryQuotaStore).
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO edge_quota (tenant, bytes_used, object_count)
		 VALUES ($1, 0, 0)
		 ON CONFLICT (tenant) DO UPDATE SET
		   bytes_used = GREATEST(edge_quota.bytes_used - $2, 0),
		   object_count = GREATEST(edge_quota.object_count - 1, 0)`,
		tenant, bytes)
	if err != nil {
		q.logger.ErrorContext(ctx, "edge: quota record delete", "tenant", tenant, "bytes", bytes, "err", err)
	}
}

// ---------------------------------------------------------------------------
// RateLimiter — Postgres-backed token bucket
// ---------------------------------------------------------------------------

// PgRateLimiter is a Postgres-backed per-key token-bucket RateLimiter: rate
// tokens accrue per second up to burst capacity, with bucket state persisted
// so limits hold across restarts and replicas. Each Allow is one
// read-modify-write transaction keyed by the bucket row, so concurrent callers
// serialize on that row.
type PgRateLimiter struct {
	db     *sql.DB
	rate   float64
	burst  float64
	logger *slog.Logger
	now    func() time.Time
}

var _ RateLimiter = (*PgRateLimiter)(nil)

// NewPgRateLimiter wraps db with ratePerSec tokens/sec up to burst. rate <= 0
// disables limiting (Allow always returns true), matching TokenBucketLimiter.
// logger may be nil (falls back to slog.Default).
func NewPgRateLimiter(db *sql.DB, ratePerSec, burst float64, logger *slog.Logger) *PgRateLimiter {
	if logger == nil {
		logger = slog.Default()
	}
	return &PgRateLimiter{db: db, rate: ratePerSec, burst: burst, logger: logger, now: time.Now}
}

// SetClock overrides the clock used for token refill (tests).
func (l *PgRateLimiter) SetClock(now func() time.Time) { l.now = now }

func (l *PgRateLimiter) Allow(ctx context.Context, key string) bool {
	if l.rate <= 0 {
		return true // disabled
	}
	now := l.now().UTC()

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		l.logger.ErrorContext(ctx, "edge: rate begin", "key", key, "err", err)
		return true // fail open: never wedge the data path on a limiter outage
	}
	defer func() { _ = tx.Rollback() }()

	var tokens float64
	var last time.Time
	err = tx.QueryRowContext(ctx,
		`SELECT tokens, last_ts FROM edge_rate_bucket WHERE bucket_key=$1 FOR UPDATE`, key).
		Scan(&tokens, &last)
	switch {
	case err == sql.ErrNoRows:
		// First request for this key: seed a full bucket and spend one token.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO edge_rate_bucket (bucket_key, tokens, last_ts) VALUES ($1, $2, $3)`,
			key, l.burst-1, now); err != nil {
			l.logger.ErrorContext(ctx, "edge: rate seed", "key", key, "err", err)
			return true
		}
		if err := tx.Commit(); err != nil {
			l.logger.ErrorContext(ctx, "edge: rate commit seed", "key", key, "err", err)
		}
		return true
	case err != nil:
		l.logger.ErrorContext(ctx, "edge: rate select", "key", key, "err", err)
		return true
	}

	elapsed := now.Sub(last).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	tokens = min(l.burst, tokens+elapsed*l.rate)
	if tokens < 1 {
		// Persist the refilled-but-insufficient state so the next call accrues
		// from this instant, then deny.
		if _, err := tx.ExecContext(ctx,
			`UPDATE edge_rate_bucket SET tokens=$2, last_ts=$3 WHERE bucket_key=$1`, key, tokens, now); err != nil {
			l.logger.ErrorContext(ctx, "edge: rate update deny", "key", key, "err", err)
			return true
		}
		if err := tx.Commit(); err != nil {
			l.logger.ErrorContext(ctx, "edge: rate commit deny", "key", key, "err", err)
		}
		return false
	}
	tokens--
	if _, err := tx.ExecContext(ctx,
		`UPDATE edge_rate_bucket SET tokens=$2, last_ts=$3 WHERE bucket_key=$1`, key, tokens, now); err != nil {
		l.logger.ErrorContext(ctx, "edge: rate update allow", "key", key, "err", err)
		return true
	}
	if err := tx.Commit(); err != nil {
		l.logger.ErrorContext(ctx, "edge: rate commit allow", "key", key, "err", err)
	}
	return true
}

// ---------------------------------------------------------------------------
// Revocation — Postgres-backed jti denylist
// ---------------------------------------------------------------------------

// PgRevocation is a Postgres-backed jti denylist. Each entry stores the revoked
// token's own expiry (edge_revocation.expires_at): Revoke sets expires_at to the
// capability's expiry so the entry is dropped exactly when the token would have
// expired anyway. When the caller cannot supply an expiry (zero), it falls back
// to a fixed retention window (the longest capability TTL plus slack) so a
// denylist entry can never outlive an unbounded token. IsRevoked ignores entries
// past expires_at, and Prune deletes them so the table stays bounded. State
// survives restarts so a revoked token cannot be resurrected by bouncing the
// edge.
type PgRevocation struct {
	db        *sql.DB
	retention time.Duration
	logger    *slog.Logger
	now       func() time.Time
}

var _ Revocation = (*PgRevocation)(nil)

// NewPgRevocation wraps db. retention is the FALLBACK window a revoked jti is
// honored for when Revoke is called with a zero expiry; set it to at least the
// maximum capability TTL so a token can never outlive its denylist entry. When
// Revoke supplies the token's real expiry, that expiry is used instead. logger
// may be nil (falls back to slog.Default).
func NewPgRevocation(db *sql.DB, retention time.Duration, logger *slog.Logger) *PgRevocation {
	if logger == nil {
		logger = slog.Default()
	}
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	return &PgRevocation{db: db, retention: retention, logger: logger, now: time.Now}
}

// SetClock overrides the clock used for expiry/pruning (tests).
func (r *PgRevocation) SetClock(now func() time.Time) { r.now = now }

func (r *PgRevocation) IsRevoked(ctx context.Context, jti string) bool {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM edge_revocation WHERE jti=$1 AND expires_at>$2)`,
		jti, r.now().UTC()).Scan(&exists)
	if err != nil {
		// Fail closed: an outage must not silently honor a revoked token.
		r.logger.ErrorContext(ctx, "edge: revocation check", "jti", jti, "err", err)
		return true
	}
	return exists
}

func (r *PgRevocation) Revoke(ctx context.Context, jti string, expiry time.Time) {
	// Set the denylist entry to expire exactly with the token when its expiry is
	// known; fall back to the fixed retention window only for a zero expiry (an
	// unbounded token or a caller that could not supply one), so an entry can never
	// outlive an unbounded token.
	expires := expiry.UTC()
	if expiry.IsZero() {
		expires = r.now().UTC().Add(r.retention)
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO edge_revocation (jti, expires_at) VALUES ($1, $2)
		 ON CONFLICT (jti) DO UPDATE SET expires_at=EXCLUDED.expires_at`,
		jti, expires)
	if err != nil {
		r.logger.ErrorContext(ctx, "edge: revoke", "jti", jti, "err", err)
	}
}

// Prune removes denylist entries whose retention window has lapsed and returns
// the count removed. Wire it on a maintenance loop so the table stays bounded.
func (r *PgRevocation) Prune(ctx context.Context) (int64, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM edge_revocation WHERE expires_at<=$1`, r.now().UTC())
	if err != nil {
		return 0, fmt.Errorf("edge: revocation prune: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
