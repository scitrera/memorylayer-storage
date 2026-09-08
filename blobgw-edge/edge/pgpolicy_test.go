// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
)

// openPolicyDB connects to the Postgres named by BLOBGW_TEST_DATABASE_URL,
// migrates the edge policy schema, and returns the pool plus a unique random
// id prefix so concurrent/repeat runs never collide on tenant/key/jti rows.
// Skips when the env var is unset, mirroring pgindex's convention. Cleanup
// removes only this run's rows.
func openPolicyDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("BLOBGW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BLOBGW_TEST_DATABASE_URL (a pgx DSN) to run edge pg-policy integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := edge.PgMigrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	id := "t" + randHexStr(t, 8)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DELETE FROM edge_quota WHERE tenant LIKE $1", id+"%")
		_, _ = db.ExecContext(ctx, "DELETE FROM edge_rate_bucket WHERE bucket_key LIKE $1", id+"%")
		_, _ = db.ExecContext(ctx, "DELETE FROM edge_revocation WHERE jti LIKE $1", id+"%")
		_ = db.Close()
	})
	return db, id
}

func randHexStr(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}

// TestPgQuota_PersistsAcrossRestart asserts that storage accounting survives a
// "restart" (a fresh store over the same DB) and that the ceiling is enforced.
func TestPgQuota_PersistsAcrossRestart(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	tenant := id + "-acme"

	q1 := edge.NewPgQuotaStore(db, 1000, 0, nil)
	if err := q1.Authorize(ctx, tenant, 400); err != nil {
		t.Fatalf("authorize first: %v", err)
	}
	q1.RecordPut(ctx, tenant, 400)
	q1.RecordPut(ctx, tenant, 400)

	// Restart: a new store over the same DB must see the accumulated 800 bytes.
	q2 := edge.NewPgQuotaStore(db, 1000, 0, nil)
	if err := q2.Authorize(ctx, tenant, 100); err != nil {
		t.Errorf("authorize within quota after restart: %v", err)
	}
	if err := q2.Authorize(ctx, tenant, 300); !errors.Is(err, edge.ErrQuota) {
		t.Errorf("over-quota after restart: got %v, want ErrQuota", err)
	}

	// Delete releases accounting (clamped at zero).
	q2.RecordDelete(ctx, tenant, 800)
	if err := q2.Authorize(ctx, tenant, 900); err != nil {
		t.Errorf("authorize after release: %v", err)
	}
}

// TestPgQuota_MaxObjects enforces the object-count ceiling.
func TestPgQuota_MaxObjects(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	tenant := id + "-objs"

	q := edge.NewPgQuotaStore(db, 0, 1, nil) // at most one object
	if err := q.Authorize(ctx, tenant, 10); err != nil {
		t.Fatalf("first authorize: %v", err)
	}
	q.RecordPut(ctx, tenant, 10)
	if err := q.Authorize(ctx, tenant, 10); !errors.Is(err, edge.ErrQuota) {
		t.Errorf("second object: got %v, want ErrQuota", err)
	}
}

// TestPgQuota_TenantLister asserts the Postgres quota store enumerates the
// tenants that have quota rows — the dynamic "tenants with data" source the
// leader GC runner sweeps. A tenant appears after its first RecordPut.
func TestPgQuota_TenantLister(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	q := edge.NewPgQuotaStore(db, 0, 0, nil)

	a, b := id+"-acme", id+"-beta"
	q.RecordPut(ctx, a, 100)
	q.RecordPut(ctx, b, 200)

	names, err := q.Tenants(ctx)
	if err != nil {
		t.Fatalf("Tenants: %v", err)
	}
	// Filter to this run's rows (the table is shared across concurrent runs).
	seen := map[string]bool{}
	for _, n := range names {
		if n == a || n == b {
			seen[n] = true
		}
	}
	if !seen[a] || !seen[b] {
		t.Fatalf("Tenants missing this run's tenants: got %v, want to contain %q,%q", names, a, b)
	}
}

// TestPgRateLimiter_PersistsAcrossRestart asserts the token bucket consumes the
// burst, denies once empty, and that the depleted state survives a restart.
func TestPgRateLimiter_PersistsAcrossRestart(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	key := id + "-tenant:sub"
	frozen := time.Now().UTC()

	// burst 2, ~no refill; freeze the clock so no tokens accrue.
	l1 := edge.NewPgRateLimiter(db, 0.0001, 2, nil)
	l1.SetClock(func() time.Time { return frozen })
	if !l1.Allow(ctx, key) {
		t.Fatal("first Allow should pass (burst token)")
	}
	if !l1.Allow(ctx, key) {
		t.Fatal("second Allow should pass (burst token)")
	}
	if l1.Allow(ctx, key) {
		t.Fatal("third Allow should be denied (bucket empty)")
	}

	// Restart: a new limiter over the same DB, same frozen clock, must see the
	// drained bucket and keep denying.
	l2 := edge.NewPgRateLimiter(db, 0.0001, 2, nil)
	l2.SetClock(func() time.Time { return frozen })
	if l2.Allow(ctx, key) {
		t.Error("after restart the drained bucket must still deny")
	}

	// Advancing the clock enough refills one token.
	l2.SetClock(func() time.Time { return frozen.Add(3 * time.Hour) }) // 0.0001*10800 > 1
	if !l2.Allow(ctx, key) {
		t.Error("after refill window the bucket should allow again")
	}
}

// TestPgRateLimiter_HonorsContextCancellation asserts that a cancelled context
// short-circuits the Allow transaction (fail-open) rather than hanging: with the
// limiter enabled but the ctx already cancelled, the PG round trip fails and
// Allow returns true (never wedge the data path on a limiter fault).
func TestPgRateLimiter_HonorsContextCancellation(t *testing.T) {
	db, id := openPolicyDB(t)
	key := id + "-cancel:sub"

	l := edge.NewPgRateLimiter(db, 1, 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the BeginTx/query must not block
	if !l.Allow(ctx, key) {
		t.Error("Allow under a cancelled ctx must fail OPEN (true), not deny/hang")
	}
}

// TestPgRevocation_PersistsAndPrunes asserts a revoked jti is honored across a
// restart, an unrevoked jti is not, and that lapsed entries are pruned.
func TestPgRevocation_PersistsAndPrunes(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	revoked := id + "-jti-revoked"
	other := id + "-jti-other"
	frozen := time.Now().UTC()

	r1 := edge.NewPgRevocation(db, time.Hour, nil)
	r1.SetClock(func() time.Time { return frozen })
	// Zero expiry → fall back to the fixed retention TTL (frozen + 1h).
	r1.Revoke(ctx, revoked, time.Time{})
	if !r1.IsRevoked(ctx, revoked) {
		t.Fatal("freshly revoked jti must report revoked")
	}
	if r1.IsRevoked(ctx, other) {
		t.Error("never-revoked jti must not report revoked")
	}

	// Restart: a new instance over the same DB still honors the revocation.
	r2 := edge.NewPgRevocation(db, time.Hour, nil)
	r2.SetClock(func() time.Time { return frozen })
	if !r2.IsRevoked(ctx, revoked) {
		t.Error("revocation must survive restart")
	}

	// Past the retention window the entry is ignored and pruned away.
	r2.SetClock(func() time.Time { return frozen.Add(2 * time.Hour) })
	if r2.IsRevoked(ctx, revoked) {
		t.Error("lapsed revocation must no longer be honored")
	}
	n, err := r2.Prune(ctx)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if n < 1 {
		t.Errorf("Prune removed %d, want >= 1", n)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM edge_revocation WHERE jti=$1", revoked).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Errorf("pruned jti still present: %d rows", remaining)
	}
}

// TestPgRevocation_HonorsTokenExpiry asserts that when Revoke is given the
// token's own expiry, the denylist entry stops being honored exactly at that
// expiry — even when it is EARLIER than the fixed retention fallback. This is
// the #34 fix: the entry expires with the token instead of lingering for a fixed
// 24h.
func TestPgRevocation_HonorsTokenExpiry(t *testing.T) {
	db, id := openPolicyDB(t)
	ctx := context.Background()
	jti := id + "-jti-shortexp"
	frozen := time.Now().UTC()

	// Retention fallback is a long 24h, but the token itself expires in 5 minutes.
	r := edge.NewPgRevocation(db, 24*time.Hour, nil)
	r.SetClock(func() time.Time { return frozen })
	tokenExpiry := frozen.Add(5 * time.Minute)
	r.Revoke(ctx, jti, tokenExpiry)

	if !r.IsRevoked(ctx, jti) {
		t.Fatal("revoked jti must be honored before its expiry")
	}
	// Confirm the row's expires_at is the token expiry, NOT now+retention.
	var storedExp time.Time
	if err := db.QueryRowContext(ctx, "SELECT expires_at FROM edge_revocation WHERE jti=$1", jti).Scan(&storedExp); err != nil {
		t.Fatalf("read expires_at: %v", err)
	}
	if d := storedExp.UTC().Sub(tokenExpiry); d < -time.Second || d > time.Second {
		t.Errorf("expires_at=%v, want token expiry %v (not now+retention)", storedExp.UTC(), tokenExpiry)
	}

	// Just past the token's expiry the entry is no longer honored, long before the
	// 24h retention fallback would have lapsed.
	r.SetClock(func() time.Time { return tokenExpiry.Add(time.Second) })
	if r.IsRevoked(ctx, jti) {
		t.Error("revocation must stop being honored at the token's own expiry")
	}
}
