// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
)

// TestMemoryQuota_TenantLister asserts the in-memory quota store enumerates
// exactly the tenants with accounted data (bytes or object count > 0), and drops
// a tenant once its accounting is fully released — the dynamic "tenants with
// data" source the leader GC runner sweeps.
func TestMemoryQuota_TenantLister(t *testing.T) {
	ctx := context.Background()
	q := edge.NewMemoryQuotaStore(0, 0) // unlimited; we only exercise accounting

	// No data yet → no tenants.
	if got, err := q.Tenants(ctx); err != nil || len(got) != 0 {
		t.Fatalf("empty store: got %v err %v, want []", got, err)
	}

	q.RecordPut(ctx, "acme", 100)
	q.RecordPut(ctx, "beta", 0) // zero bytes but one object → still has data
	q.RecordPut(ctx, "acme", 50)

	got, err := q.Tenants(ctx)
	if err != nil {
		t.Fatalf("Tenants: %v", err)
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "acme" || got[1] != "beta" {
		t.Fatalf("tenants with data: got %v, want [acme beta]", got)
	}

	// Release beta's single object → beta drops out (count back to zero); acme
	// still has bytes+objects.
	q.RecordDelete(ctx, "beta", 0)
	got, err = q.Tenants(ctx)
	if err != nil {
		t.Fatalf("Tenants after delete: %v", err)
	}
	if len(got) != 1 || got[0] != "acme" {
		t.Fatalf("after releasing beta: got %v, want [acme]", got)
	}
}

// TestMemoryQuota_ImplementsTenantLister is a compile-time-ish guard that the
// concrete memory quota store satisfies the TenantLister seam the runner wiring
// depends on.
func TestMemoryQuota_ImplementsTenantLister(t *testing.T) {
	var _ edge.TenantLister = edge.NewMemoryQuotaStore(0, 0)
}

// TestMemoryRevocation_HonorsTokenExpiry asserts the in-memory denylist stops
// honoring a revoked jti exactly at the supplied token expiry (the #34 fix), and
// that a zero expiry is honored indefinitely.
func TestMemoryRevocation_HonorsTokenExpiry(t *testing.T) {
	ctx := context.Background()
	rev := edge.NewMemoryRevocation()
	frozen := time.Now().UTC()
	rev.SetClock(func() time.Time { return frozen })

	// Explicit expiry: honored before, dropped at/after.
	exp := frozen.Add(5 * time.Minute)
	rev.Revoke(ctx, "with-exp", exp)
	if !rev.IsRevoked(ctx, "with-exp") {
		t.Fatal("revoked jti must be honored before its expiry")
	}
	rev.SetClock(func() time.Time { return exp }) // now == expiry → no longer honored
	if rev.IsRevoked(ctx, "with-exp") {
		t.Error("revocation must stop being honored at the token's own expiry")
	}

	// Zero expiry: honored regardless of clock (until process exit).
	rev.SetClock(func() time.Time { return frozen })
	rev.Revoke(ctx, "no-exp", time.Time{})
	rev.SetClock(func() time.Time { return frozen.Add(1000 * time.Hour) })
	if !rev.IsRevoked(ctx, "no-exp") {
		t.Error("a zero-expiry revocation must be honored indefinitely")
	}

	// An unknown jti is never revoked.
	if rev.IsRevoked(ctx, "unknown") {
		t.Error("never-revoked jti must not report revoked")
	}
}

// TestTokenBucketLimiter_AcceptsContext is a light guard that the in-memory
// limiter honors the ctx-carrying Allow signature (it ignores ctx but must
// compile + behave: disabled limiter always allows, enabled one spends burst).
func TestTokenBucketLimiter_AcceptsContext(t *testing.T) {
	ctx := context.Background()

	disabled := edge.NewTokenBucketLimiter(0, 0)
	if !disabled.Allow(ctx, "k") {
		t.Error("disabled limiter must always allow")
	}

	l := edge.NewTokenBucketLimiter(0.0001, 1) // burst 1, negligible refill
	if !l.Allow(ctx, "k") {
		t.Error("first Allow should pass (burst token)")
	}
	if l.Allow(ctx, "k") {
		t.Error("second Allow should be denied (bucket empty)")
	}
}
