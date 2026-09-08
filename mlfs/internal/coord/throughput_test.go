// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// TestThroughputDifferentScopesScale measures the L2.8 scaling property: because
// ownership is per-scope, two nodes writing DIFFERENT scopes proceed in parallel
// rather than serializing on a global lock. It compares the wall time of 2N
// writes done serially by one node vs. N+N writes done concurrently by two nodes
// on independent scopes, and reports the speedup. The plan target is ≥1.8x; we
// assert a conservative floor (the shared Postgres caps the absolute ratio on
// any given box) and log the measured value.
func TestThroughputDifferentScopesScale(t *testing.T) {
	db, a, b, oa, _, _ := twoNATSNodes(t)
	ctx, c := context.Background(), meta.Background()

	// Two sibling subtrees + a file in each; nodeA owns both initially.
	for _, d := range []string{"sa", "sb"} {
		if _, _, st := a.Mkdir(ctx, meta.RootInode, d, 0o755, c); st != 0 {
			t.Fatalf("mkdir %s: %v", d, st)
		}
	}
	sa, _, _ := a.Lookup(ctx, meta.RootInode, "sa")
	sb, _, _ := a.Lookup(ctx, meta.RootInode, "sb")
	fa, _, st := a.Create(ctx, sa, "fa", 0o644, c)
	if st != 0 {
		t.Fatalf("create fa: %v", st)
	}
	fb, _, st := a.Create(ctx, sb, "fb", 0o644, c)
	if st != 0 {
		t.Fatalf("create fb: %v", st)
	}
	// Hand scope sb to nodeB (model a clean release), then nodeB acquires it.
	scopeB, _ := a.ScopeOf(ctx, fb)
	_ = oa.kv.Delete(ctx, natsKey(scopeB))
	if _, err := db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scopeB); err != nil {
		t.Fatalf("release sb: %v", err)
	}
	if st := b.Write(ctx, fb, 0, 0, ds(1)); st != 0 {
		t.Fatalf("nodeB acquire sb: %v", st)
	}

	const n = 300
	writeN := func(e *meta.Engine, f meta.Ino, base uint64, count int) {
		for i := 0; i < count; i++ {
			if st := e.Write(ctx, f, 0, 0, ds(base+uint64(i))); st != 0 {
				t.Errorf("write: %v", st)
				return
			}
		}
	}

	// Serial baseline: one node does all 2N writes.
	t0 := time.Now()
	writeN(a, fa, 10_000, 2*n)
	serial := time.Since(t0)

	// Parallel: two nodes, each N writes to its own scope, concurrently.
	t1 := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); writeN(a, fa, 20_000, n) }()
	go func() { defer wg.Done(); writeN(b, fb, 30_000, n) }()
	wg.Wait()
	parallel := time.Since(t1)

	ratio := float64(serial) / float64(parallel)
	t.Logf("throughput: serial(2N=%d) %v (%.0f w/s); parallel(2x%d) %v (%.0f w/s); speedup %.2fx (plan target >=1.8x)",
		2*n, serial, float64(2*n)/serial.Seconds(),
		n, parallel, float64(2*n)/parallel.Seconds(), ratio)

	// Conservative floor: independent scopes must give a real speedup (no global
	// serialization). The absolute ratio is box-dependent (shared Postgres).
	if ratio < 1.3 {
		t.Fatalf("different-scope writes did not scale: speedup %.2fx < 1.3x", ratio)
	}
}
