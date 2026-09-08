// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// Chaos coverage for the L2.8 acceptance properties that are deterministic in a
// single-process harness (real Postgres + embedded NATS, two coordinated
// engines). The remaining acceptance items — `tc`-induced network partition
// self-fencing and the ≥1.8× different-scope throughput ratio — are ops-level
// validations that need multiple hosts / root and are exercised manually; the
// underlying primitives (generation fence, liveness expiry, scope independence)
// are what these tests pin down.

// TestChaosKilledLeaseHolderRecovery: when the lease holder dies (liveness key
// gone + SQL lease expired), a survivor steals the scope and writes; the dead
// node, if it returns still believing it owns the old generation, is fenced
// (ESTALE) — it self-fences via the authoritative SQL generation check.
func TestChaosKilledLeaseHolderRecovery(t *testing.T) {
	db, a, b, oa, _, _ := twoNATSNodes(t)
	ctx := context.Background()
	f, scope := setupFile(t, a) // nodeA owns scope

	if err := oa.kv.Delete(ctx, natsKey(scope)); err != nil { // liveness vanishes
		t.Fatalf("drop liveness: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scope); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	if st := b.Write(ctx, f, 0, 0, ds(100)); st != 0 {
		t.Fatalf("survivor steal write: %v", st)
	}
	if st := a.Write(ctx, f, 0, 0, ds(101)); st != syscall.ESTALE {
		t.Fatalf("revived owner write: got %v, want ESTALE (self-fence)", st)
	}
}

// TestChaosDifferentScopesIndependent: two nodes owning DIFFERENT scopes write
// concurrently without fencing each other — the correctness basis for
// scale-out. (The throughput ratio itself is a benchmark, not asserted here.)
func TestChaosDifferentScopesIndependent(t *testing.T) {
	db, a, b, oa, _, _ := twoNATSNodes(t)
	ctx, c := context.Background(), meta.Background()

	// nodeA builds two sibling subtrees and a file in each (owning both scopes).
	if _, _, st := a.Mkdir(ctx, meta.RootInode, "sa", 0o755, c); st != 0 {
		t.Fatalf("mkdir sa: %v", st)
	}
	if _, _, st := a.Mkdir(ctx, meta.RootInode, "sb", 0o755, c); st != 0 {
		t.Fatalf("mkdir sb: %v", st)
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

	// Hand scope sb to nodeB cleanly (model nodeA releasing it), then let nodeB
	// acquire it by writing once.
	scopeB, _ := a.ScopeOf(ctx, fb)
	_ = oa.kv.Delete(ctx, natsKey(scopeB))
	if _, err := db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scopeB); err != nil {
		t.Fatalf("expire sb: %v", err)
	}
	if st := b.Write(ctx, fb, 0, 0, ds(1)); st != 0 {
		t.Fatalf("nodeB acquire sb: %v", st)
	}

	// Concurrent independent writes: nodeA→fa (scope sa), nodeB→fb (scope sb).
	const n = 20
	var wg sync.WaitGroup
	errs := make([]syscall.Errno, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if st := a.Write(ctx, fa, 0, 0, ds(uint64(2000+i))); st != 0 {
				errs[0] = st
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if st := b.Write(ctx, fb, 0, 0, ds(uint64(3000+i))); st != 0 {
				errs[1] = st
				return
			}
		}
	}()
	wg.Wait()
	if errs[0] != 0 || errs[1] != 0 {
		t.Fatalf("independent-scope writes fenced each other: A=%v B=%v", errs[0], errs[1])
	}
}
