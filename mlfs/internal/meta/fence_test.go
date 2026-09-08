// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"syscall"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// twoNodes returns two engines (distinct nodeIDs, SQL owners) sharing ONE
// metadata schema — the multi-node setup: same SoR, independent leases.
func twoNodes(t *testing.T) (a, b *Engine) {
	t.Helper()
	db := testpg.DB(t)
	a = Open(db, 1)
	b = Open(db, 2)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	a.SetCoordinator("nodeA", NewSQLOwner(a, "nodeA", 0, 0))
	b.SetCoordinator("nodeB", NewSQLOwner(b, "nodeB", 0, 0))
	return a, b
}

// dummySlice is a meta-only write (no chunk store touched).
func dummySlice(id uint64) Slice { return Slice{Id: id, Size: 16, Off: 0, Len: 16} }

// TestFenceRejectsStaleWriterAfterSteal is the core L2.8 safety property: once a
// scope is stolen (generation bumped), the previous owner's write is fenced with
// ESTALE instead of corrupting state under a moved generation.
func TestFenceRejectsStaleWriterAfterSteal(t *testing.T) {
	a, b := twoNodes(t)
	ctx, c := context.Background(), Background()

	// nodeA establishes a subtree and a file in it, owning scope ino:<s1>.
	if _, _, st := a.Mkdir(ctx, RootInode, "s1", 0o755, c); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	s1, _, st := a.Lookup(ctx, RootInode, "s1")
	if st != 0 {
		t.Fatalf("lookup s1: %v", st)
	}
	f, _, st := a.Create(ctx, s1, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create f: %v", st)
	}
	if st := a.Write(ctx, f, 0, 0, dummySlice(1001)); st != 0 {
		t.Fatalf("nodeA first write: %v", st)
	}

	scope, err := a.scopeOf(ctx, f)
	if err != nil {
		t.Fatalf("scopeOf: %v", err)
	}

	// Force the lease to look expired so nodeB legitimately steals it (bumping
	// the generation) on its next write to the same scope.
	if _, err := a.db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scope); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// nodeB writes to the same file: it steals the scope and succeeds.
	if st := b.Write(ctx, f, 0, 0, dummySlice(2002)); st != 0 {
		t.Fatalf("nodeB write (should steal + succeed): %v", st)
	}

	// nodeA still believes it owns the old generation (fast-path cache). Its next
	// write MUST be fenced.
	if st := a.Write(ctx, f, 0, 0, dummySlice(1003)); st != syscall.ESTALE {
		t.Fatalf("nodeA stale write: got %v, want ESTALE", st)
	}

	// The fence is generation-based, not a permanent ban: once nodeA re-acquires
	// the (now nodeB-owned, but we re-expire it) scope, it writes again.
	if _, err := a.db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scope); err != nil {
		t.Fatalf("re-expire lease: %v", err)
	}
	// Drop nodeA's stale cached lease so Ensure re-acquires from SQL.
	a.owner.(*SQLOwner).releaseAll(ctx)
	if st := a.Write(ctx, f, 0, 0, dummySlice(1004)); st != 0 {
		t.Fatalf("nodeA re-acquire write: %v", st)
	}
}

// TestSingleNodeOwnerDisabledNoFence confirms the default engine (no
// coordinator) skips the fence entirely: writes never see ESTALE and scopeOf is
// never consulted on the hot path.
func TestSingleNodeOwnerDisabledNoFence(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := context.Background(), Background()
	if e.owner.Enabled() {
		t.Fatal("default engine should have fencing disabled")
	}
	if _, _, st := e.Mkdir(ctx, RootInode, "d", 0o755, c); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	d, _, _ := e.Lookup(ctx, RootInode, "d")
	f, _, st := e.Create(ctx, d, "g", 0o644, c)
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	for i := 0; i < 3; i++ {
		if st := e.Write(ctx, f, 0, 0, dummySlice(uint64(7000+i))); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
	}
}

// TestScopeOfKeys checks scope resolution: root is its own scope, and every node
// resolves to its first-level ancestor.
func TestScopeOfKeys(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := context.Background(), Background()
	if got, _ := e.scopeOf(ctx, RootInode); got != rootScopeKey {
		t.Fatalf("root scope = %q, want %q", got, rootScopeKey)
	}
	top, _, _ := e.Mkdir(ctx, RootInode, "top", 0o755, c)
	mid, _, _ := e.Mkdir(ctx, top, "mid", 0o755, c)
	leaf, _, _ := e.Create(ctx, mid, "leaf", 0o644, c)

	want := scopeKeyForInode(top)
	for _, in := range []Ino{top, mid, leaf} {
		got, err := e.scopeOf(ctx, in)
		if err != nil {
			t.Fatalf("scopeOf(%d): %v", in, err)
		}
		if got != want {
			t.Fatalf("scopeOf(%d) = %q, want %q (first-level ancestor)", in, got, want)
		}
	}
}
