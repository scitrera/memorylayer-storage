// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"
)

// seedVersions writes n versions into the store under the given key. Returns
// the metadata list newest-first (matching SnapshotStore.List ordering).
func seedVersions(t *testing.T, store SnapshotStore, key SnapshotKey, n int) []SnapshotMetadata {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		payload := []byte("payload-" + strconv.Itoa(i))
		if _, err := store.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}
	metas, err := store.List(ctx, key)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != n {
		t.Fatalf("List returned %d versions, want %d", len(metas), n)
	}
	return metas
}

func TestPruneOldVersionsKeepsLatest(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	before := seedVersions(t, store, key, 7)
	// Keep the 3 newest → delete 4.
	deleted, err := PruneOldVersions(context.Background(), store, key, 3)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if deleted != 4 {
		t.Errorf("deleted = %d, want 4", deleted)
	}

	after, err := store.List(context.Background(), key)
	if err != nil {
		t.Fatalf("List after prune: %v", err)
	}
	if len(after) != 3 {
		t.Fatalf("post-prune count = %d, want 3", len(after))
	}
	// The 3 surviving versions must be the 3 newest (List is newest-first
	// before AND after prune).
	for i := 0; i < 3; i++ {
		if after[i].Version != before[i].Version {
			t.Errorf("post-prune[%d] = %q, want newest %q", i, after[i].Version, before[i].Version)
		}
	}
}

func TestPruneOldVersionsBelowThresholdNoOp(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 2)

	deleted, err := PruneOldVersions(context.Background(), store, key, 5)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected no deletions when count < keep, got %d", deleted)
	}
}

func TestPruneOldVersionsZeroKeepIsNoOp(t *testing.T) {
	// keepLatest <= 0 is documented to no-op (used for "pruning disabled").
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 4)

	deleted, err := PruneOldVersions(context.Background(), store, key, 0)
	if err != nil {
		t.Fatalf("Prune(0): %v", err)
	}
	if deleted != 0 {
		t.Errorf("Prune(0) deleted %d versions, want 0 (no-op)", deleted)
	}

	deletedNeg, err := PruneOldVersions(context.Background(), store, key, -1)
	if err != nil {
		t.Fatalf("Prune(-1): %v", err)
	}
	if deletedNeg != 0 {
		t.Errorf("Prune(-1) deleted %d versions, want 0", deletedNeg)
	}

	// Both no-ops should have left the original 4 in place.
	after, err := store.List(context.Background(), key)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(after) != 4 {
		t.Errorf("expected 4 versions intact after no-op prunes, got %d", len(after))
	}
}

func TestPruneOldVersionsEmptyStoreIsNoOp(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "no-snapshots-yet"}
	deleted, err := PruneOldVersions(context.Background(), store, key, 5)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deletes on empty store, got %d", deleted)
	}
}

func TestPruneByAgeKeepsNewestEvenIfOlderThanCutoff(t *testing.T) {
	// When *every* version is older than the cutoff, the newest one must
	// still survive — losing the only restore point to time-based pruning
	// is exactly the behaviour PruneByAge promises NOT to have.
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 3)

	// Cutoff in the future relative to the seeded snapshots → all are "too old".
	deleted, err := PruneByAge(context.Background(), store, key, -time.Hour)
	// Negative duration → disabled → no-op.
	if err != nil {
		t.Fatalf("PruneByAge(neg): %v", err)
	}
	if deleted != 0 {
		t.Errorf("negative duration should disable; deleted %d", deleted)
	}

	// Tiny positive duration with sleep ≪ duration so nothing exceeds it.
	deleted, err = PruneByAge(context.Background(), store, key, 24*time.Hour)
	if err != nil {
		t.Fatalf("PruneByAge: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected no deletes when nothing exceeds cutoff; got %d", deleted)
	}
	after, _ := store.List(context.Background(), key)
	if len(after) != 3 {
		t.Errorf("nothing should have been pruned; have %d, want 3", len(after))
	}
}

func TestPruneByAgeRetainsNewestWhenAllAged(t *testing.T) {
	// All seeded versions exceed the (tiny) cutoff. The newest must remain.
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 4)

	// Sleep briefly so all CreatedAt timestamps precede our cutoff.
	time.Sleep(15 * time.Millisecond)

	deleted, err := PruneByAge(context.Background(), store, key, time.Millisecond)
	if err != nil {
		t.Fatalf("PruneByAge: %v", err)
	}
	if deleted != 3 {
		t.Errorf("expected 3 deletions (4 seeded, newest kept); got %d", deleted)
	}
	after, _ := store.List(context.Background(), key)
	if len(after) != 1 {
		t.Errorf("expected 1 survivor (newest); got %d", len(after))
	}
}

func TestPruneByAgeNoOpOnSingleVersion(t *testing.T) {
	// Single version → never deleted regardless of age.
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 1)
	time.Sleep(15 * time.Millisecond)

	deleted, err := PruneByAge(context.Background(), store, key, time.Millisecond)
	if err != nil {
		t.Fatalf("PruneByAge: %v", err)
	}
	if deleted != 0 {
		t.Errorf("single-version store should never be pruned; got %d", deleted)
	}
}

func TestPruneByAgeZeroIsNoOp(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 3)

	deleted, err := PruneByAge(context.Background(), store, key, 0)
	if err != nil {
		t.Fatalf("PruneByAge(0): %v", err)
	}
	if deleted != 0 {
		t.Errorf("zero duration should disable; deleted %d", deleted)
	}
}

func TestPruneOldVersionsThroughCompressionDecorator(t *testing.T) {
	// PruneOldVersions delegates to store.List + store.Delete, so it should
	// work transparently through the CompressingStore decorator (pass-through
	// methods).
	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	store, err := NewCompressingStore(base, CompressZstd)
	if err != nil {
		t.Fatalf("NewCompressingStore: %v", err)
	}
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	seedVersions(t, store, key, 6)

	deleted, err := PruneOldVersions(context.Background(), store, key, 2)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if deleted != 4 {
		t.Errorf("deleted = %d, want 4", deleted)
	}

	after, err := store.List(context.Background(), key)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(after) != 2 {
		t.Errorf("post-prune count = %d, want 2", len(after))
	}
}
