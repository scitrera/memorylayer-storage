// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// globalDedupStack builds a ChunkedStore + shared MemoryDedupStore wired for
// global dedup in `domain`, plus a ChunkedGC that honors that index. It
// returns everything tests need to drive Put/Delete/GC and inspect state.
func globalDedupStack(t *testing.T, domain string) (*ChunkedStore, *ChunkedGC, SnapshotStore, blobstore.Storage, *MemoryDedupStore) {
	t.Helper()
	store := NewMemoryDedupStore()
	cs, upstream, chunks := chunkedTestStackWithConfig(t, ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024, // 1 MiB → several packs per multi-MiB object
		DedupDomain:     domain,
		Index:           NewGlobalIndex(store, nil),
	})
	gc := NewChunkedGC(upstream, chunks, nil)
	gc.Index = store // honor the global index: purge entries for reclaimed packs
	return cs, gc, upstream, chunks, store
}

// TestChunkedGC_GlobalDedup_PackLiveUntilLastManifest is the core L0.3
// acceptance: a pack shared by several distinct objects is collected only
// after the LAST referencing manifest is deleted, and once collected its
// dedup-index entries are purged so a later identical object re-packs cleanly
// (never dangles against deleted bytes).
func TestChunkedGC_GlobalDedup_PackLiveUntilLastManifest(t *testing.T) {
	ctx := context.Background()
	cs, gc, _, chunks, store := globalDedupStack(t, "ent")

	payload := randomPayload(t, 4*1024*1024)
	keyA := SnapshotKey{Tenant: "wsA", OwnerKey: "A"}
	keyB := SnapshotKey{Tenant: "wsB", OwnerKey: "B"} // distinct object, same domain

	storedA, err := cs.Put(ctx, keyA, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put A: %v", err)
	}
	storedB, err := cs.Put(ctx, keyB, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put B: %v", err)
	}

	blobs, _ := countTenantBlobs(ctx, chunks, "ent")
	indexRows := store.Len("ent")
	if blobs == 0 || indexRows == 0 {
		t.Fatalf("expected shared blobs+index rows after two identical objects; blobs=%d rows=%d", blobs, indexRows)
	}

	// Delete A. B still references every shared pack → GC reclaims nothing.
	if err := cs.Delete(ctx, keyA, storedA.Version); err != nil {
		t.Fatalf("Delete A: %v", err)
	}
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC after delete A: %v", err)
	}
	if res.ChunksReclaimed != 0 {
		t.Errorf("pack must stay live while B references it; reclaimed %d", res.ChunksReclaimed)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != blobs {
		t.Errorf("blob count changed while B alive: %d → %d", blobs, got)
	}
	if got := store.Len("ent"); got != indexRows {
		t.Errorf("index rows changed while B alive: %d → %d", indexRows, got)
	}

	// B still restores byte-exact.
	if !restores(t, cs, keyB, payload) {
		t.Errorf("B corrupted after GC while still referenced")
	}

	// Delete B (the last referencer). Now GC reclaims everything AND purges
	// the now-dangling dedup-index entries.
	if err := cs.Delete(ctx, keyB, storedB.Version); err != nil {
		t.Fatalf("Delete B: %v", err)
	}
	res, err = gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC after delete B: %v", err)
	}
	if res.ChunksReclaimed != blobs {
		t.Errorf("expected all %d packs reclaimed after last manifest gone, got %d", blobs, res.ChunksReclaimed)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != 0 {
		t.Errorf("expected 0 blobs after last manifest gone, got %d", got)
	}
	if got := store.Len("ent"); got != 0 {
		t.Errorf("expected dedup index purged after packs reclaimed, %d rows remain", got)
	}

	// The proof the purge mattered: a fresh identical object must restore. If
	// the stale index entries had survived, C's manifest would point at the
	// deleted packs and this round-trip would fail.
	keyC := SnapshotKey{Tenant: "wsC", OwnerKey: "C"}
	if _, err := cs.Put(ctx, keyC, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put C after GC: %v", err)
	}
	if !restores(t, cs, keyC, payload) {
		t.Errorf("C unrestorable after GC — stale dedup index handed out refs to collected packs")
	}
	// C had to re-pack from scratch (index was purged), so blobs are back.
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got == 0 {
		t.Errorf("expected C to re-pack fresh blobs after index purge")
	}
}

// TestChunkedGC_RunOnceForDomain_ScopesToDomain verifies a domain-scoped pass
// reclaims only its own domain's blobs+index entries and never touches another
// domain's data, even when both share one physical chunk store and one index.
func TestChunkedGC_RunOnceForDomain_ScopesToDomain(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryDedupStore()
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	upstream, err := NewLocalStore(t.TempDir()) // shared manifest store across domains
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	newCS := func(domain string) *ChunkedStore {
		cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
			PackTargetBytes: 1 * 1024 * 1024,
			DedupDomain:     domain,
			Index:           NewGlobalIndex(store, nil),
		})
		if err != nil {
			t.Fatalf("NewChunkedStore %s: %v", domain, err)
		}
		return cs
	}

	payload := randomPayload(t, 3*1024*1024)
	csA, csB := newCS("domA"), newCS("domB")
	keyA := SnapshotKey{Tenant: "x", OwnerKey: "a"}
	keyB := SnapshotKey{Tenant: "y", OwnerKey: "b"}

	storedA, err := csA.Put(ctx, keyA, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if _, err := csB.Put(ctx, keyB, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	bBlobsBefore, _ := countTenantBlobs(ctx, chunks, "domB")
	bRowsBefore := store.Len("domB")

	// Delete domA's manifest, then run a GC scoped to domA only.
	if err := csA.Delete(ctx, keyA, storedA.Version); err != nil {
		t.Fatalf("Delete A: %v", err)
	}
	gc := NewChunkedGC(upstream, chunks, nil)
	gc.Index = store
	res, err := gc.RunOnceForDomain(ctx, "domA")
	if err != nil {
		t.Fatalf("RunOnceForDomain(domA): %v", err)
	}

	// domA fully reclaimed + purged.
	if got, _ := countTenantBlobs(ctx, chunks, "domA"); got != 0 {
		t.Errorf("domA blobs should be reclaimed, %d remain", got)
	}
	if got := store.Len("domA"); got != 0 {
		t.Errorf("domA index rows should be purged, %d remain", got)
	}
	if res.ManifestsScanned != 0 {
		t.Errorf("domA had no surviving manifests; scanned=%d (must ignore domB's)", res.ManifestsScanned)
	}

	// domB untouched.
	if got, _ := countTenantBlobs(ctx, chunks, "domB"); got != bBlobsBefore {
		t.Errorf("domB blobs touched by domA-scoped GC: %d → %d", bBlobsBefore, got)
	}
	if got := store.Len("domB"); got != bRowsBefore {
		t.Errorf("domB index rows touched by domA-scoped GC: %d → %d", bRowsBefore, got)
	}
	if !restores(t, csB, keyB, payload) {
		t.Errorf("domB object corrupted by domA-scoped GC")
	}
}

// tsSkewStore wraps a blobstore.Storage and reports a different Timestamp
// from GetMetadata than ListBlobs surfaced, simulating a concurrent writer
// touching a blob between the GC mark and sweep phases. It overrides only
// GetMetadata; every other method delegates to the embedded store.
type tsSkewStore struct {
	blobstore.Storage
	skew time.Duration
}

func (s tsSkewStore) GetMetadata(ctx context.Context, id blobstore.ID) (blobstore.Metadata, error) {
	md, err := s.Storage.GetMetadata(ctx, id)
	if err != nil {
		return md, err
	}
	md.Timestamp = md.Timestamp.Add(s.skew)
	return md, nil
}

// TestChunkedGC_DefersConcurrentlyRewrittenPack verifies the timestamp-tag
// guard: an orphan whose timestamp changed between mark and sweep (a stand-in
// for a concurrent writer touching it) is deferred, not deleted.
func TestChunkedGC_DefersConcurrentlyRewrittenPack(t *testing.T) {
	ctx := context.Background()
	cs, _, upstream, chunks, _ := globalDedupStack(t, "ent")

	payload := randomPayload(t, 3*1024*1024)
	key := SnapshotKey{Tenant: "wsA", OwnerKey: "deferred"}
	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	blobs, _ := countTenantBlobs(ctx, chunks, "ent")
	if blobs == 0 {
		t.Fatal("expected blobs")
	}

	// Orphan everything, then GC through a store that skews sweep-time
	// timestamps so the guard treats every blob as concurrently touched.
	if err := cs.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	gc := NewChunkedGC(upstream, tsSkewStore{Storage: chunks, skew: time.Second}, nil)
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.ChunksReclaimed != 0 {
		t.Errorf("concurrently-touched blobs must be deferred, not deleted; reclaimed %d", res.ChunksReclaimed)
	}
	if res.ChunksSkippedRecent != blobs {
		t.Errorf("expected %d blobs deferred via timestamp guard, got %d", blobs, res.ChunksSkippedRecent)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != blobs {
		t.Errorf("deferred blobs must remain on disk; %d → %d", blobs, got)
	}

	// On a clean pass (no skew) the orphans are reclaimed as normal.
	gc2 := NewChunkedGC(upstream, chunks, nil)
	if _, err := gc2.RunOnce(ctx); err != nil {
		t.Fatalf("GC clean pass: %v", err)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != 0 {
		t.Errorf("clean pass should reclaim deferred orphans; %d remain", got)
	}
}

// TestChunkedGC_SafetyWindowDefersYoung verifies the safety window: a freshly
// orphaned pack younger than the window is deferred (not reclaimed), then
// collected once the clock advances past the window. This closes the
// upload-pack-then-commit-manifest race.
func TestChunkedGC_SafetyWindowDefersYoung(t *testing.T) {
	ctx := context.Background()
	cs, gc, _, chunks, _ := globalDedupStack(t, "ent")
	gc.SafetyWindow = time.Hour

	payload := randomPayload(t, 3*1024*1024)
	key := SnapshotKey{Tenant: "wsA", OwnerKey: "young"}
	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	blobs, _ := countTenantBlobs(ctx, chunks, "ent")
	if err := cs.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Within the window → deferred, not reclaimed.
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC young: %v", err)
	}
	if res.ChunksReclaimed != 0 || res.ChunksDeferredYoung == 0 {
		t.Errorf("young orphan should be deferred: reclaimed=%d deferred=%d", res.ChunksReclaimed, res.ChunksDeferredYoung)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != blobs {
		t.Errorf("young blobs must remain: %d → %d", blobs, got)
	}

	// Advance the clock past the window → reclaimed.
	gc.SetClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
	res, err = gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC aged: %v", err)
	}
	if res.ChunksReclaimed != blobs {
		t.Errorf("aged orphan should be reclaimed: got %d want %d", res.ChunksReclaimed, blobs)
	}
	if got, _ := countTenantBlobs(ctx, chunks, "ent"); got != 0 {
		t.Errorf("aged blobs should be gone, %d remain", got)
	}
}

// restores reports whether GetLatest(key) returns exactly want.
func restores(t *testing.T, cs *ChunkedStore, key SnapshotKey, want []byte) bool {
	t.Helper()
	rc, _, err := cs.GetLatest(context.Background(), key)
	if err != nil {
		t.Logf("restores: GetLatest %v: %v", key, err)
		return false
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Logf("restores: ReadAll %v: %v", key, err)
		return false
	}
	return bytes.Equal(got, want)
}
