// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// chunkedTestStackWithConfig builds a ChunkedStore over fresh per-test
// tempdir-backed local stores using the supplied config. It returns the
// store plus its upstream + chunk blobstore so tests can inspect them.
func chunkedTestStackWithConfig(t *testing.T, cfg ChunkedConfig) (*ChunkedStore, SnapshotStore, blobstore.Storage) {
	t.Helper()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(context.Background()) })
	cs, err := NewChunkedStore(upstream, chunks, cfg)
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, upstream, chunks
}

// randomPayload returns n bytes of crypto-random data. The same slice is
// reused across Puts in a test to simulate byte-identical objects.
func randomPayload(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// TestGlobalIndex_DedupsAcrossDistinctObjects is the load-bearing L0.2 test:
// N distinct objects (different keys, no shared version history) holding
// identical content must store their shared chunks exactly once. This is the
// enterprise case the default PriorManifestIndex cannot serve.
func TestGlobalIndex_DedupsAcrossDistinctObjects(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDedupStore()
	cs, _, chunks := chunkedTestStackWithConfig(t, ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024, // 1 MiB → multiple packs for a 5 MiB payload
		DedupDomain:     "ent",
		Index:           NewGlobalIndex(store, nil),
	})

	payload := randomPayload(t, 5*1024*1024) // identical content, distinct objects

	// First object populates the domain.
	if _, err := cs.Put(ctx, SnapshotKey{Tenant: "wsA", OwnerKey: "obj-1"}, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put obj-1: %v", err)
	}
	afterFirst, err := countTenantBlobs(ctx, chunks, "ent")
	if err != nil {
		t.Fatalf("countTenantBlobs: %v", err)
	}
	if afterFirst == 0 {
		t.Fatalf("expected blobs after first object, got 0")
	}
	indexRowsFirst := store.Len("ent")
	if indexRowsFirst == 0 {
		t.Fatalf("expected dedup-index rows after first object, got 0")
	}

	// Nine more distinct objects with identical content. None should add blobs
	// or index rows — every chunk is reused from the domain.
	for i := 2; i <= 10; i++ {
		key := SnapshotKey{Tenant: "wsA", OwnerKey: "obj-" + string(rune('0'+i))}
		if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put %v: %v", key, err)
		}
	}

	afterAll, err := countTenantBlobs(ctx, chunks, "ent")
	if err != nil {
		t.Fatalf("countTenantBlobs #2: %v", err)
	}
	if afterAll != afterFirst {
		t.Errorf("global dedup failed: blob count grew from %d to %d across identical objects", afterFirst, afterAll)
	}
	if got := store.Len("ent"); got != indexRowsFirst {
		t.Errorf("index rows grew from %d to %d across identical objects", indexRowsFirst, got)
	}
}

// TestGlobalIndex_RoundTripThroughReusedPacks verifies that an object whose
// chunks were all deduped (its manifest points into a *different* object's
// packs) still restores byte-exact. This is the correctness guarantee that
// makes cross-object dedup safe.
func TestGlobalIndex_RoundTripThroughReusedPacks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDedupStore()
	cs, _, _ := chunkedTestStackWithConfig(t, ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024,
		DedupDomain:     "ent",
		Index:           NewGlobalIndex(store, nil),
	})

	payload := randomPayload(t, 5*1024*1024)
	keyA := SnapshotKey{Tenant: "wsA", OwnerKey: "writer"}
	keyB := SnapshotKey{Tenant: "wsB", OwnerKey: "reader"} // different tenant, same domain

	if _, err := cs.Put(ctx, keyA, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if _, err := cs.Put(ctx, keyB, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	// B's chunks were all reused from A's packs. Restore must be byte-exact.
	rc, _, err := cs.GetLatest(ctx, keyB)
	if err != nil {
		t.Fatalf("GetLatest B: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip through reused packs mismatch: got %d bytes want %d (first diff @ %d)",
			len(got), len(payload), firstDiffOffset(got, payload))
	}
}

// TestGlobalIndex_CrossDomainNeverDedups asserts the isolation boundary: two
// domains writing identical content share no blobs and no index rows.
func TestGlobalIndex_CrossDomainNeverDedups(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDedupStore()

	// Shared physical chunk store; two domains via two ChunkedStores.
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	newStore := func(domain string) *ChunkedStore {
		upstream, err := NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
			PackTargetBytes: 1 * 1024 * 1024,
			DedupDomain:     domain,
			Index:           NewGlobalIndex(store, nil),
		})
		if err != nil {
			t.Fatalf("NewChunkedStore: %v", err)
		}
		return cs
	}

	payload := randomPayload(t, 3*1024*1024)
	csA := newStore("domainA")
	csB := newStore("domainB")

	if _, err := csA.Put(ctx, SnapshotKey{Tenant: "x", OwnerKey: "o"}, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if _, err := csB.Put(ctx, SnapshotKey{Tenant: "x", OwnerKey: "o"}, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	aBlobs, _ := countTenantBlobs(ctx, chunks, "domainA")
	bBlobs, _ := countTenantBlobs(ctx, chunks, "domainB")
	if aBlobs == 0 || bBlobs == 0 {
		t.Fatalf("each domain must have its own blobs; A=%d B=%d", aBlobs, bBlobs)
	}
	if aBlobs != bBlobs {
		t.Errorf("identical content should yield equal per-domain blob counts; A=%d B=%d", aBlobs, bBlobs)
	}
	// Index rows are domain-scoped and identical in count, never shared.
	if store.Len("domainA") == 0 || store.Len("domainB") == 0 {
		t.Errorf("each domain must have its own index rows; A=%d B=%d", store.Len("domainA"), store.Len("domainB"))
	}
}

// TestDedupDomain_KnobEnablesCrossTenantDedup proves the DedupDomain knob is
// what unlocks cross-tenant dedup: identical content under two different
// tenants dedups when both map to one domain, but NOT under the default
// (per-tenant) configuration.
func TestDedupDomain_KnobEnablesCrossTenantDedup(t *testing.T) {
	ctx := context.Background()
	payload := randomPayload(t, 4*1024*1024)

	put := func(cs *ChunkedStore, tenant string) {
		if _, err := cs.Put(ctx, SnapshotKey{Tenant: tenant, OwnerKey: "o"}, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put %s: %v", tenant, err)
		}
	}

	// Default (per-tenant) global index: alice and bob get separate prefixes,
	// so no cross-tenant dedup — total blobs ~= 2x one tenant's.
	t.Run("per-tenant default does not cross tenants", func(t *testing.T) {
		store := NewMemoryDedupStore()
		cs, _, chunks := chunkedTestStackWithConfig(t, ChunkedConfig{
			PackTargetBytes: 1 * 1024 * 1024,
			Index:           NewGlobalIndex(store, nil),
			// DedupDomain empty → domain = key.Tenant
		})
		put(cs, "alice")
		alice, _ := countTenantBlobs(ctx, chunks, "alice")
		put(cs, "bob")
		bob, _ := countTenantBlobs(ctx, chunks, "bob")
		if alice == 0 || bob == 0 {
			t.Fatalf("expected per-tenant blobs; alice=%d bob=%d", alice, bob)
		}
		if store.Len("alice") == 0 || store.Len("bob") == 0 {
			t.Errorf("expected separate per-tenant index domains")
		}
	})

	// Explicit instance domain: alice and bob share domain "ent", so bob's
	// identical content adds zero blobs.
	t.Run("shared domain dedups across tenants", func(t *testing.T) {
		store := NewMemoryDedupStore()
		cs, _, chunks := chunkedTestStackWithConfig(t, ChunkedConfig{
			PackTargetBytes: 1 * 1024 * 1024,
			DedupDomain:     "ent",
			Index:           NewGlobalIndex(store, nil),
		})
		put(cs, "alice")
		afterAlice, _ := countTenantBlobs(ctx, chunks, "ent")
		put(cs, "bob")
		afterBob, _ := countTenantBlobs(ctx, chunks, "ent")
		if afterAlice == 0 {
			t.Fatalf("expected blobs in shared domain")
		}
		if afterBob != afterAlice {
			t.Errorf("cross-tenant dedup failed in shared domain: %d → %d blobs", afterAlice, afterBob)
		}
	})
}

// TestMemoryDedupStore_RecordIdempotent verifies first-writer-wins semantics:
// re-recording a chunk hash does not overwrite or duplicate it.
func TestMemoryDedupStore_RecordIdempotent(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryDedupStore()

	first := ChunkLocation{ChunkHash: "h1", PackRef: PackRef{PackHash: "pA", Offset: 0, Size: 10}}
	if err := s.Record(ctx, "d", []ChunkLocation{first}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	// Attempt to overwrite with a different location for the same hash.
	if err := s.Record(ctx, "d", []ChunkLocation{{ChunkHash: "h1", PackRef: PackRef{PackHash: "pB", Offset: 99, Size: 10}}}); err != nil {
		t.Fatalf("Record #2: %v", err)
	}
	if s.Len("d") != 1 {
		t.Errorf("expected 1 row after idempotent re-record, got %d", s.Len("d"))
	}
	got, ok, err := s.Lookup(ctx, "d", "h1")
	if err != nil || !ok {
		t.Fatalf("Lookup: ok=%v err=%v", ok, err)
	}
	if got != first.PackRef {
		t.Errorf("first writer should win: got %+v want %+v", got, first.PackRef)
	}

	// Batch lookup returns only present hashes.
	batch, err := s.LookupBatch(ctx, "d", []string{"h1", "missing"})
	if err != nil {
		t.Fatalf("LookupBatch: %v", err)
	}
	if len(batch) != 1 || batch["h1"] != first.PackRef {
		t.Errorf("LookupBatch unexpected result: %+v", batch)
	}
}

// TestGlobalIndex_PartialMidStreamDedupPreservesOrder is a regression test for
// a silent data-corruption bug in putV2's manifest assembly.
//
// Bug: deduped chunks were appended to the manifest immediately inside
// emitChunk, while new (packed) chunks were appended later, in a batch, when
// their pack was flushed. A chunk that deduped in the MIDDLE of the stream
// (preceded and followed by new chunks still buffered in the pack) was therefore
// recorded in the manifest BEFORE those new chunks even though it came after
// them in the byte stream. Restore concatenates chunks in manifest order, so the
// reconstructed object was silently reordered — correct length, no error, wrong
// bytes.
//
// Unlike the existing dedup tests, which either dedup an object's chunks ENTIRELY
// (every chunk reused, order trivially preserved) or not at all, this test
// writes a SUB-RANGE of a prior object so that only the interior chunks dedup
// while the head/tail chunks are new — the exact interleaving the bug reorders.
// Random content is required so content-defined chunk boundaries of the
// sub-range align with the parent's; iterate to make the alignment reliable.
func TestGlobalIndex_PartialMidStreamDedupPreservesOrder(t *testing.T) {
	ctx := context.Background()
	for iter := 0; iter < 8; iter++ {
		store := NewMemoryDedupStore()
		cs, _, _ := chunkedTestStackWithConfig(t, ChunkedConfig{
			// Use the default ~16 MiB pack target so the whole 8 MiB parent and
			// the 3 MiB sub-range each pack into a single pack — this is what
			// keeps new chunks buffered (unflushed) while an interior chunk
			// dedups, reproducing the ordering bug.
			DedupDomain: "ent",
			Index:       NewGlobalIndex(store, nil),
		})

		parent := randomPayload(t, 8*1024*1024)
		if _, err := cs.Put(ctx, SnapshotKey{Tenant: "x", OwnerKey: "parent"}, SnapshotMetadata{}, bytes.NewReader(parent)); err != nil {
			t.Fatalf("Put parent: %v", err)
		}
		sub := parent[1*1024*1024 : 4*1024*1024] // 3 MiB sub-range starting mid-chunk
		if _, err := cs.Put(ctx, SnapshotKey{Tenant: "x", OwnerKey: "sub"}, SnapshotMetadata{}, bytes.NewReader(sub)); err != nil {
			t.Fatalf("Put sub: %v", err)
		}

		rc, _, err := cs.GetLatest(ctx, SnapshotKey{Tenant: "x", OwnerKey: "sub"})
		if err != nil {
			t.Fatalf("GetLatest sub: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("ReadAll sub: %v", err)
		}
		if !bytes.Equal(got, sub) {
			t.Fatalf("iter %d: sub-range round-trip corrupted: got %d bytes want %d, first diff @ %d",
				iter, len(got), len(sub), firstDiffOffset(got, sub))
		}
	}
}
