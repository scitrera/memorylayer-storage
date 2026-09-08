// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// registerTestStack builds a Gateway whose casstore, dedup store, and ref store
// are all returned so a test can (1) seed chunks via a source Put, (2) read their
// ordered chunk list, and (3) RegisterRef them under a new ref with the dedup store
// wired as the ChunkValidator (mirroring production, where pgindex backs both).
func registerTestStack(t *testing.T) (*Gateway, *snapshot.ChunkedStore, *snapshot.MemoryDedupStore, blobstore.Storage) {
	t.Helper()
	ctx := context.Background()
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	dstore := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024,
		DedupDomain:     testDomain,
		Index:           snapshot.NewGlobalIndex(dstore, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	gw := New(cs, NewMemoryRefStore(), NewMemoryStagingStore(), testDomain, WithChunkValidator(dstore))
	return gw, cs, dstore, chunks
}

// TestRegisterRef_NoNewBytesReadBackEqualsOriginal is the C1 acceptance test: a
// source ref's chunks are re-registered under a new ref with ZERO new pack blobs,
// and the new ref reads back byte-identical to the original.
func TestRegisterRef_NoNewBytesReadBackEqualsOriginal(t *testing.T) {
	gw, cs, _, chunks := registerTestStack(t)
	ctx := context.Background()

	payload := randomBytes(t, 3*1024*1024) // multi-chunk
	src, err := gw.Put(ctx, "src-object", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put(src): %v", err)
	}
	blobsBefore := countDomainBlobs(t, chunks)
	if blobsBefore == 0 {
		t.Fatalf("expected domain blobs after Put, got 0")
	}

	// Read the source object's ordered chunk list (metadata only) — this is what the
	// bridge coordinator hands to RegisterRef.
	locs, err := cs.ManifestChunks(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "src-object"})
	if err != nil {
		t.Fatalf("ManifestChunks: %v", err)
	}

	info, err := gw.RegisterRef(ctx, "bridged-ref", locs, src.ContentHash, src.Size, "image/png", map[string]string{"origin": "mlfs"})
	if err != nil {
		t.Fatalf("RegisterRef: %v", err)
	}
	if info.ContentHash != src.ContentHash {
		t.Fatalf("content hash: got %q want %q", info.ContentHash, src.ContentHash)
	}
	if info.Size != src.Size {
		t.Fatalf("size: got %d want %d", info.Size, src.Size)
	}

	// INVARIANT: RegisterRef wrote no new pack/chunk blob.
	if got := countDomainBlobs(t, chunks); got != blobsBefore {
		t.Fatalf("RegisterRef created new blobs: before=%d after=%d (must be metadata-only)", blobsBefore, got)
	}

	// The bridged ref reads back byte-identical to the original object.
	rc, got, err := gw.Get(ctx, "bridged-ref")
	if err != nil {
		t.Fatalf("Get(bridged-ref): %v", err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("bridged ref bytes differ from original (got %d want %d)", len(body), len(payload))
	}
	if got.ContentType != "image/png" {
		t.Fatalf("content type: got %q", got.ContentType)
	}
	if got.UserMeta["origin"] != "mlfs" {
		t.Fatalf("user meta not round-tripped: %+v", got.UserMeta)
	}
	if got.ContentHash != src.ContentHash {
		t.Fatalf("Get content hash: got %q want %q", got.ContentHash, src.ContentHash)
	}
}

// TestRegisterRef_RejectsUnknownChunk confirms the validator refuses to bind a ref
// to a chunk that does not exist in the domain's pack index.
func TestRegisterRef_RejectsUnknownChunk(t *testing.T) {
	gw, _, _, _ := registerTestStack(t)
	ctx := context.Background()
	bogus := []snapshot.ChunkLocation{{
		ChunkHash: "deadbeef",
		PackRef:   snapshot.PackRef{PackHash: "cafef00d", Offset: 0, Size: 10},
	}}
	_, err := gw.RegisterRef(ctx, "bad-ref", bogus, "deadbeef", 10, "application/octet-stream", nil)
	if err == nil {
		t.Fatalf("expected RegisterRef to reject an unknown chunk")
	}
}

// TestRegisterRef_RejectsCorruptedPackRef is the regression test for fix #1: a
// chunk hash that IS present in the domain's pack index but whose caller-supplied
// PackRef disagrees with the index's authoritative value must be rejected with
// ErrChunkPackMismatch. Without this fix, validateChunks only checked presence,
// so a valid hash paired with wrong PackHash/Offset/Size would be accepted and
// silently mint a ref that fails casstore's per-chunk hash check on first read.
func TestRegisterRef_RejectsCorruptedPackRef(t *testing.T) {
	gw, cs, _, _ := registerTestStack(t)
	ctx := context.Background()

	// Seed a real object so a real chunk exists in the pack index.
	payload := randomBytes(t, 512*1024) // single chunk
	if _, err := gw.Put(ctx, "seed-obj", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Retrieve the authoritative chunk list for the seeded object.
	locs, err := cs.ManifestChunks(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "seed-obj"})
	if err != nil {
		t.Fatalf("ManifestChunks: %v", err)
	}
	if len(locs) == 0 {
		t.Fatal("expected at least one chunk")
	}

	// Corrupt the PackRef of the first chunk: the hash is valid (present in the
	// index) but the PackHash/Offset/Size are wrong.
	corrupted := make([]snapshot.ChunkLocation, len(locs))
	copy(corrupted, locs)
	corrupted[0].PackRef = snapshot.PackRef{
		PackHash: "000000000000000000000000000000000000000000000000000000000000dead",
		Offset:   9999,
		Size:     locs[0].PackRef.Size,
	}

	_, err = gw.RegisterRef(ctx, "corrupt-ref", corrupted, "somehash", int64(len(payload)), "application/octet-stream", nil)
	if err == nil {
		t.Fatal("expected RegisterRef to reject a chunk with a corrupted PackRef, but it succeeded")
	}
	if !errors.Is(err, ErrChunkPackMismatch) {
		t.Fatalf("expected ErrChunkPackMismatch, got: %v", err)
	}
}

// TestRegisterRef_UsesAuthoritativePackRef confirms that validateAndNormaliseChunks
// overwrites the caller-supplied PackRef with the index's authoritative value when
// they match (the normalisation path). This is a belt-and-suspenders check: in
// practice the bridge always sources locs from ManifestChunks (so they already
// match), but the overwrite makes the register path safe even if a caller constructs
// ChunkLocations by hand with the correct hash but re-derives the PackRef.
func TestRegisterRef_UsesAuthoritativePackRef(t *testing.T) {
	gw, cs, dstore, _ := registerTestStack(t)
	ctx := context.Background()

	payload := randomBytes(t, 512*1024)
	src, err := gw.Put(ctx, "norm-src", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	locs, err := cs.ManifestChunks(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "norm-src"})
	if err != nil || len(locs) == 0 {
		t.Fatalf("ManifestChunks: %v (locs=%d)", err, len(locs))
	}

	// Confirm the authoritative PackRef from the index matches locs (i.e. locs ARE
	// already authoritative — sourced from the manifest that was written by Put).
	hashes := make([]string, len(locs))
	for i, l := range locs {
		hashes[i] = l.ChunkHash
	}
	found, err := dstore.LookupBatch(ctx, testDomain, hashes)
	if err != nil {
		t.Fatalf("LookupBatch: %v", err)
	}
	for _, l := range locs {
		auth, ok := found[l.ChunkHash]
		if !ok {
			t.Fatalf("chunk %s not in dedup index", l.ChunkHash)
		}
		if l.PackRef != auth {
			t.Fatalf("ManifestChunks PackRef != index authoritative: %+v vs %+v", l.PackRef, auth)
		}
	}

	// RegisterRef with the exact authoritative locs must succeed and be readable.
	info, err := gw.RegisterRef(ctx, "norm-ref", locs, src.ContentHash, src.Size, "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("RegisterRef with authoritative locs: %v", err)
	}
	if info.ContentHash != src.ContentHash {
		t.Fatalf("content hash: got %q want %q", info.ContentHash, src.ContentHash)
	}
}

// failingRefStore wraps a RefStore and makes Put always return an error, so tests
// can simulate a refs.Put failure after the manifest has already been written.
type failingRefStore struct {
	RefStore
}

func (f failingRefStore) Put(_ context.Context, _ ObjectInfo) error {
	return errors.New("injected ref store failure")
}

// TestRegisterRef_RefStoreFail_RollsBackManifest is the regression test for fix #2b:
// when refs.Put fails after RegisterManifest succeeds, the manifest version must be
// rolled back so a retry does not accumulate orphaned manifests that pin pack chunks.
// Without this fix, each failed RegisterRef call left a pinned manifest version; GC
// would never reclaim those packs, growing without bound.
func TestRegisterRef_RefStoreFail_RollsBackManifest(t *testing.T) {
	ctx := context.Background()

	// Build a stack with a working casstore but a ref store that always fails Put.
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	dstore := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024,
		DedupDomain:     testDomain,
		Index:           snapshot.NewGlobalIndex(dstore, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	// Use a working ref store for the seed Put, then swap in the failing one.
	workingRefs := NewMemoryRefStore()
	gwSeed := New(cs, workingRefs, NewMemoryStagingStore(), testDomain, WithChunkValidator(dstore))

	payload := randomBytes(t, 512*1024)
	if _, err := gwSeed.Put(ctx, "seed", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	locs, err := cs.ManifestChunks(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "seed"})
	if err != nil || len(locs) == 0 {
		t.Fatalf("ManifestChunks: %v (locs=%d)", err, len(locs))
	}

	// Count manifest versions for "fail-ref" before the failing call.
	manifestsBefore, err := cs.List(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "fail-ref"})
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Fatalf("List before: %v", err)
	}
	versionsBefore := len(manifestsBefore)

	// Now use a gateway with a failing ref store.
	gwFail := New(cs, failingRefStore{workingRefs}, NewMemoryStagingStore(), testDomain, WithChunkValidator(dstore))
	_, err = gwFail.RegisterRef(ctx, "fail-ref", locs, "somehash", int64(len(payload)), "application/octet-stream", nil)
	if err == nil {
		t.Fatal("expected RegisterRef to fail (injected ref store error)")
	}

	// The manifest must have been rolled back: no new version under "fail-ref".
	manifestsAfter, err := cs.List(ctx, snapshot.SnapshotKey{Tenant: testDomain, OwnerKey: "fail-ref"})
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Fatalf("List after: %v", err)
	}
	if len(manifestsAfter) != versionsBefore {
		t.Fatalf("manifest rollback failed: had %d versions before, have %d after (want %d)", versionsBefore, len(manifestsAfter), versionsBefore)
	}
}
