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

// countBlobs returns the number of blobs currently in the store (across all
// prefixes). Used to assert RegisterManifest writes ZERO new pack/chunk blobs.
func countBlobs(t *testing.T, chunks blobstore.Storage) int {
	t.Helper()
	all, err := blobstore.ListAllBlobs(context.Background(), chunks, "")
	if err != nil {
		t.Fatalf("ListAllBlobs: %v", err)
	}
	return len(all)
}

// TestRegisterManifest_RoundTripNoNewBlobs is the core zero-byte-movement proof:
// Put an object, read its ordered chunk list back with ManifestChunks, then
// RegisterManifest those chunks under a NEW key. The new key must read back the
// IDENTICAL bytes, reference the SAME chunk hashes, and NOT create any new pack
// blob (pure metadata synthesis over the shared pack set).
func TestRegisterManifest_RoundTripNoNewBlobs(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t)
	ctx := context.Background()

	// A domain shared by both keys (mlfs mount domain == blobgw ref domain).
	const domain = "shared-domain"
	src := SnapshotKey{Tenant: domain, OwnerKey: "src"}
	dst := SnapshotKey{Tenant: domain, OwnerKey: "dst"}

	payload := make([]byte, 5*1024*1024) // multi-chunk
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	if _, err := cs.Put(ctx, src, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	blobsAfterPut := countBlobs(t, chunks)
	if blobsAfterPut == 0 {
		t.Fatalf("expected pack blobs after Put, got 0")
	}

	// Read the ordered chunk list (metadata only).
	locs, err := cs.ManifestChunks(ctx, src)
	if err != nil {
		t.Fatalf("ManifestChunks: %v", err)
	}
	if len(locs) == 0 {
		t.Fatalf("expected chunks, got 0")
	}

	// Register a manifest for a new key over the SAME chunks.
	if _, err := cs.RegisterManifest(ctx, dst, SnapshotMetadata{}, locs); err != nil {
		t.Fatalf("RegisterManifest: %v", err)
	}

	// INVARIANT: no new pack/chunk blob was written.
	if got := countBlobs(t, chunks); got != blobsAfterPut {
		t.Fatalf("RegisterManifest created new blobs: before=%d after=%d (must be metadata-only)", blobsAfterPut, got)
	}

	// The new key reads back the byte-identical payload.
	rc, _, err := cs.GetLatest(ctx, dst)
	if err != nil {
		t.Fatalf("GetLatest(dst): %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll(dst): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("dst bytes differ from source (len got=%d want=%d)", len(got), len(payload))
	}

	// The dst manifest references the SAME chunk hashes as the source, in order.
	dstLocs, err := cs.ManifestChunks(ctx, dst)
	if err != nil {
		t.Fatalf("ManifestChunks(dst): %v", err)
	}
	if len(dstLocs) != len(locs) {
		t.Fatalf("dst chunk count = %d, want %d", len(dstLocs), len(locs))
	}
	for i := range locs {
		if dstLocs[i] != locs[i] {
			t.Fatalf("chunk %d differs: dst=%+v src=%+v", i, dstLocs[i], locs[i])
		}
	}
}

// TestRegisterManifest_EmptyObject registers a zero-chunk manifest and confirms
// it reads back as empty, with no blobs written.
func TestRegisterManifest_EmptyObject(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "empty"}

	if _, err := cs.RegisterManifest(ctx, key, SnapshotMetadata{}, nil); err != nil {
		t.Fatalf("RegisterManifest(empty): %v", err)
	}
	if got := countBlobs(t, chunks); got != 0 {
		t.Fatalf("empty RegisterManifest wrote %d blobs, want 0", got)
	}
	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest(empty): %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty object read back %d bytes, want 0", len(got))
	}
}

// TestManifestChunks_RejectsV1 confirms a v1 (chunk-per-blob) manifest cannot be
// bridged (its chunks are not pack-referenced).
func TestManifestChunks_RejectsV1(t *testing.T) {
	cs, _, _ := chunkedTestStackWithPackSize(t, packingDisabled)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "v1"}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader([]byte("hello world"))); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := cs.ManifestChunks(ctx, key); err == nil {
		t.Fatalf("expected ManifestChunks to reject a v1 manifest")
	}
}

// TestRegisterManifest_RejectsZeroSizeChunk is the regression test for fix #5: a
// zero-size chunk location must be rejected by RegisterManifest. casstore never
// emits zero-size chunks, so a zero Size indicates a caller bug; allowing it would
// create boundary-attribution ambiguity in chunksForWindow (bridgefs.go) where a
// chunk with Size==0 has no clear window assignment.
func TestRegisterManifest_RejectsZeroSizeChunk(t *testing.T) {
	cs, _, _ := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "zero-size"}

	bad := []ChunkLocation{{
		ChunkHash: "abc123",
		PackRef:   PackRef{PackHash: "pack001", Offset: 0, Size: 0}, // zero size
	}}
	_, err := cs.RegisterManifest(ctx, key, SnapshotMetadata{}, bad)
	if err == nil {
		t.Fatal("expected RegisterManifest to reject a zero-size chunk, but it succeeded")
	}
}

// TestRegisterManifest_RejectsNegativeSizeChunk confirms negative size is also rejected.
func TestRegisterManifest_RejectsNegativeSizeChunk(t *testing.T) {
	cs, _, _ := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "neg-size"}

	bad := []ChunkLocation{{
		ChunkHash: "abc123",
		PackRef:   PackRef{PackHash: "pack001", Offset: 0, Size: -1},
	}}
	_, err := cs.RegisterManifest(ctx, key, SnapshotMetadata{}, bad)
	if err == nil {
		t.Fatal("expected RegisterManifest to reject a negative-size chunk, but it succeeded")
	}
}
