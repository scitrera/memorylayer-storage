// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// TestChunkedGC_ReclaimsOrphans is the load-bearing test for mark-and-sweep
// GC: take a snapshot, delete its manifest, run GC, all of that snapshot's
// pack/chunk blobs must be reclaimed. Validates the architect's preferred
// alternative to refcount-on-Delete.
func TestChunkedGC_ReclaimsOrphans(t *testing.T) {
	cs, upstream, chunks := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "owner-1"}

	payload := bytes.Repeat([]byte("orphan-me!"), 200_000) // 2 MB
	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// countTenantBlobs counts both chunk- and pack- blobs for the tenant.
	before, _ := countTenantBlobs(ctx, chunks, key.Tenant)
	if before == 0 {
		t.Fatal("expected at least one blob written (chunk or pack)")
	}

	// Delete the manifest. Blobs remain orphaned until GC runs.
	if err := cs.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	afterDelete, _ := countTenantBlobs(ctx, chunks, key.Tenant)
	if afterDelete != before {
		t.Fatalf("Delete should leave blobs intact; got %d → %d", before, afterDelete)
	}

	gc := NewChunkedGC(upstream, chunks, nil)
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC.RunOnce: %v", err)
	}
	if res.ChunksReclaimed != before {
		t.Errorf("expected %d blobs reclaimed, got %d (live=%d, scanned=%d)",
			before, res.ChunksReclaimed, res.LiveChunks, res.ChunksScanned)
	}

	afterGC, _ := countTenantBlobs(ctx, chunks, key.Tenant)
	if afterGC != 0 {
		t.Errorf("expected all blobs reclaimed; %d remain", afterGC)
	}
}

func TestChunkedGC_PreservesReferencedChunks(t *testing.T) {
	// Take two snapshots: one to keep, one to delete. Delete the second,
	// run GC. Only blobs unique to the deleted snapshot should be reclaimed;
	// blobs shared with the surviving snapshot must stay.
	cs, upstream, chunks := chunkedTestStack(t)
	ctx := context.Background()

	keyKeep := SnapshotKey{Tenant: "t1", OwnerKey: "keep"}
	keyKill := SnapshotKey{Tenant: "t1", OwnerKey: "kill"}

	// Identical payloads → all chunks shared. With pack grouping, both
	// snapshots produce the same pack blobs (same content → same hash).
	shared := bytes.Repeat([]byte("shared-bytes"), 100_000)
	if _, err := cs.Put(ctx, keyKeep, SnapshotMetadata{}, bytes.NewReader(shared)); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	storedKill, err := cs.Put(ctx, keyKill, SnapshotMetadata{}, bytes.NewReader(shared))
	if err != nil {
		t.Fatalf("Put kill: %v", err)
	}
	beforeBlobs, _ := countTenantBlobs(ctx, chunks, "t1")

	if err := cs.Delete(ctx, keyKill, storedKill.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	gc := NewChunkedGC(upstream, chunks, nil)
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	// All blobs are shared (identical content → same pack hashes), so none
	// should be reclaimed.
	if res.ChunksReclaimed != 0 {
		t.Errorf("shared blobs must survive; got %d reclaimed", res.ChunksReclaimed)
	}
	afterBlobs, _ := countTenantBlobs(ctx, chunks, "t1")
	if afterBlobs != beforeBlobs {
		t.Errorf("blob count changed unexpectedly; %d → %d", beforeBlobs, afterBlobs)
	}

	// The surviving snapshot must still restore byte-identical.
	rc, _, err := cs.GetLatest(ctx, keyKeep)
	if err != nil {
		t.Fatalf("GetLatest keep: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, shared) {
		t.Errorf("keep snapshot corrupted after GC (got %d bytes, want %d)", len(got), len(shared))
	}
}

func TestChunkedGC_IgnoresNonChunkedManifests(t *testing.T) {
	// A snapshot store may contain both chunked and non-chunked
	// (opaque-blob) manifests if ChunkedStore was enabled mid-flight.
	// GC must walk both kinds and ignore non-chunked ones (they don't
	// reference chunks or packs; nothing to mark).
	cs, upstream, chunks := chunkedTestStack(t)
	ctx := context.Background()

	// Chunked snapshot.
	chunkedKey := SnapshotKey{Tenant: "t1", OwnerKey: "chunked"}
	if _, err := cs.Put(ctx, chunkedKey, SnapshotMetadata{}, bytes.NewReader(bytes.Repeat([]byte("x"), 500_000))); err != nil {
		t.Fatalf("chunked Put: %v", err)
	}

	// Opaque snapshot written directly to the upstream store, bypassing
	// ChunkedStore. Mimics a snapshot from before chunked mode was enabled.
	opaqueKey := SnapshotKey{Tenant: "t1", OwnerKey: "opaque"}
	if _, err := upstream.Put(ctx, opaqueKey, SnapshotMetadata{Runtime: "runc"},
		bytes.NewReader([]byte("not a chunked manifest"))); err != nil {
		t.Fatalf("opaque Put: %v", err)
	}

	gc := NewChunkedGC(upstream, chunks, nil)
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	// One chunked manifest scanned (v2 tag); the opaque one is also walked
	// but skipped because its chunked_format tag is absent.
	if res.ManifestsScanned != 1 {
		t.Errorf("expected 1 chunked manifest scanned, got %d", res.ManifestsScanned)
	}
	if res.ChunksReclaimed != 0 {
		t.Errorf("nothing to reclaim yet; got %d", res.ChunksReclaimed)
	}
}

// TestChunkedGC_V1AndV2MixedStore verifies that a store containing both v1
// (chunk-) and v2 (pack-) blobs is GC'd correctly in a single pass.
func TestChunkedGC_V1AndV2MixedStore(t *testing.T) {
	ctx := context.Background()

	// Write one v1 snapshot (no packing) and one v2 snapshot (with packing).
	// Delete the v2 manifest, run GC — v2 pack blobs must be reclaimed;
	// v1 chunk blobs must survive.
	csV1, upstream, chunks := chunkedTestStackWithPackSize(t, packingDisabled)

	keyV1 := SnapshotKey{Tenant: "t1", OwnerKey: "v1-keep"}
	payload := bytes.Repeat([]byte("v1-data!"), 100_000)
	if _, err := csV1.Put(ctx, keyV1, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	v1Blobs, _ := countTenantChunks(ctx, chunks, "t1") // only chunk- blobs
	if v1Blobs == 0 {
		t.Fatal("expected v1 chunk blobs")
	}

	// Now write a v2 snapshot via a pack-enabled store on the SAME upstream
	// and chunks storage.
	csV2, err := NewChunkedStore(upstream, chunks, ChunkedConfig{})
	if err != nil {
		t.Fatalf("NewChunkedStore v2: %v", err)
	}
	keyV2 := SnapshotKey{Tenant: "t1", OwnerKey: "v2-kill"}
	storedV2, err := csV2.Put(ctx, keyV2, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	v2Packs, _ := countBlobsWithPrefix(ctx, chunks, packTenantPrefix("t1"))
	if v2Packs == 0 {
		t.Fatal("expected v2 pack blobs")
	}

	// Delete only the v2 manifest.
	if err := csV2.Delete(ctx, keyV2, storedV2.Version); err != nil {
		t.Fatalf("Delete v2: %v", err)
	}

	gc := NewChunkedGC(upstream, chunks, nil)
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	// Only v2 pack blobs should be reclaimed.
	if res.ChunksReclaimed != v2Packs {
		t.Errorf("expected %d pack blobs reclaimed, got %d", v2Packs, res.ChunksReclaimed)
	}

	// v1 chunk blobs must be intact.
	v1After, _ := countTenantChunks(ctx, chunks, "t1")
	if v1After != v1Blobs {
		t.Errorf("v1 chunk blobs changed after GC: %d → %d", v1Blobs, v1After)
	}

	// v1 snapshot must still restore correctly.
	rc, _, err := csV1.GetLatest(ctx, keyV1)
	if err != nil {
		t.Fatalf("GetLatest v1: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("v1 snapshot corrupted after GC")
	}
}
