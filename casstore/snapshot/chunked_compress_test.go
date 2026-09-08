// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// chunkedTestStackWithPolicy builds a ChunkedStore over local backends with an
// explicit compression policy, returning the store and its chunk blobstore so
// tests can measure physical bytes.
func chunkedTestStackWithPolicy(t *testing.T, policy CompressionPolicy) (*ChunkedStore, blobstore.Storage) {
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
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		DedupDomain:       "d",
		Index:             NewGlobalIndex(NewMemoryDedupStore(), nil),
		CompressionPolicy: policy,
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, chunks
}

// physicalPackBytes sums the on-disk length of every pack+chunk blob in domain.
func physicalPackBytes(t *testing.T, chunks blobstore.Storage, domain string) int64 {
	t.Helper()
	var total int64
	for _, p := range []blobstore.ID{packTenantPrefix(domain), chunkTenantPrefix(domain)} {
		if err := chunks.ListBlobs(context.Background(), p, func(md blobstore.Metadata) error {
			total += md.Length
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return total
}

func readAllVia(t *testing.T, cs *ChunkedStore, key SnapshotKey) []byte {
	t.Helper()
	rc, _, err := cs.GetLatest(context.Background(), key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return got
}

// TestChunkedCompress_CompressibleShrinksAndRoundTrips writes highly
// compressible content with zstd compression ON and asserts (a) byte-identical
// round-trip and (b) the physical pack bytes are meaningfully smaller than the
// input — i.e. compression actually happened.
func TestChunkedCompress_CompressibleShrinksAndRoundTrips(t *testing.T) {
	cs, chunks := chunkedTestStackWithPolicy(t, NewContentTypePolicy(CompressZstd))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "compressible"}

	// 4 MiB of zeros — trivially compressible.
	payload := make([]byte, 4<<20)
	// Empty content type → policy compresses.
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got := readAllVia(t, cs, key)
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d bytes want %d", len(got), len(payload))
	}

	phys := physicalPackBytes(t, chunks, "d")
	if phys >= int64(len(payload)) {
		t.Fatalf("compression did not shrink: physical=%d input=%d", phys, len(payload))
	}
	// Zeros should compress dramatically; require at least 4x reduction to prove
	// real compression, not just header overhead.
	if phys*4 >= int64(len(payload)) {
		t.Fatalf("compression too weak: physical=%d input=%d (want <25%%)", phys, len(payload))
	}
	t.Logf("zstd: %d input bytes -> %d physical bytes", len(payload), phys)
}

// TestChunkedCompress_TextCompresses checks repeated-text content also shrinks.
func TestChunkedCompress_TextCompresses(t *testing.T) {
	cs, chunks := chunkedTestStackWithPolicy(t, NewContentTypePolicy(CompressZstd))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "text"}

	payload := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog\n", 200000)) // ~8.6 MiB
	if _, err := cs.Put(ctx, key, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch")
	}
	phys := physicalPackBytes(t, chunks, "d")
	if phys >= int64(len(payload)) {
		t.Fatalf("text/plain did not compress: physical=%d input=%d", phys, len(payload))
	}
	t.Logf("text/plain: %d -> %d physical bytes", len(payload), phys)
}

// TestChunkedCompress_IncompressibleRoundTrips writes random (incompressible)
// content with compression ON: it must still round-trip exactly. The header
// overhead means physical bytes are ~equal to input (never corrupt).
func TestChunkedCompress_IncompressibleRoundTrips(t *testing.T) {
	cs, _ := chunkedTestStackWithPolicy(t, NewContentTypePolicy(CompressZstd))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "random"}

	payload := make([]byte, 4<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
		t.Fatalf("random round-trip mismatch")
	}
}

// TestChunkedCompress_UncompressedClassStoredRaw verifies that a content type
// in the store-uncompressed class (e.g. image/png, a GPU-loadable tensor type,
// or the explicit store_uncompressed tag) is stored WITHOUT a codec applied:
// the physical pack content is byte-identical to the original chunk bytes.
func TestChunkedCompress_UncompressedClassStoredRaw(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		tag         map[string]string
	}{
		{"png", "image/png", nil},
		{"zstd-media", "application/zstd", nil},
		{"safetensors", "application/x-safetensors", nil},
		{"gpu-loadable-tag", "text/plain", map[string]string{TagStoreUncompressed: "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs, chunks := chunkedTestStackWithPolicy(t, NewContentTypePolicy(CompressZstd))
			ctx := context.Background()
			key := SnapshotKey{Tenant: "d", OwnerKey: "raw-" + tc.name}

			// Use highly compressible content so that IF compression were
			// (wrongly) applied, physical bytes would shrink far below input.
			// Storing uncompressed means physical ≈ input + small header.
			payload := bytes.Repeat([]byte{0xAB}, 2<<20)
			meta := SnapshotMetadata{Format: tc.contentType, Tags: tc.tag}
			if _, err := cs.Put(ctx, key, meta, bytes.NewReader(payload)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
				t.Fatalf("round-trip mismatch")
			}
			phys := physicalPackBytes(t, chunks, "d")
			// Uncompressed: physical must be at least the input size (header adds
			// a few bytes per pack). If it shrank, the policy wrongly compressed.
			if phys < int64(len(payload)) {
				t.Fatalf("%s should be stored uncompressed but shrank: physical=%d input=%d",
					tc.contentType, phys, len(payload))
			}
		})
	}
}

// TestChunkedCompress_PackHashUnchangedAcrossCodecs proves the pack identity
// (blob ID) is computed over the UNCOMPRESSED content: the SAME bytes stored
// under different codecs land at the SAME pack blob ID, so cross-codec dedup
// keys correctly and GC (which matches blob IDs) is unaffected.
func TestChunkedCompress_PackHashUnchangedAcrossCodecs(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("dedup-content-"), 200000) // ~2.6 MiB, compressible

	collectPackIDs := func(policy CompressionPolicy) map[blobstore.ID]struct{} {
		cs, chunks := chunkedTestStackWithPolicy(t, policy)
		key := SnapshotKey{Tenant: "d", OwnerKey: "x"}
		if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		ids := map[blobstore.ID]struct{}{}
		if err := chunks.ListBlobs(ctx, packTenantPrefix("d"), func(md blobstore.Metadata) error {
			ids[md.BlobID] = struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
		return ids
	}

	zstdIDs := collectPackIDs(NewContentTypePolicy(CompressZstd))
	noneIDs := collectPackIDs(NewContentTypePolicy(CompressNone))

	if len(zstdIDs) == 0 || len(zstdIDs) != len(noneIDs) {
		t.Fatalf("pack count mismatch: zstd=%d none=%d", len(zstdIDs), len(noneIDs))
	}
	for id := range zstdIDs {
		if _, ok := noneIDs[id]; !ok {
			t.Fatalf("pack ID %s present under zstd but not under none — pack identity depends on codec (dedup would break)", id)
		}
	}
}

// TestChunkedCompress_DedupKeysOnUncompressedContent verifies cross-object dedup
// still works with compression ON: writing identical content under two distinct
// keys adds no new physical pack bytes the second time (the chunks dedup), and
// both objects round-trip exactly.
func TestChunkedCompress_DedupKeysOnUncompressedContent(t *testing.T) {
	cs, chunks := chunkedTestStackWithPolicy(t, NewContentTypePolicy(CompressZstd))
	ctx := context.Background()

	payload := bytes.Repeat([]byte("shared-dedup-block-"), 300000) // ~5.7 MiB

	k1 := SnapshotKey{Tenant: "d", OwnerKey: "obj-1"}
	if _, err := cs.Put(ctx, k1, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put obj-1: %v", err)
	}
	after1 := physicalPackBytes(t, chunks, "d")

	k2 := SnapshotKey{Tenant: "d", OwnerKey: "obj-2"}
	if _, err := cs.Put(ctx, k2, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put obj-2: %v", err)
	}
	after2 := physicalPackBytes(t, chunks, "d")

	if after2 != after1 {
		t.Fatalf("identical content under a second key added physical bytes (dedup broke with compression): after1=%d after2=%d",
			after1, after2)
	}
	if got := readAllVia(t, cs, k1); !bytes.Equal(got, payload) {
		t.Fatalf("obj-1 round-trip mismatch")
	}
	if got := readAllVia(t, cs, k2); !bytes.Equal(got, payload) {
		t.Fatalf("obj-2 round-trip mismatch")
	}
}

// TestChunkedCompress_GCReclaimsCompressedPacks verifies GC works with
// compressed packs: deleting all manifests then running GC reclaims every pack
// (GC matches blob IDs, which are codec-independent).
func TestChunkedCompress_GCReclaimsCompressedPacks(t *testing.T) {
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(context.Background()) })
	dedup := NewMemoryDedupStore()
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		DedupDomain:       "d",
		Index:             NewGlobalIndex(dedup, nil),
		CompressionPolicy: NewContentTypePolicy(CompressZstd),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "gc"}

	payload := bytes.Repeat([]byte("gc-block-"), 400000)
	sm, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if before := physicalPackBytes(t, chunks, "d"); before == 0 {
		t.Fatal("expected pack bytes before GC")
	}

	if err := cs.Delete(ctx, key, sm.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	gc := NewChunkedGC(upstream, chunks, nil)
	gc.Index = dedup
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.ChunksReclaimed == 0 {
		t.Fatalf("GC reclaimed nothing; result=%+v", res)
	}
	if after := physicalPackBytes(t, chunks, "d"); after != 0 {
		t.Fatalf("GC left %d physical bytes after deleting all manifests", after)
	}
}

// TestChunkedCompress_V1PathCompresses checks the v1 (packing-disabled) path
// also compresses standalone chunk blobs and round-trips.
func TestChunkedCompress_V1PathCompresses(t *testing.T) {
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(context.Background()) })
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		PackTargetBytes:   packingDisabled,
		DedupDomain:       "d",
		CompressionPolicy: NewContentTypePolicy(CompressZstd),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "d", OwnerKey: "v1"}
	payload := bytes.Repeat([]byte{0}, 2<<20)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
		t.Fatalf("v1 round-trip mismatch")
	}
	if phys := physicalPackBytes(t, chunks, "d"); phys >= int64(len(payload)) {
		t.Fatalf("v1 path did not compress: physical=%d input=%d", phys, len(payload))
	}
}
