// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// chunkedTestStack builds a fresh ChunkedStore over a LocalStore (for
// manifests) + local-filesystem BlobStore (for chunks), both backed by
// per-test tempdirs. Packing is enabled with the default 16 MiB target.
func chunkedTestStack(t *testing.T) (*ChunkedStore, SnapshotStore, blobstore.Storage) {
	t.Helper()
	return chunkedTestStackWithPackSize(t, 0 /* use default */)
}

// chunkedTestStackWithPackSize builds the same stack but with an explicit
// pack target size so tests can override it. Pass packingDisabled to
// exercise v1 (no-pack) behaviour.
func chunkedTestStackWithPackSize(t *testing.T, packTarget int) (*ChunkedStore, SnapshotStore, blobstore.Storage) {
	t.Helper()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{
		Root: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(context.Background()) })

	cfg := ChunkedConfig{PackTargetBytes: packTarget}
	cs, err := NewChunkedStore(upstream, chunks, cfg)
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, upstream, chunks
}

// compressedChunkedStackWithPackSize is chunkedTestStackWithPackSize with a zstd
// content-type compression policy — i.e. exactly the mlfs local-mode posture
// (slices carry no content type → compressed). Used to exercise compaction over
// compressed source packs, which the uncompressed stack does not cover.
func compressedChunkedStackWithPackSize(t *testing.T, packTarget int) (*ChunkedStore, SnapshotStore, blobstore.Storage) {
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
		PackTargetBytes:   packTarget,
		CompressionPolicy: NewContentTypePolicy(CompressZstd),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, upstream, chunks
}

func TestChunkedStore_RoundTripExact(t *testing.T) {
	cs, _, _ := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "owner-1"}

	// 5 MiB random payload — guarantees the splitter sees multiple boundaries
	// (Buzhash32 at 1MB target, 8MB max segment).
	payload := make([]byte, 5*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	stored, err := cs.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// v2 packing is the default so the tag is manifestFormatV2.
	if got := stored.Tags["chunked_format"]; got != manifestFormatV2 {
		t.Errorf("chunked_format tag: got %q, want %q", got, manifestFormatV2)
	}

	rc, meta, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	if meta.Version != stored.Version {
		t.Errorf("version mismatch: got %q want %q", meta.Version, stored.Version)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d (first-diff at %d)",
			len(got), len(payload), firstDiffOffset(got, payload))
	}
}

// TestChunkedStore_PutBatchSharesPacks is the throughput fix: many small objects
// PUT via PutBatch share pack blobs (≈1 pack for a few MiB) instead of one tiny
// pack each, and every object still reads back byte-identical.
func TestChunkedStore_PutBatchSharesPacks(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t) // default 16 MiB pack target
	ctx := context.Background()

	const n = 40
	const sz = 128 * 1024 // 128 KiB each; 40×128K = 5 MiB < 16 MiB ⇒ ~1 shared pack
	items := make([]BatchPutItem, n)
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatalf("rand: %v", err)
		}
		want[i] = b
		items[i] = BatchPutItem{Key: SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}, Data: b}
	}

	metas, err := cs.PutBatch(ctx, items)
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if len(metas) != n {
		t.Fatalf("got %d metas, want %d", len(metas), n)
	}

	// Packing engaged: 5 MiB of 128 KiB objects share ~1 pack, NOT 40.
	packs, err := countTenantBlobs(ctx, chunks, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if packs > 2 {
		t.Errorf("PutBatch did not coalesce: %d pack blobs for %d×%d KiB items (want ≈1)", packs, n, sz/1024)
	}

	// Every item reads back byte-identical.
	for i := 0; i < n; i++ {
		rc, _, err := cs.GetLatest(ctx, items[i].Key)
		if err != nil {
			t.Fatalf("GetLatest %d: %v", i, err)
		}
		got, rerr := io.ReadAll(rc)
		rc.Close()
		if rerr != nil {
			t.Fatalf("ReadAll %d: %v", i, rerr)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("item %d round-trip mismatch: %d vs %d bytes (first-diff %d)",
				i, len(got), len(want[i]), firstDiffOffset(got, want[i]))
		}
	}

	// Contrast: the same objects via per-item Put produce one tiny pack EACH.
	cs2, _, chunks2 := chunkedTestStack(t)
	for i := 0; i < n; i++ {
		if _, err := cs2.Put(ctx, items[i].Key, SnapshotMetadata{}, bytes.NewReader(want[i])); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	perItemPacks, err := countTenantBlobs(ctx, chunks2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if perItemPacks < n {
		t.Errorf("sanity: per-item Put expected ≥%d packs, got %d", n, perItemPacks)
	}
}

// TestChunkedStore_PutBatchPackingDisabled covers the v1 fallback: PutBatch with
// packing disabled still stores + reads back every item correctly.
func TestChunkedStore_PutBatchPackingDisabled(t *testing.T) {
	cs, _, _ := chunkedTestStackWithPackSize(t, packingDisabled)
	ctx := context.Background()
	items := []BatchPutItem{
		{Key: SnapshotKey{Tenant: "t1", OwnerKey: "s/1"}, Data: []byte("hello")},
		{Key: SnapshotKey{Tenant: "t1", OwnerKey: "s/2"}, Data: bytes.Repeat([]byte("x"), 4096)},
	}
	if _, err := cs.PutBatch(ctx, items); err != nil {
		t.Fatalf("PutBatch (v1): %v", err)
	}
	for _, it := range items {
		rc, meta, err := cs.GetLatest(ctx, it.Key)
		if err != nil {
			t.Fatalf("GetLatest %s: %v", it.Key.OwnerKey, err)
		}
		if meta.Tags["chunked_format"] != manifestFormatV1 {
			t.Errorf("%s: format %q, want v1", it.Key.OwnerKey, meta.Tags["chunked_format"])
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, it.Data) {
			t.Errorf("%s round-trip mismatch", it.Key.OwnerKey)
		}
	}
}

func TestChunkedStore_DedupsAcrossSnapshots(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "owner-1"}

	// Take TWO snapshots of IDENTICAL content. Pack storage should
	// double-count refs but only store the unique bytes once.
	payload := bytes.Repeat([]byte("abcdefghij"), 200_000) // 2MB highly repetitive
	for i := 0; i < 2; i++ {
		_, err := cs.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	// Count distinct packs in the blobstore. With identical content the
	// number should be small and constant — second Put adds no new blobs
	// because dedup reuses refs from the prior manifest.
	first, err := countTenantBlobs(ctx, chunks, key.Tenant)
	if err != nil {
		t.Fatalf("countTenantBlobs: %v", err)
	}

	// Now take a third snapshot of the same content. Blob count must
	// still be unchanged.
	if _, err := cs.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put #3: %v", err)
	}
	second, err := countTenantBlobs(ctx, chunks, key.Tenant)
	if err != nil {
		t.Fatalf("countTenantBlobs #2: %v", err)
	}
	if second != first {
		t.Errorf("expected blob count to stay %d after re-Put of identical content, got %d", first, second)
	}
}

func TestChunkedStore_DifferentContentProducesDifferentChunks(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "owner-1"}

	payload1 := bytes.Repeat([]byte("0123456789"), 200_000)
	payload2 := bytes.Repeat([]byte("ABCDEFGHIJ"), 200_000)

	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload1)); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	first, _ := countTenantBlobs(ctx, chunks, key.Tenant)

	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload2)); err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	second, _ := countTenantBlobs(ctx, chunks, key.Tenant)

	if second <= first {
		t.Errorf("expected blob count to grow when content differs; got %d→%d", first, second)
	}
}

func TestChunkedStore_TenantIsolation(t *testing.T) {
	cs, _, chunks := chunkedTestStack(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("xyz"), 500_000)

	// Two tenants snapshot identical content. With per-tenant prefixing,
	// each tenant gets its OWN set of blobs (no cross-tenant dedup
	// — that's a safety property; see architect review §Q3).
	for _, tenant := range []string{"alice", "bob"} {
		key := SnapshotKey{Tenant: tenant, OwnerKey: "owner-x"}
		if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put for %s: %v", tenant, err)
		}
	}

	aliceCount, _ := countTenantBlobs(ctx, chunks, "alice")
	bobCount, _ := countTenantBlobs(ctx, chunks, "bob")
	if aliceCount == 0 || bobCount == 0 {
		t.Fatalf("each tenant must have blobs; alice=%d bob=%d", aliceCount, bobCount)
	}

	// Alice's packs must not appear under bob's prefix and vice versa.
	for _, prefix := range []blobstore.ID{packTenantPrefix("alice"), chunkTenantPrefix("alice")} {
		if err := chunks.ListBlobs(ctx, prefix, func(md blobstore.Metadata) error {
			if !strings.HasPrefix(string(md.BlobID), string(prefix)) {
				t.Errorf("alice listing leaked %q under prefix %q", md.BlobID, prefix)
			}
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs alice prefix %q: %v", prefix, err)
		}
	}
}

func TestChunkedStore_EmptyInput(t *testing.T) {
	cs, _, _ := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "empty"}

	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	rc, _, err := cs.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("Get empty: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != 0 {
		t.Errorf("empty round-trip returned %d bytes", len(got))
	}
}

func TestChunkedStore_DeleteRemovesManifestOnly(t *testing.T) {
	// Invariant: Delete removes the manifest from the upstream
	// SnapshotStore but leaves chunks/packs alone. GC is a separate
	// process (see chunked_gc.go).
	cs, upstream, chunks := chunkedTestStack(t)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "delete-me"}

	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader([]byte("hello world")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	before, _ := countTenantBlobs(ctx, chunks, key.Tenant)

	if err := cs.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := upstream.Get(ctx, key, stored.Version); err == nil {
		t.Errorf("manifest should be gone after Delete")
	}
	after, _ := countTenantBlobs(ctx, chunks, key.Tenant)
	if after != before {
		t.Errorf("blobs must be preserved after Delete (GC reclaims them); got %d → %d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Pack-file tests (v2 format)
// ---------------------------------------------------------------------------

// TestChunkedStore_PacksMultipleChunksTogether is the load-bearing test for
// the cost-reduction claim: a large payload must produce significantly fewer
// pack blobs than it has chunks.
func TestChunkedStore_PacksMultipleChunksTogether(t *testing.T) {
	// Use a small pack target (512 KiB) so the test doesn't need 50 MiB of
	// data. With ~1 MiB average chunks and a 512 KiB pack target the packer
	// will emit one pack per chunk (each chunk is slightly larger than the
	// target). But with a 4 MiB target, a 10 MiB payload (~10 chunks)
	// should produce ~3 packs — still demonstrating grouping.
	//
	// We use a 4 MiB pack target and a 10 MiB random payload.
	packTarget := 4 * 1024 * 1024 // 4 MiB
	cs, _, chunks := chunkedTestStackWithPackSize(t, packTarget)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "pack-test"}

	payload := make([]byte, 10*1024*1024) // 10 MiB random data
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.Tags["chunked_format"] != manifestFormatV2 {
		t.Fatalf("expected v2 format tag, got %q", stored.Tags["chunked_format"])
	}

	// Count packs and chunks in the blob store.
	packCount, _ := countBlobsWithPrefix(ctx, chunks, packTenantPrefix(key.Tenant))
	chunkCount, _ := countBlobsWithPrefix(ctx, chunks, chunkTenantPrefix(key.Tenant))

	// With packing enabled, all blobs should be pack blobs (no standalone chunks).
	if chunkCount != 0 {
		t.Errorf("v2 Put should write pack blobs only; got %d standalone chunk blobs", chunkCount)
	}
	if packCount == 0 {
		t.Fatalf("expected at least one pack blob, got 0")
	}

	// Parse the manifest to count how many chunk entries it has.
	// There should be more chunks-in-manifest than pack blobs on disk.
	rc, _, err := cs.upstream.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest manifest: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if len(m.Chunks) <= packCount {
		t.Errorf("expected more chunks in manifest (%d) than pack blobs on disk (%d) — grouping not working",
			len(m.Chunks), packCount)
	}
	t.Logf("10 MiB payload: %d chunks in manifest, %d pack blobs on disk (pack target=%d KiB)",
		len(m.Chunks), packCount, packTarget/1024)

	// Verify byte-perfect round-trip.
	restoreRC, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest restore: %v", err)
	}
	got, _ := io.ReadAll(restoreRC)
	restoreRC.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d (first-diff at %d)",
			len(got), len(payload), firstDiffOffset(got, payload))
	}
}

// TestChunkedStore_RoundTripWithPacks verifies byte-perfect restore for a
// payload that spans multiple pack blobs. Uses a small pack target to force
// multiple packs.
func TestChunkedStore_RoundTripWithPacks(t *testing.T) {
	packTarget := 1 * 1024 * 1024 // 1 MiB — forces multiple packs for a 5 MiB payload
	cs, _, _ := chunkedTestStackWithPackSize(t, packTarget)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "rt-packs"}

	payload := make([]byte, 5*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d (first-diff at %d)",
			len(got), len(payload), firstDiffOffset(got, payload))
	}
}

// TestChunkedStore_BackwardCompatV1 writes a v1 manifest directly (no
// packing) and reads it back through the new ChunkedStore — must succeed.
func TestChunkedStore_BackwardCompatV1(t *testing.T) {
	// Use packingDisabled so the store writes v1 format.
	cs, _, _ := chunkedTestStackWithPackSize(t, packingDisabled)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "v1-compat"}

	payload := []byte("hello from v1 format — backward compat test payload that is longer than trivial")
	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if stored.Tags["chunked_format"] != manifestFormatV1 {
		t.Fatalf("expected v1 format tag with packing disabled, got %q", stored.Tags["chunked_format"])
	}

	// Read back through a v2-capable store (the default). It must transparently
	// decode the v1 manifest and return identical bytes.
	cs2, _, _ := chunkedTestStack(t) // default pack-enabled store
	// But cs2 has its own upstream/chunks. We need to read via the SAME store
	// that wrote. Use the same cs (which is v1-write) for the read — it also
	// supports v1 reads.
	rc, _, err := cs.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("v1 backward compat round-trip failed: got %q, want %q", got, payload)
	}
	_ = cs2 // suppress unused warning
}

// TestChunkedStore_PackSizeZeroDisablesPacking verifies that PackTargetBytes
// set to packingDisabled writes v1 single-chunk-per-blob format.
func TestChunkedStore_PackSizeZeroDisablesPacking(t *testing.T) {
	cs, _, chunks := chunkedTestStackWithPackSize(t, packingDisabled)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", OwnerKey: "no-pack"}

	// 3 MiB payload should produce multiple chunks.
	payload := make([]byte, 3*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	stored, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.Tags["chunked_format"] != manifestFormatV1 {
		t.Fatalf("expected v1 format tag when packing disabled, got %q", stored.Tags["chunked_format"])
	}

	// With packing disabled, blobs must be standalone chunk blobs (chunk- prefix).
	packCount, _ := countBlobsWithPrefix(ctx, chunks, packTenantPrefix(key.Tenant))
	chunkCount, _ := countBlobsWithPrefix(ctx, chunks, chunkTenantPrefix(key.Tenant))
	if packCount != 0 {
		t.Errorf("packing disabled: expected 0 pack blobs, got %d", packCount)
	}
	if chunkCount == 0 {
		t.Errorf("packing disabled: expected chunk blobs, got 0")
	}

	// Byte-perfect restore.
	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("no-pack round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// countTenantChunks tallies blobs under the tenant chunk prefix (v1 compat).
// Kept for GC tests that specifically count v1 chunk blobs.
func countTenantChunks(ctx context.Context, chunks blobstore.Storage, tenant string) (int, error) {
	return countBlobsWithPrefix(ctx, chunks, chunkTenantPrefix(tenant))
}

// countTenantBlobs tallies ALL chunk and pack blobs for a tenant.
// Used by tests that don't care about the v1/v2 split.
func countTenantBlobs(ctx context.Context, chunks blobstore.Storage, tenant string) (int, error) {
	n := 0
	for _, prefix := range []blobstore.ID{chunkTenantPrefix(tenant), packTenantPrefix(tenant)} {
		c, err := countBlobsWithPrefix(ctx, chunks, prefix)
		if err != nil {
			return 0, err
		}
		n += c
	}
	return n, nil
}

// countBlobsWithPrefix tallies blobs with the given prefix.
func countBlobsWithPrefix(ctx context.Context, chunks blobstore.Storage, prefix blobstore.ID) (int, error) {
	n := 0
	err := chunks.ListBlobs(ctx, prefix, func(blobstore.Metadata) error {
		n++
		return nil
	})
	return n, err
}

// firstDiffOffset returns the offset of the first differing byte between a
// and b, or -1 when they're identical. Helpful for failure messages.
func firstDiffOffset(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}
