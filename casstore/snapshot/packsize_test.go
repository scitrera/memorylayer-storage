// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"sync"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// recordingDedupStore is a MemoryDedupStore that ALSO implements the optional
// snapshot.PackSizeRecorder capability, capturing every per-pack size the write
// and compaction paths record. It proves the capability is detected and called
// exactly once per pack (never per chunk), and that PurgePacks drops the size.
type recordingDedupStore struct {
	*MemoryDedupStore
	mu       sync.Mutex
	sizes    map[string]int64 // packHash -> last recorded compressed size
	calls    int              // total RecordPackSizes invocations
	recorded int              // total PackSize rows recorded
}

func newRecordingDedupStore() *recordingDedupStore {
	return &recordingDedupStore{MemoryDedupStore: NewMemoryDedupStore(), sizes: map[string]int64{}}
}

func (r *recordingDedupStore) RecordPackSizes(_ context.Context, _ string, sizes []PackSize) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	for _, ps := range sizes {
		r.recorded++
		r.sizes[ps.PackHash] = ps.CompressedBytes
	}
	return nil
}

// PurgePacks drops the recorded sizes too, mirroring pgindex (so a reclaimed
// pack no longer contributes to the physical rollup).
func (r *recordingDedupStore) PurgePacks(ctx context.Context, domain string, packHashes []string) error {
	if err := r.MemoryDedupStore.PurgePacks(ctx, domain, packHashes); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ph := range packHashes {
		delete(r.sizes, ph)
	}
	return nil
}

var _ PackSizeRecorder = (*recordingDedupStore)(nil)

// TestWritePath_RecordsPackSizeOncePerPack proves the core-write-path change:
// when the durable store implements PackSizeRecorder, ChunkedStore.Put records
// ONE pack-size row per pack even when that pack holds MANY chunks — the per-pack
// (not per-chunk) invariant that keeps the physical SUM from over-counting.
func TestWritePath_RecordsPackSizeOncePerPack(t *testing.T) {
	ctx := context.Background()
	const blockSize = 1 << 20 // FIXED-1M → one chunk each

	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	store := newRecordingDedupStore()
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		SplitterName:    "FIXED-1M",
		PackTargetBytes: 16 << 20, // large target → all 4 chunks land in ONE pack
		DedupDomain:     "t1",
		Index:           NewGlobalIndex(store, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}

	// A 4 MiB object → 4 chunks, all packed into a single ~4 MiB pack (< 16 MiB target).
	payload := make([]byte, 4*blockSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, SnapshotKey{Tenant: "t1", OwnerKey: "obj/0"}, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	store.mu.Lock()
	packs := len(store.sizes)
	recorded := store.recorded
	var totalSize int64
	for _, sz := range store.sizes {
		totalSize += sz
	}
	store.mu.Unlock()

	// Exactly ONE pack recorded, ONE size row — not 4 (per-chunk would be 4).
	if packs != 1 {
		t.Fatalf("recorded %d distinct packs, want 1 (all 4 chunks in one pack)", packs)
	}
	if recorded != 1 {
		t.Fatalf("recorded %d PackSize rows, want 1 (per-pack, NOT per-chunk)", recorded)
	}
	// The recorded compressed size must be > 0 and roughly the pack's on-disk size
	// (random data → ~no compression, so at least the 4 MiB of content).
	if totalSize < 4*blockSize {
		t.Errorf("recorded pack size %d, want >= %d (the pack's physical bytes)", totalSize, 4*blockSize)
	}
}

// TestWritePath_NoRecorderIsNoOp proves the optional capability is truly
// optional: a plain MemoryDedupStore (no PackSizeRecorder) drives Put/Get
// end-to-end with no error — stores that don't implement it lose only the
// accounting, never correctness.
func TestWritePath_NoRecorderIsNoOp(t *testing.T) {
	ctx := context.Background()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	// Plain MemoryDedupStore: does NOT implement PackSizeRecorder.
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		SplitterName:    "FIXED-1M",
		PackTargetBytes: 16 << 20,
		DedupDomain:     "t1",
		Index:           NewGlobalIndex(NewMemoryDedupStore(), nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	// packSizeRecorder() must be nil for a store without the capability.
	if cs.packSizeRecorder() != nil {
		t.Fatal("packSizeRecorder() should be nil for a store without PackSizeRecorder")
	}

	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	key := SnapshotKey{Tenant: "t1", OwnerKey: "obj/0"}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put (no recorder): %v", err)
	}
	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatal("round-trip mismatch without a pack-size recorder")
	}
}
