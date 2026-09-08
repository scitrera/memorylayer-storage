// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// newFixedGlobalStack builds a ChunkedStore with FIXED-1M chunks (deterministic
// boundaries → identical content dedups deterministically) and a GlobalIndex
// (cross-object dedup), so a test can construct genuine pack fragmentation.
func newFixedGlobalStack(t *testing.T, packTarget int) *ChunkedStore {
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
		SplitterName:    "FIXED-1M",
		PackTargetBytes: packTarget,
		DedupDomain:     "t1",
		Index:           NewGlobalIndex(NewMemoryDedupStore(), nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs
}

// TestCompact_DefragImprovesReadLocality is the cheap-defrag prototype measurement:
// take a set of files whose chunks are scattered one-per-pack (dedup fragmentation),
// compact them HASH-order vs FILE-order, then read each file back through the
// coalescing path and compare the backing pack-GETs. File-order consolidation should
// place each file's chunks contiguously, so a read coalesces into far fewer GETs.
func TestCompact_DefragImprovesReadLocality(t *testing.T) {
	ctx := context.Background()
	const (
		blockSize  = 1 << 20 // one FIXED-1M chunk
		nFiles     = 4
		perFile    = 4 // chunks per file
		nBlocks    = nFiles * perFile
		packTarget = 4 << 20 // 4 chunks per consolidated pack — one whole file fits exactly
		minPack    = 3 << 20 // compaction selects the 2-chunk (2 MiB) filler packs (< this)
	)

	// Distinct 1 MiB blocks; file f is an INTERLEAVED selection so its chunks scatter
	// across packs and interleave with other files (the dedup-fragmentation shape).
	blocks := make([][]byte, nBlocks)
	for i := range blocks {
		b := make([]byte, blockSize)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		blocks[i] = b
	}
	fileKey := func(f int) SnapshotKey { return SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("a/%d", f)} }
	fileData := func(f int) []byte {
		var d []byte
		for k := 0; k < perFile; k++ {
			d = append(d, blocks[f+k*nFiles]...)
		}
		return d
	}

	build := func(t *testing.T, defrag bool) *ChunkedStore {
		t.Helper()
		cs := newFixedGlobalStack(t, packTarget)
		// Fillers: write blocks in PAIRS so each lands in a MULTI-chunk pack
		// (packHash != chunkHash). This matters — the v2 write path's dedup guard
		// (`ref.PackHash != hash`, store_chunked.go) skips a chunk whose recorded
		// pack hash equals the chunk hash, which is exactly the single-chunk-pack
		// case. So a single-block filler is NEVER deduped against and produces no
		// fragmentation; a 2-block filler pack is. Block 2j and 2j+1 share pack j,
		// so file f (blocks f, f+4, f+8, f+12) dedup-scatters one chunk into each of
		// 4 distinct filler packs.
		for j := 0; j < nBlocks/2; j++ {
			pair := append(append([]byte{}, blocks[2*j]...), blocks[2*j+1]...)
			if _, err := cs.Put(ctx, SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("z/%02d", j)}, SnapshotMetadata{}, bytes.NewReader(pair)); err != nil {
				t.Fatalf("put filler pair %d: %v", j, err)
			}
		}
		// Files: their chunks dedup to the filler packs → fragmented manifests.
		for f := 0; f < nFiles; f++ {
			if _, err := cs.Put(ctx, fileKey(f), SnapshotMetadata{}, bytes.NewReader(fileData(f))); err != nil {
				t.Fatalf("put file %d: %v", f, err)
			}
		}
		nb, _ := countTenantBlobs(ctx, cs.chunks, "t1")
		m0, _, _ := cs.latestManifest(ctx, fileKey(0))
		distinct := map[string]bool{}
		for _, r := range m0.Chunks {
			distinct[r.PackHash] = true
		}
		t.Logf("defrag=%v: blobs before compact=%d, file0 chunks=%d in %d distinct packs", defrag, nb, len(m0.Chunks), len(distinct))
		// Precondition: the dedup-scatter must have actually fragmented file0 (one
		// chunk per filler pack), else the measurement below is meaningless.
		if len(distinct) != perFile {
			t.Fatalf("setup did not fragment file0: want %d distinct packs, got %d", perFile, len(distinct))
		}
		cr, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: minPack, Defrag: defrag})
		if err != nil {
			t.Fatalf("compact (defrag=%v): %v", defrag, err)
		}
		m0, _, _ = cs.latestManifest(ctx, fileKey(0))
		distinct = map[string]bool{}
		for _, r := range m0.Chunks {
			distinct[r.PackHash] = true
		}
		t.Logf("defrag=%v: compact selected=%d written=%d; file0 now in %d distinct packs", defrag, cr.PacksSelected, cr.PacksWritten, len(distinct))
		return cs
	}

	measure := func(t *testing.T, cs *ChunkedStore) (totalGets int64) {
		t.Helper()
		for f := 0; f < nFiles; f++ {
			got, stats, err := cs.GetLatestBatch(ctx, []SnapshotKey{fileKey(f)}, 1)
			if err != nil {
				t.Fatalf("read file %d: %v", f, err)
			}
			if !bytes.Equal(got[fileKey(f)], fileData(f)) {
				t.Fatalf("file %d round-trip mismatch after compaction", f)
			}
			totalGets += stats.PackGets
		}
		return totalGets
	}

	hashGets := measure(t, build(t, false))
	fileGets := measure(t, build(t, true))

	t.Logf("read pack-GETs across %d files (%d chunks each): hash-order=%d file-order=%d",
		nFiles, perFile, hashGets, fileGets)
	// File-order must reduce the aggregate read round-trips (each file's chunks
	// coalesce into ~ceil(perFile*blockSize/packTarget) packs instead of scattering).
	if fileGets >= hashGets {
		t.Errorf("defrag did not reduce read pack-GETs: hash-order=%d, file-order=%d", hashGets, fileGets)
	}
	// Sanity: file-order is at the theoretical floor — one whole file (4 MiB) fits in
	// one consolidated pack (packTarget 4 MiB), so each file reads in a single GET.
	if floor := int64(nFiles * 1); fileGets > floor {
		t.Logf("note: file-order pack-GETs %d exceeds the %d floor (some dedup overlap across files)", fileGets, floor)
	}
}
