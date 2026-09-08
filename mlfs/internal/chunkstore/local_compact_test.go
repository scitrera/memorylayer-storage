// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// TestLocalCompactConsolidates exercises pack compaction through the mlfs
// chunkstore wrapper (which uses the zstd compression policy, like the live
// daemon) end-to-end: many small per-slice packs are coalesced via the exposed
// Compactor, and every slice still reads back byte-identical. This is what
// `mlfs-admin compact` drives.
func TestLocalCompactConsolidates(t *testing.T) {
	const packTarget = 1 << 20 // 1 MiB
	local, err := NewLocal(t.TempDir(), "d1", packTarget)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	ctx := context.Background()

	const n = 12
	const sz = 200 * 1024 // each Write → one small (< target) pack
	want := make(map[uint64][]byte, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		id := uint64(1000 + i)
		want[id] = b
		if err := local.Store.Write(ctx, id, b, ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}

	countPacks := func() int {
		c := 0
		if err := local.Chunks.ListBlobs(ctx, blobstore.ID("pack-d1-"), func(blobstore.Metadata) error {
			c++
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
		return c
	}
	before := countPacks()
	if before < n {
		t.Fatalf("expected ≥%d small packs, got %d", n, before)
	}

	res, err := local.Compactor.Compact(ctx, "d1", snapshot.CompactConfig{MinPackBytes: packTarget})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.ManifestsRewritten != n {
		t.Errorf("ManifestsRewritten = %d, want %d", res.ManifestsRewritten, n)
	}
	if res.PacksWritten >= n {
		t.Errorf("no consolidation: wrote %d packs for %d slices", res.PacksWritten, n)
	}

	// Every slice reads back byte-identical through the mlfs wrapper.
	for id, w := range want {
		got, err := local.Store.Read(ctx, id)
		if err != nil {
			t.Fatalf("Read %d: %v", id, err)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("slice %d mismatch after compaction (%d vs %d bytes)", id, len(got), len(w))
		}
	}
}
