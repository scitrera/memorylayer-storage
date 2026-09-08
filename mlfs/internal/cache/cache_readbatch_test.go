// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// rbBacking is a memStore that also implements chunkstore.BatchReader, recording
// how many ReadBatch calls it received and the largest batch size — so a test can
// prove DiskCache.ReadBatch forwards misses in ONE coalesced call.
type rbBacking struct {
	*memStore
	batchCalls atomic.Int32
	maxBatch   atomic.Int32
}

func (b *rbBacking) ReadBatch(ctx context.Context, ids []uint64, _ int) (map[uint64][]byte, chunkstore.BatchReadStats, error) {
	b.batchCalls.Add(1)
	if int32(len(ids)) > b.maxBatch.Load() {
		b.maxBatch.Store(int32(len(ids)))
	}
	out := make(map[uint64][]byte, len(ids))
	var stats chunkstore.BatchReadStats
	for _, id := range ids {
		if d, err := b.memStore.Read(ctx, id); err == nil {
			out[id] = d
			stats.Bytes += int64(len(d))
		}
	}
	stats.PackGets = 1 // pretend the whole batch coalesced into one pack GET
	return out, stats, nil
}

// TestDiskCache_ReadBatch_CoalescesMisses proves the cache forwards all misses to
// the backing's ReadBatch in ONE call, populates the clean cache for each, and
// serves a subsequent read locally (no backing call).
func TestDiskCache_ReadBatch_CoalescesMisses(t *testing.T) {
	ctx := context.Background()
	back := &rbBacking{memStore: newMemStore()}
	const n = 10
	want := make(map[uint64][]byte, n)
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		d := bytes.Repeat([]byte{byte(i)}, 1024)
		want[id], ids[i] = d, id
		if err := back.Write(ctx, id, d, chunkstore.ClassDefault); err != nil {
			t.Fatal(err)
		}
	}

	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	got, stats, err := c.ReadBatch(ctx, ids, 4)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Errorf("slice %d mismatch", id)
		}
	}
	if back.batchCalls.Load() != 1 {
		t.Errorf("expected exactly 1 backing ReadBatch call, got %d", back.batchCalls.Load())
	}
	if back.maxBatch.Load() != n {
		t.Errorf("expected all %d misses in one batch, got max %d", n, back.maxBatch.Load())
	}
	if stats.PackGets != 1 {
		t.Errorf("backing stats not propagated: PackGets=%d, want 1", stats.PackGets)
	}

	// Now the slices are populated locally: a second ReadBatch makes NO backing call.
	back.batchCalls.Store(0)
	got2, stats2, err := c.ReadBatch(ctx, ids, 4)
	if err != nil {
		t.Fatalf("ReadBatch 2: %v", err)
	}
	if back.batchCalls.Load() != 0 {
		t.Errorf("second ReadBatch hit backing %d times, want 0 (all local)", back.batchCalls.Load())
	}
	if stats2.PackGets != 0 {
		t.Errorf("all-local ReadBatch reported PackGets=%d, want 0", stats2.PackGets)
	}
	for id, w := range want {
		if !bytes.Equal(got2[id], w) {
			t.Errorf("slice %d local-hit mismatch", id)
		}
	}
}

// TestDiskCache_ReadBatch_FallbackConcurrent proves that when the backing can NOT
// batch (plain Store), ReadBatch still fetches every miss and populates — so a
// non-coalescing backing keeps working.
func TestDiskCache_ReadBatch_FallbackConcurrent(t *testing.T) {
	ctx := context.Background()
	back := newMemStore() // implements Store, NOT BatchReader
	const n = 8
	want := make(map[uint64][]byte, n)
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		d := bytes.Repeat([]byte{byte(i + 1)}, 512)
		want[id], ids[i] = d, id
		if err := back.Write(ctx, id, d, chunkstore.ClassDefault); err != nil {
			t.Fatal(err)
		}
	}
	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	got, stats, err := c.ReadBatch(ctx, ids, 4)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(got) != n {
		t.Errorf("got %d slices, want %d", len(got), n)
	}
	if stats.PackGets != int64(n) {
		t.Errorf("fallback PackGets=%d, want %d (one per-id Read)", stats.PackGets, n)
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Errorf("slice %d mismatch", id)
		}
	}
}
