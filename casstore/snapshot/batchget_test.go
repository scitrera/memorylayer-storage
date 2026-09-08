// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
)

// writeSharedPackKeys writes n keys via PutBatch so their chunks share packs
// (the cross-key-coalescing scenario), returning the keys and their bytes.
func writeSharedPackKeys(t *testing.T, cs *ChunkedStore, n, sz int) ([]SnapshotKey, map[SnapshotKey][]byte) {
	t.Helper()
	ctx := context.Background()
	keys := make([]SnapshotKey, n)
	want := make(map[SnapshotKey][]byte, n)
	items := make([]BatchPutItem, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		k := SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}
		keys[i], want[k] = k, b
		items[i] = BatchPutItem{Key: k, Data: b}
	}
	if _, err := cs.PutBatch(ctx, items); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	return keys, want
}

func assertBatchMatchesPerKey(t *testing.T, cs *ChunkedStore, keys []SnapshotKey, want map[SnapshotKey][]byte, got map[SnapshotKey][]byte) {
	t.Helper()
	ctx := context.Background()
	for _, k := range keys {
		if !bytes.Equal(got[k], want[k]) {
			t.Fatalf("key %v: batch bytes (%d) != want (%d)", k, len(got[k]), len(want[k]))
		}
		rc, _, err := cs.GetLatest(ctx, k)
		if err != nil {
			t.Fatalf("GetLatest %v: %v", k, err)
		}
		per, _ := io.ReadAll(rc)
		_ = rc.Close()
		if !bytes.Equal(got[k], per) {
			t.Fatalf("key %v: batch bytes != per-key GetLatest bytes", k)
		}
	}
}

// TestGetLatestBatch_CoalescesAcrossKeys is the Phase-1 win: many keys whose
// chunks share packs are served by FAR fewer backing GETs than a per-key
// GetLatest loop, while every key reads back byte-identical.
func TestGetLatestBatch_CoalescesAcrossKeys(t *testing.T) {
	const packTarget = 1 << 20
	cs, _, cc := chunkedTestStackCounting(t, packTarget)
	ctx := context.Background()

	const n, sz = 24, 200 * 1024 // ~4.7 MiB over ~5 packs of 1 MiB → many keys/pack
	keys, want := writeSharedPackKeys(t, cs, n, sz)

	// Batch read first (it warms the shared framing cache).
	before := cc.gets.Load()
	got, stats, err := cs.GetLatestBatch(ctx, keys, 4)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	batchGets := cc.gets.Load() - before

	// Per-key GetLatest of the same set, for the GET-count comparison.
	before = cc.gets.Load()
	for _, k := range keys {
		rc, _, err := cs.GetLatest(ctx, k)
		if err != nil {
			t.Fatalf("GetLatest %v: %v", k, err)
		}
		_, _ = io.ReadAll(rc)
		_ = rc.Close()
	}
	perKeyGets := cc.gets.Load() - before

	t.Logf("backing GETs: batch=%d per-key=%d (n=%d)", batchGets, perKeyGets, n)
	if batchGets >= perKeyGets {
		t.Errorf("coalescing did not reduce GETs: batch=%d per-key=%d", batchGets, perKeyGets)
	}
	if batchGets >= int64(n) {
		t.Errorf("batch issued %d GETs for %d keys — expected far fewer (one per shared pack)", batchGets, n)
	}
	// The reported stats are the field signal: far fewer data GETs than keys.
	t.Logf("stats: packGets=%d bytes=%d", stats.PackGets, stats.Bytes)
	if stats.PackGets <= 0 || stats.PackGets >= int64(n) {
		t.Errorf("stats.PackGets = %d, want in (0, %d)", stats.PackGets, n)
	}
	if stats.Bytes <= 0 {
		t.Errorf("stats.Bytes = %d, want > 0", stats.Bytes)
	}

	assertBatchMatchesPerKey(t, cs, keys, want, got)

	// A missing key is absent from the result (not an error).
	miss := SnapshotKey{Tenant: "t1", OwnerKey: "s/99999"}
	g2, _, err := cs.GetLatestBatch(ctx, []SnapshotKey{miss, keys[0]}, 0)
	if err != nil {
		t.Fatalf("GetLatestBatch (with missing): %v", err)
	}
	if _, ok := g2[miss]; ok {
		t.Errorf("missing key must be absent from the result")
	}
	if !bytes.Equal(g2[keys[0]], want[keys[0]]) {
		t.Errorf("present key wrong in mixed batch")
	}
}

// TestGetLatestBatch_Compressed exercises the whole-pack decompress path of the
// coalescer (compressed packs can't be range-sliced).
func TestGetLatestBatch_Compressed(t *testing.T) {
	const packTarget = 1 << 20
	cs, _, _ := compressedChunkedStackWithPackSize(t, packTarget)
	keys, want := writeSharedPackKeys(t, cs, 16, 200*1024)
	got, _, err := cs.GetLatestBatch(context.Background(), keys, 3)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	assertBatchMatchesPerKey(t, cs, keys, want, got)
}

// TestGetLatestBatch_V1Standalone exercises the standalone-chunk path: with
// packing disabled, manifests are v1 (empty PackHash) and each chunk is its own
// blob.
func TestGetLatestBatch_V1Standalone(t *testing.T) {
	cs, _, _ := chunkedTestStackWithPackSize(t, packingDisabled)
	ctx := context.Background()
	keys := make([]SnapshotKey, 6)
	want := make(map[SnapshotKey][]byte, 6)
	for i := range keys {
		b := make([]byte, 64*1024)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		k := SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}
		keys[i], want[k] = k, b
		if _, err := cs.Put(ctx, k, SnapshotMetadata{}, bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}
	got, _, err := cs.GetLatestBatch(ctx, keys, 2)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	assertBatchMatchesPerKey(t, cs, keys, want, got)
}

// TestGroupContiguousLocs is the §4 sparsity guard: offset-sorted chunks split
// into maximal contiguous runs, so each run becomes one tight ranged GET (no
// fetched-but-unneeded bytes between scattered chunks).
func TestGroupContiguousLocs(t *testing.T) {
	mk := func(off, size int) chunkLoc { return chunkLoc{ref: chunkRef{Offset: off, Size: size}} }
	cases := []struct {
		name string
		locs []chunkLoc
		want int
	}{
		{"adjacent→1", []chunkLoc{mk(0, 10), mk(10, 10), mk(20, 5)}, 1},
		{"scattered→3", []chunkLoc{mk(0, 10), mk(100, 10), mk(1000, 10)}, 3},
		{"dup+adjacent→1", []chunkLoc{mk(0, 10), mk(0, 10), mk(10, 5)}, 1},
		{"gap then pair→2", []chunkLoc{mk(0, 10), mk(50, 10), mk(60, 10)}, 2},
		{"single→1", []chunkLoc{mk(7, 3)}, 1},
	}
	for _, c := range cases {
		if got := len(groupContiguousLocs(c.locs)); got != c.want {
			t.Errorf("%s: %d groups, want %d", c.name, got, c.want)
		}
	}
}

// TestGetLatestBatch_ScatteredSubset reads a NON-contiguous subset of keys whose
// chunks were packed together — the dedup-fragmentation shape. Correctness must
// hold (the sparsity guard fetches tight sub-runs, skipping the unneeded chunks
// between).
func TestGetLatestBatch_ScatteredSubset(t *testing.T) {
	cs, _, _ := chunkedTestStackWithPackSize(t, 1<<20)
	keys, want := writeSharedPackKeys(t, cs, 24, 200*1024)
	var sub []SnapshotKey
	subWant := map[SnapshotKey][]byte{}
	for i := 0; i < len(keys); i += 3 { // every 3rd key: non-adjacent within packs
		sub = append(sub, keys[i])
		subWant[keys[i]] = want[keys[i]]
	}
	got, _, err := cs.GetLatestBatch(context.Background(), sub, 4)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	assertBatchMatchesPerKey(t, cs, sub, subWant, got)
}

// TestGetLatestBatch_Empty returns an empty, non-nil map with no error.
func TestGetLatestBatch_Empty(t *testing.T) {
	cs, _, _ := chunkedTestStackWithPackSize(t, 1<<20)
	got, _, err := cs.GetLatestBatch(context.Background(), nil, 0)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("GetLatestBatch(nil) = (%v, %v), want (empty map, nil)", got, err)
	}
}
