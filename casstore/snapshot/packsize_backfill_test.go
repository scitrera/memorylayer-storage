// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// (recordingDedupStore + newRecordingDedupStore live in packsize_test.go; they
// implement PackSizeRecorder over a per-packHash `sizes` map and drop sizes on
// PurgePacks, exactly what these backfill tests need — reused here.)

// packMeta lists every pack blob for a domain, returning packHash -> on-disk length.
func packMeta(t *testing.T, ctx context.Context, chunks blobstore.Storage, domain string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	if err := chunks.ListBlobs(ctx, packTenantPrefix(domain), func(md blobstore.Metadata) error {
		if _, hash, ok := parsePackID(md.BlobID); ok {
			out[hash] = md.Length
		}
		return nil
	}); err != nil {
		t.Fatalf("list packs: %v", err)
	}
	return out
}

// recSize returns the size recorded for packHash (0/false if none). Reads under
// the recorder's mutex.
func recSize(r *recordingDedupStore, packHash string) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.sizes[packHash]
	return n, ok
}

// TestChunkedGC_BackfillsLivePackSizes proves the GC sweep records the on-disk
// size of every LIVE pack (via the index's PackSizeRecorder) while a reclaimed
// (dead) pack's size is NOT recorded by the backfill. It uses a GlobalIndex over
// a recording store so ChunkedStore writes through the same dedup index the GC
// purges + backfills.
func TestChunkedGC_BackfillsLivePackSizes(t *testing.T) {
	ctx := context.Background()
	rec := newRecordingDedupStore()

	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		DedupDomain: "t1",
		Index:       NewGlobalIndex(rec, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}

	// Live snapshot (keep) and a distinct snapshot (kill) → different pack blobs.
	keyKeep := SnapshotKey{Tenant: "t1", OwnerKey: "keep"}
	keyKill := SnapshotKey{Tenant: "t1", OwnerKey: "kill"}
	if _, err := cs.Put(ctx, keyKeep, SnapshotMetadata{}, bytes.NewReader(bytes.Repeat([]byte("keep-me-alive!"), 100_000))); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	storedKill, err := cs.Put(ctx, keyKill, SnapshotMetadata{}, bytes.NewReader(bytes.Repeat([]byte("kill-this-one!"), 100_000)))
	if err != nil {
		t.Fatalf("Put kill: %v", err)
	}

	allPacks := packMeta(t, ctx, chunks, "t1")
	if len(allPacks) < 2 {
		t.Fatalf("expected at least 2 distinct packs, got %d", len(allPacks))
	}
	deadPacks := killPackHashes(t, ctx, upstream, keyKill)
	if len(deadPacks) == 0 {
		t.Fatal("expected the kill snapshot to reference at least one pack")
	}

	// Reset the recorder so the assertions below measure only what the GC pass
	// records (the Put write path already recorded during the writes above).
	rec.mu.Lock()
	rec.sizes = map[string]int64{}
	rec.mu.Unlock()

	// Delete the kill manifest → its unique packs become orphaned; run GC.
	if err := cs.Delete(ctx, keyKill, storedKill.Version); err != nil {
		t.Fatalf("Delete kill: %v", err)
	}
	gc := NewChunkedGC(upstream, chunks, nil)
	gc.Index = rec // purges hit the recording store; also the backfill target
	if _, err := gc.RunOnce(ctx); err != nil {
		t.Fatalf("GC.RunOnce: %v", err)
	}

	// Every LIVE pack's (hash, length) must have been recorded by the sweep hook.
	livePacks := packMeta(t, ctx, chunks, "t1")
	if len(livePacks) == 0 {
		t.Fatal("expected live packs to remain after GC")
	}
	for hash, length := range livePacks {
		got, ok := recSize(rec, hash)
		if !ok {
			t.Errorf("live pack %s not recorded", hash)
			continue
		}
		if got != length {
			t.Errorf("live pack %s recorded size = %d, want %d", hash, got, length)
		}
	}

	// A dead (reclaimed) pack must NOT have been recorded by the backfill hook.
	for _, dead := range deadPacks {
		if _, ok := livePacks[dead]; ok {
			continue // shared with a live snapshot → legitimately still live
		}
		if _, ok := recSize(rec, dead); ok {
			t.Errorf("dead pack %s was recorded but should not have been", dead)
		}
	}
}

// killPackHashes reads the kill snapshot's manifest and returns its referenced
// pack hashes, so the test knows which packs the deleted snapshot owned.
func killPackHashes(t *testing.T, ctx context.Context, upstream SnapshotStore, key SnapshotKey) []string {
	t.Helper()
	rc, _, err := upstream.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest kill manifest: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read kill manifest: %v", err)
	}
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal kill manifest: %v", err)
	}
	seen := map[string]struct{}{}
	var out []string
	for _, ref := range m.Chunks {
		if ref.PackHash == "" {
			continue
		}
		if _, ok := seen[ref.PackHash]; !ok {
			seen[ref.PackHash] = struct{}{}
			out = append(out, ref.PackHash)
		}
	}
	return out
}

// TestBackfillPackSizes_RecordsAllPacks proves the one-time backfill records N
// packs with correct sizes over an in-memory blobstore, and that an index
// without PackSizeRecorder returns the error.
func TestBackfillPackSizes_RecordsAllPacks(t *testing.T) {
	ctx := context.Background()
	rec := newRecordingDedupStore()

	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		DedupDomain: "t1",
		Index:       NewGlobalIndex(rec, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}

	// Write a few distinct snapshots → several distinct pack blobs.
	for _, owner := range []string{"a", "b", "c"} {
		key := SnapshotKey{Tenant: "t1", OwnerKey: owner}
		payload := bytes.Repeat([]byte("pack-"+owner+"-data!"), 100_000)
		if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put %s: %v", owner, err)
		}
	}

	// Reset the recorder so we only measure what the backfill records (the writes
	// above already recorded via GlobalIndex's write path).
	rec.mu.Lock()
	rec.sizes = map[string]int64{}
	rec.mu.Unlock()

	want := packMeta(t, ctx, chunks, "t1")
	if len(want) == 0 {
		t.Fatal("expected pack blobs written")
	}

	n, err := BackfillPackSizes(ctx, chunks, rec, "t1", nil)
	if err != nil {
		t.Fatalf("BackfillPackSizes: %v", err)
	}
	if n != len(want) {
		t.Errorf("recorded count = %d, want %d", n, len(want))
	}
	for hash, length := range want {
		got, ok := recSize(rec, hash)
		if !ok {
			t.Errorf("pack %s not recorded", hash)
			continue
		}
		if got != length {
			t.Errorf("pack %s recorded size = %d, want %d", hash, got, length)
		}
	}

	// All-domains sweep (domain=="") records the same packs.
	rec2 := newRecordingDedupStore()
	n2, err := BackfillPackSizes(ctx, chunks, rec2, "", nil)
	if err != nil {
		t.Fatalf("BackfillPackSizes all-domains: %v", err)
	}
	if n2 != len(want) {
		t.Errorf("all-domains recorded count = %d, want %d", n2, len(want))
	}

	// An index that does NOT implement PackSizeRecorder → error, zero recorded.
	nonRecorder := NewMemoryDedupStore()
	if got, err := BackfillPackSizes(ctx, chunks, nonRecorder, "t1", nil); err == nil {
		t.Errorf("expected error for non-recording index, got recorded=%d nil err", got)
	} else if got != 0 {
		t.Errorf("non-recording index recorded=%d, want 0", got)
	}
}
