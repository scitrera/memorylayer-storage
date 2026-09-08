// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// TestCompact_ConsolidatesSmallPacks is the repack fix: many tiny per-write packs
// (the legacy ~128 KB streaming-write pattern) are coalesced into ~pack-target
// packs, every object still reads back byte-identical, and the old packs become
// orphaned so GC reclaims them.
func TestCompact_ConsolidatesSmallPacks(t *testing.T) {
	const packTarget = 1 << 20 // 1 MiB
	cs, _, chunks := chunkedTestStackWithPackSize(t, packTarget)
	ctx := context.Background()

	const n = 20
	const sz = 200 * 1024 // each Put → one ~200 KiB pack (< target) → n tiny packs
	want := make([][]byte, n)
	keys := make([]SnapshotKey, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		want[i] = b
		keys[i] = SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}
		if _, err := cs.Put(ctx, keys[i], SnapshotMetadata{}, bytes.NewReader(b)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	before, err := countTenantBlobs(ctx, chunks, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if before < n {
		t.Fatalf("expected ≥%d small packs before compaction, got %d", n, before)
	}

	// Compact: the 200 KiB packs are "small" (< 1 MiB), so they're all selected
	// and coalesced into ~1 MiB packs.
	res, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: packTarget})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.PacksSelected != n {
		t.Errorf("PacksSelected = %d, want %d", res.PacksSelected, n)
	}
	if res.ManifestsRewritten != n {
		t.Errorf("ManifestsRewritten = %d, want %d", res.ManifestsRewritten, n)
	}
	if res.PacksWritten >= n {
		t.Errorf("no consolidation: wrote %d packs for %d objects", res.PacksWritten, n)
	}

	// Every object still reads back byte-identical (now from the new packs).
	for i := 0; i < n; i++ {
		rc, _, err := cs.GetLatest(ctx, keys[i])
		if err != nil {
			t.Fatalf("GetLatest %d: %v", i, err)
		}
		got, rerr := io.ReadAll(rc)
		rc.Close()
		if rerr != nil {
			t.Fatalf("ReadAll %d: %v", i, rerr)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("object %d round-trip mismatch after compaction (%d vs %d bytes, first-diff %d)",
				i, len(got), len(want[i]), firstDiffOffset(got, want[i]))
		}
	}

	// The old packs are now orphaned — a GC pass reclaims them, leaving only the
	// consolidated packs.
	gc := NewChunkedGC(cs.upstream, chunks, slog.Default())
	gc.SafetyWindow = 0
	if _, err := gc.RunOnceForDomain(ctx, "t1"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	after, err := countTenantBlobs(ctx, chunks, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if after != res.PacksWritten {
		t.Errorf("after GC: %d packs remain, want %d (the consolidated set)", after, res.PacksWritten)
	}
	if after >= before {
		t.Errorf("compaction+GC did not reduce pack count: before=%d after=%d", before, after)
	}

	// And reads STILL work after the old packs are gone (proves the rewrite, not
	// surviving old blobs, is serving the data).
	for i := 0; i < n; i++ {
		rc, _, err := cs.GetLatest(ctx, keys[i])
		if err != nil {
			t.Fatalf("post-GC GetLatest %d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("object %d mismatch after GC reclaimed old packs", i)
		}
	}
}

// TestCompact_CompressedPacks exercises compaction over ZSTD-compressed source
// packs — the mlfs local-mode posture (the uncompressed stack does not cover the
// fetch→decompress→re-slice path). Consolidated packs are written uncompressed,
// so reads must still round-trip byte-identical across the compression boundary,
// before AND after GC reclaims the old compressed packs.
func TestCompact_CompressedPacks(t *testing.T) {
	const packTarget = 1 << 20 // 1 MiB
	cs, _, chunks := compressedChunkedStackWithPackSize(t, packTarget)
	ctx := context.Background()

	const n = 12
	const sz = 200 * 1024 // each Put → one small (< target) compressed pack
	want := make([][]byte, n)
	keys := make([]SnapshotKey, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil { // incompressible → still zstd-framed
			t.Fatal(err)
		}
		want[i] = b
		keys[i] = SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}
		if _, err := cs.Put(ctx, keys[i], SnapshotMetadata{}, bytes.NewReader(b)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	before, err := countTenantBlobs(ctx, chunks, "t1")
	if err != nil {
		t.Fatal(err)
	}

	res, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: packTarget})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.PacksSelected != n || res.ManifestsRewritten != n {
		t.Errorf("selected=%d rewritten=%d, want %d each", res.PacksSelected, res.ManifestsRewritten, n)
	}
	if res.PacksWritten >= n {
		t.Errorf("no consolidation: wrote %d packs for %d objects", res.PacksWritten, n)
	}

	// Byte-identical across the compressed→uncompressed boundary.
	for i := 0; i < n; i++ {
		rc, _, err := cs.GetLatest(ctx, keys[i])
		if err != nil {
			t.Fatalf("GetLatest %d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("object %d mismatch after compaction (first-diff %d)", i, firstDiffOffset(got, want[i]))
		}
	}

	// GC reclaims the orphaned compressed packs; reads STILL work from the new ones.
	gc := NewChunkedGC(cs.upstream, chunks, slog.Default())
	gc.SafetyWindow = 0
	if _, err := gc.RunOnceForDomain(ctx, "t1"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	after, err := countTenantBlobs(ctx, chunks, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if after != res.PacksWritten || after >= before {
		t.Errorf("post-GC packs=%d, want %d (< before=%d)", after, res.PacksWritten, before)
	}
	for i := 0; i < n; i++ {
		rc, _, err := cs.GetLatest(ctx, keys[i])
		if err != nil {
			t.Fatalf("post-GC GetLatest %d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("object %d mismatch after GC reclaimed old compressed packs", i)
		}
	}
}

// TestCompact_Idempotent confirms a second pass over already-consolidated packs
// is a no-op (nothing left under the threshold).
func TestCompact_Idempotent(t *testing.T) {
	const packTarget = 1 << 20
	cs, _, _ := chunkedTestStackWithPackSize(t, packTarget)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		b := make([]byte, 200*1024)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		if _, err := cs.Put(ctx, SnapshotKey{Tenant: "t1", OwnerKey: fmt.Sprintf("s/%d", i)}, SnapshotMetadata{}, bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: packTarget}); err != nil {
		t.Fatalf("Compact 1: %v", err)
	}
	// After consolidation the packs are ~1 MiB; a second pass with the same
	// threshold finds nothing small/under-filled to do.
	res2, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: packTarget})
	if err != nil {
		t.Fatalf("Compact 2: %v", err)
	}
	if res2.PacksWritten != 0 || res2.ManifestsRewritten != 0 {
		t.Errorf("second compaction was not a no-op: %+v", res2)
	}
}
