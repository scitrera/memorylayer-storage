// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// TestDedupSubrangeStreamOrder is a regression test for a silent
// data-corruption bug in the casstore v2 (pack) write path.
//
// Bug: ChunkedStore.putV2 built the manifest's chunk list from two sources at
// two different times — deduped chunks were appended to m.Chunks immediately
// inside emitChunk, while brand-new (packed) chunks were appended later, in a
// batch, when their pack was flushed. When a deduped chunk appeared in the
// MIDDLE of the stream (i.e. it was emitted before some still-buffered new
// chunks were flushed), the manifest recorded the deduped chunk BEFORE those
// new chunks, even though it came after them in the original byte stream. The
// reader concatenates chunks in manifest order, so the reconstructed bytes were
// silently reordered: correct length, no error, wrong content from offset 0.
//
// Trigger conditions (why it was ~50% flaky and only with random content):
//   - Slice 1 = 8 MiB of crypto-random bytes.
//   - Slice 2 = big[1MiB:4MiB], a 3 MiB sub-range starting mid-chunk.
//   - Content-defined chunking makes slice 2's INTERIOR chunk boundaries align
//     with slice 1's, so a slice-2 interior chunk dedups (via the GlobalIndex)
//     against slice 1. That interior deduped chunk is preceded AND followed by
//     new chunks, so the ordering bug reorders the manifest.
//   - Deterministic/repeating content does not produce a mid-stream interior
//     dedup against the other slice, so it never tripped the bug. Random
//     content trips it only on the ~half of seeds where the boundaries align.
//
// Fix: putV2 now appends every chunk's ref to m.Chunks in strict stream order
// at emit time (packed chunks get a placeholder pack hash that flushPack
// back-fills), so the manifest order always matches the byte stream.
func TestDedupSubrangeStreamOrder(t *testing.T) {
	ctx := context.Background()
	for iter := 0; iter < 8; iter++ {
		l := newLocal(t)
		big := make([]byte, 8<<20)
		if _, err := rand.Read(big); err != nil {
			t.Fatal(err)
		}
		if err := l.Store.Write(ctx, 1, big, chunkstore.ClassDefault); err != nil {
			t.Fatalf("write 1: %v", err)
		}
		sub := big[1<<20 : 4<<20] // 3 MiB sub-range starting at 1 MiB
		if err := l.Store.Write(ctx, 2, sub, chunkstore.ClassDefault); err != nil {
			t.Fatalf("write 2: %v", err)
		}

		// Full read of slice 2 must reproduce sub exactly.
		full, err := l.Store.Read(ctx, 2)
		if err != nil {
			t.Fatalf("iter %d Read(2): %v", iter, err)
		}
		if !bytes.Equal(full, sub) {
			t.Errorf("iter %d Read(2) WRONG: firstdiff=%d", iter, firstDiff(full, sub))
		}

		// ReadAt slice 2 at offset 0 must reproduce sub exactly.
		got := make([]byte, 3<<20)
		n, err := l.Store.ReadAt(ctx, 2, 0, got)
		if err != nil {
			t.Fatalf("iter %d ReadAt(2,0): %v", iter, err)
		}
		if !bytes.Equal(got[:n], sub[:n]) {
			t.Errorf("iter %d ReadAt(2,0,n=%d) WRONG: firstdiff=%d", iter, n, firstDiff(got[:n], sub))
		}

		// Slice 1 must still read back correctly (the dedup source object must
		// not be disturbed).
		full1, err := l.Store.Read(ctx, 1)
		if err != nil {
			t.Fatalf("iter %d Read(1): %v", iter, err)
		}
		if !bytes.Equal(full1, big) {
			t.Errorf("iter %d Read(1) WRONG: firstdiff=%d", iter, firstDiff(full1, big))
		}
	}
}

func firstDiff(a, b []byte) int {
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
