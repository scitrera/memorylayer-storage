// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
)

// TestBridge_RefToMlfs_CrossesMlfsChunkBoundary bridges a ref LARGER than one mlfs
// 64 MiB chunk into an mlfs file, exercising the boundary-straddle slice mapping
// (a casstore chunk that spans a 64 MiB boundary is shared whole by two slices via
// their soff/slen windows). It asserts the file reads back byte-identical and that
// bridging it BACK to a ref reproduces the original chunk list — the round trip that
// would break if the straddle mapping split a chunk or mis-set a read window.
func TestBridge_RefToMlfs_CrossesMlfsChunkBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("large-file bridge test skipped in -short")
	}
	stack, counting := newE2EStack(t)
	ctx := context.Background()

	// 70 MiB: > one 64 MiB mlfs chunk, so the file spans two mlfs chunks and at least
	// one casstore chunk straddles the 64 MiB boundary.
	payload := make([]byte, 70*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	src, err := stack.Gateway.Put(ctx, "big/object", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Gateway.Put: %v", err)
	}

	getsBefore, putsBefore := counting.gets.Load(), counting.puts.Load()
	ino, err := stack.Bridge.RefToMlfs(ctx, "big/object", "/big.bin", ClassDefault)
	if err != nil {
		t.Fatalf("RefToMlfs: %v", err)
	}
	if g, p := counting.gets.Load()-getsBefore, counting.puts.Load()-putsBefore; g != 0 || p != 0 {
		t.Fatalf("bridge moved chunk bytes on a boundary-crossing file: gets=%d puts=%d (want 0/0)", g, p)
	}

	// Read the whole file back through the mlfs data path (spans two mlfs chunks).
	files := fileio.New(stack.Engine, stack.SliceStore)
	got := make([]byte, len(payload))
	total := 0
	for total < len(payload) {
		n, err := files.Read(ctx, ino, int64(total), got[total:])
		if err != nil {
			t.Fatalf("Read at %d: %v", total, err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("boundary-crossing file bytes differ (read %d of %d)", total, len(payload))
	}

	// Bridge the mlfs file BACK to a new ref; its chunk list must equal the source
	// ref's — the straddle mapping must be lossless in both directions.
	if st := stack.Engine.SetContentHash(ctx, ino, src.ContentHash); st != 0 {
		t.Fatalf("SetContentHash: errno %v", st)
	}
	back, err := stack.Bridge.MlfsToRef(ctx, "/big.bin", "big/roundtrip", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("MlfsToRef (round trip): %v", err)
	}
	if back.ContentHash != src.ContentHash {
		t.Fatalf("round-trip content hash: got %q want %q", back.ContentHash, src.ContentHash)
	}
	srcChunks, err := stack.Gateway.RefChunks(ctx, "big/object")
	if err != nil {
		t.Fatalf("RefChunks(src): %v", err)
	}
	rtChunks, err := stack.Gateway.RefChunks(ctx, "big/roundtrip")
	if err != nil {
		t.Fatalf("RefChunks(roundtrip): %v", err)
	}
	if len(srcChunks) != len(rtChunks) {
		t.Fatalf("round-trip chunk count: got %d want %d", len(rtChunks), len(srcChunks))
	}
	for i := range srcChunks {
		if srcChunks[i] != rtChunks[i] {
			t.Fatalf("round-trip chunk %d differs: rt=%+v src=%+v", i, rtChunks[i], srcChunks[i])
		}
	}
}
