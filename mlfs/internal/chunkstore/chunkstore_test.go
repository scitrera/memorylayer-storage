// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

const testDomain = "mlfs"

// dataSize is large enough to span several casstore packs (default ~16 MiB) so
// the dedup + multi-pack paths are exercised. The same code path scales to the
// plan's 1 GiB acceptance size.
const dataSize = 64 << 20 // 64 MiB

func newLocal(t *testing.T) *chunkstore.Local {
	t.Helper()
	l, err := chunkstore.NewLocal(t.TempDir(), testDomain, 0)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	t.Cleanup(func() { _ = l.Chunks.Close(context.Background()) })
	return l
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func countBlobs(t *testing.T, chunks blobstore.Storage) int {
	t.Helper()
	n := 0
	for _, p := range []blobstore.ID{
		blobstore.ID("chunk-" + testDomain + "-"),
		blobstore.ID("pack-" + testDomain + "-"),
	} {
		if err := chunks.ListBlobs(context.Background(), p, func(blobstore.Metadata) error { n++; return nil }); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return n
}

func TestRoundTripSHA256(t *testing.T) {
	l := newLocal(t)
	ctx := context.Background()
	data := randomBytes(t, dataSize)
	want := sha256.Sum256(data)

	if err := l.Store.Write(ctx, 1, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := l.Store.Read(ctx, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if sha256.Sum256(got) != want {
		t.Fatalf("round-trip sha256 mismatch (%d vs %d bytes)", len(got), len(data))
	}

	// Ranged read in the middle.
	const off, ln = 30 << 20, 1 << 20
	buf := make([]byte, ln)
	n, err := l.Store.ReadAt(ctx, 1, off, buf)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != ln || !bytes.Equal(buf, data[off:off+ln]) {
		t.Errorf("ReadAt mismatch: n=%d", n)
	}
}

// TestDedupSameContent is the load-bearing acceptance: writing the same data
// under a DISTINCT slice id adds no physical blobs (casstore dedups the
// underlying chunks across the two slice keys) — "write the same data twice
// with no second-pass uploads".
func TestDedupSameContent(t *testing.T) {
	l := newLocal(t)
	ctx := context.Background()
	data := randomBytes(t, dataSize)

	if err := l.Store.Write(ctx, 1, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	blobsAfterFirst := countBlobs(t, l.Chunks)
	if blobsAfterFirst == 0 {
		t.Fatal("expected blobs after first write")
	}

	if err := l.Store.Write(ctx, 2, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	blobsAfterSecond := countBlobs(t, l.Chunks)
	if blobsAfterSecond != blobsAfterFirst {
		t.Errorf("dedup failed: identical data under a new slice grew blobs %d → %d", blobsAfterFirst, blobsAfterSecond)
	}

	// Both slices still read back correctly.
	for _, id := range []uint64{1, 2} {
		got, err := l.Store.Read(ctx, id)
		if err != nil || !bytes.Equal(got, data) {
			t.Errorf("slice %d read-back failed: err=%v equal=%v", id, err, bytes.Equal(got, data))
		}
	}
}

func TestDistinctContentGrows(t *testing.T) {
	l := newLocal(t)
	ctx := context.Background()
	if err := l.Store.Write(ctx, 1, randomBytes(t, dataSize), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	first := countBlobs(t, l.Chunks)
	if err := l.Store.Write(ctx, 2, randomBytes(t, dataSize), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if countBlobs(t, l.Chunks) <= first {
		t.Errorf("distinct content should add blobs (was %d)", first)
	}
}

func TestExistsAndRemoveGC(t *testing.T) {
	l := newLocal(t)
	ctx := context.Background()
	data := randomBytes(t, 8<<20) // distinct, unshared content
	if err := l.Store.Write(ctx, 42, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if ok, err := l.Store.Exists(ctx, 42); err != nil || !ok {
		t.Fatalf("Exists(42): ok=%v err=%v", ok, err)
	}
	if ok, _ := l.Store.Exists(ctx, 9999); ok {
		t.Errorf("Exists(9999) should be false")
	}

	before := countBlobs(t, l.Chunks)
	if before == 0 {
		t.Fatal("expected blobs")
	}
	if err := l.Store.Remove(ctx, 42); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if ok, _ := l.Store.Exists(ctx, 42); ok {
		t.Errorf("Exists(42) should be false after Remove")
	}
	// GC reclaims the now-orphaned chunks.
	if _, err := l.GC.RunOnce(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if after := countBlobs(t, l.Chunks); after != 0 {
		t.Errorf("expected 0 blobs after Remove + GC, got %d", after)
	}
}
