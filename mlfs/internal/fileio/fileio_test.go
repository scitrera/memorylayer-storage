// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

const domain = "mlfs"

type stack struct {
	f     *fileio.Files
	e     *meta.Engine
	local *chunkstore.Local
}

func openStack(t *testing.T) *stack {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 3)
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	local, err := chunkstore.NewLocal(t.TempDir(), domain, 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	t.Cleanup(func() { _ = local.Chunks.Close(ctx) })
	return &stack{f: fileio.New(e, local.Store), e: e, local: local}
}

func (s *stack) createFile(t *testing.T, name string) meta.Ino {
	t.Helper()
	ino, _, st := s.e.Create(context.Background(), meta.RootInode, name, 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("Create %s: %v", name, st)
	}
	return ino
}

func (s *stack) countBlobs(t *testing.T) int {
	t.Helper()
	n := 0
	for _, p := range []blobstore.ID{blobstore.ID("chunk-" + domain + "-"), blobstore.ID("pack-" + domain + "-")} {
		if err := s.local.Chunks.ListBlobs(context.Background(), p, func(blobstore.Metadata) error { n++; return nil }); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return n
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func readAll(t *testing.T, s *stack, ino meta.Ino, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	n, err := s.f.Read(context.Background(), ino, 0, buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return buf[:n]
}

func TestRoundTrip100MiB(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "big")
	data := randomBytes(t, 100<<20) // spans 2 chunks (64 MiB each)
	if n, err := s.f.Write(ctx, ino, 0, data, chunkstore.ClassDefault); err != nil || n != len(data) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	got := readAll(t, s, ino, len(data))
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("100MiB round-trip sha256 mismatch (%d bytes)", len(got))
	}
	// Ranged read across the chunk boundary.
	buf := make([]byte, 1<<20)
	rn, err := s.f.Read(ctx, ino, (64<<20)-512<<10, buf) // straddles 64 MiB boundary
	if err != nil || rn != len(buf) {
		t.Fatalf("ranged read: n=%d err=%v", rn, err)
	}
	if !bytes.Equal(buf, data[(64<<20)-512<<10:(64<<20)+512<<10]) {
		t.Error("ranged read across chunk boundary mismatch")
	}
}

func TestOverwriteNewerWins(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "ow")
	base := bytes.Repeat([]byte{'A'}, 1<<20)
	s.f.Write(ctx, ino, 0, base, chunkstore.ClassDefault)
	over := bytes.Repeat([]byte{'B'}, 256<<10)
	if _, err := s.f.Write(ctx, ino, 256<<10, over, chunkstore.ClassDefault); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	got := readAll(t, s, ino, 1<<20)
	want := append([]byte(nil), base...)
	copy(want[256<<10:], over)
	if !bytes.Equal(got, want) {
		t.Error("newer write did not win in overwritten region")
	}
}

func TestSparseHoles(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "sparse")
	s.f.Write(ctx, ino, 0, []byte("hello"), chunkstore.ClassDefault)
	const gap = 10 << 20
	s.f.Write(ctx, ino, gap, []byte("world"), chunkstore.ClassDefault)
	got := readAll(t, s, ino, gap+5)
	if string(got[:5]) != "hello" || string(got[gap:gap+5]) != "world" {
		t.Errorf("sparse data wrong at ends")
	}
	for i := 5; i < gap; i++ {
		if got[i] != 0 {
			t.Fatalf("hole byte %d not zero", i)
		}
	}
}

func TestTruncate(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "trunc")
	data := randomBytes(t, 1<<20)
	s.f.Write(ctx, ino, 0, data, chunkstore.ClassDefault)

	if err := s.f.Truncate(ctx, ino, 512<<10); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	got := readAll(t, s, ino, 1<<20)
	if len(got) != 512<<10 || !bytes.Equal(got, data[:512<<10]) {
		t.Errorf("after shrink: len=%d", len(got))
	}
	if err := s.f.Truncate(ctx, ino, 2<<20); err != nil {
		t.Fatalf("grow: %v", err)
	}
	got = readAll(t, s, ino, 2<<20)
	if len(got) != 2<<20 || !bytes.Equal(got[:512<<10], data[:512<<10]) {
		t.Errorf("after grow: len=%d", len(got))
	}
	for i := 512 << 10; i < 2<<20; i++ {
		if got[i] != 0 {
			t.Fatalf("grown tail byte %d not zero", i)
		}
	}
}

// TestCloneCoW is the copy-on-write acceptance: cloning a file reads identical
// bytes and adds NO physical blobs (chunks shared), and many clones stay flat.
func TestCloneCoW(t *testing.T) {
	s := openStack(t)
	ctx, c := context.Background(), meta.Background()
	src := s.createFile(t, "src")
	data := randomBytes(t, 16<<20)
	s.f.Write(ctx, src, 0, data, chunkstore.ClassDefault)

	blobsBefore := s.countBlobs(t)
	if blobsBefore == 0 {
		t.Fatal("expected blobs after writing src")
	}

	// Clone src into 50 fresh files; bytes must match, blobs must not grow.
	for i := 0; i < 50; i++ {
		dst, _, st := s.e.Create(ctx, meta.RootInode, fmt.Sprintf("clone-%d", i), 0o644, c)
		if st != 0 {
			t.Fatalf("Create clone %d: %v", i, st)
		}
		if err := s.f.Clone(ctx, src, dst); err != nil {
			t.Fatalf("Clone %d: %v", i, err)
		}
		if i < 3 { // spot-check a few for byte-equality
			got := readAll(t, s, dst, len(data))
			if !bytes.Equal(got, data) {
				t.Fatalf("clone %d bytes differ from src", i)
			}
		}
	}
	if after := s.countBlobs(t); after != blobsBefore {
		t.Errorf("CoW added physical blobs: %d → %d (50 clones should share)", blobsBefore, after)
	}
}

// TestCopyFileRangeCoW exercises the copy_file_range data path directly: a
// chunk-aligned, multi-chunk range copies via chunk sharing (no new blobs),
// while a misaligned range falls back to a byte-correct read+write copy.
//
// The payload is crypto-random: a misaligned copy re-stores a sub-range whose
// content-defined chunk boundaries align with the source's, so the sub-range's
// interior chunks dedup against the source. This is exactly the path that used
// to silently corrupt reads (casstore putV2 manifest-ordering bug, fixed in
// casstore/snapshot/store_chunked.go and covered by
// TestGlobalIndex_PartialMidStreamDedupPreservesOrder + the chunkstore
// TestDedupSubrangeStreamOrder regression). Random content asserts the fix end
// to end through the fileio layer.
func TestCopyFileRangeCoW(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	src := s.createFile(t, "src")
	const cs = int64(meta.ChunkSize)
	data := randomBytes(t, int(2*cs)) // exactly two chunks of random content
	if _, err := s.f.Write(ctx, src, 0, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("write src: %v", err)
	}
	blobsBefore := s.countBlobs(t)
	if blobsBefore == 0 {
		t.Fatal("expected blobs after writing src")
	}

	// Aligned, whole-file (2-chunk) copy → chunk sharing, no new blobs.
	dst := s.createFile(t, "dst")
	n, err := s.f.CopyFileRange(ctx, src, dst, 0, 0, int(2*cs))
	if err != nil || n != int(2*cs) {
		t.Fatalf("aligned CopyFileRange: n=%d err=%v", n, err)
	}
	if got := readAll(t, s, dst, len(data)); !bytes.Equal(got, data) {
		t.Fatal("aligned copy bytes differ from src")
	}
	if after := s.countBlobs(t); after != blobsBefore {
		t.Errorf("aligned copy_file_range added blobs: %d → %d (should share)", blobsBefore, after)
	}

	// Past-EOF copy is clamped to src length (returns 0 once at/after EOF).
	if n, err := s.f.CopyFileRange(ctx, src, dst, 2*cs, 0, 1<<20); err != nil || n != 0 {
		t.Fatalf("past-EOF copy: n=%d err=%v (want 0)", n, err)
	}

	// Misaligned range (mid-chunk offsets + partial length): falls back to
	// read+write but must still be byte-correct.
	dst2 := s.createFile(t, "dst2")
	const off = int64(1 << 20)
	const length = 3 << 20
	m, err := s.f.CopyFileRange(ctx, src, dst2, off, off, length)
	if err != nil || m != length {
		t.Fatalf("misaligned CopyFileRange: n=%d err=%v", m, err)
	}
	got := make([]byte, length)
	rn, err := s.f.Read(ctx, dst2, off, got)
	if err != nil || rn != length {
		t.Fatalf("read dst2: n=%d err=%v", rn, err)
	}
	if !bytes.Equal(got, data[off:off+length]) {
		t.Error("misaligned copy bytes differ from src range")
	}
}
