// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// hashRig is a minimal meta+chunkstore+bridge stack used to drive the FUSE
// Write/Flush/Release handlers directly (no kernel mount needed) and assert the
// whole-file content hash the bridge persists. It mirrors newRig in fuse_test.go
// but lives in the internal test package so it can reach unexported fields.
type hashRig struct {
	e      *meta.Engine
	files  *fileio.Files
	cache  *cache.DiskCache
	bridge *Bridge
}

func newHashRig(t *testing.T) *hashRig {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 9)
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	dir := t.TempDir()
	local, err := chunkstore.NewLocal(filepath.Join(dir, "cas"), "mlfs", 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	dc, err := cache.New(local.Store, cache.Config{Dir: filepath.Join(dir, "cache"), AutoUpload: true})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	files := fileio.New(e, dc)
	t.Cleanup(func() { _ = dc.Close(); _ = local.Chunks.Close(ctx) })
	return &hashRig{e: e, files: files, cache: dc, bridge: New(e, files, dc)}
}

// create makes an empty regular file under root and returns its inode.
func (rg *hashRig) create(t *testing.T, name string) meta.Ino {
	t.Helper()
	ino, _, st := rg.e.Create(context.Background(), meta.RootInode, name, 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create %s: %v", name, st)
	}
	rg.e.OpenRef(ino)
	return ino
}

// write drives the bridge's FUSE Write handler at offset.
func (rg *hashRig) write(t *testing.T, ino meta.Ino, offset int64, data []byte) {
	t.Helper()
	in := &fuse.WriteIn{Offset: uint64(offset), Size: uint32(len(data))}
	in.NodeId = uint64(ino)
	n, st := rg.bridge.Write(nil, in, data)
	if st != fuse.OK || int(n) != len(data) {
		t.Fatalf("bridge.Write: n=%d st=%v", n, st)
	}
}

// flush drives the bridge's FUSE Flush handler (close(2)), which finalizes and
// persists the content hash.
func (rg *hashRig) flush(t *testing.T, ino meta.Ino) {
	t.Helper()
	in := &fuse.FlushIn{}
	in.NodeId = uint64(ino)
	if st := rg.bridge.Flush(nil, in); st != fuse.OK {
		t.Fatalf("bridge.Flush: %v", st)
	}
}

// persisted reads the whole-file hash the bridge stamped on the inode.
func (rg *hashRig) persisted(t *testing.T, ino meta.Ino) string {
	t.Helper()
	h, st := rg.e.ContentHash(context.Background(), ino)
	if st != 0 {
		t.Fatalf("ContentHash: %v", st)
	}
	return h
}

func hexSHA256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// TestBridgeHashSequential is the fast-path case: a sequential write then close
// must persist a content hash equal to sha256 of the bytes — via the incremental
// running digest (no recompute). Verified against crypto/sha256 AND a hardcoded
// vector so the encoding is pinned to blobgw's (lowercase hex, no prefix).
func TestBridgeHashSequential(t *testing.T) {
	rg := newHashRig(t)

	// Hardcoded vector: sha256("abc").
	const wantABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	ino := rg.create(t, "abc")
	rg.write(t, ino, 0, []byte("abc"))
	rg.flush(t, ino)
	if got := rg.persisted(t, ino); got != wantABC {
		t.Fatalf("seq hash = %q, want %q (vector)", got, wantABC)
	}

	// Larger sequential write in several chunks (still all at the watermark → fast
	// path), compared to crypto/sha256 of the concatenation.
	ino2 := rg.create(t, "seq")
	data := randBytes(t, 5<<20)
	off := int64(0)
	for off < int64(len(data)) {
		end := off + (1 << 20)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		rg.write(t, ino2, off, data[off:end])
		off = end
	}
	rg.flush(t, ino2)
	if got, want := rg.persisted(t, ino2), hexSHA256(data); got != want {
		t.Fatalf("seq multi-write hash = %q, want %q", got, want)
	}
}

// TestBridgeHashRandomRecompute is the fallback case: a behind-watermark
// overwrite invalidates the running digest, so Flush must recompute the whole
// file and still persist the correct FINAL-content hash — identical to what the
// sequential path would produce for the same final bytes.
func TestBridgeHashRandomRecompute(t *testing.T) {
	rg := newHashRig(t)
	ino := rg.create(t, "rand")

	final := randBytes(t, 3<<20)
	// Tail first, then head (out of order), then a behind-watermark overwrite.
	rg.write(t, ino, 1<<20, final[1<<20:])
	rg.write(t, ino, 0, final[:1<<20])
	over := randBytes(t, 512<<10)
	copy(final[1<<20:], over)
	rg.write(t, ino, 1<<20, over)

	rg.flush(t, ino)
	if got, want := rg.persisted(t, ino), hexSHA256(final); got != want {
		t.Fatalf("recompute hash = %q, want %q", got, want)
	}
}

// TestBridgeHashSurvivesReopen confirms the persisted hash outlives a "restart":
// after Flush stamps it, a fresh Engine over the same DB reads the same value —
// it lives on the inode, not in the per-open accumulator.
func TestBridgeHashSurvivesReopen(t *testing.T) {
	db := testpg.DB(t)
	e := meta.Open(db, 9)
	ctx := context.Background()
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	dir := t.TempDir()
	local, err := chunkstore.NewLocal(filepath.Join(dir, "cas"), "mlfs", 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	dc, err := cache.New(local.Store, cache.Config{Dir: filepath.Join(dir, "cache"), AutoUpload: true})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	t.Cleanup(func() { _ = dc.Close(); _ = local.Chunks.Close(ctx) })
	b := New(e, fileio.New(e, dc), dc)

	ino, _, st := e.Create(ctx, meta.RootInode, "persist", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	e.OpenRef(ino)
	data := randBytes(t, 1<<20)
	win := &fuse.WriteIn{Offset: 0, Size: uint32(len(data))}
	win.NodeId = uint64(ino)
	if n, st := b.Write(nil, win, data); st != fuse.OK || int(n) != len(data) {
		t.Fatalf("write: n=%d st=%v", n, st)
	}
	fin := &fuse.FlushIn{}
	fin.NodeId = uint64(ino)
	if st := b.Flush(nil, fin); st != fuse.OK {
		t.Fatalf("flush: %v", st)
	}

	// Fresh engine over the same DB — the running state is gone; only the column
	// remains.
	e2 := meta.Open(db, 9)
	got, st := e2.ContentHash(ctx, ino)
	if st != 0 {
		t.Fatalf("reopened ContentHash: %v", st)
	}
	if want := hexSHA256(data); got != want {
		t.Fatalf("after reopen hash = %q, want %q", got, want)
	}
}

// TestBridgeHashDisabled confirms the natural off-gate: with hashing disabled the
// write path skips all hashing and Flush persists nothing (the column stays
// empty), so the feature adds zero cost when off.
func TestBridgeHashDisabled(t *testing.T) {
	rg := newHashRig(t)
	rg.bridge.hashDisabled = true
	ino := rg.create(t, "off")
	rg.write(t, ino, 0, []byte("abc"))
	rg.flush(t, ino)
	if got := rg.persisted(t, ino); got != "" {
		t.Fatalf("disabled: hash = %q, want empty", got)
	}
}
