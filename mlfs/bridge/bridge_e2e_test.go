// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync/atomic"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/manifeststore"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// countingBlobStore wraps a chunk blobstore and counts data-moving ops (PutBlob /
// GetBlob). Manifests live in Postgres (manifeststore), never here, so ANY count
// during a bridge operation is chunk-byte movement — which the bridge must NOT do.
// All other Storage methods are forwarded via embedding.
type countingBlobStore struct {
	blobstore.Storage
	gets atomic.Int64
	puts atomic.Int64
}

func (c *countingBlobStore) GetBlob(ctx context.Context, id blobstore.ID, off, length int64, out blobstore.OutputBuffer) error {
	c.gets.Add(1)
	return c.Storage.GetBlob(ctx, id, off, length, out)
}

func (c *countingBlobStore) PutBlob(ctx context.Context, id blobstore.ID, data blobstore.Bytes, opts blobstore.PutOptions) error {
	c.puts.Add(1)
	return c.Storage.PutBlob(ctx, id, data, opts)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// newE2EStack builds a shared-PG bridge stack over an instrumented chunk store so a
// test can assert zero chunk I/O across a bridge op. It skips without a test PG.
func newE2EStack(t *testing.T) (*Stack, *countingBlobStore) {
	t.Helper()
	ctx := context.Background()
	db := testpg.DB(t)
	local, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	counting := &countingBlobStore{Storage: local}
	stack, err := NewStack(ctx, StackConfig{
		DB:              db,
		Domain:          "tenant-x",
		PackTargetBytes: 1 * 1024 * 1024, // small packs so a multi-MiB file spans many
		Region:          9,
		Chunks:          counting,
	})
	if err != nil {
		t.Fatalf("NewStack: %v", err)
	}
	t.Cleanup(func() { _ = stack.Close(ctx) })
	return stack, counting
}

// TestBridge_MlfsToRef_ZeroByteMovement writes a real mlfs file (through the data
// path), bridges it to a blob_ref, and asserts: the ref reads back byte-identical
// via the blobgw gateway, ZERO chunk GET/PUT happened during the bridge op, and the
// ref references the SAME casstore chunks as the mlfs file.
func TestBridge_MlfsToRef_ZeroByteMovement(t *testing.T) {
	stack, counting := newE2EStack(t)
	ctx := context.Background()

	// Write a multi-MiB file through the mlfs data path so it is genuinely chunked
	// and packed in the tenant domain.
	payload := make([]byte, 5*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	wantHash := sha256Hex(payload)

	files := fileio.New(stack.Engine, mustSliceStore(t, stack))
	ino, _, st := stack.Engine.Create(ctx, meta.RootInode, "model.bin", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("Create: errno %v", st)
	}
	if _, err := files.Write(ctx, ino, 0, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Stamp the whole-file hash (C0: mlfs does this on flush/close).
	if st := stack.Engine.SetContentHash(ctx, ino, wantHash); st != 0 {
		t.Fatalf("SetContentHash: errno %v", st)
	}

	// Snapshot the I/O counters, then bridge — the bridge op must move zero bytes.
	getsBefore, putsBefore := counting.gets.Load(), counting.puts.Load()
	info, err := stack.Bridge.MlfsToRef(ctx, "/model.bin", "bridged/model", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("MlfsToRef: %v", err)
	}
	if g, p := counting.gets.Load()-getsBefore, counting.puts.Load()-putsBefore; g != 0 || p != 0 {
		t.Fatalf("bridge mlfs→ref moved chunk bytes: gets=%d puts=%d (want 0/0)", g, p)
	}
	if info.ContentHash != wantHash {
		t.Fatalf("ref content hash: got %q want %q", info.ContentHash, wantHash)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("ref size: got %d want %d", info.Size, len(payload))
	}

	// Reading the ref through blobgw yields the original bytes (GETs are fine here —
	// this is a real read, not the bridge op).
	rc, got, err := stack.Gateway.Get(ctx, "bridged/model")
	if err != nil {
		t.Fatalf("Gateway.Get: %v", err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("ref bytes differ from mlfs file (got %d want %d)", len(body), len(payload))
	}
	if got.ContentHash != wantHash {
		t.Fatalf("Get content hash: got %q want %q", got.ContentHash, wantHash)
	}

	// The ref and the mlfs file reference the SAME casstore chunks (dedup preserved).
	mlfsChunks, err := stack.FS.FileChunks(ctx, ino)
	if err != nil {
		t.Fatalf("FileChunks: %v", err)
	}
	refChunks, err := stack.Gateway.RefChunks(ctx, "bridged/model")
	if err != nil {
		t.Fatalf("RefChunks: %v", err)
	}
	if len(mlfsChunks) != len(refChunks) || len(refChunks) == 0 {
		t.Fatalf("chunk count mismatch: mlfs=%d ref=%d", len(mlfsChunks), len(refChunks))
	}
	for i := range mlfsChunks {
		if mlfsChunks[i] != refChunks[i] {
			t.Fatalf("chunk %d differs: mlfs=%+v ref=%+v", i, mlfsChunks[i], refChunks[i])
		}
	}
}

// TestBridge_RefToMlfs_ZeroByteMovement puts an object through blobgw, bridges it to
// a new mlfs file, and asserts: the mlfs file reads back byte-identical (through the
// mlfs data path), ZERO chunk GET/PUT happened during the bridge op, and the new
// inode references the SAME chunks as the ref.
func TestBridge_RefToMlfs_ZeroByteMovement(t *testing.T) {
	stack, counting := newE2EStack(t)
	ctx := context.Background()

	payload := make([]byte, 4*1024*1024+123) // non-round size, multi-chunk
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	wantHash := sha256Hex(payload)

	// Ingest through blobgw (real chunk PUTs happen here, before the bridge op).
	if _, err := stack.Gateway.Put(ctx, "src/object", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Gateway.Put: %v", err)
	}

	getsBefore, putsBefore := counting.gets.Load(), counting.puts.Load()
	ino, err := stack.Bridge.RefToMlfs(ctx, "src/object", "/restored.bin", ClassDefault)
	if err != nil {
		t.Fatalf("RefToMlfs: %v", err)
	}
	if g, p := counting.gets.Load()-getsBefore, counting.puts.Load()-putsBefore; g != 0 || p != 0 {
		t.Fatalf("bridge ref→mlfs moved chunk bytes: gets=%d puts=%d (want 0/0)", g, p)
	}

	// The new mlfs file's content hash + length match the ref.
	gotHash, gotSize, err := stack.FS.ContentHashAndSize(ctx, ino)
	if err != nil {
		t.Fatalf("ContentHashAndSize: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("mlfs content hash: got %q want %q", gotHash, wantHash)
	}
	if gotSize != int64(len(payload)) {
		t.Fatalf("mlfs size: got %d want %d", gotSize, len(payload))
	}

	// Read the new mlfs file through the data path — byte-identical to the ref.
	files := fileio.New(stack.Engine, mustSliceStore(t, stack))
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
		t.Fatalf("mlfs file bytes differ from ref (read %d of %d)", total, len(payload))
	}
	// Recompute the hash through the data path to be doubly sure the overlay resolves
	// correctly.
	if h := sha256Hex(got); h != wantHash {
		t.Fatalf("mlfs read-back hash: got %q want %q", h, wantHash)
	}

	// The new inode references the SAME chunks as the ref.
	refChunks, err := stack.Gateway.RefChunks(ctx, "src/object")
	if err != nil {
		t.Fatalf("RefChunks: %v", err)
	}
	mlfsChunks, err := stack.FS.FileChunks(ctx, ino)
	if err != nil {
		t.Fatalf("FileChunks: %v", err)
	}
	if len(mlfsChunks) != len(refChunks) || len(mlfsChunks) == 0 {
		t.Fatalf("chunk count mismatch: mlfs=%d ref=%d", len(mlfsChunks), len(refChunks))
	}
	for i := range refChunks {
		if mlfsChunks[i] != refChunks[i] {
			t.Fatalf("chunk %d differs: mlfs=%+v ref=%+v", i, mlfsChunks[i], refChunks[i])
		}
	}
}

// mustSliceStore returns the mlfs chunk store the data path uses, over the SAME
// shared casstore the bridge FS uses (so a slice the bridge registers is readable by
// fileio).
func mustSliceStore(t *testing.T, s *Stack) *chunkstore.CasStore {
	t.Helper()
	return s.SliceStore
}

// TestStack_AssertGCCoversAllManifests_PassesForNewStack confirms that a Stack
// built by NewStack always satisfies AssertGCCoversAllManifests — the structural
// guarantee that the GC walks the same manifest store as both sides.
func TestStack_AssertGCCoversAllManifests_PassesForNewStack(t *testing.T) {
	stack, _ := newE2EStack(t)
	if err := stack.AssertGCCoversAllManifests(); err != nil {
		t.Fatalf("NewStack produced a Stack that fails AssertGCCoversAllManifests: %v", err)
	}
}

// TestStack_AssertGCCoversAllManifests_FailsForSplitManifest is the regression test
// for fix #3: a Stack whose GC is wired with a DIFFERENT manifest store than the one
// backing both ChunkedStores must be detected and refused. Without this guard a GC
// pass that walks only one store would silently reclaim packs the other side
// references — data loss with no error signal.
//
// We simulate the split by directly setting gcManifest to a different store pointer
// (the bridge package's own test file can access unexported fields).
func TestStack_AssertGCCoversAllManifests_FailsForSplitManifest(t *testing.T) {
	stack, _ := newE2EStack(t)

	// Sabotage: replace gcManifest with a different store pointer (simulating what
	// a hand-assembled Stack with split manifest stores would look like).
	db := testpg.DB(t)
	otherManifest := manifeststore.New(db, nil)
	stack.gcManifest = otherManifest

	if err := stack.AssertGCCoversAllManifests(); err == nil {
		t.Fatal("expected AssertGCCoversAllManifests to return ErrSplitManifestStore for a split-manifest Stack, but got nil")
	}
}
