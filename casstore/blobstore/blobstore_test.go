// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

func newTestStorage(t *testing.T) blobstore.Storage {
	t.Helper()
	st, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{
		Root: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	return st
}

// putBytes wraps a byte slice in the Bytes interface kopia's PutBlob expects.
// We use our own minimal implementation instead of kopia's internal gather
// package (which Go forbids us from importing outside its own module).
func putBytes(b []byte) blobstore.Bytes { return blobstore.BytesFromSlice(b) }

// getBytes reads a blob fully into memory for assertion.
func getBytes(t *testing.T, ctx context.Context, st blobstore.Storage, id blobstore.ID) []byte {
	t.Helper()
	buf := blobstore.NewOutputBuffer()
	if err := st.GetBlob(ctx, id, 0, -1, buf); err != nil {
		t.Fatalf("GetBlob(%q): %v", id, err)
	}
	return buf.Bytes()
}

func TestLocalStorageRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newTestStorage(t)

	const id = blobstore.ID("test/round-trip")
	payload := []byte("hello, blob store")

	if err := st.PutBlob(ctx, id, putBytes(payload), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	got := getBytes(t, ctx, st, id)
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %q, want %q", got, payload)
	}

	md, err := st.GetMetadata(ctx, id)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if md.Length != int64(len(payload)) {
		t.Errorf("Length = %d, want %d", md.Length, len(payload))
	}
	if md.Timestamp.IsZero() {
		t.Error("Timestamp must be non-zero (mark-and-sweep GC depends on this)")
	}
}

func TestPutIfAbsent_AdapterHandlesBothBackends(t *testing.T) {
	// PutIfAbsent is the adapter-level CAS primitive that hides the fact
	// that kopia's filesystem backend doesn't natively support
	// DoNotRecreate while S3 does. Either way the contract must hold:
	// first writer wins, subsequent writers no-op, blob contents preserved.
	ctx := context.Background()
	st := newTestStorage(t)

	const id = blobstore.ID("test/cas")
	first := []byte("first writer")
	second := []byte("second writer")

	if err := blobstore.PutIfAbsent(ctx, st, id, putBytes(first)); err != nil {
		t.Fatalf("initial PutIfAbsent: %v", err)
	}
	// Second writer to the same key must no-op (return nil, leave blob alone).
	if err := blobstore.PutIfAbsent(ctx, st, id, putBytes(second)); err != nil {
		t.Errorf("second PutIfAbsent should be a no-op, got %v", err)
	}
	// Contents from the first writer must be intact.
	got := getBytes(t, ctx, st, id)
	if !bytes.Equal(got, first) {
		t.Errorf("blob clobbered despite PutIfAbsent; got %q, want %q", got, first)
	}
}

func TestGetMetadataNotFound(t *testing.T) {
	ctx := context.Background()
	st := newTestStorage(t)

	_, err := st.GetMetadata(ctx, blobstore.ID("does/not/exist"))
	if !errors.Is(err, blobstore.ErrBlobNotFound) {
		t.Errorf("expected ErrBlobNotFound, got %v", err)
	}
}

func TestListBlobsCallback(t *testing.T) {
	// ListBlobs is our Walk primitive — needed for mark-and-sweep GC.
	// Confirm it streams via callback, supports prefix filter, and yields
	// each blob exactly once.
	ctx := context.Background()
	st := newTestStorage(t)

	// Kopia's filesystem-backend Lister treats the BlobID as a flat string
	// in the kopia-internal namespace. The default sharded layout splits
	// blobs by the first N characters of the ID, so a prefix that crosses
	// a shard boundary (e.g. "chunks/aa" when keys are "chunks/aabb...")
	// requires care. Use a flat hash-style ID space — the ChunkedStore
	// will do this naturally because chunk IDs are hex hashes.
	want := []blobstore.ID{
		"aa11111111111111",
		"aa22222222222222",
		"bb33333333333333",
		"manifest-keep",
	}
	for _, id := range want {
		if err := st.PutBlob(ctx, id, putBytes([]byte("x")), blobstore.PutOptions{}); err != nil {
			t.Fatalf("seed PutBlob(%q): %v", id, err)
		}
	}

	// Listing with prefix "aa" should yield only the two aa blobs.
	seen := map[blobstore.ID]bool{}
	err := st.ListBlobs(ctx, blobstore.ID("aa"), func(md blobstore.Metadata) error {
		if seen[md.BlobID] {
			t.Errorf("duplicate yield: %q", md.BlobID)
		}
		seen[md.BlobID] = true
		return nil
	})
	if err != nil {
		t.Fatalf("ListBlobs: %v", err)
	}
	if len(seen) != 2 {
		t.Errorf("got %d blobs under prefix aa, want 2: %v", len(seen), seen)
	}
}

func TestDeleteBlobIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newTestStorage(t)

	const id = blobstore.ID("test/delete")
	if err := st.PutBlob(ctx, id, putBytes([]byte("x")), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if err := st.DeleteBlob(ctx, id); err != nil {
		t.Fatalf("first DeleteBlob: %v", err)
	}
	// Second delete on the same key — kopia's filesystem backend should
	// not error. Confirm so we don't have to wrap callers in errors.Is
	// checks.
	if err := st.DeleteBlob(ctx, id); err != nil {
		t.Errorf("second DeleteBlob should be a no-op, got %v", err)
	}
}

func TestSetModTimePreservesValueWhenSupported(t *testing.T) {
	// Mark-and-sweep GC depends on Metadata.Timestamp being a stable,
	// observable property of a blob. Verify that the filesystem backend
	// reports a timestamp on each blob (the exact mtime semantics differ
	// per backend but the field must be populated and non-zero).
	ctx := context.Background()
	st := newTestStorage(t)

	const id = blobstore.ID("test/timestamps")
	if err := st.PutBlob(ctx, id, putBytes([]byte("x")), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	md, err := st.GetMetadata(ctx, id)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if md.Timestamp.IsZero() {
		t.Error("timestamp must be populated for mark-and-sweep GC to work")
	}
	if d := time.Since(md.Timestamp); d < 0 || d > 10*time.Second {
		t.Errorf("timestamp out of plausible range: %v ago", d)
	}
}

// sharded.Options is re-exported through filesystem.Options — sanity-check
// that our sharding config flows through.
func TestShardingIsConfigured(t *testing.T) {
	st, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{
		Root:            t.TempDir(),
		DirectoryShards: []int{2, 2},
	})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	defer st.Close(context.Background())

	// Underlying type must be a kopia filesystem store; we can't assert on
	// the on-disk path layout without import-hacking, but a smoke
	// round-trip with the sharded config confirms it doesn't error.
	const id = blobstore.ID("aabbccddeeff/sharded")
	if err := st.PutBlob(context.Background(), id, putBytes([]byte("x")), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob with multi-level sharding: %v", err)
	}
}
