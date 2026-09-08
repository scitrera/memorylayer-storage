// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

func TestIntegrityKind(t *testing.T) {
	if got := IntegrityKind(nil); got != "" {
		t.Errorf("nil: %q, want \"\"", got)
	}
	if got := IntegrityKind(errors.New("boom")); got != "" {
		t.Errorf("plain: %q, want \"\"", got)
	}
	if got := IntegrityKind(ErrCorrupt); got != "corrupt" {
		t.Errorf("corrupt: %q", got)
	}
	if got := IntegrityKind(ErrMissingBlob); got != "not_found" {
		t.Errorf("missing: %q", got)
	}
}

// onlyPackID returns the single pack blob's ID in the store (the test objects below
// each fit one pack).
func onlyPackID(t *testing.T, chunks blobstore.Storage) blobstore.ID {
	t.Helper()
	var id blobstore.ID
	if err := chunks.ListBlobs(context.Background(), "pack-", func(md blobstore.Metadata) error {
		id = md.BlobID
		return nil
	}); err != nil {
		t.Fatalf("ListBlobs: %v", err)
	}
	if id == "" {
		t.Fatal("no pack blob found")
	}
	return id
}

func readAll(t *testing.T, cs *ChunkedStore, key SnapshotKey) error {
	rc, _, err := cs.GetLatest(context.Background(), key)
	if err != nil {
		return err
	}
	_, err = io.ReadAll(rc)
	_ = rc.Close()
	return err
}

// TestRead_MissingBlobSentinel: deleting a pack a manifest references makes the read
// fail with ErrMissingBlob — the GC-over-deletion (#50) canary.
func TestRead_MissingBlobSentinel(t *testing.T) {
	ctx := context.Background()
	cs, _, chunks := chunkedTestStackWithPackSize(t, 1<<20)
	key := SnapshotKey{Tenant: "t1", OwnerKey: "obj"}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(bytes.Repeat([]byte("z"), 64*1024))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := chunks.DeleteBlob(ctx, onlyPackID(t, chunks)); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}
	if err := readAll(t, cs, key); !errors.Is(err, ErrMissingBlob) {
		t.Fatalf("read after pack delete: want ErrMissingBlob, got %v", err)
	}
}

// TestRead_CorruptSentinel: flipping a content byte in a pack (past the header, so
// framing still parses) makes a chunk fail its hash check → ErrCorrupt.
func TestRead_CorruptSentinel(t *testing.T) {
	ctx := context.Background()
	cs, _, chunks := chunkedTestStackWithPackSize(t, 1<<20)
	key := SnapshotKey{Tenant: "t1", OwnerKey: "obj"}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(bytes.Repeat([]byte("integrity-canary"), 8*1024))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	pid := onlyPackID(t, chunks)

	buf := blobstore.NewOutputBuffer()
	if err := chunks.GetBlob(ctx, pid, 0, -1, buf); err != nil {
		t.Fatalf("GetBlob pack: %v", err)
	}
	raw := buf.Bytes()
	// Flip a byte near the end — well past the pack header, inside chunk content.
	raw[len(raw)-1] ^= 0xff
	if err := chunks.DeleteBlob(ctx, pid); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}
	if err := chunks.PutBlob(ctx, pid, blobstore.BytesFromSlice(raw), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob corrupt: %v", err)
	}
	if err := readAll(t, cs, key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read after content corruption: want ErrCorrupt, got %v", err)
	}
}
