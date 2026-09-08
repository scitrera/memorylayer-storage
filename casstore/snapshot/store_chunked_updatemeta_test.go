// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// countingChunkStore wraps a blobstore.Storage and counts chunk/pack data
// accesses: GetBlob (reads) and body PutBlob (writes, excluding the
// DoNotRecreate probe that stores nothing). It is the instrument that proves
// UpdateLatestMeta touches NO chunk/pack blob — only the manifest, which lives
// in the upstream SnapshotStore, never in this chunk store.
type countingChunkStore struct {
	blobstore.Storage
	gets atomic.Int64
	puts atomic.Int64
}

func (c *countingChunkStore) GetBlob(ctx context.Context, id blobstore.ID, offset, length int64, output blobstore.OutputBuffer) error {
	c.gets.Add(1)
	return c.Storage.GetBlob(ctx, id, offset, length, output)
}

func (c *countingChunkStore) PutBlob(ctx context.Context, id blobstore.ID, data blobstore.Bytes, opts blobstore.PutOptions) error {
	if !opts.DoNotRecreate {
		c.puts.Add(1)
	}
	return c.Storage.PutBlob(ctx, id, data, opts)
}

// chunkedTestStackCounting builds a ChunkedStore whose chunk backend counts
// GetBlob/PutBlob, so a test can assert metadata-only updates do no chunk I/O.
func chunkedTestStackCounting(t *testing.T, packTarget int) (*ChunkedStore, SnapshotStore, *countingChunkStore) {
	t.Helper()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	raw, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close(context.Background()) })
	chunks := &countingChunkStore{Storage: raw}
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{PackTargetBytes: packTarget})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, upstream, chunks
}

// liveBlobSet replays ChunkedGC's mark phase: it returns the set of live blob
// IDs referenced by all manifests for the store's chunk backend, so a test can
// assert the live set is identical before and after a metadata-only update.
func liveBlobSet(t *testing.T, upstream SnapshotStore) map[blobstore.ID]struct{} {
	t.Helper()
	live := make(map[blobstore.ID]struct{})
	ctx := context.Background()
	err := upstream.Walk(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata) error {
		tag := meta.Tags["chunked_format"]
		if tag != manifestFormatV1 && tag != manifestFormatV2 {
			return nil
		}
		rc, _, err := upstream.Get(ctx, key, version)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		var m chunkManifest
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		switch m.Format {
		case manifestFormatV1:
			for _, ref := range m.Chunks {
				live[chunkBlobID(m.Tenant, ref.Hash)] = struct{}{}
			}
		case manifestFormatV2:
			for _, ref := range m.Chunks {
				live[packBlobID(m.Tenant, ref.PackHash)] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk manifests: %v", err)
	}
	return live
}

func equalLiveSets(a, b map[blobstore.ID]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

// TestUpdateLatestMeta_NoChunkReadOrWrite is the core tech-debt-#39 regression:
// a metadata-only update must rewrite ONLY the manifest, never re-reading or
// re-uploading any chunk/pack blob. It snapshots the chunk-backend GetBlob and
// PutBlob counters after the initial write, runs UpdateLatestMeta, and asserts
// the counters are UNCHANGED, the object still reads back byte-identical with
// the merged tags, and the GC live set is identical.
func TestUpdateLatestMeta_NoChunkReadOrWrite(t *testing.T) {
	for _, tc := range []struct {
		name       string
		packTarget int
	}{
		{"v2-packed", 1 * 1024 * 1024},
		{"v1-no-pack", packingDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs, upstream, chunks := chunkedTestStackCounting(t, tc.packTarget)
			ctx := context.Background()
			key := SnapshotKey{Tenant: "t1", OwnerKey: "owner-1"}

			payload := make([]byte, 5*1024*1024)
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}
			if _, err := cs.Put(ctx, key, SnapshotMetadata{
				Runtime: "runc",
				Tags:    map[string]string{"author": "alice", "purpose": "page"},
			}, bytes.NewReader(payload)); err != nil {
				t.Fatalf("Put: %v", err)
			}

			liveBefore := liveBlobSet(t, upstream)
			getsBefore := chunks.gets.Load()
			putsBefore := chunks.puts.Load()

			// Metadata-only update: overlay author, delete purpose, add etag.
			if _, err := cs.UpdateLatestMeta(ctx, key, map[string]string{
				"author":  "bob",
				"purpose": "", // delete
				"etag":    "abc123",
			}); err != nil {
				t.Fatalf("UpdateLatestMeta: %v", err)
			}

			if got := chunks.gets.Load(); got != getsBefore {
				t.Errorf("UpdateLatestMeta read chunk/pack blobs: GetBlob count %d -> %d (must be unchanged)", getsBefore, got)
			}
			if got := chunks.puts.Load(); got != putsBefore {
				t.Errorf("UpdateLatestMeta wrote chunk/pack blobs: PutBlob count %d -> %d (must be unchanged)", putsBefore, got)
			}

			// Tags merged correctly on the latest manifest.
			sm, err := cs.GetLatestMetadata(ctx, key)
			if err != nil {
				t.Fatalf("GetLatestMetadata: %v", err)
			}
			if sm.Tags["author"] != "bob" {
				t.Errorf("author not overlaid: %q", sm.Tags["author"])
			}
			if _, ok := sm.Tags["purpose"]; ok {
				t.Errorf("purpose not deleted: %q", sm.Tags["purpose"])
			}
			if sm.Tags["etag"] != "abc123" {
				t.Errorf("etag not added: %q", sm.Tags["etag"])
			}
			// casstore-internal tag preserved.
			wantFmt := manifestFormatV2
			if tc.packTarget == packingDisabled {
				wantFmt = manifestFormatV1
			}
			if sm.Tags["chunked_format"] != wantFmt {
				t.Errorf("chunked_format clobbered: got %q want %q", sm.Tags["chunked_format"], wantFmt)
			}

			// Object reads back byte-identical.
			rc, _, err := cs.GetLatest(ctx, key)
			if err != nil {
				t.Fatalf("GetLatest: %v", err)
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if !bytes.Equal(got, payload) {
				t.Errorf("object bytes changed after metadata update: got %d bytes want %d", len(got), len(payload))
			}

			// GC live set unchanged (dedup/GC unaffected). The reads above on the
			// counting backend are fine to compare against the pre-update set
			// because liveBlobSet is computed purely from manifests.
			liveAfter := liveBlobSet(t, upstream)
			if !equalLiveSets(liveBefore, liveAfter) {
				t.Errorf("GC live set changed after metadata update: before=%d after=%d", len(liveBefore), len(liveAfter))
			}
		})
	}
}

// TestUpdateLatestMeta_NoSnapshot returns ErrNoSnapshot for an unknown key.
func TestUpdateLatestMeta_NoSnapshot(t *testing.T) {
	cs, _, _ := chunkedTestStackCounting(t, 1*1024*1024)
	ctx := context.Background()
	_, err := cs.UpdateLatestMeta(ctx, SnapshotKey{Tenant: "t1", OwnerKey: "absent"}, map[string]string{"x": "y"})
	if err != ErrNoSnapshot {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
}
