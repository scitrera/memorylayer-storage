// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// TestCompact_RepointsDedupIndex proves the dedup-index repoint (TECH_DEBT #51):
// after compaction re-homes a shared chunk into a consolidated pack, the durable
// dedup index points that chunk at the NEW pack, so a later Put dedups to the
// consolidated pack and the old pack goes truly unreferenced — letting GC reclaim
// it. Without the repoint the later Put re-references (re-pins) the old pack and GC
// keeps it forever ("dedup-pinned, won't drain").
func TestCompact_RepointsDedupIndex(t *testing.T) {
	ctx := context.Background()
	const blockSize = 1 << 20 // one FIXED-1M chunk

	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	store := NewMemoryDedupStore()
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		SplitterName:    "FIXED-1M",
		PackTargetBytes: 4 << 20, // 4 MiB target → the 2-chunk (2 MiB) filler packs are under-filled + small
		DedupDomain:     "t1",
		Index:           NewGlobalIndex(store, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}

	mk := func() []byte {
		b := make([]byte, blockSize)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	hashOf := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	key := func(owner string) SnapshotKey { return SnapshotKey{Tenant: "t1", OwnerKey: owner} }
	put := func(owner string, data []byte) SnapshotMetadata {
		m, err := cs.Put(ctx, key(owner), SnapshotMetadata{}, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put %s: %v", owner, err)
		}
		return m
	}

	blockA, blockB, blockC, blockD := mk(), mk(), mk(), mk()

	// Two MULTI-chunk filler packs (packHash != chunkHash, so dedup actually fires —
	// a single-chunk pack would be skipped by the write-path dedup guard): P1=[A,B],
	// P2=[C,D].
	f1 := put("z/0", append(append([]byte{}, blockA...), blockB...))
	f2 := put("z/1", append(append([]byte{}, blockC...), blockD...))
	// Live objects dedup one chunk out of each filler pack.
	put("a/0", blockA) // A dedups → P1
	put("a/1", blockC) // C dedups → P2

	// The pre-compaction home of A is P1; capture it.
	refA0, ok, err := store.Lookup(ctx, "t1", hashOf(blockA))
	if err != nil || !ok {
		t.Fatalf("pre-compact Lookup(A): ok=%v err=%v", ok, err)
	}
	oldPackA := refA0.PackHash

	// Drop the fillers so P1/P2 are 50%-filled (A live via a/0, B dead; C live, D dead).
	if err := cs.upstream.Delete(ctx, key("z/0"), f1.Version); err != nil {
		t.Fatalf("delete filler z/0: %v", err)
	}
	if err := cs.upstream.Delete(ctx, key("z/1"), f2.Version); err != nil {
		t.Fatalf("delete filler z/1: %v", err)
	}

	// Compact: both packs are small (2 MiB < 3 MiB) and consolidate 2 → 1.
	res, err := cs.Compact(ctx, "t1", CompactConfig{MinPackBytes: 3 << 20})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.PacksSelected != 2 {
		t.Fatalf("PacksSelected=%d, want 2 (P1,P2)", res.PacksSelected)
	}

	// (1) The index must now point A at the consolidated pack, NOT the old P1.
	refA1, ok, err := store.Lookup(ctx, "t1", hashOf(blockA))
	if err != nil || !ok {
		t.Fatalf("post-compact Lookup(A): ok=%v err=%v", ok, err)
	}
	if refA1.PackHash == oldPackA {
		t.Fatalf("dedup index not repointed: A still → old pack %s", oldPackA)
	}

	// (2) A later Put of A must dedup to the consolidated pack (not the old one).
	put("a/2", blockA)
	mz, ok, err := cs.latestManifest(ctx, key("a/2"))
	if err != nil || !ok {
		t.Fatalf("latestManifest(a/2): ok=%v err=%v", ok, err)
	}
	if got := mz.Chunks[0].PackHash; got != refA1.PackHash {
		t.Fatalf("new Put re-referenced wrong pack: got %s, want consolidated %s (old=%s)", got, refA1.PackHash, oldPackA)
	}

	// (3) GC (with the dedup index wired so reclaim purges) must reclaim the old pack:
	// nothing references it anymore (a/0 rewritten, a/2 → consolidated, fillers gone).
	gc := NewChunkedGC(cs.upstream, chunks, slog.Default())
	gc.SafetyWindow = 0
	gc.Index = store
	if _, err := gc.RunOnceForDomain(ctx, "t1"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	buf := blobstore.NewOutputBuffer()
	if err := chunks.GetBlob(ctx, packBlobID("t1", oldPackA), 0, -1, buf); !errors.Is(err, blobstore.ErrBlobNotFound) {
		t.Fatalf("old pack %s not reclaimed after GC (re-pinned?): GetBlob err=%v", oldPackA, err)
	}

	// (4) a/2 still reads back A — served from the consolidated pack, proving it never
	// depended on the reclaimed old pack.
	rc, _, err := cs.GetLatest(ctx, key("a/2"))
	if err != nil {
		t.Fatalf("GetLatest a/2: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, blockA) {
		t.Fatalf("a/2 mismatch after GC reclaimed old pack")
	}
}
