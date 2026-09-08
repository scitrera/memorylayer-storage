// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// sha256Hex returns the hex-encoded sha256 of b — the whole-object content hash
// the gateway computes, used to arm the A1 integrity gate in tests.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

const testDomain = "ent"

// testStack builds a Gateway over a local-fs casstore configured for global
// dedup in testDomain, plus an index-aware GC and the in-memory ref/staging
// stores. It returns everything tests need to drive and inspect the gateway.
func testStack(t *testing.T) (*Gateway, *snapshot.ChunkedGC, *MemoryStagingStore, blobstore.Storage) {
	t.Helper()
	ctx := context.Background()
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	dstore := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024,
		DedupDomain:     testDomain,
		Index:           snapshot.NewGlobalIndex(dstore, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = dstore

	stage := NewMemoryStagingStore()
	gw := New(cs, NewMemoryRefStore(), stage, testDomain)
	return gw, gc, stage, chunks
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func countDomainBlobs(t *testing.T, chunks blobstore.Storage) int {
	t.Helper()
	n := 0
	for _, prefix := range []blobstore.ID{blobstore.ID("chunk-" + testDomain + "-"), blobstore.ID("pack-" + testDomain + "-")} {
		if err := chunks.ListBlobs(context.Background(), prefix, func(blobstore.Metadata) error {
			n++
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs %q: %v", prefix, err)
		}
	}
	return n
}

func TestGateway_PutGetHeadRoundTrip(t *testing.T) {
	gw, _, _, _ := testStack(t)
	ctx := context.Background()

	payload := randomBytes(t, 2*1024*1024)
	info, err := gw.Put(ctx, "docs/page-1.png", "image/png", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("size: got %d want %d", info.Size, len(payload))
	}
	if info.ContentType != "image/png" {
		t.Errorf("content type: got %q", info.ContentType)
	}
	if info.ContentHash == "" {
		t.Error("expected a content hash")
	}

	// Head returns metadata, no body.
	head, err := gw.Head(ctx, "docs/page-1.png")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.ContentHash != info.ContentHash || head.Size != info.Size {
		t.Errorf("Head metadata mismatch: %+v vs %+v", head, info)
	}

	// Get returns the exact bytes.
	rc, got, err := gw.Get(ctx, "docs/page-1.png")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Errorf("round-trip mismatch: got %d bytes want %d", len(body), len(payload))
	}
	if got.ContentType != "image/png" {
		t.Errorf("Get content type: got %q", got.ContentType)
	}
}

// TestGateway_DedupAcrossDistinctRefs is the L1.1 acceptance test (scaled
// down): many identical objects under DISTINCT refs store ~one physical copy.
func TestGateway_DedupAcrossDistinctRefs(t *testing.T) {
	gw, _, _, chunks := testStack(t)
	ctx := context.Background()

	payload := randomBytes(t, 3*1024*1024) // identical content, distinct refs

	first, err := gw.Put(ctx, "tensor/0", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put 0: %v", err)
	}
	blobsAfterFirst := countDomainBlobs(t, chunks)
	if blobsAfterFirst == 0 {
		t.Fatal("expected blobs after first object")
	}

	const n = 100
	for i := 1; i < n; i++ {
		info, err := gw.Put(ctx, fmt.Sprintf("tensor/%d", i), "application/octet-stream", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		// Every identical object has the same whole-object content hash.
		if info.ContentHash != first.ContentHash {
			t.Errorf("object %d content hash differs: %s vs %s", i, info.ContentHash, first.ContentHash)
		}
	}

	blobsAfterAll := countDomainBlobs(t, chunks)
	if blobsAfterAll != blobsAfterFirst {
		t.Errorf("dedup failed: %d distinct identical objects grew blob count %d → %d", n, blobsAfterFirst, blobsAfterAll)
	}

	// All refs are independently retrievable and byte-exact.
	for _, ref := range []string{"tensor/0", "tensor/42", "tensor/99"} {
		rc, _, err := gw.Get(ctx, ref)
		if err != nil {
			t.Fatalf("Get %s: %v", ref, err)
		}
		body, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(body, payload) {
			t.Errorf("ref %s round-trip mismatch", ref)
		}
	}

	// Listing by prefix returns all of them.
	objs, err := gw.List(ctx, "tensor/", 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) != n {
		t.Errorf("List returned %d objects, want %d", len(objs), n)
	}
}

// TestGateway_DeleteThenGCReclaimsOnlyUnreferenced verifies delete + GC frees
// only content no surviving object references.
func TestGateway_DeleteThenGCReclaimsOnlyUnreferenced(t *testing.T) {
	gw, gc, _, chunks := testStack(t)
	ctx := context.Background()

	keep := randomBytes(t, 2*1024*1024)
	kill := randomBytes(t, 2*1024*1024) // distinct content

	if _, err := gw.Put(ctx, "keep", "application/octet-stream", bytes.NewReader(keep)); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	blobsKeepOnly := countDomainBlobs(t, chunks)
	if _, err := gw.Put(ctx, "kill", "application/octet-stream", bytes.NewReader(kill)); err != nil {
		t.Fatalf("Put kill: %v", err)
	}
	blobsBoth := countDomainBlobs(t, chunks)
	if blobsBoth <= blobsKeepOnly {
		t.Fatalf("distinct content should add blobs: %d → %d", blobsKeepOnly, blobsBoth)
	}

	// Delete kill, then GC. Head must 404; keep must survive intact.
	if err := gw.Delete(ctx, "kill"); err != nil {
		t.Fatalf("Delete kill: %v", err)
	}
	if _, err := gw.Head(ctx, "kill"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Head(kill) after delete: got %v, want ErrNotFound", err)
	}
	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.ChunksReclaimed == 0 {
		t.Error("expected GC to reclaim kill's unreferenced packs")
	}
	afterGC := countDomainBlobs(t, chunks)
	if afterGC != blobsKeepOnly {
		t.Errorf("GC should leave exactly keep's blobs: want %d, got %d", blobsKeepOnly, afterGC)
	}

	// keep still restores byte-exact.
	rc, _, err := gw.Get(ctx, "keep")
	if err != nil {
		t.Fatalf("Get keep after GC: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(body, keep) {
		t.Error("keep corrupted after deleting kill + GC")
	}
}

func TestGateway_StageThenFinalize(t *testing.T) {
	gw, _, stage, _ := testStack(t)
	ctx := context.Background()

	staged, err := gw.MintRef(ctx, "", "application/pdf", 100*1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("MintRef: %v", err)
	}
	if staged.Ref == "" || staged.UploadURL == "" || staged.StagingKey == "" {
		t.Fatalf("incomplete StagedUpload: %+v", staged)
	}

	// Before finalize the ref is pending → Head/Get must 404.
	if _, err := gw.Head(ctx, staged.Ref); !errors.Is(err, ErrNotFound) {
		t.Errorf("pending ref Head: got %v, want ErrNotFound", err)
	}

	// Client "uploads" to the presigned target (simulated).
	payload := randomBytes(t, 2*1024*1024)
	stage.PutStaged(staged.StagingKey, payload)

	info, err := gw.Finalize(ctx, staged.Ref)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if info.Pending {
		t.Error("finalized object should not be pending")
	}
	if info.Size != int64(len(payload)) || info.ContentType != "application/pdf" {
		t.Errorf("finalized metadata wrong: %+v", info)
	}

	// Now retrievable and byte-exact.
	rc, _, err := gw.Get(ctx, staged.Ref)
	if err != nil {
		t.Fatalf("Get after finalize: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(body, payload) {
		t.Error("staged upload round-trip mismatch")
	}

	// Finalize is idempotent.
	again, err := gw.Finalize(ctx, staged.Ref)
	if err != nil {
		t.Fatalf("Finalize idempotent: %v", err)
	}
	if again.ContentHash != info.ContentHash {
		t.Errorf("idempotent finalize changed content hash: %s vs %s", again.ContentHash, info.ContentHash)
	}
}

// TestStagingGC_ReclaimsStalePendingOnly mints two pending refs, advances the
// sweeper's clock past the TTL for one of them, and asserts the stale slot
// (its staging object + pending row) is reclaimed while a fresh pending ref
// within the TTL is preserved.
func TestStagingGC_ReclaimsStalePendingOnly(t *testing.T) {
	gw, _, stage, _ := testStack(t)
	ctx := context.Background()

	const ttl = 24 * time.Hour

	// Mint a pending ref "now", upload its staging bytes, but never finalize it.
	stale, err := gw.MintRef(ctx, "stale", "application/pdf", 0, time.Hour)
	if err != nil {
		t.Fatalf("MintRef stale: %v", err)
	}
	stage.PutStaged(stale.StagingKey, randomBytes(t, 4096))

	// The sweeper sees a clock TTL+1h in the future, so "stale" is past the TTL.
	gc := NewStagingGC(gw.refs, stage, testDomain, ttl, nil)
	gc.SetClock(func() time.Time { return time.Now().UTC().Add(ttl + time.Hour) })

	// Mint a SECOND pending ref that is fresh relative to the sweeper's clock:
	// its CreatedAt is the sweeper's "now", so it is within the TTL window.
	fresh := ObjectInfo{
		Ref:        "fresh",
		Domain:     testDomain,
		Pending:    true,
		StagingKey: testDomain + "/staging/fresh/up",
		CreatedAt:  time.Now().UTC().Add(ttl + time.Hour),
	}
	if err := gw.refs.Put(ctx, fresh); err != nil {
		t.Fatalf("put fresh pending: %v", err)
	}
	stage.PutStaged(fresh.StagingKey, randomBytes(t, 4096))

	res, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.SlotsReclaimed != 1 || res.PendingScanned != 1 {
		t.Errorf("sweep summary: %+v, want 1 scanned/1 reclaimed", res)
	}

	// The stale pending row is gone.
	if _, ok, _ := gw.refs.Get(ctx, testDomain, "stale"); ok {
		t.Error("stale pending row should be reclaimed")
	}
	// Its staging object is gone (Open now fails).
	if _, err := stage.Open(ctx, stale.StagingKey); err == nil {
		t.Error("stale staging object should be deleted")
	}

	// The fresh pending ref (within TTL) is preserved, row and staging object.
	if _, ok, _ := gw.refs.Get(ctx, testDomain, "fresh"); !ok {
		t.Error("fresh pending row must be preserved within TTL")
	}
	if _, err := stage.Open(ctx, fresh.StagingKey); err != nil {
		t.Errorf("fresh staging object must be preserved: %v", err)
	}
}

// TestGateway_UserMetaRoundTrip proves caller user metadata attached on write is
// stored durably in the casstore manifest and recovered on Get/Head — including
// across a fresh ref store (simulating a restart whose only durable state is the
// casstore backend). It also verifies casstore's own internal tags never leak
// into ObjectInfo.UserMeta and that SetUserMeta replaces the durable set without
// disturbing the bytes.
func TestGateway_UserMetaRoundTrip(t *testing.T) {
	ctx := context.Background()

	// Build a stack but keep handles to the casstore backends so we can rebuild a
	// gateway with a fresh (empty) ref store over the SAME manifests/chunks.
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	dstore := snapshot.NewMemoryDedupStore()
	newGW := func(refs RefStore) *Gateway {
		cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
			PackTargetBytes: 1 * 1024 * 1024,
			DedupDomain:     testDomain,
			Index:           snapshot.NewGlobalIndex(dstore, nil),
		})
		if err != nil {
			t.Fatalf("NewChunkedStore: %v", err)
		}
		return New(cs, refs, NewMemoryStagingStore(), testDomain)
	}

	gw := newGW(NewMemoryRefStore())
	payload := randomBytes(t, 1024*1024)
	meta := map[string]string{"author": "alice", "purpose": "page-image"}

	info, err := gw.PutWithMeta(ctx, "docs/p1.png", "image/png", meta, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	if info.UserMeta["author"] != "alice" || info.UserMeta["purpose"] != "page-image" {
		t.Errorf("Put returned wrong user meta: %+v", info.UserMeta)
	}

	// Head/Get on the same gateway return the metadata.
	head, err := gw.Head(ctx, "docs/p1.png")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.UserMeta["author"] != "alice" || head.UserMeta["purpose"] != "page-image" {
		t.Errorf("Head user meta: %+v", head.UserMeta)
	}
	// casstore's own internal tag (chunked_format) must NOT surface.
	if _, leaked := head.UserMeta["chunked_format"]; leaked {
		t.Error("casstore internal tag leaked into UserMeta")
	}

	// Restart: a brand-new gateway with an EMPTY ref store, same casstore backend.
	// Repopulate only the ref existence row (the ref index is the gateway's own
	// durable concern; in production pgindex persists it). The user metadata must
	// still come back from the manifest, not the ref store.
	fresh := newGW(NewMemoryRefStore())
	if err := fresh.refs.Put(ctx, ObjectInfo{
		Ref: "docs/p1.png", Domain: testDomain, Size: info.Size, ContentType: "image/png", Version: info.Version,
	}); err != nil {
		t.Fatalf("seed fresh ref: %v", err)
	}
	rehead, err := fresh.Head(ctx, "docs/p1.png")
	if err != nil {
		t.Fatalf("fresh Head: %v", err)
	}
	if rehead.UserMeta["author"] != "alice" || rehead.UserMeta["purpose"] != "page-image" {
		t.Errorf("durability lost: fresh Head user meta: %+v", rehead.UserMeta)
	}
	rc, reget, err := fresh.Get(ctx, "docs/p1.png")
	if err != nil {
		t.Fatalf("fresh Get: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(body, payload) {
		t.Error("fresh Get body mismatch")
	}
	if reget.UserMeta["author"] != "alice" {
		t.Errorf("fresh Get user meta: %+v", reget.UserMeta)
	}

	// SetUserMeta replaces the durable set without touching the bytes.
	if err := gw.SetUserMeta(ctx, "docs/p1.png", map[string]string{"author": "bob"}); err != nil {
		t.Fatalf("SetUserMeta: %v", err)
	}
	after, err := gw.Head(ctx, "docs/p1.png")
	if err != nil {
		t.Fatalf("Head after SetUserMeta: %v", err)
	}
	if after.UserMeta["author"] != "bob" {
		t.Errorf("SetUserMeta did not replace author: %+v", after.UserMeta)
	}
	if _, stale := after.UserMeta["purpose"]; stale {
		t.Error("SetUserMeta should have replaced (not merged) the metadata set")
	}
	rc2, _, err := gw.Get(ctx, "docs/p1.png")
	if err != nil {
		t.Fatalf("Get after SetUserMeta: %v", err)
	}
	body2, _ := io.ReadAll(rc2)
	rc2.Close()
	if !bytes.Equal(body2, payload) {
		t.Error("SetUserMeta corrupted the object bytes")
	}
}

// TestGateway_SetUserMetaNoRepack proves SetUserMeta rewrites only the manifest
// (tech-debt #39): after the initial PutWithMeta no new chunk/pack blob appears,
// the new tags are visible via Head/Get, and the object bytes are unchanged.
func TestGateway_SetUserMetaNoRepack(t *testing.T) {
	gw, _, _, chunks := testStack(t)
	ctx := context.Background()

	payload := randomBytes(t, 3*1024*1024)
	if _, err := gw.PutWithMeta(ctx, "obj/a", "image/png",
		map[string]string{"author": "alice", "purpose": "page"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	blobsBefore := countDomainBlobs(t, chunks)

	// Post-hoc metadata bind (the S3 ETag case): replaces the user-meta set.
	if err := gw.SetUserMeta(ctx, "obj/a", map[string]string{"etag": "deadbeef"}); err != nil {
		t.Fatalf("SetUserMeta: %v", err)
	}

	if blobsAfter := countDomainBlobs(t, chunks); blobsAfter != blobsBefore {
		t.Errorf("SetUserMeta re-packed: chunk/pack blob count %d -> %d (must be unchanged)", blobsBefore, blobsAfter)
	}

	head, err := gw.Head(ctx, "obj/a")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.UserMeta["etag"] != "deadbeef" {
		t.Errorf("etag not set: %+v", head.UserMeta)
	}
	if _, stale := head.UserMeta["author"]; stale {
		t.Errorf("SetUserMeta must replace, not merge: %+v", head.UserMeta)
	}

	rc, get, err := gw.Get(ctx, "obj/a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(body, payload) {
		t.Error("SetUserMeta corrupted object bytes")
	}
	if get.UserMeta["etag"] != "deadbeef" {
		t.Errorf("Get etag: %+v", get.UserMeta)
	}
}

// TestGateway_FinalizeIntegrityGate covers the A1 integrity gate: a finalize
// whose staged bytes match the expected hash+size binds the ref; a mismatched
// hash or size, or an empty staged object under an armed ExpectedSize, returns
// ErrIntegrity and does NOT bind the ref (Head stays 404).
func TestGateway_FinalizeIntegrityGate(t *testing.T) {
	ctx := context.Background()

	// stageAndFinalize mints with the given expectations, uploads payload, and
	// returns the finalize result. A nil payload simulates an empty upload.
	mintPayload := func(t *testing.T, gw *Gateway, stage *MemoryStagingStore, ref string, payload []byte, expHash string, expSize int64) (ObjectInfo, error) {
		t.Helper()
		staged, err := gw.MintRefWithOptions(ctx, ref, MintOptions{
			ContentType:  "application/octet-stream",
			MaxSize:      0,
			TTL:          time.Hour,
			ExpectedHash: expHash,
			ExpectedSize: expSize,
		})
		if err != nil {
			t.Fatalf("MintRefWithOptions: %v", err)
		}
		stage.PutStaged(staged.StagingKey, payload)
		info, err := gw.Finalize(ctx, staged.Ref)
		return info, err
	}

	payload := randomBytes(t, 256*1024)
	goodHash := sha256Hex(payload)

	t.Run("matching hash and size finalizes", func(t *testing.T) {
		gw, _, stage, _ := testStack(t)
		info, err := mintPayload(t, gw, stage, "ok", payload, goodHash, int64(len(payload)))
		if err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		if info.Pending {
			t.Error("finalized object should not be pending")
		}
		if info.ContentHash != goodHash || info.Size != int64(len(payload)) {
			t.Errorf("finalized metadata wrong: %+v", info)
		}
		if _, err := gw.Head(ctx, "ok"); err != nil {
			t.Errorf("Head after good finalize: %v", err)
		}
	})

	t.Run("mismatched hash returns ErrIntegrity and does not bind", func(t *testing.T) {
		gw, _, stage, _ := testStack(t)
		_, err := mintPayload(t, gw, stage, "badhash", payload, sha256Hex([]byte("other")), 0)
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("Finalize got %v, want ErrIntegrity", err)
		}
		if _, err := gw.Head(ctx, "badhash"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ref bound despite hash mismatch: Head got %v", err)
		}
	})

	t.Run("mismatched size returns ErrIntegrity", func(t *testing.T) {
		gw, _, stage, _ := testStack(t)
		_, err := mintPayload(t, gw, stage, "badsize", payload, "", int64(len(payload))+1)
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("Finalize got %v, want ErrIntegrity", err)
		}
		if _, err := gw.Head(ctx, "badsize"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ref bound despite size mismatch: Head got %v", err)
		}
	})

	t.Run("empty staged with ExpectedSize set returns ErrIntegrity", func(t *testing.T) {
		gw, _, stage, _ := testStack(t)
		_, err := mintPayload(t, gw, stage, "empty", []byte{}, "", 1024)
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("Finalize empty got %v, want ErrIntegrity", err)
		}
		if _, err := gw.Head(ctx, "empty"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ref bound despite empty staged object: Head got %v", err)
		}
	})

	t.Run("no expectation accepts whatever is staged", func(t *testing.T) {
		gw, _, stage, _ := testStack(t)
		info, err := mintPayload(t, gw, stage, "free", payload, "", 0)
		if err != nil {
			t.Fatalf("Finalize without expectation: %v", err)
		}
		if info.ContentHash != goodHash {
			t.Errorf("unexpected content hash: %s", info.ContentHash)
		}
	})
}

func TestGateway_NotFound(t *testing.T) {
	gw, _, _, _ := testStack(t)
	ctx := context.Background()
	if _, err := gw.Head(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Head missing: got %v", err)
	}
	if _, _, err := gw.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing: got %v", err)
	}
	if _, err := gw.Put(ctx, "", "x", bytes.NewReader(nil)); !errors.Is(err, ErrInvalidRef) {
		t.Errorf("Put empty ref: got %v", err)
	}
	// Delete of an unknown ref is a no-op.
	if err := gw.Delete(ctx, "missing"); err != nil {
		t.Errorf("Delete missing should be no-op: %v", err)
	}
}

// TestGateway_DeletePrefix proves the A8 server-side bulk delete: it removes
// every ref under the prefix (each via the same per-ref Delete that drops all
// backing manifest versions, so a follow-up GC can reclaim the bytes), returns
// the count, leaves non-matching refs intact, and rejects an empty prefix.
func TestGateway_DeletePrefix(t *testing.T) {
	gw, gc, _, chunks := testStack(t)
	ctx := context.Background()

	countBlobs := func() int {
		n := 0
		for _, p := range []blobstore.ID{
			blobstore.ID("chunk-" + testDomain + "-"),
			blobstore.ID("pack-" + testDomain + "-"),
		} {
			_ = chunks.ListBlobs(ctx, p, func(blobstore.Metadata) error { n++; return nil })
		}
		return n
	}

	// Empty prefix is rejected outright (footgun guard).
	if _, err := gw.DeletePrefix(ctx, ""); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("DeletePrefix(\"\") = %v, want ErrInvalidRef", err)
	}

	// A subtree under tree/doc7/ plus an unrelated ref that must survive.
	const n = 12
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("tree/doc7/pages/page_%04d.png", i)
		if _, err := gw.Put(ctx, ref, "image/png", bytes.NewReader(randBytes(t, 1024))); err != nil {
			t.Fatalf("Put %s: %v", ref, err)
		}
	}
	if _, err := gw.Put(ctx, "tree/doc8/keep.png", "image/png", bytes.NewReader(randBytes(t, 1024))); err != nil {
		t.Fatalf("Put keep: %v", err)
	}

	deleted, err := gw.DeletePrefix(ctx, "tree/doc7/")
	if err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	if deleted != n {
		t.Fatalf("DeletePrefix deleted %d, want %d", deleted, n)
	}

	// Every matching ref is gone (manifest versions dropped → Head 404).
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("tree/doc7/pages/page_%04d.png", i)
		if _, err := gw.Head(ctx, ref); !errors.Is(err, ErrNotFound) {
			t.Errorf("Head %s after prefix delete: got %v, want ErrNotFound", ref, err)
		}
	}
	// The unrelated ref survives.
	if _, err := gw.Head(ctx, "tree/doc8/keep.png"); err != nil {
		t.Errorf("unrelated ref removed by prefix delete: %v", err)
	}

	// The deleted objects' manifests are gone, so GC reclaims their bytes and
	// only the surviving object's chunks remain.
	if _, err := gc.RunOnce(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}
	survivor, _, err := gw.Get(ctx, "tree/doc8/keep.png")
	if err != nil {
		t.Fatalf("Get survivor after GC: %v", err)
	}
	survivor.Close()
	if countBlobs() == 0 {
		t.Error("survivor object lost its backing blobs after prefix delete + GC")
	}
}

// randBytes is a small random-payload helper for the bulk-delete test.
func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}
