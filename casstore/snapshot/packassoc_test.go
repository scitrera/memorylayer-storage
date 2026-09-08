// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// assocStack builds a ChunkedStore whose GlobalIndex is backed by a
// MemoryDedupStore (which implements PackAssocStore), plus the reclaimer and the
// raw handles a test needs to inspect physical state.
func assocStack(t *testing.T, domain string, packTarget int) (*ChunkedStore, *MemoryDedupStore, blobstore.Storage, *PackReclaimer) {
	t.Helper()
	ctx := context.Background()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	dedup := NewMemoryDedupStore()
	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		PackTargetBytes: packTarget,
		DedupDomain:     domain,
		Index:           NewGlobalIndex(dedup, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := NewPackReclaimer(dedup, chunks, dedup, nil)
	rec.DrainInterval = 0
	rec.SetSleep(func(time.Duration) {})
	return cs, dedup, chunks, rec
}

// packsOf returns the distinct pack hashes the key's latest manifest references.
func packsOf(t *testing.T, cs *ChunkedStore, key SnapshotKey) []string {
	t.Helper()
	m, ok, err := cs.latestManifest(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("latestManifest %v: ok=%v err=%v", key, ok, err)
	}
	seen := map[string]struct{}{}
	var out []string
	for _, ref := range m.Chunks {
		if ref.PackHash == "" {
			continue
		}
		if _, dup := seen[ref.PackHash]; dup {
			continue
		}
		seen[ref.PackHash] = struct{}{}
		out = append(out, ref.PackHash)
	}
	return out
}

func blobExists(t *testing.T, chunks blobstore.Storage, id blobstore.ID) bool {
	t.Helper()
	_, err := chunks.GetMetadata(context.Background(), id)
	if err == nil {
		return true
	}
	if errors.Is(err, blobstore.ErrBlobNotFound) {
		return false
	}
	t.Fatalf("GetMetadata(%s): %v", id, err)
	return false
}

// TestAssoc_PutRecordsAssociations is the base wiring check: a Put records one
// association per pack its manifest references, under the object's OwnerKey.
func TestAssoc_PutRecordsAssociations(t *testing.T) {
	ctx := context.Background()
	cs, dedup, _, _ := assocStack(t, "d", 1<<20)
	key := SnapshotKey{Tenant: "t", OwnerKey: "obj-a"}
	payload := bytes.Repeat([]byte("association-block-"), 200000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	packs := packsOf(t, cs, key)
	if len(packs) < 2 {
		t.Fatalf("want a multi-pack object to make the test meaningful, got %d packs", len(packs))
	}
	has, err := dedup.HasAssociations(ctx, "d", packs)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range packs {
		if !has[p] {
			t.Fatalf("pack %s has no association after Put", p)
		}
	}

	// And the live-pack walk sees exactly those packs.
	live := map[string]bool{}
	if err := dedup.WalkLivePacks(ctx, "d", func(p string) error { live[p] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, p := range packs {
		if !live[p] {
			t.Fatalf("pack %s absent from the association live set", p)
		}
	}
}

// TestAssoc_ObliterateContextReclaimsOnlyItsOwnBytes is the headline behaviour:
// dropping one owner reclaims only the packs that owner alone referenced, and
// leaves packs another owner still references completely alone.
//
// The sharing is made EXACT rather than incidental: two objects with identical
// payloads dedup to the same packs by construction, so the test does not depend
// on content-defined chunk boundaries happening to align (which is luck, and was
// flaky when the unique region was random).
func TestAssoc_ObliterateContextReclaimsOnlyItsOwnBytes(t *testing.T) {
	ctx := context.Background()
	cs, dedup, chunks, rec := assocStack(t, "d", 1<<20)

	shared := bytes.Repeat([]byte("shared-content-block-"), 200000)
	solo := bytes.Repeat([]byte("solo-content-block-"), 200000)

	// twinA and twinB hold IDENTICAL bytes, so twinB's chunks dedup exactly into
	// twinA's packs and both own every one of them.
	twinA := SnapshotKey{Tenant: "t", OwnerKey: "twin-a"}
	twinB := SnapshotKey{Tenant: "t", OwnerKey: "twin-b"}
	soloKey := SnapshotKey{Tenant: "t", OwnerKey: "solo"}
	for _, tc := range []struct {
		key  SnapshotKey
		body []byte
	}{{twinA, shared}, {twinB, shared}, {soloKey, solo}} {
		if _, err := cs.Put(ctx, tc.key, SnapshotMetadata{}, bytes.NewReader(tc.body)); err != nil {
			t.Fatalf("Put %v: %v", tc.key, err)
		}
	}

	sharedPacks := packsOf(t, cs, twinB)
	soloPacks := packsOf(t, cs, soloKey)
	if len(sharedPacks) == 0 || len(soloPacks) == 0 {
		t.Fatalf("need packs on both objects: shared=%d solo=%d", len(sharedPacks), len(soloPacks))
	}

	// Obliterating twin-a must spare EVERY shared pack: twin-b still owns them all.
	res, err := rec.ObliterateContext(ctx, "d", "twin-a")
	if err != nil {
		t.Fatalf("ObliterateContext(twin-a): %v", err)
	}
	if res.PacksReclaimed != 0 {
		t.Fatalf("obliterating twin-a reclaimed %d packs that twin-b still references", res.PacksReclaimed)
	}
	if res.PacksStillReferenced != len(sharedPacks) {
		t.Fatalf("spared %d packs, want all %d shared packs", res.PacksStillReferenced, len(sharedPacks))
	}
	for _, p := range sharedPacks {
		if !blobExists(t, chunks, packBlobID("d", p)) {
			t.Fatalf("pack %s that twin-b still references was deleted", p)
		}
	}
	if got := readAllVia(t, cs, twinB); !bytes.Equal(got, shared) {
		t.Fatal("twin-b is corrupt after obliterating twin-a")
	}

	// Obliterating the sole owner of the solo object DOES reclaim its packs.
	res, err = rec.ObliterateContext(ctx, "d", "solo")
	if err != nil {
		t.Fatalf("ObliterateContext(solo): %v", err)
	}
	if res.PacksReclaimed == 0 {
		t.Fatal("obliterating the sole owner reclaimed nothing")
	}
	t.Logf("obliterate solo: considered=%d reclaimed=%d spared=%d bytes=%d",
		res.PacksConsidered, res.PacksReclaimed, res.PacksStillReferenced, res.BytesReclaimed)
	for _, p := range soloPacks {
		if blobExists(t, chunks, packBlobID("d", p)) {
			t.Fatalf("solo-only pack %s survived its owner's obliteration", p)
		}
	}
	// And twin-b is STILL intact after the second reclamation.
	if got := readAllVia(t, cs, twinB); !bytes.Equal(got, shared) {
		t.Fatal("twin-b corrupt after reclaiming the solo object")
	}
	has, err := dedup.HasAssociations(ctx, "d", sharedPacks)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range sharedPacks {
		if !has[p] {
			t.Fatalf("shared pack %s lost twin-b's association", p)
		}
	}
}

// TestAssoc_ObliterateIsIdempotent proves a repeated obliterate is harmless —
// the operational property that matters when a reclaim pass is retried.
func TestAssoc_ObliterateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cs, _, _, rec := assocStack(t, "d", 1<<20)
	key := SnapshotKey{Tenant: "t", OwnerKey: "solo"}
	payload := bytes.Repeat([]byte("idempotent-block-"), 100000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	first, err := rec.ObliterateContext(ctx, "d", "solo")
	if err != nil {
		t.Fatalf("first obliterate: %v", err)
	}
	if first.PacksReclaimed == 0 {
		t.Fatal("first obliterate reclaimed nothing")
	}
	second, err := rec.ObliterateContext(ctx, "d", "solo")
	if err != nil {
		t.Fatalf("second obliterate: %v", err)
	}
	if second.PacksConsidered != 0 || second.PacksReclaimed != 0 {
		t.Fatalf("second obliterate was not a no-op: %+v", second)
	}
}

// TestAssoc_LookupRefusesReclaimingPack is guard 1: a pack under the obliterating
// mark must be invisible to dedup, so a concurrent Put cannot adopt bytes that
// are on their way out.
func TestAssoc_LookupRefusesReclaimingPack(t *testing.T) {
	ctx := context.Background()
	dedup := NewMemoryDedupStore()
	const domain, chunkHash, packHash = "d", "chunkhash", "packhash"

	if err := dedup.Record(ctx, domain, []ChunkLocation{{
		ChunkHash: chunkHash,
		PackRef:   PackRef{PackHash: packHash, Offset: 0, Size: 10},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := dedup.Lookup(ctx, domain, chunkHash); err != nil || !ok {
		t.Fatalf("baseline lookup: ok=%v err=%v", ok, err)
	}

	won, err := dedup.MarkObliterating(ctx, domain, packHash)
	if err != nil || !won {
		t.Fatalf("MarkObliterating: won=%v err=%v", won, err)
	}
	if _, ok, err := dedup.Lookup(ctx, domain, chunkHash); err != nil || ok {
		t.Fatalf("Lookup handed out a reclaiming pack: ok=%v err=%v", ok, err)
	}
	batch, err := dedup.LookupBatch(ctx, domain, []string{chunkHash})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 0 {
		t.Fatalf("LookupBatch handed out a reclaiming pack: %+v", batch)
	}

	// Releasing the mark makes it visible again.
	if err := dedup.ReleaseMark(ctx, domain, packHash); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := dedup.Lookup(ctx, domain, chunkHash); err != nil || !ok {
		t.Fatalf("Lookup still hides the pack after mark release: ok=%v err=%v", ok, err)
	}
}

// TestAssoc_AssociateRefusesReclaimingPack is guard 2: the narrow TOCTOU where a
// Put resolved a dedup hit just before the mark went up must fail the Put rather
// than let it write a manifest pointing at doomed bytes. It also pins the
// all-or-nothing contract: a refused batch records NOTHING.
func TestAssoc_AssociateRefusesReclaimingPack(t *testing.T) {
	ctx := context.Background()
	dedup := NewMemoryDedupStore()
	const domain = "d"

	won, err := dedup.MarkObliterating(ctx, domain, "doomed")
	if err != nil || !won {
		t.Fatalf("MarkObliterating: won=%v err=%v", won, err)
	}

	err = dedup.Associate(ctx, domain, []PackAssociation{
		{PackHash: "healthy", Context: "c1"},
		{PackHash: "doomed", Context: "c1"},
	})
	if !errors.Is(err, ErrPackObliterating) {
		t.Fatalf("Associate error = %v, want ErrPackObliterating", err)
	}
	// All-or-nothing: the healthy pack in the refused batch must NOT be recorded,
	// or the caller's "the whole Put failed" assumption would be wrong.
	has, herr := dedup.HasAssociations(ctx, domain, []string{"healthy"})
	if herr != nil {
		t.Fatal(herr)
	}
	if has["healthy"] {
		t.Fatal("a refused Associate batch recorded a partial result")
	}
}

// TestAssoc_DrainReCheckSparesResurrectedPack covers the race the drain window
// exists for: an association that lands after the mark is published must spare
// the pack, and the mark must be released so the pack stays usable.
func TestAssoc_DrainReCheckSparesResurrectedPack(t *testing.T) {
	ctx := context.Background()
	dedup := NewMemoryDedupStore()
	const domain, pack = "d", "p1"

	if err := dedup.Associate(ctx, domain, []PackAssociation{{PackHash: pack, Context: "owner"}}); err != nil {
		t.Fatal(err)
	}
	// Simulate reclamation reaching the marked state...
	if _, err := dedup.DropContext(ctx, domain, "owner"); err != nil {
		t.Fatal(err)
	}
	won, err := dedup.MarkObliterating(ctx, domain, pack)
	if err != nil || !won {
		t.Fatalf("MarkObliterating: won=%v err=%v", won, err)
	}
	// ...then force an association back on, as a racing writer would if it had
	// resolved its dedup hit before the mark. CommitObliterated must refuse.
	dedup.assocMu.Lock()
	a := dedup.assocFor(domain)
	a.byPack[pack] = map[string]struct{}{"late-owner": {}}
	a.byContext["late-owner"] = map[string]struct{}{pack: {}}
	dedup.assocMu.Unlock()

	committed, err := dedup.CommitObliterated(ctx, domain, pack)
	if err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Fatal("CommitObliterated deleted a pack that regained an association")
	}
	if err := dedup.ReleaseMark(ctx, domain, pack); err != nil {
		t.Fatal(err)
	}
	// After release the pack is usable again.
	if err := dedup.Associate(ctx, domain, []PackAssociation{{PackHash: pack, Context: "another"}}); err != nil {
		t.Fatalf("pack unusable after mark release: %v", err)
	}
}

// TestAssoc_GCRefusesAssociationLiveSetWithoutBackfill is the anti-footgun test.
// A domain that was never backfilled holds no associations for its pre-existing
// objects, so an association-derived live set would judge everything garbage. The
// GC must fall back to the manifest walk and delete NOTHING that is still
// referenced.
func TestAssoc_GCRefusesAssociationLiveSetWithoutBackfill(t *testing.T) {
	ctx := context.Background()
	cs, dedup, chunks, _ := assocStack(t, "d", 1<<20)

	key := SnapshotKey{Tenant: "t", OwnerKey: "legacy"}
	payload := bytes.Repeat([]byte("legacy-object-block-"), 150000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	packs := packsOf(t, cs, key)

	// Wipe the associations to simulate data written before the feature existed,
	// while leaving the manifests intact.
	dedup.assocMu.Lock()
	dedup.assoc = nil
	dedup.assocMu.Unlock()

	gc := NewChunkedGC(cs.upstream, chunks, nil)
	gc.Index = dedup
	gc.Assoc = dedup // not backfilled: the fast path must refuse itself

	res, err := gc.RunOnceForDomain(ctx, "d")
	if err != nil {
		t.Fatalf("RunOnceForDomain: %v", err)
	}
	if res.LiveFromAssociations {
		t.Fatal("GC used the association live set for a domain that was never backfilled")
	}
	for _, p := range packs {
		if !blobExists(t, chunks, packBlobID("d", p)) {
			t.Fatalf("GC deleted live pack %s on a non-backfilled domain", p)
		}
	}
	if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
		t.Fatal("legacy object corrupt after GC")
	}
}

// TestAssoc_BackfillEnablesAssociationLiveSet proves the adoption path end to
// end: backfill from existing manifests, then the GC uses the association live
// set and still keeps every referenced pack.
func TestAssoc_BackfillEnablesAssociationLiveSet(t *testing.T) {
	ctx := context.Background()
	cs, dedup, chunks, _ := assocStack(t, "d", 1<<20)

	key := SnapshotKey{Tenant: "t", OwnerKey: "legacy"}
	payload := bytes.Repeat([]byte("legacy-object-block-"), 150000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	packs := packsOf(t, cs, key)

	// Simulate pre-feature data: manifests exist, associations do not.
	dedup.assocMu.Lock()
	dedup.assoc = nil
	dedup.assocMu.Unlock()

	res, err := cs.BackfillAssociations(ctx, "d", nil)
	if err != nil {
		t.Fatalf("BackfillAssociations: %v", err)
	}
	if res.ManifestsScanned == 0 || res.AssociationsRecorded == 0 {
		t.Fatalf("backfill recorded nothing: %+v", res)
	}
	done, err := dedup.IsBackfilled(ctx, "d")
	if err != nil || !done {
		t.Fatalf("IsBackfilled after backfill: %v %v", done, err)
	}

	gc := NewChunkedGC(cs.upstream, chunks, nil)
	gc.Index = dedup
	gc.Assoc = dedup
	gcRes, err := gc.RunOnceForDomain(ctx, "d")
	if err != nil {
		t.Fatalf("RunOnceForDomain: %v", err)
	}
	if !gcRes.LiveFromAssociations {
		t.Fatal("GC did not use the association live set after a completed backfill")
	}
	for _, p := range packs {
		if !blobExists(t, chunks, packBlobID("d", p)) {
			t.Fatalf("association-driven GC deleted live pack %s", p)
		}
	}
	if got := readAllVia(t, cs, key); !bytes.Equal(got, payload) {
		t.Fatal("object corrupt after association-driven GC")
	}
}

// TestAssoc_BackfillIsIdempotent — re-running must not double-count or change the
// resulting live set, since operators will re-run it after restores.
func TestAssoc_BackfillIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cs, dedup, _, _ := assocStack(t, "d", 1<<20)
	key := SnapshotKey{Tenant: "t", OwnerKey: "obj"}
	payload := bytes.Repeat([]byte("idem-backfill-"), 120000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	countLive := func() int {
		n := 0
		if err := dedup.WalkLivePacks(ctx, "d", func(string) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := countLive()
	if _, err := cs.BackfillAssociations(ctx, "d", nil); err != nil {
		t.Fatalf("backfill 1: %v", err)
	}
	afterOne := countLive()
	if _, err := cs.BackfillAssociations(ctx, "d", nil); err != nil {
		t.Fatalf("backfill 2: %v", err)
	}
	afterTwo := countLive()
	if before != afterOne || afterOne != afterTwo {
		t.Fatalf("live pack count drifted across backfills: before=%d after1=%d after2=%d", before, afterOne, afterTwo)
	}
}
