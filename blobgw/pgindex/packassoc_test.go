// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package pgindex_test

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// These exercise the SQL behind snapshot.PackAssocStore against a real Postgres.
// They matter more than the usual contract test because this is the DELETION
// path: the queries decide whether a pack's bytes may be destroyed, and a subtle
// SQL mistake (a wrong join, a conditional UPDATE that always matches) would
// destroy live data rather than merely misbehave.

func TestPgIndex_AssociationLifecycle(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	// Two owners share pack "shared"; only "solo" holds pack "sole".
	if err := store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "shared", Context: "owner-a"},
		{PackHash: "shared", Context: "owner-b"},
		{PackHash: "sole", Context: "owner-a"},
	}); err != nil {
		t.Fatalf("Associate: %v", err)
	}
	// Idempotent.
	if err := store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "shared", Context: "owner-a"},
	}); err != nil {
		t.Fatalf("Associate idempotent: %v", err)
	}

	live := map[string]bool{}
	if err := store.WalkLivePacks(ctx, domain, func(p string) error { live[p] = true; return nil }); err != nil {
		t.Fatalf("WalkLivePacks: %v", err)
	}
	if !live["shared"] || !live["sole"] {
		t.Fatalf("live set missing packs: %+v", live)
	}

	// Dropping owner-a returns both its packs as reclamation candidates...
	candidates, err := store.DropContext(ctx, domain, "owner-a")
	if err != nil {
		t.Fatalf("DropContext: %v", err)
	}
	got := map[string]bool{}
	for _, c := range candidates {
		got[c] = true
	}
	if !got["shared"] || !got["sole"] || len(candidates) != 2 {
		t.Fatalf("DropContext candidates = %v, want exactly [shared sole]", candidates)
	}

	// ...but only "sole" is actually unreferenced; owner-b still holds "shared".
	has, err := store.HasAssociations(ctx, domain, []string{"shared", "sole"})
	if err != nil {
		t.Fatalf("HasAssociations: %v", err)
	}
	if !has["shared"] {
		t.Fatal("shared lost its association while owner-b still holds it")
	}
	if has["sole"] {
		t.Fatal("sole still reports an association after its only owner was dropped")
	}

	// Re-dropping is a no-op.
	again, err := store.DropContext(ctx, domain, "owner-a")
	if err != nil {
		t.Fatalf("DropContext repeat: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("repeat DropContext returned %v, want none", again)
	}
}

// TestPgIndex_MarkTransitions pins the conditional state SQL: exactly one
// reclaimer wins a mark, and a pack that regained an association cannot be
// committed to obliterated.
func TestPgIndex_MarkTransitions(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	// A pack with no packs row must still be markable (rows are created lazily by
	// storage accounting, so reclamation cannot assume one exists).
	won, err := store.MarkObliterating(ctx, domain, "ghost")
	if err != nil || !won {
		t.Fatalf("MarkObliterating(ghost): won=%v err=%v", won, err)
	}
	// A second reclaimer must lose.
	won2, err := store.MarkObliterating(ctx, domain, "ghost")
	if err != nil {
		t.Fatalf("MarkObliterating second: %v", err)
	}
	if won2 {
		t.Fatal("two reclaimers both won the mark for the same pack")
	}

	// Unreferenced: the commit succeeds and is terminal.
	committed, err := store.CommitObliterated(ctx, domain, "ghost")
	if err != nil || !committed {
		t.Fatalf("CommitObliterated(ghost): committed=%v err=%v", committed, err)
	}
	again, err := store.CommitObliterated(ctx, domain, "ghost")
	if err != nil {
		t.Fatalf("CommitObliterated repeat: %v", err)
	}
	if again {
		t.Fatal("CommitObliterated committed an already-obliterated pack twice")
	}

	// A pack that regains an association during the drain must NOT be committed.
	if err := store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "resurrected", Context: "late"},
	}); err != nil {
		t.Fatalf("Associate: %v", err)
	}
	if _, err := store.DropContext(ctx, domain, "late"); err != nil {
		t.Fatalf("DropContext: %v", err)
	}
	won, err = store.MarkObliterating(ctx, domain, "resurrected")
	if err != nil || !won {
		t.Fatalf("MarkObliterating(resurrected): won=%v err=%v", won, err)
	}
	// A racing writer cannot associate a marked pack...
	err = store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "resurrected", Context: "racer"},
	})
	if !errors.Is(err, snapshot.ErrPackObliterating) {
		t.Fatalf("Associate on a marked pack = %v, want ErrPackObliterating", err)
	}
	// ...so release the mark first, as the reclaimer does when it spares a pack.
	if err := store.ReleaseMark(ctx, domain, "resurrected"); err != nil {
		t.Fatalf("ReleaseMark: %v", err)
	}
	if err := store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "resurrected", Context: "racer"},
	}); err != nil {
		t.Fatalf("Associate after release: %v", err)
	}
	// Now mark again and confirm the association blocks the commit.
	won, err = store.MarkObliterating(ctx, domain, "resurrected")
	if err != nil || !won {
		t.Fatalf("MarkObliterating(resurrected) again: won=%v err=%v", won, err)
	}
	committed, err = store.CommitObliterated(ctx, domain, "resurrected")
	if err != nil {
		t.Fatalf("CommitObliterated(resurrected): %v", err)
	}
	if committed {
		t.Fatal("CommitObliterated destroyed a pack that still holds an association")
	}
}

// TestPgIndex_AssociateIsAllOrNothing pins the transactional contract the
// ChunkedStore write path depends on: a refused batch must record nothing, or a
// caller that treats its Put as failed would leave a half-recorded owner behind.
func TestPgIndex_AssociateIsAllOrNothing(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	won, err := store.MarkObliterating(ctx, domain, "doomed")
	if err != nil || !won {
		t.Fatalf("MarkObliterating: won=%v err=%v", won, err)
	}
	err = store.Associate(ctx, domain, []snapshot.PackAssociation{
		{PackHash: "healthy", Context: "c1"},
		{PackHash: "doomed", Context: "c1"},
	})
	if !errors.Is(err, snapshot.ErrPackObliterating) {
		t.Fatalf("Associate = %v, want ErrPackObliterating", err)
	}
	has, err := store.HasAssociations(ctx, domain, []string{"healthy"})
	if err != nil {
		t.Fatalf("HasAssociations: %v", err)
	}
	if has["healthy"] {
		t.Fatal("a refused Associate batch left a partial row behind")
	}
}

// TestPgIndex_LookupHidesReclaimingPack is the guard that keeps a concurrent Put
// from deduping into bytes on their way out. It exercises the LEFT JOIN against
// packs.state in both Lookup and LookupBatch.
func TestPgIndex_LookupHidesReclaimingPack(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	if err := store.Record(ctx, domain, []snapshot.ChunkLocation{
		{ChunkHash: "ch1", PackRef: snapshot.PackRef{PackHash: "pk1", Offset: 0, Size: 10}},
		{ChunkHash: "ch2", PackRef: snapshot.PackRef{PackHash: "pk2", Offset: 0, Size: 10}},
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if _, ok, err := store.Lookup(ctx, domain, "ch1"); err != nil || !ok {
		t.Fatalf("baseline Lookup: ok=%v err=%v", ok, err)
	}

	if won, err := store.MarkObliterating(ctx, domain, "pk1"); err != nil || !won {
		t.Fatalf("MarkObliterating: won=%v err=%v", won, err)
	}
	if _, ok, err := store.Lookup(ctx, domain, "ch1"); err != nil || ok {
		t.Fatalf("Lookup handed out a reclaiming pack: ok=%v err=%v", ok, err)
	}
	batch, err := store.LookupBatch(ctx, domain, []string{"ch1", "ch2"})
	if err != nil {
		t.Fatalf("LookupBatch: %v", err)
	}
	if _, present := batch["ch1"]; present {
		t.Fatal("LookupBatch handed out a reclaiming pack")
	}
	if _, present := batch["ch2"]; !present {
		t.Fatal("LookupBatch dropped an unrelated healthy pack")
	}

	// Releasing the mark restores visibility.
	if err := store.ReleaseMark(ctx, domain, "pk1"); err != nil {
		t.Fatalf("ReleaseMark: %v", err)
	}
	if _, ok, err := store.Lookup(ctx, domain, "ch1"); err != nil || !ok {
		t.Fatalf("Lookup still hides the pack after release: ok=%v err=%v", ok, err)
	}
}

func TestPgIndex_BackfillMarker(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	done, err := store.IsBackfilled(ctx, domain)
	if err != nil {
		t.Fatalf("IsBackfilled: %v", err)
	}
	if done {
		t.Fatal("a fresh domain reports itself association-backfilled")
	}
	if err := store.MarkBackfilled(ctx, domain); err != nil {
		t.Fatalf("MarkBackfilled: %v", err)
	}
	// Idempotent.
	if err := store.MarkBackfilled(ctx, domain); err != nil {
		t.Fatalf("MarkBackfilled repeat: %v", err)
	}
	done, err = store.IsBackfilled(ctx, domain)
	if err != nil || !done {
		t.Fatalf("IsBackfilled after mark: done=%v err=%v", done, err)
	}
}
