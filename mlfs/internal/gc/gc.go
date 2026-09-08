// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package gc is mlfs's refcount-free, mark-and-sweep garbage collector.
//
// There is no chunk_refcount table (by design). Liveness is derived: the
// metadata engine's slice_ref rows are the system-of-record for what data is
// reachable, and the physical chunks beneath are content-addressed by casstore.
// A GC pass therefore:
//
//  1. marks the live slice set = DISTINCT slice_id in slice_ref;
//  2. deletes the casstore manifest of every stored slice NOT in that set
//     (an orphan whose last referencing inode was unlinked), EXCEPT manifests
//     younger than the safety window;
//  3. runs the casstore blob sweep to reclaim the now-unreferenced chunk/pack
//     blobs (also safety-windowed).
//
// The safety window guards the GC↔write race: fileio writes a slice's manifest
// BEFORE committing its slice_ref row, so a just-written slice momentarily looks
// like an orphan. The window (default 24h in prod, tiny in tests) ensures an
// in-flight write's data is never reclaimed out from under it.
package gc

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// GC collects orphaned slice data for an engine + local chunk store.
type GC struct {
	engine *meta.Engine
	store  *chunkstore.CasStore
	cas    *snapshot.ChunkedGC
	now    func() time.Time
	safety time.Duration
	batch  int // manifest-walk batch size (0 = sliceGCBatch); overridable in tests
}

// New builds a GC over the engine and a local chunk store bundle.
func New(engine *meta.Engine, local *chunkstore.Local) *GC {
	return &GC{engine: engine, store: local.Store, cas: local.GC, now: time.Now}
}

// SetSafetyWindow sets the minimum age before an orphaned slice manifest or
// chunk blob may be reclaimed. Propagated to the casstore blob sweep.
func (g *GC) SetSafetyWindow(d time.Duration) {
	g.safety = d
	g.cas.SafetyWindow = d
}

// SetClock overrides the clock (tests). Propagated to the casstore sweep so
// both layers agree on "now".
func (g *GC) SetClock(now func() time.Time) {
	g.now = now
	g.cas.SetClock(now)
}

// SetBatchSize overrides the manifest-walk batch size (0 keeps the default). Used
// by tests to force the multi-batch path; production leaves it at sliceGCBatch.
func (g *GC) SetBatchSize(n int) { g.batch = n }

// Result summarizes one GC pass.
type Result struct {
	LiveSlices          int               // live slice manifests seen (referenced by some file)
	SlicesScanned       int               // stored slice manifests examined
	SlicesDeleted       int               // orphaned slice manifests reclaimed this pass
	SlicesDeferredYoung int               // orphans skipped for being younger than the window
	Cas                 snapshot.GCResult // underlying casstore blob sweep result
}

// sliceGCBatch bounds how many stored slice ids a pass holds in memory at once:
// the manifest walk is processed in batches, each checked against slice_ref with a
// single batched query, so peak RAM is bounded by the batch — not the total slice
// count (TECH_DEBT #28). Well under Postgres' parameter limit.
const sliceGCBatch = 5000

// Run executes one GC pass. It streams the stored slice manifests, and for each
// batch resolves liveness against slice_ref and reclaims the orphans older than the
// safety window — so neither the live set nor the orphan set is ever fully
// materialised. The casstore blob sweep follows (itself bounded by its live set).
func (g *GC) Run(ctx context.Context) (Result, error) {
	var res Result
	bs := sliceGCBatch
	if g.batch > 0 {
		bs = g.batch
	}
	type cand struct {
		id      uint64
		created time.Time
	}
	batch := make([]cand, 0, bs)

	// Aged orphans accumulate here and are reclaimed AFTER the walk completes —
	// never mid-walk, so the manifest enumeration is not mutated under itself.
	var reclaim []uint64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Aggregate the newest CreatedAt per id (defensive against a slice id with
		// multiple manifest versions in one batch — mlfs slices are write-once, so
		// normally one each; if any version is young the slice must be deferred).
		newest := make(map[uint64]time.Time, len(batch))
		for _, c := range batch {
			if t, ok := newest[c.id]; !ok || c.created.After(t) {
				newest[c.id] = c.created
			}
		}
		ids := make([]uint64, 0, len(newest))
		for id := range newest {
			ids = append(ids, id)
		}
		live, err := g.engine.LiveSliceIDsIn(ctx, ids)
		if err != nil {
			return fmt.Errorf("mlfs gc: live slice batch: %w", err)
		}
		res.LiveSlices += len(live)
		for id, created := range newest {
			if _, ok := live[id]; ok {
				continue // still referenced
			}
			if g.safety > 0 && g.now().Sub(created) < g.safety {
				res.SlicesDeferredYoung++
				continue
			}
			reclaim = append(reclaim, id)
		}
		batch = batch[:0]
		return nil
	}

	if err := g.store.WalkSliceIDs(ctx, func(id uint64, createdAt time.Time) error {
		res.SlicesScanned++
		batch = append(batch, cand{id: id, created: createdAt})
		if len(batch) >= bs {
			return flush()
		}
		return nil
	}); err != nil {
		return res, fmt.Errorf("mlfs gc: walk slices: %w", err)
	}
	if err := flush(); err != nil {
		return res, err
	}
	for _, id := range reclaim {
		if err := g.store.Remove(ctx, id); err != nil {
			return res, fmt.Errorf("mlfs gc: remove orphan slice %d: %w", id, err)
		}
		res.SlicesDeleted++
	}

	casRes, err := g.cas.RunOnce(ctx)
	if err != nil {
		return res, fmt.Errorf("mlfs gc: casstore blob sweep: %w", err)
	}
	res.Cas = casRes
	return res, nil
}
