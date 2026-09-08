// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/gc"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

const domain = "gctest"

type rig struct {
	e     *meta.Engine
	files *fileio.Files
	local *chunkstore.Local
}

func newRig(t *testing.T) *rig {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 9)
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	local, err := chunkstore.NewLocal(t.TempDir(), domain, 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	t.Cleanup(func() { _ = local.Chunks.Close(ctx) })
	return &rig{e: e, files: fileio.New(e, local.Store), local: local}
}

func (r *rig) blobCount(t *testing.T) int {
	t.Helper()
	n := 0
	for _, p := range []blobstore.ID{blobstore.ID("chunk-" + domain + "-"), blobstore.ID("pack-" + domain + "-")} {
		if err := r.local.Chunks.ListBlobs(context.Background(), p, func(blobstore.Metadata) error { n++; return nil }); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return n
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// TestGCReclaimsOrphanedSlices is the L2.7 acceptance at component scope: write
// a file, delete it, and GC reclaims its slices + chunk blobs; a live file's
// data survives GC.
func TestGCReclaimsOrphanedSlices(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)
	g := gc.New(r.e, r.local)
	g.SetSafetyWindow(0) // reclaim immediately

	ino, _, st := r.e.Create(ctx, meta.RootInode, "f", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}
	if _, err := r.files.Write(ctx, ino, 0, randomBytes(t, 4<<20), chunkstore.ClassDefault); err != nil {
		t.Fatalf("write: %v", err)
	}
	before := r.blobCount(t)
	if before == 0 {
		t.Fatal("expected chunk/pack blobs after a 4 MiB write")
	}

	// GC while the file is live: nothing reclaimed.
	res, err := g.Run(ctx)
	if err != nil {
		t.Fatalf("gc (live): %v", err)
	}
	if res.SlicesDeleted != 0 {
		t.Fatalf("live file: %d slices deleted, want 0", res.SlicesDeleted)
	}
	if got := r.blobCount(t); got != before {
		t.Fatalf("live file blobs should survive GC: before=%d after=%d", before, got)
	}

	// Delete the file → its slice_ref rows are gone, slices become orphans.
	if st := r.e.Unlink(ctx, meta.RootInode, "f", meta.Background()); st != 0 {
		t.Fatalf("unlink: errno %v", st)
	}
	if live, err := r.e.LiveSliceIDs(ctx); err != nil || len(live) != 0 {
		t.Fatalf("expected 0 live slices after unlink, got %d (err %v)", len(live), err)
	}

	// GC now reclaims the orphaned slices and their chunk blobs.
	res, err = g.Run(ctx)
	if err != nil {
		t.Fatalf("gc (orphan): %v", err)
	}
	if res.SlicesDeleted == 0 {
		t.Fatal("expected orphan slice manifests to be deleted")
	}
	if res.Cas.ChunksReclaimed == 0 {
		t.Fatal("expected chunk blobs to be reclaimed")
	}
	if got := r.blobCount(t); got != 0 {
		t.Fatalf("expected all blobs reclaimed, %d remain", got)
	}
}

// TestGCBatchedSweep forces the cursored multi-batch path (small batch size over
// many files) and verifies orphan reclamation is correct ACROSS batch boundaries:
// deleted files' slices are reclaimed, live files' data survives, regardless of how
// the walk's batches split.
func TestGCBatchedSweep(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)
	g := gc.New(r.e, r.local)
	g.SetSafetyWindow(0)
	g.SetBatchSize(3) // tiny → many flushes over the file set

	const n = 11
	inos := make([]meta.Ino, n)
	for i := 0; i < n; i++ {
		ino, _, st := r.e.Create(ctx, meta.RootInode, fmtName(i), 0o644, meta.Background())
		if st != 0 {
			t.Fatalf("create %d: errno %v", i, st)
		}
		inos[i] = ino
		if _, err := r.files.Write(ctx, ino, 0, randomBytes(t, 1<<20), chunkstore.ClassDefault); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Delete the even-indexed files; odd ones stay live.
	deleted := 0
	for i := 0; i < n; i += 2 {
		if st := r.e.Unlink(ctx, meta.RootInode, fmtName(i), meta.Background()); st != 0 {
			t.Fatalf("unlink %d: errno %v", i, st)
		}
		deleted++
	}

	res, err := g.Run(ctx)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if res.SlicesDeleted != deleted {
		t.Errorf("SlicesDeleted = %d, want %d (one slice per deleted 1 MiB file)", res.SlicesDeleted, deleted)
	}
	if res.LiveSlices != n-deleted {
		t.Errorf("LiveSlices = %d, want %d", res.LiveSlices, n-deleted)
	}

	// The surviving (odd) files must still read back; their data wasn't reclaimed.
	for i := 1; i < n; i += 2 {
		buf := make([]byte, 1<<20)
		if _, err := r.files.Read(ctx, inos[i], 0, buf); err != nil {
			t.Errorf("live file %d unreadable after batched GC: %v", i, err)
		}
	}

	// A second pass is a no-op (everything is either live or already reclaimed).
	res2, err := g.Run(ctx)
	if err != nil {
		t.Fatalf("gc 2: %v", err)
	}
	if res2.SlicesDeleted != 0 {
		t.Errorf("second pass deleted %d, want 0", res2.SlicesDeleted)
	}
}

func fmtName(i int) string { return "f" + string(rune('0'+i/10)) + string(rune('0'+i%10)) }

// TestGCSafetyWindowDefersYoungOrphans proves a freshly-orphaned slice is NOT
// reclaimed while younger than the safety window (the GC↔write race guard), and
// IS reclaimed once the clock advances past it.
func TestGCSafetyWindowDefersYoungOrphans(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)
	g := gc.New(r.e, r.local)
	g.SetSafetyWindow(time.Hour)

	ino, _, st := r.e.Create(ctx, meta.RootInode, "f", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}
	if _, err := r.files.Write(ctx, ino, 0, randomBytes(t, 1<<20), chunkstore.ClassDefault); err != nil {
		t.Fatalf("write: %v", err)
	}
	if st := r.e.Unlink(ctx, meta.RootInode, "f", meta.Background()); st != 0 {
		t.Fatalf("unlink: errno %v", st)
	}
	before := r.blobCount(t)
	if before == 0 {
		t.Fatal("expected blobs after write")
	}

	// Clock at ~now: the orphan is younger than the 1h window → deferred.
	g.SetClock(func() time.Time { return time.Now() })
	res, err := g.Run(ctx)
	if err != nil {
		t.Fatalf("gc (young): %v", err)
	}
	if res.SlicesDeferredYoung == 0 {
		t.Fatal("expected the young orphan to be deferred")
	}
	if res.SlicesDeleted != 0 {
		t.Fatalf("young orphan should not be deleted, got %d", res.SlicesDeleted)
	}
	if got := r.blobCount(t); got != before {
		t.Fatalf("young orphan blobs should survive: before=%d after=%d", before, got)
	}

	// Advance the clock past the window → reclaimed.
	g.SetClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
	res, err = g.Run(ctx)
	if err != nil {
		t.Fatalf("gc (aged): %v", err)
	}
	if res.SlicesDeleted == 0 {
		t.Fatal("expected the aged orphan to be reclaimed")
	}
	if got := r.blobCount(t); got != 0 {
		t.Fatalf("expected all blobs reclaimed after window, %d remain", got)
	}
}
