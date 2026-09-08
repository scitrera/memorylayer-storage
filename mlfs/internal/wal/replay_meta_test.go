// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package wal_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/wal"
)

// openMeta returns a clean engine on a private, throwaway schema (testpg.DB).
// Skips when MLFS_TEST_DATABASE_URL is unset (mirrors the meta engine tests).
func openMeta(t *testing.T) *meta.Engine {
	t.Helper()
	e := meta.Open(testpg.DB(t), 2)
	if err := e.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return e
}

// TestWALReplayReconstructsMeta is the L2.4 crash-recovery acceptance at the
// component level: a write is logged (durable) but its slice_ref never
// committed (crash); on restart, WAL replay re-applies it to the meta engine
// with no loss, and replaying again is idempotent.
func TestWALReplayReconstructsMeta(t *testing.T) {
	e := openMeta(t)
	ctx, c := context.Background(), meta.Background()

	ino, _, st := e.Create(ctx, meta.RootInode, "data", 0o644, c)
	if st != 0 {
		t.Fatalf("Create: %v", st)
	}

	dir := t.TempDir()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	// Log the write (durable) — but DO NOT commit the slice_ref (simulating a
	// crash after the WAL append but before the SQL metadata write).
	const sliceID, sliceLen = 5000, 4096
	if _, err := w.Append(wal.Record{
		Op: wal.OpWrite, Ino: uint64(ino), Indx: 0, Off: 0,
		SliceID: sliceID, Size: sliceLen, SliceOff: 0, SliceLen: sliceLen,
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if slices, _ := e.Read(ctx, ino, 0); len(slices) != 0 {
		t.Fatalf("slice_ref should be absent before replay, got %d", len(slices))
	}
	// "crash": abandon w.

	// The replay handler re-applies logged writes; meta.Write is idempotent on
	// slice_id, so re-applying a committed write is a no-op.
	apply := func(r wal.Record) error {
		if r.Op != wal.OpWrite {
			return nil
		}
		if st := e.Write(ctx, meta.Ino(r.Ino), r.Indx, r.Off,
			meta.Slice{Id: r.SliceID, Size: r.Size, Off: r.SliceOff, Len: r.SliceLen}); st != 0 {
			return fmt.Errorf("meta.Write errno %v", st)
		}
		return nil
	}

	// Restart: reopen + replay.
	w2, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if err := w2.Replay(apply); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	slices, _ := e.Read(ctx, ino, 0)
	if len(slices) != 1 || slices[0].Id != sliceID {
		t.Fatalf("after replay: %d slices %+v", len(slices), slices)
	}
	if a, _ := e.GetAttr(ctx, ino); a.Length != sliceLen {
		t.Errorf("file length after replay: got %d want %d", a.Length, sliceLen)
	}

	// Replay again → idempotent (no duplicate slice).
	if err := w2.Replay(apply); err != nil {
		t.Fatalf("Replay 2: %v", err)
	}
	if slices, _ := e.Read(ctx, ino, 0); len(slices) != 1 {
		t.Errorf("replay not idempotent: %d slices after second replay", len(slices))
	}
}
