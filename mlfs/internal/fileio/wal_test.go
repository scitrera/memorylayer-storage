// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/wal"
)

// walStack is a data-path stack backed by a real WAL.
type walStack struct {
	f     *fileio.Files
	e     *meta.Engine
	local *chunkstore.Local
	w     *wal.WAL
}

func openWALStack(t *testing.T) *walStack {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 4)
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	local, err := chunkstore.NewLocal(t.TempDir(), domain, 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	t.Cleanup(func() { _ = local.Chunks.Close(ctx) })
	w, err := wal.Open(t.TempDir())
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return &walStack{f: fileio.NewWithWAL(e, local.Store, w), e: e, local: local, w: w}
}

// TestWALRecoversUncommittedWrite is the L2.4 crash-durability property: data is
// staged + logged but the metadata commit is lost (crash). On restart ReplayWAL
// re-applies the metadata write, so the acknowledged bytes are readable.
func TestWALRecoversUncommittedWrite(t *testing.T) {
	s := openWALStack(t)
	ctx := context.Background()
	f, _, st := s.e.Create(ctx, meta.RootInode, "f", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	data := bytes.Repeat([]byte("walrus!"), 1000) // 7000 bytes

	// Simulate the crash window: allocate a slice, stage the data, log the
	// record — but do NOT commit the slice_ref (the "crash").
	id, err := s.e.NewSlice(ctx)
	if err != nil {
		t.Fatalf("new slice: %v", err)
	}
	if err := s.local.Store.Write(ctx, id, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("stage chunk: %v", err)
	}
	if _, err := s.w.Append(wal.Record{
		Op: wal.OpWrite, Ino: uint64(f), Indx: 0, Off: 0,
		SliceID: id, Size: uint32(len(data)), SliceOff: 0, SliceLen: uint32(len(data)),
	}); err != nil {
		t.Fatalf("wal append: %v", err)
	}

	// Pre-recovery: the metadata never committed, so the file reads empty.
	pre := make([]byte, len(data))
	if n, err := s.f.Read(ctx, f, 0, pre); err != nil || n != 0 {
		t.Fatalf("pre-recovery read: n=%d err=%v (want 0, nil)", n, err)
	}

	// Recover.
	res, err := fileio.ReplayWAL(ctx, s.w, s.e)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Applied != 1 || res.Skipped != 0 {
		t.Fatalf("replay result: %+v (want applied=1 skipped=0)", res)
	}

	// Post-recovery: the bytes are readable.
	got := make([]byte, len(data))
	n, err := s.f.Read(ctx, f, 0, got)
	if err != nil {
		t.Fatalf("post-recovery read: %v", err)
	}
	if n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("post-recovery data mismatch: n=%d", n)
	}
}

// TestWALReplayIdempotent confirms re-running recovery over already-committed
// writes is a harmless no-op (the slice_id natural key absorbs duplicates) and
// leaves the data intact.
func TestWALReplayIdempotent(t *testing.T) {
	s := openWALStack(t)
	ctx := context.Background()
	f, _, st := s.e.Create(ctx, meta.RootInode, "f", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	data := bytes.Repeat([]byte("x"), 5000)
	if _, err := s.f.Write(ctx, f, 0, data, chunkstore.ClassDefault); err != nil { // normal path: WAL + meta both land
		t.Fatalf("write: %v", err)
	}
	// Replaying committed records re-applies them idempotently.
	res, err := fileio.ReplayWAL(ctx, s.w, s.e)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Applied != 1 {
		t.Fatalf("replay applied=%d (want 1 idempotent no-op)", res.Applied)
	}
	got := make([]byte, len(data))
	if n, err := s.f.Read(ctx, f, 0, got); err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read after idempotent replay: n=%d err=%v", n, err)
	}
}
