// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"context"
	"strings"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

// TestVirtualMetricsFile exercises the .mlfs/metrics path end-to-end at the
// RawFileSystem handler level (no kernel mount, no metadata DB needed — the
// magic-inode branches short-circuit before any meta call), proving lookup,
// getattr, open (direct-I/O), paged read, and release all work and that the
// served bytes are the live Prometheus render.
func TestVirtualMetricsFile(t *testing.T) {
	reg, err := metrics.New(context.Background())
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	b := New(nil, nil, nil) // meta/files/flusher unused on the magic path
	b.SetMetrics(reg)
	reg.FuseOp("read") // ensure there is a counter line to serve

	// Lookup .mlfs at the root, then metrics inside it.
	var de fuse.EntryOut
	if st := b.Lookup(nil, &fuse.InHeader{NodeId: uint64(meta.RootInode)}, magicDirName, &de); st != fuse.OK {
		t.Fatalf("lookup .mlfs: %v", st)
	}
	if de.NodeId != uint64(magicDirIno) {
		t.Fatalf("lookup .mlfs NodeId = %#x, want %#x", de.NodeId, magicDirIno)
	}
	var fe fuse.EntryOut
	if st := b.Lookup(nil, &fuse.InHeader{NodeId: uint64(magicDirIno)}, magicMetricsTxt, &fe); st != fuse.OK {
		t.Fatalf("lookup metrics: %v", st)
	}
	if fe.NodeId != uint64(magicMetricsIno) {
		t.Fatalf("lookup metrics NodeId = %#x, want %#x", fe.NodeId, magicMetricsIno)
	}
	if st := b.Lookup(nil, &fuse.InHeader{NodeId: uint64(magicDirIno)}, "nope", &fe); st != fuse.ENOENT {
		t.Fatalf("lookup bogus child = %v, want ENOENT", st)
	}

	// GetAttr: a read-only regular file.
	var ao fuse.AttrOut
	if st := b.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: uint64(magicMetricsIno)}}, &ao); st != fuse.OK {
		t.Fatalf("getattr metrics: %v", st)
	}
	if ao.Mode&0o777 != 0o444 {
		t.Errorf("metrics mode = %o, want 0444", ao.Mode&0o777)
	}

	// Open: direct I/O, with a non-zero handle.
	var oo fuse.OpenOut
	if st := b.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: uint64(magicMetricsIno)}}, &oo); st != fuse.OK {
		t.Fatalf("open metrics: %v", st)
	}
	if oo.OpenFlags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Error("open metrics: expected FOPEN_DIRECT_IO")
	}

	// Read the snapshot and confirm it is the Prometheus render.
	buf := make([]byte, 64<<10)
	res, st := b.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: uint64(magicMetricsIno)},
		Fh:       oo.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != fuse.OK {
		t.Fatalf("read metrics: %v", st)
	}
	got, st := res.Bytes(buf)
	if st != fuse.OK {
		t.Fatalf("read metrics bytes: %v", st)
	}
	if !strings.Contains(string(got), "mlfs_fuse_ops_total") {
		t.Errorf("served body missing expected metric:\n%s", got)
	}

	// Release frees the parked snapshot; a subsequent read of that handle is empty.
	b.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: uint64(magicMetricsIno)}, Fh: oo.Fh})
	res2, _ := b.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: uint64(magicMetricsIno)},
		Fh:       oo.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if g, _ := res2.Bytes(buf); len(g) != 0 {
		t.Errorf("read after release returned %d bytes, want 0", len(g))
	}
}
