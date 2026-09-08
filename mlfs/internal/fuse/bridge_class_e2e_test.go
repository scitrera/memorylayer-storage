// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge_test

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// TestBridgeClassifiesOnCreate proves the create-time classification reaches the
// inode: a model/tensor extension stamps store_class = ClassUncompressed, a
// generic file stays ClassDefault. The write path then reads this back
// (storeClassFor) to stamp each slice. Gated on Postgres via newRig.
func TestBridgeClassifiesOnCreate(t *testing.T) {
	rg := newRig(t, t.TempDir())
	ctx := context.Background()

	create := func(name string) meta.Ino {
		t.Helper()
		var out fuse.CreateOut
		in := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: uint64(meta.RootInode)}, Mode: 0o644}
		if st := rg.bridge.Create(nil, in, name, &out); st != fuse.OK {
			t.Fatalf("Create %q: status %v", name, st)
		}
		return meta.Ino(out.EntryOut.NodeId)
	}

	model := create("model.safetensors")
	generic := create("notes.txt")

	ma, st := rg.e.GetAttr(ctx, model)
	if st != 0 {
		t.Fatalf("GetAttr model: %v", st)
	}
	if ma.StoreClass != uint8(chunkstore.ClassUncompressed) {
		t.Errorf("model inode store_class = %d, want ClassUncompressed", ma.StoreClass)
	}

	ga, st := rg.e.GetAttr(ctx, generic)
	if st != 0 {
		t.Fatalf("GetAttr generic: %v", st)
	}
	if ga.StoreClass != uint8(chunkstore.ClassDefault) {
		t.Errorf("generic inode store_class = %d, want ClassDefault", ga.StoreClass)
	}
}
