// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// TestNameTooLong asserts every name-accepting op rejects a component longer
// than NAME_MAX (255 bytes) with ENAMETOOLONG, which the kernel does not
// pre-enforce under default_permissions (pjdfstest */02.t). The guards
// short-circuit before any metadata call, so a nil-meta bridge suffices.
func TestNameTooLong(t *testing.T) {
	b := New(nil, nil, nil)
	long := strings.Repeat("x", nameMax+1)
	want := fuse.Status(syscall.ENAMETOOLONG)
	h := fuse.InHeader{NodeId: uint64(meta.RootInode)}

	cases := []struct {
		op string
		st fuse.Status
	}{
		{"lookup", b.Lookup(nil, &h, long, &fuse.EntryOut{})},
		{"mkdir", b.Mkdir(nil, &fuse.MkdirIn{InHeader: h}, long, &fuse.EntryOut{})},
		{"mknod", b.Mknod(nil, &fuse.MknodIn{InHeader: h}, long, &fuse.EntryOut{})},
		{"create", b.Create(nil, &fuse.CreateIn{InHeader: h}, long, &fuse.CreateOut{})},
		{"unlink", b.Unlink(nil, &h, long)},
		{"rmdir", b.Rmdir(nil, &h, long)},
		{"rename-old", b.Rename(nil, &fuse.RenameIn{InHeader: h}, long, "ok")},
		{"rename-new", b.Rename(nil, &fuse.RenameIn{InHeader: h}, "ok", long)},
		{"symlink", b.Symlink(nil, &h, "target", long, &fuse.EntryOut{})},
		{"link", b.Link(nil, &fuse.LinkIn{InHeader: h}, long, &fuse.EntryOut{})},
	}
	for _, c := range cases {
		if c.st != want {
			t.Errorf("%s(256-byte name) = %v, want ENAMETOOLONG", c.op, c.st)
		}
	}
}
