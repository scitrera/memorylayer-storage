// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"syscall"
	"testing"
)

func TestXAttrSetGetListRemove(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	// Missing attribute → ENODATA.
	if _, st := e.GetXAttr(ctx, ino, "user.k"); st != syscall.ENODATA {
		t.Fatalf("get missing: want ENODATA, got %v", st)
	}
	// Set + get.
	if st := e.SetXAttr(ctx, ino, "user.k", []byte("v1"), 0); st != 0 {
		t.Fatalf("set: errno %v", st)
	}
	if v, st := e.GetXAttr(ctx, ino, "user.k"); st != 0 || string(v) != "v1" {
		t.Fatalf("get: %q errno %v", v, st)
	}
	// Overwrite (no flags).
	if st := e.SetXAttr(ctx, ino, "user.k", []byte("v2-longer"), 0); st != 0 {
		t.Fatalf("overwrite: errno %v", st)
	}
	if v, _ := e.GetXAttr(ctx, ino, "user.k"); string(v) != "v2-longer" {
		t.Fatalf("overwrite read: %q", v)
	}
	// XattrCreate on an existing name → EEXIST.
	if st := e.SetXAttr(ctx, ino, "user.k", []byte("x"), XattrCreate); st != syscall.EEXIST {
		t.Fatalf("create-existing: want EEXIST, got %v", st)
	}
	// XattrReplace on a missing name → ENODATA.
	if st := e.SetXAttr(ctx, ino, "user.absent", []byte("x"), XattrReplace); st != syscall.ENODATA {
		t.Fatalf("replace-missing: want ENODATA, got %v", st)
	}
	// Second attribute + sorted list.
	if st := e.SetXAttr(ctx, ino, "user.a", []byte("1"), 0); st != 0 {
		t.Fatalf("set a: errno %v", st)
	}
	names, st := e.ListXAttr(ctx, ino)
	if st != 0 {
		t.Fatalf("list: errno %v", st)
	}
	if len(names) != 2 || names[0] != "user.a" || names[1] != "user.k" {
		t.Fatalf("list got %v, want [user.a user.k]", names)
	}
	// Remove, then second remove → ENODATA.
	if st := e.RemoveXAttr(ctx, ino, "user.k"); st != 0 {
		t.Fatalf("remove: errno %v", st)
	}
	if st := e.RemoveXAttr(ctx, ino, "user.k"); st != syscall.ENODATA {
		t.Fatalf("remove again: want ENODATA, got %v", st)
	}
	if _, st := e.GetXAttr(ctx, ino, "user.k"); st != syscall.ENODATA {
		t.Fatalf("get removed: want ENODATA, got %v", st)
	}
	// Set on a missing inode → ENOENT.
	if st := e.SetXAttr(ctx, Ino(1<<60), "user.k", []byte("v"), 0); st != syscall.ENOENT {
		t.Fatalf("set on missing inode: want ENOENT, got %v", st)
	}
}

// TestXAttrDeletedWithInode proves xattrs are reclaimed when their inode is
// deleted (so they don't leak rows after unlink).
func TestXAttrDeletedWithInode(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}
	if st := e.SetXAttr(ctx, ino, "user.k", []byte("v"), 0); st != 0 {
		t.Fatalf("set: errno %v", st)
	}
	if st := e.Unlink(ctx, RootInode, "f", c); st != 0 { // not open → immediate delete
		t.Fatalf("unlink: errno %v", st)
	}
	var cnt int
	if err := e.db.QueryRowContext(ctx, `SELECT count(*) FROM xattr WHERE inode=$1`, int64(ino)).Scan(&cnt); err != nil {
		t.Fatalf("count xattr: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("xattr rows survived inode deletion: %d", cnt)
	}
}
