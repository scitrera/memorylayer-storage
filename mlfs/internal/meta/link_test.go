// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"syscall"
	"testing"
)

func TestLinkHardlink(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "a", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	a, st := e.Link(ctx, ino, RootInode, "b", c)
	if st != 0 {
		t.Fatalf("link: errno %v", st)
	}
	if a.Nlink != 2 {
		t.Fatalf("nlink after link = %d, want 2", a.Nlink)
	}
	// Both names resolve to the same inode.
	if bino, _, st := e.Lookup(ctx, RootInode, "b"); st != 0 || bino != ino {
		t.Fatalf("lookup b: ino=%d st=%v (want %d)", bino, st, ino)
	}
	// Linking over an existing name → EEXIST.
	if _, st := e.Link(ctx, ino, RootInode, "b", c); st != syscall.EEXIST {
		t.Fatalf("link over existing name: want EEXIST, got %v", st)
	}
	// Unlink one name: the inode survives (nlink 1) and the other name resolves.
	if st := e.Unlink(ctx, RootInode, "a", c); st != 0 {
		t.Fatalf("unlink a: errno %v", st)
	}
	at, st := e.GetAttr(ctx, ino)
	if st != 0 {
		t.Fatalf("getattr after unlink a: errno %v", st)
	}
	if at.Nlink != 1 {
		t.Fatalf("nlink after unlink a = %d, want 1", at.Nlink)
	}
	if _, _, st := e.Lookup(ctx, RootInode, "b"); st != 0 {
		t.Fatalf("b should still resolve after unlinking a: %v", st)
	}
	// Unlink the last name: the inode is gone.
	if st := e.Unlink(ctx, RootInode, "b", c); st != 0 {
		t.Fatalf("unlink b: errno %v", st)
	}
	if _, st := e.GetAttr(ctx, ino); st != syscall.ENOENT {
		t.Fatalf("inode should be gone after last unlink, got %v", st)
	}
}

func TestLinkDirectoryRefused(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	dino, _, st := e.Mkdir(ctx, RootInode, "d", 0o755, c)
	if st != 0 {
		t.Fatalf("mkdir: errno %v", st)
	}
	if _, st := e.Link(ctx, dino, RootInode, "dlink", c); st != syscall.EPERM {
		t.Fatalf("hardlink to a directory: want EPERM, got %v", st)
	}
}
