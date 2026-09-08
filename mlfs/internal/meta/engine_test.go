// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// openTestEngine returns a migrated engine on a private, throwaway schema
// (testpg.DB), so each test starts fresh and runs in isolation. Skips when
// MLFS_TEST_DATABASE_URL is unset (mirrors the casstore/blobgw pattern).
func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	e := Open(testpg.DB(t), 1)
	if err := e.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return e
}

func ctxBg() context.Context { return context.Background() }

func TestRootBootstrap(t *testing.T) {
	e := openTestEngine(t)
	a, st := e.GetAttr(ctxBg(), RootInode)
	if st != 0 {
		t.Fatalf("GetAttr root: errno %v", st)
	}
	if a.Typ != TypeDirectory || a.Nlink != 2 {
		t.Errorf("root attr wrong: typ=%d nlink=%d", a.Typ, a.Nlink)
	}
}

func TestMkdirLookupAndParentLinks(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, attr, st := e.Mkdir(ctx, RootInode, "sub", 0o755, c)
	if st != 0 {
		t.Fatalf("Mkdir: %v", st)
	}
	if attr.Typ != TypeDirectory || attr.Nlink != 2 {
		t.Errorf("new dir attr: %+v", attr)
	}
	gi, ga, st := e.Lookup(ctx, RootInode, "sub")
	if st != 0 || gi != ino || ga.Typ != TypeDirectory {
		t.Errorf("Lookup sub: ino=%d/%d st=%v", gi, ino, st)
	}
	// Root gained a subdirectory → nlink 3.
	root, _ := e.GetAttr(ctx, RootInode)
	if root.Nlink != 3 {
		t.Errorf("root nlink after mkdir: got %d want 3", root.Nlink)
	}
	// Duplicate name → EEXIST.
	if _, _, st := e.Mkdir(ctx, RootInode, "sub", 0o755, c); st != syscall.EEXIST {
		t.Errorf("dup mkdir: got %v want EEXIST", st)
	}
}

func TestCreateUnlink(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	_, _, st := e.Create(ctx, RootInode, "f.txt", 0o644, c)
	if st != 0 {
		t.Fatalf("Create: %v", st)
	}
	if st := e.Unlink(ctx, RootInode, "f.txt", c); st != 0 {
		t.Fatalf("Unlink: %v", st)
	}
	if _, _, st := e.Lookup(ctx, RootInode, "f.txt"); st != syscall.ENOENT {
		t.Errorf("after unlink: got %v want ENOENT", st)
	}
	// Unlink of a directory → EISDIR.
	e.Mkdir(ctx, RootInode, "d", 0o755, c)
	if st := e.Unlink(ctx, RootInode, "d", c); st != syscall.EISDIR {
		t.Errorf("unlink dir: got %v want EISDIR", st)
	}
}

func TestRmdir(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	di, _, _ := e.Mkdir(ctx, RootInode, "d", 0o755, c)
	// Non-empty → ENOTEMPTY.
	e.Create(ctx, di, "child", 0o644, c)
	if st := e.Rmdir(ctx, RootInode, "d", c); st != syscall.ENOTEMPTY {
		t.Errorf("rmdir non-empty: got %v want ENOTEMPTY", st)
	}
	e.Unlink(ctx, di, "child", c)
	if st := e.Rmdir(ctx, RootInode, "d", c); st != 0 {
		t.Fatalf("rmdir empty: %v", st)
	}
	root, _ := e.GetAttr(ctx, RootInode)
	if root.Nlink != 2 {
		t.Errorf("root nlink after rmdir: got %d want 2", root.Nlink)
	}
}

func TestReaddir(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	for _, n := range []string{"a", "b", "c"} {
		e.Create(ctx, RootInode, n, 0o644, c)
	}
	entries, st := e.Readdir(ctx, RootInode, true)
	if st != 0 {
		t.Fatalf("Readdir: %v", st)
	}
	names := map[string]bool{}
	for _, en := range entries {
		names[en.Name] = true
	}
	for _, want := range []string{".", "..", "a", "b", "c"} {
		if !names[want] {
			t.Errorf("Readdir missing %q (got %v)", want, names)
		}
	}
}

func TestSymlink(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, attr, st := e.Symlink(ctx, RootInode, "lnk", "/target/path", c)
	if st != 0 || attr.Typ != TypeSymlink {
		t.Fatalf("Symlink: st=%v typ=%d", st, attr.Typ)
	}
	target, st := e.ReadLink(ctx, ino)
	if st != 0 || string(target) != "/target/path" {
		t.Errorf("ReadLink: %q (%v)", target, st)
	}
}

func TestSetAttr(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	got, st := e.SetAttr(ctx, ino, SetMode|SetSize, &Attr{Mode: 0o600, Length: 4096}, c)
	if st != 0 {
		t.Fatalf("SetAttr: %v", st)
	}
	if got.Mode != 0o600 || got.Length != 4096 {
		t.Errorf("SetAttr result: mode=%o len=%d", got.Mode, got.Length)
	}
}

func TestWriteReadSlices(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "data", 0o644, c)
	if st := e.Write(ctx, ino, 0, 0, Slice{Id: 111, Size: 1024, Off: 0, Len: 1024}); st != 0 {
		t.Fatalf("Write: %v", st)
	}
	if st := e.Write(ctx, ino, 0, 1024, Slice{Id: 222, Size: 512, Off: 0, Len: 512}); st != 0 {
		t.Fatalf("Write 2: %v", st)
	}
	slices, st := e.Read(ctx, ino, 0)
	if st != 0 {
		t.Fatalf("Read: %v", st)
	}
	if len(slices) != 2 || slices[0].Id != 111 || slices[1].Id != 222 {
		t.Errorf("slices: %+v", slices)
	}
	a, _ := e.GetAttr(ctx, ino)
	if a.Length != 1536 {
		t.Errorf("length after writes: got %d want 1536", a.Length)
	}
}

func TestRenameFileMoveAndOverwrite(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	src, _, _ := e.Create(ctx, RootInode, "src", 0o644, c)
	da, _, _ := e.Mkdir(ctx, RootInode, "dir", 0o755, c)

	// Move src → dir/moved.
	gi, _, st := e.Rename(ctx, RootInode, "src", da, "moved", 0, c)
	if st != 0 || gi != src {
		t.Fatalf("Rename move: st=%v ino=%d/%d", st, gi, src)
	}
	if _, _, st := e.Lookup(ctx, RootInode, "src"); st != syscall.ENOENT {
		t.Errorf("src should be gone: %v", st)
	}
	if li, _, st := e.Lookup(ctx, da, "moved"); st != 0 || li != src {
		t.Errorf("moved lookup: %v %d", st, li)
	}

	// Overwrite: create two files, rename one onto the other.
	keep, _, _ := e.Create(ctx, RootInode, "keep", 0o644, c)
	e.Create(ctx, RootInode, "victim", 0o644, c)
	if _, _, st := e.Rename(ctx, RootInode, "keep", RootInode, "victim", 0, c); st != 0 {
		t.Fatalf("Rename overwrite: %v", st)
	}
	li, _, st := e.Lookup(ctx, RootInode, "victim")
	if st != 0 || li != keep {
		t.Errorf("victim should now be keep's inode: %v %d/%d", st, li, keep)
	}

	// NoReplace onto existing → EEXIST.
	e.Create(ctx, RootInode, "x", 0o644, c)
	e.Create(ctx, RootInode, "y", 0o644, c)
	if _, _, st := e.Rename(ctx, RootInode, "x", RootInode, "y", RenameNoReplace, c); st != syscall.EEXIST {
		t.Errorf("noreplace: got %v want EEXIST", st)
	}
}

func TestRenameDirAcrossParents(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	a, _, _ := e.Mkdir(ctx, RootInode, "a", 0o755, c)
	b, _, _ := e.Mkdir(ctx, RootInode, "b", 0o755, c)
	mv, _, _ := e.Mkdir(ctx, a, "mv", 0o755, c)

	aBefore, _ := e.GetAttr(ctx, a)
	bBefore, _ := e.GetAttr(ctx, b)
	if _, _, st := e.Rename(ctx, a, "mv", b, "mv", 0, c); st != 0 {
		t.Fatalf("rename dir across parents: %v", st)
	}
	aAfter, _ := e.GetAttr(ctx, a)
	bAfter, _ := e.GetAttr(ctx, b)
	if aAfter.Nlink != aBefore.Nlink-1 {
		t.Errorf("source parent nlink: %d → %d (want -1)", aBefore.Nlink, aAfter.Nlink)
	}
	if bAfter.Nlink != bBefore.Nlink+1 {
		t.Errorf("dest parent nlink: %d → %d (want +1)", bBefore.Nlink, bAfter.Nlink)
	}
	moved, _ := e.GetAttr(ctx, mv)
	if moved.Parent != b {
		t.Errorf("moved dir parent: got %d want %d", moved.Parent, b)
	}
}

func TestRenameExchange(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()

	// Two files in the same directory swap names atomically.
	fa, _, _ := e.Create(ctx, RootInode, "a", 0o644, c)
	fb, _, _ := e.Create(ctx, RootInode, "b", 0o600, c)
	if _, _, st := e.Rename(ctx, RootInode, "a", RootInode, "b", RenameExchange, c); st != 0 {
		t.Fatalf("exchange a<->b: %v", st)
	}
	if li, _, st := e.Lookup(ctx, RootInode, "a"); st != 0 || li != fb {
		t.Errorf("after exchange, \"a\" should resolve to fb: ino=%d st=%v", li, st)
	}
	if li, _, st := e.Lookup(ctx, RootInode, "b"); st != 0 || li != fa {
		t.Errorf("after exchange, \"b\" should resolve to fa: ino=%d st=%v", li, st)
	}

	// Exchange when a side is missing → ENOENT, and nothing changes.
	if _, _, st := e.Rename(ctx, RootInode, "a", RootInode, "nope", RenameExchange, c); st != syscall.ENOENT {
		t.Errorf("exchange with missing dst: got %v want ENOENT", st)
	}
	if li, _, _ := e.Lookup(ctx, RootInode, "a"); li != fb {
		t.Errorf("failed exchange must not mutate: \"a\" ino=%d want %d", li, fb)
	}

	// Exchange a directory and a file across parents: each lands in the other's
	// parent, and the directory's parent-nlink moves with it.
	d1, _, _ := e.Mkdir(ctx, RootInode, "d1", 0o755, c)
	d2, _, _ := e.Mkdir(ctx, RootInode, "d2", 0o755, c)
	sub, _, _ := e.Mkdir(ctx, d1, "sub", 0o755, c)    // directory living under d1
	file, _, _ := e.Create(ctx, d2, "file", 0o644, c) // file living under d2
	d1Before, _ := e.GetAttr(ctx, d1)
	d2Before, _ := e.GetAttr(ctx, d2)
	if _, _, st := e.Rename(ctx, d1, "sub", d2, "file", RenameExchange, c); st != 0 {
		t.Fatalf("exchange dir<->file across parents: %v", st)
	}
	if li, _, st := e.Lookup(ctx, d1, "sub"); st != 0 || li != file {
		t.Errorf("d1/sub should now be the file: ino=%d st=%v", li, st)
	}
	if li, _, st := e.Lookup(ctx, d2, "file"); st != 0 || li != sub {
		t.Errorf("d2/file should now be the dir: ino=%d st=%v", li, st)
	}
	subAttr, _ := e.GetAttr(ctx, sub)
	if subAttr.Parent != d2 {
		t.Errorf("moved dir parent: got %d want %d", subAttr.Parent, d2)
	}
	fileAttr, _ := e.GetAttr(ctx, file)
	if fileAttr.Parent != d1 {
		t.Errorf("moved file parent: got %d want %d", fileAttr.Parent, d1)
	}
	// The directory left d1 (-1 subdir) and entered d2 (+1 subdir); the file
	// carries no nlink. So d1 loses one and d2 gains one.
	d1After, _ := e.GetAttr(ctx, d1)
	d2After, _ := e.GetAttr(ctx, d2)
	if d1After.Nlink != d1Before.Nlink-1 {
		t.Errorf("d1 nlink: %d → %d (want -1)", d1Before.Nlink, d1After.Nlink)
	}
	if d2After.Nlink != d2Before.Nlink+1 {
		t.Errorf("d2 nlink: %d → %d (want +1)", d2Before.Nlink, d2After.Nlink)
	}

	// WHITEOUT stays unsupported.
	e.Create(ctx, RootInode, "w", 0o644, c)
	if _, _, st := e.Rename(ctx, RootInode, "w", RootInode, "w2", RenameWhiteout, c); st != syscall.ENOSYS {
		t.Errorf("whiteout: got %v want ENOSYS", st)
	}
}

func TestInodeUniquenessAndPrefix(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	seen := map[Ino]bool{}
	const n = 3000 // crosses the inodeBatch (1024) boundary several times
	for i := 0; i < n; i++ {
		ino, _, st := e.Create(ctx, RootInode, "f"+itoa(i), 0o644, c)
		if st != 0 {
			t.Fatalf("Create %d: %v", i, st)
		}
		if seen[ino] {
			t.Fatalf("duplicate inode %d at i=%d", ino, i)
		}
		seen[ino] = true
		if uint16(uint64(ino)>>48) != e.prefix {
			t.Fatalf("inode %d missing region/session prefix %d", ino, e.prefix)
		}
	}
}

func TestChangelogOrdering(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	e.Mkdir(ctx, RootInode, "d", 0o755, c)
	e.Create(ctx, RootInode, "f", 0o644, c)
	e.Unlink(ctx, RootInode, "f", c)
	var lsns []int64
	var ops []uint8
	if err := e.ScanChangelog(ctx, 0, func(ce ChangeEntry) error {
		lsns = append(lsns, ce.LSN)
		ops = append(ops, ce.Op)
		return nil
	}); err != nil {
		t.Fatalf("ScanChangelog: %v", err)
	}
	if len(lsns) < 3 {
		t.Fatalf("expected >=3 changelog entries, got %d", len(lsns))
	}
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("lsn not monotonic: %v", lsns)
		}
	}
}

func TestOwnershipLeaseAndFencing(t *testing.T) {
	e := openTestEngine(t)
	ctx := ctxBg()

	// Acquire fresh → generation 1.
	l, held, err := e.AcquireOrResume(ctx, "ws/scope", "nodeA", 30_000)
	if err != nil || !held || l.Generation != 1 {
		t.Fatalf("acquire: held=%v gen=%d err=%v", held, l.Generation, err)
	}
	// Another node, live lease → not held, generation unchanged.
	l2, held2, _ := e.AcquireOrResume(ctx, "ws/scope", "nodeB", 30_000)
	if held2 || l2.Generation != 1 {
		t.Errorf("nodeB should not steal a live lease: held=%v gen=%d", held2, l2.Generation)
	}
	// nodeA refresh at gen 1 → ok; at wrong gen → fenced out.
	if ok, _ := e.Refresh(ctx, "ws/scope", "nodeA", 1, 30_000); !ok {
		t.Errorf("nodeA refresh should succeed")
	}
	if ok, _ := e.Refresh(ctx, "ws/scope", "nodeA", 99, 30_000); ok {
		t.Errorf("refresh at wrong generation should fail")
	}
	// Expire (ttl 0) and let nodeB steal → generation bumps to 2.
	e.AcquireOrResume(ctx, "ws/expired", "nodeA", 0)
	l3, held3, _ := e.AcquireOrResume(ctx, "ws/expired", "nodeB", 30_000)
	if !held3 || l3.Generation != 2 {
		t.Errorf("nodeB steal expired: held=%v gen=%d want gen 2", held3, l3.Generation)
	}
	// nodeA, fenced out at gen 1, is no longer valid.
	if ok, _ := e.Fenced(ctx, "ws/expired", 1); ok {
		t.Errorf("stale generation 1 should be fenced out")
	}
	if ok, _ := e.Fenced(ctx, "ws/expired", 2); !ok {
		t.Errorf("current generation 2 should be valid")
	}
}

// TestConcurrent exercises concurrent metadata writes (acceptance): many
// goroutines mkdir distinct names in one parent + concurrent setattr on one
// node. Parent nlink must reflect every subdir with no lost updates.
func TestConcurrent(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	base, _, _ := e.Mkdir(ctx, RootInode, "base", 0o755, c)
	target, _, _ := e.Create(ctx, base, "target", 0o644, c)

	const workers = 24
	var wg sync.WaitGroup
	errs := make(chan error, workers*2)
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if _, _, st := e.Mkdir(ctx, base, "d"+itoa(i), 0o755, c); st != 0 {
				errs <- &opErr{"mkdir", st}
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			if _, st := e.SetAttr(ctx, target, SetMode, &Attr{Mode: uint16(0o600 + i%64)}, c); st != 0 {
				errs <- &opErr{"setattr", st}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent op failed: %v", err)
	}
	// base started at nlink 2, gained `workers` subdirectories.
	a, _ := e.GetAttr(ctx, base)
	if a.Nlink != uint32(2+workers) {
		t.Errorf("base nlink after %d concurrent mkdirs: got %d want %d", workers, a.Nlink, 2+workers)
	}
	ents, _ := e.Readdir(ctx, base, false)
	// workers subdirs + target file + "." + ".."
	if len(ents) != workers+3 {
		t.Errorf("base entries: got %d want %d", len(ents), workers+3)
	}
}

// TestOpenUnlinkedSurvivesUntilClose: a file unlinked while open is kept
// (data readable) and deleted only on the last Close — POSIX semantics.
func TestOpenUnlinkedSurvivesUntilClose(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	if st := e.Write(ctx, ino, 0, 0, Slice{Id: 7000, Size: 16, Off: 0, Len: 16}); st != 0 {
		t.Fatalf("Write: %v", st)
	}
	if _, st := e.Open(ctx, ino); st != 0 { // holds an open handle
		t.Fatalf("Open: %v", st)
	}
	if st := e.Unlink(ctx, RootInode, "f", c); st != 0 {
		t.Fatalf("Unlink: %v", st)
	}
	// Name is gone, but the open inode + its data survive.
	if _, _, st := e.Lookup(ctx, RootInode, "f"); st != syscall.ENOENT {
		t.Errorf("Lookup after unlink: got %v want ENOENT", st)
	}
	if _, st := e.GetAttr(ctx, ino); st != 0 {
		t.Errorf("open-unlinked inode should remain accessible: %v", st)
	}
	if slices, _ := e.ReadSlices(ctx, ino, 0); len(slices) != 1 {
		t.Errorf("open-unlinked data should remain: %d slices", len(slices))
	}
	// Last close finally deletes the inode + its slice rows.
	if st := e.Close(ctx, ino); st != 0 {
		t.Fatalf("Close: %v", st)
	}
	if _, st := e.GetAttr(ctx, ino); st != syscall.ENOENT {
		t.Errorf("after last close inode should be gone: %v", st)
	}
	if slices, _ := e.ReadSlices(ctx, ino, 0); len(slices) != 0 {
		t.Errorf("slice rows should be deleted on final close: %d", len(slices))
	}
}

func TestUnlinkNotOpenDeletesImmediately(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	e.Write(ctx, ino, 0, 0, Slice{Id: 7001, Size: 8, Off: 0, Len: 8})
	if st := e.Unlink(ctx, RootInode, "f", c); st != 0 {
		t.Fatalf("Unlink: %v", st)
	}
	if _, st := e.GetAttr(ctx, ino); st != syscall.ENOENT {
		t.Errorf("unlinked (not open) inode should be gone: %v", st)
	}
	if slices, _ := e.ReadSlices(ctx, ino, 0); len(slices) != 0 {
		t.Errorf("slice rows should be deleted: %d", len(slices))
	}
}

// TestSetAttrPermissions: only owner/root may chmod/chgrp; only root may chown.
func TestSetAttrPermissions(t *testing.T) {
	e := openTestEngine(t)
	ctx := ctxBg()
	owner := &Context{Uid: 1000, Gid: 1000}
	other := &Context{Uid: 2000, Gid: 2000}
	root := &Context{Uid: 0}
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, owner)
	if st != 0 {
		t.Fatalf("Create: %v", st)
	}
	if _, st := e.SetAttr(ctx, ino, SetMode, &Attr{Mode: 0o600}, owner); st != 0 {
		t.Errorf("owner chmod should succeed: %v", st)
	}
	if _, st := e.SetAttr(ctx, ino, SetMode, &Attr{Mode: 0o666}, other); st != syscall.EPERM {
		t.Errorf("non-owner chmod: got %v want EPERM", st)
	}
	if _, st := e.SetAttr(ctx, ino, SetUID, &Attr{Uid: 3000}, owner); st != syscall.EPERM {
		t.Errorf("non-root chown: got %v want EPERM", st)
	}
	if _, st := e.SetAttr(ctx, ino, SetUID|SetMode, &Attr{Uid: 3000, Mode: 0o640}, root); st != 0 {
		t.Errorf("root chown+chmod should succeed: %v", st)
	}
}

func TestAccess(t *testing.T) {
	e := openTestEngine(t)
	ctx := ctxBg()
	owner := &Context{Uid: 1000, Gid: 1000}
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o640, owner) // rw-r-----
	if st := e.Access(ctx, ino, MayRead|MayWrite, owner); st != 0 {
		t.Errorf("owner rw: %v", st)
	}
	grp := &Context{Uid: 2000, Gid: 1000}
	if st := e.Access(ctx, ino, MayRead, grp); st != 0 {
		t.Errorf("group read: %v", st)
	}
	if st := e.Access(ctx, ino, MayWrite, grp); st != syscall.EACCES {
		t.Errorf("group write: got %v want EACCES", st)
	}
	other := &Context{Uid: 3000, Gid: 3000}
	if st := e.Access(ctx, ino, MayRead, other); st != syscall.EACCES {
		t.Errorf("other read: got %v want EACCES", st)
	}
	if st := e.Access(ctx, ino, MayRead|MayWrite|MayExec, &Context{Uid: 0}); st != 0 {
		t.Errorf("root access: %v", st)
	}
}

type opErr struct {
	op string
	st syscall.Errno
}

func (e *opErr) Error() string { return e.op + ": " + e.st.Error() }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
