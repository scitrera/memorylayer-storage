// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	fusebridge "github.com/scitrera/memorylayer-storage/mlfs/internal/fuse"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

type rig struct {
	e      *meta.Engine
	cache  *cache.DiskCache
	local  *chunkstore.Local
	bridge *fusebridge.Bridge
	dir    string
}

// countBlobs returns the number of physical chunk/pack blobs in the backing
// store. Flush the cache first so write-back data has landed. Mirrors the
// fileio/gc tests' blob-count helper.
func (rg *rig) countBlobs(t *testing.T) int {
	t.Helper()
	if err := rg.cache.Flush(context.Background()); err != nil {
		t.Fatalf("flush before count: %v", err)
	}
	n := 0
	for _, p := range []blobstore.ID{blobstore.ID("chunk-mlfs-"), blobstore.ID("pack-mlfs-")} {
		if err := rg.local.Chunks.ListBlobs(context.Background(), p, func(blobstore.Metadata) error { n++; return nil }); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return n
}

// newRig builds meta(PG, clean) + casstore-backed chunk store + write-back disk
// cache + data path + FUSE bridge over a shared on-disk dir (so a remount sees
// the same data). Skips without a Postgres DSN.
func newRig(t *testing.T, dataDir string) *rig {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 4) // private schema → already clean, no TRUNCATE
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	local, err := chunkstore.NewLocal(filepath.Join(dataDir, "cas"), "mlfs", 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	dc, err := cache.New(local.Store, cache.Config{Dir: filepath.Join(dataDir, "cache"), AutoUpload: true})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	files := fileio.New(e, dc)
	t.Cleanup(func() { _ = dc.Close(); _ = local.Chunks.Close(ctx) })
	return &rig{e: e, cache: dc, local: local, bridge: fusebridge.New(e, files, dc), dir: dataDir}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestFUSEMountEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	rg := newRig(t, dataDir)

	mnt := t.TempDir()
	srv, err := fusebridge.Mount(rg.bridge, mnt, nil, false)
	if err != nil {
		t.Skipf("cannot mount FUSE here (%v); the data path + CoW are covered by the fileio tests", err)
	}
	mounted := true
	defer func() {
		if mounted {
			_ = srv.Unmount()
		}
	}()

	// dd-equivalent: write a multi-MiB file through the kernel → our Write path.
	data := randomBytes(t, 16<<20)
	path := filepath.Join(mnt, "big.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write through mount: %v", err)
	}
	// Read it back through the mount and compare hashes.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read through mount: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("mount round-trip sha256 mismatch (%d vs %d bytes)", len(got), len(data))
	}

	// Directory ops: mkdir, create a file inside, list.
	if err := os.Mkdir(filepath.Join(mnt, "d"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "d", "f.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("create in subdir: %v", err)
	}
	ents, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	names := []string{}
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if !contains(names, "big.bin") || !contains(names, "d") {
		t.Errorf("readdir missing entries: %v", names)
	}
	// stat reflects size.
	fi, err := os.Stat(path)
	if err != nil || fi.Size() != int64(len(data)) {
		t.Errorf("stat: size=%v err=%v", fi.Size(), err)
	}

	// Truncate via the mount.
	if err := os.Truncate(path, 4<<20); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if fi, _ := os.Stat(path); fi.Size() != 4<<20 {
		t.Errorf("after truncate size=%d", fi.Size())
	}

	// Open-but-unlinked (POSIX): open a file, unlink it while open, keep
	// reading via the fd; it vanishes from the namespace but the data lives
	// until the last close.
	tmpPath := filepath.Join(mnt, "ephemeral")
	tf, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open ephemeral: %v", err)
	}
	if _, err := tf.WriteString("persist-me"); err != nil {
		t.Fatalf("write ephemeral: %v", err)
	}
	if err := os.Remove(tmpPath); err != nil { // unlink while still open
		t.Fatalf("unlink open file: %v", err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("unlinked file should be gone from the namespace: %v", err)
	}
	rb := make([]byte, 10)
	if _, err := tf.ReadAt(rb, 0); err != nil {
		t.Fatalf("read via fd after unlink: %v", err)
	}
	if string(rb) != "persist-me" {
		t.Errorf("open-unlinked read mismatch: %q", rb)
	}
	if err := tf.Close(); err != nil { // last close → inode deleted
		t.Fatalf("close ephemeral: %v", err)
	}

	// xattr round-trip through the kernel: set, get, list, remove on a file.
	xp := filepath.Join(mnt, "d", "f.txt")
	if err := syscall.Setxattr(xp, "user.greeting", []byte("hello"), 0); err != nil {
		t.Fatalf("setxattr: %v", err)
	}
	xbuf := make([]byte, 64)
	xn, err := syscall.Getxattr(xp, "user.greeting", xbuf)
	if err != nil {
		t.Fatalf("getxattr: %v", err)
	}
	if string(xbuf[:xn]) != "hello" {
		t.Errorf("getxattr mismatch: %q", xbuf[:xn])
	}
	lbuf := make([]byte, 256)
	ln, err := syscall.Listxattr(xp, lbuf)
	if err != nil {
		t.Fatalf("listxattr: %v", err)
	}
	if !strings.Contains(string(lbuf[:ln]), "user.greeting") {
		t.Errorf("listxattr missing user.greeting: %q", lbuf[:ln])
	}
	if err := syscall.Removexattr(xp, "user.greeting"); err != nil {
		t.Fatalf("removexattr: %v", err)
	}
	if _, err := syscall.Getxattr(xp, "user.greeting", xbuf); err == nil {
		t.Error("getxattr after remove should fail with ENODATA")
	}

	// Hardlink through the kernel: link bumps nlink, both names share content
	// and inode, and removing one leaves the other intact.
	orig := filepath.Join(mnt, "d", "f.txt")
	hl := filepath.Join(mnt, "d", "f.hardlink")
	if err := os.Link(orig, hl); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	hfi, err := os.Stat(hl)
	if err != nil {
		t.Fatalf("stat hardlink: %v", err)
	}
	if sys, ok := hfi.Sys().(*syscall.Stat_t); ok && sys.Nlink != 2 {
		t.Errorf("hardlink nlink = %d, want 2", sys.Nlink)
	}
	ofi, _ := os.Stat(orig)
	if !os.SameFile(ofi, hfi) {
		t.Error("hardlink should be the same file as the original")
	}
	if got, _ := os.ReadFile(hl); string(got) != "hi" {
		t.Errorf("hardlink content = %q, want \"hi\"", got)
	}
	if err := os.Remove(orig); err != nil {
		t.Fatalf("remove original: %v", err)
	}
	if got, err := os.ReadFile(hl); err != nil || string(got) != "hi" {
		t.Errorf("hardlink should survive removal of the original: err=%v content=%q", err, got)
	}

	// flock(2) through the kernel: an exclusive lock on one fd blocks a
	// non-blocking exclusive lock on a second, independent open of the file.
	lp := filepath.Join(mnt, "lockfile")
	f1, err := os.Create(lp)
	if err != nil {
		t.Fatalf("create lockfile: %v", err)
	}
	if err := syscall.Flock(int(f1.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("flock f1 LOCK_EX: %v", err)
	}
	f2, err := os.Open(lp)
	if err != nil {
		t.Fatalf("open lockfile again: %v", err)
	}
	if err := syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != syscall.EWOULDBLOCK {
		t.Fatalf("flock f2 non-blocking should fail EWOULDBLOCK, got %v", err)
	}
	if err := syscall.Flock(int(f1.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("flock f1 unlock: %v", err)
	}
	if err := syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock f2 after f1 released: %v", err)
	}
	_ = syscall.Flock(int(f2.Fd()), syscall.LOCK_UN)
	_ = f1.Close()
	_ = f2.Close()

	// Unmount, then remount with the SAME backends → data persists.
	if err := srv.Unmount(); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	mounted = false
	_ = rg.cache.Flush(context.Background())

	mnt2 := t.TempDir()
	srv2, err := fusebridge.Mount(rg.bridge, mnt2, nil, false)
	if err != nil {
		t.Fatalf("remount: %v", err)
	}
	defer srv2.Unmount()
	got2, err := os.ReadFile(filepath.Join(mnt2, "big.bin"))
	if err != nil {
		t.Fatalf("read after remount: %v", err)
	}
	if len(got2) != 4<<20 || sha256.Sum256(got2) != sha256.Sum256(data[:4<<20]) {
		t.Errorf("post-remount content mismatch: %d bytes", len(got2))
	}
}

// mountRig builds a rig and mounts it, skipping if FUSE is unavailable.
func mountRig(t *testing.T) (*rig, string) {
	t.Helper()
	rg := newRig(t, t.TempDir())
	mnt := t.TempDir()
	srv, err := fusebridge.Mount(rg.bridge, mnt, nil, false)
	if err != nil {
		t.Skipf("cannot mount FUSE here (%v)", err)
	}
	t.Cleanup(func() { _ = srv.Unmount() })
	return rg, mnt
}

// TestFUSECopyFileRange exercises copy_file_range(2) through a real mount: a
// multi-MiB, chunk-aligned region copies byte-for-byte while sharing the
// underlying chunks copy-on-write (the physical blob count must not double).
func TestFUSECopyFileRange(t *testing.T) {
	rg, mnt := mountRig(t)
	ctx := context.Background()

	// A multi-chunk source so the copy is a whole-chunk, aligned run.
	const cs = int64(meta.ChunkSize)
	data := randomBytes(t, int(2*cs))
	srcPath := filepath.Join(mnt, "src.bin")
	if err := os.WriteFile(srcPath, data, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	_ = rg.cache.Flush(ctx)
	blobsBefore := rg.countBlobs(t)
	if blobsBefore == 0 {
		t.Fatal("expected blobs after writing src")
	}

	dstPath := filepath.Join(mnt, "dst.bin")
	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	srcF, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}

	// Copy the whole 2-chunk file via copy_file_range(2). The kernel may return
	// short, so loop until the full length is copied.
	var off1, off2 int64
	total := 0
	for total < len(data) {
		n, err := unix.CopyFileRange(int(srcF.Fd()), &off1, int(dst.Fd()), &off2, len(data)-total, 0)
		if err != nil {
			t.Fatalf("copy_file_range: %v", err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total != len(data) {
		t.Fatalf("copy_file_range copied %d of %d bytes", total, len(data))
	}
	_ = srcF.Close()
	if err := dst.Close(); err != nil {
		t.Fatalf("close dst: %v", err)
	}

	// Byte-identical content.
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("copy_file_range content mismatch (%d vs %d bytes)", len(got), len(data))
	}
	// Chunks shared: the blob count must not roughly double from the copy.
	after := rg.countBlobs(t)
	if after > blobsBefore+1 {
		t.Errorf("copy_file_range did not share chunks: blobs %d → %d (expected ~no growth)", blobsBefore, after)
	}
}

// TestFUSERenameExchange swaps two files atomically through a real mount via
// renameat2(RENAME_EXCHANGE): their contents/inodes exchange in one operation.
func TestFUSERenameExchange(t *testing.T) {
	_, mnt := mountRig(t)

	pa := filepath.Join(mnt, "a")
	pb := filepath.Join(mnt, "b")
	if err := os.WriteFile(pa, []byte("AAAA"), 0o644); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(pb, []byte("BBBB"), 0o644); err != nil {
		t.Fatalf("write b: %v", err)
	}
	sa, _ := os.Stat(pa)
	sb, _ := os.Stat(pb)
	inoA := sa.Sys().(*syscall.Stat_t).Ino
	inoB := sb.Sys().(*syscall.Stat_t).Ino

	if err := unix.Renameat2(unix.AT_FDCWD, pa, unix.AT_FDCWD, pb, unix.RENAME_EXCHANGE); err != nil {
		t.Fatalf("renameat2 RENAME_EXCHANGE: %v", err)
	}

	// Contents swapped.
	if got, _ := os.ReadFile(pa); string(got) != "BBBB" {
		t.Errorf("after exchange, a = %q want BBBB", got)
	}
	if got, _ := os.ReadFile(pb); string(got) != "AAAA" {
		t.Errorf("after exchange, b = %q want AAAA", got)
	}
	// Inodes swapped.
	sa2, _ := os.Stat(pa)
	sb2, _ := os.Stat(pb)
	if sa2.Sys().(*syscall.Stat_t).Ino != inoB {
		t.Errorf("a should now carry b's inode")
	}
	if sb2.Sys().(*syscall.Stat_t).Ino != inoA {
		t.Errorf("b should now carry a's inode")
	}
}

// TestFUSEReadDirSnapshot opens a directory, then mutates it (adds and removes
// entries) mid-iteration, and asserts the reader sees a consistent snapshot of
// what existed at open time: no duplicates, and no entry that existed at open
// is skipped. Newly created entries may or may not appear, but must not corrupt
// the scan.
func TestFUSEReadDirSnapshot(t *testing.T) {
	_, mnt := mountRig(t)

	dir := filepath.Join(mnt, "scan")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Seed a sizeable set so the kernel needs multiple readdir round-trips.
	const seeded = 400
	atOpen := map[string]bool{}
	for i := 0; i < seeded; i++ {
		name := fmt.Sprintf("f%04d", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		atOpen[name] = true
	}

	// Open via raw syscall so we hold the dir fd across mutations and drive
	// getdents in small chunks (Readdirnames(1) reads incrementally).
	df, err := os.Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer df.Close()

	seen := map[string]int{}
	mutated := false
	for {
		names, err := df.Readdirnames(8) // small batch → multiple ReadDirPlus calls
		if len(names) == 0 || err != nil {
			break
		}
		for _, n := range names {
			seen[n]++
		}
		if !mutated {
			// Mutate mid-scan: delete some seeded entries, add brand-new ones.
			for i := 0; i < 50; i++ {
				_ = os.Remove(filepath.Join(dir, fmt.Sprintf("f%04d", i)))
			}
			for i := 0; i < 50; i++ {
				_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("new%04d", i)), []byte("y"), 0o644)
			}
			mutated = true
		}
	}

	// No entry seen twice.
	for n, c := range seen {
		if c != 1 {
			t.Errorf("entry %q seen %d times (snapshot should yield each once)", n, c)
		}
	}
	// Every entry that existed at open time must appear exactly once, even the
	// ones deleted mid-scan (they were in the snapshot).
	for n := range atOpen {
		if seen[n] != 1 {
			t.Errorf("entry %q present at open was skipped (seen %d)", n, seen[n])
		}
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
