// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge_test

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// flockLkIn builds an LkIn describing a whole-file (flock) lock request of the
// given fcntl type (F_RDLCK/F_WRLCK/F_UNLCK) for owner on ino.
func flockLkIn(ino meta.Ino, owner uint64, typ uint32) *fuse.LkIn {
	in := &fuse.LkIn{Owner: owner, LkFlags: fuse.FUSE_LK_FLOCK}
	in.NodeId = uint64(ino)
	in.Lk = fuse.FileLock{Typ: typ, Start: 0, End: ^uint64(0)}
	return in
}

// TestBridgeSetLkwBlocksUntilRelease exercises the true blocking SETLKW: owner A
// holds an exclusive flock; a second SETLKW-style acquire for owner B blocks
// (does NOT spuriously fail with EAGAIN under contention); when A releases, B
// is woken and acquires within a short timeout. This drives the bridge methods
// directly — no kernel mount required.
func TestBridgeSetLkwBlocksUntilRelease(t *testing.T) {
	rg := newRig(t, t.TempDir())
	b := rg.bridge
	ctx := context.Background()

	ino, _, st := rg.e.Create(ctx, meta.RootInode, "lk", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	const ownerA, ownerB = uint64(0xA), uint64(0xB)
	// A takes an exclusive flock (non-blocking SetLk succeeds — no contention).
	if got := b.SetLk(nil, flockLkIn(ino, ownerA, syscall.F_WRLCK)); got != fuse.OK {
		t.Fatalf("A SetLk exclusive: %v", got)
	}

	// B issues a blocking SETLKW for the same exclusive lock; it must block.
	cancel := make(chan struct{})
	acquired := make(chan fuse.Status, 1)
	go func() {
		acquired <- b.SetLkw(cancel, flockLkIn(ino, ownerB, syscall.F_WRLCK))
	}()

	// Assert B is still blocked after a grace period (it must not return EAGAIN
	// or succeed while A holds the lock).
	select {
	case got := <-acquired:
		t.Fatalf("B SetLkw should block while A holds the lock, returned %v", got)
	case <-time.After(150 * time.Millisecond):
		// still blocked — correct
	}

	// A releases; B must be woken and acquire.
	if got := b.SetLk(nil, flockLkIn(ino, ownerA, syscall.F_UNLCK)); got != fuse.OK {
		t.Fatalf("A unlock: %v", got)
	}
	select {
	case got := <-acquired:
		if got != fuse.OK {
			t.Fatalf("B SetLkw after A release: %v", got)
		}
	case <-time.After(2 * time.Second):
		close(cancel)
		t.Fatal("B SetLkw did not acquire within timeout after A released")
	}
}

// TestBridgeSetLkwCancelInterrupts verifies that a blocked SETLKW honors the
// FUSE cancel channel (an interrupted/killed syscall) and returns EINTR rather
// than hanging the request forever.
func TestBridgeSetLkwCancelInterrupts(t *testing.T) {
	rg := newRig(t, t.TempDir())
	b := rg.bridge
	ctx := context.Background()

	ino, _, st := rg.e.Create(ctx, meta.RootInode, "lk", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	const ownerA, ownerB = uint64(1), uint64(2)
	if got := b.SetLk(nil, flockLkIn(ino, ownerA, syscall.F_WRLCK)); got != fuse.OK {
		t.Fatalf("A SetLk exclusive: %v", got)
	}

	cancel := make(chan struct{})
	result := make(chan fuse.Status, 1)
	go func() {
		result <- b.SetLkw(cancel, flockLkIn(ino, ownerB, syscall.F_WRLCK))
	}()

	// Let it park, then cancel.
	time.Sleep(100 * time.Millisecond)
	close(cancel)

	select {
	case got := <-result:
		if got != fuse.EINTR {
			t.Fatalf("cancelled SetLkw should return EINTR, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled SetLkw did not return; cancel channel not honored")
	}
}

// TestBridgeSetLkwByteRangeBlocksUntilRelease exercises blocking on a POSIX
// byte-range (plock) conflict released via the FLUSH (close) path, which clears
// the owner's record locks and must wake the waiter.
func TestBridgeSetLkwByteRangeBlocksUntilRelease(t *testing.T) {
	rg := newRig(t, t.TempDir())
	b := rg.bridge
	ctx := context.Background()

	ino, _, st := rg.e.Create(ctx, meta.RootInode, "lk", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	const ownerA, ownerB = uint64(10), uint64(20)
	plock := func(owner uint64, typ uint32) *fuse.LkIn {
		in := &fuse.LkIn{Owner: owner}
		in.NodeId = uint64(ino)
		in.Lk = fuse.FileLock{Typ: typ, Start: 0, End: 99, Pid: uint32(owner)}
		return in
	}

	if got := b.SetLk(nil, plock(ownerA, syscall.F_WRLCK)); got != fuse.OK {
		t.Fatalf("A SetLk wrlck: %v", got)
	}

	cancel := make(chan struct{})
	acquired := make(chan fuse.Status, 1)
	go func() {
		acquired <- b.SetLkw(cancel, plock(ownerB, syscall.F_WRLCK))
	}()

	select {
	case got := <-acquired:
		t.Fatalf("B byte-range SetLkw should block, returned %v", got)
	case <-time.After(150 * time.Millisecond):
	}

	// A closes its fd: FLUSH clears A's record locks and wakes B.
	flush := &fuse.FlushIn{LockOwner: ownerA}
	flush.NodeId = uint64(ino)
	if got := b.Flush(nil, flush); got != fuse.OK {
		t.Fatalf("A Flush: %v", got)
	}

	select {
	case got := <-acquired:
		if got != fuse.OK {
			t.Fatalf("B byte-range SetLkw after A close: %v", got)
		}
	case <-time.After(2 * time.Second):
		close(cancel)
		t.Fatal("B byte-range SetLkw did not acquire after A's close")
	}
}
