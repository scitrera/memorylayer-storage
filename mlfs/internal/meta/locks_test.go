// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"syscall"
	"testing"
)

func TestFlockSharedAndExclusive(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}

	// Two shared locks coexist.
	if st := e.Flock(ctx, ino, 1001, FlockShared); st != 0 {
		t.Fatalf("A shared: %v", st)
	}
	if st := e.Flock(ctx, ino, 1002, FlockShared); st != 0 {
		t.Fatalf("B shared: %v", st)
	}
	// Exclusive conflicts with held shared locks.
	if st := e.Flock(ctx, ino, 1003, FlockExclusive); st != syscall.EAGAIN {
		t.Fatalf("C exclusive should EAGAIN while shared held, got %v", st)
	}
	// Release both shared; now exclusive can be taken.
	e.Flock(ctx, ino, 1001, FlockUnlock)
	e.Flock(ctx, ino, 1002, FlockUnlock)
	if st := e.Flock(ctx, ino, 1003, FlockExclusive); st != 0 {
		t.Fatalf("C exclusive after release: %v", st)
	}
	// A shared lock conflicts with a held exclusive.
	if st := e.Flock(ctx, ino, 1001, FlockShared); st != syscall.EAGAIN {
		t.Fatalf("A shared vs exclusive should EAGAIN, got %v", st)
	}
	// The exclusive holder may re-assert its own lock (idempotent).
	if st := e.Flock(ctx, ino, 1003, FlockExclusive); st != 0 {
		t.Fatalf("C re-lock own exclusive: %v", st)
	}
	e.Flock(ctx, ino, 1003, FlockUnlock)
	// After full release, a shared lock is fine again.
	if st := e.Flock(ctx, ino, 1001, FlockShared); st != 0 {
		t.Fatalf("A shared after all released: %v", st)
	}
}

func TestPlockByteRangeConflict(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}
	W, R, U := uint8(syscall.F_WRLCK), uint8(syscall.F_RDLCK), uint8(syscall.F_UNLCK)
	const a, b = 2001, 2002

	if st := e.Setlk(ctx, ino, a, W, 0, 9, 100); st != 0 {
		t.Fatalf("A wrlck [0,9]: %v", st)
	}
	// Disjoint range for a different owner is fine.
	if st := e.Setlk(ctx, ino, b, W, 20, 29, 200); st != 0 {
		t.Fatalf("B disjoint wrlck [20,29]: %v", st)
	}
	// Overlapping write → conflict.
	if st := e.Setlk(ctx, ino, b, W, 5, 15, 200); st != syscall.EAGAIN {
		t.Fatalf("B overlap write should EAGAIN, got %v", st)
	}
	// Overlapping read against a write → still conflict.
	if st := e.Setlk(ctx, ino, b, R, 5, 15, 200); st != syscall.EAGAIN {
		t.Fatalf("B read over write should EAGAIN, got %v", st)
	}
	// Getlk reports the conflicting lock precisely.
	typ, s, en, pid, st := e.Getlk(ctx, ino, b, W, 5, 15)
	if st != 0 {
		t.Fatalf("getlk: %v", st)
	}
	if typ != W || s != 0 || en != 9 || pid != 100 {
		t.Fatalf("getlk reported typ=%d [%d,%d] pid=%d, want WRLCK [0,9] pid 100", typ, s, en, pid)
	}
	// A unlocks; B can then take the range.
	if st := e.Setlk(ctx, ino, a, U, 0, 9, 100); st != 0 {
		t.Fatalf("A unlock: %v", st)
	}
	if st := e.Setlk(ctx, ino, b, W, 5, 15, 200); st != 0 {
		t.Fatalf("B after A unlock: %v", st)
	}
	// [0,4] is now lock-free.
	if typ, _, _, _, _ := e.Getlk(ctx, ino, a, W, 0, 4); typ != U {
		t.Fatalf("getlk [0,4] should be F_UNLCK, got %d", typ)
	}
}

func TestPlockSharedReadsCoexist(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	R := uint8(syscall.F_RDLCK)
	if st := e.Setlk(ctx, ino, 1, R, 0, 99, 1); st != 0 {
		t.Fatalf("read lock 1: %v", st)
	}
	if st := e.Setlk(ctx, ino, 2, R, 0, 99, 2); st != 0 {
		t.Fatalf("overlapping read lock 2 should coexist: %v", st)
	}
}

// TestPlockPartialUnlockSplitsRange verifies that unlocking the middle of a held
// range leaves the surrounding sub-ranges locked.
func TestPlockPartialUnlockSplitsRange(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	W, U := uint8(syscall.F_WRLCK), uint8(syscall.F_UNLCK)
	if st := e.Setlk(ctx, ino, 1, W, 0, 99, 1); st != 0 {
		t.Fatalf("lock [0,99]: %v", st)
	}
	// Unlock the middle [40,59].
	if st := e.Setlk(ctx, ino, 1, U, 40, 59, 1); st != 0 {
		t.Fatalf("unlock middle: %v", st)
	}
	// A different owner can lock the freed hole...
	if st := e.Setlk(ctx, ino, 2, W, 45, 55, 2); st != 0 {
		t.Fatalf("owner2 lock in hole [45,55]: %v", st)
	}
	// ...but not the still-locked tail.
	if st := e.Setlk(ctx, ino, 2, W, 60, 70, 2); st != syscall.EAGAIN {
		t.Fatalf("owner2 lock in held tail should EAGAIN, got %v", st)
	}
}

// TestClearAllLocksReapsStaleSession reproduces the crash-leak (#25): a lock row
// left under a dead session id is seen as a live foreign holder and blocks every
// future locker; the startup reap clears it.
func TestClearAllLocksReapsStaleSession(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, st := e.Create(ctx, RootInode, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: errno %v", st)
	}
	// Simulate a crashed mount's leaked exclusive flock + write plock under a
	// foreign session id (one this engine instance will never use).
	if _, err := e.db.ExecContext(ctx,
		`INSERT INTO flock (inode, sid, owner, ltype) VALUES ($1, 999999, 42, $2)`,
		int64(ino), int(FlockExclusive)); err != nil {
		t.Fatalf("seed stale flock: %v", err)
	}
	if _, err := e.db.ExecContext(ctx,
		`INSERT INTO plock (inode, sid, owner, records) VALUES ($1, 999999, 42, $2)`,
		int64(ino), []byte(`[{"t":1,"s":0,"e":99,"p":42}]`)); err != nil {
		t.Fatalf("seed stale plock: %v", err)
	}
	// A new client is wedged: the stale exclusive flock and write plock block it.
	if st := e.Flock(ctx, ino, 7, FlockShared); st != syscall.EAGAIN {
		t.Fatalf("stale flock should block a new locker, got %v", st)
	}
	if st := e.Setlk(ctx, ino, 7, uint8(syscall.F_WRLCK), 0, 9, 7); st != syscall.EAGAIN {
		t.Fatalf("stale plock should block a new locker, got %v", st)
	}
	// Startup reap clears all lock rows.
	if err := e.ClearAllLocks(ctx); err != nil {
		t.Fatalf("ClearAllLocks: %v", err)
	}
	if st := e.Flock(ctx, ino, 7, FlockShared); st != 0 {
		t.Fatalf("after reap, flock should succeed, got %v", st)
	}
	if st := e.Setlk(ctx, ino, 7, uint8(syscall.F_WRLCK), 0, 9, 7); st != 0 {
		t.Fatalf("after reap, plock should succeed, got %v", st)
	}
}

// TestClearPlocksReleasesOnClose simulates close(2) clearing a process's
// record locks so another owner can proceed.
func TestClearPlocksReleasesOnClose(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	ino, _, _ := e.Create(ctx, RootInode, "f", 0o644, c)
	W := uint8(syscall.F_WRLCK)
	if st := e.Setlk(ctx, ino, 1, W, 0, 9, 1); st != 0 {
		t.Fatalf("owner1 lock: %v", st)
	}
	if st := e.Setlk(ctx, ino, 2, W, 0, 9, 2); st != syscall.EAGAIN {
		t.Fatalf("owner2 should be blocked, got %v", st)
	}
	e.ClearPlocks(ctx, ino, 1) // close() for owner 1
	if st := e.Setlk(ctx, ino, 2, W, 0, 9, 2); st != 0 {
		t.Fatalf("owner2 after owner1 close: %v", st)
	}
}
