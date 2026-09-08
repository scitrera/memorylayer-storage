// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// memMounter is an in-memory Mounter for manager tests. `mounted` models
// mountinfo presence; `stale` models a present-but-dead FUSE endpoint (daemon
// killed) — IsMounted reports presence, IsMountedLive reports liveness.
type memMounter struct {
	mu      sync.Mutex
	mounted map[string]bool
	stale   map[string]bool
}

func newMemMounter() *memMounter {
	return &memMounter{mounted: map[string]bool{}, stale: map[string]bool{}}
}

func (m *memMounter) set(path string, v bool) {
	m.mu.Lock()
	m.mounted[path] = v
	m.stale[path] = false
	m.mu.Unlock()
}

// setStale simulates a cgroup-killed daemon: the mount object lingers in
// mountinfo (mounted) but its filesystem no longer serves (stale ENOTCONN).
func (m *memMounter) setStale(path string) {
	m.mu.Lock()
	m.mounted[path] = true
	m.stale[path] = true
	m.mu.Unlock()
}

func (m *memMounter) BindMount(_, target string, _ bool) error { m.set(target, true); return nil }
func (m *memMounter) Unmount(target string) error              { m.set(target, false); return nil }
func (m *memMounter) IsMounted(target string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mounted[target], nil
}
func (m *memMounter) IsMountedLive(target string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mounted[target] && !m.stale[target], nil
}

// fakeLauncher simulates spawning mlfs: it marks the mount ready (so the
// manager's readiness poll succeeds) and returns a handle that "unmounts" on
// Stop. It counts starts so tests can assert per-domain reuse.
type fakeLauncher struct {
	mu       sync.Mutex
	mounter  *memMounter
	starts   int
	nextPid  int
	failNext bool
}

func (l *fakeLauncher) Start(_ context.Context, spec DomainSpec, mountDir, _, _ string) (MountHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failNext {
		l.failNext = false
		return nil, errors.New("launch failed")
	}
	l.starts++
	l.nextPid++
	pid := 9000 + l.nextPid
	l.mounter.set(mountDir, true) // simulate the mount appearing
	return &fakeHandle{pid: pid}, nil
}

// Adopt reconstructs a handle from a persisted ref (pid string). The manager
// unmounts the mount dir itself in teardown, so the handle need not.
func (l *fakeLauncher) Adopt(ref, _ string) MountHandle {
	pid, _ := strconv.Atoi(ref)
	return &fakeHandle{pid: pid, adopted: true}
}

type fakeHandle struct {
	pid     int
	adopted bool
	stopped bool
}

func (h *fakeHandle) Ref() string { return strconv.Itoa(h.pid) }
func (h *fakeHandle) Stop(_ context.Context) error {
	h.stopped = true
	return nil
}

func newTestManager(t *testing.T) (*MountManager, *fakeLauncher, *memMounter) {
	t.Helper()
	mm := newMemMounter()
	fl := &fakeLauncher{mounter: mm}
	mgr := NewMountManager(t.TempDir(), fl, mm)
	return mgr, fl, mm
}

func specFor(domain string) DomainSpec {
	return DomainSpec{Domain: domain, MetaDSN: "dsn", NATSURL: "nats://x", Creds: "C", NodeID: "node-1"}
}

func TestAcquireLaunchesOncePerDomain(t *testing.T) {
	mgr, fl, _ := newTestManager(t)
	ctx := context.Background()

	m1, err := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	// Second PVC for the SAME domain reuses the mount (no new launch).
	m2, err := mgr.Acquire(ctx, specFor("acme"), "/t/b")
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if m1 != m2 {
		t.Errorf("same domain returned different mount dirs: %q vs %q", m1, m2)
	}
	// A different domain launches its own mount.
	if _, err := mgr.Acquire(ctx, specFor("beta"), "/t/c"); err != nil {
		t.Fatalf("acquire c: %v", err)
	}
	if fl.starts != 2 {
		t.Errorf("starts = %d, want 2 (one per domain)", fl.starts)
	}
}

func TestReleaseTearsDownOnLastRef(t *testing.T) {
	mgr, _, mm := newTestManager(t)
	ctx := context.Background()

	mountDir, _ := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if _, err := mgr.Acquire(ctx, specFor("acme"), "/t/b"); err != nil {
		t.Fatal(err)
	}

	// First release keeps the mount (one holder remains).
	released, err := mgr.Release(ctx, "/t/a")
	if err != nil || !released {
		t.Fatalf("release a: released=%v err=%v", released, err)
	}
	if ok, _ := mm.IsMounted(mountDir); !ok {
		t.Errorf("mount torn down too early")
	}

	// Last release tears the domain mount down.
	released, err = mgr.Release(ctx, "/t/b")
	if err != nil || !released {
		t.Fatalf("release b: released=%v err=%v", released, err)
	}
	if ok, _ := mm.IsMounted(mountDir); ok {
		t.Errorf("mount not torn down after last release")
	}
	// Domain dir tree removed.
	if _, err := os.Stat(mountDir); !os.IsNotExist(err) {
		t.Errorf("domain dir not cleaned: stat err=%v", err)
	}
}

// TestReleaseDefersTeardownWithGrace asserts that with an idle-grace window the
// LAST release keeps the domain mount LIVE (deferred teardown), and a quick
// re-acquire within the window re-adopts the running mount instead of relaunching
// — the fast path that removes the ~30s coordination-handoff stall.
func TestReleaseDefersTeardownWithGrace(t *testing.T) {
	mgr, fl, mm := newTestManager(t)
	mgr.SetLingerGrace(10 * time.Second) // long enough not to fire during the test
	ctx := context.Background()

	mountDir, err := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	released, err := mgr.Release(ctx, "/t/a")
	if err != nil || !released {
		t.Fatalf("release: released=%v err=%v", released, err)
	}
	// Mount is retained (zero-ref) with a pending teardown timer, not torn down.
	if ok, _ := mm.IsMountedLive(mountDir); !ok {
		t.Fatal("mount torn down despite idle-grace window")
	}
	mgr.mu.Lock()
	dm := mgr.domains["acme"]
	mgr.mu.Unlock()
	if dm == nil {
		t.Fatal("domain entry dropped during grace window")
	}
	if len(dm.refs) != 0 {
		t.Fatalf("refs during grace = %d, want 0", len(dm.refs))
	}
	if dm.teardown == nil {
		t.Fatal("no deferred teardown timer armed")
	}

	// Quick recreate re-adopts the LIVE mount: no relaunch, timer cancelled.
	m2, err := mgr.Acquire(ctx, specFor("acme"), "/t/b")
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if m2 != mountDir {
		t.Fatalf("recreate got different mount %q vs %q", m2, mountDir)
	}
	if fl.starts != 1 {
		t.Fatalf("starts = %d, want 1 (mount reused, not relaunched)", fl.starts)
	}
	mgr.mu.Lock()
	pending := mgr.domains["acme"].teardown
	mgr.mu.Unlock()
	if pending != nil {
		t.Fatal("teardown timer not cancelled on re-acquire")
	}
}

// TestGraceTeardownFiresWhenNotReacquired asserts the deferred teardown actually
// fires (and cleans up) when no recreate arrives within the grace window.
func TestGraceTeardownFiresWhenNotReacquired(t *testing.T) {
	mgr, _, mm := newTestManager(t)
	mgr.SetLingerGrace(15 * time.Millisecond)
	ctx := context.Background()

	mountDir, err := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := mgr.Release(ctx, "/t/a"); err != nil {
		t.Fatalf("release: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if ok, _ := mm.IsMountedLive(mountDir); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle mount not torn down after grace elapsed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mgr.mu.Lock()
	_, present := mgr.domains["acme"]
	mgr.mu.Unlock()
	if present {
		t.Fatal("domain entry not removed after deferred teardown")
	}
}

func TestAcquireRelaunchesDeadMount(t *testing.T) {
	mgr, fl, mm := newTestManager(t)
	ctx := context.Background()

	mountDir, err := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatal(err)
	}
	if fl.starts != 1 {
		t.Fatalf("starts = %d, want 1", fl.starts)
	}

	// The domain's mlfs dies (mount vanishes) while /t/a still holds it.
	mm.set(mountDir, false)

	// A new PVC for the same domain must RELAUNCH (not hand back the dead mount)
	// and carry the existing holder forward.
	if _, err := mgr.Acquire(ctx, specFor("acme"), "/t/b"); err != nil {
		t.Fatalf("acquire after death: %v", err)
	}
	if fl.starts != 2 {
		t.Errorf("dead mount not relaunched: starts = %d, want 2", fl.starts)
	}
	if ok, _ := mm.IsMounted(mountDir); !ok {
		t.Errorf("relaunched mount not live")
	}

	// The carried-forward holder means releasing only /t/b keeps the mount up.
	if _, err := mgr.Release(ctx, "/t/b"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := mm.IsMounted(mountDir); !ok {
		t.Errorf("mount torn down despite carried-forward holder /t/a")
	}
	// Releasing the original holder tears it down.
	if _, err := mgr.Release(ctx, "/t/a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := mm.IsMounted(mountDir); ok {
		t.Errorf("mount not torn down after last holder released")
	}
}

func TestReleaseUnknownTargetIsNoop(t *testing.T) {
	mgr, _, _ := newTestManager(t)
	released, err := mgr.Release(context.Background(), "/never/published")
	if err != nil || released {
		t.Fatalf("release unknown: released=%v err=%v, want false,nil", released, err)
	}
}

func TestAcquireLaunchFailureLeavesNoDomain(t *testing.T) {
	mgr, fl, _ := newTestManager(t)
	fl.failNext = true
	if _, err := mgr.Acquire(context.Background(), specFor("acme"), "/t/a"); err == nil {
		t.Fatalf("expected launch failure")
	}
	// A retry must succeed (no half-registered domain blocking it).
	if _, err := mgr.Acquire(context.Background(), specFor("acme"), "/t/a"); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
}

func TestAdoptReAttachesLiveMount(t *testing.T) {
	mm := newMemMounter()
	fl := &fakeLauncher{mounter: mm}
	base := t.TempDir()
	ctx := context.Background()

	// First manager brings up a domain and persists state.
	mgr1 := NewMountManager(base, fl, mm)
	mountDir, err := mgr1.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatal(err)
	}

	// A new manager over the same base (simulating a node-plugin restart) adopts
	// the still-live mount without relaunching.
	mgr2 := NewMountManager(base, fl, mm)
	if err := mgr2.Adopt(ctx); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	startsBefore := fl.starts

	// A re-issued publish for the adopted volume must NOT relaunch.
	if _, err := mgr2.Acquire(ctx, specFor("acme"), "/t/a"); err != nil {
		t.Fatalf("acquire after adopt: %v", err)
	}
	if fl.starts != startsBefore {
		t.Errorf("adopt relaunched mlfs: starts %d -> %d", startsBefore, fl.starts)
	}

	// Releasing the last ref tears the adopted domain down (handle reconstructed
	// from the persisted ref); the manager unmounts it.
	if _, err := mgr2.Release(ctx, "/t/a"); err != nil {
		t.Fatalf("release adopted: %v", err)
	}
	if ok, _ := mm.IsMounted(mountDir); ok {
		t.Errorf("adopted domain not unmounted after last release")
	}
}

func TestAdoptPrunesDeadMount(t *testing.T) {
	mm := newMemMounter()
	fl := &fakeLauncher{mounter: mm}
	base := t.TempDir()
	ctx := context.Background()

	mgr1 := NewMountManager(base, fl, mm)
	mountDir, err := mgr1.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatal(err)
	}

	// The mlfs process did NOT survive the restart: its mount is gone.
	mm.set(mountDir, false)

	mgr2 := NewMountManager(base, fl, mm)
	if err := mgr2.Adopt(ctx); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	// The dead domain is pruned: a release for its old target finds nothing.
	released, err := mgr2.Release(ctx, "/t/a")
	if err != nil {
		t.Fatalf("release after prune: %v", err)
	}
	if released {
		t.Errorf("dead mount should have been pruned on adopt, but a holder was found")
	}
}

// TestAdoptPrunesStaleMount is the regression for §7.3 Bug 2: a daemon
// cgroup-killed across a restart leaves a STALE FUSE endpoint that still appears
// in mountinfo. Adopt must NOT re-adopt that corpse (which would hand pods a
// dead ENOTCONN mount) — it must prune it via the liveness (statfs) probe.
func TestAdoptPrunesStaleMount(t *testing.T) {
	mm := newMemMounter()
	fl := &fakeLauncher{mounter: mm}
	base := t.TempDir()
	ctx := context.Background()

	mgr1 := NewMountManager(base, fl, mm)
	mountDir, err := mgr1.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatal(err)
	}

	// Daemon killed: mount object lingers in mountinfo but the FUSE is stale.
	mm.setStale(mountDir)

	mgr2 := NewMountManager(base, fl, mm)
	if err := mgr2.Adopt(ctx); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	released, err := mgr2.Release(ctx, "/t/a")
	if err != nil {
		t.Fatalf("release after prune: %v", err)
	}
	if released {
		t.Errorf("stale FUSE mount was wrongly re-adopted instead of pruned")
	}
}

// TestAcquireRelaunchesStaleMount is the Acquire-side of Bug 2: a new PVC for a
// domain whose mount went stale must relaunch (self-heal), not hand back the
// corpse.
func TestAcquireRelaunchesStaleMount(t *testing.T) {
	mgr, fl, mm := newTestManager(t)
	ctx := context.Background()

	mountDir, err := mgr.Acquire(ctx, specFor("acme"), "/t/a")
	if err != nil {
		t.Fatal(err)
	}
	if fl.starts != 1 {
		t.Fatalf("starts = %d, want 1", fl.starts)
	}

	mm.setStale(mountDir) // daemon died; stale FUSE remains in mountinfo

	if _, err := mgr.Acquire(ctx, specFor("acme"), "/t/b"); err != nil {
		t.Fatalf("acquire over stale mount: %v", err)
	}
	if fl.starts != 2 {
		t.Errorf("stale mount not relaunched: starts = %d, want 2", fl.starts)
	}
	if ok, _ := mm.IsMountedLive(mountDir); !ok {
		t.Errorf("relaunched mount not live")
	}
}
