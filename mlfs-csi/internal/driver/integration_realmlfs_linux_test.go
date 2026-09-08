// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build integration && linux

// Tier-2 integration: drive the REAL mlfs daemon (in local single-node mode, so
// it needs only Postgres — not the full remote blobgw/NATS/S3 backend) under the
// real MountManager + exec launcher, and prove a per-domain mount actually works
// as a filesystem (write→read) and tears down leak-free. Gated by the
// `integration` tag, root, and these env vars (the orchestration script sets
// them and pre-creates the two databases):
//
//	MLFS_TEST_BIN    path to a built mlfs binary
//	MLFS_TEST_DSN_A  ready Postgres DSN for domain "acme"
//	MLFS_TEST_DSN_B  ready Postgres DSN for domain "beta"
package driver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// localMlfsArgs runs mlfs in local single-node mode (local casstore under
// -data-dir, no -remote/-nats), which needs only a Postgres meta DSN.
func localMlfsArgs(spec DomainSpec, mountDir, dataDir, cacheDir, credsPath string) []string {
	return []string{
		"-mount", mountDir,
		"-domain", spec.Domain, // casstore dedup domain (blob-ID prefix) in local mode
		"-meta-dsn", spec.MetaDSN,
		"-data-dir", filepath.Join(dataDir, "cas"),
		"-cache-dir", cacheDir,
	}
}

func TestIntegrationRealMlfsTwoDomainsNoLeak(t *testing.T) {
	requireRoot(t)
	bin := os.Getenv("MLFS_TEST_BIN")
	dsnA := os.Getenv("MLFS_TEST_DSN_A")
	dsnB := os.Getenv("MLFS_TEST_DSN_B")
	if bin == "" || dsnA == "" || dsnB == "" {
		t.Skip("set MLFS_TEST_BIN, MLFS_TEST_DSN_A, MLFS_TEST_DSN_B (and run privileged) to exercise the real mlfs daemon")
	}

	base := t.TempDir()
	ctx := context.Background()
	mgr := NewMountManager(base, NewExecLauncherWithArgs(bin, localMlfsArgs), NewLinuxMounter())

	specA := DomainSpec{Domain: "acme", MetaDSN: dsnA, NodeID: "itest"}
	specB := DomainSpec{Domain: "beta", MetaDSN: dsnB, NodeID: "itest"}

	mA, err := mgr.Acquire(ctx, specA, "/itest/a1")
	if err != nil {
		t.Fatalf("acquire acme: %v", err)
	}
	mB, err := mgr.Acquire(ctx, specB, "/itest/b1")
	if err != nil {
		t.Fatalf("acquire beta: %v", err)
	}

	// Two real, distinct FUSE mounts.
	if mounts := mountpointsUnder(t, base); len(mounts) != 2 {
		t.Fatalf("want 2 real FUSE mounts, got %d: %v", len(mounts), mounts)
	}
	pids := statePids(t, base)

	// The mounts actually work as filesystems AND are isolated: a file written
	// into acme's mount is readable back there and absent from beta's.
	payload := []byte("hello-from-acme")
	fA := filepath.Join(mA, "probe.txt")
	if err := os.WriteFile(fA, payload, 0o644); err != nil {
		t.Fatalf("write through acme FUSE mount: %v", err)
	}
	got, err := os.ReadFile(fA)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("read back through acme mount: got %q err %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(mB, "probe.txt")); !os.IsNotExist(err) {
		t.Errorf("isolation breach: acme's file visible in beta's mount (stat err=%v)", err)
	}

	// Release everything; assert no leaked mounts or daemons.
	if _, err := mgr.Release(ctx, "/itest/a1"); err != nil {
		t.Fatalf("release acme: %v", err)
	}
	if _, err := mgr.Release(ctx, "/itest/b1"); err != nil {
		t.Fatalf("release beta: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // allow clean FUSE unmount + exit
	if mounts := mountpointsUnder(t, base); len(mounts) != 0 {
		t.Errorf("LEAK: real FUSE mounts remain after release: %v", mounts)
	}
	for d, pid := range pids {
		if pidAlive(pid) {
			t.Errorf("LEAK: real mlfs daemon for %s (pid %d) alive after release", d, pid)
		}
	}
	entries, _ := os.ReadDir(base)
	for _, e := range entries {
		if e.Name() != "state.json" {
			t.Errorf("LEAK: leftover entry under base: %q", e.Name())
		}
	}
}

// TestIntegrationRealMlfsLivenessUnderWrite reproduces the platform's mount-flap
// regression: while a heavy write drives the write-back drain (batched +
// auto-concurrency), the node-plugin's IsMountedLive(statfs) probe must keep
// seeing the mount LIVE. If the drain starves the FUSE serve path, statfs flips
// to ENOTCONN and the driver flaps the mount.
func TestIntegrationRealMlfsLivenessUnderWrite(t *testing.T) {
	requireRoot(t)
	bin := os.Getenv("MLFS_TEST_BIN")
	dsnA := os.Getenv("MLFS_TEST_DSN_A")
	if bin == "" || dsnA == "" {
		t.Skip("set MLFS_TEST_BIN + MLFS_TEST_DSN_A (privileged) for the liveness-under-write test")
	}

	base := t.TempDir()
	ctx := context.Background()
	lm := NewLinuxMounter()
	mgr := NewMountManager(base, NewExecLauncherWithArgs(bin, localMlfsArgs), lm)
	mountDir, err := mgr.Acquire(ctx, DomainSpec{Domain: "acme", MetaDSN: dsnA, NodeID: "itest"}, "/itest/a1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer mgr.Release(ctx, "/itest/a1")

	// Writer: hammer the mount for ~6s to keep the drain busy.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 256*1024)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			f := filepath.Join(mountDir, "w", "f"+strconv.Itoa(i))
			_ = os.MkdirAll(filepath.Dir(f), 0o755)
			if err := os.WriteFile(f, buf, 0o644); err != nil {
				return // mount may be wedged; the poller will catch it
			}
		}
	}()

	// Poller: assert the mount stays live to statfs throughout.
	deadline := time.Now().Add(6 * time.Second)
	var flips int
	for time.Now().Before(deadline) {
		if ok, _ := lm.IsMountedLive(mountDir); !ok {
			flips++
			t.Errorf("mount went NOT-live to statfs while the drain ran (flip #%d) — repro of the platform flap", flips)
			if flips >= 3 {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	close(stop)
	<-done
	if flips == 0 {
		t.Logf("mount stayed live through the write storm (no flap reproduced in local mode)")
	}
}

// TestIntegrationRealMlfsStaleAdoptNoFalseAdopt is the §7.3 Bug 2 regression
// against the REAL daemon: SIGKILL an mlfs daemon WITHOUT a clean unmount (as a
// cgroup-kill would), confirm the FUSE endpoint is genuinely stale (real
// ENOTCONN), and confirm a restarting manager PRUNES the corpse on Adopt rather
// than re-adopting it (which would wedge every consumer pod) — and cleans it up
// leak-free.
func TestIntegrationRealMlfsStaleAdoptNoFalseAdopt(t *testing.T) {
	requireRoot(t)
	bin := os.Getenv("MLFS_TEST_BIN")
	dsnA := os.Getenv("MLFS_TEST_DSN_A")
	if bin == "" || dsnA == "" {
		t.Skip("set MLFS_TEST_BIN + MLFS_TEST_DSN_A (privileged) for the real stale-adopt test")
	}

	base := t.TempDir()
	ctx := context.Background()
	lm := NewLinuxMounter()
	mgr := NewMountManager(base, NewExecLauncherWithArgs(bin, localMlfsArgs), lm)
	spec := DomainSpec{Domain: "acme", MetaDSN: dsnA, NodeID: "itest"}

	mountDir, err := mgr.Acquire(ctx, spec, "/itest/a1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pid := statePids(t, base)["acme"]
	if ok, _ := lm.IsMountedLive(mountDir); !ok {
		t.Fatalf("mount not live after acquire")
	}

	// Simulate a cgroup-kill: SIGKILL the daemon with no clean unmount, leaving a
	// stale FUSE endpoint.
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill daemon: %v", err)
	}
	live := true
	for i := 0; i < 50 && live; i++ {
		time.Sleep(100 * time.Millisecond)
		live, _ = lm.IsMountedLive(mountDir)
	}
	// mountinfo still lists it (so plain IsMounted would falsely say "live")...
	if ok, _ := lm.IsMounted(mountDir); !ok {
		t.Errorf("expected the stale mount to still be present in mountinfo")
	}
	// ...but the liveness probe correctly reports it dead (real ENOTCONN).
	if ok, _ := lm.IsMountedLive(mountDir); ok {
		t.Fatalf("stale FUSE still reported live — statfs/ENOTCONN liveness probe failed")
	}

	// A restarting plugin must PRUNE the corpse, not re-adopt it.
	mgr2 := NewMountManager(base, NewExecLauncherWithArgs(bin, localMlfsArgs), lm)
	if err := mgr2.Adopt(ctx); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	released, err := mgr2.Release(ctx, "/itest/a1")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released {
		t.Errorf("stale mount was wrongly re-adopted instead of pruned")
	}

	// The stale endpoint was unmounted + reclaimed by the prune — no leak.
	leaked := true
	for i := 0; i < 30 && leaked; i++ {
		time.Sleep(100 * time.Millisecond)
		leaked = len(mountpointsUnder(t, base)) != 0
	}
	if leaked {
		t.Errorf("LEAK: stale mount not cleaned up on adopt-prune: %v", mountpointsUnder(t, base))
	}
}
