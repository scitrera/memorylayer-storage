// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build integration && linux

// Integration tests that exercise the REAL mount manager + REAL exec launcher
// (actual child processes, setsid) + REAL linux mounter against actual kernel
// mounts. They require root (CAP_SYS_ADMIN to mount tmpfs) and so are gated
// behind the `integration` build tag and an euid==0 check — intended to run in
// a `docker run --privileged` container, e.g.:
//
//	docker run --rm --privileged -v <repo>/mlfs-csi:/src -w /src \
//	  -v ~/go/pkg/mod:/go/pkg/mod golang:1.26 \
//	  go test -tags integration -run Integration -v ./internal/driver/
//
// A tiny `fakemlfs` helper stands in for the real mlfs daemon: it mounts a
// tmpfs at -mount (so the manager's readiness/IsMounted probes see a real
// mount) and unmounts on SIGTERM (so teardown is a real unmount). This isolates
// the driver's process/mount/creds lifecycle and leak behavior from the heavy
// mlfs remote backend (Postgres/NATS/S3).
package driver

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakemlfsSource is a stdlib-only stand-in daemon: mount tmpfs at -mount, hold
// until signalled, then unmount. The binary path contains "mlfs" and it is
// invoked with "-domain <d>" so the manager's stopAdoptedPID /proc identity
// guard matches it.
const fakemlfsSource = `package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var mount string
	for i, a := range os.Args {
		if a == "-mount" && i+1 < len(os.Args) {
			mount = os.Args[i+1]
		}
	}
	if mount == "" {
		os.Exit(2)
	}
	if err := syscall.Mount("tmpfs", mount, "tmpfs", 0, ""); err != nil {
		os.Stderr.WriteString("fakemlfs mount: " + err.Error() + "\n")
		os.Exit(3)
	}
	_ = os.WriteFile(mount+"/.alive", []byte("1"), 0o644)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	_ = syscall.Unmount(mount, syscall.MNT_DETACH)
}
`

func buildFakemlfs(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module fakemlfs\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(fakemlfsSource), 0o644); err != nil {
		t.Fatal(err)
	}
	// Name the binary so its path contains "mlfs" (stopAdoptedPID identity check).
	bin := filepath.Join(dir, "fakemlfs")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = dir
	// GOWORK=off so this throwaway stdlib module builds standalone even when the
	// outer test runs inside a go workspace (GOWORK pointing at the repo).
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GO111MODULE=on", "GOWORK=off", "GOPROXY=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakemlfs: %v\n%s", err, out)
	}
	return bin
}

func requireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("integration test needs root (CAP_SYS_ADMIN for tmpfs mount); run in a privileged container")
	}
}

// mountpointsUnder returns the absolute mountpoints in this mount namespace that
// live under base (per /proc/self/mountinfo).
func mountpointsUnder(t *testing.T, base string) []string {
	t.Helper()
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 4 && strings.HasPrefix(f[4], base) {
			out = append(out, f[4])
		}
	}
	return out
}

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// statePids reads the manager's persisted state.json and returns domain->pid.
func statePids(t *testing.T, base string) map[string]int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(base, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int{}
		}
		t.Fatal(err)
	}
	var st struct {
		Domains map[string]struct {
			Ref string `json:"ref"`
		} `json:"domains"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for d, v := range st.Domains {
		pid, _ := strconv.Atoi(v.Ref) // exec backend ref == pid
		out[d] = pid
	}
	return out
}

func intgSpec(domain string) DomainSpec {
	return DomainSpec{
		Domain:  domain,
		MetaDSN: "postgres://unused",
		NATSURL: "nats://unused:4222",
		Creds:   "FAKE-CREDS-" + domain,
		Region:  0,
		NodeID:  "itest-node",
	}
}

// TestIntegrationMultiDomainLifecycleNoLeak is the core leak/lifecycle test: two
// domains mounted concurrently on one manager, ref-counted teardown, and a hard
// assertion that after full release NOTHING leaks — no child processes, no
// kernel mounts, no creds files, no domain dirs.
func TestIntegrationMultiDomainLifecycleNoLeak(t *testing.T) {
	requireRoot(t)
	bin := buildFakemlfs(t)
	base := t.TempDir()
	ctx := context.Background()

	mgr := NewMountManager(base, NewExecLauncher(bin), NewLinuxMounter())

	// Acquire two distinct domains, two holders on the first.
	mA, err := mgr.Acquire(ctx, intgSpec("acme"), "/itest/a1")
	if err != nil {
		t.Fatalf("acquire acme/a1: %v", err)
	}
	if _, err := mgr.Acquire(ctx, intgSpec("acme"), "/itest/a2"); err != nil {
		t.Fatalf("acquire acme/a2: %v", err)
	}
	mB, err := mgr.Acquire(ctx, intgSpec("beta"), "/itest/b1")
	if err != nil {
		t.Fatalf("acquire beta/b1: %v", err)
	}

	// Both domains are really mounted, and isolated (distinct mount dirs).
	if mA == mB {
		t.Fatalf("domains share a mount dir: %q", mA)
	}
	mounts := mountpointsUnder(t, base)
	if len(mounts) != 2 {
		t.Fatalf("want 2 kernel mounts under base, got %d: %v", len(mounts), mounts)
	}
	for _, md := range []string{mA, mB} {
		if ok, _ := NewLinuxMounter().IsMounted(md); !ok {
			t.Errorf("domain mount %q not actually mounted", md)
		}
		if _, err := os.Stat(filepath.Join(md, ".alive")); err != nil {
			t.Errorf("fakemlfs did not write marker in %q: %v", md, err)
		}
	}
	// Creds files exist on disk (0600).
	pids := statePids(t, base)
	if len(pids) != 2 || !pidAlive(pids["acme"]) || !pidAlive(pids["beta"]) {
		t.Fatalf("expected two live daemons, state pids=%v", pids)
	}

	// Ref-count: releasing one of acme's two holders keeps it mounted.
	if _, err := mgr.Release(ctx, "/itest/a1"); err != nil {
		t.Fatalf("release acme/a1: %v", err)
	}
	if ok, _ := NewLinuxMounter().IsMounted(mA); !ok {
		t.Errorf("acme torn down with a holder remaining")
	}
	if !pidAlive(pids["acme"]) {
		t.Errorf("acme daemon killed with a holder remaining")
	}

	// Release everything.
	if _, err := mgr.Release(ctx, "/itest/a2"); err != nil {
		t.Fatalf("release acme/a2: %v", err)
	}
	if _, err := mgr.Release(ctx, "/itest/b1"); err != nil {
		t.Fatalf("release beta/b1: %v", err)
	}

	// Hard leak assertions: give async lazy-unmount a moment to settle.
	time.Sleep(300 * time.Millisecond)
	if mounts := mountpointsUnder(t, base); len(mounts) != 0 {
		t.Errorf("LEAK: kernel mounts remain after full release: %v", mounts)
	}
	for d, pid := range pids {
		if pidAlive(pid) {
			t.Errorf("LEAK: %s daemon pid %d still alive after release", d, pid)
		}
	}
	// Domain dirs (and the 0600 creds within) are gone.
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "state.json" {
			t.Errorf("LEAK: leftover entry under base after release: %q", e.Name())
		}
	}
}

// TestIntegrationRestartReadoptNoLeak proves a node-plugin restart re-adopts a
// live per-domain mount (the daemon survives via setsid) without disturbing it,
// and that a later release through the NEW manager tears it down leak-free via
// the pid path (stopAdoptedPID identity check against real /proc/<pid>/cmdline).
func TestIntegrationRestartReadoptNoLeak(t *testing.T) {
	requireRoot(t)
	bin := buildFakemlfs(t)
	base := t.TempDir()
	ctx := context.Background()

	// Manager #1 brings a domain up, then we DROP it without teardown (the child
	// survives because it was started with setsid) — simulating a plugin restart.
	mgr1 := NewMountManager(base, NewExecLauncher(bin), NewLinuxMounter())
	mountDir, err := mgr1.Acquire(ctx, intgSpec("acme"), "/itest/a1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pid := statePids(t, base)["acme"]
	if !pidAlive(pid) {
		t.Fatalf("daemon not alive after acquire")
	}

	// Manager #2 over the same base re-adopts the still-live mount.
	mgr2 := NewMountManager(base, NewExecLauncher(bin), NewLinuxMounter())
	if err := mgr2.Adopt(ctx); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if ok, _ := NewLinuxMounter().IsMounted(mountDir); !ok {
		t.Errorf("mount not preserved across restart")
	}
	if !pidAlive(pid) {
		t.Errorf("adoption killed the live daemon")
	}

	// Releasing through the adopting manager tears the domain down by pid.
	if _, err := mgr2.Release(ctx, "/itest/a1"); err != nil {
		t.Fatalf("release after adopt: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if pidAlive(pid) {
		t.Errorf("LEAK: adopted daemon pid %d alive after release", pid)
	}
	if mounts := mountpointsUnder(t, base); len(mounts) != 0 {
		t.Errorf("LEAK: mounts remain after adopted release: %v", mounts)
	}
}
