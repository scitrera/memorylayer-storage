// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package mountlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckQuiescedNoFile(t *testing.T) {
	dir := t.TempDir()
	if err := CheckQuiesced(dir, false, failWarn(t)); err != nil {
		t.Fatalf("no lock file should be quiesced, got %v", err)
	}
}

func TestCheckQuiescedLiveRefuses(t *testing.T) {
	dir := t.TempDir()
	// Write a lock file claiming the current (live) process.
	if err := Write(dir); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err := CheckQuiesced(dir, false, failWarn(t))
	var le *LiveError
	if !errors.As(err, &le) {
		t.Fatalf("live daemon should refuse with *LiveError, got %v", err)
	}
	if le.PID != os.Getpid() {
		t.Fatalf("LiveError pid = %d, want %d", le.PID, os.Getpid())
	}
}

func TestCheckQuiescedForceOnlineOverrides(t *testing.T) {
	dir := t.TempDir()
	if err := Write(dir); err != nil {
		t.Fatalf("Write: %v", err)
	}
	warned := false
	if err := CheckQuiesced(dir, true, func(string) { warned = true }); err != nil {
		t.Fatalf("-force-online should proceed past a live daemon, got %v", err)
	}
	if !warned {
		t.Fatal("-force-online should emit a warning")
	}
}

func TestCheckQuiescedStalePIDProceeds(t *testing.T) {
	dir := t.TempDir()
	// A PID that is almost certainly not running. PIDs are capped well below
	// this on Linux; kill(0) returns ESRCH.
	stalePID := 0x7FFFFFFE
	if err := os.WriteFile(Path(dir), []byte(itoa(stalePID)), 0o644); err != nil {
		t.Fatalf("seed stale lock: %v", err)
	}
	warned := false
	if err := CheckQuiesced(dir, false, func(string) { warned = true }); err != nil {
		t.Fatalf("stale PID should proceed, got %v", err)
	}
	if !warned {
		t.Fatal("stale lock file should emit a warning")
	}
}

func TestCheckQuiescedUnparseableProceeds(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(Path(dir), []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatalf("seed garbage lock: %v", err)
	}
	warned := false
	if err := CheckQuiesced(dir, false, func(string) { warned = true }); err != nil {
		t.Fatalf("unparseable lock should proceed, got %v", err)
	}
	if !warned {
		t.Fatal("unparseable lock file should emit a warning")
	}
}

func TestWriteRemoveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := Write(dir); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := os.Stat(Path(dir)); err != nil {
		t.Fatalf("lock file should exist after Write: %v", err)
	}
	if err := Remove(dir); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(Path(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lock file should be gone after Remove, stat err = %v", err)
	}
	// Remove is idempotent: a second call (file already gone) is not an error.
	if err := Remove(dir); err != nil {
		t.Fatalf("Remove on missing file should be nil, got %v", err)
	}
}

func TestPathLayout(t *testing.T) {
	if got, want := Path("/data"), filepath.Join("/data", FileName); got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func failWarn(t *testing.T) func(string) {
	t.Helper()
	return func(msg string) { t.Fatalf("unexpected warning: %s", msg) }
}

// itoa avoids importing strconv just for the test seed.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
