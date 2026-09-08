// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux

package driver

import (
	"slices"
	"strings"
	"testing"
)

// argVal returns the value following flag in args, or "" if absent.
func argVal(args []string, flag string) string {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}

// TestAppendFlags_StagingSplitOnBase verifies that when the clean cache is on a
// separate Base disk, the launcher pins the durable write-back staging + WAL to
// the persistent data dir (so an ephemeral cache disk keeps write durability),
// and that without Base neither flag is emitted.
func TestAppendFlags_StagingSplitOnBase(t *testing.T) {
	const dataDir = "/var/lib/mlfs/domains/acme"

	// No Base: cache + staging stay together under -cache-dir; no split flags.
	base := CacheOpts{MaxBytes: 1 << 30}.appendFlags(nil, dataDir)
	if slices.Contains(base, "-staging-dir") || slices.Contains(base, "-wal-dir") {
		t.Errorf("no Base should not emit -staging-dir/-wal-dir: %v", base)
	}
	if argVal(base, "-cache-max-bytes") != "1073741824" {
		t.Errorf("missing -cache-max-bytes: %v", base)
	}

	// Base set: clean cache on Base, durable staging + WAL pinned to dataDir.
	split := CacheOpts{Base: "/mnt/nvme", MinFreeFraction: 0.15}.appendFlags(nil, dataDir)
	if got := argVal(split, "-staging-dir"); got != dataDir {
		t.Errorf("-staging-dir = %q, want %q", got, dataDir)
	}
	if got := argVal(split, "-wal-dir"); got != dataDir+"/wal" {
		t.Errorf("-wal-dir = %q, want %q", got, dataDir+"/wal")
	}
	if !strings.Contains(strings.Join(split, " "), "-cache-min-free-fraction 0.15") {
		t.Errorf("missing -cache-min-free-fraction: %v", split)
	}
}

// TestAppendFlags_PerNodeTuning verifies the read-heavy / GPU node knobs are
// forwarded only when set.
func TestAppendFlags_PerNodeTuning(t *testing.T) {
	const dataDir = "/var/lib/mlfs/domains/acme"

	none := CacheOpts{}.appendFlags(nil, dataDir)
	for _, f := range []string{"-max-background", "-max-rw-size", "-db-max-open-conns", "-readahead-bytes", "-readahead-concurrency"} {
		if slices.Contains(none, f) {
			t.Errorf("unset opts should not emit %s: %v", f, none)
		}
	}

	tuned := CacheOpts{MaxBackground: 128, MaxRWSize: 1 << 20, DBMaxOpenConns: 4, ReadaheadBytes: 64 << 20, ReadaheadConcurrency: 32}.appendFlags(nil, dataDir)
	if got := argVal(tuned, "-readahead-concurrency"); got != "32" {
		t.Errorf("-readahead-concurrency = %q, want 32", got)
	}
	if got := argVal(tuned, "-max-background"); got != "128" {
		t.Errorf("-max-background = %q, want 128", got)
	}
	if got := argVal(tuned, "-max-rw-size"); got != "1048576" {
		t.Errorf("-max-rw-size = %q, want 1048576", got)
	}
	if got := argVal(tuned, "-db-max-open-conns"); got != "4" {
		t.Errorf("-db-max-open-conns = %q, want 4", got)
	}
	if got := argVal(tuned, "-readahead-bytes"); got != "67108864" {
		t.Errorf("-readahead-bytes = %q, want 67108864", got)
	}
}

// TestAppendFlags_Compression verifies the file-type compression policy flags are
// forwarded to the mlfs daemon only when set (TECH_DEBT #48 CSI wiring).
func TestAppendFlags_Compression(t *testing.T) {
	const dataDir = "/var/lib/mlfs/domains/acme"

	none := CacheOpts{}.appendFlags(nil, dataDir)
	for _, f := range []string{"-compression", "-default-store-class"} {
		if slices.Contains(none, f) {
			t.Errorf("unset opts should not emit %s: %v", f, none)
		}
	}

	mixed := CacheOpts{Compression: "zstd", DefaultStoreClass: "uncompressed"}.appendFlags(nil, dataDir)
	if got := argVal(mixed, "-compression"); got != "zstd" {
		t.Errorf("-compression = %q, want zstd", got)
	}
	if got := argVal(mixed, "-default-store-class"); got != "uncompressed" {
		t.Errorf("-default-store-class = %q, want uncompressed", got)
	}

	// Compression without an explicit default-store-class emits only -compression.
	onlyComp := CacheOpts{Compression: "zstd"}.appendFlags(nil, dataDir)
	if !slices.Contains(onlyComp, "-compression") || slices.Contains(onlyComp, "-default-store-class") {
		t.Errorf("expected -compression only, got %v", onlyComp)
	}
}
