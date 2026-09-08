// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// DefaultReloadInterval is how often ReloadingResolver polls the tenant-config
// file for changes. It is a coarse interval: tenant add/remove is a rare,
// operator-driven event (a ConfigMap edit), and a newly-added tenant only needs
// to resolve "within a minute or so" of the edit, not instantly. Polling (vs an
// fsnotify watch) keeps this stdlib-only and robust to the atomic-symlink swap
// Kubernetes uses to publish a new ConfigMap projection.
const DefaultReloadInterval = 20 * time.Second

// ReloadingResolver is a tenantbind.Resolver that keeps its tenant→Descriptor
// map fresh by polling the -tenant-config file, so tenant add/remove takes
// effect WITHOUT restarting blobgw. It holds the current *tenantbind.MapResolver
// behind an atomic.Pointer and delegates Resolve to it; a background goroutine
// re-Loads the file on change and atomically swaps in a new resolver.
//
// Change detection is by sha256 of the file bytes, NOT mtime: Kubernetes
// publishes a ConfigMap update by atomically swapping the ..data symlink the
// mounted file points through, which does not reliably bump the observed mtime
// of the file path. Hashing the content is the reliable signal.
//
// Failure posture: a read/parse error during a reload (unreadable path, a
// malformed or mid-write ConfigMap projection) is logged at WARN and the
// existing good resolver is KEPT — a bad reload must never crash the process or
// blank the tenant set. The initial load (in NewReloadingResolver) is the only
// point that fails fast, preserving the prior startup behavior.
//
// path may be EITHER a single JSON file (the original behavior, unchanged) OR a
// directory of per-tenant *.json records (LoadDir). The mode is fixed at
// construction from os.Stat(path).IsDir(). In directory mode the change-detection
// hash covers ALL *.json files (sorted by name, each contributing its name and
// bytes) so adding, removing, or editing any one file triggers a reload; and a
// single malformed/unreadable file inside the dir is skipped+logged rather than
// wedging the reload — the same onboarding-robustness posture LoadDir applies at
// startup.
type ReloadingResolver struct {
	path    string
	isDir   bool
	cur     atomic.Pointer[tenantbind.MapResolver]
	lastSum atomic.Pointer[[32]byte]
}

var _ tenantbind.Resolver = (*ReloadingResolver)(nil)

// NewReloadingResolver performs an INITIAL synchronous load+Resolver() (so a
// startup misconfiguration still fails fast, exactly as the one-shot path did)
// and returns a resolver whose map can subsequently be refreshed by Start. It
// does NOT start the polling goroutine; call Start(ctx) for that.
//
// path is a single file OR a directory (decided here via os.Stat().IsDir()). In
// file mode it Loads the one file and hashes its bytes; in directory mode it
// LoadDirs the *.json set and hashes all files. The initial load fails fast on a
// HARD error only — a missing path, or (in dir mode) zero valid tenants; an
// individual malformed/unreadable file INSIDE a dir is skipped+logged (by
// LoadDir) even at startup, so one broken tenant file never blocks bring-up.
func NewReloadingResolver(path string) (*ReloadingResolver, error) {
	if path == "" {
		return nil, fmt.Errorf("tenantconfig: empty config path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: stat %q: %w", path, err)
	}
	rr := &ReloadingResolver{path: path, isDir: info.IsDir()}
	cfg, sum, err := rr.loadAndSum()
	if err != nil {
		return nil, err
	}
	rr.cur.Store(cfg.Resolver())
	rr.lastSum.Store(&sum)
	return rr, nil
}

// loadAndSum reads+parses the config (file or directory) and returns the parsed
// File alongside the content hash used for change detection. It is the shared
// core of the initial load and each reload, so both apply the identical
// file-vs-dir dispatch, parsing, and hashing.
func (r *ReloadingResolver) loadAndSum() (*File, [32]byte, error) {
	if r.isDir {
		cfg, err := LoadDir(r.path)
		if err != nil {
			return nil, [32]byte{}, err
		}
		sum, err := dirSum(r.path)
		if err != nil {
			return nil, [32]byte{}, err
		}
		return cfg, sum, nil
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("tenantconfig: read %q: %w", r.path, err)
	}
	cfg, err := parseFile(r.path, data)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return cfg, sha256.Sum256(data), nil
}

// dirSum computes a content hash over every *.json file directly under dir,
// sorted by name, each contributing its base name and bytes to the digest. This
// makes the change signal cover the WHOLE set: adding, removing, editing, or
// renaming any tenant file changes the hash (and thus triggers a reload), whereas
// hashing only a concatenation of bytes would miss a pure add/remove that leaves
// the surviving bytes unchanged. An unreadable file is skipped (its absence from
// the digest is itself a change on the next pass); the corresponding LoadDir call
// applies the same skip-and-warn posture when it actually reparses.
func dirSum(dir string) ([32]byte, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return [32]byte{}, fmt.Errorf("tenantconfig: glob %q: %w", dir, err)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			continue // skip unreadable; its exclusion is itself a hash change
		}
		// Length-prefix the name so file boundaries can't collide (two files whose
		// name+bytes happen to concatenate the same as one file's).
		fmt.Fprintf(h, "%s\x00%d\x00", filepath.Base(p), len(data))
		h.Write(data)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// Resolve delegates to the current inner MapResolver. It is safe for concurrent
// use with the background reload (the pointer load is atomic and MapResolver is
// itself concurrency-safe).
func (r *ReloadingResolver) Resolve(ctx context.Context, tenant string) (tenantbind.Descriptor, error) {
	return r.cur.Load().Resolve(ctx, tenant)
}

// Start launches the background polling goroutine bound to ctx: every
// DefaultReloadInterval it calls reloadOnce, and it exits when ctx is cancelled
// (process shutdown). It is non-blocking.
func (r *ReloadingResolver) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(DefaultReloadInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.reloadOnce()
			}
		}
	}()
}

// reloadOnce reads the config (file or directory) once and, if its content hash
// changed since the last successful load, parses it and atomically swaps in a
// fresh resolver. On ANY read or parse error it KEEPS the existing resolver and
// logs at WARN; an unchanged content hash is a silent no-op. It is unexported so
// tests can drive a reload deterministically without waiting on the ticker.
//
// In directory mode an individual malformed/unreadable file does NOT surface as
// an error here — LoadDir skips+logs it and returns the good tenants — so only a
// HARD failure (dir vanished, or zero valid tenants) keeps the previous set.
func (r *ReloadingResolver) reloadOnce() {
	cfg, sum, err := r.loadAndSum()
	if err != nil {
		slog.Warn("blobgw: tenant-config reload failed; keeping previous tenants",
			"path", r.path, "is_dir", r.isDir, "err", err)
		return
	}
	if prev := r.lastSum.Load(); prev != nil && *prev == sum {
		return // unchanged
	}
	r.cur.Store(cfg.Resolver())
	r.lastSum.Store(&sum)
	slog.Info("blobgw: tenant-config reloaded", "tenants", len(cfg.Tenants), "is_dir", r.isDir)
}
