// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// CacheStore wraps an upstream SnapshotStore with a local-filesystem LRU
// cache. Each tenant gets its own sub-directory under cacheDir to prevent
// cross-tenant blob leakage (risk #10 in the convergence plan).
//
// Cache truth: the upstream store is always authoritative for List /
// GetLatest. The cache is a read-acceleration layer; it may hold a subset of
// what the upstream holds.
//
// Eviction: runs synchronously after each Put (best-effort; does not block Put
// on eviction). The LRU index is seeded at startup from an on-disk scan and
// kept consistent in memory thereafter.
type CacheStore struct {
	upstream SnapshotStore
	local    *LocalStore // cache backend on-disk

	maxBytes int64

	mu      sync.Mutex
	entries []cacheEntry // LRU list: oldest first, newest last
	total   int64        // current total bytes tracked
}

type cacheEntry struct {
	cacheKey string // opaque key for the cache LRU index
	size     int64
}

// NewCacheStore constructs a CacheStore that uses cacheDir as the local
// cache root and upstream as the durable backing store. maxBytes is the soft
// cap on total cached bytes; when exceeded after a Put the oldest entries are
// evicted until the total is within budget.
func NewCacheStore(upstream SnapshotStore, cacheDir string, maxBytes int64) (*CacheStore, error) {
	local, err := NewLocalStore(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("snapshot/cache: init local cache: %w", err)
	}
	c := &CacheStore{
		upstream: upstream,
		local:    local,
		maxBytes: maxBytes,
	}
	if err := c.seedIndex(); err != nil {
		// Non-fatal: worst case we over-evict or under-evict until the index
		// is rebuilt from actual Puts and Gets.
		slog.Warn("snapshot/cache: index seed failed (cache still usable)", "err", err)
	}
	return c, nil
}

// Put writes the blob to both the upstream store and the local cache. If the
// upstream write fails the cache entry is rolled back and the error is
// returned. SizeBytes is recomputed from the stream by the upstream store;
// the cache stores whatever the upstream confirms.
func (c *CacheStore) Put(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	// Buffer the stream so we can write to both stores.
	// Note: for very large blobs this is memory-intensive. A production
	// hardening step (Phase 5) could tee through a temp file instead.
	buf, err := io.ReadAll(r)
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("snapshot/cache: buffer stream: %w", err)
	}

	// Write to upstream first — it is the source of truth.
	stored, err := c.upstream.Put(ctx, key, meta, bytes.NewReader(buf))
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("snapshot/cache: upstream put: %w", err)
	}

	// Populate local cache under the exact version assigned by upstream.
	// Use putWithMeta so the local store does not generate a second version.
	// Best-effort: cache failure does not fail the Put.
	if _, cerr := c.local.putWithMeta(ctx, stored, bytes.NewReader(buf)); cerr != nil {
		slog.WarnContext(ctx, "snapshot/cache: local cache write failed (upstream succeeded)",
			"key", key, "version", stored.Version, "err", cerr)
	} else {
		c.mu.Lock()
		ck := cacheKeyFor(key, stored.Version)
		c.entries = append(c.entries, cacheEntry{cacheKey: ck, size: stored.SizeBytes})
		c.total += stored.SizeBytes
		c.mu.Unlock()
		c.evict(ctx)
	}

	return stored, nil
}

// Get tries the local cache first. On a cache miss it fetches from upstream
// and populates the cache while streaming to the caller.
func (c *CacheStore) Get(ctx context.Context, key SnapshotKey, version string) (io.ReadCloser, SnapshotMetadata, error) {
	// Try cache.
	rc, meta, err := c.local.Get(ctx, key, version)
	if err == nil {
		slog.DebugContext(ctx, "snapshot/cache: cache hit", "key", key, "version", version)
		return rc, meta, nil
	}
	if err != ErrNoSnapshot {
		// Unexpected local error; fall through to upstream.
		slog.WarnContext(ctx, "snapshot/cache: local get error; falling back to upstream",
			"key", key, "version", version, "err", err)
	}

	// Cache miss: fetch from upstream.
	slog.DebugContext(ctx, "snapshot/cache: cache miss; fetching from upstream",
		"key", key, "version", version)
	upRC, upMeta, err := c.upstream.Get(ctx, key, version)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}

	// Read the full blob from upstream so we can populate the cache and still
	// return a reader to the caller.
	data, err := io.ReadAll(upRC)
	_ = upRC.Close()
	if err != nil {
		return nil, SnapshotMetadata{}, fmt.Errorf("snapshot/cache: read upstream blob: %w", err)
	}

	// Populate cache under the exact version from upstream (best-effort).
	if _, cerr := c.local.putWithMeta(ctx, upMeta, bytes.NewReader(data)); cerr != nil {
		slog.WarnContext(ctx, "snapshot/cache: failed to populate cache on miss",
			"key", key, "version", version, "err", cerr)
	} else {
		c.mu.Lock()
		ck := cacheKeyFor(key, upMeta.Version)
		c.entries = append(c.entries, cacheEntry{cacheKey: ck, size: upMeta.SizeBytes})
		c.total += upMeta.SizeBytes
		c.mu.Unlock()
		c.evict(ctx)
	}

	return io.NopCloser(bytes.NewReader(data)), upMeta, nil
}

// GetLatest delegates to the upstream store for authoritative version
// resolution, then falls through to Get (which hits cache if warm).
func (c *CacheStore) GetLatest(ctx context.Context, key SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	// Ask upstream which version is latest.
	_, upMeta, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}
	return c.Get(ctx, key, upMeta.Version)
}

// GetLatestMetadata returns only the metadata for the highest-version snapshot
// without fetching the blob. It delegates version resolution to the upstream
// store (authoritative), tries the local cache's metadata first, and falls
// back to the upstream metadata if the cache doesn't have it. The blob is
// never opened in either path.
func (c *CacheStore) GetLatestMetadata(ctx context.Context, key SnapshotKey) (SnapshotMetadata, error) {
	// Resolve which version is latest from upstream.
	upMeta, err := c.upstream.GetLatestMetadata(ctx, key)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	// Try local cache metadata first (avoids a second upstream round-trip).
	if localMeta, lerr := c.local.GetLatestMetadata(ctx, key); lerr == nil && localMeta.Version == upMeta.Version {
		slog.DebugContext(ctx, "snapshot/cache: get-latest-metadata: cache hit",
			"key", key, "version", localMeta.Version)
		return localMeta, nil
	}
	// Cache miss or version mismatch — return what upstream already told us.
	slog.DebugContext(ctx, "snapshot/cache: get-latest-metadata: using upstream metadata",
		"key", key, "version", upMeta.Version)
	return upMeta, nil
}

// List is a pass-through to upstream; cache may hold only a subset.
func (c *CacheStore) List(ctx context.Context, key SnapshotKey) ([]SnapshotMetadata, error) {
	return c.upstream.List(ctx, key)
}

// Walk is a pass-through to upstream. The cache holds a subset by
// design, so authoritative enumeration must consult the source of truth.
func (c *CacheStore) Walk(ctx context.Context, yield func(SnapshotKey, string, SnapshotMetadata) error) error {
	return c.upstream.Walk(ctx, yield)
}

// Delete removes the version from both the upstream store and the local cache.
func (c *CacheStore) Delete(ctx context.Context, key SnapshotKey, version string) error {
	if err := c.upstream.Delete(ctx, key, version); err != nil {
		return err
	}
	// Best-effort cache eviction.
	if err := c.local.Delete(ctx, key, version); err != nil {
		slog.WarnContext(ctx, "snapshot/cache: local delete failed (upstream deleted)",
			"key", key, "version", version, "err", err)
	}
	c.mu.Lock()
	ck := cacheKeyFor(key, version)
	c.removeEntry(ck)
	c.mu.Unlock()
	return nil
}

// --- LRU eviction -----------------------------------------------------------

// evict removes the oldest cache entries until total is within maxBytes.
// Must NOT be called with c.mu held.
func (c *CacheStore) evict(ctx context.Context) {
	c.mu.Lock()
	toEvict := c.collectEvictions()
	c.mu.Unlock()

	for _, e := range toEvict {
		key, version, ok := parseCacheKey(e.cacheKey)
		if !ok {
			continue
		}
		if err := c.local.Delete(ctx, key, version); err != nil {
			slog.WarnContext(ctx, "snapshot/cache: eviction delete failed",
				"cacheKey", e.cacheKey, "err", err)
		}
	}
}

// collectEvictions selects entries to remove and updates the index.
// Must be called with c.mu held.
func (c *CacheStore) collectEvictions() []cacheEntry {
	if c.total <= c.maxBytes {
		return nil
	}
	var victims []cacheEntry
	// Evict oldest-first (entries[0] is oldest).
	for c.total > c.maxBytes && len(c.entries) > 0 {
		v := c.entries[0]
		c.entries = c.entries[1:]
		c.total -= v.size
		victims = append(victims, v)
	}
	return victims
}

// removeEntry removes the entry with the given cacheKey from the LRU index.
// Must be called with c.mu held.
func (c *CacheStore) removeEntry(cacheKey string) {
	for i, e := range c.entries {
		if e.cacheKey == cacheKey {
			c.total -= e.size
			c.entries = append(c.entries[:i], c.entries[i+1:]...)
			return
		}
	}
}

// --- startup index seed -----------------------------------------------------

// seedIndex scans cacheDir to rebuild the in-memory LRU index. Entries are
// ordered by mtime (oldest first) so eviction is approximately LRU even across
// service restarts.
func (c *CacheStore) seedIndex() error {
	type entry struct {
		cacheKey string
		size     int64
		mtime    int64
	}
	var all []entry

	// Walk up to 5 levels: tenant/workspace/user/ownerKey/version
	err := filepath.WalkDir(c.local.rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		blobFile := filepath.Join(path, "blob")
		info, berr := os.Stat(blobFile)
		if berr != nil {
			return nil // not a version directory
		}
		// Reconstruct cache key from path relative to rootDir.
		rel, _ := filepath.Rel(c.local.rootDir, path)
		parts := splitPath(rel)
		if len(parts) != 5 {
			return nil // not a tenant/workspace/user/ownerKey/version directory
		}
		// parts: [tenant, workspace, user, ownerKey, version]
		key := SnapshotKey{
			Tenant:    unSanitize(parts[0]),
			Workspace: unSanitize(parts[1]),
			User:      unSanitize(parts[2]),
			OwnerKey:  unSanitize(parts[3]),
		}
		version := parts[4]
		ck := cacheKeyFor(key, version)
		all = append(all, entry{cacheKey: ck, size: info.Size(), mtime: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return err
	}

	// Sort by mtime ascending so we evict oldest first.
	sort.Slice(all, func(i, j int) bool { return all[i].mtime < all[j].mtime })

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make([]cacheEntry, 0, len(all))
	c.total = 0
	for _, e := range all {
		c.entries = append(c.entries, cacheEntry{cacheKey: e.cacheKey, size: e.size})
		c.total += e.size
	}
	return nil
}

// --- cache key helpers ------------------------------------------------------

// cacheKeyFor builds an opaque string used as the LRU map key.
func cacheKeyFor(key SnapshotKey, version string) string {
	return sanitize(key.Tenant) + "/" +
		sanitize(key.Workspace) + "/" +
		sanitize(key.User) + "/" +
		sanitize(key.OwnerKey) + "/" +
		version
}

// parseCacheKey reverses cacheKeyFor. Returns (key, version, ok).
func parseCacheKey(ck string) (SnapshotKey, string, bool) {
	parts := splitPath(ck)
	if len(parts) != 5 {
		return SnapshotKey{}, "", false
	}
	key := SnapshotKey{
		Tenant:    unSanitize(parts[0]),
		Workspace: unSanitize(parts[1]),
		User:      unSanitize(parts[2]),
		OwnerKey:  unSanitize(parts[3]),
	}
	return key, parts[4], true
}

// splitPath splits on filepath.Separator (OS-native) and also on "/" for
// portability with cacheKeyFor which always uses "/".
func splitPath(p string) []string {
	// Normalise separators then split.
	p = filepath.ToSlash(p)
	var parts []string
	for _, seg := range filepath.SplitList(p) {
		// SplitList uses the OS path list separator, not what we want.
		_ = seg
		break
	}
	// Simple split on "/" after ToSlash normalisation.
	for _, seg := range splitSlash(p) {
		if seg != "" {
			parts = append(parts, seg)
		}
	}
	return parts
}

func splitSlash(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// unSanitize reverses the sanitize encoding: "_" → "".
// We don't attempt to reverse base64url because the SnapshotKey round-trip
// through cacheKeyFor/parseCacheKey only needs to correctly reconstruct the
// empty-field case for eviction (the key is used only for local.Delete).
func unSanitize(s string) string {
	if s == "_" {
		return ""
	}
	return s
}
