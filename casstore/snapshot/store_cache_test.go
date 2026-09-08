// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// errorStore is a minimal SnapshotStore that always returns an error.
// Used to verify the cache serves blobs even when upstream is broken.
type errorStore struct{}

func (e *errorStore) Put(_ context.Context, _ SnapshotKey, _ SnapshotMetadata, _ io.Reader) (SnapshotMetadata, error) {
	return SnapshotMetadata{}, errors.New("upstream unavailable")
}
func (e *errorStore) Get(_ context.Context, _ SnapshotKey, _ string) (io.ReadCloser, SnapshotMetadata, error) {
	return nil, SnapshotMetadata{}, errors.New("upstream unavailable")
}
func (e *errorStore) GetLatest(_ context.Context, _ SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	return nil, SnapshotMetadata{}, errors.New("upstream unavailable")
}
func (e *errorStore) GetLatestMetadata(_ context.Context, _ SnapshotKey) (SnapshotMetadata, error) {
	return SnapshotMetadata{}, errors.New("upstream unavailable")
}
func (e *errorStore) List(_ context.Context, _ SnapshotKey) ([]SnapshotMetadata, error) {
	return nil, errors.New("upstream unavailable")
}
func (e *errorStore) Delete(_ context.Context, _ SnapshotKey, _ string) error {
	return errors.New("upstream unavailable")
}
func (e *errorStore) Walk(_ context.Context, _ func(SnapshotKey, string, SnapshotMetadata) error) error {
	return errors.New("upstream unavailable")
}

// captureStore wraps a LocalStore and records every Put call.
type captureStore struct {
	inner *LocalStore
	puts  int
}

func (c *captureStore) Put(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	c.puts++
	return c.inner.Put(ctx, key, meta, r)
}
func (c *captureStore) Get(ctx context.Context, key SnapshotKey, v string) (io.ReadCloser, SnapshotMetadata, error) {
	return c.inner.Get(ctx, key, v)
}
func (c *captureStore) GetLatest(ctx context.Context, key SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	return c.inner.GetLatest(ctx, key)
}
func (c *captureStore) GetLatestMetadata(ctx context.Context, key SnapshotKey) (SnapshotMetadata, error) {
	return c.inner.GetLatestMetadata(ctx, key)
}
func (c *captureStore) List(ctx context.Context, key SnapshotKey) ([]SnapshotMetadata, error) {
	return c.inner.List(ctx, key)
}
func (c *captureStore) Delete(ctx context.Context, key SnapshotKey, v string) error {
	return c.inner.Delete(ctx, key, v)
}
func (c *captureStore) Walk(ctx context.Context, yield func(SnapshotKey, string, SnapshotMetadata) error) error {
	return c.inner.Walk(ctx, yield)
}

func TestCacheStore_PutPropagatesUpstreamAndCache(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	cap := &captureStore{inner: upstream}
	cache, err := NewCacheStore(cap, cacheDir, 10<<20) // 10 MB cap
	if err != nil {
		t.Fatalf("NewCacheStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	blob := []byte("blob data")
	stored, err := cache.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if cap.puts != 1 {
		t.Errorf("upstream Put count = %d, want 1", cap.puts)
	}

	// Verify the blob is in the local cache.
	localStore, _ := NewLocalStore(cacheDir)
	rc, _, err := localStore.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("cache local Get: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(data, blob) {
		t.Errorf("cache content = %q, want %q", data, blob)
	}
}

func TestCacheStore_GetHitsCache_UpstreamNotCalled(t *testing.T) {
	// Put via a real upstream first, then replace upstream with an errorStore.
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	realUpstream, _ := NewLocalStore(upstreamDir)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Pre-populate the upstream.
	stored, err := realUpstream.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("cached blob"))
	if err != nil {
		t.Fatalf("pre-populate upstream: %v", err)
	}

	// Build cache with the real upstream and warm the cache via a Get.
	cache1, _ := NewCacheStore(realUpstream, cacheDir, 10<<20)
	rc, _, err := cache1.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("cache warm Get: %v", err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()

	// Swap in the errorStore — the local cache should still serve.
	cache2, err := NewCacheStore(&errorStore{}, cacheDir, 10<<20)
	if err != nil {
		t.Fatalf("NewCacheStore with errorStore: %v", err)
	}
	rc2, meta, err := cache2.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("Get from cache (upstream broken): %v", err)
	}
	data, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(data) != "cached blob" {
		t.Errorf("cached content = %q, want \"cached blob\"", data)
	}
	if meta.Version != stored.Version {
		t.Errorf("meta.Version = %q, want %q", meta.Version, stored.Version)
	}
}

func TestCacheStore_GetFillsCacheOnMiss(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Write directly to upstream (bypassing cache).
	stored, err := upstream.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("upstream blob"))
	if err != nil {
		t.Fatalf("upstream Put: %v", err)
	}

	cache, _ := NewCacheStore(upstream, cacheDir, 10<<20)

	// Cache miss: Get should fetch from upstream and populate cache.
	rc, _, err := cache.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("Get (miss): %v", err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()

	// Verify the blob landed in the cache directory.
	localCache, _ := NewLocalStore(cacheDir)
	rc2, _, err := localCache.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("cache local Get after miss: %v", err)
	}
	data, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(data) != "upstream blob" {
		t.Errorf("cache after miss = %q, want \"upstream blob\"", data)
	}
}

func TestCacheStore_Eviction(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	// Set maxBytes so that after two 10-byte blobs the oldest is evicted.
	const maxBytes = 15
	cache, err := NewCacheStore(upstream, cacheDir, maxBytes)
	if err != nil {
		t.Fatalf("NewCacheStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	blob10 := bytes.Repeat([]byte("x"), 10)

	m1, err := cache.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(blob10))
	if err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	// After first Put: total = 10, within budget.
	cache.mu.Lock()
	total1 := cache.total
	cache.mu.Unlock()
	if total1 != 10 {
		t.Errorf("total after 1 Put = %d, want 10", total1)
	}

	m2, err := cache.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(blob10))
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	// After second Put: 20 bytes > maxBytes (15), oldest entry should be evicted.
	cache.mu.Lock()
	total2 := cache.total
	entries2 := len(cache.entries)
	cache.mu.Unlock()
	if total2 > maxBytes {
		t.Errorf("total after eviction = %d, still exceeds maxBytes %d", total2, maxBytes)
	}
	if entries2 != 1 {
		t.Errorf("entries after eviction = %d, want 1", entries2)
	}

	// The oldest (m1) should be gone from cache; m2 should still be there.
	localCache, _ := NewLocalStore(cacheDir)
	_, _, err = localCache.Get(ctx, key, m1.Version)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("evicted m1 still in cache: got %v", err)
	}
	rc, _, err := localCache.Get(ctx, key, m2.Version)
	if err != nil {
		t.Errorf("m2 not in cache after eviction: %v", err)
	} else {
		rc.Close()
	}
}

func TestCacheStore_PerTenantIsolation(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	cache, _ := NewCacheStore(upstream, cacheDir, 10<<20)
	ctx := context.Background()

	keyA := SnapshotKey{Tenant: "tenantA", OwnerKey: "o"}
	keyB := SnapshotKey{Tenant: "tenantB", OwnerKey: "o"}

	mA, err := cache.Put(ctx, keyA, SnapshotMetadata{}, strings.NewReader("blob for A"))
	if err != nil {
		t.Fatalf("Put tenant A: %v", err)
	}

	// Tenant B should not see tenant A's blob.
	localCache, _ := NewLocalStore(cacheDir)
	_, _, err = localCache.Get(ctx, keyB, mA.Version)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("tenant B found tenant A's blob in cache: got %v", err)
	}
}

func TestCacheStore_GetLatestMetadata_FlowsCorrectly(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	cache, err := NewCacheStore(upstream, cacheDir, 10<<20)
	if err != nil {
		t.Fatalf("NewCacheStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// No snapshot yet — must return ErrNoSnapshot.
	_, err = cache.GetLatestMetadata(ctx, key)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("GetLatestMetadata empty: got %v, want ErrNoSnapshot", err)
	}

	// Put a snapshot with a tag.
	stored, putErr := cache.Put(ctx, key, SnapshotMetadata{
		Tags: map[string]string{"sandbox_cidr": "10.0.0.0/30"},
	}, strings.NewReader("blob"))
	if putErr != nil {
		t.Fatalf("Put: %v", putErr)
	}

	// GetLatestMetadata must return the tag without error.
	meta, err := cache.GetLatestMetadata(ctx, key)
	if err != nil {
		t.Fatalf("GetLatestMetadata: %v", err)
	}
	if meta.Version != stored.Version {
		t.Errorf("Version = %q, want %q", meta.Version, stored.Version)
	}
	if meta.Tags["sandbox_cidr"] != "10.0.0.0/30" {
		t.Errorf("sandbox_cidr = %q, want 10.0.0.0/30", meta.Tags["sandbox_cidr"])
	}
}

func TestCacheStore_DeleteRemovesFromBoth(t *testing.T) {
	upstreamDir := t.TempDir()
	cacheDir := t.TempDir()

	upstream, _ := NewLocalStore(upstreamDir)
	cache, _ := NewCacheStore(upstream, cacheDir, 10<<20)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	stored, err := cache.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("to be deleted"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := cache.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify upstream is gone.
	_, _, err = upstream.Get(ctx, key, stored.Version)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("upstream after Delete: got %v, want ErrNoSnapshot", err)
	}

	// Verify cache is gone.
	localCache, _ := NewLocalStore(cacheDir)
	_, _, err = localCache.Get(ctx, key, stored.Version)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("local cache after Delete: got %v, want ErrNoSnapshot", err)
	}
}
