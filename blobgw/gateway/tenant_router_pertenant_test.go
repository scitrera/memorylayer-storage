// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// localBackendFactory is a fake BackendFactory for the per-tenant path (ADR
// §2.6) that returns, per tenant, LOCAL casstore stores rooted in a distinct
// temp directory — i.e. each tenant resolves to a physically distinct backend,
// exactly like a per-tenant S3 bucket, without requiring real S3. It caches the
// constructed stores per tenant, mirroring perTenantBackend.
type localBackendFactory struct {
	t    *testing.T
	root string

	mu    sync.Mutex
	cache map[string]tenantStores
	dirs  map[string]string // tenant -> its backend root, for inspection
}

func newLocalBackendFactory(t *testing.T) *localBackendFactory {
	t.Helper()
	return &localBackendFactory{
		t:     t,
		root:  t.TempDir(),
		cache: make(map[string]tenantStores),
		dirs:  make(map[string]string),
	}
}

func (f *localBackendFactory) For(ctx context.Context, tenant string) (snapshot.SnapshotStore, blobstore.Storage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ts, ok := f.cache[tenant]; ok {
		return ts.upstream, ts.chunks, nil
	}
	dir := filepath.Join(f.root, tenant)
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		return nil, nil, err
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		return nil, nil, err
	}
	f.t.Cleanup(func() { _ = chunks.Close(context.Background()) })
	f.cache[tenant] = tenantStores{upstream: upstream, chunks: chunks}
	f.dirs[tenant] = dir
	return upstream, chunks, nil
}

// chunksFor returns the chunk store a previously-resolved tenant was bound to,
// for asserting physical isolation between tenants.
func (f *localBackendFactory) chunksFor(tenant string) blobstore.Storage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cache[tenant].chunks
}

// countBlobs totals the physical blob bytes in a chunk store (all packs +
// chunks across every domain prefix). Used to detect whether a tenant's bytes
// landed in the expected (and only the expected) backend.
func countBlobs(t *testing.T, chunks blobstore.Storage, domain string) (count int, bytesTotal int64) {
	t.Helper()
	for _, pfx := range []blobstore.ID{
		blobstore.ID("pack-" + domain + "-"),
		blobstore.ID("chunk-" + domain + "-"),
	} {
		if err := chunks.ListBlobs(context.Background(), pfx, func(md blobstore.Metadata) error {
			count++
			bytesTotal += md.Length
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs %q: %v", pfx, err)
		}
	}
	return count, bytesTotal
}

// TestTenantRouter_PerTenantBackend_DistinctPhysicalBackends is the core ADR
// §2.6 assertion: under a per-tenant backend binding, tenant A and tenant B
// resolve to DISTINCT physical backends. A blob written via A's gateway lands
// only in A's chunk store and is NOT present in (nor deduped against) B's chunk
// store — even for byte-identical content. Cross-tenant dedup is gone, by
// design.
func TestTenantRouter_PerTenantBackend_DistinctPhysicalBackends(t *testing.T) {
	ctx := context.Background()
	factory := newLocalBackendFactory(t)
	// Disable compression so physical-byte assertions are over raw, predictable
	// content (the per-tenant binding is orthogonal to the compression policy).
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithCompressionPolicy(nil), withBackendFactory(factory))

	payload := bytes.Repeat([]byte("per-tenant isolation payload. "), 4096) // ~120 KiB, identical for both tenants

	gwA, err := r.For("alpha")
	if err != nil {
		t.Fatalf("For(alpha): %v", err)
	}
	if _, err := gwA.Put(ctx, "doc.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put alpha/doc.bin: %v", err)
	}

	// A's bytes are in A's backend under A's domain.
	chunksA := factory.chunksFor("alpha")
	if cnt, total := countBlobs(t, chunksA, "alpha"); cnt == 0 || total == 0 {
		t.Fatalf("expected alpha's blobs in alpha backend, got count=%d bytes=%d", cnt, total)
	}

	// B has not even been resolved yet: writing identical content to B must NOT
	// dedup against A (no shared chunk store, no shared physical copy).
	gwB, err := r.For("beta")
	if err != nil {
		t.Fatalf("For(beta): %v", err)
	}
	if _, err := gwB.Put(ctx, "doc.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put beta/doc.bin: %v", err)
	}

	chunksB := factory.chunksFor("beta")
	if chunksA == chunksB {
		t.Fatal("tenants A and B share the same chunk store: backends are not distinct")
	}

	// B's identical content produced its OWN physical copy in B's backend.
	cntB, bytesB := countBlobs(t, chunksB, "beta")
	if cntB == 0 || bytesB == 0 {
		t.Fatalf("expected beta's own physical copy, got count=%d bytes=%d", cntB, bytesB)
	}

	// Cross-tenant existence check: alpha's domain has NO blobs in B's backend,
	// and beta's domain has NO blobs in A's backend. Nothing crossed over.
	if cnt, _ := countBlobs(t, chunksB, "alpha"); cnt != 0 {
		t.Fatalf("alpha's blobs leaked into beta's backend: count=%d", cnt)
	}
	if cnt, _ := countBlobs(t, chunksA, "beta"); cnt != 0 {
		t.Fatalf("beta's blobs leaked into alpha's backend: count=%d", cnt)
	}

	// Both objects round-trip from their own backends.
	if got := getBody(t, gwA, "doc.bin"); !bytes.Equal(got, payload) {
		t.Fatal("alpha round-trip mismatch")
	}
	if got := getBody(t, gwB, "doc.bin"); !bytes.Equal(got, payload) {
		t.Fatal("beta round-trip mismatch")
	}
}

// TestTenantRouter_PerTenantBackend_DedupsWithinTenant confirms the per-tenant
// posture still dedups WITHIN a tenant (the dedup index is shared and keyed by
// DedupDomain=tenant, ADR §2.6): the same content under two refs in one tenant
// stores one physical copy.
func TestTenantRouter_PerTenantBackend_DedupsWithinTenant(t *testing.T) {
	ctx := context.Background()
	factory := newLocalBackendFactory(t)
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithCompressionPolicy(nil), withBackendFactory(factory))

	gw, err := r.For("alpha")
	if err != nil {
		t.Fatalf("For(alpha): %v", err)
	}
	payload := bytes.Repeat([]byte("dedup within tenant. "), 4096)

	if _, err := gw.Put(ctx, "a.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put a.bin: %v", err)
	}
	_, bytesAfterFirst := countBlobs(t, factory.chunksFor("alpha"), "alpha")
	if bytesAfterFirst == 0 {
		t.Fatal("expected physical blobs after first put")
	}
	if _, err := gw.Put(ctx, "b.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put b.bin: %v", err)
	}
	_, bytesAfterSecond := countBlobs(t, factory.chunksFor("alpha"), "alpha")
	if bytesAfterSecond != bytesAfterFirst {
		t.Errorf("within-tenant dedup failed: bytes grew %d -> %d on identical content",
			bytesAfterFirst, bytesAfterSecond)
	}
}

// TestTenantRouter_WithBindingProvider_WiresPerTenantBackend exercises the
// PUBLIC WithBindingProvider wiring (ADR §2.6 / §2.9) with a real
// tenantbind.Provider, without real S3. perTenantBackend.For builds live S3
// clients (kopia's blobstore contacts the endpoint at construction), which is
// out of scope for a unit test, so this test asserts the seam UP TO that point:
//
//  1. WithBindingProvider installs a perTenantBackend (not the shared default).
//  2. The installed backend holds the supplied provider, and the provider's
//     per-tenant binding seam works (each tenant resolves to its own bucket).
//  3. GC() is nil in the per-tenant posture while GCForTenant is the supported
//     path (its backend resolution is the same For() the S3 path uses).
//
// The full For()→S3 build path is covered against distinct *physical* backends
// by TestTenantRouter_PerTenantBackend_DistinctPhysicalBackends via the local
// fake factory, so the only gap here — real S3 client construction — is an
// integration concern, not a unit one.
func TestTenantRouter_WithBindingProvider_WiresPerTenantBackend(t *testing.T) {
	ctx := context.Background()
	resolver := tenantbind.NewMapResolver()
	secrets := tenantbind.NewMapSecretStore()
	// Two tenants, two distinct buckets — the §2.6 per-tenant bucket binding.
	resolver.Set("alpha", tenantbind.Descriptor{
		Endpoint: "s3.local.invalid:9000", Region: "us-east-1", Bucket: "tenant-alpha", CredentialRef: "alpha-cred",
	})
	resolver.Set("beta", tenantbind.Descriptor{
		Endpoint: "s3.local.invalid:9000", Region: "us-east-1", Bucket: "tenant-beta", CredentialRef: "beta-cred",
	})
	secrets.Set("alpha-cred", "ak-alpha", "sk-alpha")
	secrets.Set("beta-cred", "ak-beta", "sk-beta")

	provider := &recordingProvider{inner: tenantbind.NewStaticKeysProvider(resolver, secrets)}
	cache, err := tenantbind.NewCachingProvider(provider, time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}

	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithBindingProvider(cache, nil))

	// (1) The option installed a perTenantBackend over our provider.
	ptb, ok := r.backend.(*perTenantBackend)
	if !ok {
		t.Fatalf("WithBindingProvider did not install perTenantBackend, got %T", r.backend)
	}
	if ptb.provider != tenantbind.Provider(cache) {
		t.Fatal("perTenantBackend does not hold the supplied provider")
	}

	// (2) The per-tenant binding seam resolves each tenant to its own bucket.
	bAlpha, err := ptb.provider.BindingFor(ctx, "alpha")
	if err != nil {
		t.Fatalf("BindingFor(alpha): %v", err)
	}
	bBeta, err := ptb.provider.BindingFor(ctx, "beta")
	if err != nil {
		t.Fatalf("BindingFor(beta): %v", err)
	}
	if bAlpha.Bucket != "tenant-alpha" || bBeta.Bucket != "tenant-beta" {
		t.Fatalf("per-tenant buckets not distinct: alpha=%q beta=%q", bAlpha.Bucket, bBeta.Bucket)
	}
	if got := provider.tenants(); len(got) != 2 {
		t.Fatalf("expected provider consulted for 2 distinct tenants, got %v", got)
	}

	// (3) GC() is not valid in the per-tenant posture; GCForTenant is the path.
	if r.GC() != nil {
		t.Fatal("GC() should be nil under the per-tenant posture (use GCForTenant)")
	}
}

// TestTenantRouter_ChunksForTenant resolves a tenant's OWN chunk store through
// the router (ADR §2.6): it returns exactly the store the backend factory bound
// to that tenant, distinct tenants get distinct stores, and an empty tenant is
// an error. This is what the tenant-router-aware pack-size backfill relies on to
// list a real tenant's packs instead of the shared/placeholder backend.
func TestTenantRouter_ChunksForTenant(t *testing.T) {
	ctx := context.Background()
	factory := newLocalBackendFactory(t)
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithCompressionPolicy(nil), withBackendFactory(factory))

	// Empty tenant → error, no store.
	if _, err := r.ChunksForTenant(ctx, ""); err == nil {
		t.Fatal("ChunksForTenant(\"\") = nil error, want error")
	}

	// alpha resolves to the SAME chunk store the factory bound it to.
	chunksAlpha, err := r.ChunksForTenant(ctx, "alpha")
	if err != nil {
		t.Fatalf("ChunksForTenant(alpha): %v", err)
	}
	if chunksAlpha == nil {
		t.Fatal("ChunksForTenant(alpha) returned nil store")
	}
	if got := factory.chunksFor("alpha"); got != chunksAlpha {
		t.Fatalf("ChunksForTenant(alpha) = %p, want the factory-bound store %p", chunksAlpha, got)
	}

	// A blob written through alpha's gateway is present in the store ChunksForTenant
	// returned — proving it is the tenant's REAL backend, not a placeholder.
	gw, err := r.For("alpha")
	if err != nil {
		t.Fatalf("For(alpha): %v", err)
	}
	payload := bytes.Repeat([]byte("chunks-for-tenant payload. "), 4096)
	if _, err := gw.Put(ctx, "doc.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put alpha/doc.bin: %v", err)
	}
	if cnt, total := countBlobs(t, chunksAlpha, "alpha"); cnt == 0 || total == 0 {
		t.Fatalf("expected alpha's blobs in the ChunksForTenant store, got count=%d bytes=%d", cnt, total)
	}

	// A distinct tenant resolves to a distinct store.
	chunksBeta, err := r.ChunksForTenant(ctx, "beta")
	if err != nil {
		t.Fatalf("ChunksForTenant(beta): %v", err)
	}
	if chunksBeta == chunksAlpha {
		t.Fatal("ChunksForTenant returned the same store for alpha and beta: backends not distinct")
	}
}

// countingBackendFactory returns fresh local stores on EVERY For call (no
// caching), counting builds per tenant. It lets a test observe whether the
// router rebuilt the per-tenant chain (which it must do on cache-TTL expiry so
// rotated credentials propagate, ADR §2.9).
type countingBackendFactory struct {
	t    *testing.T
	root string

	mu     sync.Mutex
	builds map[string]int
}

func newCountingBackendFactory(t *testing.T) *countingBackendFactory {
	t.Helper()
	return &countingBackendFactory{t: t, root: t.TempDir(), builds: make(map[string]int)}
}

func (f *countingBackendFactory) For(ctx context.Context, tenant string) (snapshot.SnapshotStore, blobstore.Storage, error) {
	f.mu.Lock()
	f.builds[tenant]++
	n := f.builds[tenant]
	f.mu.Unlock()
	// A distinct directory per build so each rebuild is a physically distinct
	// backend (mirroring an S3 client rebuilt with rotated credentials).
	dir := filepath.Join(f.root, tenant, itoa(n))
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		return nil, nil, err
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		return nil, nil, err
	}
	f.t.Cleanup(func() { _ = chunks.Close(context.Background()) })
	return upstream, chunks, nil
}

func (f *countingBackendFactory) buildCount(tenant string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.builds[tenant]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestTenantRouter_PerTenantCacheTTL_RebuildsOnExpiry verifies item-3 credential
// rotation handling (ADR §2.9): with a per-tenant cache TTL the router rebuilds
// the whole per-tenant chain (gateway + chunked store + backend stores) after the
// TTL, picking up a rotated binding; within the TTL it serves the cached gateway.
func TestTenantRouter_PerTenantCacheTTL_RebuildsOnExpiry(t *testing.T) {
	ctx := context.Background()
	factory := newCountingBackendFactory(t)
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithCompressionPolicy(nil), withBackendFactory(factory),
		WithPerTenantCacheTTL(time.Minute))

	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	r.now = func() time.Time { return time.Unix(0, nowNs.Load()) }

	gw1, err := r.ForCtx(ctx, "alpha")
	if err != nil {
		t.Fatalf("ForCtx alpha: %v", err)
	}
	if got := factory.buildCount("alpha"); got != 1 {
		t.Fatalf("expected 1 backend build, got %d", got)
	}

	// Within TTL: cached gateway, no rebuild.
	nowNs.Add(int64(30 * time.Second))
	gw2, err := r.ForCtx(ctx, "alpha")
	if err != nil {
		t.Fatalf("ForCtx alpha within ttl: %v", err)
	}
	if gw2 != gw1 {
		t.Fatal("gateway rebuilt within TTL: cache not honored")
	}
	if got := factory.buildCount("alpha"); got != 1 {
		t.Fatalf("expected no rebuild within ttl, got %d builds", got)
	}

	// Past TTL: rebuild (rotation pickup) — fresh gateway + fresh backend build.
	nowNs.Add(int64(2 * time.Minute))
	gw3, err := r.ForCtx(ctx, "alpha")
	if err != nil {
		t.Fatalf("ForCtx alpha after ttl: %v", err)
	}
	if gw3 == gw1 {
		t.Fatal("gateway not rebuilt after TTL: rotated credentials would not propagate")
	}
	if got := factory.buildCount("alpha"); got != 2 {
		t.Fatalf("expected rebuild after ttl (2 builds), got %d", got)
	}
}

// TestTenantRouter_SharedBackend_CachePermanent confirms the SHARED-backend path
// keeps its permanent cache (cacheTTL==0): no rebuild ever, regardless of elapsed
// time — item-3 requires the shared path's behavior is untouched.
func TestTenantRouter_SharedBackend_CachePermanent(t *testing.T) {
	ctx := context.Background()
	factory := newCountingBackendFactory(t)
	// No WithBindingProvider / WithPerTenantCacheTTL → cacheTTL stays 0.
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithCompressionPolicy(nil), withBackendFactory(factory))
	if r.cacheTTL != 0 {
		t.Fatalf("shared posture must have cacheTTL=0, got %s", r.cacheTTL)
	}

	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	r.now = func() time.Time { return time.Unix(0, nowNs.Load()) }

	gw1, err := r.ForCtx(ctx, "alpha")
	if err != nil {
		t.Fatalf("ForCtx alpha: %v", err)
	}
	// Jump far past any plausible TTL.
	nowNs.Add(int64(24 * time.Hour))
	gw2, err := r.ForCtx(ctx, "alpha")
	if err != nil {
		t.Fatalf("ForCtx alpha later: %v", err)
	}
	if gw2 != gw1 {
		t.Fatal("shared-backend gateway rebuilt: permanent cache behavior changed")
	}
	if got := factory.buildCount("alpha"); got != 1 {
		t.Fatalf("shared path should build once, got %d builds", got)
	}
}

// TestTenantRouter_WithBindingProvider_DefaultsCacheTTL confirms WithBindingProvider
// turns on a non-zero per-tenant cache TTL by default (so rotation is bounded
// without the caller having to remember WithPerTenantCacheTTL).
func TestTenantRouter_WithBindingProvider_DefaultsCacheTTL(t *testing.T) {
	resolver := tenantbind.NewMapResolver()
	secrets := tenantbind.NewMapSecretStore()
	provider := tenantbind.NewStaticKeysProvider(resolver, secrets)
	r := NewTenantRouter(nil, nil, snapshot.NewMemoryDedupStore(), NewMemoryRefStore(),
		NewMemoryStagingStore(), 0, WithBindingProvider(provider, nil))
	if r.cacheTTL <= 0 {
		t.Fatalf("WithBindingProvider must set a positive per-tenant cache TTL, got %s", r.cacheTTL)
	}
}

// recordingProvider wraps a tenantbind.Provider, recording each tenant it is
// asked to bind, so a test can assert the per-tenant seam is consulted.
type recordingProvider struct {
	inner tenantbind.Provider
	mu    sync.Mutex
	seen  map[string]struct{}
}

func (p *recordingProvider) BindingFor(ctx context.Context, tenant string) (tenantbind.Binding, error) {
	p.mu.Lock()
	if p.seen == nil {
		p.seen = make(map[string]struct{})
	}
	p.seen[tenant] = struct{}{}
	p.mu.Unlock()
	return p.inner.BindingFor(ctx, tenant)
}

func (p *recordingProvider) tenants() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.seen))
	for k := range p.seen {
		out = append(out, k)
	}
	return out
}
