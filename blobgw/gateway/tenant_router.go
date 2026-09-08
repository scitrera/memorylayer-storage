// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// defaultPerTenantCacheTTL is the default lifetime of a per-tenant gateway cache
// entry on the WithBindingProvider path. On expiry the per-tenant chain (gateway
// + chunked store + casstore S3 clients) is rebuilt, which re-resolves the
// binding through the provider and so picks up rotated credentials within one
// TTL (ADR §2.9: "the TTL doubles as the secret-rotation pickup"). It defaults to
// align with the typical provider credential TTL; override via
// WithPerTenantCacheTTL.
const defaultPerTenantCacheTTL = 15 * time.Minute

// TenantRouter hands out a per-tenant Gateway, each with its dedup domain set
// to the tenant. It supports two backend postures (ADR-001 §2.6):
//
//   - SHARED backend (default, NewTenantRouter / NewLocalTenantRouter): all
//     tenants share one manifest store + chunk store; isolation comes entirely
//     from the per-tenant dedup domain. Identical content within a tenant dedups
//     to one physical copy, while distinct tenants never share chunks or index
//     entries (the DedupStore is domain-keyed and blob IDs are domain-prefixed)
//     — closing the cross-tenant existence oracle at the physical layer, not just
//     the access layer.
//   - PER-TENANT backend (WithBindingProvider, ADR §2.6 "Per-tenant bucket
//     binding"): each tenant's manifest + chunk stores are bound to ITS OWN S3
//     bucket via a tenantbind.Provider. Cross-tenant dedup is lost (a
//     confidentiality win); the dedup index and ref/staging stores stay SHARED
//     and PG-backed, isolated by DedupDomain=tenant.
//
// Which posture is active is decided by the BackendFactory the router holds: a
// sharedBackend for the shared path, a perTenantBackend for the per-tenant path.
// The dedup index, ref index, and staging store are always shared.
//
// Compression is applied per-Put via CompressionPolicy, defaulting to
// NewContentTypePolicy(CompressZstd) (matching NewLocalStack / cmd/blobgw).
// Pass WithCompressionPolicy(nil) or WithCompressionPolicy(noPolicy) to opt out.
type TenantRouter struct {
	backend    BackendFactory
	dedup      snapshot.DedupStore
	refs       RefStore
	stage      StagingStore
	packTarget int
	policy     snapshot.CompressionPolicy
	// packCompression is the framing applied to packs the policy compresses.
	// The zero value is per-chunk framing (range-readable compressed packs);
	// WithPackCompression selects the whole-pack rollback.
	packCompression snapshot.PackCompressionMode

	// cacheTTL bounds the lifetime of a per-tenant gateway cache entry. A zero
	// value means entries never expire (the SHARED-backend posture: there are no
	// per-tenant credentials to rotate, so the permanent cache is correct and
	// unchanged). A positive value (set on the WithBindingProvider path) makes
	// the whole per-tenant chain rebuild on expiry so rotated credentials are
	// picked up within ≤ TTL (ADR §2.9). now is an injectable clock for tests.
	cacheTTL time.Duration
	now      func() time.Time

	// bindingProvider/bindingDSNs stash the per-tenant binding posture from
	// WithBindingProvider so the perTenantBackend can be built AFTER all options
	// run (in NewTenantRouter), once cacheTTL is final. This makes the result
	// independent of option order: the backend always receives the same effective
	// per-tenant cacheTTL the router uses (so its built S3 clients are rebuilt with
	// fresh credentials before the STS session cliff — the GC-sweep expiry fix). nil
	// bindingProvider means the shared-backend posture (no per-tenant backend).
	bindingProvider tenantbind.Provider
	bindingDSNs     map[string]string

	// backingMeter, when non-nil, wraps each per-tenant S3 chunk store the
	// perTenantBackend builds with the casstore backing-store RED adapter
	// (obs.Wrap), tagged backend=s3 + tenant. nil leaves the chunk store
	// un-instrumented (the shared-backend posture wraps its single chunk store at
	// the daemon instead). Set via WithBackingMeter.
	backingMeter metric.Meter

	mu    sync.Mutex
	cache map[string]gatewayEntry
}

// gatewayEntry is a cached per-tenant Gateway plus its expiry. A zero expiresAt
// means the entry never expires (shared-backend posture).
type gatewayEntry struct {
	gw        *Gateway
	expiresAt time.Time
}

// RouterOption is a functional option for NewTenantRouter.
type RouterOption func(*TenantRouter)

// WithCompressionPolicy sets the CompressionPolicy applied to each per-tenant
// ChunkedStore. Pass nil to disable compression entirely (uncompressed storage,
// matching pre-fix behavior). The default when no option is provided is
// snapshot.NewContentTypePolicy(snapshot.CompressZstd), matching NewLocalStack.
func WithCompressionPolicy(p snapshot.CompressionPolicy) RouterOption {
	return func(r *TenantRouter) { r.policy = p }
}

// WithPackCompression selects the framing used for each per-tenant gateway's
// COMPRESSED packs: per-chunk (default, keeps them range-readable) or whole-pack
// (the rollback). Packs the policy stores uncompressed are unaffected.
func WithPackCompression(m snapshot.PackCompressionMode) RouterOption {
	return func(r *TenantRouter) { r.packCompression = m }
}

// WithBindingProvider switches the router to the PER-TENANT backend binding of
// ADR-001 §2.6: instead of the shared manifest + chunk stores passed to
// NewTenantRouter, each tenant gets casstore S3 stores bound to ITS OWN bucket,
// resolved (and cached) per tenant through provider. The shared upstream/chunks
// passed to NewTenantRouter are IGNORED when this option is set (pass nil for
// them); the dedup index and ref/staging stores remain shared, with isolation
// preserved by DedupDomain=tenant.
//
// Use a tenantbind.CachingProvider so credentials are fetched on demand with a
// short TTL (ADR §2.9). When this option is set, GC() does not apply (the shared
// backend it sweeps is absent) — use GCForTenant for the per-tenant path.
//
// Credential rotation (ADR §2.9): the casstore S3 clients embed a point-in-time
// access/secret key (snapshot.NewS3Store / blobstore.NewS3Storage bind the static
// keys at construction), so once a tenant's gateway is cached its S3 clients keep
// using those keys regardless of a later provider rotation. To bound that, this
// option also gives the per-tenant gateway cache a TTL (defaultPerTenantCacheTTL,
// override with WithPerTenantCacheTTL): on expiry the whole per-tenant chain
// (gateway + chunked store + S3 clients) is rebuilt, re-resolving the binding and
// picking up the rotated credential within ≤ TTL. The residual lag (a presigned
// URL or in-flight request minted just before expiry may use the old key until
// the next rebuild, ≤ TTL) is accepted per ADR §2.9. The SHARED-backend path has
// no per-tenant credentials, so it keeps its permanent cache (cacheTTL=0).
// manifestDSNs (may be nil) routes listed tenants' manifests to PostgreSQL — the
// converged mlfs -remote posture where slice_manifest lives in the tenant's meta
// DB, not S3 — so the per-tenant GC reads the live set from PG; tenants absent
// from the map keep the S3 manifest store.
func WithBindingProvider(p tenantbind.Provider, manifestDSNs map[string]string) RouterOption {
	return func(r *TenantRouter) {
		// Stash the binding posture; the perTenantBackend is constructed in
		// NewTenantRouter AFTER all options run, so it receives the final cacheTTL
		// regardless of option order (e.g. WithPerTenantCacheTTL set before OR after
		// this option). The backend's cacheTTL must match the router's so its built
		// S3 clients are rebuilt with fresh credentials before the STS session cliff.
		r.bindingProvider = p
		r.bindingDSNs = manifestDSNs
		if r.cacheTTL == 0 {
			r.cacheTTL = defaultPerTenantCacheTTL
		}
	}
}

// Close releases per-tenant backend resources (e.g. PG manifest DB pools). Safe to
// call once at shutdown; a no-op for the shared backend.
func (r *TenantRouter) Close() error {
	if c, ok := r.backend.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// WithPerTenantCacheTTL overrides the per-tenant gateway cache TTL used on the
// WithBindingProvider path (default defaultPerTenantCacheTTL). Align it with the
// provider credential TTL: it bounds how long a rotated credential takes to
// propagate to the cached S3 clients (ADR §2.9). A non-positive value is ignored
// (the default stands). This has no effect on the SHARED-backend posture, whose
// cache is permanent.
func WithPerTenantCacheTTL(d time.Duration) RouterOption {
	return func(r *TenantRouter) {
		if d > 0 {
			r.cacheTTL = d
		}
	}
}

// WithBackingMeter instruments each per-tenant S3 chunk store the router builds
// (on the WithBindingProvider path) with the casstore backing-store RED adapter,
// recording casstore.backing.op.duration / errors / bytes tagged backend=s3 +
// tenant. Pass the daemon's shared OTEL meter (metrics.Registry.Meter(), which is
// a no-op when metrics are disabled). It has no effect on the shared-backend
// posture, whose single chunk store is wrapped by the daemon at construction. A
// nil meter is ignored.
func WithBackingMeter(m metric.Meter) RouterOption {
	return func(r *TenantRouter) { r.backingMeter = m }
}

// withBackendFactory injects an arbitrary BackendFactory, overriding the
// shared/per-tenant default. It is unexported on purpose: production callers
// pick a posture via NewTenantRouter (shared) or WithBindingProvider
// (per-tenant), while tests use it to inject a fake factory that returns
// per-tenant LOCAL stores without requiring real S3.
func withBackendFactory(f BackendFactory) RouterOption {
	return func(r *TenantRouter) { r.backend = f }
}

// NewTenantRouter builds a router over shared backends. packTarget is the
// casstore pack target in bytes (0 = default). Optional RouterOptions allow
// overriding the default compression policy, or switching to the per-tenant
// backend binding via WithBindingProvider (ADR-001 §2.6), in which case the
// upstream/chunks arguments are ignored and may be nil.
//
// Callers that do not pass any options receive the same zstd content-type
// compression policy as NewLocalStack/cmd/blobgw, so multi-tenant storage is
// now compressed by default. In the default (shared-backend) posture the
// upstream and chunks stores are wrapped in a sharedBackend, preserving the
// pre-§2.6 behavior where isolation comes entirely from the dedup domain.
func NewTenantRouter(upstream snapshot.SnapshotStore, chunks blobstore.Storage,
	dedup snapshot.DedupStore, refs RefStore, stage StagingStore, packTarget int, opts ...RouterOption) *TenantRouter {
	r := &TenantRouter{
		backend:    &sharedBackend{upstream: upstream, chunks: chunks},
		dedup:      dedup,
		refs:       refs,
		stage:      stage,
		packTarget: packTarget,
		policy:     snapshot.NewContentTypePolicy(snapshot.CompressZstd),
		now:        time.Now,
		cache:      make(map[string]gatewayEntry),
	}
	for _, o := range opts {
		o(r)
	}
	// Finalize the per-tenant backend AFTER all options have run, so it is built
	// with the FINAL cacheTTL (WithBindingProvider + WithPerTenantCacheTTL may be
	// supplied in either order). The backend's store cache must expire on the same
	// TTL as the router's gateway cache so the built S3 clients are rebuilt with
	// freshly-resolved credentials before the STS session cliff (the GC-sweep
	// "token has expired" fix). withBackendFactory (test-only) injects its own
	// backend and leaves bindingProvider nil, so it is unaffected.
	if r.bindingProvider != nil {
		r.backend = newPerTenantBackend(r.bindingProvider, r.bindingDSNs, r.cacheTTL, r.backingMeter)
	}
	return r
}

// For returns the Gateway for tenant. It is a thin back-compat wrapper over
// ForCtx with a background context; callers that have a request context should
// prefer ForCtx so cancellation and deadlines reach the backend build path.
func (r *TenantRouter) For(tenant string) (*Gateway, error) {
	return r.ForCtx(context.Background(), tenant)
}

// ForCtx returns the Gateway for tenant, constructing (and caching) it on first
// use, threading ctx into the backend build (binding resolution + S3 client
// construction). The returned gateway's dedup domain and ref namespace are the
// tenant, so a gateway for tenant A can never read or dedup against tenant B's
// data.
//
// The manifest + chunk stores backing the gateway come from the router's
// BackendFactory: the same shared stores for every tenant in the default
// posture, or this tenant's own S3 bucket under WithBindingProvider (ADR §2.6).
// The dedup index, ref index, and staging store are always shared; isolation is
// preserved by DedupDomain=tenant.
//
// Cache lifetime (ADR §2.9): in the SHARED-backend posture (cacheTTL==0) a cached
// gateway never expires — there are no per-tenant credentials to rotate. Under
// WithBindingProvider (cacheTTL>0) a cached gateway expires after the TTL, after
// which the whole per-tenant chain (gateway + chunked store + casstore S3
// clients) is rebuilt, re-resolving the binding so rotated credentials propagate
// within ≤ TTL.
func (r *TenantRouter) ForCtx(ctx context.Context, tenant string) (*Gateway, error) {
	if tenant == "" {
		return nil, fmt.Errorf("tenant router: empty tenant")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.cache[tenant]; ok && !r.expired(e) {
		return e.gw, nil
	}
	upstream, chunks, err := r.backend.For(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant router: resolve backend for %q: %w", tenant, err)
	}
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes:   r.packTarget,
		DedupDomain:       tenant,
		Index:             snapshot.NewGlobalIndex(r.dedup, nil),
		CompressionPolicy: r.policy,
		PackCompression:   r.packCompression,
	})
	if err != nil {
		return nil, fmt.Errorf("tenant router: build chunked store for %q: %w", tenant, err)
	}
	gw := New(cs, r.refs, r.stage, tenant)
	r.cache[tenant] = gatewayEntry{gw: gw, expiresAt: r.entryExpiry()}
	return gw, nil
}

// entryExpiry returns the expiry stamp for a new cache entry: the zero time
// (never expires) when cacheTTL is non-positive (shared posture), otherwise
// now+cacheTTL (per-tenant posture).
func (r *TenantRouter) entryExpiry() time.Time {
	if r.cacheTTL <= 0 {
		return time.Time{}
	}
	return r.now().Add(r.cacheTTL)
}

// expired reports whether a cache entry has passed its expiry. A zero expiresAt
// (shared posture) never expires.
func (r *TenantRouter) expired(e gatewayEntry) bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return !r.now().Before(e.expiresAt)
}

// GC returns a single index-aware ChunkedGC over the SHARED backends. RunOnce
// collects across all tenant domains; RunOnceForDomain(tenant) scopes to one.
//
// GC only applies to the SHARED-backend posture (NewTenantRouter without
// WithBindingProvider) — its existing single-return signature is preserved for
// the sibling callers (blobgw-s3, blobgw-edge) that wire it. Under the
// per-tenant backend binding (ADR §2.6) there is no single shared
// chunk/manifest store to sweep — GC must run per-tenant over that tenant's own
// bucket; use GCForTenant instead. In the per-tenant posture GC returns nil (the
// shared backend it would sweep is absent); callers on that path must use
// GCForTenant and not GC.
func (r *TenantRouter) GC() *snapshot.ChunkedGC {
	sb, ok := r.backend.(*sharedBackend)
	if !ok {
		// Per-tenant posture: no shared store to sweep. GCForTenant is the
		// supported path (ADR §2.6); a shared GC here would be meaningless.
		return nil
	}
	gc := snapshot.NewChunkedGC(sb.upstream, sb.chunks, nil)
	gc.Index = r.dedup
	return gc
}

// GCForTenant returns a ChunkedGC scoped to one tenant's backend, for the
// per-tenant bucket binding of ADR §2.6 (where GC must run per-tenant over that
// tenant's own S3 bucket, since there is no shared chunk/manifest store). It
// resolves the tenant's manifest + chunk stores through the BackendFactory and
// wires the SHARED dedup index, so domain isolation is preserved by
// DedupDomain=tenant. RunOnceForDomain(tenant) on the returned GC scopes the
// sweep to that tenant's domain.
//
// It also works in the shared-backend posture (returning a GC over the shared
// stores scoped via RunOnceForDomain), so callers that always want per-tenant
// GC need not branch on the posture.
func (r *TenantRouter) GCForTenant(ctx context.Context, tenant string) (*snapshot.ChunkedGC, error) {
	if tenant == "" {
		return nil, fmt.Errorf("tenant router: GCForTenant: empty tenant")
	}
	upstream, chunks, err := r.backend.For(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant router: GCForTenant: resolve backend for %q: %w", tenant, err)
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = r.dedup
	return gc, nil
}

// ChunksForTenant resolves the chunk (pack) store bound to tenant through the
// router's BackendFactory: the shared chunk store in the default posture, or
// this tenant's OWN S3 bucket under WithBindingProvider (ADR §2.6). It is the
// pack-store analogue of GCForTenant (same backend.For resolution, no manifest
// side) for callers that need only the chunk store — e.g. the pack-size
// backfill, which lists a tenant's pack blobs and records their sizes into the
// shared PG index. In the per-tenant posture the backfill MUST list the
// tenant's own backend (not the shared/placeholder store), or it records 0 for
// a real tenant.
func (r *TenantRouter) ChunksForTenant(ctx context.Context, tenant string) (blobstore.Storage, error) {
	if tenant == "" {
		return nil, fmt.Errorf("tenant router: ChunksForTenant: empty tenant")
	}
	_, chunks, err := r.backend.For(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant router: ChunksForTenant: resolve backend for %q: %w", tenant, err)
	}
	return chunks, nil
}

// CompactorForTenant yields a per-tenant ChunkedStore for pack compaction
// (repack) over that tenant's own backend + the shared dedup index. It mirrors
// the write-path store construction in ForCtx (same pack target, dedup domain,
// index, compression policy), so compaction reads/writes packs identically. It
// is built fresh per pass (not cached) — compaction runs infrequently on the GC
// leader — and works in both the shared and per-tenant backend postures.
func (r *TenantRouter) CompactorForTenant(ctx context.Context, tenant string) (*snapshot.ChunkedStore, error) {
	if tenant == "" {
		return nil, fmt.Errorf("tenant router: CompactorForTenant: empty tenant")
	}
	upstream, chunks, err := r.backend.For(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant router: CompactorForTenant: resolve backend for %q: %w", tenant, err)
	}
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes:   r.packTarget,
		DedupDomain:       tenant,
		Index:             snapshot.NewGlobalIndex(r.dedup, nil),
		CompressionPolicy: r.policy,
		PackCompression:   r.packCompression,
	})
	if err != nil {
		return nil, fmt.Errorf("tenant router: CompactorForTenant: build compactor for %q: %w", tenant, err)
	}
	return cs, nil
}

// LocalTenantRouter bundles a TenantRouter with the in-memory stores it was
// built from, for dev/test inspection (e.g. asserting physical blob counts).
type LocalTenantRouter struct {
	Router  *TenantRouter
	Dedup   *snapshot.MemoryDedupStore
	Refs    *MemoryRefStore
	Staging *MemoryStagingStore
	Chunks  blobstore.Storage
}

// NewLocalTenantRouter wires a TenantRouter over local-filesystem casstore
// backends + in-memory dedup/ref/staging, for the edge's dev mode and tests.
// Optional RouterOptions are forwarded to NewTenantRouter; when none are
// provided the router uses the default zstd content-type compression policy.
func NewLocalTenantRouter(dir string, packTarget int, opts ...RouterOption) (*LocalTenantRouter, error) {
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		return nil, fmt.Errorf("tenant router local: manifest store: %w", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		return nil, fmt.Errorf("tenant router local: chunk store: %w", err)
	}
	dedup := snapshot.NewMemoryDedupStore()
	refs := NewMemoryRefStore()
	staging := NewMemoryStagingStore()
	return &LocalTenantRouter{
		Router: NewTenantRouter(upstream, chunks, dedup, refs, staging, packTarget, opts...),
		Dedup:  dedup, Refs: refs, Staging: staging, Chunks: chunks,
	}, nil
}
