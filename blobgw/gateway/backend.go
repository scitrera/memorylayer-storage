// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for per-tenant manifest DBs

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/manifeststore"
)

// BackendFactory resolves the casstore backends a TenantRouter builds a
// ChunkedStore over, per tenant. It is the seam ADR-001 §2.6 ("Per-tenant
// bucket binding") turns on: the shared impl preserves today's
// "one shared backend, domain-prefix isolation" posture, while the per-tenant
// impl binds each tenant to ITS OWN S3 bucket via a tenantbind.Provider.
//
// Only the manifest (upstream) and chunk (pack object) stores are per-tenant;
// the dedup index and ref/staging stores stay shared and PG-backed, isolated by
// DedupDomain=tenant (ADR §2.6: "dedup index + refs stay SHARED and PG-backed").
type BackendFactory interface {
	// For returns the manifest store and chunk store for tenant. Implementations
	// must be safe for concurrent use.
	For(ctx context.Context, tenant string) (upstream snapshot.SnapshotStore, chunks blobstore.Storage, err error)
}

// sharedBackend returns the same injected manifest + chunk stores for every
// tenant, preserving the pre-ADR-§2.6 shared-backend posture where isolation
// comes entirely from the per-tenant dedup domain (not the physical bucket).
type sharedBackend struct {
	upstream snapshot.SnapshotStore
	chunks   blobstore.Storage
}

var _ BackendFactory = (*sharedBackend)(nil)

// For ignores tenant and returns the shared stores. The error is always nil;
// the signature matches BackendFactory so the shared and per-tenant paths are
// interchangeable in TenantRouter.
func (b *sharedBackend) For(_ context.Context, _ string) (snapshot.SnapshotStore, blobstore.Storage, error) {
	return b.upstream, b.chunks, nil
}

// perTenantBackend binds each tenant to its OWN S3 bucket (ADR §2.6) via a
// tenantbind.Provider. For(tenant) resolves the tenant's Binding, constructs the
// casstore S3 manifest store (snapshot.NewS3Store over ToSnapshotS3Config) and
// chunk store (blobstore.NewS3Storage over ToBlobstoreS3Config), and CACHES the
// constructed stores per tenant so S3 clients are not rebuilt per request.
//
// The provider is expected to be a tenantbind.CachingProvider (ADR §2.9
// on-demand fetch + short-TTL credential cache); perTenantBackend caches the
// constructed *stores* (the S3 clients), a separate concern from the credential
// cache.
//
// CREDENTIAL EXPIRY (the prod GC-sweep failure, "token has expired"): the casstore
// S3 clients embed point-in-time credentials at construction — for the AWS STS
// impl those are temporary access/secret/session-token creds that expire at the
// assume-role session cliff (~1h). The GC + compaction paths (TenantRouter
// GCForTenant / CompactorForTenant) build a FRESH ChunkedStore each pass but resolve
// the backend through THIS factory's For(); if For caches the stores with no expiry,
// the embedded STS creds go stale after ~1h and every later sweep fails with
// "The provided token has expired", even though the CachingProvider keeps refreshing
// the BINDING. So perTenantBackend's store cache MUST itself expire and rebuild the
// S3 clients (re-resolving fresh creds through the provider) before the STS session
// cliff. cacheTTL bounds entry lifetime; entries older than the TTL are treated as a
// miss and rebuilt. It mirrors TenantRouter's per-tenant cacheTTL semantics
// (defaultPerTenantCacheTTL, 15m < the ~1h STS session). A zero TTL means entries
// never expire (the shared-backend posture has no per-tenant credentials to refresh,
// so it never installs this factory; the zero default is purely a fail-safe).
type perTenantBackend struct {
	provider tenantbind.Provider
	// manifestDSNs maps tenant → its PostgreSQL DSN when manifests live in PG (the
	// converged mlfs -remote posture: slice_manifest is in the tenant's meta DB,
	// NOT S3). A tenant present here gets a PG manifest store as the GC upstream;
	// absent tenants fall back to the S3 manifest store. nil/empty = all-S3 (the
	// pre-existing behavior).
	manifestDSNs map[string]string

	// cacheTTL bounds how long a cached per-tenant store set (the built S3 clients
	// holding STS creds) is reused before it is rebuilt with freshly-resolved
	// credentials. Zero = never expire (fail-safe; production always sets it via
	// newPerTenantBackend). now is an injectable clock for tests.
	cacheTTL time.Duration
	now      func() time.Time

	// backingMeter, when non-nil, wraps each tenant's S3 chunk store with the
	// casstore backing-store RED adapter (obs.Wrap), tagged backend=s3 + tenant.
	// nil leaves the chunk store un-instrumented.
	backingMeter metric.Meter

	mu    sync.RWMutex
	cache map[string]tenantStores
	group singleflight.Group
}

type tenantStores struct {
	upstream   snapshot.SnapshotStore
	chunks     blobstore.Storage
	manifestDB *sql.DB // non-nil when upstream is the PG manifest store (closed on Close)
	// expiresAt is the cliff after which this cached entry is rebuilt (re-resolving
	// fresh credentials). A zero value never expires (cacheTTL==0 fail-safe).
	expiresAt time.Time
}

var _ BackendFactory = (*perTenantBackend)(nil)

// newPerTenantBackend builds a per-tenant backend factory over provider.
// manifestDSNs (may be nil) routes listed tenants' manifests to PostgreSQL.
// cacheTTL bounds how long built S3 clients are reused before being rebuilt with
// freshly-resolved credentials (must be < the STS session duration so GC/compaction
// never run on expired temporary creds — see the type doc). A non-positive cacheTTL
// disables expiry (fail-safe); production passes the router's per-tenant TTL.
func newPerTenantBackend(provider tenantbind.Provider, manifestDSNs map[string]string, cacheTTL time.Duration, backingMeter metric.Meter) *perTenantBackend {
	return &perTenantBackend{
		provider:     provider,
		manifestDSNs: manifestDSNs,
		cacheTTL:     cacheTTL,
		now:          time.Now,
		backingMeter: backingMeter,
		cache:        make(map[string]tenantStores),
	}
}

// For resolves the tenant's bucket binding and returns (constructing+caching on
// first use) the S3 manifest + chunk stores bound to that tenant's bucket.
//
// To avoid one tenant's first build blocking every other tenant (the global-lock
// starvation the CachingProvider already solves for credentials, ADR §2.9), the
// hot path takes only an RLock for the cache-hit case, and the construction path
// is collapsed per tenant via single-flight: concurrent first-use callers for
// the SAME tenant share one build, while different tenants build concurrently.
// The write-lock is held only to store the freshly-built stores.
func (b *perTenantBackend) For(ctx context.Context, tenant string) (snapshot.SnapshotStore, blobstore.Storage, error) {
	if tenant == "" {
		return nil, nil, fmt.Errorf("tenant router: per-tenant backend: empty tenant")
	}
	if ts, ok := b.lookup(tenant); ok {
		return ts.upstream, ts.chunks, nil
	}

	// Miss (or expired): build under single-flight so a tenant's S3 clients are
	// constructed exactly once even under concurrent first-use, without blocking
	// other tenants. On TTL expiry this rebuild re-resolves the binding through the
	// provider, so the freshly-built S3 clients carry refreshed (non-expired) STS
	// credentials.
	v, err, _ := b.group.Do(tenant, func() (any, error) {
		// Re-check inside the flight: a racing caller may have populated the
		// cache (with a still-live entry) between our lookup miss and entering the
		// flight.
		if ts, ok := b.lookup(tenant); ok {
			return ts, nil
		}
		binding, err := b.provider.BindingFor(ctx, tenant)
		if err != nil {
			return nil, fmt.Errorf("tenant router: bind backend for %q: %w", tenant, err)
		}
		// Manifest store: PostgreSQL when a DSN is configured for this tenant (the
		// converged mlfs -remote posture — manifests are in the tenant's meta DB),
		// otherwise the S3 manifest store (blobgw's own object-gateway tenants).
		// Built before the chunk store so a misconfigured/unreachable manifest DB
		// fails before we construct (and would have to discard) an S3 chunk client.
		var (
			upstream snapshot.SnapshotStore
			mdb      *sql.DB
		)
		// Manifest DSN source, per-record-first: the resolved binding's ManifestDSN
		// (folded into the tenant's resolver record, hot-reloadable) takes precedence,
		// falling back to the legacy startup manifestDSNs map so an existing
		// -manifest-dsn-file deployment still routes correctly during migration.
		dsn := binding.ManifestDSN
		if dsn == "" {
			dsn = b.manifestDSNs[tenant]
		}
		if dsn != "" {
			db, derr := sql.Open("pgx", dsn)
			if derr != nil {
				return nil, fmt.Errorf("tenant router: open manifest DB for %q: %w", tenant, derr)
			}
			if perr := db.PingContext(ctx); perr != nil {
				_ = db.Close()
				return nil, fmt.Errorf("tenant router: reach manifest DB for %q: %w", tenant, perr)
			}
			upstream, mdb = manifeststore.New(db, nil), db
		} else {
			upstream, err = snapshot.NewS3Store(ctx, binding.ToSnapshotS3Config())
			if err != nil {
				return nil, fmt.Errorf("tenant router: build manifest store for %q: %w", tenant, err)
			}
		}
		chunks, err := blobstore.NewS3Storage(ctx, binding.ToBlobstoreS3Config())
		if err != nil {
			if mdb != nil {
				_ = mdb.Close()
			}
			return nil, fmt.Errorf("tenant router: build chunk store for %q: %w", tenant, err)
		}
		// Backing-store RED: wrap the per-tenant S3 chunk store so casstore IO
		// records duration/errors/bytes tagged backend=s3 + tenant. Skipped when no
		// meter was wired (WithBackingMeter unset). A wrap failure is non-fatal —
		// fall back to the raw store rather than failing the bind.
		if b.backingMeter != nil {
			if wrapped, werr := obs.Wrap(chunks, b.backingMeter, "s3", attribute.String("tenant", tenant)); werr == nil {
				chunks = wrapped
			}
		}
		ts := tenantStores{upstream: upstream, chunks: chunks, manifestDB: mdb, expiresAt: b.entryExpiry()}
		b.store(tenant, ts)
		return ts, nil
	})
	if err != nil {
		return nil, nil, err
	}
	ts := v.(tenantStores)
	return ts.upstream, ts.chunks, nil
}

// lookup returns the cached stores for tenant under an RLock, treating an entry
// past its expiry as a miss so the caller rebuilds with freshly-resolved
// credentials (the STS-expiry fix).
func (b *perTenantBackend) lookup(tenant string) (tenantStores, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ts, ok := b.cache[tenant]
	if !ok || b.expired(ts) {
		return tenantStores{}, false
	}
	return ts, ok
}

// store records freshly-built stores under a write lock. If it replaces an
// existing entry that held a PG manifest DB pool (the rebuild-on-expiry path), the
// OLD pool is closed so a rebuild every TTL does not leak a connection pool per
// tenant. The close runs after the lock is released to keep the critical section
// short (Close only drains the pool; the entry is already unreachable).
func (b *perTenantBackend) store(tenant string, ts tenantStores) {
	b.mu.Lock()
	prev, hadPrev := b.cache[tenant]
	b.cache[tenant] = ts
	b.mu.Unlock()
	if hadPrev && prev.manifestDB != nil && prev.manifestDB != ts.manifestDB {
		_ = prev.manifestDB.Close()
	}
}

// entryExpiry returns the cliff stamp for a new cache entry: the zero time (never
// expires) when cacheTTL is non-positive (the fail-safe), otherwise now+cacheTTL.
func (b *perTenantBackend) entryExpiry() time.Time {
	if b.cacheTTL <= 0 {
		return time.Time{}
	}
	return b.now().Add(b.cacheTTL)
}

// expired reports whether a cached entry has passed its expiry. A zero expiresAt
// never expires (the cacheTTL==0 fail-safe).
func (b *perTenantBackend) expired(ts tenantStores) bool {
	if ts.expiresAt.IsZero() {
		return false
	}
	return !b.now().Before(ts.expiresAt)
}

// Close releases every cached per-tenant manifest DB pool. Safe to call once at
// shutdown; the cache is cleared so a later For() rebuilds.
func (b *perTenantBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ts := range b.cache {
		if ts.manifestDB != nil {
			_ = ts.manifestDB.Close()
		}
	}
	b.cache = make(map[string]tenantStores)
	return nil
}
