// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// TestPerTenantBackend_CacheExpiry is the BUG-2 (STS-credential-expiry) regression
// at the perTenantBackend layer: the built S3 clients embed point-in-time STS
// credentials, so the store cache MUST expire entries on its TTL and rebuild —
// otherwise GCForTenant/CompactorForTenant reuse a stale client past the ~1h STS
// session and every sweep fails with "The provided token has expired".
//
// perTenantBackend.For builds REAL S3 clients (a network probe), out of scope for a
// unit test, so this exercises the cache lifecycle (lookup/store/entryExpiry/expired)
// directly with an injected clock — the exact seam the fix added.
func TestPerTenantBackend_CacheExpiry(t *testing.T) {
	provider := tenantbind.NewStaticKeysProvider(tenantbind.NewMapResolver(), tenantbind.NewMapSecretStore())
	const ttl = 15 * time.Minute
	b := newPerTenantBackend(provider, nil, ttl, nil)

	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	b.now = func() time.Time { return time.Unix(0, nowNs.Load()) }

	// Seed a cached entry stamped with the current clock (as a real build would via
	// entryExpiry()).
	seed := tenantStores{expiresAt: b.entryExpiry()}
	b.store("alpha", seed)

	// Within TTL: live hit.
	nowNs.Add(int64(14 * time.Minute))
	if _, ok := b.lookup("alpha"); !ok {
		t.Fatal("entry should be a live cache hit within the TTL")
	}

	// Past TTL: treated as a miss so For() rebuilds (re-resolving fresh STS creds).
	nowNs.Add(int64(2 * time.Minute)) // now 16m > 15m TTL
	if _, ok := b.lookup("alpha"); ok {
		t.Fatal("expired entry must be a cache MISS so the S3 client is rebuilt with fresh credentials")
	}
}

// TestPerTenantBackend_NeverExpiresWhenTTLZero confirms the cacheTTL<=0 fail-safe:
// a zero TTL keeps entries permanent (no spurious rebuild). Production always passes
// a positive TTL, but the zero default must not break the cache.
func TestPerTenantBackend_NeverExpiresWhenTTLZero(t *testing.T) {
	provider := tenantbind.NewStaticKeysProvider(tenantbind.NewMapResolver(), tenantbind.NewMapSecretStore())
	b := newPerTenantBackend(provider, nil, 0, nil)

	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	b.now = func() time.Time { return time.Unix(0, nowNs.Load()) }

	b.store("alpha", tenantStores{expiresAt: b.entryExpiry()})
	nowNs.Add(int64(48 * time.Hour))
	if _, ok := b.lookup("alpha"); !ok {
		t.Fatal("with cacheTTL=0 the entry must never expire")
	}
}

// TestWithBindingProvider_PropagatesCacheTTLToBackend verifies the order-independent
// wiring: WithBindingProvider + WithPerTenantCacheTTL (in EITHER order) build the
// perTenantBackend with the router's final cacheTTL, so the backend store cache and
// the router gateway cache expire together — the credential refresh is bounded.
func TestWithBindingProvider_PropagatesCacheTTLToBackend(t *testing.T) {
	provider := tenantbind.NewStaticKeysProvider(tenantbind.NewMapResolver(), tenantbind.NewMapSecretStore())
	const custom = 3 * time.Minute

	for _, tc := range []struct {
		name string
		opts []RouterOption
		want time.Duration
	}{
		{"default ttl", []RouterOption{WithBindingProvider(provider, nil)}, defaultPerTenantCacheTTL},
		{"ttl before binding", []RouterOption{WithPerTenantCacheTTL(custom), WithBindingProvider(provider, nil)}, custom},
		{"ttl after binding", []RouterOption{WithBindingProvider(provider, nil), WithPerTenantCacheTTL(custom)}, custom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewTenantRouter(nil, nil, nil, nil, nil, 0, tc.opts...)
			ptb, ok := r.backend.(*perTenantBackend)
			if !ok {
				t.Fatalf("expected perTenantBackend, got %T", r.backend)
			}
			if ptb.cacheTTL != tc.want {
				t.Fatalf("backend cacheTTL = %s, want %s (must match router cacheTTL %s)", ptb.cacheTTL, tc.want, r.cacheTTL)
			}
			if ptb.cacheTTL != r.cacheTTL {
				t.Fatalf("backend cacheTTL %s != router cacheTTL %s: store + gateway caches would expire at different times", ptb.cacheTTL, r.cacheTTL)
			}
		})
	}
}
