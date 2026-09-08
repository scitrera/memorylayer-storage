// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// credentialExpirySafetyMargin is subtracted from a Binding's Creds.Expiry when
// computing the cache entry's effective expiry, so an STS session is refreshed
// before it actually expires (ADR §2.9). It guards against serving a credential
// that expires mid-request (clock skew + request latency + propagation).
const credentialExpirySafetyMargin = time.Minute

// CachingProvider wraps any Provider with the ADR §2.9 credential lifecycle:
// per-tenant on-demand fetch + a short TTL cache. It NEVER loads all tenants at
// startup — an entry is fetched only when a tenant is first requested and is
// keyed by tenant. Each entry expires after the configured TTL, which doubles as
// the secret-rotation pickup (rotate in the store → entry expires → next fetch
// picks up the new credential; no separate rotation path). This bounds the
// credential blast radius to currently-active tenants.
//
// The cache is EXPIRY-AWARE: when a fetched Binding carries a non-zero
// Creds.Expiry (the AWS STS impl's session cliff), the entry expires at
// min(now+ttl, Creds.Expiry-safetyMargin), so STS sessions are refreshed before
// they expire rather than served stale. When Creds.Expiry is zero (static keys)
// the fixed TTL is used unchanged.
//
// CachingProvider is safe for concurrent use and uses single-flight so that
// concurrent BindingFor calls for the same tenant trigger exactly one underlying
// fetch.
type CachingProvider struct {
	inner Provider
	ttl   time.Duration
	now   func() time.Time // injectable clock for tests; defaults to time.Now

	mu      sync.RWMutex
	entries map[string]cacheEntry
	group   singleflight.Group
}

type cacheEntry struct {
	binding   Binding
	expiresAt time.Time
}

var _ Provider = (*CachingProvider)(nil)

// NewCachingProvider wraps inner with a per-tenant TTL cache. ttl must be
// positive; the secret-rotation pickup latency equals ttl (ADR §2.9).
func NewCachingProvider(inner Provider, ttl time.Duration) (*CachingProvider, error) {
	if inner == nil {
		return nil, fmt.Errorf("tenantbind: caching provider: nil inner provider")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("tenantbind: caching provider: ttl must be positive, got %s", ttl)
	}
	return &CachingProvider{
		inner:   inner,
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[string]cacheEntry),
	}, nil
}

// BindingFor returns the tenant's Binding, serving a live (unexpired) cached
// entry when present and otherwise fetching on demand through inner. Concurrent
// callers for the same tenant collapse to a single underlying fetch via
// single-flight.
func (c *CachingProvider) BindingFor(ctx context.Context, tenant string) (Binding, error) {
	if b, ok := c.lookup(tenant); ok {
		return b, nil
	}

	// Miss (or expired): fetch on demand, collapsing concurrent same-tenant
	// callers into one inner.BindingFor via single-flight.
	v, err, _ := c.group.Do(tenant, func() (any, error) {
		// Re-check the cache inside the flight: a racing caller may have
		// populated it between our lookup miss and acquiring the flight, so we
		// avoid a redundant fetch.
		if b, ok := c.lookup(tenant); ok {
			return b, nil
		}
		b, err := c.inner.BindingFor(ctx, tenant)
		if err != nil {
			return Binding{}, err
		}
		c.store(tenant, b)
		return b, nil
	})
	if err != nil {
		return Binding{}, err
	}
	return v.(Binding), nil
}

// lookup returns a cached binding when present and not expired.
func (c *CachingProvider) lookup(tenant string) (Binding, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[tenant]
	if !ok || !c.now().Before(e.expiresAt) {
		return Binding{}, false
	}
	return e.binding, true
}

// store records a freshly fetched binding with its effective expiry.
func (c *CachingProvider) store(tenant string, b Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[tenant] = cacheEntry{binding: b, expiresAt: c.effectiveExpiry(b)}
}

// effectiveExpiry computes when a freshly fetched binding's cache entry expires.
// The fixed-TTL deadline (now+ttl) doubles as the secret-rotation pickup. When
// the binding carries a non-zero credential Expiry (the AWS STS session cliff),
// the entry expires at the EARLIER of the TTL deadline and (Expiry - safety
// margin), so an STS session is refreshed before it expires rather than served
// stale/expired. A zero credential Expiry (static keys) keeps the fixed TTL.
func (c *CachingProvider) effectiveExpiry(b Binding) time.Time {
	ttlExpiry := c.now().Add(c.ttl)
	if b.Creds.Expiry.IsZero() {
		return ttlExpiry
	}
	credExpiry := b.Creds.Expiry.Add(-credentialExpirySafetyMargin)
	if credExpiry.Before(ttlExpiry) {
		return credExpiry
	}
	return ttlExpiry
}
