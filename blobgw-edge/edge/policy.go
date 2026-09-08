// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

// IdentityProvider asserts the authenticated (tenant, subject) for a mint
// request. In production this reads the shared auth-proxy's asserted-identity
// headers (the same proxy memorylayer/Aether use); HeaderIdentityProvider is
// the dev/test implementation.
type IdentityProvider interface {
	Identify(r *http.Request) (tenant, subject string, err error)
}

// ErrUnauthenticated is returned by an IdentityProvider when the request
// carries no usable asserted identity.
var ErrUnauthenticated = errors.New("edge: unauthenticated")

// HeaderIdentityProvider trusts the auth proxy's asserted-identity headers. The
// defaults are the CANONICAL auth-go headers the Envoy ext_authz headersToBackend
// allowlist delivers to the upstream: X-Auth-Tenant-ID (tenant) and
// X-Scitrera-User (subject/email). NEVER expose this to a caller that can reach
// the edge around the proxy — a client that can set these headers defeats
// auth-binding (the proxy must strip-and-restamp them; see the design doc §4.2).
// The header names are overridable for dev/tests.
type HeaderIdentityProvider struct {
	TenantHeader  string // default "X-Auth-Tenant-ID"
	SubjectHeader string // default "X-Scitrera-User"
}

func (h HeaderIdentityProvider) Identify(r *http.Request) (string, string, error) {
	th, sh := h.TenantHeader, h.SubjectHeader
	if th == "" {
		th = "X-Auth-Tenant-ID"
	}
	if sh == "" {
		sh = "X-Scitrera-User"
	}
	tenant := r.Header.Get(th)
	subject := r.Header.Get(sh)
	if tenant == "" || subject == "" {
		return "", "", ErrUnauthenticated
	}
	return tenant, subject, nil
}

// ErrQuota is returned when an operation would exceed a tenant's quota.
var ErrQuota = errors.New("edge: tenant quota exceeded")

// QuotaStore enforces per-tenant storage and object-count limits.
type QuotaStore interface {
	// Authorize reports whether adding one object of up to addBytes would keep
	// the tenant within quota. Returns ErrQuota otherwise.
	Authorize(ctx context.Context, tenant string, addBytes int64) error
	// RecordPut accounts a stored object of the given size.
	RecordPut(ctx context.Context, tenant string, bytes int64)
	// RecordDelete reverses the accounting for a removed object.
	RecordDelete(ctx context.Context, tenant string, bytes int64)
}

// MemoryQuotaStore is an in-memory QuotaStore with one shared limit applied to
// every tenant. Production would back this with per-tenant rows.
type MemoryQuotaStore struct {
	MaxBytes   int64 // 0 = unlimited
	MaxObjects int64 // 0 = unlimited

	mu    sync.Mutex
	bytes map[string]int64
	count map[string]int64
}

func NewMemoryQuotaStore(maxBytes, maxObjects int64) *MemoryQuotaStore {
	return &MemoryQuotaStore{
		MaxBytes: maxBytes, MaxObjects: maxObjects,
		bytes: make(map[string]int64), count: make(map[string]int64),
	}
}

// TenantLister enumerates the tenants that currently have data, so the
// leader-elected GC runner can sweep exactly those tenants (the edge has no
// static tenant list — tenants arrive dynamically via capabilities). The quota
// store is the natural source: a tenant appears here as soon as it stores its
// first object and until its accounting is released. Tenants is called on every
// GC pass, so a tenant added at runtime is picked up on the next sweep.
type TenantLister interface {
	// Tenants returns the current set of tenants with accounted data. It honors
	// ctx and returns an error only on a backend failure (the memory impl never
	// errors); the runner logs an error and skips the pass rather than sweeping a
	// stale/empty set.
	Tenants(ctx context.Context) ([]string, error)
}

// Tenants reports every tenant with non-zero accounting. It satisfies
// TenantLister so the GC runner can enumerate tenants-with-data from the
// in-memory quota store (dev/tests). A tenant whose bytes and object count have
// both been released back to zero is omitted, so GC never sweeps a tenant that
// no longer has data.
func (q *MemoryQuotaStore) Tenants(_ context.Context) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	seen := make(map[string]struct{}, len(q.bytes)+len(q.count))
	for t, b := range q.bytes {
		if b > 0 {
			seen[t] = struct{}{}
		}
	}
	for t, c := range q.count {
		if c > 0 {
			seen[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	return out, nil
}

var _ TenantLister = (*MemoryQuotaStore)(nil)

func (q *MemoryQuotaStore) Authorize(_ context.Context, tenant string, addBytes int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.MaxObjects > 0 && q.count[tenant]+1 > q.MaxObjects {
		return ErrQuota
	}
	if q.MaxBytes > 0 && q.bytes[tenant]+addBytes > q.MaxBytes {
		return ErrQuota
	}
	return nil
}

func (q *MemoryQuotaStore) RecordPut(_ context.Context, tenant string, bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.bytes[tenant] += bytes
	q.count[tenant]++
}

func (q *MemoryQuotaStore) RecordDelete(_ context.Context, tenant string, bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.bytes[tenant] -= bytes
	if q.bytes[tenant] < 0 {
		q.bytes[tenant] = 0
	}
	if q.count[tenant] > 0 {
		q.count[tenant]--
	}
}

// RateLimiter gates requests by an opaque key (typically "tenant" or
// "tenant:subject"). Allow returns false when the key is over its rate. ctx
// carries the request deadline/cancellation so a persistent backend (PG) honors
// it instead of running detached; the in-memory impl ignores it. A backend
// outage MUST fail OPEN (return true) so a limiter fault never wedges the data
// path.
type RateLimiter interface {
	Allow(ctx context.Context, key string) bool
}

// TokenBucketLimiter is a per-key token-bucket RateLimiter. rate tokens are
// added per second up to burst capacity.
type TokenBucketLimiter struct {
	rate  float64
	burst float64
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewTokenBucketLimiter(ratePerSec, burst float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{rate: ratePerSec, burst: burst, now: time.Now, buckets: make(map[string]*bucket)}
}

func (l *TokenBucketLimiter) Allow(_ context.Context, key string) bool {
	if l.rate <= 0 {
		return true // disabled
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &bucket{tokens: l.burst - 1, last: now}
		return true
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(l.burst, b.tokens+elapsed*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Revocation is an optional short-lived jti denylist for revoking high-value
// capabilities before they expire. The default reliance is short TTLs + key
// rotation; this closes the gap when a specific token must die early.
//
// ctx carries the request deadline/cancellation so a persistent backend (PG)
// honors it. IsRevoked MUST fail CLOSED (return true on a backend fault) so an
// outage can never silently honor a revoked token. Revoke takes the token's own
// expiry so the denylist entry can be dropped exactly when the token would have
// expired anyway (no point honoring a revocation past the token's natural death):
// a zero expiry falls back to a fixed retention TTL.
type Revocation interface {
	IsRevoked(ctx context.Context, jti string) bool
	Revoke(ctx context.Context, jti string, expiry time.Time)
}

// MemoryRevocation is an in-memory jti denylist. Each entry records the token's
// expiry so IsRevoked stops honoring it once the token would have expired anyway,
// matching the Postgres impl's semantics.
type MemoryRevocation struct {
	now func() time.Time

	mu  sync.RWMutex
	set map[string]time.Time // jti -> expiry (zero = never auto-expires)
}

func NewMemoryRevocation() *MemoryRevocation {
	return &MemoryRevocation{now: time.Now, set: make(map[string]time.Time)}
}

// SetClock overrides the clock used for expiry (tests).
func (m *MemoryRevocation) SetClock(now func() time.Time) { m.now = now }

func (m *MemoryRevocation) IsRevoked(_ context.Context, jti string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	expiry, ok := m.set[jti]
	if !ok {
		return false
	}
	// A zero expiry never auto-expires (denylisted until process exit); otherwise
	// the entry is honored only until the token's own expiry.
	if !expiry.IsZero() && !m.now().Before(expiry) {
		return false
	}
	return true
}

func (m *MemoryRevocation) Revoke(_ context.Context, jti string, expiry time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.set[jti] = expiry
}

// AuditEvent is one auditable edge action.
type AuditEvent struct {
	Kind    string // "mint" | "access"
	Tenant  string
	Subject string
	// LiveSubject is the LIVE asserted principal (from the auth proxy's identity
	// headers) that presented an auth-bound capability on the data path, distinct
	// from Subject (the minter). Empty on the shareable path (no live subject) and
	// on mint events. Lets audit distinguish "used by its bound owner" from
	// anonymous shareable access (§3.4).
	LiveSubject string
	Op          string
	Ref         string
	JTI         string
	Allowed     bool
	Reason      string // why denied, if not allowed
	SizeBytes   int64
}

// AuditSink receives every mint and data-path access for audit/analytics.
type AuditSink interface {
	Record(ctx context.Context, ev AuditEvent)
}
