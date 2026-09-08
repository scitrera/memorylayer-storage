// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingProvider is a Provider that records how many times BindingFor is
// invoked per tenant and can block until released, for exercising the cache's
// on-demand-fetch and single-flight behavior. It optionally fails.
type countingProvider struct {
	mu     sync.Mutex
	calls  map[string]int
	total  atomic.Int64
	gate   chan struct{} // when non-nil, BindingFor blocks until closed/received
	err    error
	prefix string // distinguishes binding instances per fetch via Prefix
	seq    atomic.Int64
}

func newCountingProvider() *countingProvider {
	return &countingProvider{calls: make(map[string]int)}
}

func (c *countingProvider) BindingFor(ctx context.Context, tenant string) (Binding, error) {
	c.total.Add(1)
	c.mu.Lock()
	c.calls[tenant]++
	c.mu.Unlock()
	if c.gate != nil {
		<-c.gate
	}
	if c.err != nil {
		return Binding{}, c.err
	}
	n := c.seq.Add(1)
	return Binding{
		Bucket: "bucket-" + tenant,
		Prefix: c.prefix + itoa(n),
		Creds:  Creds{AccessKeyID: "ak-" + tenant, SecretAccessKey: "sk-" + tenant},
	}, nil
}

func (c *countingProvider) callCount(tenant string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[tenant]
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestCachingProviderOnDemandFetchAndHit(t *testing.T) {
	inner := newCountingProvider()
	cp, err := NewCachingProvider(inner, time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	ctx := context.Background()

	// Nothing fetched until first request (no load-all-at-startup).
	if got := inner.total.Load(); got != 0 {
		t.Fatalf("expected 0 fetches before first request, got %d", got)
	}

	b1, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor t1: %v", err)
	}
	if b1.Bucket != "bucket-t1" {
		t.Fatalf("unexpected binding: %+v", b1)
	}

	// Second call for the same tenant is served from cache (no new fetch).
	b2, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor t1 (cached): %v", err)
	}
	if inner.callCount("t1") != 1 {
		t.Fatalf("expected 1 fetch for t1, got %d", inner.callCount("t1"))
	}
	if b1 != b2 {
		t.Fatalf("cached binding differs: %+v vs %+v", b1, b2)
	}

	// A different tenant triggers its own fetch.
	if _, err := cp.BindingFor(ctx, "t2"); err != nil {
		t.Fatalf("BindingFor t2: %v", err)
	}
	if inner.callCount("t2") != 1 {
		t.Fatalf("expected 1 fetch for t2, got %d", inner.callCount("t2"))
	}
}

func TestCachingProviderTTLExpiryRefetch(t *testing.T) {
	inner := newCountingProvider()
	cp, err := NewCachingProvider(inner, time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	// Inject a controllable clock.
	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	cp.now = func() time.Time { return time.Unix(0, nowNs.Load()) }

	ctx := context.Background()
	b1, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}

	// Within TTL: cached, no refetch, identical binding.
	nowNs.Add(int64(30 * time.Second))
	b2, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor within ttl: %v", err)
	}
	if inner.callCount("t1") != 1 {
		t.Fatalf("expected 1 fetch within ttl, got %d", inner.callCount("t1"))
	}
	if b1 != b2 {
		t.Fatalf("binding changed within ttl: %+v vs %+v", b1, b2)
	}

	// Past TTL: entry expired → refetch (rotation pickup), new binding instance.
	nowNs.Add(int64(2 * time.Minute))
	b3, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor after ttl: %v", err)
	}
	if inner.callCount("t1") != 2 {
		t.Fatalf("expected 2 fetches after ttl expiry, got %d", inner.callCount("t1"))
	}
	if b3.Prefix == b1.Prefix {
		t.Fatalf("expected a fresh binding after refetch, got same prefix %q", b3.Prefix)
	}
}

// expiringProvider returns a Binding whose Creds carry a fixed (absolute)
// Expiry, with a per-fetch-distinct Prefix so refetches are observable. It
// exercises the cache's expiry-awareness.
type expiringProvider struct {
	expiry atomic.Int64 // unix nanos of the STS expiry to stamp on returned creds
	calls  atomic.Int64
}

func (p *expiringProvider) BindingFor(_ context.Context, tenant string) (Binding, error) {
	n := p.calls.Add(1)
	return Binding{
		Bucket: "bucket-" + tenant,
		Prefix: itoa(n),
		Creds: Creds{
			AccessKeyID:     "ak-" + tenant,
			SecretAccessKey: "sk-" + tenant,
			SessionToken:    "tok-" + tenant,
			Expiry:          time.Unix(0, p.expiry.Load()),
		},
	}, nil
}

func TestCachingProviderExpiryAwareRefresh(t *testing.T) {
	inner := &expiringProvider{}
	// TTL is long (1h) so the credential Expiry, not the TTL, drives eviction.
	cp, err := NewCachingProvider(inner, time.Hour)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	cp.now = func() time.Time { return time.Unix(0, nowNs.Load()) }
	ctx := context.Background()

	// STS session expires 10 minutes from t=0; safety margin is 1m, so the cache
	// entry's effective expiry is t=9m.
	stsExpiry := time.Unix(0, 0).Add(10 * time.Minute)
	inner.expiry.Store(stsExpiry.UnixNano())

	b1, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}
	if b1.Creds.SessionToken != "tok-t1" {
		t.Fatalf("unexpected session token: %q", b1.Creds.SessionToken)
	}

	// At t=8m (before Expiry-margin=9m): still cached, no refetch.
	nowNs.Store(time.Unix(0, 0).Add(8 * time.Minute).UnixNano())
	b2, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor at 8m: %v", err)
	}
	if inner.calls.Load() != 1 {
		t.Fatalf("expected 1 fetch before Expiry-margin, got %d", inner.calls.Load())
	}
	if b2.Prefix != b1.Prefix {
		t.Fatalf("binding changed before Expiry-margin: %q vs %q", b2.Prefix, b1.Prefix)
	}

	// At t=9m30s (past Expiry-margin=9m but well before raw Expiry=10m and TTL):
	// the entry has expired → refetch (STS session refreshed BEFORE it expires).
	nowNs.Store(time.Unix(0, 0).Add(9*time.Minute + 30*time.Second).UnixNano())
	// Advance the next session's expiry so the refreshed entry is live again.
	inner.expiry.Store(time.Unix(0, 0).Add(20 * time.Minute).UnixNano())
	b3, err := cp.BindingFor(ctx, "t1")
	if err != nil {
		t.Fatalf("BindingFor at 9m30s: %v", err)
	}
	if inner.calls.Load() != 2 {
		t.Fatalf("expected refetch past Expiry-margin, got %d fetches", inner.calls.Load())
	}
	if b3.Prefix == b1.Prefix {
		t.Fatalf("expected a fresh binding after STS-expiry refresh, got same prefix %q", b3.Prefix)
	}
}

func TestCachingProviderTTLWinsWhenShorterThanExpiry(t *testing.T) {
	inner := &expiringProvider{}
	// Short TTL (1m) with a far-future credential Expiry: the TTL must drive
	// eviction (rotation pickup), not the distant Expiry.
	cp, err := NewCachingProvider(inner, time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	cp.now = func() time.Time { return time.Unix(0, nowNs.Load()) }
	ctx := context.Background()

	inner.expiry.Store(time.Unix(0, 0).Add(time.Hour).UnixNano()) // expiry far away

	if _, err := cp.BindingFor(ctx, "t1"); err != nil {
		t.Fatalf("BindingFor: %v", err)
	}
	// At t=90s (past the 1m TTL, far before the 1h Expiry): TTL evicts → refetch.
	nowNs.Store(time.Unix(0, 0).Add(90 * time.Second).UnixNano())
	if _, err := cp.BindingFor(ctx, "t1"); err != nil {
		t.Fatalf("BindingFor at 90s: %v", err)
	}
	if inner.calls.Load() != 2 {
		t.Fatalf("expected TTL-driven refetch, got %d fetches", inner.calls.Load())
	}
}

func TestCachingProviderSingleFlight(t *testing.T) {
	inner := newCountingProvider()
	inner.gate = make(chan struct{})
	cp, err := NewCachingProvider(inner, time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	ctx := context.Background()

	const n = 16
	var wg sync.WaitGroup
	results := make([]Binding, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = cp.BindingFor(ctx, "t1")
		}(i)
	}
	close(start)

	// Give the goroutines time to enter the flight and block on the gate, then
	// release the single underlying fetch.
	time.Sleep(50 * time.Millisecond)
	close(inner.gate)
	wg.Wait()

	if got := inner.callCount("t1"); got != 1 {
		t.Fatalf("expected exactly 1 underlying fetch under single-flight, got %d", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d errored: %v", i, errs[i])
		}
		if results[i] != results[0] {
			t.Fatalf("goroutine %d got a different binding: %+v vs %+v", i, results[i], results[0])
		}
	}
}

func TestCachingProviderConstructorValidation(t *testing.T) {
	if _, err := NewCachingProvider(nil, time.Minute); err == nil {
		t.Fatal("expected error for nil inner provider")
	}
	inner := newCountingProvider()
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := NewCachingProvider(inner, ttl); err == nil {
			t.Fatalf("expected error for non-positive ttl %s", ttl)
		}
	}
}

func TestStaticKeysProvider(t *testing.T) {
	tests := []struct {
		name       string
		tenant     string
		descriptor *Descriptor // nil = not published
		secret     *secretPair // nil = not stored at the descriptor's ref
		wantErr    error       // sentinel to errors.Is against; nil = success
		wantAK     string
		wantBucket string
	}{
		{
			name:       "happy path",
			tenant:     "acme",
			descriptor: &Descriptor{Endpoint: "r2.example.com", Region: "auto", Bucket: "acme-bkt-x9", Prefix: "p", CredentialRef: "secret/acme"},
			secret:     &secretPair{ak: "AKIA-acme", sk: "shh-acme"},
			wantAK:     "AKIA-acme",
			wantBucket: "acme-bkt-x9",
		},
		{
			name:    "unknown tenant",
			tenant:  "ghost",
			wantErr: ErrTenantNotFound,
		},
		{
			name:       "missing secret",
			tenant:     "acme",
			descriptor: &Descriptor{Bucket: "acme-bkt", CredentialRef: "secret/missing"},
			wantErr:    ErrSecretNotFound,
		},
		{
			name:    "empty tenant",
			tenant:  "",
			wantErr: nil, // checked separately below: error but no sentinel
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := NewMapResolver()
			secrets := NewMapSecretStore()
			if tt.descriptor != nil {
				resolver.Set(tt.tenant, *tt.descriptor)
				if tt.secret != nil {
					secrets.Set(tt.descriptor.CredentialRef, tt.secret.ak, tt.secret.sk)
				}
			}
			p := NewStaticKeysProvider(resolver, secrets)

			b, err := p.BindingFor(context.Background(), tt.tenant)

			if tt.name == "empty tenant" {
				if err == nil {
					t.Fatal("expected error for empty tenant")
				}
				return
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if b.Creds.AccessKeyID != tt.wantAK {
				t.Errorf("AccessKeyID = %q, want %q", b.Creds.AccessKeyID, tt.wantAK)
			}
			if b.Bucket != tt.wantBucket {
				t.Errorf("Bucket = %q, want %q", b.Bucket, tt.wantBucket)
			}
			if b.Creds.SecretAccessKey != tt.secret.sk {
				t.Errorf("SecretAccessKey = %q, want %q", b.Creds.SecretAccessKey, tt.secret.sk)
			}
			if b.Endpoint != tt.descriptor.Endpoint {
				t.Errorf("Endpoint = %q, want %q", b.Endpoint, tt.descriptor.Endpoint)
			}
			if b.Region != tt.descriptor.Region {
				t.Errorf("Region = %q, want %q", b.Region, tt.descriptor.Region)
			}
			if b.Prefix != tt.descriptor.Prefix {
				t.Errorf("Prefix = %q, want %q", b.Prefix, tt.descriptor.Prefix)
			}
			if !b.Creds.Expiry.IsZero() {
				t.Errorf("expected zero Expiry for static keys, got %v", b.Creds.Expiry)
			}
		})
	}
}

// TestManifestDSNFlowsDescriptorToBinding asserts the optional per-tenant
// ManifestDSN (slice-manifest PG routing) is copied from the resolved Descriptor
// onto the Binding by the static-keys provider, and that an empty Descriptor DSN
// yields an empty Binding DSN (the S3/shared-manifest posture).
func TestManifestDSNFlowsDescriptorToBinding(t *testing.T) {
	resolver := NewMapResolver()
	resolver.Set("acme", Descriptor{Bucket: "acme-bkt", CredentialRef: "secret/acme", ManifestDSN: "postgres://acme"})
	resolver.Set("globex", Descriptor{Bucket: "globex-bkt", CredentialRef: "secret/globex"}) // no DSN
	secrets := NewMapSecretStore()
	secrets.Set("secret/acme", "AK", "SK")
	secrets.Set("secret/globex", "AK", "SK")
	p := NewStaticKeysProvider(resolver, secrets)
	ctx := context.Background()

	acme, err := p.BindingFor(ctx, "acme")
	if err != nil {
		t.Fatalf("BindingFor acme: %v", err)
	}
	if acme.ManifestDSN != "postgres://acme" {
		t.Fatalf("acme Binding.ManifestDSN = %q, want %q", acme.ManifestDSN, "postgres://acme")
	}

	globex, err := p.BindingFor(ctx, "globex")
	if err != nil {
		t.Fatalf("BindingFor globex: %v", err)
	}
	if globex.ManifestDSN != "" {
		t.Fatalf("globex Binding.ManifestDSN = %q, want empty (S3 manifest posture)", globex.ManifestDSN)
	}
}

func TestBindingToS3Configs(t *testing.T) {
	b := Binding{
		Endpoint: "minio.local:9000",
		Region:   "us-east-1",
		Bucket:   "tenant-bkt",
		Prefix:   "pfx",
		// SessionToken is set deliberately: casstore's S3Config types now carry a
		// SessionToken field (ADR §7.8), so the converters MUST thread the STS
		// session token through to both configs — the direct-I/O path for the AWS
		// AssumeRoleWithWebIdentity impl (ADR §2.9). This reverses the earlier
		// "intentionally dropped" assertion.
		Creds: Creds{AccessKeyID: "AK", SecretAccessKey: "SK", SessionToken: "tok"},
	}

	// The kopia/blobstore client wants a BARE HOST (endpoint normalization is
	// covered exhaustively in convert_test.go) AND a TRAILING-SLASH prefix so its
	// raw "prefix+blobID" key matches the presign objectKey "prefix/key" (else the
	// pack LIST returns 0 and compaction never runs). So Prefix "pfx" → "pfx/".
	blob := b.ToBlobstoreS3Config()
	if blob.Bucket != b.Bucket || blob.Prefix != b.Prefix+"/" || blob.Region != b.Region ||
		blob.Endpoint != b.Endpoint || blob.AccessKey != b.Creds.AccessKeyID || blob.SecretKey != b.Creds.SecretAccessKey {
		t.Fatalf("blobstore.S3Config mismatch: %+v from %+v", blob, b)
	}
	if blob.SessionToken != b.Creds.SessionToken {
		t.Fatalf("blobstore.S3Config.SessionToken = %q, want %q", blob.SessionToken, b.Creds.SessionToken)
	}

	// The aws-sdk/snapshot client wants a valid URI, so the bare-host binding is
	// normalized to https://minio.local:9000 (the BUG-1 fix). All other fields are
	// passed through unchanged.
	snap := b.ToSnapshotS3Config()
	if snap.Bucket != b.Bucket || snap.Prefix != b.Prefix || snap.Region != b.Region ||
		snap.Endpoint != "https://"+b.Endpoint || snap.AccessKey != b.Creds.AccessKeyID || snap.SecretKey != b.Creds.SecretAccessKey {
		t.Fatalf("snapshot.S3Config mismatch: %+v from %+v", snap, b)
	}
	if snap.SessionToken != b.Creds.SessionToken {
		t.Fatalf("snapshot.S3Config.SessionToken = %q, want %q", snap.SessionToken, b.Creds.SessionToken)
	}

	// A zero session token (the static-keys case) must thread through as empty,
	// preserving the pre-existing behavior.
	bNoTok := b
	bNoTok.Creds.SessionToken = ""
	if got := bNoTok.ToBlobstoreS3Config().SessionToken; got != "" {
		t.Fatalf("ToBlobstoreS3Config SessionToken = %q for static keys, want empty", got)
	}
	if got := bNoTok.ToSnapshotS3Config().SessionToken; got != "" {
		t.Fatalf("ToSnapshotS3Config SessionToken = %q for static keys, want empty", got)
	}
}

// TestStaticKeysThroughCache exercises the intended composition: the cache in
// front of the static-keys provider, with a real rotation through the secret
// store picked up after the TTL.
func TestStaticKeysThroughCache(t *testing.T) {
	resolver := NewMapResolver()
	resolver.Set("acme", Descriptor{Bucket: "acme-bkt", CredentialRef: "secret/acme"})
	secrets := NewMapSecretStore()
	secrets.Set("secret/acme", "AK1", "SK1")

	cp, err := NewCachingProvider(NewStaticKeysProvider(resolver, secrets), time.Minute)
	if err != nil {
		t.Fatalf("NewCachingProvider: %v", err)
	}
	var nowNs atomic.Int64
	nowNs.Store(time.Unix(0, 0).UnixNano())
	cp.now = func() time.Time { return time.Unix(0, nowNs.Load()) }
	ctx := context.Background()

	b, err := cp.BindingFor(ctx, "acme")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}
	if b.Creds.AccessKeyID != "AK1" {
		t.Fatalf("AccessKeyID = %q, want AK1", b.Creds.AccessKeyID)
	}

	// Rotate the secret in the store. Within TTL the cache still serves the old.
	secrets.Set("secret/acme", "AK2", "SK2")
	b, _ = cp.BindingFor(ctx, "acme")
	if b.Creds.AccessKeyID != "AK1" {
		t.Fatalf("within ttl AccessKeyID = %q, want cached AK1", b.Creds.AccessKeyID)
	}

	// After TTL the entry expires and the rotation is picked up — no separate
	// rotation path (ADR §2.9).
	nowNs.Add(int64(2 * time.Minute))
	b, _ = cp.BindingFor(ctx, "acme")
	if b.Creds.AccessKeyID != "AK2" {
		t.Fatalf("after ttl AccessKeyID = %q, want rotated AK2", b.Creds.AccessKeyID)
	}
}
