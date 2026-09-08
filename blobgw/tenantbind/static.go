// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"context"
	"fmt"
	"sync"
)

// StaticKeysProvider is the portable Provider impl (dev + non-AWS prod: R2
// bucket-scoped token, MinIO per-tenant user, GCS HMAC, dev static keys) of
// ADR §2.9. It composes a Resolver (tenant → Descriptor) with a SecretStore
// (CredentialRef → key pair): BindingFor resolves the Descriptor, fetches the
// per-tenant-scoped key via SecretStore.Get(descriptor.CredentialRef), and
// returns the Binding. The fetched key is itself scoped to one tenant, so the
// only gap vs the AWS STS impl is credential lifetime, not blast radius.
//
// StaticKeysProvider does no caching itself; wrap it in a CachingProvider for
// the ADR §2.9 on-demand-fetch + short-TTL lifecycle.
type StaticKeysProvider struct {
	resolver Resolver
	secrets  SecretStore
}

var _ Provider = (*StaticKeysProvider)(nil)

// NewStaticKeysProvider builds a StaticKeysProvider over the given resolver and
// secret store. Both are required.
func NewStaticKeysProvider(resolver Resolver, secrets SecretStore) *StaticKeysProvider {
	return &StaticKeysProvider{resolver: resolver, secrets: secrets}
}

// BindingFor resolves the tenant's Descriptor, fetches its scoped static
// credential, and assembles the Binding. The returned Creds have a zero Expiry
// (static keys are long-lived per ADR §2.9).
func (p *StaticKeysProvider) BindingFor(ctx context.Context, tenant string) (Binding, error) {
	if tenant == "" {
		return Binding{}, fmt.Errorf("tenantbind: empty tenant")
	}
	desc, err := p.resolver.Resolve(ctx, tenant)
	if err != nil {
		return Binding{}, fmt.Errorf("tenantbind: resolve %q: %w", tenant, err)
	}
	ak, sk, err := p.secrets.Get(ctx, desc.CredentialRef)
	if err != nil {
		return Binding{}, fmt.Errorf("tenantbind: fetch secret for %q (ref %q): %w", tenant, desc.CredentialRef, err)
	}
	return Binding{
		Endpoint:    desc.Endpoint,
		Region:      desc.Region,
		Bucket:      desc.Bucket,
		Prefix:      desc.Prefix,
		ManifestDSN: desc.ManifestDSN,
		Creds: Creds{
			AccessKeyID:     ak,
			SecretAccessKey: sk,
		},
	}, nil
}

// ErrTenantNotFound is returned by MapResolver when a tenant has no published
// Descriptor. Callers may errors.Is against it to distinguish a missing
// resolver record from a transport/store failure.
var ErrTenantNotFound = fmt.Errorf("tenantbind: tenant not found")

// ErrSecretNotFound is returned by MapSecretStore when a CredentialRef has no
// entry. Callers may errors.Is against it to distinguish a missing secret from
// a transport/store failure.
var ErrSecretNotFound = fmt.Errorf("tenantbind: secret not found")

// MapResolver is an in-memory Resolver for tests and dev, backed by a
// tenant → Descriptor map. It is safe for concurrent use.
type MapResolver struct {
	mu sync.RWMutex
	m  map[string]Descriptor
}

var _ Resolver = (*MapResolver)(nil)

// NewMapResolver builds an empty MapResolver.
func NewMapResolver() *MapResolver {
	return &MapResolver{m: make(map[string]Descriptor)}
}

// Set publishes (or replaces) the Descriptor for a tenant.
func (r *MapResolver) Set(tenant string, desc Descriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[tenant] = desc
}

// Resolve returns the tenant's Descriptor, or ErrTenantNotFound if none is
// published.
func (r *MapResolver) Resolve(_ context.Context, tenant string) (Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	desc, ok := r.m[tenant]
	if !ok {
		return Descriptor{}, fmt.Errorf("%w: %q", ErrTenantNotFound, tenant)
	}
	return desc, nil
}

// MapSecretStore is an in-memory SecretStore for tests and dev, backed by a
// ref → key-pair map. It is safe for concurrent use.
type MapSecretStore struct {
	mu sync.RWMutex
	m  map[string]secretPair
}

type secretPair struct {
	ak string
	sk string
}

var _ SecretStore = (*MapSecretStore)(nil)

// NewMapSecretStore builds an empty MapSecretStore.
func NewMapSecretStore() *MapSecretStore {
	return &MapSecretStore{m: make(map[string]secretPair)}
}

// Set stores (or replaces) the key pair for a ref.
func (s *MapSecretStore) Set(ref, accessKey, secretKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[ref] = secretPair{ak: accessKey, sk: secretKey}
}

// Get returns the key pair for ref, or ErrSecretNotFound if none is stored.
func (s *MapSecretStore) Get(_ context.Context, ref string) (accessKey, secretKey string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[ref]
	if !ok {
		return "", "", fmt.Errorf("%w: %q", ErrSecretNotFound, ref)
	}
	return p.ak, p.sk, nil
}
