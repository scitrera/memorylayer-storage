// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

// Credential is one resolvable S3 identity: an access key paired with its
// secret (used to recompute the SigV4 signature) and the dedup domain / tenant
// the key is authorized to act as. The domain is the casstore dedup namespace
// the gateway routes the request's bucket into.
type Credential struct {
	AccessKeyID string
	SecretKey   string
	// Domain is the tenant / dedup-domain this key acts within. Bucket→domain
	// resolution layers on top (see BucketResolver); for the simple single-tenant
	// Stage-1 deployment the key's Domain is the effective domain for every
	// bucket it can reach.
	Domain string
}

// CredentialStore resolves an access key id to its credential. Implementations
// must be safe for concurrent use. Production swaps the in-memory map for a
// Postgres- or secrets-manager-backed lookup; the interface keeps the auth path
// agnostic to that choice.
type CredentialStore interface {
	// Lookup returns the credential for accessKeyID, ok=false if unknown.
	Lookup(accessKeyID string) (Credential, bool)
}

// MemoryCredentialStore is an in-process access-key → credential map for dev,
// tests, and single-node deployments. It is read-only after construction so it
// needs no locking.
type MemoryCredentialStore struct {
	byID map[string]Credential
}

// NewMemoryCredentialStore builds a store from the given credentials, keyed by
// access key id. A later duplicate id overwrites an earlier one.
func NewMemoryCredentialStore(creds ...Credential) *MemoryCredentialStore {
	byID := make(map[string]Credential, len(creds))
	for _, c := range creds {
		byID[c.AccessKeyID] = c
	}
	return &MemoryCredentialStore{byID: byID}
}

// Lookup implements CredentialStore.
func (s *MemoryCredentialStore) Lookup(accessKeyID string) (Credential, bool) {
	c, ok := s.byID[accessKeyID]
	return c, ok
}

// BucketResolver maps an S3 bucket name to the casstore dedup domain that backs
// it. Stage 1 keeps this deliberately small: the default resolver maps every
// bucket to the authenticated credential's own domain, which is enough to prove
// dedup end-to-end while leaving room for a real bucket registry (bucket →
// tenant rows) in a later stage.
type BucketResolver interface {
	// Resolve returns the dedup domain for bucket given the authenticated
	// credential, ok=false if the credential may not address that bucket.
	Resolve(cred Credential, bucket string) (domain string, ok bool)
}

// credentialDomainResolver routes every bucket to the credential's own domain.
// It is the Stage-1 default: a single logical bucket space per tenant.
type credentialDomainResolver struct{}

func (credentialDomainResolver) Resolve(cred Credential, bucket string) (string, bool) {
	if bucket == "" || cred.Domain == "" {
		return "", false
	}
	return cred.Domain, true
}
