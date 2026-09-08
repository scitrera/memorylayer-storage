// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package tenantbind implements blobgw's per-tenant credential + backend-binding
// seam, the BLOBGW-ONLY half of the CredentialProvider contract in
// ADR-001 §2.9 ("Per-tenant credential model").
//
// Only blobgw holds per-tenant bucket credentials; mlfs nodes stay
// credential-light and receive short-lived presigned URLs (ADR §2.2, §6). This
// package is therefore the single place credentials are resolved, so the
// credential blast radius stays at blobgw.
//
// The seam produces, per tenant, a Binding sufficient to both (a) construct a
// casstore S3 client for direct I/O and (b) hold the credential used to mint
// presigned URLs — the two needs ADR §2.9 unifies into one binding. It is the
// factory TenantRouter.For(tenant) consumes (ADR §2.6): the credential model
// and the per-tenant backend binding are one seam, not two.
//
// Structure, mirroring ADR §2.9:
//
//   - Resolver maps tenant → Descriptor (the static, provisioning-published
//     locator). The resolver is mandatory and non-derivable: the provisioned
//     bucket/role name carries an optional "-<nonce>", so blobgw cannot compute
//     it from the slug.
//
//   - SecretStore abstracts where per-tenant-scoped static credentials live
//     (Vault / K8s secret / file / dev).
//
//   - Provider is the seam TenantRouter calls: BindingFor(tenant) → Binding.
//
//   - StaticKeysProvider composes a Resolver + SecretStore — the portable impl
//     (dev + non-AWS prod: R2 / MinIO / GCS HMAC / dev keys). Each fetched key
//     is itself scoped to one tenant.
//
//   - CachingProvider wraps any Provider with per-tenant on-demand fetch + a
//     short TTL cache (ADR §2.9 lifecycle: never load-all-at-startup; TTL
//     doubles as rotation pickup; bounds blast radius to currently-active
//     tenants).
//
//   - AssumeRoleProvider is the AWS AssumeRoleWithWebIdentity impl (ADR §2.9
//     "Option 2"): for tenant T it interprets Descriptor.CredentialRef as the
//     IAM role ARN and uses the pod's IRSA web-identity token to STS-assume that
//     role, returning a Binding whose Creds carry the temporary access/secret
//     key, a SessionToken, and the STS Expiry. It slots in as just another
//     Provider — the interfaces keep it purely additive, so no AWS-specific
//     ergonomics leak into call sites. The STS call is abstracted behind the
//     WebIdentityAssumer interface so it is injectable in tests.
package tenantbind

import (
	"context"
	"time"
)

// Creds is a single set of S3 credentials. A zero Expiry means the credential
// is long-lived / static (the static-keys impl); a non-zero Expiry is the cliff
// a session-credential impl (the future AWS STS impl) must refresh before.
type Creds struct {
	AccessKeyID     string
	SecretAccessKey string
	// SessionToken is the STS session token for temporary credentials (the AWS
	// AssumeRoleWithWebIdentity impl, ADR §2.9). It reaches BOTH the PRESIGN path
	// (controlplane.newPresignClient threads it into the signer) and the
	// direct-I/O path: casstore's S3Config (blobstore.S3Config / s3util.Config)
	// now carries a SessionToken field (ADR §7.8), so ToBlobstoreS3Config /
	// ToSnapshotS3Config map it through (see convert.go). A zero token is the
	// static-keys case and threads through as empty (unchanged behavior).
	SessionToken string
	Expiry       time.Time
}

// Binding is the per-tenant-scoped backend binding the seam produces: enough to
// construct a casstore S3 client for direct I/O and to hold the credential used
// to mint presigned URLs (ADR §2.9). Use ToBlobstoreS3Config / ToSnapshotS3Config
// to feed it into casstore.
type Binding struct {
	Endpoint string
	Region   string
	Bucket   string
	Prefix   string
	Creds    Creds
	// ManifestDSN is the optional per-tenant PostgreSQL DSN for slice-manifest
	// routing: when non-empty this tenant's manifests live in its own meta DB (the
	// converged mlfs -remote posture) and the per-tenant backend builds a PG
	// manifest store instead of the S3 one. Empty is the S3/shared-manifest posture
	// (blobgw's own object-gateway tenants). It is folded INTO the binding — rather
	// than a separate startup map — so onboarding one tenant is one resolver record,
	// and it is picked up by the hot-reloading resolver like every other field.
	ManifestDSN string
}

// Descriptor is the static, provisioning-published locator for a tenant — the
// record a Resolver returns. CredentialRef is an opaque, impl-interpreted
// reference: a secret path for the static-keys impl, a role ARN for the future
// AWS impl. It models the ADR §2.9 requirement that the tenant→binding resolver
// is mandatory (not derivable) because the bucket/role name carries a nonce.
type Descriptor struct {
	Endpoint      string
	Region        string
	Bucket        string
	Prefix        string
	CredentialRef string
	// ManifestDSN is the optional per-tenant PostgreSQL DSN for slice-manifest
	// routing (the converged mlfs -remote posture: this tenant's manifests live in
	// its own meta DB, not S3). It is copied into the resolved Binding. Empty is the
	// S3/shared-manifest posture. Provisioning publishes it per tenant so a tenant's
	// manifest destination onboards with the rest of its resolver record — no
	// separate startup DSN map.
	ManifestDSN string
}

// Resolver maps a tenant to its Descriptor. The mapping is provisioning-published
// and non-derivable (ADR §2.9): impls perform a runtime lookup, never compute the
// bucket/role name from the slug.
type Resolver interface {
	Resolve(ctx context.Context, tenant string) (Descriptor, error)
}

// Provider is the seam TenantRouter calls (ADR §2.6): it returns the per-tenant
// Binding. Every credential strategy (static-keys today, AWS STS later) is a
// Provider impl.
type Provider interface {
	BindingFor(ctx context.Context, tenant string) (Binding, error)
}

// SecretStore abstracts where per-tenant-scoped static credentials come from
// (Vault / K8s secret / SealedSecret / file / dev). Get resolves the opaque ref
// (Descriptor.CredentialRef) to one tenant's access/secret key pair. Because the
// fetched key is itself scoped to one tenant, the static-keys impl's residual
// gap vs the AWS STS impl is credential lifetime, not cross-tenant blast radius
// (ADR §2.9).
type SecretStore interface {
	Get(ctx context.Context, ref string) (accessKey, secretKey string, err error)
}
