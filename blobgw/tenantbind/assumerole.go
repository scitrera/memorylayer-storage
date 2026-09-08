// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"context"
	"fmt"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// WebIdentityAssumer abstracts the single STS AssumeRoleWithWebIdentity call the
// AssumeRoleProvider needs. Abstracting it (rather than calling aws-sdk directly)
// keeps the provider's tenant→roleARN→sessionName→Binding mapping unit-testable
// with a deterministic fake, with no AWS network dependency. The production impl
// is WebIdentityAssumerSTS.
type WebIdentityAssumer interface {
	// Assume STS-assumes roleARN under sessionName using the pod's IRSA
	// web-identity token and returns the resulting temporary credentials. The
	// returned Creds carry AccessKeyID, SecretAccessKey, SessionToken, and the
	// STS Expiry (Expiration).
	Assume(ctx context.Context, roleARN, sessionName string) (Creds, error)
}

// AssumeRoleProvider is the AWS AssumeRoleWithWebIdentity credential impl of
// ADR §2.9 ("Option 2"). For a tenant T it resolves the Descriptor, interprets
// Descriptor.CredentialRef as the per-tenant IAM role ARN, and STS-assumes that
// role using the pod's IRSA web-identity token with RoleSessionName
// "blobgw-<tenant>" (the §2.9 audit-distinguishing session name). The returned
// Binding carries Endpoint/Region/Bucket/Prefix from the Descriptor and Creds
// (AccessKeyID/SecretAccessKey/SessionToken/Expiry) from the STS response.
//
// AssumeRoleProvider does no caching itself; wrap it in a CachingProvider for the
// ADR §2.9 on-demand-fetch + expiry-aware lifecycle (the cache refreshes the STS
// session before Creds.Expiry).
type AssumeRoleProvider struct {
	resolver Resolver
	assumer  WebIdentityAssumer
}

var _ Provider = (*AssumeRoleProvider)(nil)

// NewAssumeRoleProvider builds an AssumeRoleProvider over the given resolver and
// STS assumer. Both are required. The production assumer is built by
// NewWebIdentityAssumer; tests inject a fake.
func NewAssumeRoleProvider(resolver Resolver, assumer WebIdentityAssumer) (*AssumeRoleProvider, error) {
	if resolver == nil {
		return nil, fmt.Errorf("tenantbind: assume-role provider: nil resolver")
	}
	if assumer == nil {
		return nil, fmt.Errorf("tenantbind: assume-role provider: nil assumer")
	}
	return &AssumeRoleProvider{resolver: resolver, assumer: assumer}, nil
}

// BindingFor resolves the tenant's Descriptor, STS-assumes the role named by
// Descriptor.CredentialRef under the session name "blobgw-<tenant>", and
// assembles the Binding. The returned Creds carry the STS Expiry, the cliff the
// CachingProvider refreshes before (ADR §2.9).
func (p *AssumeRoleProvider) BindingFor(ctx context.Context, tenant string) (Binding, error) {
	if tenant == "" {
		return Binding{}, fmt.Errorf("tenantbind: empty tenant")
	}
	desc, err := p.resolver.Resolve(ctx, tenant)
	if err != nil {
		return Binding{}, fmt.Errorf("tenantbind: resolve %q: %w", tenant, err)
	}
	if desc.CredentialRef == "" {
		return Binding{}, fmt.Errorf("tenantbind: tenant %q: descriptor has empty CredentialRef (expected an IAM role ARN)", tenant)
	}
	sessionName := RoleSessionName(tenant)
	creds, err := p.assumer.Assume(ctx, desc.CredentialRef, sessionName)
	if err != nil {
		return Binding{}, fmt.Errorf("tenantbind: assume role for %q (role %q): %w", tenant, desc.CredentialRef, err)
	}
	return Binding{
		Endpoint:    desc.Endpoint,
		Region:      desc.Region,
		Bucket:      desc.Bucket,
		Prefix:      desc.Prefix,
		ManifestDSN: desc.ManifestDSN,
		Creds:       creds,
	}, nil
}

// RoleSessionName builds the STS RoleSessionName for a tenant: "blobgw-<tenant>".
// Per ADR §2.9 this is the audit-distinguishing name so CloudTrail attributes
// each assumed-role session to the originating blobgw + tenant.
func RoleSessionName(tenant string) string {
	return "blobgw-" + tenant
}

// WebIdentityAssumerSTS is the production WebIdentityAssumer. It wraps
// aws-sdk-go-v2's stscreds.WebIdentityRoleProvider over an STS client, reading
// the pod's IRSA web-identity token from the file at AWS_WEB_IDENTITY_TOKEN_FILE.
// A fresh provider is built per Assume call so the RoleSessionName (which varies
// per tenant) and a current token read are used each time.
type WebIdentityAssumerSTS struct {
	stsClient stscreds.AssumeRoleWithWebIdentityAPIClient
	tokenFile string
}

var _ WebIdentityAssumer = (*WebIdentityAssumerSTS)(nil)

// NewWebIdentityAssumer builds the production STS web-identity assumer. It loads
// the default AWS config (for region/STS endpoint resolution) and reads the IRSA
// web-identity token path from AWS_WEB_IDENTITY_TOKEN_FILE — the env var the
// IRSA admission webhook injects into the pod. It fails if that env var is unset,
// since AssumeRoleWithWebIdentity has no token source without it.
//
// NOTE: aws-sts mode requires IRSA — the pod must have AWS_WEB_IDENTITY_TOKEN_FILE
// (and an associated ServiceAccount role binding) projected by the IRSA webhook.
func NewWebIdentityAssumer(ctx context.Context) (*WebIdentityAssumerSTS, error) {
	tokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if tokenFile == "" {
		return nil, fmt.Errorf("tenantbind: aws-sts requires IRSA: AWS_WEB_IDENTITY_TOKEN_FILE is unset")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("tenantbind: load aws config for sts: %w", err)
	}
	return &WebIdentityAssumerSTS{
		stsClient: sts.NewFromConfig(awsCfg),
		tokenFile: tokenFile,
	}, nil
}

// Assume STS-assumes roleARN under sessionName via AssumeRoleWithWebIdentity,
// reading the IRSA token from the configured token file, and maps the resulting
// aws.Credentials onto Creds (including the STS Expiry).
func (a *WebIdentityAssumerSTS) Assume(ctx context.Context, roleARN, sessionName string) (Creds, error) {
	provider := stscreds.NewWebIdentityRoleProvider(
		a.stsClient, roleARN, stscreds.IdentityTokenFile(a.tokenFile),
		func(o *stscreds.WebIdentityRoleOptions) {
			o.RoleSessionName = sessionName
		},
	)
	awsCreds, err := provider.Retrieve(ctx)
	if err != nil {
		return Creds{}, fmt.Errorf("assume role %q with web identity: %w", roleARN, err)
	}
	c := Creds{
		AccessKeyID:     awsCreds.AccessKeyID,
		SecretAccessKey: awsCreds.SecretAccessKey,
		SessionToken:    awsCreds.SessionToken,
	}
	if awsCreds.CanExpire {
		c.Expiry = awsCreds.Expires
	}
	return c, nil
}
