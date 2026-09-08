// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// CredentialMode selects how per-tenant S3 credentials are obtained (ADR §2.9).
type CredentialMode string

const (
	// CredentialModeStatic is the portable static-keys path (dev + non-AWS prod:
	// R2 / MinIO / GCS HMAC / dev keys). CredentialRef in each resolver record is
	// a secret reference resolved via the FileEnvSecretStore. This is the default
	// for back-compat.
	CredentialModeStatic CredentialMode = "static"

	// CredentialModeAWSSTS is the AWS AssumeRoleWithWebIdentity path (ADR §2.9
	// "Option 2"). CredentialRef in each resolver record is the per-tenant IAM
	// role ARN, STS-assumed via the pod's IRSA web-identity token. It REQUIRES
	// IRSA: the pod must have AWS_WEB_IDENTITY_TOKEN_FILE projected (and a
	// ServiceAccount role binding) by the IRSA admission webhook.
	CredentialModeAWSSTS CredentialMode = "aws-sts"
)

// ParseCredentialMode validates a credential-mode string, returning an error for
// any value other than the supported modes so a misconfiguration fails fast.
func ParseCredentialMode(s string) (CredentialMode, error) {
	switch CredentialMode(s) {
	case CredentialModeStatic:
		return CredentialModeStatic, nil
	case CredentialModeAWSSTS:
		return CredentialModeAWSSTS, nil
	default:
		return "", fmt.Errorf("tenantconfig: unknown credential mode %q (want %q|%q)", s, CredentialModeStatic, CredentialModeAWSSTS)
	}
}

// DefaultCacheTTL is the default per-tenant credential cache TTL (ADR §2.9: the
// TTL doubles as the secret-rotation pickup). It matches the gateway's default
// per-tenant cache TTL so a rotated credential propagates to both the presign
// path and the per-tenant direct-I/O path within one TTL.
const DefaultCacheTTL = 15 * time.Minute

// LoadProvider is the one-call wiring of the ADR §2.9 static-keys credential
// path: it loads the tenant-config file at configPath, builds the
// tenant→Descriptor MapResolver and the file/env-backed SecretStore (secretsFile
// optional), composes them into a StaticKeysProvider, and wraps that in a
// CachingProvider with the given TTL. A non-positive ttl falls back to
// DefaultCacheTTL.
//
// The returned Provider is what controlplane.NewServer and
// gateway.WithBindingProvider both consume — one seam for the presign path and
// the per-tenant direct-I/O path (ADR §2.6/§2.9).
func LoadProvider(configPath, secretsFile string, ttl time.Duration) (tenantbind.Provider, error) {
	cfg, err := Load(configPath)
	if err != nil {
		return nil, err
	}
	return buildStaticProvider(cfg.Resolver(), secretsFile, ttl)
}

// buildStaticProvider composes the static-keys provider stack over the given
// resolver: StaticKeysProvider(resolver, FileEnvSecretStore) wrapped in the
// CachingProvider. Split out from LoadProvider so both the one-shot path
// (LoadProvider, over cfg.Resolver()) and the restart-free path
// (LoadProviderModeWatched, over a ReloadingResolver) reuse the identical
// StaticKeys/Caching wiring. A non-positive ttl falls back to DefaultCacheTTL.
func buildStaticProvider(resolver tenantbind.Resolver, secretsFile string, ttl time.Duration) (tenantbind.Provider, error) {
	secrets, err := NewFileEnvSecretStore(secretsFile)
	if err != nil {
		return nil, err
	}
	static := tenantbind.NewStaticKeysProvider(resolver, secrets)
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	caching, err := tenantbind.NewCachingProvider(static, ttl)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: build caching provider: %w", err)
	}
	return caching, nil
}

// LoadProviderMode is the credential-mode-aware wiring of the ADR §2.9 provider.
// In CredentialModeStatic it is identical to LoadProvider (the portable
// static-keys path). In CredentialModeAWSSTS it builds an AssumeRoleProvider that
// interprets each resolver record's CredentialRef as an IAM role ARN and assumes
// it via the pod's IRSA web-identity token (requires AWS_WEB_IDENTITY_TOKEN_FILE
// in the pod), wrapped in the expiry-aware CachingProvider so STS sessions are
// refreshed before they expire.
//
// secretsFile is consulted only in static mode (it has no meaning when
// credentials come from STS). A non-positive ttl falls back to DefaultCacheTTL.
func LoadProviderMode(ctx context.Context, mode CredentialMode, configPath, secretsFile string, ttl time.Duration) (tenantbind.Provider, error) {
	switch mode {
	case CredentialModeStatic:
		return LoadProvider(configPath, secretsFile, ttl)
	case CredentialModeAWSSTS:
		cfg, err := Load(configPath)
		if err != nil {
			return nil, err
		}
		assumer, err := tenantbind.NewWebIdentityAssumer(ctx)
		if err != nil {
			return nil, fmt.Errorf("tenantconfig: build aws-sts assumer: %w", err)
		}
		return buildAWSSTSProvider(cfg.Resolver(), assumer, ttl)
	default:
		return nil, fmt.Errorf("tenantconfig: unsupported credential mode %q", mode)
	}
}

// LoadProviderModeWatched is the restart-free variant of LoadProviderMode: it
// builds the provider stack over a ReloadingResolver instead of a one-shot
// cfg.Resolver(), so tenant add/remove in the -tenant-config file takes effect
// WITHOUT restarting the process. It (a) constructs the ReloadingResolver with
// an INITIAL synchronous load (startup still fails fast on a bad config),
// (b) starts the resolver's polling goroutine bound to ctx (it exits on ctx
// cancel / shutdown), and (c) composes the same StaticKeys/AssumeRole + Caching
// stack LoadProviderMode uses, over that resolver.
//
// Because the resolver is consulted on every BindingFor, a newly-ADDED tenant
// resolves immediately once a reload lands (it was never a cached success, only
// an ErrTenantNotFound). A REMOVED or rebound tenant propagates within the
// CachingProvider ttl (its stale binding may remain cached until then). TTL
// semantics are unchanged from LoadProviderMode.
//
// secretsFile is consulted only in static mode. A non-positive ttl falls back
// to DefaultCacheTTL.
func LoadProviderModeWatched(ctx context.Context, mode CredentialMode, configPath, secretsFile string, ttl time.Duration) (tenantbind.Provider, error) {
	resolver, err := NewReloadingResolver(configPath)
	if err != nil {
		return nil, err
	}
	switch mode {
	case CredentialModeStatic:
		prov, err := buildStaticProvider(resolver, secretsFile, ttl)
		if err != nil {
			return nil, err
		}
		resolver.Start(ctx)
		return prov, nil
	case CredentialModeAWSSTS:
		assumer, err := tenantbind.NewWebIdentityAssumer(ctx)
		if err != nil {
			return nil, fmt.Errorf("tenantconfig: build aws-sts assumer: %w", err)
		}
		prov, err := buildAWSSTSProvider(resolver, assumer, ttl)
		if err != nil {
			return nil, err
		}
		resolver.Start(ctx)
		return prov, nil
	default:
		return nil, fmt.Errorf("tenantconfig: unsupported credential mode %q", mode)
	}
}

// buildAWSSTSProvider composes the AssumeRoleProvider over resolver+assumer and
// wraps it in the expiry-aware CachingProvider. Split out from LoadProviderMode
// so tests can inject a fake assumer and verify construction without calling STS.
func buildAWSSTSProvider(resolver tenantbind.Resolver, assumer tenantbind.WebIdentityAssumer, ttl time.Duration) (tenantbind.Provider, error) {
	ar, err := tenantbind.NewAssumeRoleProvider(resolver, assumer)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: build assume-role provider: %w", err)
	}
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	caching, err := tenantbind.NewCachingProvider(ar, ttl)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: build caching provider: %w", err)
	}
	return caching, nil
}
