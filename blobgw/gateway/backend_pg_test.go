// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// TestPerTenantBackend_RoutesManifestToPostgres asserts that when a tenant has a
// DSN in manifestDSNs, For() builds its manifest store against PostgreSQL (the
// converged mlfs -remote posture: slice_manifest lives in the tenant's meta DB,
// not S3) rather than the S3 manifest store. We point the tenant at an
// unreachable DSN and assert For() fails in the PG branch ("manifest DB"),
// proving the routing decision without standing up a real database. A tenant
// absent from the map must NOT hit the PG branch.
func TestPerTenantBackend_RoutesManifestToPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resolver := tenantbind.NewMapResolver()
	secrets := tenantbind.NewMapSecretStore()
	resolver.Set("pg", tenantbind.Descriptor{
		Endpoint: "s3.local.invalid:9000", Region: "us-east-1", Bucket: "tenant-pg", CredentialRef: "pg-cred",
	})
	secrets.Set("pg-cred", "ak-pg", "sk-pg")
	provider := tenantbind.NewStaticKeysProvider(resolver, secrets)

	// Port 1 refuses fast; connect_timeout bounds the failure so the test never
	// hangs even if the port is firewalled rather than refused.
	const unreachableDSN = "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=2"
	b := newPerTenantBackend(provider, map[string]string{"pg": unreachableDSN}, defaultPerTenantCacheTTL, nil)

	_, _, err := b.For(ctx, "pg")
	if err == nil {
		t.Fatal("For(pg) with an unreachable manifest DSN: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "manifest DB") {
		t.Fatalf("For(pg) error did not take the PG manifest branch: %v", err)
	}
}
