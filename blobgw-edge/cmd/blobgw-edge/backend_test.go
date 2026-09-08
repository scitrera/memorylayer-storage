// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestBuildBackend_LocalDefault asserts the default (backend=local) posture
// still constructs the shared-backend local router and reports in-process GC —
// the dev behavior this change must preserve exactly.
func TestBuildBackend_LocalDefault(t *testing.T) {
	b, err := buildBackend(context.Background(), backendOptions{
		backend: "local",
		dataDir: t.TempDir(),
	}, nil) // nil registry: reg.Meter() is nil-safe (no-op meter)
	if err != nil {
		t.Fatalf("buildBackend(local): %v", err)
	}
	defer b.cleanup()
	if b.router == nil {
		t.Fatal("local backend: nil router")
	}
	if !b.inProcessGC {
		t.Error("local backend: want inProcessGC=true (shared-backend posture)")
	}
	if b.router.GC() == nil {
		t.Error("local backend: shared-backend GC() must be non-nil")
	}
}

// TestBuildBackend_UnknownBackend asserts an unrecognized -backend fails fast.
func TestBuildBackend_UnknownBackend(t *testing.T) {
	if _, err := buildBackend(context.Background(), backendOptions{backend: "gcs"}, nil); err == nil {
		t.Fatal("buildBackend(gcs): want error, got nil")
	}
}

// TestBuildBackend_S3RequiresTenantConfig asserts s3 mode without -tenant-config
// fails fast (mirrors blobgw's fail-style checks).
func TestBuildBackend_S3RequiresTenantConfig(t *testing.T) {
	_, err := buildBackend(context.Background(), backendOptions{
		backend: "s3",
		index:   "memory",
		staging: "none",
	}, nil)
	if err == nil {
		t.Fatal("buildBackend(s3, no tenant-config): want error, got nil")
	}
}

// TestBuildBackend_S3IndexPostgresRequiresDSN asserts index=postgres without a
// DSN fails fast even when a tenant-config is present.
func TestBuildBackend_S3IndexPostgresRequiresDSN(t *testing.T) {
	cfg := writeTenantConfig(t)
	_, err := buildBackend(context.Background(), backendOptions{
		backend:      "s3",
		index:        "postgres",
		staging:      "none",
		tenantConfig: cfg,
	}, nil)
	if err == nil {
		t.Fatal("buildBackend(s3, index=postgres, no DSN): want error, got nil")
	}
}

// TestBuildBackend_S3MemoryIndexConstructs asserts the s3 per-tenant router
// assembles with a valid tenant-config over the dev memory index + no staging,
// WITHOUT dialing S3/PG (binding resolution is lazy), and reports NOT in-process
// GC (per-tenant posture: router.GC() is nil, sweeps run via GCForTenant/A4).
func TestBuildBackend_S3MemoryIndexConstructs(t *testing.T) {
	cfg := writeTenantConfig(t)
	b, err := buildBackend(context.Background(), backendOptions{
		backend:        "s3",
		index:          "memory",
		staging:        "none",
		tenantConfig:   cfg,
		credentialMode: "static",
	}, nil)
	if err != nil {
		t.Fatalf("buildBackend(s3, memory index): %v", err)
	}
	defer b.cleanup()
	if b.router == nil {
		t.Fatal("s3 backend: nil router")
	}
	if b.inProcessGC {
		t.Error("s3 backend: want inProcessGC=false (per-tenant posture)")
	}
	if b.router.GC() != nil {
		t.Error("s3 backend: per-tenant posture GC() must be nil (use GCForTenant)")
	}
	// GCForTenant is the A4 leader-sweep entry point in this posture. Actually
	// resolving a tenant's binding constructs a live per-tenant S3 client (bucket
	// from the tenant-config), which is S3-integration-gated; the unit test only
	// asserts the router assembled with the per-tenant GC seam wired, above.
}

// writeTenantConfig writes a minimal valid tenant-config file and returns its
// path. Static-keys mode resolves bindings lazily, so no live S3 is touched by
// buildBackend.
func writeTenantConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tenants.json")
	const body = `{
  "tenants": {
    "acme": {
      "region": "us-east-1",
      "bucket": "tenant-acme",
      "credentialRef": "acme"
    }
  }
}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write tenant config: %v", err)
	}
	return path
}
