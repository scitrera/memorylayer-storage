// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// writeFile writes content to path, creating/truncating it. Used to (re)publish
// a tenant-config file mid-test to drive a reload.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

const acmeOnly = `{
  "tenants": {
    "acme": {"region": "us-east-1", "bucket": "acme-bkt", "credentialRef": "acme"}
  }
}`

const acmeAndbeta = `{
  "tenants": {
    "acme": {"region": "us-east-1", "bucket": "acme-bkt", "credentialRef": "acme"},
    "beta": {"region": "us-west-2", "bucket": "beta-bkt", "credentialRef": "beta"}
  }
}`

const betaOnly = `{
  "tenants": {
    "beta": {"region": "us-west-2", "bucket": "beta-bkt", "credentialRef": "beta"}
  }
}`

// mustResolve asserts the resolver returns a descriptor for tenant with the
// expected bucket.
func mustResolve(t *testing.T, r *ReloadingResolver, tenant, wantBucket string) {
	t.Helper()
	desc, err := r.Resolve(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Resolve(%q): unexpected error: %v", tenant, err)
	}
	if desc.Bucket != wantBucket {
		t.Fatalf("Resolve(%q).Bucket = %q, want %q", tenant, desc.Bucket, wantBucket)
	}
}

// mustNotResolve asserts the resolver reports tenant as not found.
func mustNotResolve(t *testing.T, r *ReloadingResolver, tenant string) {
	t.Helper()
	if _, err := r.Resolve(context.Background(), tenant); !errors.Is(err, tenantbind.ErrTenantNotFound) {
		t.Fatalf("Resolve(%q): want ErrTenantNotFound, got %v", tenant, err)
	}
}

// TestReloadingResolver_InitialLoad verifies the constructor resolves a seeded
// tenant (initial synchronous load, no poll needed).
func TestReloadingResolver_InitialLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeOnly)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")
}

// TestReloadingResolver_InitialLoadFailsFast verifies a bad config fails the
// constructor (preserving the one-shot startup behavior).
func TestReloadingResolver_InitialLoadFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, `{ this is not json`)

	if _, err := NewReloadingResolver(path); err == nil {
		t.Fatal("NewReloadingResolver: expected error on malformed config")
	}
}

// TestReloadingResolver_AddTenant verifies that rewriting the file with a NEW
// tenant makes it resolve after one reload cycle (driven deterministically via
// reloadOnce, not the ticker).
func TestReloadingResolver_AddTenant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeOnly)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")
	mustNotResolve(t, r, "beta")

	writeFile(t, path, acmeAndbeta)
	r.reloadOnce()

	mustResolve(t, r, "acme", "acme-bkt") // still present
	mustResolve(t, r, "beta", "beta-bkt") // newly added, resolves
}

// TestReloadingResolver_MalformedRewriteKeepsPrevious verifies that a malformed
// rewrite keeps the previous good resolver (old tenant still resolves, no
// panic), then a subsequent good rewrite is picked up.
func TestReloadingResolver_MalformedRewriteKeepsPrevious(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeOnly)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")

	// Malformed (e.g. a mid-write ConfigMap projection) — must NOT blank tenants.
	writeFile(t, path, `{ "tenants": broken`)
	r.reloadOnce()
	mustResolve(t, r, "acme", "acme-bkt") // previous good resolver retained

	// A subsequent good rewrite is still picked up (the malformed load did not
	// wedge the hash / state).
	writeFile(t, path, acmeAndbeta)
	r.reloadOnce()
	mustResolve(t, r, "beta", "beta-bkt")
}

// TestReloadingResolver_RemoveTenant verifies that removing a tenant makes it
// stop resolving after a reload.
func TestReloadingResolver_RemoveTenant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeAndbeta)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")
	mustResolve(t, r, "beta", "beta-bkt")

	writeFile(t, path, betaOnly)
	r.reloadOnce()

	mustNotResolve(t, r, "acme") // removed
	mustResolve(t, r, "beta", "beta-bkt")
}

// TestReloadingResolver_UnchangedIsNoOp verifies that reloadOnce on an unchanged
// file does not error or alter the resolved set (content-hash short-circuit).
func TestReloadingResolver_UnchangedIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeOnly)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	before := r.cur.Load()
	r.reloadOnce()
	if r.cur.Load() != before {
		t.Fatal("reloadOnce swapped the resolver for an unchanged file")
	}
	mustResolve(t, r, "acme", "acme-bkt")
}

// TestReloadingResolver_ReadErrorKeepsPrevious verifies that a read failure
// (file removed) keeps the previous resolver rather than crashing/blanking.
func TestReloadingResolver_ReadErrorKeepsPrevious(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	writeFile(t, path, acmeOnly)

	r, err := NewReloadingResolver(path)
	if err != nil {
		t.Fatalf("NewReloadingResolver: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	r.reloadOnce() // read error path — must be a no-op
	mustResolve(t, r, "acme", "acme-bkt")
}

// Per-tenant file bodies for the directory-mode reload tests. Each is a single
// {"tenants":{...}} record, the onboarding layout (one file per tenant).
const acmeFileBody = `{"tenants":{"acme":{"region":"us-east-1","bucket":"acme-bkt","credentialRef":"acme"}}}`
const betaFileBody = `{"tenants":{"beta":{"region":"us-west-2","bucket":"beta-bkt","credentialRef":"beta"}}}`

// TestReloadingResolver_DirInitialLoad verifies the constructor detects a
// DIRECTORY path and merges the per-tenant files at startup.
func TestReloadingResolver_DirInitialLoad(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "acme.json"), acmeFileBody)
	writeFile(t, filepath.Join(dir, "beta.json"), betaFileBody)

	r, err := NewReloadingResolver(dir)
	if err != nil {
		t.Fatalf("NewReloadingResolver(dir): %v", err)
	}
	if !r.isDir {
		t.Fatal("expected isDir=true for a directory path")
	}
	mustResolve(t, r, "acme", "acme-bkt")
	mustResolve(t, r, "beta", "beta-bkt")
}

// TestReloadingResolver_DirAddFile verifies that DROPPING a new per-tenant file
// into the directory makes that tenant resolve after one reload cycle — the
// one-file-per-tenant onboarding property, hot-reloaded.
func TestReloadingResolver_DirAddFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "acme.json"), acmeFileBody)

	r, err := NewReloadingResolver(dir)
	if err != nil {
		t.Fatalf("NewReloadingResolver(dir): %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")
	mustNotResolve(t, r, "beta")

	// Onboard beta = add one file.
	writeFile(t, filepath.Join(dir, "beta.json"), betaFileBody)
	r.reloadOnce()

	mustResolve(t, r, "acme", "acme-bkt") // still present
	mustResolve(t, r, "beta", "beta-bkt") // newly added file resolves
}

// TestReloadingResolver_DirRemoveFile verifies that REMOVING a per-tenant file
// stops that tenant resolving after a reload (offboarding = delete one file).
func TestReloadingResolver_DirRemoveFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "acme.json"), acmeFileBody)
	betaPath := filepath.Join(dir, "beta.json")
	writeFile(t, betaPath, betaFileBody)

	r, err := NewReloadingResolver(dir)
	if err != nil {
		t.Fatalf("NewReloadingResolver(dir): %v", err)
	}
	mustResolve(t, r, "beta", "beta-bkt")

	if err := os.Remove(betaPath); err != nil {
		t.Fatalf("remove beta file: %v", err)
	}
	r.reloadOnce()

	mustResolve(t, r, "acme", "acme-bkt")
	mustNotResolve(t, r, "beta")
}

// TestReloadingResolver_DirMalformedFileSkippedKeepsGood verifies that adding a
// MALFORMED file to the directory does not blank the good tenants: the good file
// still resolves and the reload does not error out.
func TestReloadingResolver_DirMalformedFileSkippedKeepsGood(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "acme.json"), acmeFileBody)

	r, err := NewReloadingResolver(dir)
	if err != nil {
		t.Fatalf("NewReloadingResolver(dir): %v", err)
	}
	mustResolve(t, r, "acme", "acme-bkt")

	// A malformed sibling file (e.g. a mid-write projection) must be skipped, not
	// wedge the whole set.
	writeFile(t, filepath.Join(dir, "broken.json"), `{ broken`)
	r.reloadOnce()
	mustResolve(t, r, "acme", "acme-bkt") // good tenant retained

	// A subsequent good add is still picked up.
	writeFile(t, filepath.Join(dir, "beta.json"), betaFileBody)
	r.reloadOnce()
	mustResolve(t, r, "beta", "beta-bkt")
}
