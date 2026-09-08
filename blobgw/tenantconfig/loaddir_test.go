// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
)

// writeDirFile writes content to name under dir (created if needed) and returns
// its path. Used to compose a directory of per-tenant record files.
func writeDirFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

const acmeFile = `{"tenants":{"acme":{"region":"us-east-1","bucket":"acme-bkt","credentialRef":"acme"}}}`
const globexFile = `{"tenants":{"globex":{"region":"eu-west-1","bucket":"globex-bkt","credentialRef":"globex"}}}`

// TestLoadDir_MergesAcrossFiles asserts the happy path: two per-tenant files in a
// directory merge into one File carrying both tenants, each resolving correctly.
func TestLoadDir_MergesAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	writeDirFile(t, dir, "acme.json", acmeFile)
	writeDirFile(t, dir, "globex.json", globexFile)

	f, err := tenantconfig.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(f.Tenants) != 2 {
		t.Fatalf("want 2 merged tenants, got %d", len(f.Tenants))
	}
	res := f.Resolver()
	ctx := context.Background()
	if d, err := res.Resolve(ctx, "acme"); err != nil || d.Bucket != "acme-bkt" {
		t.Fatalf("resolve acme: %+v err=%v", d, err)
	}
	if d, err := res.Resolve(ctx, "globex"); err != nil || d.Bucket != "globex-bkt" {
		t.Fatalf("resolve globex: %+v err=%v", d, err)
	}
}

// TestLoadDir_SkipsMalformedButLoadsGood asserts a single malformed (and a single
// unreadable-shaped, here non-.json is simply ignored) file does NOT sink the
// load: the good tenants still resolve, mirroring the onboarding-robustness goal.
func TestLoadDir_SkipsMalformedButLoadsGood(t *testing.T) {
	dir := t.TempDir()
	writeDirFile(t, dir, "acme.json", acmeFile)
	writeDirFile(t, dir, "broken.json", `{ this is not json `)
	// A non-*.json file is not globbed at all, so it is silently ignored.
	writeDirFile(t, dir, "notes.txt", `ignore me`)

	f, err := tenantconfig.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir with one malformed file should still load the good one: %v", err)
	}
	if len(f.Tenants) != 1 {
		t.Fatalf("want 1 loaded tenant (malformed skipped), got %d", len(f.Tenants))
	}
	if _, ok := f.Tenants["acme"]; !ok {
		t.Fatalf("acme missing after skipping malformed file: %+v", f.Tenants)
	}
}

// TestLoadDir_ZeroValidTenantsError asserts that a directory yielding no valid
// tenants (empty dir, or every file malformed) fails fast, mirroring the
// single-file "lists no tenants" error.
func TestLoadDir_ZeroValidTenantsError(t *testing.T) {
	t.Run("empty dir", func(t *testing.T) {
		if _, err := tenantconfig.LoadDir(t.TempDir()); err == nil {
			t.Fatal("empty dir: want error, got nil")
		}
	})
	t.Run("all malformed", func(t *testing.T) {
		dir := t.TempDir()
		writeDirFile(t, dir, "a.json", `{ broken`)
		writeDirFile(t, dir, "b.json", `also broken`)
		if _, err := tenantconfig.LoadDir(dir); err == nil {
			t.Fatal("all-malformed dir: want error, got nil")
		}
	})
}

// TestLoadDir_DuplicateTenantAcrossFilesError asserts that the same tenant name
// appearing in two files is a hard error (no silent last-wins).
func TestLoadDir_DuplicateTenantAcrossFilesError(t *testing.T) {
	dir := t.TempDir()
	writeDirFile(t, dir, "acme-a.json", acmeFile)
	writeDirFile(t, dir, "acme-b.json", acmeFile) // same tenant "acme" again
	if _, err := tenantconfig.LoadDir(dir); err == nil {
		t.Fatal("duplicate tenant across files: want error, got nil")
	}
}

// TestLoadDir_MergedPrefixAmbiguityRejected asserts the prefix-ambiguity guard
// runs over the MERGED set: two files each individually valid, but whose tenant
// names are prefix-ambiguous ("acme" vs "acme-eu"), are rejected together.
func TestLoadDir_MergedPrefixAmbiguityRejected(t *testing.T) {
	dir := t.TempDir()
	writeDirFile(t, dir, "acme.json", acmeFile)
	writeDirFile(t, dir, "acme-eu.json", `{"tenants":{"acme-eu":{"region":"eu-west-1","bucket":"acme-eu-bkt","credentialRef":"acme-eu"}}}`)
	if _, err := tenantconfig.LoadDir(dir); err == nil {
		t.Fatal("prefix-ambiguous tenants across files: want error, got nil")
	}
}

// TestLoadDir_ParsesManifestDSN asserts the optional per-record manifestDSN is
// parsed and threaded into the resolved Descriptor.
func TestLoadDir_ParsesManifestDSN(t *testing.T) {
	dir := t.TempDir()
	writeDirFile(t, dir, "acme.json", `{"tenants":{"acme":{"region":"us-east-1","bucket":"acme-bkt","credentialRef":"acme","manifestDSN":"postgres://acme-meta"}}}`)

	f, err := tenantconfig.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	desc, err := f.Resolver().Resolve(context.Background(), "acme")
	if err != nil {
		t.Fatalf("resolve acme: %v", err)
	}
	if desc.ManifestDSN != "postgres://acme-meta" {
		t.Fatalf("acme Descriptor.ManifestDSN = %q, want %q", desc.ManifestDSN, "postgres://acme-meta")
	}
}

// TestLoad_ParsesManifestDSN asserts the single-file path also threads the
// optional manifestDSN into the Descriptor (and that its absence is empty).
func TestLoad_ParsesManifestDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.json")
	if err := os.WriteFile(path, []byte(`{"tenants":{
	  "acme":{"region":"us-east-1","bucket":"acme-bkt","credentialRef":"acme","manifestDSN":"postgres://acme-meta"},
	  "globex":{"region":"eu-west-1","bucket":"globex-bkt","credentialRef":"globex"}
	}}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := tenantconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	res := f.Resolver()
	ctx := context.Background()
	if d, _ := res.Resolve(ctx, "acme"); d.ManifestDSN != "postgres://acme-meta" {
		t.Fatalf("acme ManifestDSN = %q, want postgres://acme-meta", d.ManifestDSN)
	}
	if d, _ := res.Resolve(ctx, "globex"); d.ManifestDSN != "" {
		t.Fatalf("globex ManifestDSN = %q, want empty", d.ManifestDSN)
	}
}

// TestLoadPath_DispatchesFileAndDir asserts LoadPath handles both a single file
// and a directory, matching Load / LoadDir respectively.
func TestLoadPath_DispatchesFileAndDir(t *testing.T) {
	// File.
	filePath := filepath.Join(t.TempDir(), "tenants.json")
	if err := os.WriteFile(filePath, []byte(acmeFile), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if f, err := tenantconfig.LoadPath(filePath); err != nil || len(f.Tenants) != 1 {
		t.Fatalf("LoadPath(file): f=%v err=%v", f, err)
	}

	// Directory.
	dir := t.TempDir()
	writeDirFile(t, dir, "acme.json", acmeFile)
	writeDirFile(t, dir, "globex.json", globexFile)
	if f, err := tenantconfig.LoadPath(dir); err != nil || len(f.Tenants) != 2 {
		t.Fatalf("LoadPath(dir): f=%v err=%v", f, err)
	}

	if _, err := tenantconfig.LoadPath(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("LoadPath(missing): want error, got nil")
	}
}
