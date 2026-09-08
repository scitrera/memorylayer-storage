// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
)

// writeTemp writes content to a temp file under t.TempDir and returns its path.
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

const goodConfig = `{
  "tenants": {
    "acme": {
      "endpoint": "https://s3.example.com",
      "region": "us-east-1",
      "bucket": "tenant-acme-7f3a",
      "prefix": "packs",
      "credentialRef": "acme"
    },
    "globex": {
      "region": "eu-west-1",
      "bucket": "tenant-globex-9b1c",
      "credentialRef": "globex-ref"
    }
  }
}`

func TestLoad_GoodConfigBuildsResolver(t *testing.T) {
	path := writeTemp(t, "tenants.json", goodConfig)
	f, err := tenantconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(f.Tenants) != 2 {
		t.Fatalf("want 2 tenants, got %d", len(f.Tenants))
	}

	res := f.Resolver()
	ctx := context.Background()

	acme, err := res.Resolve(ctx, "acme")
	if err != nil {
		t.Fatalf("resolve acme: %v", err)
	}
	if acme.Endpoint != "https://s3.example.com" || acme.Region != "us-east-1" ||
		acme.Bucket != "tenant-acme-7f3a" || acme.Prefix != "packs" || acme.CredentialRef != "acme" {
		t.Fatalf("acme descriptor mismatch: %+v", acme)
	}

	globex, err := res.Resolve(ctx, "globex")
	if err != nil {
		t.Fatalf("resolve globex: %v", err)
	}
	if globex.Endpoint != "" || globex.Region != "eu-west-1" || globex.Prefix != "" {
		t.Fatalf("globex descriptor mismatch (optional fields): %+v", globex)
	}

	if _, err := res.Resolve(ctx, "absent"); !errors.Is(err, tenantbind.ErrTenantNotFound) {
		t.Fatalf("resolve absent: want ErrTenantNotFound, got %v", err)
	}
}

func TestLoad_Malformed(t *testing.T) {
	cases := map[string]string{
		"invalid json":          `{ this is not json `,
		"unknown field":         `{"tenants":{"acme":{"region":"r","bucket":"b","credentialRef":"c","bogus":1}}}`,
		"no tenants":            `{"tenants":{}}`,
		"missing region":        `{"tenants":{"acme":{"bucket":"b","credentialRef":"c"}}}`,
		"missing bucket":        `{"tenants":{"acme":{"region":"r","credentialRef":"c"}}}`,
		"missing credentialRef": `{"tenants":{"acme":{"region":"r","bucket":"b"}}}`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeTemp(t, "bad.json", content)
			if _, err := tenantconfig.Load(path); err == nil {
				t.Fatalf("want error for %q, got nil", name)
			}
		})
	}
}

// TestLoad_RejectsPrefixAmbiguousTenants asserts the GC prefix-listing safety
// guard: a config where one tenant name ("acme") is a prefix of another
// ("acme-eu") is rejected at Load, because their "pack-<tenant>-" blob-ID
// namespaces overlap and acme's GC pass could otherwise delete acme-eu's packs.
func TestLoad_RejectsPrefixAmbiguousTenants(t *testing.T) {
	const cfg = `{
  "tenants": {
    "acme":    {"region": "us-east-1", "bucket": "tenant-acme",    "credentialRef": "acme"},
    "acme-eu": {"region": "eu-west-1", "bucket": "tenant-acme-eu", "credentialRef": "acme-eu"}
  }
}`
	path := writeTemp(t, "prefix.json", cfg)
	if _, err := tenantconfig.Load(path); err == nil {
		t.Fatal("acme is a prefix of acme-eu: want Load error, got nil")
	}
}

// TestLoad_RejectsUnsafeTenantName asserts a tenant name that is not a safe ID
// segment (it would smuggle structure into the "pack-<tenant>-" namespace) is
// rejected at Load.
func TestLoad_RejectsUnsafeTenantName(t *testing.T) {
	const cfg = `{
  "tenants": {
    "ac me/x": {"region": "us-east-1", "bucket": "b", "credentialRef": "c"}
  }
}`
	path := writeTemp(t, "unsafe.json", cfg)
	if _, err := tenantconfig.Load(path); err == nil {
		t.Fatal("unsafe tenant name: want Load error, got nil")
	}
}

func TestLoad_MissingFileAndEmptyPath(t *testing.T) {
	if _, err := tenantconfig.Load(""); err == nil {
		t.Fatal("empty path: want error, got nil")
	}
	if _, err := tenantconfig.Load(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("missing file: want error, got nil")
	}
}

func TestSecretStore_FileSource(t *testing.T) {
	secretsPath := writeTemp(t, "secrets.json", `{
  "acme": {"accessKey": "AKIA_ACME", "secretKey": "SECRET_ACME"},
  "globex-ref": {"accessKey": "AKIA_GLOBEX", "secretKey": "SECRET_GLOBEX"}
}`)
	store, err := tenantconfig.NewFileEnvSecretStore(secretsPath)
	if err != nil {
		t.Fatalf("NewFileEnvSecretStore: %v", err)
	}
	ctx := context.Background()

	ak, sk, err := store.Get(ctx, "acme")
	if err != nil {
		t.Fatalf("get acme: %v", err)
	}
	if ak != "AKIA_ACME" || sk != "SECRET_ACME" {
		t.Fatalf("acme keys = %q/%q", ak, sk)
	}

	if _, _, err := store.Get(ctx, "absent"); !errors.Is(err, tenantbind.ErrSecretNotFound) {
		t.Fatalf("absent ref: want ErrSecretNotFound, got %v", err)
	}
}

func TestSecretStore_EnvOverridesFile(t *testing.T) {
	secretsPath := writeTemp(t, "secrets.json", `{"acme":{"accessKey":"FILE_AK","secretKey":"FILE_SK"}}`)
	store, err := tenantconfig.NewFileEnvSecretStore(secretsPath)
	if err != nil {
		t.Fatalf("NewFileEnvSecretStore: %v", err)
	}
	// credentialRef "acme" → suffix "ACME".
	t.Setenv("BLOBGW_TENANT_KEY_ACME", "ENV_AK")
	t.Setenv("BLOBGW_TENANT_SECRET_ACME", "ENV_SK")

	ak, sk, err := store.Get(context.Background(), "acme")
	if err != nil {
		t.Fatalf("get acme: %v", err)
	}
	if ak != "ENV_AK" || sk != "ENV_SK" {
		t.Fatalf("env did not override file: %q/%q", ak, sk)
	}
}

func TestSecretStore_EnvSuffixNormalization(t *testing.T) {
	store, err := tenantconfig.NewFileEnvSecretStore("")
	if err != nil {
		t.Fatalf("NewFileEnvSecretStore: %v", err)
	}
	// "globex-ref" → non-alnum '-' becomes '_', upper-cased → "GLOBEX_REF".
	t.Setenv("BLOBGW_TENANT_KEY_GLOBEX_REF", "AK")
	t.Setenv("BLOBGW_TENANT_SECRET_GLOBEX_REF", "SK")

	ak, sk, err := store.Get(context.Background(), "globex-ref")
	if err != nil {
		t.Fatalf("get globex-ref: %v", err)
	}
	if ak != "AK" || sk != "SK" {
		t.Fatalf("suffix normalization failed: %q/%q", ak, sk)
	}
}

func TestSecretStore_IncompleteEnvIsError(t *testing.T) {
	store, err := tenantconfig.NewFileEnvSecretStore("")
	if err != nil {
		t.Fatalf("NewFileEnvSecretStore: %v", err)
	}
	t.Setenv("BLOBGW_TENANT_KEY_ACME", "ENV_AK") // secret half missing
	if _, _, err := store.Get(context.Background(), "acme"); err == nil {
		t.Fatal("incomplete env credential: want error, got nil")
	}
}

func TestSecretStore_MalformedSecretsFile(t *testing.T) {
	path := writeTemp(t, "secrets.json", `{ not valid `)
	if _, err := tenantconfig.NewFileEnvSecretStore(path); err == nil {
		t.Fatal("malformed secrets file: want error, got nil")
	}
}

func TestLoadProvider_EndToEnd(t *testing.T) {
	configPath := writeTemp(t, "tenants.json", goodConfig)
	secretsPath := writeTemp(t, "secrets.json", `{
  "acme": {"accessKey": "AK_ACME", "secretKey": "SK_ACME"}
}`)
	provider, err := tenantconfig.LoadProvider(configPath, secretsPath, 0)
	if err != nil {
		t.Fatalf("LoadProvider: %v", err)
	}
	binding, err := provider.BindingFor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("BindingFor acme: %v", err)
	}
	if binding.Bucket != "tenant-acme-7f3a" || binding.Region != "us-east-1" {
		t.Fatalf("binding locator mismatch: %+v", binding)
	}
	if binding.Creds.AccessKeyID != "AK_ACME" || binding.Creds.SecretAccessKey != "SK_ACME" {
		t.Fatalf("binding creds mismatch: %+v", binding.Creds)
	}

	// A tenant with no secret published must surface a clear per-tenant error.
	if _, err := provider.BindingFor(context.Background(), "globex"); err == nil {
		t.Fatal("globex has no secret: want error, got nil")
	}
}

func TestLoadProvider_BadConfigPath(t *testing.T) {
	if _, err := tenantconfig.LoadProvider(filepath.Join(t.TempDir(), "nope.json"), "", 0); err == nil {
		t.Fatal("bad config path: want error, got nil")
	}
}
