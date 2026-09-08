// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantconfig

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// writeTempInternal writes content to a temp file under t.TempDir and returns its
// path. (Local to the internal test package; the external test file's writeTemp
// is in a different package and not visible here.)
func writeTempInternal(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp %q: %v", name, err)
	}
	return path
}

func TestParseCredentialMode(t *testing.T) {
	for _, tt := range []struct {
		in      string
		want    CredentialMode
		wantErr bool
	}{
		{"static", CredentialModeStatic, false},
		{"aws-sts", CredentialModeAWSSTS, false},
		{"", "", true},
		{"bogus", "", true},
	} {
		got, err := ParseCredentialMode(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseCredentialMode(%q): expected error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCredentialMode(%q): %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("ParseCredentialMode(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// fakeAssumer is a deterministic tenantbind.WebIdentityAssumer so the aws-sts
// provider construction + binding flow can be exercised without calling STS.
type fakeAssumer struct {
	creds tenantbind.Creds
}

func (f fakeAssumer) Assume(_ context.Context, _, _ string) (tenantbind.Creds, error) {
	return f.creds, nil
}

// TestBuildAWSSTSProvider verifies the aws-sts provider is constructed (resolver
// → AssumeRoleProvider → expiry-aware CachingProvider) and resolves a binding via
// the injected fake assumer, with no STS call. It exercises the same wiring
// LoadProviderMode uses in aws-sts mode (minus the IRSA-backed assumer).
func TestBuildAWSSTSProvider(t *testing.T) {
	resolver := tenantbind.NewMapResolver()
	resolver.Set("acme", tenantbind.Descriptor{
		Region:        "us-east-1",
		Bucket:        "acme-bkt",
		Prefix:        "packs",
		CredentialRef: "arn:aws:iam::123456789012:role/blobgw-acme",
	})
	assumer := fakeAssumer{creds: tenantbind.Creds{
		AccessKeyID:     "ASIA-x",
		SecretAccessKey: "sec",
		SessionToken:    "tok",
		Expiry:          time.Now().Add(time.Hour),
	}}

	prov, err := buildAWSSTSProvider(resolver, assumer, 0 /* default ttl */)
	if err != nil {
		t.Fatalf("buildAWSSTSProvider: %v", err)
	}

	b, err := prov.BindingFor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}
	if b.Bucket != "acme-bkt" || b.Creds.SessionToken != "tok" {
		t.Fatalf("unexpected binding: %+v", b)
	}
	if b.Creds.AccessKeyID != "ASIA-x" {
		t.Fatalf("unexpected creds: %+v", b.Creds)
	}
}

// TestLoadProviderMode_Static verifies the static path is selected and builds a
// working provider from a config + env-backed secret (no STS / IRSA needed).
func TestLoadProviderMode_Static(t *testing.T) {
	cfgPath := writeTempInternal(t, "tenants.json", `{
	  "tenants": {
	    "acme": {"region": "us-east-1", "bucket": "acme-bkt", "credentialRef": "acme"}
	  }
	}`)
	t.Setenv("BLOBGW_TENANT_KEY_ACME", "AK")
	t.Setenv("BLOBGW_TENANT_SECRET_ACME", "SK")

	prov, err := LoadProviderMode(context.Background(), CredentialModeStatic, cfgPath, "", time.Minute)
	if err != nil {
		t.Fatalf("LoadProviderMode static: %v", err)
	}
	b, err := prov.BindingFor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}
	if b.Creds.AccessKeyID != "AK" || b.Bucket != "acme-bkt" {
		t.Fatalf("unexpected static binding: %+v", b)
	}
}
