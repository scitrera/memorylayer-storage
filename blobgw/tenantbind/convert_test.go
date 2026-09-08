// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import "testing"

// TestToBlobstoreS3Config_EndpointNormalization covers the kopia-path endpoint
// shaping (stripScheme): kopia's s3 driver wants a BARE HOST, so a scheme is
// stripped; an http:// scheme additionally turns TLS off; an already-bare host
// and an empty endpoint pass through unchanged with TLS on.
func TestToBlobstoreS3Config_EndpointNormalization(t *testing.T) {
	cases := []struct {
		name         string
		endpoint     string
		wantEndpoint string
		wantNoTLS    bool
	}{
		{"bare host", "s3.us-east-2.amazonaws.com", "s3.us-east-2.amazonaws.com", false},
		{"https URI", "https://s3.us-east-2.amazonaws.com", "s3.us-east-2.amazonaws.com", false},
		{"http URI", "http://rustfs.local:9000", "rustfs.local:9000", true},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := Binding{Endpoint: tc.endpoint, Bucket: "bkt", Region: "us-east-2"}
			cfg := b.ToBlobstoreS3Config()
			if cfg.Endpoint != tc.wantEndpoint {
				t.Errorf("Endpoint: got %q want %q", cfg.Endpoint, tc.wantEndpoint)
			}
			if cfg.DoNotUseTLS != tc.wantNoTLS {
				t.Errorf("DoNotUseTLS: got %v want %v", cfg.DoNotUseTLS, tc.wantNoTLS)
			}
			// Identity fields the normalization must NOT disturb.
			if cfg.Bucket != "bkt" || cfg.Region != "us-east-2" {
				t.Errorf("normalization disturbed identity fields: %+v", cfg)
			}
		})
	}
}

// TestToSnapshotS3Config_EndpointNormalization covers the aws-sdk-path endpoint
// shaping (ensureScheme): the AWS SDK wants a valid URI or empty, so a scheme-less
// host gets https://; an already-schemed value passes through; an empty endpoint
// stays empty (region-derived).
func TestToSnapshotS3Config_EndpointNormalization(t *testing.T) {
	cases := []struct {
		name         string
		endpoint     string
		wantEndpoint string
	}{
		{"bare host", "s3.us-east-2.amazonaws.com", "https://s3.us-east-2.amazonaws.com"},
		{"https URI", "https://s3.us-east-2.amazonaws.com", "https://s3.us-east-2.amazonaws.com"},
		{"http URI", "http://rustfs.local:9000", "http://rustfs.local:9000"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := Binding{Endpoint: tc.endpoint, Bucket: "bkt", Region: "us-east-2"}
			cfg := b.ToSnapshotS3Config()
			if cfg.Endpoint != tc.wantEndpoint {
				t.Errorf("Endpoint: got %q want %q", cfg.Endpoint, tc.wantEndpoint)
			}
			if cfg.Bucket != "bkt" || cfg.Region != "us-east-2" {
				t.Errorf("normalization disturbed identity fields: %+v", cfg)
			}
		})
	}
}

// TestEndpointNormalization_SingleBareHostFeedsBothClients is the BUG-1
// regression: the SINGLE bare-host binding value a deployment may set in its configuration
// must satisfy BOTH clients at once — bare for kopia, https:// for the AWS SDK.
func TestEndpointNormalization_SingleBareHostFeedsBothClients(t *testing.T) {
	b := Binding{Endpoint: "s3.us-east-2.amazonaws.com", Bucket: "bkt", Region: "us-east-2"}
	if got := b.ToBlobstoreS3Config().Endpoint; got != "s3.us-east-2.amazonaws.com" {
		t.Errorf("kopia endpoint: got %q, want bare host", got)
	}
	if got := b.ToSnapshotS3Config().Endpoint; got != "https://s3.us-east-2.amazonaws.com" {
		t.Errorf("aws-sdk endpoint: got %q, want https:// URI", got)
	}
}

func TestStripScheme(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantHTTP bool
	}{
		{"s3.example.com", "s3.example.com", false},
		{"https://s3.example.com", "s3.example.com", false},
		{"http://s3.example.com", "s3.example.com", true},
		{"", "", false},
	}
	for _, tc := range cases {
		host, httpScheme := stripScheme(tc.in)
		if host != tc.wantHost || httpScheme != tc.wantHTTP {
			t.Errorf("stripScheme(%q) = (%q,%v), want (%q,%v)", tc.in, host, httpScheme, tc.wantHost, tc.wantHTTP)
		}
	}
}

func TestEnsureScheme(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"s3.example.com", "https://s3.example.com"},
		{"https://s3.example.com", "https://s3.example.com"},
		{"http://s3.example.com", "http://s3.example.com"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := ensureScheme(tc.in); got != tc.want {
			t.Errorf("ensureScheme(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
