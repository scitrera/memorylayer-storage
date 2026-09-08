// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"net/http/httptest"
	"testing"
)

func TestParseAuthorization(t *testing.T) {
	good := "AWS4-HMAC-SHA256 " +
		"Credential=AKID/20260602/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
		"Signature=deadbeef"
	tests := []struct {
		name    string
		header  string
		wantErr error
		wantKey string
		wantSig string
	}{
		{name: "valid", header: good, wantKey: "AKID", wantSig: "deadbeef"},
		{name: "empty", header: "", wantErr: errMissingSecurityHeader},
		{name: "wrong algorithm", header: "AWS4-FOO Credential=x", wantErr: errAuthorizationHeaderForm},
		{name: "missing signature", header: "AWS4-HMAC-SHA256 Credential=AKID/d/r/s/aws4_request, SignedHeaders=host", wantErr: errAuthorizationHeaderForm},
		{name: "bad credential scope", header: "AWS4-HMAC-SHA256 Credential=AKID/d/r/s, SignedHeaders=host, Signature=x", wantErr: errAuthorizationHeaderForm},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAuthorization(tc.header)
			if tc.wantErr != nil {
				if err != tc.wantErr {
					t.Fatalf("err: got %v want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got.accessKeyID != tc.wantKey {
				t.Errorf("accessKeyID: got %q want %q", got.accessKeyID, tc.wantKey)
			}
			if got.signature != tc.wantSig {
				t.Errorf("signature: got %q want %q", got.signature, tc.wantSig)
			}
			if got.credentialScope() != "20260602/us-east-1/s3/aws4_request" {
				t.Errorf("credentialScope: got %q", got.credentialScope())
			}
		})
	}
}

func TestAWSURIEncode(t *testing.T) {
	tests := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"abcXYZ090-_.~", false, "abcXYZ090-_.~"},
		{"a/b/c", false, "a/b/c"},
		{"a/b/c", true, "a%2Fb%2Fc"},
		{"a b", false, "a%20b"},
		{"k=v&x", true, "k%3Dv%26x"},
	}
	for _, tc := range tests {
		if got := awsURIEncode(tc.in, tc.encodeSlash); got != tc.want {
			t.Errorf("awsURIEncode(%q, %v): got %q want %q", tc.in, tc.encodeSlash, got, tc.want)
		}
	}
}

// TestSigningKeyKnownAnswer checks the signing-key derivation against the AWS
// documentation's published example vector, anchoring the HMAC chain.
func TestSigningKeyKnownAnswer(t *testing.T) {
	// From the AWS SigV4 "Examples of how to derive a signing key" reference.
	key := signingKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam")
	want := "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
	if got := hexString(key); got != want {
		t.Errorf("signing key: got %s want %s", got, want)
	}
}

func TestCanonicalQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/bucket/key?b=2&a=1&a=0", nil)
	if got := canonicalQuery(r.URL); got != "a=0&a=1&b=2" {
		t.Errorf("canonicalQuery: got %q", got)
	}
}

func hexString(b []byte) string {
	const hexchars = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexchars[c>>4]
		out[i*2+1] = hexchars[c&0x0f]
	}
	return string(out)
}
