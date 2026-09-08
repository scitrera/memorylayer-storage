// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package ctlproto_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// codecRoundTrip is a generic helper that marshals src, unmarshals into a
// zero-value of the same type, and asserts deep equality.
func codecRoundTrip[T any](t *testing.T, c ctlproto.Codec, src T) {
	t.Helper()
	data, err := c.Marshal(src)
	if err != nil {
		t.Fatalf("Marshal(%T): %v", src, err)
	}
	var dst T
	if err := c.Unmarshal(data, &dst); err != nil {
		t.Fatalf("Unmarshal(%T): %v", src, err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("round-trip mismatch for %T\n  got:  %+v\n  want: %+v", src, dst, src)
	}
}

// ---------------------------------------------------------------------------
// Codec round-trip tests — one per message type
// ---------------------------------------------------------------------------

func TestCodecRoundTrip_PackRef(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PackRef{
		PackHash: "abc123def456",
		Offset:   4096,
		Size:     8192,
	})
}

func TestCodecRoundTrip_ChunkLocation(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.ChunkLocation{
		ChunkHash: "deadbeef01234567",
		PackRef: ctlproto.PackRef{
			PackHash: "packpack99",
			Offset:   0,
			Size:     512,
		},
	})
}

func TestCodecRoundTrip_LookupBatchRequest(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.LookupBatchRequest{
		Domain:      "tenant-a",
		ChunkHashes: []string{"hash1", "hash2", "hash3"},
	})
}

func TestCodecRoundTrip_LookupBatchResponse(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.LookupBatchResponse{
		Locations: map[string]ctlproto.PackRef{
			"hash1": {PackHash: "pack1", Offset: 0, Size: 100},
			"hash2": {PackHash: "pack2", Offset: 100, Size: 200},
		},
		Error: "",
	})
}

func TestCodecRoundTrip_LookupBatchResponse_Error(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.LookupBatchResponse{
		Error: "index unavailable",
	})
}

func TestCodecRoundTrip_RecordRequest(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.RecordRequest{
		Domain: "tenant-b",
		Locations: []ctlproto.ChunkLocation{
			{ChunkHash: "c1", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 0, Size: 64}},
			{ChunkHash: "c2", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 64, Size: 128}},
		},
	})
}

func TestCodecRoundTrip_RecordResponse(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.RecordResponse{Error: ""})
	codecRoundTrip(t, c, ctlproto.RecordResponse{Error: "postgres unavailable"})
}

func TestCodecRoundTrip_PresignBatchRequest(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PresignBatchRequest{
		Domain:     "tenant-c",
		Op:         ctlproto.PresignGet,
		Keys:       []string{"packs/aabbcc.pack", "packs/ddeeff.pack"},
		TTLSeconds: 3600,
	})
	codecRoundTrip(t, c, ctlproto.PresignBatchRequest{
		Domain:     "tenant-c",
		Op:         ctlproto.PresignPut,
		Keys:       []string{"packs/new.pack"},
		TTLSeconds: 900,
	})
}

func TestCodecRoundTrip_PresignBatchResponse(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PresignBatchResponse{
		URLs: map[string]string{
			"packs/aabbcc.pack": "https://s3.example.com/bucket/packs/aabbcc.pack?X-Amz-Signature=sig1",
			"packs/ddeeff.pack": "https://s3.example.com/bucket/packs/ddeeff.pack?X-Amz-Signature=sig2",
		},
		ExpiresAtUnix: 1_700_000_000,
		Error:         "",
	})
}

func TestCodecRoundTrip_PresignBatchResponse_Error(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PresignBatchResponse{
		Error: "credential provider unavailable",
	})
}

func TestCodecRoundTrip_PurgePacksRequest(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PurgePacksRequest{
		Domain:     "tenant-d",
		PackHashes: []string{"dead0001", "dead0002"},
	})
}

func TestCodecRoundTrip_PurgePacksResponse(t *testing.T) {
	c := ctlproto.JSONCodec{}
	codecRoundTrip(t, c, ctlproto.PurgePacksResponse{Error: ""})
	codecRoundTrip(t, c, ctlproto.PurgePacksResponse{Error: "pack not found"})
}

// ---------------------------------------------------------------------------
// Subject helper tests
// ---------------------------------------------------------------------------

func TestSubjects(t *testing.T) {
	cases := []struct {
		name string
		fn   func(string) string
		want string
	}{
		{"IndexLookup", ctlproto.IndexLookupSubject, "blobgw.acme.index.lookup"},
		{"IndexRecord", ctlproto.IndexRecordSubject, "blobgw.acme.index.record"},
		{"Presign", ctlproto.PresignSubject, "blobgw.acme.presign"},
		{"GCPurge", ctlproto.GCPurgeSubject, "blobgw.acme.gc.purge"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.fn("acme")
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSubjects_TenantPreserved(t *testing.T) {
	// Ensure the tenant token appears verbatim in each subject.
	tenant := "my-tenant-123"
	subjects := []string{
		ctlproto.IndexLookupSubject(tenant),
		ctlproto.IndexRecordSubject(tenant),
		ctlproto.PresignSubject(tenant),
		ctlproto.GCPurgeSubject(tenant),
	}
	for _, s := range subjects {
		if !strings.Contains(s, tenant) {
			t.Errorf("tenant %q not found in subject %q", tenant, s)
		}
	}
}

func TestTenantFromSubject(t *testing.T) {
	cases := []struct {
		subject    string
		wantTenant string
		wantOK     bool
	}{
		// Round-trip the four operation subjects.
		{ctlproto.IndexLookupSubject("acme"), "acme", true},
		{ctlproto.IndexRecordSubject("acme"), "acme", true},
		{ctlproto.PresignSubject("acme"), "acme", true},
		{ctlproto.GCPurgeSubject("acme"), "acme", true},
		// Tenant token with hyphens/underscores survives.
		{ctlproto.PresignSubject("my-tenant_123"), "my-tenant_123", true},
		// Malformed subjects.
		{"blobgw", "", false},
		{"blobgw.acme", "", false},
		{"other.acme.index.lookup", "", false},
		{"", "", false},
		// A wildcard token is not a valid tenant.
		{"blobgw.*.index.lookup", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.subject, func(t *testing.T) {
			gotTenant, gotOK := ctlproto.TenantFromSubject(tc.subject)
			if gotTenant != tc.wantTenant || gotOK != tc.wantOK {
				t.Fatalf("TenantFromSubject(%q) = (%q, %v), want (%q, %v)",
					tc.subject, gotTenant, gotOK, tc.wantTenant, tc.wantOK)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidateTenant tests
// ---------------------------------------------------------------------------

func TestValidateTenant_Valid(t *testing.T) {
	valid := []string{
		"acme",
		"tenant-123",
		"my_tenant",
		"UPPER",
		"mixedCase-with-dashes",
	}
	for _, tc := range valid {
		if err := ctlproto.ValidateTenant(tc); err != nil {
			t.Errorf("ValidateTenant(%q) unexpected error: %v", tc, err)
		}
	}
}

func TestValidateTenant_Invalid(t *testing.T) {
	invalid := []struct {
		tenant string
		desc   string
	}{
		{"", "empty string"},
		{"has.dot", "contains dot"},
		{"has space", "contains space"},
		{"wild*card", "contains asterisk"},
		{"greater>than", "contains greater-than"},
		{"a.b.c", "multiple dots"},
	}
	for _, tc := range invalid {
		if err := ctlproto.ValidateTenant(tc.tenant); err == nil {
			t.Errorf("ValidateTenant(%q) (%s): expected error, got nil", tc.tenant, tc.desc)
		}
	}
}

// TestValidateTenant_SubjectHelpersPanic verifies that subject helpers panic
// on invalid tenant input rather than silently producing a broken subject.
func TestValidateTenant_SubjectHelpersPanic(t *testing.T) {
	badTenants := []string{"has.dot", "", "wild*"}
	fns := []struct {
		name string
		fn   func(string) string
	}{
		{"IndexLookupSubject", ctlproto.IndexLookupSubject},
		{"IndexRecordSubject", ctlproto.IndexRecordSubject},
		{"PresignSubject", ctlproto.PresignSubject},
		{"GCPurgeSubject", ctlproto.GCPurgeSubject},
	}
	for _, tenant := range badTenants {
		for _, f := range fns {
			t.Run(f.name+"/"+tenant, func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("%s(%q): expected panic on invalid tenant, got none", f.name, tenant)
					}
				}()
				_ = f.fn(tenant)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Constants sanity checks
// ---------------------------------------------------------------------------

// worstCaseHash returns a 64-character hex string, the maximum length of a
// SHA-256 hash in hex encoding.
func worstCaseHash() string {
	return strings.Repeat("f", 64)
}

func TestConstants(t *testing.T) {
	if ctlproto.MaxChunkHashesPerBatch <= 0 {
		t.Error("MaxChunkHashesPerBatch must be positive")
	}
	if ctlproto.MaxPresignKeysPerBatch <= 0 {
		t.Error("MaxPresignKeysPerBatch must be positive")
	}
	if ctlproto.MaxPackHashesPerBatch <= 0 {
		t.Error("MaxPackHashesPerBatch must be positive")
	}
	if ctlproto.QueueGroup == "" {
		t.Error("QueueGroup must not be empty")
	}

	const limit = 900_000 // conservative budget under NATS 1 MB max_payload

	// --- Worst-case LookupBatchResponse at full hit-rate ---
	// map[string]PackRef: every entry is a 64-char hash key mapping to a PackRef
	// whose pack_hash is also 64 chars and offset/size are large integers.
	{
		n := ctlproto.MaxChunkHashesPerBatch
		locs := make(map[string]ctlproto.PackRef, n)
		for i := 0; i < n; i++ {
			key := worstCaseHash()
			locs[key+fmt.Sprintf("%d", i)] = ctlproto.PackRef{
				PackHash: worstCaseHash(),
				Offset:   9_999_999_999_999,
				Size:     9_999_999_999_999,
			}
		}
		resp := ctlproto.LookupBatchResponse{Locations: locs}
		data, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal worst-case LookupBatchResponse: %v", err)
		}
		if len(data) >= limit {
			t.Errorf("worst-case LookupBatchResponse at MaxChunkHashesPerBatch=%d encodes to %d bytes, want < %d",
				n, len(data), limit)
		}
	}

	// --- Worst-case RecordRequest ---
	// []ChunkLocation: each entry has a 64-char chunk hash and a PackRef with
	// 64-char pack hash plus large offset/size integers.
	{
		n := ctlproto.MaxChunkHashesPerBatch
		locs := make([]ctlproto.ChunkLocation, n)
		for i := range locs {
			locs[i] = ctlproto.ChunkLocation{
				ChunkHash: worstCaseHash(),
				PackRef: ctlproto.PackRef{
					PackHash: worstCaseHash(),
					Offset:   9_999_999_999_999,
					Size:     9_999_999_999_999,
				},
			}
		}
		req := ctlproto.RecordRequest{Domain: worstCaseHash(), Locations: locs}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal worst-case RecordRequest: %v", err)
		}
		if len(data) >= limit {
			t.Errorf("worst-case RecordRequest at MaxChunkHashesPerBatch=%d encodes to %d bytes, want < %d",
				n, len(data), limit)
		}
	}

	// --- Worst-case PurgePacksRequest ---
	{
		n := ctlproto.MaxPackHashesPerBatch
		hashes := make([]string, n)
		for i := range hashes {
			hashes[i] = worstCaseHash()
		}
		req := ctlproto.PurgePacksRequest{Domain: worstCaseHash(), PackHashes: hashes}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal worst-case PurgePacksRequest: %v", err)
		}
		if len(data) >= limit {
			t.Errorf("worst-case PurgePacksRequest at MaxPackHashesPerBatch=%d encodes to %d bytes, want < %d",
				n, len(data), limit)
		}
	}

	// --- Worst-case PresignBatchRequest ---
	// S3 keys up to 1024 bytes; response URL ~300 bytes each.
	{
		n := ctlproto.MaxPresignKeysPerBatch
		keys := make([]string, n)
		for i := range keys {
			keys[i] = strings.Repeat("k", 80) // typical pack key ~80 chars
		}
		req := ctlproto.PresignBatchRequest{
			Domain:     worstCaseHash(),
			Op:         ctlproto.PresignPut,
			Keys:       keys,
			TTLSeconds: 3600,
		}
		data, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal worst-case PresignBatchRequest: %v", err)
		}
		if len(data) >= limit {
			t.Errorf("worst-case PresignBatchRequest at MaxPresignKeysPerBatch=%d encodes to %d bytes, want < %d",
				n, len(data), limit)
		}
	}
}

func TestPresignOpValues(t *testing.T) {
	if ctlproto.PresignGet != "GET" {
		t.Errorf("PresignGet = %q, want %q", ctlproto.PresignGet, "GET")
	}
	if ctlproto.PresignPut != "PUT" {
		t.Errorf("PresignPut = %q, want %q", ctlproto.PresignPut, "PUT")
	}
}
