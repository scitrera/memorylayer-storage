// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

const audience = "edge-test"

type harness struct {
	srv      *httptest.Server
	ltr      *gateway.LocalTenantRouter
	minter   *capability.Minter
	verifier *capability.Verifier
}

func newHarness(t *testing.T, cfg edge.Config, deps edge.Deps) *harness {
	t.Helper()
	ltr, err := gateway.NewLocalTenantRouter(t.TempDir(), 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalTenantRouter: %v", err)
	}
	pub, priv, _ := capability.GenerateKey()
	cfg.Audience = audience
	minter := capability.NewMinter("k1", priv, audience)
	ver := capability.NewVerifier(audience)
	ver.AddKey("k1", pub)
	if deps.Identity == nil {
		deps.Identity = edge.HeaderIdentityProvider{}
	}
	s := edge.New(ltr.Router, minter, ver, deps, cfg)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &harness{srv: srv, ltr: ltr, minter: minter, verifier: ver}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// mintCap calls POST /capabilities as (tenant, subject) and returns the token.
func (h *harness) mintCap(t *testing.T, tenant, subject, op, ref, contentType string, maxSize int64) (string, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"op": op, "ref": ref, "content_type": contentType, "max_size": maxSize, "ttl_seconds": 600,
	})
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/capabilities", bytes.NewReader(body))
	req.Header.Set("X-Auth-Tenant-ID", tenant)
	req.Header.Set("X-Scitrera-User", subject)
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.Token, resp.StatusCode
}

func (h *harness) blobURL(ref, token string) string {
	return h.srv.URL + "/blob/" + ref + "?cap=" + token
}

func (h *harness) put(t *testing.T, tenant, subject, ref, ct string, data []byte) int {
	t.Helper()
	tok, st := h.mintCap(t, tenant, subject, "PUT", ref, ct, 0)
	if st != http.StatusOK {
		return st
	}
	req, _ := http.NewRequest(http.MethodPut, h.blobURL(ref, tok), bytes.NewReader(data))
	req.Header.Set("Content-Type", ct)
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (h *harness) countBlobs(t *testing.T, tenant string) int {
	t.Helper()
	n := 0
	for _, p := range []blobstore.ID{blobstore.ID("chunk-" + tenant + "-"), blobstore.ID("pack-" + tenant + "-")} {
		if err := h.ltr.Chunks.ListBlobs(context.Background(), p, func(blobstore.Metadata) error { n++; return nil }); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return n
}

func TestEdge_MintUploadDownload(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	payload := randomBytes(t, 512*1024)

	if st := h.put(t, "acme", "alice", "docs/report.pdf", "application/pdf", payload); st != http.StatusCreated {
		t.Fatalf("PUT status %d", st)
	}

	// GET via a fresh capability.
	tok, _ := h.mintCap(t, "acme", "alice", "GET", "docs/report.pdf", "", 0)
	resp, err := h.srv.Client().Get(h.blobURL("docs/report.pdf", tok))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, payload) {
		t.Errorf("download mismatch: %d vs %d bytes", len(got), len(payload))
	}
}

// 25 users in one tenant upload the identical document → ~1 physical copy.
func TestEdge_IntraTenantDedup(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	doc := randomBytes(t, 1024*1024)

	if st := h.put(t, "acme", "user0", "uploads/u0/doc.pdf", "application/pdf", doc); st != http.StatusCreated {
		t.Fatalf("first PUT: %d", st)
	}
	after1 := h.countBlobs(t, "acme")
	if after1 == 0 {
		t.Fatal("expected blobs after first upload")
	}
	for i := 1; i < 25; i++ {
		ref := fmt.Sprintf("uploads/u%d/doc.pdf", i)
		if st := h.put(t, "acme", fmt.Sprintf("user%d", i), ref, "application/pdf", doc); st != http.StatusCreated {
			t.Fatalf("PUT %d: %d", i, st)
		}
	}
	after25 := h.countBlobs(t, "acme")
	if after25 != after1 {
		t.Errorf("intra-tenant dedup failed: 25 identical docs grew blobs %d → %d", after1, after25)
	}
}

func TestEdge_CrossTenantIsolation(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	secret := randomBytes(t, 256*1024)

	// Tenant B stores a private object.
	if st := h.put(t, "tenantB", "bob", "private/secret.bin", "application/octet-stream", secret); st != http.StatusCreated {
		t.Fatalf("B PUT: %d", st)
	}

	// Tenant A mints a GET capability for the SAME ref and tries to read it.
	// The capability is bound to tenant A, so it resolves in A's namespace —
	// B's object is unreachable (404), and identical content is NOT deduped
	// across tenants.
	tok, _ := h.mintCap(t, "tenantA", "alice", "GET", "private/secret.bin", "", 0)
	resp, err := h.srv.Client().Get(h.blobURL("private/secret.bin", tok))
	if err != nil {
		t.Fatalf("A GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("tenant A reading tenant B's ref: got %d, want 404", resp.StatusCode)
	}

	// B can still read its own object.
	tokB, _ := h.mintCap(t, "tenantB", "bob", "GET", "private/secret.bin", "", 0)
	respB, err := h.srv.Client().Get(h.blobURL("private/secret.bin", tokB))
	if err != nil {
		t.Fatalf("B GET own: %v", err)
	}
	defer respB.Body.Close()
	gotB, _ := io.ReadAll(respB.Body)
	if !bytes.Equal(gotB, secret) {
		t.Error("tenant B cannot read its own object")
	}

	// Cross-tenant: no physical sharing. A stores identical content; both
	// tenants keep their own blobs.
	if st := h.put(t, "tenantA", "alice", "copy.bin", "application/octet-stream", secret); st != http.StatusCreated {
		t.Fatalf("A PUT: %d", st)
	}
	if h.countBlobs(t, "tenantA") == 0 || h.countBlobs(t, "tenantB") == 0 {
		t.Error("each tenant must hold its own physical blobs")
	}
}

func TestEdge_RejectsBadCapabilities(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	payload := randomBytes(t, 4096)
	h.put(t, "acme", "alice", "x", "application/octet-stream", payload)

	getTok, _ := h.mintCap(t, "acme", "alice", "GET", "x", "", 0)

	// Wrong op: GET capability used for PUT.
	req, _ := http.NewRequest(http.MethodPut, h.blobURL("x", getTok), bytes.NewReader(payload))
	resp, _ := h.srv.Client().Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("wrong-op: got %d, want 403", resp.StatusCode)
	}

	// Wrong ref: GET capability for "x" used against "y".
	resp, _ = h.srv.Client().Get(h.blobURL("y", getTok))
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("wrong-ref: got %d, want 403", resp.StatusCode)
	}

	// Tampered token.
	bad := []byte(getTok)
	bad[0] ^= 0x01
	resp, _ = h.srv.Client().Get(h.blobURL("x", string(bad)))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("tampered: got %d, want 401", resp.StatusCode)
	}

	// Missing token.
	resp, _ = h.srv.Client().Get(h.srv.URL + "/blob/x")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("missing-cap: got %d, want 401", resp.StatusCode)
	}
}

func TestEdge_ExpiredToken(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	// Mint a token that is already expired by driving the minter's clock back.
	past := time.Now().Add(-2 * time.Hour)
	h.minter.SetClock(func() time.Time { return past })
	tok, _, _ := h.minter.Mint(capability.Claims{Tenant: "acme", Subject: "a", Op: capability.OpGet, Ref: "x"}, time.Minute)
	resp, _ := h.srv.Client().Get(h.blobURL("x", tok))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expired token: got %d, want 401", resp.StatusCode)
	}
}

func TestEdge_Revocation(t *testing.T) {
	rev := edge.NewMemoryRevocation()
	h := newHarness(t, edge.Config{}, edge.Deps{Revocation: rev})
	h.put(t, "acme", "alice", "x", "application/octet-stream", randomBytes(t, 4096))

	tok, _ := h.mintCap(t, "acme", "alice", "GET", "x", "", 0)
	// Decode the jti from the token to revoke it (verifier exposes claims).
	claims, err := h.verifier.Verify(tok)
	if err != nil {
		t.Fatalf("verify for jti: %v", err)
	}
	rev.Revoke(context.Background(), claims.JTI, time.Time{})

	resp, _ := h.srv.Client().Get(h.blobURL("x", tok))
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("revoked token: got %d, want 403", resp.StatusCode)
	}
}

func TestEdge_ConfidentialModeDisablesExistence(t *testing.T) {
	h := newHarness(t, edge.Config{ExistenceMode: edge.ModeConfidential}, edge.Deps{})
	// Minting a HEAD capability is refused.
	_, st := h.mintCap(t, "acme", "alice", "HEAD", "x", "", 0)
	if st != http.StatusForbidden {
		t.Errorf("confidential HEAD mint: got %d, want 403", st)
	}
}

func TestEdge_TrustedModeAllowsHead(t *testing.T) {
	h := newHarness(t, edge.Config{ExistenceMode: edge.ModeTrusted}, edge.Deps{})
	payload := randomBytes(t, 8192)
	h.put(t, "acme", "alice", "x", "image/png", payload)
	tok, st := h.mintCap(t, "acme", "alice", "HEAD", "x", "", 0)
	if st != http.StatusOK {
		t.Fatalf("HEAD mint: %d", st)
	}
	req, _ := http.NewRequest(http.MethodHead, h.blobURL("x", tok), nil)
	resp, _ := h.srv.Client().Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("HEAD: got %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != fmt.Sprint(len(payload)) {
		t.Errorf("HEAD Content-Length %q", resp.Header.Get("Content-Length"))
	}
}

func TestEdge_QuotaEnforced(t *testing.T) {
	q := edge.NewMemoryQuotaStore(0, 1) // at most 1 object per tenant
	h := newHarness(t, edge.Config{}, edge.Deps{Quota: q})
	if st := h.put(t, "acme", "alice", "a", "application/octet-stream", randomBytes(t, 4096)); st != http.StatusCreated {
		t.Fatalf("first PUT: %d", st)
	}
	if st := h.put(t, "acme", "alice", "b", "application/octet-stream", randomBytes(t, 4096)); st != http.StatusInsufficientStorage {
		t.Errorf("over-quota PUT: got %d, want 507", st)
	}
}

func TestEdge_RateLimitOnMint(t *testing.T) {
	rl := edge.NewTokenBucketLimiter(0.001, 1) // burst 1, ~no refill
	h := newHarness(t, edge.Config{}, edge.Deps{Rate: rl})
	if _, st := h.mintCap(t, "acme", "alice", "GET", "x", "", 0); st != http.StatusOK {
		t.Fatalf("first mint: %d", st)
	}
	if _, st := h.mintCap(t, "acme", "bob", "GET", "y", "", 0); st != http.StatusTooManyRequests {
		t.Errorf("rate-limited mint: got %d, want 429", st)
	}
}

func TestEdge_ContentTypeAllowlist(t *testing.T) {
	h := newHarness(t, edge.Config{AllowedContentTypes: []string{"image/png"}}, edge.Deps{})
	// Disallowed content type is rejected at mint.
	_, st := h.mintCap(t, "acme", "alice", "PUT", "x", "application/pdf", 0)
	if st != http.StatusForbidden {
		t.Errorf("disallowed content type mint: got %d, want 403", st)
	}
	// Allowed content type works end to end.
	if st := h.put(t, "acme", "alice", "y", "image/png", randomBytes(t, 4096)); st != http.StatusCreated {
		t.Errorf("allowed content type PUT: got %d, want 201", st)
	}
}

func TestEdge_MaxSizeEnforced(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	// Cap with a 1 KiB max; upload 4 KiB → 413.
	tok, _ := h.mintCap(t, "acme", "alice", "PUT", "big", "application/octet-stream", 1024)
	req, _ := http.NewRequest(http.MethodPut, h.blobURL("big", tok), bytes.NewReader(randomBytes(t, 4096)))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, _ := h.srv.Client().Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize PUT: got %d, want 413", resp.StatusCode)
	}
}

// TestEdge_ContentHashBinding proves a PUT capability carrying a ContentHash
// claim binds the uploaded bytes: a body hashing to a different value is
// rejected with 403 and leaves no readable ref, while a matching body succeeds.
func TestEdge_ContentHashBinding(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "ch/object.bin"
	const ct = "application/octet-stream"
	good := randomBytes(t, 4096)
	goodHash := hex.EncodeToString(sha256Sum(good))

	// putWithCapHash mints a PUT capability bound to capHash (via the minter
	// directly, since the JSON mint helper does not expose content_hash) and
	// uploads body, returning the response status.
	putWithCapHash := func(capHash string, body []byte) int {
		t.Helper()
		tok, _, err := h.minter.Mint(capability.Claims{
			Tenant: "acme", Subject: "alice", Op: capability.OpPut, Ref: ref, ContentHash: capHash,
		}, time.Minute)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		req, _ := http.NewRequest(http.MethodPut, h.blobURL(ref, tok), bytes.NewReader(body))
		req.Header.Set("Content-Type", ct)
		resp, err := h.srv.Client().Do(req)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// Mismatch: capability demands goodHash but the body is different bytes.
	bad := randomBytes(t, 4096)
	if st := putWithCapHash(goodHash, bad); st != http.StatusForbidden {
		t.Fatalf("hash-mismatch PUT: got %d, want 403", st)
	}
	// The rejected upload must not leave a readable ref behind.
	getTok, _ := h.mintCap(t, "acme", "alice", "GET", ref, "", 0)
	resp, err := h.srv.Client().Get(h.blobURL(ref, getTok))
	if err != nil {
		t.Fatalf("GET after mismatch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("ref after rejected PUT: got %d, want 404", resp.StatusCode)
	}

	// Match: body hashes to the capability's ContentHash → upload succeeds.
	if st := putWithCapHash(goodHash, good); st != http.StatusCreated {
		t.Fatalf("hash-match PUT: got %d, want 201", st)
	}
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func TestEdge_UnauthenticatedMint(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	// No X-Auth-Tenant-ID/X-Scitrera-User headers.
	body, _ := json.Marshal(map[string]any{"op": "GET", "ref": "x"})
	resp, err := h.srv.Client().Post(h.srv.URL+"/capabilities", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated mint: got %d, want 401", resp.StatusCode)
	}
}
