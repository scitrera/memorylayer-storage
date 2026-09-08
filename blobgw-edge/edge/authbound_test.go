// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
)

// mintRaw POSTs an arbitrary mint body as (tenant, subject) and returns the
// decoded token (empty on non-200), the finalized claims decoded from the token
// (zero when there is no token), and the HTTP status. It is the auth-binding
// tests' workhorse: it lets a test send require_auth / match_mode (which the
// edge_test.go mintCap helper does not) and inspect the stamped ra/mm claims.
func (h *harness) mintRaw(t *testing.T, tenant, subject string, body map[string]any) (string, capability.Claims, int) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/capabilities", bytes.NewReader(raw))
	req.Header.Set("X-Auth-Tenant-ID", tenant)
	req.Header.Set("X-Scitrera-User", subject)
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", capability.Claims{}, resp.StatusCode
	}
	var out struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	claims, err := h.verifier.Verify(out.Token)
	if err != nil {
		t.Fatalf("verify minted token: %v", err)
	}
	return out.Token, claims, resp.StatusCode
}

// TestMint_StampsRequireAuthAndMatchMode covers the mint-side resolution of the
// tri-state require_auth flag and the match_mode claim, including the reserved /
// invalid mode rejections.
func TestMint_StampsRequireAuthAndMatchMode(t *testing.T) {
	tests := []struct {
		name          string
		defaultRA     bool
		body          map[string]any
		wantStatus    int
		wantRA        bool
		wantMatchMode string
	}{
		{
			name:          "absent require_auth + default false => shareable, no match-mode",
			defaultRA:     false,
			body:          map[string]any{"op": "GET", "ref": "x"},
			wantStatus:    http.StatusOK,
			wantRA:        false,
			wantMatchMode: "",
		},
		{
			name:          "require_auth true => auth-bound, match-mode defaults exact",
			defaultRA:     false,
			body:          map[string]any{"op": "GET", "ref": "x", "require_auth": true},
			wantStatus:    http.StatusOK,
			wantRA:        true,
			wantMatchMode: "exact",
		},
		{
			name:          "require_auth true + match_mode same-tenant",
			defaultRA:     false,
			body:          map[string]any{"op": "GET", "ref": "x", "require_auth": true, "match_mode": "same-tenant"},
			wantStatus:    http.StatusOK,
			wantRA:        true,
			wantMatchMode: "same-tenant",
		},
		{
			name:          "absent require_auth + default true => auth-bound exact",
			defaultRA:     true,
			body:          map[string]any{"op": "GET", "ref": "x"},
			wantStatus:    http.StatusOK,
			wantRA:        true,
			wantMatchMode: "exact",
		},
		{
			name:          "explicit require_auth false overrides default true => shareable",
			defaultRA:     true,
			body:          map[string]any{"op": "GET", "ref": "x", "require_auth": false},
			wantStatus:    http.StatusOK,
			wantRA:        false,
			wantMatchMode: "",
		},
		{
			name:       "match_mode group is reserved => 400",
			defaultRA:  false,
			body:       map[string]any{"op": "GET", "ref": "x", "require_auth": true, "match_mode": "group"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "match_mode bogus => 400",
			defaultRA:  false,
			body:       map[string]any{"op": "GET", "ref": "x", "require_auth": true, "match_mode": "bogus"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "match_mode ignored when shareable (forced empty)",
			defaultRA:     false,
			body:          map[string]any{"op": "GET", "ref": "x", "require_auth": false, "match_mode": "same-tenant"},
			wantStatus:    http.StatusOK,
			wantRA:        false,
			wantMatchMode: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, edge.Config{DefaultRequireAuth: tc.defaultRA}, edge.Deps{})
			_, claims, st := h.mintRaw(t, "acme", "alice", tc.body)
			if st != tc.wantStatus {
				t.Fatalf("status: got %d, want %d", st, tc.wantStatus)
			}
			if st != http.StatusOK {
				return
			}
			if claims.RequireAuth != tc.wantRA {
				t.Errorf("RequireAuth: got %v, want %v", claims.RequireAuth, tc.wantRA)
			}
			if claims.MatchMode != tc.wantMatchMode {
				t.Errorf("MatchMode: got %q, want %q", claims.MatchMode, tc.wantMatchMode)
			}
		})
	}
}

// TestMint_TTLClamp covers the mode-keyed TTL clamps: a shareable request for a
// long TTL is clamped down to ShareableMaxTTL, and an auth-bound request for an
// absurd TTL is clamped to AuthBoundMaxTTL.
func TestMint_TTLClamp(t *testing.T) {
	tests := []struct {
		name       string
		body       map[string]any
		wantMaxTTL time.Duration
	}{
		{
			name:       "shareable 3600s clamps to ShareableMaxTTL (300s)",
			body:       map[string]any{"op": "GET", "ref": "x", "require_auth": false, "ttl_seconds": 3600},
			wantMaxTTL: 5 * time.Minute,
		},
		{
			name:       "auth-bound 999999s clamps to AuthBoundMaxTTL (60m)",
			body:       map[string]any{"op": "GET", "ref": "x", "require_auth": true, "ttl_seconds": 999999},
			wantMaxTTL: 60 * time.Minute,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Defaults (ShareableMaxTTL=5m, AuthBoundMaxTTL=60m) come from New().
			h := newHarness(t, edge.Config{}, edge.Deps{})
			_, claims, st := h.mintRaw(t, "acme", "alice", tc.body)
			if st != http.StatusOK {
				t.Fatalf("mint status %d", st)
			}
			// Expiry is now + clamped-ttl; NotBefore is now - 30s skew. The window
			// (exp-nbf) must not exceed the clamp + the 30s skew allowance.
			window := time.Duration(claims.Expiry-claims.NotBefore) * time.Second
			if window > tc.wantMaxTTL+45*time.Second {
				t.Errorf("token window %v exceeds clamp %v (+skew)", window, tc.wantMaxTTL)
			}
			// And it must be at least the clamp (the request asked for far more), so we
			// know the clamp took effect rather than some smaller default.
			if window < tc.wantMaxTTL {
				t.Errorf("token window %v is below the clamp ceiling %v", window, tc.wantMaxTTL)
			}
		})
	}
}

// authBoundGet mints an auth-bound GET capability (via the minter directly so the
// test controls ra/mm) for (tenant, subject) and issues the GET carrying the
// supplied live identity headers, returning the response status. liveTenant /
// liveSubject empty means the header is omitted (anonymous).
func (h *harness) authBoundGet(t *testing.T, tenant, subject, matchMode, ref, liveTenant, liveSubject string) int {
	t.Helper()
	tok, _, err := h.minter.Mint(capability.Claims{
		Tenant: tenant, Subject: subject, Op: capability.OpGet, Ref: ref,
		RequireAuth: true, MatchMode: matchMode,
	}, time.Minute)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, h.blobURL(ref, tok), nil)
	if liveTenant != "" {
		req.Header.Set("X-Auth-Tenant-ID", liveTenant)
	}
	if liveSubject != "" {
		req.Header.Set("X-Scitrera-User", liveSubject)
	}
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestVerify_AuthBoundEnforcement exercises the data-path principal match on an
// auth-bound capability across exact / same-tenant / missing-headers, plus the
// shareable (ra=false) baseline that must be unchanged (no headers required).
func TestVerify_AuthBoundEnforcement(t *testing.T) {
	// Seed an object as (acme, alice) so a matching GET can return 200.
	setup := func(t *testing.T) *harness {
		h := newHarness(t, edge.Config{}, edge.Deps{})
		if st := h.put(t, "acme", "alice", "doc", "application/octet-stream", randomBytes(t, 2048)); st != http.StatusCreated {
			t.Fatalf("seed PUT: %d", st)
		}
		return h
	}

	t.Run("exact match with correct headers => 200", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "exact", "doc", "acme", "alice"); st != http.StatusOK {
			t.Errorf("got %d, want 200", st)
		}
	})
	t.Run("exact match wrong subject => 403", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "exact", "doc", "acme", "mallory"); st != http.StatusForbidden {
			t.Errorf("got %d, want 403", st)
		}
	})
	t.Run("exact match missing headers => 403", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "exact", "doc", "", ""); st != http.StatusForbidden {
			t.Errorf("got %d, want 403", st)
		}
	})
	t.Run("empty match-mode defaults to exact => 200 on match", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "", "doc", "acme", "alice"); st != http.StatusOK {
			t.Errorf("got %d, want 200", st)
		}
	})
	t.Run("same-tenant, same tenant different subject => 200", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "same-tenant", "doc", "acme", "bob"); st != http.StatusOK {
			t.Errorf("got %d, want 200", st)
		}
	})
	t.Run("same-tenant, different tenant => 403", func(t *testing.T) {
		h := setup(t)
		if st := h.authBoundGet(t, "acme", "alice", "same-tenant", "doc", "other", "bob"); st != http.StatusForbidden {
			t.Errorf("got %d, want 403", st)
		}
	})
	t.Run("shareable (ra=false) with no headers => 200 (unchanged)", func(t *testing.T) {
		h := setup(t)
		// A plain bearer GET carrying NO identity headers must still succeed.
		tok, _, err := h.minter.Mint(capability.Claims{
			Tenant: "acme", Subject: "alice", Op: capability.OpGet, Ref: "doc",
		}, time.Minute)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		resp, err := h.srv.Client().Get(h.blobURL("doc", tok))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("shareable GET: got %d, want 200", resp.StatusCode)
		}
	})
}
