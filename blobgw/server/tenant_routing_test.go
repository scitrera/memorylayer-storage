// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/server"
)

type configuredRouter struct{ router *gateway.TenantRouter }

func (r configuredRouter) ForCtx(ctx context.Context, tenant string) (*gateway.Gateway, error) {
	if tenant != "alpha" && tenant != "beta" {
		return nil, fmt.Errorf("unknown tenant")
	}
	return r.router.ForCtx(ctx, tenant)
}

func TestTenantHTTPRouting(t *testing.T) {
	stack, err := gateway.NewLocalTenantRouter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stack.Router.Close()
	fallback, err := stack.Router.For("default")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fallback.Put(context.Background(), "docs/shared", "text/plain", strings.NewReader("default data"))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.New(fallback, server.WithTenantRouter(configuredRouter{stack.Router}))
	call := func(method, path, tenant, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if tenant != "" {
			req.Header.Set("X-Blobgw-Domain", tenant)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("%s %s tenant=%s status=%d want=%d body=%s", method, path, tenant, rec.Code, want, rec.Body.String())
		}
		if want < 400 && strings.HasPrefix(path, "/v1/") && rec.Header().Get("X-Blobgw-Domain") != tenant {
			t.Fatal("response domain mismatch")
		}
		return rec
	}
	for _, tenant := range []string{"alpha", "beta"} {
		call("PUT", "/v1/objects/docs/shared", tenant, tenant+" data", http.StatusCreated)
	}
	for _, tenant := range []string{"alpha", "beta"} {
		if body := call("GET", "/v1/objects/docs/shared", tenant, "", 200).Body.String(); body != tenant+" data" {
			t.Fatalf("cross-tenant read: %q", body)
		}
		call("HEAD", "/v1/objects/docs/shared", tenant, "", 200)
		rec := call("GET", "/v1/objects?prefix=docs/", tenant, "", 200)
		var infos []gateway.ObjectInfo
		if err := json.Unmarshal(rec.Body.Bytes(), &infos); err != nil {
			t.Fatal(err)
		}
		if len(infos) != 1 || infos[0].Domain != tenant {
			t.Fatalf("cross-tenant list: %+v", infos)
		}
	}
	// Stage and finalize also use the selected tenant's namespace.
	staged := call("POST", "/v1/staged", "alpha", `{"ref":"staged"}`, 200)
	var upload gateway.StagedUpload
	if err := json.Unmarshal(staged.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	stack.Staging.PutStaged(strings.TrimPrefix(upload.UploadURL, "mem://staging/"), []byte("staged bytes"))
	call("POST", "/v1/finalize", "beta", `{"ref":"staged"}`, 404)
	call("POST", "/v1/finalize", "alpha", `{"ref":"staged"}`, 200)
	call("DELETE", "/v1/objects/docs/shared", "alpha", "", 204)
	call("GET", "/v1/objects/docs/shared", "alpha", "", 404)
	call("GET", "/v1/objects/docs/shared", "beta", "", 200)
	call("DELETE", "/v1/objects?prefix=docs/", "beta", "", 200)
	call("GET", "/v1/objects/docs/shared", "beta", "", 404)
	// Fail before executing any handler for absent or unknown tenant headers.
	for _, route := range []struct{ method, path string }{{"GET", "/v1/objects/docs/shared"}, {"HEAD", "/v1/objects/docs/shared"}, {"PUT", "/v1/objects/docs/shared"}, {"DELETE", "/v1/objects/docs/shared"}, {"GET", "/v1/objects"}, {"DELETE", "/v1/objects?prefix=docs/"}, {"POST", "/v1/staged"}, {"POST", "/v1/finalize"}} {
		call(route.method, route.path, "", "", 400)
		call(route.method, route.path, "unknown", "", 503)
	}
	call("GET", "/healthz", "", "", 200)
	call("GET", "/readyz", "", "", 200)
	// Neither omitted tenant headers nor scoped deletes touched the fallback.
	info, err := fallback.Head(context.Background(), "docs/shared")
	if err != nil || info.Size != int64(len("default data")) {
		t.Fatalf("fallback changed: %+v %v", info, err)
	}
}

func TestSingleDomainRejectsMismatchedHeader(t *testing.T) {
	_, stack := newTestServer(t)
	handler := server.New(stack.Gateway)
	for _, tenant := range []string{"ent", "", "another"} {
		req := httptest.NewRequest("PUT", "/v1/objects/test", strings.NewReader("bytes"))
		if tenant != "" {
			req.Header.Set("X-Blobgw-Domain", tenant)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		want := 201
		if tenant == "another" {
			want = 400
		}
		if rec.Code != want {
			t.Fatalf("tenant=%q status=%d want=%d", tenant, rec.Code, want)
		}
	}
	req := httptest.NewRequest("GET", "/v1/objects/test", nil)
	req.Header.Add("X-Blobgw-Domain", "ent")
	req.Header.Add("X-Blobgw-Domain", "another")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("ambiguous tenant header accepted: %d", rec.Code)
	}
}
