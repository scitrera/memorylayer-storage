// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/server"
)

type tenantGateways map[string]*gateway.Gateway

func (g tenantGateways) ForCtx(_ context.Context, tenant string) (*gateway.Gateway, error) {
	if gw := g[tenant]; gw != nil {
		return gw, nil
	}
	return nil, fmt.Errorf("unregistered tenant")
}

func TestTenantRoutingIsolationAndSharedObjects(t *testing.T) {
	tenants := tenantGateways{}
	for _, name := range []string{"one", "two", "fallback"} {
		stack, err := gateway.NewLocalStack(t.TempDir(), name, 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		tenants[name] = stack.Gateway
		// Simulate objects created through the edge's tenant gateway first.
		if _, err := stack.Gateway.Put(context.Background(), "same-ref", "text/plain", strings.NewReader(name)); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(server.New(tenants["fallback"], server.WithTenantRouter(tenants)))
	defer srv.Close()
	request := func(method, path, tenant, body string) (*http.Response, string) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if tenant != "" {
			req.Header.Set("X-Blobgw-Domain", tenant)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, string(b)
	}
	// Concurrent requests must not mutate a shared server gateway pointer.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		for _, tenant := range []string{"one", "two"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, body := request("GET", "/v1/objects/same-ref", tenant, "")
				if resp.StatusCode != 200 || body != tenant {
					t.Errorf("%s got %d %q", tenant, resp.StatusCode, body)
				}
			}()
		}
	}
	wg.Wait()
	resp, _ := request("PUT", "/v1/objects/new", "one", "new bytes")
	if resp.StatusCode != 201 {
		t.Fatal(resp.StatusCode)
	}
	if _, err := tenants["one"].Head(context.Background(), "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := tenants["two"].Head(context.Background(), "new"); err == nil {
		t.Fatal("cross-tenant write")
	}
	resp, _ = request("DELETE", "/v1/objects/same-ref", "one", "")
	if resp.StatusCode != 204 {
		t.Fatal(resp.StatusCode)
	}
	resp, body := request("GET", "/v1/objects/same-ref", "two", "")
	if resp.StatusCode != 200 || body != "two" {
		t.Fatal("cross-tenant delete")
	}
	for _, path := range []string{"/v1/objects/same-ref", "/v1/objects"} {
		resp, _ := request("GET", path, "", "")
		if resp.StatusCode != 400 {
			t.Errorf("missing domain got %d", resp.StatusCode)
		}
		resp, _ = request("GET", path, "unknown", "")
		if resp.StatusCode != 503 {
			t.Errorf("unknown domain got %d", resp.StatusCode)
		}
	}
	for _, path := range []string{"/v1/staged", "/v1/finalize"} {
		resp, _ := request("POST", path, "", `{}`)
		if resp.StatusCode != 400 {
			t.Errorf("missing domain got %d", resp.StatusCode)
		}
	}
	resp, _ = request("GET", "/healthz", "", "")
	if resp.StatusCode != 200 {
		t.Fatal("health must not require tenant")
	}
	req, _ := http.NewRequest("GET", srv.URL+"/v1/objects/same-ref", nil)
	req.Header.Add("X-Blobgw-Domain", "one")
	req.Header.Add("X-Blobgw-Domain", "two")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal("ambiguous domain accepted")
	}
}
