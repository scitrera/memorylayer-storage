// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package server_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/server"
)

func newTestServer(t *testing.T) (*httptest.Server, *gateway.LocalStack) {
	t.Helper()
	stack, err := gateway.NewLocalStack(t.TempDir(), "ent", 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalStack: %v", err)
	}
	srv := httptest.NewServer(server.New(stack.Gateway))
	t.Cleanup(srv.Close)
	return srv, stack
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestServer_PutGetHeadDelete(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()
	payload := randomBytes(t, 1024*1024)
	url := srv.URL + "/v1/objects/docs/page-1.png"

	// PUT
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "image/png")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status: %d", resp.StatusCode)
	}
	var info gateway.ObjectInfo
	_ = json.NewDecoder(resp.Body).Decode(&info)
	resp.Body.Close()
	if info.Size != int64(len(payload)) {
		t.Errorf("PUT info size %d want %d", info.Size, len(payload))
	}

	// HEAD — headers, no body.
	resp, err = c.Head(url)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(payload)) {
		t.Errorf("HEAD Content-Length %q want %d", got, len(payload))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("HEAD Content-Type %q", ct)
	}
	if et := resp.Header.Get("ETag"); et != `"`+info.ContentHash+`"` {
		t.Errorf("HEAD ETag %q want %q", et, `"`+info.ContentHash+`"`)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) != 0 {
		t.Errorf("HEAD returned a body of %d bytes", len(body))
	}

	// GET — exact bytes.
	resp, err = c.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("GET body mismatch: %d vs %d bytes", len(got), len(payload))
	}

	// DELETE → 204, then GET 404.
	req, _ = http.NewRequest(http.MethodDelete, url, nil)
	resp, err = c.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = c.Get(url)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after delete status: %d want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestServer_ListByPrefix(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()
	payload := randomBytes(t, 256*1024)
	for _, ref := range []string{"a/1", "a/2", "b/1"} {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/objects/"+ref, bytes.NewReader(payload))
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("PUT %s: %v", ref, err)
		}
		resp.Body.Close()
	}

	resp, err := c.Get(srv.URL + "/v1/objects?prefix=a/")
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	var objs []gateway.ObjectInfo
	_ = json.NewDecoder(resp.Body).Decode(&objs)
	resp.Body.Close()
	if len(objs) != 2 {
		t.Errorf("list prefix a/ returned %d, want 2", len(objs))
	}
	for _, o := range objs {
		if !strings.HasPrefix(o.Ref, "a/") {
			t.Errorf("unexpected ref in a/ listing: %s", o.Ref)
		}
	}
}

func TestServer_StageThenFinalize(t *testing.T) {
	srv, stack := newTestServer(t)
	c := srv.Client()

	// Mint.
	mintBody, _ := json.Marshal(map[string]any{"content_type": "application/pdf", "ttl_seconds": 600})
	resp, err := c.Post(srv.URL+"/v1/staged", "application/json", bytes.NewReader(mintBody))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	var staged gateway.StagedUpload
	_ = json.NewDecoder(resp.Body).Decode(&staged)
	resp.Body.Close()
	if staged.Ref == "" || staged.StagingKey == "" {
		t.Fatalf("bad staged upload: %+v", staged)
	}

	// Simulate the presigned upload by writing to the in-memory staging store.
	payload := randomBytes(t, 512*1024)
	stack.Staging.PutStaged(staged.StagingKey, payload)

	// Finalize.
	finBody, _ := json.Marshal(map[string]string{"ref": staged.Ref})
	resp, err = c.Post(srv.URL+"/v1/finalize", "application/json", bytes.NewReader(finBody))
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("finalize status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// GET the finalized object.
	resp, err = c.Get(srv.URL + "/v1/objects/" + staged.Ref)
	if err != nil {
		t.Fatalf("GET finalized: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("finalized round-trip mismatch")
	}
}

func TestServer_GetMissing404(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, err := srv.Client().Get(srv.URL + "/v1/objects/nope")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status %d want 404", resp.StatusCode)
	}
}

// TestServer_HealthzAndReadyz covers the operational endpoints (A6): healthz is
// a pure liveness 200; readyz reflects the wired dependency probe — 200 when the
// probe passes (healthy local stack), 503 when it fails.
func TestServer_HealthzAndReadyz(t *testing.T) {
	stack, err := gateway.NewLocalStack(t.TempDir(), "ent", 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalStack: %v", err)
	}

	// Healthy: probe returns nil → /readyz 200. healthz is always 200.
	healthy := httptest.NewServer(server.New(stack.Gateway,
		server.WithReadiness(func(context.Context) error { return nil })))
	t.Cleanup(healthy.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := healthy.Client().Get(healthy.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("healthy %s status %d, want 200", path, resp.StatusCode)
		}
	}

	// Unready: probe returns an error → /readyz 503, but /healthz stays 200.
	unready := httptest.NewServer(server.New(stack.Gateway,
		server.WithReadiness(func(context.Context) error { return errors.New("index down") })))
	t.Cleanup(unready.Close)

	resp, err := unready.Client().Get(unready.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unready /readyz status %d, want 503", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !strings.Contains(body["error"], "index down") {
		t.Errorf("unready /readyz body %v, want it to mention the probe error", body)
	}

	// healthz must remain 200 even while unready.
	resp2, err := unready.Client().Get(unready.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("/healthz status %d while unready, want 200 (pure liveness)", resp2.StatusCode)
	}
}

// TestServer_Metrics verifies /metrics emits valid Prometheus exposition and
// that a data request increments the request counter and byte gauges (A6).
func TestServer_Metrics(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()

	// Drive one PUT so the counters are non-zero.
	payload := randomBytes(t, 64*1024)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/objects/m/obj", bytes.NewReader(payload))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()

	resp, err = c.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("/metrics Content-Type %q, want text/plain exposition", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	// Valid exposition: every metric has HELP+TYPE lines.
	for _, name := range []string{
		"blobgw_requests_total", "blobgw_in_flight_requests",
		"blobgw_bytes_put_total", "blobgw_put_total",
	} {
		if !strings.Contains(text, "# HELP "+name) || !strings.Contains(text, "# TYPE "+name) {
			t.Errorf("/metrics missing HELP/TYPE for %s:\n%s", name, text)
		}
	}
	// The PUT we issued must have been counted.
	if !strings.Contains(text, `blobgw_requests_total{method="PUT",status="2xx"}`) {
		t.Errorf("/metrics did not record the PUT request:\n%s", text)
	}
	if strings.Contains(text, "blobgw_bytes_put_total 0\n") {
		t.Errorf("/metrics shows zero bytes_put after a PUT:\n%s", text)
	}
}

// TestServer_FinalizeIntegrity409 proves the A1 integrity gate surfaces over
// HTTP: a mismatched expected hash on finalize returns 409 Conflict and the ref
// is not bound (GET still 404).
func TestServer_FinalizeIntegrity409(t *testing.T) {
	srv, stack := newTestServer(t)
	c := srv.Client()

	// Mint with an expected hash that the staged bytes won't match.
	mintBody, _ := json.Marshal(map[string]any{
		"content_type": "application/octet-stream",
		"content_hash": "00deadbeef",
	})
	resp, err := c.Post(srv.URL+"/v1/staged", "application/json", bytes.NewReader(mintBody))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	var staged gateway.StagedUpload
	_ = json.NewDecoder(resp.Body).Decode(&staged)
	resp.Body.Close()

	stack.Staging.PutStaged(staged.StagingKey, randomBytes(t, 1024))

	finBody, _ := json.Marshal(map[string]string{"ref": staged.Ref})
	resp, err = c.Post(srv.URL+"/v1/finalize", "application/json", bytes.NewReader(finBody))
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("finalize integrity mismatch status %d, want 409", resp.StatusCode)
	}

	// The ref must not be retrievable (not bound to the mismatched content).
	get, err := c.Get(srv.URL + "/v1/objects/" + staged.Ref)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	get.Body.Close()
	if get.StatusCode != http.StatusNotFound {
		t.Errorf("GET after failed finalize status %d, want 404", get.StatusCode)
	}
}

// TestServer_HeadMetadataHeaders proves A4: HEAD emits the full ObjectInfo over
// X-Blobgw-* response headers (domain, created-at, version, pending, user_meta
// JSON) in addition to Content-Type/Content-Length/ETag. User metadata is set
// via the gateway's PutWithMeta (the HTTP PUT path carries none) so the JSON
// header round-trips a real map.
func TestServer_HeadMetadataHeaders(t *testing.T) {
	srv, stack := newTestServer(t)
	c := srv.Client()

	payload := randomBytes(t, 4096)
	meta := map[string]string{"author": "alice", "etag": "deadbeef"}
	info, err := stack.Gateway.PutWithMeta(context.Background(), "meta/obj", "application/pdf", meta, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	resp, err := c.Head(srv.URL + "/v1/objects/meta/obj")
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status %d", resp.StatusCode)
	}

	if got := resp.Header.Get("X-Blobgw-Domain"); got != "ent" {
		t.Errorf("X-Blobgw-Domain %q, want ent", got)
	}
	if got := resp.Header.Get("X-Blobgw-Version"); got != info.Version || got == "" {
		t.Errorf("X-Blobgw-Version %q, want %q", got, info.Version)
	}
	if got := resp.Header.Get("X-Blobgw-Pending"); got != "false" {
		t.Errorf("X-Blobgw-Pending %q, want false", got)
	}
	if got := resp.Header.Get("X-Blobgw-Created-At"); got == "" {
		t.Error("X-Blobgw-Created-At missing")
	}
	var gotMeta map[string]string
	if err := json.Unmarshal([]byte(resp.Header.Get("X-Blobgw-User-Meta")), &gotMeta); err != nil {
		t.Fatalf("X-Blobgw-User-Meta not valid JSON: %v (raw=%q)", err, resp.Header.Get("X-Blobgw-User-Meta"))
	}
	if gotMeta["author"] != "alice" || gotMeta["etag"] != "deadbeef" {
		t.Errorf("X-Blobgw-User-Meta %v, want %v", gotMeta, meta)
	}
}

// TestServer_ListPagination proves A8 keyset paging over HTTP: a limited list
// returns at most `limit` rows plus an X-Blobgw-Next-Cursor header when more
// remain, and following the cursor via ?after= returns the next page. The body
// stays a bare JSON array (backward-compatible).
func TestServer_ListPagination(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()

	const total = 5
	for i := 0; i < total; i++ {
		ref := "page/" + strconv.Itoa(i)
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/objects/"+ref, bytes.NewReader([]byte{byte(i)}))
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("PUT %s: %v", ref, err)
		}
		resp.Body.Close()
	}

	// First page: limit=2 → 2 rows + a next cursor.
	resp, err := c.Get(srv.URL + "/v1/objects?prefix=page/&limit=2")
	if err != nil {
		t.Fatalf("LIST page 1: %v", err)
	}
	var page1 []gateway.ObjectInfo
	_ = json.NewDecoder(resp.Body).Decode(&page1)
	cursor := resp.Header.Get("X-Blobgw-Next-Cursor")
	resp.Body.Close()
	if len(page1) != 2 {
		t.Fatalf("page 1 returned %d rows, want 2", len(page1))
	}
	if cursor == "" {
		t.Fatal("page 1 missing X-Blobgw-Next-Cursor with more rows remaining")
	}

	// Page through the rest with ?after=, collecting all refs exactly once.
	seen := map[string]int{}
	for _, o := range page1 {
		seen[o.Ref]++
	}
	guard := 0
	for cursor != "" {
		guard++
		if guard > total {
			t.Fatal("pagination did not terminate")
		}
		u := srv.URL + "/v1/objects?prefix=page/&limit=2&after=" + url.QueryEscape(cursor)
		resp, err := c.Get(u)
		if err != nil {
			t.Fatalf("LIST after=%s: %v", cursor, err)
		}
		var pg []gateway.ObjectInfo
		_ = json.NewDecoder(resp.Body).Decode(&pg)
		cursor = resp.Header.Get("X-Blobgw-Next-Cursor")
		resp.Body.Close()
		for _, o := range pg {
			seen[o.Ref]++
		}
	}
	if len(seen) != total {
		t.Fatalf("paged %d distinct refs, want %d", len(seen), total)
	}
	for ref, n := range seen {
		if n != 1 {
			t.Errorf("ref %q seen %d times, want once", ref, n)
		}
	}
}

// TestServer_DeletePrefix proves the A8 bulk-delete route: DELETE /v1/objects
// without a prefix is rejected 400; with a prefix it removes every matching
// object and replies {"deleted":N}; unrelated objects survive.
func TestServer_DeletePrefix(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()

	const n = 4
	for i := 0; i < n; i++ {
		ref := "bulk/doc/" + strconv.Itoa(i)
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/objects/"+ref, bytes.NewReader([]byte{byte(i)}))
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("PUT %s: %v", ref, err)
		}
		resp.Body.Close()
	}
	// An unrelated object that must survive.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/objects/bulk/keep", bytes.NewReader([]byte("k")))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("PUT keep: %v", err)
	}
	resp.Body.Close()

	// No prefix → 400.
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/v1/objects", nil)
	resp, err = c.Do(req)
	if err != nil {
		t.Fatalf("DELETE no prefix: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("DELETE without prefix status %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// With prefix → deletes N.
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/v1/objects?prefix="+url.QueryEscape("bulk/doc/"), nil)
	resp, err = c.Do(req)
	if err != nil {
		t.Fatalf("DELETE prefix: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE prefix status %d, want 200", resp.StatusCode)
	}
	var body struct {
		Deleted int `json:"deleted"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if body.Deleted != n {
		t.Fatalf("bulk delete reported %d, want %d", body.Deleted, n)
	}

	// All matching objects gone; the unrelated one survives.
	for i := 0; i < n; i++ {
		ref := "bulk/doc/" + strconv.Itoa(i)
		r, _ := c.Get(srv.URL + "/v1/objects/" + ref)
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s after bulk delete status %d, want 404", ref, r.StatusCode)
		}
		r.Body.Close()
	}
	r, _ := c.Get(srv.URL + "/v1/objects/bulk/keep")
	if r.StatusCode != http.StatusOK {
		t.Errorf("unrelated object removed by bulk delete: status %d", r.StatusCode)
	}
	r.Body.Close()
}

// fakeUsager is a server.DomainUsager stub for the /admin/usage handler test:
// it returns a fixed rollup for a known domain and an error for a sentinel one,
// so the handler test never needs a real index/DB.
type fakeUsager struct {
	stats  server.DomainUsage
	failOn string
}

func (f fakeUsager) DomainUsage(_ context.Context, domain string) (server.DomainUsage, error) {
	if domain == f.failOn {
		return server.DomainUsage{}, errors.New("boom")
	}
	return f.stats, nil
}

// TestServer_AdminUsage exercises GET /admin/usage: the JSON shape + computed
// compression_ratio for a wired usage source, the 400 on a missing domain, and
// the 501 when no usage source is wired.
func TestServer_AdminUsage(t *testing.T) {
	stack, err := gateway.NewLocalStack(t.TempDir(), "ent", 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalStack: %v", err)
	}

	// With a wired usager: physical=200, logical=1000 → compression 5.0;
	// object_apparent=3000 (blobgw objects only; mlfs slice refs combined downstream).
	usable := httptest.NewServer(server.New(stack.Gateway,
		server.WithUsage(fakeUsager{stats: server.DomainUsage{PhysicalBytes: 200, LogicalDedupedBytes: 1000, ObjectApparentBytes: 3000}})))
	t.Cleanup(usable.Close)

	resp, err := usable.Client().Get(usable.URL + "/admin/usage?domain=ent")
	if err != nil {
		t.Fatalf("GET usage: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("usage status: %d", resp.StatusCode)
	}
	var body struct {
		Domain              string  `json:"domain"`
		PhysicalBytes       int64   `json:"physical_bytes"`
		LogicalDedupedBytes int64   `json:"logical_deduped_bytes"`
		CompressionRatio    float64 `json:"compression_ratio"`
		ObjectApparentBytes int64   `json:"object_apparent_bytes"`
		ComputedAt          string  `json:"computed_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	resp.Body.Close()
	if body.Domain != "ent" || body.PhysicalBytes != 200 || body.LogicalDedupedBytes != 1000 {
		t.Errorf("usage body fields wrong: %+v", body)
	}
	if body.CompressionRatio != 5.0 {
		t.Errorf("compression_ratio = %v, want 5.0", body.CompressionRatio)
	}
	if body.ObjectApparentBytes != 3000 {
		t.Errorf("object_apparent_bytes = %d, want 3000", body.ObjectApparentBytes)
	}
	if _, perr := time.Parse(time.RFC3339, body.ComputedAt); perr != nil {
		t.Errorf("computed_at %q not RFC3339: %v", body.ComputedAt, perr)
	}

	// Missing domain → 400.
	resp, err = usable.Client().Get(usable.URL + "/admin/usage")
	if err != nil {
		t.Fatalf("GET usage no-domain: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("no-domain status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Zero physical → ratio 0.0 (no divide-by-zero).
	zeroSrv := httptest.NewServer(server.New(stack.Gateway,
		server.WithUsage(fakeUsager{stats: server.DomainUsage{PhysicalBytes: 0, LogicalDedupedBytes: 0}})))
	t.Cleanup(zeroSrv.Close)
	resp, _ = zeroSrv.Client().Get(zeroSrv.URL + "/admin/usage?domain=ent")
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if body.CompressionRatio != 0.0 {
		t.Errorf("empty-domain compression_ratio = %v, want 0.0", body.CompressionRatio)
	}

	// Usage source error → 500.
	errSrv := httptest.NewServer(server.New(stack.Gateway,
		server.WithUsage(fakeUsager{failOn: "ent"})))
	t.Cleanup(errSrv.Close)
	resp, err = errSrv.Client().Get(errSrv.URL + "/admin/usage?domain=ent")
	if err != nil {
		t.Fatalf("GET usage 500: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("usage error status = %d, want 500", resp.StatusCode)
	}
	resp.Body.Close()

	// No usage source wired → 501.
	noUsage := httptest.NewServer(server.New(stack.Gateway))
	t.Cleanup(noUsage.Close)
	resp, err = noUsage.Client().Get(noUsage.URL + "/admin/usage?domain=ent")
	if err != nil {
		t.Fatalf("GET usage 501: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("no-usage-source status = %d, want 501", resp.StatusCode)
	}
	resp.Body.Close()
}

// fakeBackfiller is a server.PackBackfiller stub for the /admin/backfill-packs
// handler test: it captures the requested domain and returns a fixed count, or an
// error for a sentinel domain, so the handler test never needs a real index/DB.
type fakeBackfiller struct {
	recorded  int
	failOn    string
	gotDomain string
	called    bool
}

func (f *fakeBackfiller) BackfillPacks(_ context.Context, domain string) (int, error) {
	f.called = true
	f.gotDomain = domain
	if f.failOn != "" && domain == f.failOn {
		return 0, errors.New("boom")
	}
	return f.recorded, nil
}

// TestServer_AdminBackfillPacks exercises POST /admin/backfill-packs: 200 with
// {domain,recorded} for a wired backfiller (including the all-domains empty
// domain), 500 on a backfill error, and 501 when no backfiller is wired.
func TestServer_AdminBackfillPacks(t *testing.T) {
	stack, err := gateway.NewLocalStack(t.TempDir(), "ent", 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalStack: %v", err)
	}

	// Wired backfiller, explicit domain → 200 {domain,recorded}.
	bf := &fakeBackfiller{recorded: 7}
	srv := httptest.NewServer(server.New(stack.Gateway, server.WithBackfill(bf)))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/admin/backfill-packs?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST backfill: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("backfill status: %d", resp.StatusCode)
	}
	var body struct {
		Domain   string `json:"domain"`
		Recorded int    `json:"recorded"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode backfill: %v", err)
	}
	resp.Body.Close()
	if body.Domain != "ent" || body.Recorded != 7 {
		t.Errorf("backfill body wrong: %+v", body)
	}
	if bf.gotDomain != "ent" {
		t.Errorf("backfiller got domain %q, want ent", bf.gotDomain)
	}

	// Empty domain (all domains) → 200, domain passed through empty.
	bf.gotDomain = "sentinel"
	resp, err = srv.Client().Post(srv.URL+"/admin/backfill-packs", "", nil)
	if err != nil {
		t.Fatalf("POST backfill all: %v", err)
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("all-domains status = %d, want 200", resp.StatusCode)
	}
	if bf.gotDomain != "" {
		t.Errorf("all-domains backfiller got domain %q, want empty", bf.gotDomain)
	}

	// Backfill error → 500.
	errSrv := httptest.NewServer(server.New(stack.Gateway, server.WithBackfill(&fakeBackfiller{failOn: "ent"})))
	t.Cleanup(errSrv.Close)
	resp, err = errSrv.Client().Post(errSrv.URL+"/admin/backfill-packs?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST backfill 500: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("backfill error status = %d, want 500", resp.StatusCode)
	}
	resp.Body.Close()

	// No backfiller wired → 501.
	noBF := httptest.NewServer(server.New(stack.Gateway))
	t.Cleanup(noBF.Close)
	resp, err = noBF.Client().Post(noBF.URL+"/admin/backfill-packs?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST backfill 501: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("no-backfiller status = %d, want 501", resp.StatusCode)
	}
	resp.Body.Close()
}

// fakeTenantGCer is a server.TenantGCer stub for the /admin/gc handler test: it
// captures the requested domain and returns a fixed summary, or an error for a
// sentinel domain, so the handler test never needs a real router/backend/DB.
type fakeTenantGCer struct {
	summary   server.GCSummary
	failOn    string
	gotDomain string
	called    bool
}

func (f *fakeTenantGCer) RunGCForDomain(_ context.Context, domain string) (server.GCSummary, error) {
	f.called = true
	f.gotDomain = domain
	if f.failOn != "" && domain == f.failOn {
		return server.GCSummary{}, errors.New("boom")
	}
	return f.summary, nil
}

// TestServer_AdminGC exercises POST /admin/gc: 200 with the {domain,live_chunks,
// chunks_reclaimed,bytes_reclaimed} summary for a wired GCer, 400 on a missing
// domain (GC is scoped to one tenant), 500 on a GC error, and 501 when no
// per-tenant GC source is wired.
func TestServer_AdminGC(t *testing.T) {
	stack, err := gateway.NewLocalStack(t.TempDir(), "ent", 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalStack: %v", err)
	}

	// Wired GCer, explicit domain → 200 with the summary.
	gcer := &fakeTenantGCer{summary: server.GCSummary{LiveChunks: 12, ChunksReclaimed: 3, BytesReclaimed: 4096}}
	srv := httptest.NewServer(server.New(stack.Gateway, server.WithTenantGC(gcer)))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/admin/gc?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST gc: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gc status: %d", resp.StatusCode)
	}
	var body struct {
		Domain          string `json:"domain"`
		LiveChunks      int    `json:"live_chunks"`
		ChunksReclaimed int    `json:"chunks_reclaimed"`
		BytesReclaimed  int64  `json:"bytes_reclaimed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode gc: %v", err)
	}
	resp.Body.Close()
	if body.Domain != "ent" || body.LiveChunks != 12 || body.ChunksReclaimed != 3 || body.BytesReclaimed != 4096 {
		t.Errorf("gc body wrong: %+v", body)
	}
	if gcer.gotDomain != "ent" {
		t.Errorf("gcer got domain %q, want ent", gcer.gotDomain)
	}

	// Missing domain → 400 (GC must be scoped to one tenant), GCer not called.
	gcer.called = false
	resp, err = srv.Client().Post(srv.URL+"/admin/gc", "", nil)
	if err != nil {
		t.Fatalf("POST gc no-domain: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("no-domain gc status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
	if gcer.called {
		t.Error("GCer was called for a missing domain; want 400 before dispatch")
	}

	// GC error → 500.
	errSrv := httptest.NewServer(server.New(stack.Gateway, server.WithTenantGC(&fakeTenantGCer{failOn: "ent"})))
	t.Cleanup(errSrv.Close)
	resp, err = errSrv.Client().Post(errSrv.URL+"/admin/gc?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST gc 500: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("gc error status = %d, want 500", resp.StatusCode)
	}
	resp.Body.Close()

	// No per-tenant GC source wired → 501.
	noGC := httptest.NewServer(server.New(stack.Gateway))
	t.Cleanup(noGC.Close)
	resp, err = noGC.Client().Post(noGC.URL+"/admin/gc?domain=ent", "", nil)
	if err != nil {
		t.Fatalf("POST gc 501: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("no-gc-source status = %d, want 501", resp.StatusCode)
	}
	resp.Body.Close()
}
