// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// obsHarness is the observability twin of harness: it wires an edge Server over a
// TenantRouter whose shared chunk store is obs.Wrap-ed (so casstore.backing.*
// client spans emit exactly as production wires it on the per-tenant S3 path),
// plus a ManualReader for metrics and a span recorder as the GLOBAL tracer
// provider. It mirrors blobgw/server's tracing_test newTracedStack + the
// ManualReader pattern from casstore/blobstore/obs.
type obsHarness struct {
	srv    *httptest.Server
	reader *sdkmetric.ManualReader
	rec    *tracetest.SpanRecorder
}

// newObsHarness builds the traced+metered edge. The wrapped chunk store shares
// meter with the ManualReader; spans go to the recorder installed as the global
// provider. Metrics + tracing are wired explicitly via WithMetrics/WithTracing.
func newObsHarness(t *testing.T) *obsHarness {
	t.Helper()

	// Metrics: a ManualReader-backed meter feeds both the edge instrument set and
	// the casstore backing wrapper, so one Collect() sees both.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("blobgw-edge-test")

	// Traces: install a recording provider as the GLOBAL provider BEFORE building
	// anything, so obs.Wrap (captures otel.Tracer at construction) records to it.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	dir := t.TempDir()
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatalf("manifest store: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		t.Fatalf("chunk store: %v", err)
	}
	wrapped, err := obs.Wrap(chunks, meter, "local")
	if err != nil {
		t.Fatalf("obs.Wrap: %v", err)
	}
	router := gateway.NewTenantRouter(upstream, wrapped,
		snapshot.NewMemoryDedupStore(), gateway.NewMemoryRefStore(), gateway.NewMemoryStagingStore(), 1*1024*1024)

	pub, priv, _ := capability.GenerateKey()
	minter := capability.NewMinter("k1", priv, audience)
	ver := capability.NewVerifier(audience)
	ver.AddKey("k1", pub)

	em, err := edge.NewMetrics(meter)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	s := edge.New(router, minter, ver, edge.Deps{Identity: edge.HeaderIdentityProvider{}},
		edge.Config{Audience: audience},
		edge.WithMetrics(em),
		edge.WithTracing(tp.Tracer("edge-test"), propagation.TraceContext{}))
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &obsHarness{srv: srv, reader: reader, rec: rec}
}

func (h *obsHarness) mintToken(t *testing.T, tenant, subject, op, ref string) (string, int) {
	t.Helper()
	body := []byte(`{"op":"` + op + `","ref":"` + ref + `","ttl_seconds":600}`)
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
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("mint decode: %v", err)
	}
	return out.Token, resp.StatusCode
}

// collectMetrics gathers the current metric snapshot from the ManualReader.
func (h *obsHarness) collectMetrics(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

// findHistogram returns the sum of counts across all data points of the named
// Float64 histogram, or -1 if the instrument is absent.
func findHistogram(rm metricdata.ResourceMetrics, name string) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
				var total int64
				for _, dp := range hist.DataPoints {
					total += int64(dp.Count)
				}
				return total
			}
		}
	}
	return -1
}

// findCounter returns the summed value of the named Int64 sum instrument, or -1
// if absent.
func findCounter(rm metricdata.ResourceMetrics, name string) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				var total int64
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
				return total
			}
		}
	}
	return -1
}

// TestObs_RequestDurationRecordedAndSpanNesting drives a PUT then GET through the
// edge and asserts (a) edge.request.duration recorded both requests, (b) an
// edge.get server span carries the bounded operation/tenant attributes, and (c) a
// casstore.backing.* span opened during the GET nests under it — proving the span
// ctx threads from the edge parent span into casstore (so exemplars link).
func TestObs_RequestDurationRecordedAndSpanNesting(t *testing.T) {
	h := newObsHarness(t)
	payload := []byte("hello edge observability")

	putTok, st := h.mintToken(t, "acme", "alice", "PUT", "docs/o.bin")
	if st != http.StatusOK {
		t.Fatalf("mint PUT: %d", st)
	}
	putReq, _ := http.NewRequest(http.MethodPut, h.srv.URL+"/blob/docs/o.bin?cap="+putTok, bytes.NewReader(payload))
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := h.srv.Client().Do(putReq)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	getTok, _ := h.mintToken(t, "acme", "alice", "GET", "docs/o.bin")
	getResp, err := h.srv.Client().Get(h.srv.URL + "/blob/docs/o.bin?cap=" + getTok)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	got, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("GET body mismatch")
	}

	// (a) edge.request.duration recorded the PUT + the GET.
	rm := h.collectMetrics(t)
	if n := findHistogram(rm, "edge.request.duration"); n < 2 {
		t.Errorf("edge.request.duration count = %d, want >= 2 (put+get)", n)
	}
	// bytes counters moved on both paths.
	if n := findCounter(rm, "edge.bytes_put"); n != int64(len(payload)) {
		t.Errorf("edge.bytes_put = %d, want %d", n, len(payload))
	}
	if n := findCounter(rm, "edge.bytes_get"); n != int64(len(payload)) {
		t.Errorf("edge.bytes_get = %d, want %d", n, len(payload))
	}

	// (b) the edge.get server span.
	spans := h.rec.Ended()
	var edgeGet sdktrace.ReadOnlySpan
	for _, sp := range spans {
		if sp.Name() == "edge.get" {
			edgeGet = sp
			break
		}
	}
	if edgeGet == nil {
		t.Fatalf("no edge.get span; got %v", obsSpanNames(spans))
	}
	if edgeGet.SpanKind() != trace.SpanKindServer {
		t.Errorf("edge.get kind = %v, want Server", edgeGet.SpanKind())
	}
	if got := obsAttrString(edgeGet, "operation"); got != "get" {
		t.Errorf("operation attr = %q, want get", got)
	}
	if got := obsAttrString(edgeGet, "tenant"); got != "acme" {
		t.Errorf("tenant attr = %q, want acme", got)
	}
	if got := obsAttrString(edgeGet, "object.ref"); got != "docs/o.bin" {
		t.Errorf("object.ref attr = %q, want docs/o.bin", got)
	}

	// (c) a casstore backing span opened during the GET nests under edge.get.
	var nested bool
	for _, sp := range spans {
		if sp.Name() == "casstore.backing.get" &&
			sp.Parent().SpanID() == edgeGet.SpanContext().SpanID() &&
			sp.Parent().TraceID() == edgeGet.SpanContext().TraceID() {
			nested = true
			break
		}
	}
	if !nested {
		t.Errorf("no casstore.backing.get span nested under edge.get; spans=%v", obsSpanNames(spans))
	}
}

// TestObs_MintRejectedCounter asserts a rejected mint (invalid op) bumps
// edge.mint.rejected {reason} and records the mint duration with outcome=error,
// while a successful mint records edge.mint.duration and an edge.mint span.
func TestObs_MintRejectedCounter(t *testing.T) {
	h := newObsHarness(t)

	// A malformed op is rejected before signing.
	if _, st := h.mintToken(t, "acme", "alice", "FROB", "docs/o.bin"); st != http.StatusBadRequest {
		t.Fatalf("mint bad op status = %d, want 400", st)
	}
	// A valid mint succeeds.
	if _, st := h.mintToken(t, "acme", "alice", "GET", "docs/o.bin"); st != http.StatusOK {
		t.Fatalf("mint valid status = %d, want 200", st)
	}

	rm := h.collectMetrics(t)
	if n := findCounter(rm, "edge.mint.rejected"); n != 1 {
		t.Errorf("edge.mint.rejected = %d, want 1", n)
	}
	if n := findHistogram(rm, "edge.mint.duration"); n < 2 {
		t.Errorf("edge.mint.duration count = %d, want >= 2 (reject+ok)", n)
	}

	// Both mints produced an edge.mint server span.
	var mintSpans int
	for _, sp := range h.rec.Ended() {
		if sp.Name() == "edge.mint" {
			mintSpans++
		}
	}
	if mintSpans < 2 {
		t.Errorf("edge.mint spans = %d, want >= 2", mintSpans)
	}
}

// TestObs_VerifyFailureCounter asserts a data-path request with a garbage token
// bumps the edge.capability.verify_failures auth canary and returns 401.
func TestObs_VerifyFailureCounter(t *testing.T) {
	h := newObsHarness(t)

	resp, err := h.srv.Client().Get(h.srv.URL + "/blob/docs/o.bin?cap=not-a-real-token")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET status = %d, want 401", resp.StatusCode)
	}

	rm := h.collectMetrics(t)
	if n := findCounter(rm, "edge.capability.verify_failures"); n != 1 {
		t.Errorf("edge.capability.verify_failures = %d, want 1", n)
	}
}

func obsSpanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, sp := range spans {
		out[i] = sp.Name()
	}
	return out
}

func obsAttrString(sp sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range sp.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}
