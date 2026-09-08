// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package server_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	obsmetrics "github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/blobgw/server"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// newTracedStack builds a Gateway whose chunk store is wrapped with blobstore/obs
// (so casstore.backing.* client spans are emitted, exactly as production wires
// it), over the same local backends NewLocalStack uses. The wrap shares meter so
// the obs adapter is happy; spans go to the global tracer provider the test sets.
func newTracedStack(t *testing.T, dir, domain string) *gateway.Gateway {
	t.Helper()
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatalf("manifest store: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		t.Fatalf("chunk store: %v", err)
	}
	meter := sdkmetric.NewMeterProvider().Meter("test")
	wrapped, err := obs.Wrap(chunks, meter, "local")
	if err != nil {
		t.Fatalf("obs.Wrap: %v", err)
	}
	dedup := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, wrapped, snapshot.ChunkedConfig{
		DedupDomain: domain,
		Index:       snapshot.NewGlobalIndex(dedup, nil),
	})
	if err != nil {
		t.Fatalf("chunked store: %v", err)
	}
	return gateway.New(cs, gateway.NewMemoryRefStore(), gateway.NewMemoryStagingStore(), domain)
}

// TestTracing_ServerSpanAndNesting verifies the HTTP server span (B): a GET
// through the server produces a blobgw.http.get SpanKind=Server span carrying the
// bounded operation + object.ref attributes, and the casstore backing span
// (casstore.backing.get, opened via the GLOBAL tracer by blobstore/obs during the
// GET read path) nests under it — proving request context (and thus exemplar
// linkage) flows from the parent span into casstore.
func TestTracing_ServerSpanAndNesting(t *testing.T) {
	// Install a recording tracer provider as the GLOBAL provider BEFORE building
	// the server: server.New + blobstore/obs both capture otel.Tracer() at
	// construction, so the global provider must already be the recorder.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	gw := newTracedStack(t, t.TempDir(), "ent")
	dp, err := obsmetrics.NewDataPath(sdkmetric.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}
	srv := httptest.NewServer(server.New(gw, server.WithMetrics(dp)))
	t.Cleanup(srv.Close)
	c := srv.Client()

	objURL := srv.URL + "/v1/objects/docs/page-1.bin"
	payload := []byte("hello blobgw tracing")

	// PUT then GET — the GET read path pulls from casstore, emitting backing spans.
	putReq, _ := http.NewRequest(http.MethodPut, objURL, bytes.NewReader(payload))
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := c.Do(putReq)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	getResp, err := c.Get(objURL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}

	spans := rec.Ended()

	// Find the blobgw.http.get server span.
	var httpGet sdktrace.ReadOnlySpan
	for _, sp := range spans {
		if sp.Name() == "blobgw.http.get" {
			httpGet = sp
			break
		}
	}
	if httpGet == nil {
		t.Fatalf("no blobgw.http.get span; got %v", spanNames(spans))
	}
	if httpGet.SpanKind() != trace.SpanKindServer {
		t.Errorf("blobgw.http.get kind = %v, want Server", httpGet.SpanKind())
	}
	if got := attrString(httpGet, "operation"); got != "get" {
		t.Errorf("operation attr = %q, want get", got)
	}
	if got := attrString(httpGet, "object.ref"); got != "docs/page-1.bin" {
		t.Errorf("object.ref attr = %q, want docs/page-1.bin", got)
	}

	// A casstore backing span opened during the GET must nest under the http span.
	var nested bool
	for _, sp := range spans {
		if sp.Name() == "casstore.backing.get" &&
			sp.Parent().SpanID() == httpGet.SpanContext().SpanID() &&
			sp.Parent().TraceID() == httpGet.SpanContext().TraceID() {
			nested = true
			break
		}
	}
	if !nested {
		t.Errorf("no casstore.backing.get span nested under blobgw.http.get; spans=%v", spanNames(spans))
	}
}

// TestTracing_ContextPropagation verifies the server continues an upstream
// caller's trace: a GET carrying W3C traceparent headers produces a
// blobgw.http.get span on the SAME trace id as the injected parent.
func TestTracing_ContextPropagation(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	gw := newTracedStack(t, t.TempDir(), "ent")
	srv := httptest.NewServer(server.New(gw))
	t.Cleanup(srv.Close)
	c := srv.Client()

	objURL := srv.URL + "/v1/objects/docs/p.bin"
	putReq, _ := http.NewRequest(http.MethodPut, objURL, bytes.NewReader([]byte("x")))
	putResp, err := c.Do(putReq)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	putResp.Body.Close()

	// Open an upstream span and inject its context into the GET request headers.
	upCtx, upSpan := tp.Tracer("upstream").Start(context.Background(), "caller")
	getReq, _ := http.NewRequest(http.MethodGet, objURL, nil)
	otel.GetTextMapPropagator().Inject(upCtx, propagation.HeaderCarrier(getReq.Header))
	upSpan.End()
	getResp, err := c.Do(getReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	getResp.Body.Close()

	wantTrace := upSpan.SpanContext().TraceID()
	var found bool
	for _, sp := range rec.Ended() {
		if sp.Name() == "blobgw.http.get" {
			found = true
			if sp.SpanContext().TraceID() != wantTrace {
				t.Errorf("blobgw.http.get trace id = %s, want continued %s",
					sp.SpanContext().TraceID(), wantTrace)
			}
			if sp.Parent().SpanID() != upSpan.SpanContext().SpanID() {
				t.Errorf("blobgw.http.get parent = %s, want upstream %s",
					sp.Parent().SpanID(), upSpan.SpanContext().SpanID())
			}
		}
	}
	if !found {
		t.Fatalf("no blobgw.http.get span produced")
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, sp := range spans {
		out[i] = sp.Name()
	}
	return out
}

func attrString(sp sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range sp.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}
