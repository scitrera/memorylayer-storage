// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// TestTracing_CPSpan verifies the control-plane RPC span (B): a record RPC
// produces a blobgw.cp.record SpanKind=Server span via the global tracer the
// server captured at construction.
func TestTracing_CPSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	var recResp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Size: 1}}},
	}, &recResp)
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	var cp sdktrace.ReadOnlySpan
	for _, sp := range rec.Ended() {
		if sp.Name() == "blobgw.cp.record" {
			cp = sp
			break
		}
	}
	if cp == nil {
		t.Fatalf("no blobgw.cp.record span produced")
	}
	if cp.SpanKind() != trace.SpanKindServer {
		t.Errorf("blobgw.cp.record kind = %v, want Server", cp.SpanKind())
	}
}

// TestTracing_CPContextPropagation verifies the cp span continues a caller's
// trace injected into the NATS message headers (best-effort): a record published
// with W3C traceparent headers yields a blobgw.cp.record span on the SAME trace.
func TestTracing_CPContextPropagation(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	// Open an upstream span, inject it into NATS message headers, and publish.
	upCtx, upSpan := tp.Tracer("upstream").Start(context.Background(), "caller")
	data, _ := ctlproto.DefaultCodec.Marshal(ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Size: 1}}},
	})
	msg := nats.NewMsg(ctlproto.IndexRecordSubject(testTenant))
	msg.Data = data
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	// Inject over a nats.Header carrier (the same case-sensitive view the server's
	// extractor uses) — NOT propagation.HeaderCarrier, which canonicalizes keys to
	// Title-Case and so would not match nats.Header's case-sensitive Get on extract.
	otel.GetTextMapPropagator().Inject(upCtx, natsMsgCarrier(msg.Header))
	upSpan.End()

	reply, err := conn.RequestMsg(msg, 3*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var recResp ctlproto.RecordResponse
	if err := ctlproto.DefaultCodec.Unmarshal(reply.Data, &recResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	wantTrace := upSpan.SpanContext().TraceID()
	var found bool
	for _, sp := range rec.Ended() {
		if sp.Name() == "blobgw.cp.record" {
			found = true
			if sp.SpanContext().TraceID() != wantTrace {
				t.Errorf("blobgw.cp.record trace id = %s, want continued %s",
					sp.SpanContext().TraceID(), wantTrace)
			}
			if sp.Parent().SpanID() != upSpan.SpanContext().SpanID() {
				t.Errorf("blobgw.cp.record parent = %s, want upstream %s",
					sp.Parent().SpanID(), upSpan.SpanContext().SpanID())
			}
		}
	}
	if !found {
		t.Fatalf("no blobgw.cp.record span produced")
	}
}

// natsMsgCarrier is a propagation.TextMapCarrier over nats.Header matching the
// server's case-sensitive extraction view, so injected trace-context keys are
// found on extract.
type natsMsgCarrier nats.Header

func (c natsMsgCarrier) Get(key string) string { return nats.Header(c).Get(key) }
func (c natsMsgCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }
func (c natsMsgCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
