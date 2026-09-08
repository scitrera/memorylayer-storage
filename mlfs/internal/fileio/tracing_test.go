// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestReadColdSpan proves the data-path child span: a Read opens an
// mlfs.read.cold span around the manifest-resolve + chunk-fetch loop, and that
// span is a CHILD of the caller's span (the FUSE op span in production) — the
// nesting that makes a slow cold read decompose into manifest(PG) + pack(S3) and
// that attaches exemplars to the latency histograms. PG-gated (skips without
// MLFS_TEST_DATABASE_URL) because the data path needs the metadata engine.
func TestReadColdSpan(t *testing.T) {
	// Install an in-memory recorder as the global provider (the fileio tracer is
	// the global otel.Tracer), restoring the prior provider after the test.
	prev := otel.GetTracerProvider()
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	const mib = 1 << 20
	regions := [][]byte{make([]byte, mib)}
	for i := range regions[0] {
		regions[0][i] = byte(i)
	}
	stack, ino := newReadaheadStack(t, 0, 0, regions) // no readahead, no delay

	// Open a parent span to stand in for the bridge's FUSE op span, so we can
	// assert mlfs.read.cold nests under it.
	parentTracer := otel.Tracer("test/parent")
	ctx, parent := parentTracer.Start(context.Background(), "test.parent")

	buf := make([]byte, mib)
	if _, err := stack.f.Read(ctx, ino, 0, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	parent.End()

	var cold sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "mlfs.read.cold" {
			cold = s
		}
	}
	if cold == nil {
		t.Fatalf("no mlfs.read.cold span recorded; got %d spans", len(sr.Ended()))
	}
	if cold.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("mlfs.read.cold parent = %v, want the caller span %v",
			cold.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if cold.Status().Code == codes.Error {
		t.Errorf("cold span status = Error on a clean read: %v", cold.Status())
	}
}
