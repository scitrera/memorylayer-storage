// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// withRecorder installs an in-memory span recorder as the global TracerProvider
// for the duration of a test and returns it, restoring the prior provider on
// cleanup. The bridge opens spans via the GLOBAL otel.Tracer (so it is a no-op
// in production until main installs a real provider), so a test exercises that
// exact path by swapping the global provider for a recorder — no kernel mount,
// no PG.
func withRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return sr
}

// TestFuseSpanRecorded proves the bridge's FUSE-op span seam: startFuseSpan opens
// an mlfs.fuse.<op> Internal span carrying the (high-cardinality) inode attribute
// and threads it into the returned ctx, and endFuseSpan ends it Unset on OK. This
// exercises the same path Read/Write/Lookup/... use without needing the metadata
// DB (the full Read span is covered by a PG-gated fileio test).
func TestFuseSpanRecorded(t *testing.T) {
	sr := withRecorder(t)
	b := New(nil, nil, nil) // tracer is set by New; meta/files unused on this seam

	st := fuse.OK
	ctx, span := b.startFuseSpan(context.Background(), "read", attribute.Int64("mlfs.inode", 42))
	// The span ctx must carry the span so downstream calls nest under it; assert it
	// is the recorded span (the nesting+exemplar contract).
	if sc := span.SpanContext(); sc.SpanID() != trace.SpanContextFromContext(ctx).SpanID() {
		t.Fatalf("span ctx not threaded: span %v vs ctx %v",
			sc.SpanID(), trace.SpanContextFromContext(ctx).SpanID())
	}
	endFuseSpan(span, &st)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "mlfs.fuse.read" {
		t.Errorf("span name = %q, want mlfs.fuse.read", s.Name())
	}
	if s.SpanKind() != trace.SpanKindInternal {
		t.Errorf("span kind = %v, want Internal", s.SpanKind())
	}
	var sawInode bool
	for _, a := range s.Attributes() {
		if a.Key == "mlfs.inode" && a.Value.AsInt64() == 42 {
			sawInode = true
		}
	}
	if !sawInode {
		t.Errorf("span missing mlfs.inode=42 attribute: %v", s.Attributes())
	}
	if got := s.Status().Code; got != codes.Unset {
		t.Errorf("status on OK = %v, want Unset", got)
	}
}

// TestFuseSpanErrorStatus proves endFuseSpan stamps Error on a real failure but
// leaves ENOENT (expected control flow, a lookup miss) Unset — matching the
// recordFuse outcome=ok mapping and the not_found-is-not-an-error convention.
func TestFuseSpanErrorStatus(t *testing.T) {
	sr := withRecorder(t)
	b := New(nil, nil, nil)

	enoent := fuse.ENOENT
	_, span := b.startFuseSpan(context.Background(), "lookup")
	endFuseSpan(span, &enoent)

	eio := fuse.EIO
	_, span2 := b.startFuseSpan(context.Background(), "lookup")
	endFuseSpan(span2, &eio)

	spans := sr.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	if c := spans[0].Status().Code; c != codes.Unset {
		t.Errorf("ENOENT status = %v, want Unset", c)
	}
	if c := spans[1].Status().Code; c != codes.Error {
		t.Errorf("EIO status = %v, want Error", c)
	}
}
