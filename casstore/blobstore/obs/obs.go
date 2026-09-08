// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package obs is the opt-in OpenTelemetry adapter for blobstore.Storage: Wrap
// decorates a backing store so every I/O operation records a duration histogram,
// an error counter, and read/write byte counters — the backing-store RED view that
// is otherwise dark (an object gateway's latency and failures are dominated by S3).
//
// It lives in a sibling package, NOT in blobstore/casstore core, so casstore keeps
// holding no telemetry dependency (the same principle as snapshot.GCMetrics): the
// caller owns the meter provider and opts in by wrapping at construction. Both
// blobgw and mlfs wrap their S3 backing store with the same adapter, so the backing
// view is identical across layers. See docs/OBSERVABILITY.md for the conventions.
package obs

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// ioDurationBuckets (seconds) span a sub-millisecond local/cached op to a slow,
// multi-second S3 round trip. Finer than the GC/compaction buckets because backing
// IO latency is the thing operators tune against. See docs/OBSERVABILITY.md.
var ioDurationBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// instrumentedStorage wraps a blobstore.Storage, instrumenting the five I/O methods
// and passing every other method (Volume/Reader extras, Close, FlushCaches, …)
// straight through via the embedded interface.
type instrumentedStorage struct {
	blobstore.Storage // embedded: passthrough for un-instrumented methods

	base         []attribute.KeyValue // backend + caller extras, on every measurement
	now          func() time.Time
	tracer       trace.Tracer
	opDuration   metric.Float64Histogram
	opErrors     metric.Int64Counter
	bytesRead    metric.Int64Counter
	bytesWritten metric.Int64Counter
}

// Wrap returns a blobstore.Storage that records OTEL metrics for each backing-store
// operation. backend identifies the dependency ("s3" / "local"); extra static
// attributes (e.g. a tenant or domain) are attached to every measurement. meter is
// the caller's OTEL meter (the caller owns the provider). Returns an error only if
// instrument construction fails.
//
// Instruments (all tagged {backend, operation[, outcome|error.kind], +extra}):
//   - casstore.backing.op.duration  histogram (s)   {operation, outcome}
//   - casstore.backing.errors       counter         {operation, error.kind}
//   - casstore.backing.read.bytes   counter (By)    {operation}
//   - casstore.backing.write.bytes  counter (By)    {operation}
func Wrap(inner blobstore.Storage, meter metric.Meter, backend string, extra ...attribute.KeyValue) (blobstore.Storage, error) {
	if inner == nil {
		return nil, errors.New("blobstore/obs: nil inner storage")
	}
	if meter == nil {
		return nil, errors.New("blobstore/obs: nil meter")
	}
	base := make([]attribute.KeyValue, 0, 1+len(extra))
	base = append(base, attribute.String("backend", backend))
	base = append(base, extra...)

	var ierr error
	dur, err := meter.Float64Histogram("casstore.backing.op.duration",
		metric.WithDescription("Backing-store I/O operation latency"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(ioDurationBuckets...))
	ierr = errors.Join(ierr, err)
	errs, err := meter.Int64Counter("casstore.backing.errors",
		metric.WithDescription("Backing-store I/O operations that failed, by error kind"))
	ierr = errors.Join(ierr, err)
	rd, err := meter.Int64Counter("casstore.backing.read.bytes",
		metric.WithDescription("Bytes read from the backing store"), metric.WithUnit("By"))
	ierr = errors.Join(ierr, err)
	wr, err := meter.Int64Counter("casstore.backing.write.bytes",
		metric.WithDescription("Bytes written to the backing store"), metric.WithUnit("By"))
	ierr = errors.Join(ierr, err)
	if ierr != nil {
		return nil, ierr
	}

	return &instrumentedStorage{
		Storage:      inner,
		base:         base,
		now:          time.Now,
		tracer:       otel.Tracer("github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"),
		opDuration:   dur,
		opErrors:     errs,
		bytesRead:    rd,
		bytesWritten: wr,
	}, nil
}

// begin starts a client span for one backing op and returns the span context (so
// child work and the duration Record below carry the trace, which is what attaches
// exemplars to the histogram) and the span. id is the blob being operated on — a
// fine span attribute (spans are not aggregated, so per-blob cardinality is OK)
// even though it must never be a metric label. When no tracer provider is set the
// span is a cheap no-op. Returned start time pairs with finish.
func (s *instrumentedStorage) begin(ctx context.Context, op string, id blobstore.ID) (context.Context, trace.Span, time.Time) {
	ctx, span := s.tracer.Start(ctx, "casstore.backing."+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(append(s.attrs(), attribute.String("operation", op), attribute.String("blob.id", string(id)))...))
	return ctx, span, s.now()
}

// finish records the duration histogram (always, within the span ctx so a sampled
// trace yields an exemplar) and the error counter (on failure), sets the span
// status, ends the span, and returns err unchanged so call sites stay one-liners.
func (s *instrumentedStorage) finish(ctx context.Context, span trace.Span, op string, start time.Time, err error) error {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	s.opDuration.Record(ctx, s.now().Sub(start).Seconds(),
		metric.WithAttributes(append(s.attrs(), attribute.String("operation", op), attribute.String("outcome", outcome))...))
	if err != nil {
		kind := errorKind(err)
		s.opErrors.Add(ctx, 1,
			metric.WithAttributes(append(s.attrs(), attribute.String("operation", op), attribute.String("error.kind", kind))...))
		span.RecordError(err)
		span.SetStatus(codes.Error, kind)
	}
	span.End()
	return err
}

// attrs returns a fresh copy of the base attributes so per-call appends never alias
// the shared slice.
func (s *instrumentedStorage) attrs() []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(s.base), len(s.base)+2)
	copy(out, s.base)
	return out
}

func (s *instrumentedStorage) GetBlob(ctx context.Context, id blobstore.ID, offset, length int64, output blobstore.OutputBuffer) error {
	ctx, span, start := s.begin(ctx, "get", id)
	before := output.Length()
	err := s.Storage.GetBlob(ctx, id, offset, length, output)
	if n := output.Length() - before; n > 0 {
		s.bytesRead.Add(ctx, int64(n), metric.WithAttributes(append(s.attrs(), attribute.String("operation", "get"))...))
	}
	return s.finish(ctx, span, "get", start, err)
}

func (s *instrumentedStorage) GetMetadata(ctx context.Context, id blobstore.ID) (blobstore.Metadata, error) {
	ctx, span, start := s.begin(ctx, "get_metadata", id)
	md, err := s.Storage.GetMetadata(ctx, id)
	return md, s.finish(ctx, span, "get_metadata", start, err)
}

func (s *instrumentedStorage) PutBlob(ctx context.Context, id blobstore.ID, data blobstore.Bytes, opts blobstore.PutOptions) error {
	ctx, span, start := s.begin(ctx, "put", id)
	err := s.Storage.PutBlob(ctx, id, data, opts)
	if err == nil && data != nil {
		s.bytesWritten.Add(ctx, int64(data.Length()), metric.WithAttributes(append(s.attrs(), attribute.String("operation", "put"))...))
	}
	return s.finish(ctx, span, "put", start, err)
}

func (s *instrumentedStorage) DeleteBlob(ctx context.Context, id blobstore.ID) error {
	ctx, span, start := s.begin(ctx, "delete", id)
	return s.finish(ctx, span, "delete", start, s.Storage.DeleteBlob(ctx, id))
}

func (s *instrumentedStorage) ListBlobs(ctx context.Context, prefix blobstore.ID, cb func(blobstore.Metadata) error) error {
	ctx, span, start := s.begin(ctx, "list", prefix)
	return s.finish(ctx, span, "list", start, s.Storage.ListBlobs(ctx, prefix, cb))
}

// errorKind maps a backing error to the small bounded set in docs/OBSERVABILITY.md.
// Sentinel matches first; the string fallbacks classify the common S3 conditions
// (notably auth/credential expiry — the "token has expired" prod GC failure) that
// have no typed error. not_found/already_exists are well-defined responses, not
// failures — dashboards exclude them from failure alerts.
func errorKind(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, blobstore.ErrBlobNotFound):
		return "not_found"
	case errors.Is(err, blobstore.ErrBlobAlreadyExists):
		return "already_exists"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "InvalidRange") || strings.Contains(s, "invalid range"):
		return "range"
	case strings.Contains(s, "expired") || strings.Contains(s, "AccessDenied") ||
		strings.Contains(s, "ExpiredToken") || strings.Contains(s, "InvalidAccessKeyId"):
		return "auth"
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return "timeout"
	default:
		return "other"
	}
}
