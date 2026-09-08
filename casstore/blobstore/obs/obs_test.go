// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package obs

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// fakeStorage implements just the blobstore.Storage methods the wrapper instruments
// (the embedded nil interface panics if any other method is called — none are here).
type fakeStorage struct {
	blobstore.Storage
	getWrite int   // bytes to append to the output buffer on GetBlob
	getErr   error // error GetBlob returns
	putErr   error // error PutBlob returns
}

func (f *fakeStorage) GetBlob(_ context.Context, _ blobstore.ID, _, _ int64, out blobstore.OutputBuffer) error {
	if f.getWrite > 0 {
		_, _ = out.(*blobstore.OutputBufferImpl).Write(make([]byte, f.getWrite))
	}
	return f.getErr
}
func (f *fakeStorage) PutBlob(_ context.Context, _ blobstore.ID, _ blobstore.Bytes, _ blobstore.PutOptions) error {
	return f.putErr
}

func collect(t *testing.T, r *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func sumCounter(rm metricdata.ResourceMetrics, name string, match map[string]string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if attrsMatch(dp.Attributes, match) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func histCount(rm metricdata.ResourceMetrics, name string, match map[string]string) uint64 {
	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				if attrsMatch(dp.Attributes, match) {
					total += dp.Count
				}
			}
		}
	}
	return total
}

func attrsMatch(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}

func TestWrap_RecordsLatencyBytesAndErrors(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("test")

	fake := &fakeStorage{getWrite: 1024, putErr: errors.New("ExpiredToken: the security token expired")}
	st, err := Wrap(fake, meter, "s3", attribute.String("tenant", "t1"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	ctx := context.Background()
	out := blobstore.NewOutputBuffer()
	if err := st.GetBlob(ctx, "blob-1", 0, -1, out); err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if err := st.PutBlob(ctx, "blob-2", blobstore.BytesFromSlice([]byte("x")), blobstore.PutOptions{}); err == nil {
		t.Fatal("PutBlob: expected error")
	}

	rm := collect(t, reader)

	// read.bytes counts the GetBlob output delta, tagged backend+tenant+operation.
	if got := sumCounter(rm, "casstore.backing.read.bytes", map[string]string{
		"backend": "s3", "tenant": "t1", "operation": "get",
	}); got != 1024 {
		t.Errorf("read.bytes = %d, want 1024", got)
	}

	// op.duration recorded one ok GET and one errored PUT.
	if got := histCount(rm, "casstore.backing.op.duration", map[string]string{"operation": "get", "outcome": "ok"}); got != 1 {
		t.Errorf("duration{get,ok} count = %d, want 1", got)
	}
	if got := histCount(rm, "casstore.backing.op.duration", map[string]string{"operation": "put", "outcome": "error"}); got != 1 {
		t.Errorf("duration{put,error} count = %d, want 1", got)
	}

	// The credential-expiry PutBlob error is classified auth (the prod GC failure).
	if got := sumCounter(rm, "casstore.backing.errors", map[string]string{
		"operation": "put", "error.kind": "auth",
	}); got != 1 {
		t.Errorf("errors{put,auth} = %d, want 1", got)
	}
}

func TestWrap_EmitsSpans(t *testing.T) {
	// obs.Wrap captures the tracer from the GLOBAL provider at Wrap time, so the
	// recorder must be installed first.
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))

	reader := metric.NewManualReader()
	meter := metric.NewMeterProvider(metric.WithReader(reader)).Meter("test")
	st, err := Wrap(&fakeStorage{getWrite: 8, putErr: errors.New("boom")}, meter, "s3")
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	ctx := context.Background()
	_ = st.GetBlob(ctx, "blob-get", 0, -1, blobstore.NewOutputBuffer())
	_ = st.PutBlob(ctx, "blob-put", blobstore.BytesFromSlice([]byte("x")), blobstore.PutOptions{})

	spans := sr.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}
	get, ok := byName["casstore.backing.get"]
	if !ok {
		t.Fatalf("no casstore.backing.get span; got %v", names(spans))
	}
	if !hasAttr(get, "blob.id", "blob-get") || !hasAttr(get, "operation", "get") {
		t.Errorf("get span missing expected attributes: %v", get.Attributes())
	}
	put, ok := byName["casstore.backing.put"]
	if !ok {
		t.Fatalf("no casstore.backing.put span")
	}
	if put.Status().Code != codes.Error {
		t.Errorf("put span status = %v, want Error", put.Status().Code)
	}
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}

func hasAttr(s sdktrace.ReadOnlySpan, k, v string) bool {
	for _, a := range s.Attributes() {
		if string(a.Key) == k && a.Value.AsString() == v {
			return true
		}
	}
	return false
}

func TestErrorKind(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"nil":            {nil, "ok"},
		"not_found":      {blobstore.ErrBlobNotFound, "not_found"},
		"already_exists": {blobstore.ErrBlobAlreadyExists, "already_exists"},
		"canceled":       {context.Canceled, "canceled"},
		"timeout":        {context.DeadlineExceeded, "timeout"},
		"auth":           {errors.New("ExpiredToken: token expired"), "auth"},
		"range":          {errors.New("InvalidRange: bad"), "range"},
		"other":          {errors.New("boom"), "other"},
	}
	for name, tc := range cases {
		if got := errorKind(tc.err); got != tc.want {
			t.Errorf("%s: errorKind = %q, want %q", name, got, tc.want)
		}
	}
}
