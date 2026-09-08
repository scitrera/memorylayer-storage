// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// --- collection helpers (mirror casstore/blobstore/obs/obs_test.go) ---

func collect(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
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

func newTestDataPath(t *testing.T) (*DataPath, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	dp, err := NewDataPath(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}
	return dp, reader
}

func TestDataPath_PG_RecordsDurationAndErrorKind(t *testing.T) {
	dp, reader := newTestDataPath(t)
	ctx := context.Background()

	// One ok lookup, one failed lookup whose error classifies as not_found.
	_ = dp.RecordPG(ctx, "lookup", time.Now(), nil)
	_ = dp.RecordPG(ctx, "lookup", time.Now(), errors.New("pgindex: lookup: sql: no rows in result set"))
	// One auth failure on a record op (the STS "token has expired" prod class).
	_ = dp.RecordPG(ctx, "record", time.Now(), errors.New("ExpiredToken: the security token included in the request has expired"))

	rm := collect(t, reader)

	if got := histCount(rm, "blobgw.pg.op.duration", map[string]string{"operation": "lookup", "outcome": "ok"}); got != 1 {
		t.Errorf("pg duration{lookup,ok} = %d, want 1", got)
	}
	if got := histCount(rm, "blobgw.pg.op.duration", map[string]string{"operation": "lookup", "outcome": "error"}); got != 1 {
		t.Errorf("pg duration{lookup,error} = %d, want 1", got)
	}
	if got := sumCounter(rm, "blobgw.pg.errors", map[string]string{"operation": "lookup", "error.kind": "not_found"}); got != 1 {
		t.Errorf("pg errors{lookup,not_found} = %d, want 1", got)
	}
	if got := sumCounter(rm, "blobgw.pg.errors", map[string]string{"operation": "record", "error.kind": "auth"}); got != 1 {
		t.Errorf("pg errors{record,auth} = %d, want 1", got)
	}
}

func TestDataPath_Integrity_BumpsOnlyOnFault(t *testing.T) {
	dp, reader := newTestDataPath(t)
	ctx := context.Background()

	dp.IntegrityError(ctx, "corrupt")
	dp.IntegrityError(ctx, "not_found")
	dp.IntegrityError(ctx, "") // empty = no fault, must be ignored

	rm := collect(t, reader)
	if got := sumCounter(rm, "blobgw.integrity.errors", map[string]string{"error.kind": "corrupt"}); got != 1 {
		t.Errorf("integrity{corrupt} = %d, want 1", got)
	}
	if got := sumCounter(rm, "blobgw.integrity.errors", map[string]string{"error.kind": "not_found"}); got != 1 {
		t.Errorf("integrity{not_found} = %d, want 1", got)
	}
	// The empty-kind call must not have created any series.
	if got := sumCounter(rm, "blobgw.integrity.errors", map[string]string{"error.kind": ""}); got != 0 {
		t.Errorf("integrity{\"\"} = %d, want 0", got)
	}
}

func TestDataPath_HTTP_OutcomeFromStatus(t *testing.T) {
	dp, reader := newTestDataPath(t)
	ctx := context.Background()

	dp.RecordHTTP(ctx, "get", time.Now(), true)  // ok
	dp.RecordHTTP(ctx, "get", time.Now(), false) // error
	dp.AddBytesGet(ctx, 2048)
	dp.AddBytesPut(ctx, 0) // ignored (non-positive)

	rm := collect(t, reader)
	if got := histCount(rm, "blobgw.http.request.duration", map[string]string{"operation": "get", "outcome": "ok"}); got != 1 {
		t.Errorf("http duration{get,ok} = %d, want 1", got)
	}
	if got := histCount(rm, "blobgw.http.request.duration", map[string]string{"operation": "get", "outcome": "error"}); got != 1 {
		t.Errorf("http duration{get,error} = %d, want 1", got)
	}
	if got := sumCounter(rm, "blobgw.http.bytes_get", nil); got != 2048 {
		t.Errorf("http bytes_get = %d, want 2048", got)
	}
	if got := sumCounter(rm, "blobgw.http.bytes_put", nil); got != 0 {
		t.Errorf("http bytes_put = %d, want 0 (non-positive ignored)", got)
	}
}

func TestDataPath_NilSafe(t *testing.T) {
	var dp *DataPath // disabled-metrics call sites hold a nil *DataPath
	ctx := context.Background()
	// None of these may panic.
	_ = dp.RecordPG(ctx, "lookup", time.Now(), nil)
	dp.RecordCP(ctx, "lookup", time.Now(), nil)
	dp.RecordHTTP(ctx, "get", time.Now(), true)
	dp.AddBytesGet(ctx, 1)
	dp.AddBytesPut(ctx, 1)
	dp.IntegrityError(ctx, "corrupt")
	if err := dp.BindPool(nil, "x", nil); err != nil {
		t.Errorf("BindPool on nil DataPath = %v, want nil", err)
	}
}

func TestErrorKind(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"nil":            {nil, "ok"},
		"canceled":       {context.Canceled, "canceled"},
		"timeout":        {context.DeadlineExceeded, "timeout"},
		"not_found_rows": {errors.New("pgindex: ref get: sql: no rows in result set"), "not_found"},
		"not_found_s3":   {errors.New("NoSuchKey: the key does not exist"), "not_found"},
		"already_exists": {errors.New("duplicate key value violates unique constraint"), "already_exists"},
		"range":          {errors.New("InvalidRange: bad"), "range"},
		"auth":           {errors.New("ExpiredToken: token expired"), "auth"},
		"conflict":       {errors.New("could not serialize access due to concurrent update"), "conflict"},
		"other":          {errors.New("boom"), "other"},
	}
	for name, tc := range cases {
		if got := ErrorKind(tc.err); got != tc.want {
			t.Errorf("%s: ErrorKind = %q, want %q", name, got, tc.want)
		}
	}
}
