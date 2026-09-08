// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	obsmetrics "github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// TestRecordIntegrity_BumpsCanaryOnFault verifies the GET read path's integrity
// classification: a casstore ErrMissingBlob / ErrCorrupt error bumps
// blobgw.integrity.errors with the right error.kind, while an ordinary error does
// not (it is not an integrity fault).
func TestRecordIntegrity_BumpsCanaryOnFault(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	dp, err := obsmetrics.NewDataPath(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}
	s := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), dp: dp}

	r := httptest.NewRequest("GET", "/v1/objects/x", nil)

	// A missing referenced blob (the #50 GC over-deletion canary) → not_found.
	s.recordIntegrity(r, fmt.Errorf("blobgw get %q: %w", "x", snapshot.ErrMissingBlob))
	// A corrupt-bytes fault → corrupt.
	s.recordIntegrity(r, fmt.Errorf("read pack: %w", snapshot.ErrCorrupt))
	// An ordinary error is NOT an integrity fault → ignored.
	s.recordIntegrity(r, io.EOF)

	rm := collectInt(t, reader)
	if got := sumCounterInt(rm, "blobgw.integrity.errors", map[string]string{"error.kind": "not_found"}); got != 1 {
		t.Errorf("integrity{not_found} = %d, want 1", got)
	}
	if got := sumCounterInt(rm, "blobgw.integrity.errors", map[string]string{"error.kind": "corrupt"}); got != 1 {
		t.Errorf("integrity{corrupt} = %d, want 1", got)
	}
	// No "other"/EOF series — io.EOF is not an integrity fault.
	if got := sumCounterInt(rm, "blobgw.integrity.errors", map[string]string{"error.kind": "other"}); got != 0 {
		t.Errorf("integrity{other} = %d, want 0", got)
	}
}

func collectInt(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func sumCounterInt(rm metricdata.ResourceMetrics, name string, match map[string]string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					ok := true
					for k, v := range match {
						got, has := dp.Attributes.Value(attribute.Key(k))
						if !has || got.AsString() != v {
							ok = false
							break
						}
					}
					if ok {
						total += dp.Value
					}
				}
			}
		}
	}
	return total
}
