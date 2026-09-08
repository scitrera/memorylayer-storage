// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx" for the pool test

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

func TestEmptyLiveSet(t *testing.T) {
	cases := map[string]struct {
		res  snapshot.GCResult
		want bool
	}{
		"empty store (nothing scanned)": {snapshot.GCResult{ChunksScanned: 0, LiveChunks: 0}, false},
		"healthy (live > 0)":            {snapshot.GCResult{ChunksScanned: 10, LiveChunks: 4}, false},
		"disaster (scanned, none live)": {snapshot.GCResult{ChunksScanned: 10, LiveChunks: 0}, true},
	}
	for name, tc := range cases {
		if got := EmptyLiveSet(tc.res); got != tc.want {
			t.Errorf("%s: EmptyLiveSet = %v, want %v", name, got, tc.want)
		}
	}
}

// TestRegistry_RecordGC_EmptyLiveSetCounter checks the #50 canary counter is bumped
// only when a pass scanned chunks yet found none live (via the Prometheus scrape).
func TestRegistry_RecordGC_EmptyLiveSetCounter(t *testing.T) {
	reg, err := New(context.Background(), WithInstance("solo"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Shutdown(context.Background())

	// Healthy pass: must NOT bump the canary.
	reg.RecordGC("healthy", snapshot.GCResult{ChunksScanned: 5, LiveChunks: 5})
	// Disaster pass: scanned but zero live → bump.
	reg.RecordGC("doomed", snapshot.GCResult{ChunksScanned: 9, LiveChunks: 0})

	body := scrape(t, reg)
	if v := lineValue(body, `blobgw_gc_empty_live_set_total{service_instance_id="solo",tenant="doomed"}`); v != 1 {
		t.Errorf("empty_live_set{doomed} = %v, want 1\n%s", v, body)
	}
	if v := lineValue(body, `blobgw_gc_empty_live_set_total{service_instance_id="solo",tenant="healthy"}`); v != -1 {
		t.Errorf("empty_live_set{healthy} present (=%v), want absent\n%s", v, body)
	}
}

// TestDataPath_BindPool_ObservesStats checks the pool gauges surface a bound
// *sql.DB's Stats() under the pool=name label.
func TestDataPath_BindPool_ObservesStats(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")
	dp, err := NewDataPath(meter)
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}

	// A closed pool yields a valid, all-zero Stats() — enough to prove the gauges
	// are registered and the callback runs without a live database.
	db, err := sql.Open("pgx", "postgres://invalid")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(7)

	if err := dp.BindPool(meter, "index", db); err != nil {
		t.Fatalf("BindPool: %v", err)
	}

	rm := collect(t, reader)
	// The gauge must exist for pool=index (value 0 idle/in_use on an unused pool).
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "blobgw.pg.pool.idle" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("blobgw.pg.pool.idle gauge not registered")
	}
}

// --- scrape helpers (scrape() lives in metrics_test.go) ---

// lineValue returns the numeric value of the metric line beginning with series, or
// -1 when absent.
func lineValue(body, series string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(line[len(series):]), 64)
			if err != nil {
				return -1
			}
			return v
		}
	}
	return -1
}
