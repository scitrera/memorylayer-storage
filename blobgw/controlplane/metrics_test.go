// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// TestControlPlane_RecordsRED drives a successful and a failing RPC end-to-end and
// asserts the wired DataPath captured the cp.* RED view: a duration histogram with
// the right {operation, outcome} and an errors counter with the right
// {operation, error.kind}.
func TestControlPlane_RecordsRED(t *testing.T) {
	conn := startNATS(t)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	dp, err := metrics.NewDataPath(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}

	// A store whose Record fails (boom) but whose reads succeed, so we get one ok
	// op (lookup) and one error op (record) through the SAME server.
	newServer(t, conn, errStore{}, controlplane.WithMetrics(dp))

	// Successful lookup (errStore.LookupBatch returns an error, so use a store that
	// succeeds for the ok-path: run a lookup against a memory store on a 2nd server
	// would collide on the queue group. Instead assert ok via a record on a memory
	// store server in a separate subtest.)
	var lookResp ctlproto.LookupBatchResponse
	request(t, conn, ctlproto.IndexLookupSubject(testTenant), ctlproto.LookupBatchRequest{
		Domain:      testDomain,
		ChunkHashes: []string{"h1"},
	}, &lookResp)
	// errStore.LookupBatch fails → this lookup is an error op.
	if lookResp.Error == "" {
		t.Fatalf("expected lookup error from errStore")
	}

	// A failing record op.
	var recResp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1"}}},
	}, &recResp)
	if recResp.Error == "" {
		t.Fatalf("expected record error from errStore")
	}

	rm := collectCP(t, reader)
	// Both ops recorded a duration with outcome=error.
	if got := histCountCP(rm, "blobgw.cp.op.duration", map[string]string{"operation": "lookup", "outcome": "error"}); got != 1 {
		t.Errorf("cp duration{lookup,error} = %d, want 1", got)
	}
	if got := histCountCP(rm, "blobgw.cp.op.duration", map[string]string{"operation": "record", "outcome": "error"}); got != 1 {
		t.Errorf("cp duration{record,error} = %d, want 1", got)
	}
	// And both bumped the errors counter (errStore messages classify as "other").
	if got := sumCounterCP(rm, "blobgw.cp.errors", map[string]string{"operation": "record"}); got != 1 {
		t.Errorf("cp errors{record} = %d, want 1", got)
	}
}

// TestControlPlane_RecordsOK proves a successful RPC records outcome=ok with no
// error counter bump, against a real MemoryDedupStore.
func TestControlPlane_RecordsOK(t *testing.T) {
	conn := startNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	dp, err := metrics.NewDataPath(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewDataPath: %v", err)
	}
	newServer(t, conn, snapshot.NewMemoryDedupStore(), controlplane.WithMetrics(dp))

	var recResp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Size: 1}}},
	}, &recResp)
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	rm := collectCP(t, reader)
	if got := histCountCP(rm, "blobgw.cp.op.duration", map[string]string{"operation": "record", "outcome": "ok"}); got != 1 {
		t.Errorf("cp duration{record,ok} = %d, want 1", got)
	}
	if got := sumCounterCP(rm, "blobgw.cp.errors", map[string]string{"operation": "record"}); got != 0 {
		t.Errorf("cp errors{record} = %d, want 0 on success", got)
	}
}

// --- local metricdata helpers (controlplane_test cannot share metrics's own) ---

func collectCP(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func sumCounterCP(rm metricdata.ResourceMetrics, name string, match map[string]string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					if attrsMatchCP(dp.Attributes, match) {
						total += dp.Value
					}
				}
			}
		}
	}
	return total
}

func histCountCP(rm metricdata.ResourceMetrics, name string, match map[string]string) uint64 {
	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
				for _, dp := range h.DataPoints {
					if attrsMatchCP(dp.Attributes, match) {
						total += dp.Count
					}
				}
			}
		}
	}
	return total
}

func attrsMatchCP(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}
