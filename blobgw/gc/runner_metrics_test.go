// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gc"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
)

// TestRunner_RecordsPerTenantMetrics drives a real leader-elected sweep over a
// tenant's backend and asserts the wired metrics.Registry captured that tenant's
// GC outcome — i.e. the runner→metrics path works end-to-end, not just the
// recorder in isolation.
func TestRunner_RecordsPerTenantMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTenantBackends(t)

	const tenant = "acme"
	gw := tb.gateway(ctx, tenant)

	// Write then orphan an object so its packs become GC-eligible.
	payload := bytes.Repeat([]byte("metrics payload bytes. "), 8192) // ~184 KiB
	if _, err := gw.Put(ctx, "doc.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := gw.Delete(ctx, "doc.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	clk := &fakeClock{now: time.Now()}
	reg, err := metrics.New(ctx, metrics.WithInstance("solo"))
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	defer reg.Shutdown(context.Background())

	connect := startJetStreamNATS(t)
	leader, err := gc.NewLeader(ctx, connect("solo"), gc.LeaderConfig{
		Key: "gc", NodeID: "solo", TTL: time.Second, Renew: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeader: %v", err)
	}
	go leader.Run(ctx)
	waitFor(t, 5*time.Second, "leader elected", leader.IsLeader)

	runner, err := gc.NewRunner(leader, tb.gcFunc(clk.Now),
		func() []string { return []string{tenant} },
		gc.RunnerConfig{Interval: 50 * time.Millisecond, SafetyWindow: time.Hour, Metrics: reg})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go runner.Run(runCtx)

	// Cross the safety window so the orphaned packs get reclaimed.
	clk.advance(2 * time.Hour)
	waitFor(t, 5*time.Second, "old unreferenced packs to be swept", func() bool {
		return tb.countPacks(tenant) == 0
	})
	// A reclaiming pass has run; its outcome must be recorded against the tenant.
	waitFor(t, 5*time.Second, "gc metrics recorded for tenant", func() bool {
		return scrapeValue(t, reg, `blobgw_gc_chunks_reclaimed_total{service_instance_id="solo",tenant="acme"}`) >= 1
	})

	out := scrapeBody(t, reg)
	if scrapeValue(t, reg, `blobgw_gc_passes_total{service_instance_id="solo",tenant="acme"}`) < 1 {
		t.Errorf("expected at least one recorded GC pass for acme\n%s", out)
	}
	if !strings.Contains(out, `blobgw_gc_live_chunks{service_instance_id="solo",tenant="acme"}`) {
		t.Errorf("expected a live_chunks gauge for acme\n%s", out)
	}
}

func scrapeBody(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

// scrapeValue returns the numeric value of the series whose "name{labels}" prefix
// matches, or -1 if absent.
func scrapeValue(t *testing.T, reg *metrics.Registry, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrapeBody(t, reg), "\n") {
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
