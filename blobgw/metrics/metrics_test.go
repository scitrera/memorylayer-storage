// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// scrape renders the registry's Prometheus exposition via the HTTP handler.
func scrape(t *testing.T, r *Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// TestPerTenantMetrics proves GC + compaction outcomes are recorded per tenant
// (the tenant is a label, since one blobgw process sweeps many tenants).
func TestPerTenantMetrics(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.RecordGC("t1", snapshot.GCResult{ChunksScanned: 100, LiveChunks: 42, ChunksReclaimed: 5, BytesReclaimed: 1000, Duration: 250 * time.Millisecond})
	r.RecordGC("t2", snapshot.GCResult{ChunksScanned: 7, LiveChunks: 7, ChunksReclaimed: 0, Duration: 5 * time.Millisecond})
	r.RecordCompact("t1", snapshot.CompactResult{PacksScanned: 20, PacksSelected: 12, PacksWritten: 2, ManifestsRewritten: 12, BytesRewritten: 4096, Duration: 80 * time.Millisecond})
	r.GCError("t2")
	r.CompactError("t1")

	out := scrape(t, r)
	for _, want := range []string{
		`blobgw_gc_chunks_reclaimed_total{tenant="t1"} 5`,
		`blobgw_gc_bytes_reclaimed_total{tenant="t1"} 1000`,
		`blobgw_gc_chunks_scanned_total{tenant="t2"} 7`,
		`blobgw_gc_live_chunks{tenant="t1"} 42`,
		`blobgw_gc_errors_total{tenant="t2"} 1`,
		`blobgw_compact_packs_written_total{tenant="t1"} 2`,
		`blobgw_compact_bytes_rewritten_total{tenant="t1"} 4096`,
		`blobgw_compact_errors_total{tenant="t1"} 1`,
		`blobgw_gc_duration_count{tenant="t1"} 1`,
		`blobgw_compact_duration_count{tenant="t1"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape missing %q\n--- got ---\n%s", want, out)
		}
	}
	// t2 must NOT carry t1's reclaimed bytes — series are isolated by tenant.
	if strings.Contains(out, `blobgw_gc_bytes_reclaimed_total{tenant="t2"} 1000`) {
		t.Error("tenant series leaked across tenants")
	}
}

// TestInstanceIdentity proves WithInstance stamps service.instance.id as a
// constant label, so a federated scrape tells blobgw replicas apart.
func TestInstanceIdentity(t *testing.T) {
	r, err := New(context.Background(), WithInstance("blobgw-node-7"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())
	r.RecordGC("acme", snapshot.GCResult{ChunksReclaimed: 1})

	out := scrape(t, r)
	if !strings.Contains(out, `service_instance_id="blobgw-node-7"`) {
		t.Errorf("scrape missing instance identity label:\n%s", out)
	}
	if !strings.Contains(out, `tenant="acme"`) {
		t.Errorf("scrape missing tenant label:\n%s", out)
	}
}

// TestNilRegistryIsNoOp proves the nil-safe contract the runner relies on.
func TestNilRegistryIsNoOp(t *testing.T) {
	var r *Registry
	r.RecordGC("t", snapshot.GCResult{ChunksReclaimed: 1})
	r.GCError("t")
	r.RecordCompact("t", snapshot.CompactResult{PacksWritten: 1})
	r.CompactError("t")
	if r.Handler() != nil {
		t.Error("nil Handler should be nil")
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Errorf("nil Shutdown: %v", err)
	}
}

// TestWithOTLPConstructs proves the OTLP push reader wires up (lazy dial).
func TestWithOTLPConstructs(t *testing.T) {
	r, err := New(context.Background(), WithInstance("n1"), WithOTLP("127.0.0.1:4317", true, time.Second))
	if err != nil {
		t.Fatalf("New WithOTLP: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Shutdown(ctx) // no collector; flush fails fast, ignored
}
