// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegistryRenderPrometheus(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.FuseOp("read")
	r.FuseOp("read")
	r.FuseOp("write")
	r.AddReadBytes(4096)
	r.AddWriteBytes(1024)
	r.CacheHit()
	r.CacheMiss()
	r.Upload(1 << 20)
	r.SetStagingObserver(func() (int64, int64) { return 3, 9000 })

	out, err := r.RenderPrometheus()
	if err != nil {
		t.Fatalf("RenderPrometheus: %v", err)
	}
	text := string(out)
	for _, want := range []string{
		`mlfs_fuse_ops_total{op="read"} 2`,
		`mlfs_fuse_ops_total{op="write"} 1`,
		"mlfs_read_bytes_total 4096",
		"mlfs_write_bytes_total 1024",
		"mlfs_cache_hits_total 1",
		"mlfs_cache_misses_total 1",
		"mlfs_upload_ops_total 1",
		"mlfs_upload_bytes_total 1.048576e+06",
		"mlfs_staging_files 3",
		"mlfs_staging_bytes 9000",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestWriteClassBytes checks the by-class write-bytes counter splits into the
// "default" and "uncompressed" series so the model-vs-generic split is observable.
func TestWriteClassBytes(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.AddWriteClassBytes(1000, true) // uncompressed (model/tensor)
	r.AddWriteClassBytes(700, false) // default (generic)
	r.AddWriteClassBytes(300, false) // default again → 1000 total
	r.AddWriteClassBytes(0, true)    // ignored
	r.AddWriteClassBytes(-5, false)  // ignored

	out, err := r.RenderPrometheus()
	if err != nil {
		t.Fatalf("RenderPrometheus: %v", err)
	}
	text := string(out)
	for _, want := range []string{
		`mlfs_write_class_bytes_total{class="uncompressed"} 1000`,
		`mlfs_write_class_bytes_total{class="default"} 1000`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestPrefetchPacks checks the coalesced-pack-GET counter renders (the field
// signal for read-side pack coalescing) and ignores non-positive counts.
func TestPrefetchPacks(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.Prefetch(4096)    // a warmed slice (ops + bytes)
	r.PrefetchPacks(3)  // 3 coalesced backing GETs served the batch
	r.PrefetchPacks(0)  // ignored
	r.PrefetchPacks(-1) // ignored

	out, err := r.RenderPrometheus()
	if err != nil {
		t.Fatalf("RenderPrometheus: %v", err)
	}
	if !strings.Contains(string(out), "mlfs_prefetch_packs_total 3") {
		t.Errorf("render missing mlfs_prefetch_packs_total 3\n--- got ---\n%s", out)
	}
}

// TestHandlerScrape checks the HTTP /metrics surface serves the same counters.
func TestHandlerScrape(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())
	r.FuseOp("read")
	r.AddReadBytes(123)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "mlfs_read_bytes_total 123") {
		t.Errorf("scrape body missing counter:\n%s", rec.Body.String())
	}
}

// TestWithOTLPConstructs proves the OTLP push reader wires up (the gRPC exporter
// dials lazily, so construction succeeds without a live collector). Empty
// endpoint must add no reader.
func TestWithOTLPConstructs(t *testing.T) {
	if r, err := New(context.Background(), WithOTLP("", false, 0)); err != nil {
		t.Fatalf("New WithOTLP(empty): %v", err)
	} else {
		r.Shutdown(context.Background())
	}
	r, err := New(context.Background(), WithOTLP("127.0.0.1:4317", true, time.Second))
	if err != nil {
		t.Fatalf("New WithOTLP: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Shutdown(ctx) // no collector listening; the flush export fails fast and is ignored
}

// TestIdentityLabels proves WithIdentity stamps the producer's domain/region/
// instance onto every Prometheus series, so multi-tenant scrapes don't collide.
func TestIdentityLabels(t *testing.T) {
	r, err := New(context.Background(), WithIdentity("acme-tenant", "node-7", 3))
	if err != nil {
		t.Fatalf("New WithIdentity: %v", err)
	}
	defer r.Shutdown(context.Background())
	r.AddReadBytes(1)

	out, err := r.RenderPrometheus()
	if err != nil {
		t.Fatalf("RenderPrometheus: %v", err)
	}
	text := string(out)
	for _, want := range []string{`mlfs_domain="acme-tenant"`, `service_instance_id="node-7"`, `mlfs_region="3"`} {
		if !strings.Contains(text, want) {
			t.Errorf("identity label %q missing from series:\n%s", want, text)
		}
	}
}

// TestNilRegistryIsNoOp proves the nil-safe contract the hot paths rely on.
func TestNilRegistryIsNoOp(t *testing.T) {
	var r *Registry
	r.FuseOp("read")
	r.AddReadBytes(10)
	r.AddWriteBytes(10)
	r.AddWriteClassBytes(10, true)
	r.PrefetchPacks(2)
	r.CacheHit()
	r.CacheMiss()
	r.Upload(10)
	r.SetStagingObserver(func() (int64, int64) { return 1, 1 })
	// New P1/P2 instruments must be nil-safe too (call sites are unconditional).
	r.SetOldestUnflushedObserver(func() float64 { return 1 })
	r.SetMetaPoolObserver(func() (int64, int64, int64) { return 1, 1, 1 })
	r.RecordFuseOp("read", time.Now(), true)
	err := errors.New("boom")
	r.RecordMetaOp("read_slices", time.Now(), &err)
	r.IntegrityError("corrupt")
	r.LeaseAcquired()
	r.LeaseLost()
	r.FenceRejected()
	r.LeaderTransition(true)
	r.MountEvent("mount")
	if m := r.Meter(); m == nil {
		t.Error("nil Registry Meter() must return a usable (noop) meter, got nil")
	}
	if out, err := r.RenderPrometheus(); err != nil || out != nil {
		t.Errorf("nil RenderPrometheus = (%v, %v), want (nil, nil)", out, err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Errorf("nil Shutdown: %v", err)
	}
}

// TestRecordFuseOp checks the FUSE-op duration histogram records under
// {operation, outcome} and bumps the existing fuse.ops counter in one call.
func TestRecordFuseOp(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.RecordFuseOp("read", time.Now().Add(-2*time.Millisecond), true)
	r.RecordFuseOp("read", time.Now().Add(-2*time.Millisecond), false)

	text := render(t, r)
	for _, want := range []string{
		`mlfs_fuse_op_duration_seconds_count{operation="read",outcome="ok"} 1`,
		`mlfs_fuse_op_duration_seconds_count{operation="read",outcome="error"} 1`,
		`mlfs_fuse_ops_total{op="read"} 2`, // counter still bumped by RecordFuseOp
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestRecordMetaOp checks the meta-DB RED split: a success records duration
// outcome=ok with no error series; a failure records outcome=error AND bumps
// meta.errors under the classified error.kind.
func TestRecordMetaOp(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	var ok error // nil → success
	r.RecordMetaOp("read_slices", time.Now().Add(-time.Millisecond), &ok)
	notFound := sql.ErrNoRows
	r.RecordMetaOp("lookup", time.Now().Add(-time.Millisecond), &notFound)

	text := render(t, r)
	for _, want := range []string{
		`mlfs_meta_op_duration_seconds_count{operation="read_slices",outcome="ok"} 1`,
		`mlfs_meta_op_duration_seconds_count{operation="lookup",outcome="error"} 1`,
		`mlfs_meta_errors_total{error_kind="not_found",operation="lookup"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestIntegrityAndMultiNodeCounters checks the integrity canary and the
// multi-node event counters render with their bounded labels.
func TestIntegrityAndMultiNodeCounters(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.IntegrityError("corrupt")
	r.IntegrityError("not_found")
	r.IntegrityError("") // ignored
	r.LeaseAcquired()
	r.LeaseLost()
	r.FenceRejected()
	r.LeaderTransition(true)
	r.LeaderTransition(false)
	r.MountEvent("mount")
	r.MountEvent("unmount")

	text := render(t, r)
	for _, want := range []string{
		`mlfs_integrity_errors_total{error_kind="corrupt"} 1`,
		`mlfs_integrity_errors_total{error_kind="not_found"} 1`,
		"mlfs_lease_acquired_total 1",
		"mlfs_lease_lost_total 1",
		"mlfs_fence_rejected_total 1",
		`mlfs_leader_transitions_total{to="leader"} 1`,
		`mlfs_leader_transitions_total{to="follower"} 1`,
		`mlfs_mount_flap_total{event="mount"} 1`,
		`mlfs_mount_flap_total{event="unmount"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestOldestUnflushedGauge checks the write-back oldest-unflushed-age gauge
// renders the observer's value (the GC safety-window canary).
func TestOldestUnflushedGauge(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Shutdown(context.Background())

	r.SetOldestUnflushedObserver(func() float64 { return 42 })
	r.SetMetaPoolObserver(func() (int64, int64, int64) { return 5, 3, 7 })

	text := render(t, r)
	for _, want := range []string{
		"mlfs_writeback_oldest_unflushed_age_seconds 42",
		"mlfs_meta_pool_in_use 5",
		"mlfs_meta_pool_idle 3",
		"mlfs_meta_pool_wait_count 7",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, text)
		}
	}
}

// TestErrorKind checks the meta error.kind taxonomy maps the common Postgres /
// sql conditions to the bounded OBSERVABILITY.md set.
func TestErrorKind(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"nil":            {nil, "ok"},
		"not_found":      {sql.ErrNoRows, "not_found"},
		"canceled":       {context.Canceled, "canceled"},
		"timeout":        {context.DeadlineExceeded, "timeout"},
		"already_exists": {errors.New(`duplicate key value violates unique constraint`), "already_exists"},
		"conflict":       {errors.New("could not serialize access due to concurrent update"), "conflict"},
		"stmt_timeout":   {errors.New("canceling statement due to statement timeout"), "timeout"},
		"auth":           {errors.New(`password authentication failed for user "x"`), "auth"},
		"other":          {errors.New("boom"), "other"},
	}
	for name, tc := range cases {
		if got := ErrorKind(tc.err); got != tc.want {
			t.Errorf("%s: ErrorKind = %q, want %q", name, got, tc.want)
		}
	}
}

// render gathers the registry's Prometheus text, failing the test on error.
func render(t *testing.T, r *Registry) string {
	t.Helper()
	out, err := r.RenderPrometheus()
	if err != nil {
		t.Fatalf("RenderPrometheus: %v", err)
	}
	return string(out)
}
