// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package metrics is blobgw's instrument registry for the leader-elected GC and
// pack-compaction (repack) sweeper. Unlike the mlfs daemon (one tenant per
// process, so its identity is a resource attribute), one blobgw process sweeps
// MANY tenants — so the tenant is a per-measurement attribute on every metric,
// while the resource carries only this blobgw node's identity. A central OTLP
// collector (or a Prometheus scrape) can then break GC/compaction down by
// tenant and tell blobgw replicas apart.
//
// Instruments are OpenTelemetry metric instruments fed into a Prometheus
// exporter (so the same definitions back the /metrics scrape) plus an optional
// OTLP/gRPC push. All record methods are nil-safe: a nil *Registry is a no-op.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Option configures the registry.
type Option func(*config)

type config struct {
	instanceID   string
	serviceName  string // OTEL resource service.name (default "blobgw"); set via WithServiceName
	otlpEndpoint string
	otlpInsecure bool
	pushInterval time.Duration
	traceSampler float64 // head-sampling ratio for the TracerProvider (see NewTracing)
}

// WithInstance sets this blobgw node's identity (service.instance.id), so a
// collector / federated scrape can tell replicas apart.
func WithInstance(id string) Option { return func(c *config) { c.instanceID = id } }

// WithServiceName overrides the OTEL resource service.name (default "blobgw").
// A sibling binary reusing this package — e.g. blobgw-edge — sets its own name
// so a central collector distinguishes the services, not just their instances.
func WithServiceName(name string) Option { return func(c *config) { c.serviceName = name } }

// WithOTLP adds an OTLP/gRPC push reader to the given collector endpoint. Empty
// endpoint is a no-op; interval ≤ 0 uses the SDK default.
func WithOTLP(endpoint string, insecure bool, interval time.Duration) Option {
	return func(c *config) {
		c.otlpEndpoint = endpoint
		c.otlpInsecure = insecure
		c.pushInterval = interval
	}
}

// WithTraceSampler sets the head-sampling ratio for the TracerProvider built by
// NewTracing: 1.0 samples every root trace, 0.0 none. It is wrapped in a
// parent-based sampler so a sampled upstream caller's trace is always continued.
// ≤ 0 falls back to NewTracing's default (sample everything, since tracing is
// opt-in behind an explicit OTLP endpoint).
func WithTraceSampler(ratio float64) Option { return func(c *config) { c.traceSampler = ratio } }

// durationBuckets (seconds) span sub-ms compaction no-ops to multi-minute sweeps.
var durationBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120}

// fineDurationBuckets (seconds) are the backing-store / DB / request IO set from
// docs/OBSERVABILITY.md: sub-millisecond local/cached ops up to a slow multi-second
// round trip. The data-path instruments (PG / control-plane / HTTP) use these, NOT
// the coarse GC/compaction buckets above, since data-path latency is the thing
// operators tune against.
var fineDurationBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds blobgw's GC + compaction instruments.
type Registry struct {
	provider *sdkmetric.MeterProvider
	promReg  *promclient.Registry
	meter    metric.Meter // the "blobgw" meter, shared with the data path

	// GC.
	gcPasses        metric.Int64Counter
	gcChunksScanned metric.Int64Counter
	gcChunksReclaim metric.Int64Counter
	gcBytesReclaim  metric.Int64Counter
	gcDeferredYoung metric.Int64Counter
	gcSkippedRecent metric.Int64Counter
	gcSkippedError  metric.Int64Counter
	gcErrors        metric.Int64Counter
	gcDuration      metric.Float64Histogram
	gcEmptyLiveSet  metric.Int64Counter // #50 canary: live==0 while packs exist

	// Compaction (repack).
	cpPasses       metric.Int64Counter
	cpPacksScanned metric.Int64Counter
	cpPacksSelect  metric.Int64Counter
	cpPacksWritten metric.Int64Counter
	cpManifestsRW  metric.Int64Counter
	cpBytesRW      metric.Int64Counter
	cpErrors       metric.Int64Counter
	cpDuration     metric.Float64Histogram

	// liveChunks is the last observed live-chunk count per tenant, surfaced as a
	// gauge (per-pass point-in-time, not cumulative — so it cannot be a counter).
	mu         sync.Mutex
	liveChunks map[string]int64
}

// New builds the registry. A Prometheus reader is always present (backs Handler);
// WithOTLP adds a push reader. WithInstance stamps the node identity.
func New(ctx context.Context, opts ...Option) (*Registry, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	res, err := buildResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics: resource: %w", err)
	}

	promReg := promclient.NewRegistry()
	exporter, err := prometheus.New(
		prometheus.WithRegisterer(promReg),
		prometheus.WithoutScopeInfo(),
		prometheus.WithoutTargetInfo(),
		prometheus.WithoutUnits(),
		// service.instance.id becomes a constant label so a federated scrape tells
		// replicas apart; tenant is already a per-metric label.
		prometheus.WithResourceAsConstantLabels(func(kv attribute.KeyValue) bool {
			return string(kv.Key) == "service.instance.id"
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}
	mpOpts := []sdkmetric.Option{sdkmetric.WithReader(exporter), sdkmetric.WithResource(res)}

	if cfg.otlpEndpoint != "" {
		eopts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.otlpEndpoint)}
		if cfg.otlpInsecure {
			eopts = append(eopts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, eopts...)
		if err != nil {
			return nil, fmt.Errorf("metrics: otlp exporter: %w", err)
		}
		ropts := []sdkmetric.PeriodicReaderOption{}
		if cfg.pushInterval > 0 {
			ropts = append(ropts, sdkmetric.WithInterval(cfg.pushInterval))
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, ropts...)))
	}

	provider := sdkmetric.NewMeterProvider(mpOpts...)
	m := provider.Meter("blobgw")
	r := &Registry{provider: provider, promReg: promReg, meter: m, liveChunks: make(map[string]int64)}

	var ierr error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithDescription(desc))
		if err != nil && ierr == nil {
			ierr = err
		}
		return c
	}
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(durationBuckets...))
		if err != nil && ierr == nil {
			ierr = err
		}
		return h
	}
	r.gcPasses = ctr("blobgw.gc.passes", "GC passes completed, by tenant")
	r.gcChunksScanned = ctr("blobgw.gc.chunks_scanned", "Chunks scanned during GC mark, by tenant")
	r.gcChunksReclaim = ctr("blobgw.gc.chunks_reclaimed", "Orphaned chunks deleted by GC, by tenant")
	r.gcBytesReclaim = ctr("blobgw.gc.bytes_reclaimed", "Bytes reclaimed by GC, by tenant")
	r.gcDeferredYoung = ctr("blobgw.gc.chunks_deferred_young", "Chunks not reclaimed (younger than the safety window), by tenant")
	r.gcSkippedRecent = ctr("blobgw.gc.chunks_skipped_recent", "Chunks not reclaimed (changed between mark and sweep), by tenant")
	r.gcSkippedError = ctr("blobgw.gc.chunks_skipped_error", "Chunks not reclaimed (backend error), by tenant")
	r.gcErrors = ctr("blobgw.gc.errors", "GC passes that failed, by tenant")
	r.gcDuration = hist("blobgw.gc.duration", "GC pass wall-clock time, by tenant")
	r.gcEmptyLiveSet = ctr("blobgw.gc.empty_live_set", "GC passes that found zero live chunks while pack blobs still exist (the TECH_DEBT #50 over-deletion canary), by tenant")
	r.cpPasses = ctr("blobgw.compact.passes", "Compaction passes that did work, by tenant")
	r.cpPacksScanned = ctr("blobgw.compact.packs_scanned", "Packs scanned during compaction, by tenant")
	r.cpPacksSelect = ctr("blobgw.compact.packs_selected", "Under-filled/small packs selected for repack, by tenant")
	r.cpPacksWritten = ctr("blobgw.compact.packs_written", "Consolidated packs written by compaction, by tenant")
	r.cpManifestsRW = ctr("blobgw.compact.manifests_rewritten", "Manifests rewritten to point at consolidated packs, by tenant")
	r.cpBytesRW = ctr("blobgw.compact.bytes_rewritten", "Live bytes rewritten by compaction, by tenant")
	r.cpErrors = ctr("blobgw.compact.errors", "Compaction passes that failed, by tenant")
	r.cpDuration = hist("blobgw.compact.duration", "Compaction pass wall-clock time, by tenant")
	if ierr != nil {
		return nil, fmt.Errorf("metrics: create instruments: %w", ierr)
	}

	liveGauge, err := m.Int64ObservableGauge("blobgw.gc.live_chunks",
		metric.WithDescription("Live (referenced) chunk count at the last GC pass, by tenant"))
	if err != nil {
		return nil, fmt.Errorf("metrics: live_chunks gauge: %w", err)
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		for tenant, n := range r.liveChunks {
			o.ObserveInt64(liveGauge, n, metric.WithAttributes(attribute.String("tenant", tenant)))
		}
		return nil
	}, liveGauge); err != nil {
		return nil, fmt.Errorf("metrics: register live_chunks callback: %w", err)
	}
	return r, nil
}

// RecordGC records the outcome of one tenant's GC pass.
func (r *Registry) RecordGC(tenant string, res snapshot.GCResult) {
	if r == nil {
		return
	}
	ctx := context.Background()
	a := metric.WithAttributes(attribute.String("tenant", tenant))
	r.gcPasses.Add(ctx, 1, a)
	r.gcChunksScanned.Add(ctx, int64(res.ChunksScanned), a)
	r.gcChunksReclaim.Add(ctx, int64(res.ChunksReclaimed), a)
	r.gcBytesReclaim.Add(ctx, res.BytesReclaimed, a)
	r.gcDeferredYoung.Add(ctx, int64(res.ChunksDeferredYoung), a)
	r.gcSkippedRecent.Add(ctx, int64(res.ChunksSkippedRecent), a)
	r.gcSkippedError.Add(ctx, int64(res.ChunksSkippedError), a)
	r.gcDuration.Record(ctx, res.Duration.Seconds(), a)
	r.mu.Lock()
	r.liveChunks[tenant] = int64(res.LiveChunks)
	r.mu.Unlock()

	// #50 over-deletion canary (docs/OBSERVABILITY.md operational canaries): a pass
	// that scanned chunks (the tenant HAS pack blobs) yet found zero live is the
	// single condition that precedes GC reclaiming live data. It should be ~0
	// forever; the caller (gc runner) also logs it prominently.
	if res.LiveChunks == 0 && res.ChunksScanned > 0 {
		r.gcEmptyLiveSet.Add(ctx, 1, a)
	}
}

// EmptyLiveSet reports whether a GC pass result is the TECH_DEBT #50 disaster
// signal: zero live chunks while pack blobs still exist (the pass scanned > 0).
// The gc runner uses it to decide whether to log the prominent ERROR alongside
// the counter RecordGC already bumps, so the threshold lives in exactly one place.
func EmptyLiveSet(res snapshot.GCResult) bool {
	return res.LiveChunks == 0 && res.ChunksScanned > 0
}

// Meter returns the shared "blobgw" OTEL meter so the data path (backing-store
// wrappers, PG/control-plane/HTTP instruments) records onto the SAME provider as
// GC/compaction — one /metrics scrape and one OTLP push cover both. Returns a
// no-op meter for a nil Registry (metrics disabled), so disabled-metrics call
// sites never branch on nil: every instrument they build is a no-op with no
// exporter behind it.
func (r *Registry) Meter() metric.Meter {
	if r == nil || r.meter == nil {
		return noopmetric.NewMeterProvider().Meter("blobgw")
	}
	return r.meter
}

// GCError records a failed GC pass for a tenant.
func (r *Registry) GCError(tenant string) {
	if r == nil {
		return
	}
	r.gcErrors.Add(context.Background(), 1, metric.WithAttributes(attribute.String("tenant", tenant)))
}

// RecordCompact records the outcome of one tenant's compaction (repack) pass.
func (r *Registry) RecordCompact(tenant string, res snapshot.CompactResult) {
	if r == nil {
		return
	}
	ctx := context.Background()
	a := metric.WithAttributes(attribute.String("tenant", tenant))
	r.cpPasses.Add(ctx, 1, a)
	r.cpPacksScanned.Add(ctx, int64(res.PacksScanned), a)
	r.cpPacksSelect.Add(ctx, int64(res.PacksSelected), a)
	r.cpPacksWritten.Add(ctx, int64(res.PacksWritten), a)
	r.cpManifestsRW.Add(ctx, int64(res.ManifestsRewritten), a)
	r.cpBytesRW.Add(ctx, res.BytesRewritten, a)
	r.cpDuration.Record(ctx, res.Duration.Seconds(), a)
}

// CompactError records a failed compaction pass for a tenant.
func (r *Registry) CompactError(tenant string) {
	if r == nil {
		return
	}
	r.cpErrors.Add(context.Background(), 1, metric.WithAttributes(attribute.String("tenant", tenant)))
}

// Handler serves the current metrics in Prometheus exposition format. Returns nil
// for a nil Registry.
func (r *Registry) Handler() http.Handler {
	if r == nil {
		return nil
	}
	return promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{})
}

// Shutdown flushes and stops the meter provider.
func (r *Registry) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.provider.Shutdown(ctx)
}
