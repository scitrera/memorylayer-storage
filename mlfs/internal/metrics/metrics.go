// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package metrics is the mlfs daemon's in-process instrument registry. It is the
// single source of truth for runtime counters (FUSE ops, bytes moved, cache
// hit/miss, write-back upload + staging depth) and is read through several
// surfaces without double-counting: a Prometheus-text render that the FUSE
// bridge serves as a virtual file inside the mount (so a workload pod can poll
// its own live feed with no extra access), an HTTP /metrics scrape, and an OTLP
// push — the last two for centralized reporting. WithIdentity tags every stream
// with the producer's domain/instance/region so tenants don't collide centrally.
//
// Instruments are OpenTelemetry metric instruments fed into a Prometheus
// exporter, so the same definitions back every surface. All record methods are
// nil-safe: a nil *Registry is a no-op, letting callers instrument hot paths
// unconditionally without a per-call branch at the call site beyond the cheap
// nil check.
package metrics

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/otlptranslator"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// identityKeys are the resource attributes surfaced as constant labels on the
// Prometheus output (so a federated scrape can tell daemons apart, the same way
// the OTLP resource does for a collector).
var identityKeys = map[string]bool{"mlfs.domain": true, "mlfs.region": true, "service.instance.id": true}

// ioDurationBuckets (seconds) is the FINE request-IO bucket set from
// docs/OBSERVABILITY.md: a sub-millisecond local/cached op through a slow,
// multi-second backing round trip. Shared by the FUSE-op and meta-DB latency
// histograms (both are request-IO surfaces operators tune against). Identical to
// casstore/blobstore/obs so the layers' latency views compose.
var ioDurationBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Option configures optional export surfaces on top of the always-on Prometheus
// reader (which backs the in-mount virtual file and the HTTP scrape).
type Option func(*config)

type config struct {
	otlpEndpoint string
	otlpInsecure bool
	pushInterval time.Duration

	hasIdentity bool
	domain      string // dedup/tenant domain (the daemon's -domain)
	instanceID  string // node identity (coordination node-id, or hostname-pid)
	region      int
}

// WithOTLP adds an OTLP/gRPC push reader: the meter's metrics are exported to the
// given collector endpoint (host:port) every interval, for centralized reporting.
// An empty endpoint is a no-op. interval ≤ 0 uses the SDK default.
func WithOTLP(endpoint string, insecure bool, interval time.Duration) Option {
	return func(c *config) {
		c.otlpEndpoint = endpoint
		c.otlpInsecure = insecure
		c.pushInterval = interval
	}
}

// WithIdentity stamps the producer's identity onto every exported metric stream:
// the dedup/tenant domain, the node instance id, and the region. These become
// OTLP resource attributes (so a central collector can tell tenants apart) and
// constant labels on the Prometheus surfaces (so a federated scrape can too).
// Without it the streams are anonymous and collide across daemons.
func WithIdentity(domain, instanceID string, region int) Option {
	return func(c *config) {
		c.hasIdentity = true
		c.domain = domain
		c.instanceID = instanceID
		c.region = region
	}
}

// knownOps are the FUSE operations whose attribute sets are precomputed, so the
// hot path records an op without allocating an attribute set per call.
var knownOps = []string{
	"lookup", "getattr", "setattr", "open", "create", "read", "write",
	"mkdir", "mknod", "unlink", "rmdir", "rename", "readdir", "fsync", "flush",
}

// Registry holds the daemon's instruments and the Prometheus registry they
// export through.
type Registry struct {
	provider *sdkmetric.MeterProvider
	promReg  *promclient.Registry

	meter metric.Meter // the daemon's OTEL meter; exposed via Meter() so adapters
	// (e.g. casstore/blobstore/obs wrapping the S3 backing store) can register
	// their own instruments on the SAME provider, with the same identity stream.

	fuseOps metric.Int64Counter
	// fuseOpDuration is the per-FUSE-op latency histogram {operation, outcome}.
	// Same op vocabulary as fuseOps (bounded by knownOps); operators read it to
	// spot a slow op or an error spike that the bytes/op counters can't.
	fuseOpDuration metric.Float64Histogram
	readBytes      metric.Int64Counter
	writeBytes     metric.Int64Counter
	// writeClassBytes is the write byte count attributed by compression class
	// ("default" | "uncompressed"), so a dashboard can confirm the model-vs-generic
	// split the per-file classification produces. Same bytes as writeBytes, cut by
	// class.
	writeClassBytes metric.Int64Counter
	cacheHits       metric.Int64Counter
	cacheMisses     metric.Int64Counter
	uploadBytes     metric.Int64Counter
	uploadOps       metric.Int64Counter
	prefetchOps     metric.Int64Counter
	prefetchBytes   metric.Int64Counter
	prefetchPacks   metric.Int64Counter

	// Meta-DB (PostgreSQL) RED: latency histogram + error counter for the
	// metadata ops (slice manifest read/write, ownership/lease queries). The pool
	// gauges (metaPoolObs) read sql.DB.Stats() lazily on collect.
	metaOpDuration metric.Float64Histogram
	metaErrors     metric.Int64Counter

	// integrityErrors is the data-integrity canary {error.kind}: a casstore read
	// error classified by snapshot.IntegrityKind (corrupt / not_found / range).
	// ~0 forever; non-zero is the corruption / GC-over-deletion smoke alarm.
	integrityErrors metric.Int64Counter

	// Multi-node (L2.8) coordination events. Counters for the transitions that map
	// to real incidents: lease churn, fence rejections (a write hitting a moved
	// generation), leader-election flips, and mount flap.
	leaseAcquired    metric.Int64Counter
	leaseLost        metric.Int64Counter
	fenceRejected    metric.Int64Counter
	leaderTransition metric.Int64Counter // {to=leader|follower}
	mountFlap        metric.Int64Counter // {event=mount|unmount}

	// stagingObs, if set, is called on each collect to read the current
	// write-back staging depth. Lazy (observed only when metrics are scraped) so
	// the cost — a directory walk — never lands on the drain hot path. Guarded by
	// obsMu because the cache registers it after the registry is built.
	obsMu      sync.Mutex
	stagingObs func() (files, bytes int64)
	// oldestUnflushedObs reports the age (seconds) of the oldest still-unflushed
	// staged write, or 0 when staging is empty. It is the GC safety-window canary
	// (docs/OBSERVABILITY.md HC2): if it exceeds the GC retention window, GC can
	// reclaim still-live data. Lazy like stagingObs; set by the cache.
	oldestUnflushedObs func() float64
	// metaPoolObs reports the metadata DB connection-pool levels (in-use / idle /
	// wait count) from sql.DB.Stats(). Lazy; set by the daemon after opening the
	// pool.
	metaPoolObs func() (inUse, idle, waitCount int64)

	opAttrs    map[string]metric.MeasurementOption
	classAttrs map[string]metric.MeasurementOption
}

// write-class attribute labels for writeClassBytes.
const (
	classLabelDefault      = "default"
	classLabelUncompressed = "uncompressed"
)

// New builds a registry. A Prometheus reader is always present (it backs both the
// in-mount virtual file via RenderPrometheus and the HTTP scrape via Handler);
// WithOTLP adds a push reader for centralized reporting. The returned Registry is
// ready to record.
func New(ctx context.Context, opts ...Option) (*Registry, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	// Producer identity: service.name is always "mlfs"; the domain/instance/region
	// are added by WithIdentity. As an OTLP resource a collector groups tenants by
	// it; as Prometheus constant labels a federated scrape does the same.
	idAttrs := []attribute.KeyValue{attribute.String("service.name", "mlfs")}
	if cfg.hasIdentity {
		if cfg.instanceID != "" {
			idAttrs = append(idAttrs, attribute.String("service.instance.id", cfg.instanceID))
		}
		if cfg.domain != "" {
			idAttrs = append(idAttrs, attribute.String("mlfs.domain", cfg.domain))
		}
		idAttrs = append(idAttrs, attribute.Int("mlfs.region", cfg.region))
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(idAttrs...))
	if err != nil {
		return nil, fmt.Errorf("metrics: resource: %w", err)
	}

	promReg := promclient.NewRegistry()
	exporter, err := prometheus.New(
		prometheus.WithRegisterer(promReg),
		prometheus.WithoutScopeInfo(),
		prometheus.WithoutTargetInfo(),
		// Full Prometheus-style names: escape dots→underscores and keep the
		// `_total` counter suffix (our test/dashboard contract). Set explicitly
		// rather than relying on the scheme-dependent default. No unit suffix is
		// added because our instruments declare no unit — this replaces the
		// deprecated WithoutUnits(), which was a no-op for unitless instruments.
		prometheus.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithSuffixes),
		// Stamp the identity attributes onto every series (mlfs_domain, mlfs_region,
		// service_instance_id); other resource attrs stay off the Prometheus labels.
		prometheus.WithResourceAsConstantLabels(func(kv attribute.KeyValue) bool {
			return identityKeys[string(kv.Key)]
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}
	mpOpts := []sdkmetric.Option{sdkmetric.WithReader(exporter), sdkmetric.WithResource(res)}

	// Optional OTLP/gRPC push reader (centralized export). The same instruments
	// feed it and the Prometheus reader, so there is one source of truth.
	if cfg.otlpEndpoint != "" {
		eopts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.otlpEndpoint)}
		if cfg.otlpInsecure {
			eopts = append(eopts, otlpmetricgrpc.WithInsecure())
		}
		otlpExp, err := otlpmetricgrpc.New(ctx, eopts...)
		if err != nil {
			return nil, fmt.Errorf("metrics: otlp exporter: %w", err)
		}
		ropts := []sdkmetric.PeriodicReaderOption{}
		if cfg.pushInterval > 0 {
			ropts = append(ropts, sdkmetric.WithInterval(cfg.pushInterval))
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExp, ropts...)))
	}

	provider := sdkmetric.NewMeterProvider(mpOpts...)
	m := provider.Meter("mlfs")

	r := &Registry{provider: provider, promReg: promReg, meter: m}
	var ierr error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithDescription(desc))
		if err != nil && ierr == nil {
			ierr = err
		}
		return c
	}
	r.fuseOps = ctr("mlfs.fuse.ops", "FUSE operations served, by op")
	r.readBytes = ctr("mlfs.read.bytes", "Bytes returned to read(2)")
	r.writeBytes = ctr("mlfs.write.bytes", "Bytes accepted from write(2)")
	r.cacheHits = ctr("mlfs.cache.hits", "Chunk reads served from the local disk cache")
	r.cacheMisses = ctr("mlfs.cache.misses", "Chunk reads that fell through to the backing store")
	r.uploadBytes = ctr("mlfs.upload.bytes", "Bytes drained from staging to the backing store")
	r.uploadOps = ctr("mlfs.upload.ops", "Write-back upload operations to the backing store")
	r.prefetchOps = ctr("mlfs.prefetch.slices", "Slices warmed into the cache by sequential-read prefetch")
	r.prefetchBytes = ctr("mlfs.prefetch.bytes", "Bytes warmed into the cache by sequential-read prefetch")
	r.prefetchPacks = ctr("mlfs.prefetch.packs", "Coalesced backing data GETs (packs) issued by sequential-read prefetch — slices/packs is the coalescing factor")
	r.writeClassBytes = ctr("mlfs.write.class.bytes", "Bytes accepted from write(2), by compression class")
	r.metaErrors = ctr("mlfs.meta.errors", "Metadata-DB operations that failed, by op and error kind")
	r.integrityErrors = ctr("mlfs.integrity.errors", "Data-integrity faults on the read path (corrupt / missing blob), by error kind — the corruption / GC-over-deletion canary, ~0 forever")
	r.leaseAcquired = ctr("mlfs.lease.acquired", "Ownership leases this node acquired (multi-node)")
	r.leaseLost = ctr("mlfs.lease.lost", "Ownership leases this node lost to a peer steal/fence (multi-node)")
	r.fenceRejected = ctr("mlfs.fence.rejected", "Metadata writes rejected because the ownership generation moved (fenced, multi-node)")
	r.leaderTransition = ctr("mlfs.leader.transitions", "GC-leader election transitions, by new role (multi-node)")
	r.mountFlap = ctr("mlfs.mount.flap", "Mount lifecycle events (mount / unmount) — repeated cycling is mount flap")
	if ierr != nil {
		return nil, fmt.Errorf("metrics: create instruments: %w", ierr)
	}

	// Request-IO latency histograms (FINE buckets, seconds): FUSE-op and meta-DB
	// op duration — the RED "Duration" both surfaces' rate/errors derive from.
	r.fuseOpDuration, err = m.Float64Histogram("mlfs.fuse.op.duration",
		metric.WithDescription("FUSE operation latency, by op and outcome"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(ioDurationBuckets...))
	if err != nil {
		return nil, fmt.Errorf("metrics: fuse.op.duration histogram: %w", err)
	}
	r.metaOpDuration, err = m.Float64Histogram("mlfs.meta.op.duration",
		metric.WithDescription("Metadata-DB operation latency, by op and outcome"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(ioDurationBuckets...))
	if err != nil {
		return nil, fmt.Errorf("metrics: meta.op.duration histogram: %w", err)
	}

	files, err := m.Int64ObservableGauge("mlfs.staging.files",
		metric.WithDescription("Undurable write-back entries currently in staging"))
	if err != nil {
		return nil, fmt.Errorf("metrics: staging.files gauge: %w", err)
	}
	bytesG, err := m.Int64ObservableGauge("mlfs.staging.bytes",
		metric.WithDescription("Bytes currently in write-back staging"))
	if err != nil {
		return nil, fmt.Errorf("metrics: staging.bytes gauge: %w", err)
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r.obsMu.Lock()
		obs := r.stagingObs
		r.obsMu.Unlock()
		if obs == nil {
			return nil
		}
		f, by := obs()
		o.ObserveInt64(files, f)
		o.ObserveInt64(bytesG, by)
		return nil
	}, files, bytesG); err != nil {
		return nil, fmt.Errorf("metrics: register staging callback: %w", err)
	}

	// Write-back oldest-unflushed-age (seconds): the GC safety-window canary. The
	// existing staging.files/bytes show the backlog's SIZE; this shows its AGE,
	// which is what bounds correctness — GC's retention window is only sound while
	// this stays under it. Lazy (the cache's observer stats the oldest staging
	// file only on collect), 0 when staging is empty.
	oldestAge, err := m.Float64ObservableGauge("mlfs.writeback.oldest_unflushed_age",
		metric.WithDescription("Age in seconds of the oldest still-unflushed staged write (0 = none); the GC safety-window canary"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("metrics: writeback.oldest_unflushed_age gauge: %w", err)
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r.obsMu.Lock()
		obs := r.oldestUnflushedObs
		r.obsMu.Unlock()
		if obs == nil {
			return nil
		}
		o.ObserveFloat64(oldestAge, obs())
		return nil
	}, oldestAge); err != nil {
		return nil, fmt.Errorf("metrics: register oldest-unflushed callback: %w", err)
	}

	// Meta-DB connection-pool gauges (USE: saturation of the bounded PG pool). A
	// rising wait_count / in_use at the cap is the signal to raise -db-max-open-conns
	// (see main.go). Lazy: reads sql.DB.Stats() on collect, set by the daemon.
	poolInUse, err := m.Int64ObservableGauge("mlfs.meta.pool.in_use",
		metric.WithDescription("Metadata-DB connections currently in use"))
	if err != nil {
		return nil, fmt.Errorf("metrics: meta.pool.in_use gauge: %w", err)
	}
	poolIdle, err := m.Int64ObservableGauge("mlfs.meta.pool.idle",
		metric.WithDescription("Idle metadata-DB connections in the pool"))
	if err != nil {
		return nil, fmt.Errorf("metrics: meta.pool.idle gauge: %w", err)
	}
	poolWait, err := m.Int64ObservableGauge("mlfs.meta.pool.wait_count",
		metric.WithDescription("Cumulative count of metadata-DB connection waits (pool saturation)"))
	if err != nil {
		return nil, fmt.Errorf("metrics: meta.pool.wait_count gauge: %w", err)
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r.obsMu.Lock()
		obs := r.metaPoolObs
		r.obsMu.Unlock()
		if obs == nil {
			return nil
		}
		inUse, idle, wait := obs()
		o.ObserveInt64(poolInUse, inUse)
		o.ObserveInt64(poolIdle, idle)
		o.ObserveInt64(poolWait, wait)
		return nil
	}, poolInUse, poolIdle, poolWait); err != nil {
		return nil, fmt.Errorf("metrics: register meta-pool callback: %w", err)
	}

	r.opAttrs = make(map[string]metric.MeasurementOption, len(knownOps))
	for _, op := range knownOps {
		r.opAttrs[op] = metric.WithAttributes(attribute.String("op", op))
	}
	r.classAttrs = map[string]metric.MeasurementOption{
		classLabelDefault:      metric.WithAttributes(attribute.String("class", classLabelDefault)),
		classLabelUncompressed: metric.WithAttributes(attribute.String("class", classLabelUncompressed)),
	}
	return r, nil
}

// FuseOp records one served FUSE operation of the named op.
func (r *Registry) FuseOp(op string) {
	if r == nil {
		return
	}
	if a, ok := r.opAttrs[op]; ok {
		r.fuseOps.Add(context.Background(), 1, a)
		return
	}
	r.fuseOps.Add(context.Background(), 1, metric.WithAttributes(attribute.String("op", op)))
}

// AddReadBytes records bytes returned to a read(2).
func (r *Registry) AddReadBytes(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.readBytes.Add(context.Background(), int64(n))
}

// AddWriteBytes records bytes accepted from a write(2).
func (r *Registry) AddWriteBytes(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.writeBytes.Add(context.Background(), int64(n))
}

// AddWriteClassBytes records write(2) bytes attributed by compression class so the
// model-vs-generic split is observable. uncompressed selects the "uncompressed"
// (model/tensor) series; otherwise "default". Same bytes as AddWriteBytes, cut by
// class.
func (r *Registry) AddWriteClassBytes(n int, uncompressed bool) {
	if r == nil || n <= 0 {
		return
	}
	label := classLabelDefault
	if uncompressed {
		label = classLabelUncompressed
	}
	r.writeClassBytes.Add(context.Background(), int64(n), r.classAttrs[label])
}

// PrefetchPacks records the number of coalesced backing data GETs (packs) a
// prefetch batch issued. With mlfs.prefetch.slices it shows the round-trip
// reduction: slices/packs is the coalescing factor.
func (r *Registry) PrefetchPacks(n int64) {
	if r == nil || n <= 0 {
		return
	}
	r.prefetchPacks.Add(context.Background(), n)
}

// CacheHit / CacheMiss record a chunk read served locally vs from the backing store.
func (r *Registry) CacheHit() {
	if r == nil {
		return
	}
	r.cacheHits.Add(context.Background(), 1)
}

func (r *Registry) CacheMiss() {
	if r == nil {
		return
	}
	r.cacheMisses.Add(context.Background(), 1)
}

// Upload records a completed write-back upload of n bytes to the backing store.
func (r *Registry) Upload(n int64) {
	if r == nil {
		return
	}
	r.uploadOps.Add(context.Background(), 1)
	if n > 0 {
		r.uploadBytes.Add(context.Background(), n)
	}
}

// Prefetch records one slice (n bytes) warmed into the cache by sequential-read
// readahead — the speculative work that converts later demand-read misses into
// local hits. Distinct from Upload (write-back) and from CacheMiss (the backing
// fetch a prefetch itself incurs).
func (r *Registry) Prefetch(n int64) {
	if r == nil {
		return
	}
	r.prefetchOps.Add(context.Background(), 1)
	if n > 0 {
		r.prefetchBytes.Add(context.Background(), n)
	}
}

// SetStagingObserver registers a callback the registry invokes on each collect
// to read the current write-back staging depth (files, bytes). The cache sets
// this to a directory walk of its staging dir, so the cost lands only when
// metrics are actually scraped, never on the drain path. Nil clears it.
func (r *Registry) SetStagingObserver(fn func() (files, bytes int64)) {
	if r == nil {
		return
	}
	r.obsMu.Lock()
	r.stagingObs = fn
	r.obsMu.Unlock()
}

// SetOldestUnflushedObserver registers the callback the registry invokes on each
// collect to read the age (seconds) of the oldest still-unflushed staged write
// (0 = staging empty) — the GC safety-window canary. The cache sets it to a stat
// of its staging dir, so the cost lands only when metrics are scraped. Nil clears
// it.
func (r *Registry) SetOldestUnflushedObserver(fn func() float64) {
	if r == nil {
		return
	}
	r.obsMu.Lock()
	r.oldestUnflushedObs = fn
	r.obsMu.Unlock()
}

// SetMetaPoolObserver registers the callback the registry invokes on each collect
// to read the metadata DB connection-pool levels (in-use / idle / cumulative
// wait count). The daemon sets it to sql.DB.Stats(). Nil clears it.
func (r *Registry) SetMetaPoolObserver(fn func() (inUse, idle, waitCount int64)) {
	if r == nil {
		return
	}
	r.obsMu.Lock()
	r.metaPoolObs = fn
	r.obsMu.Unlock()
}

// Meter returns the daemon's OTEL meter so opt-in adapters (e.g.
// casstore/blobstore/obs wrapping the S3 backing store) register their own
// instruments on the SAME provider — sharing this daemon's identity stream and
// the three export surfaces. Returns an otel noop meter for a nil Registry, so a
// caller (e.g. obs.Wrap) never has to branch on metrics being disabled.
func (r *Registry) Meter() metric.Meter {
	if r == nil || r.meter == nil {
		return noop.NewMeterProvider().Meter("mlfs")
	}
	return r.meter
}

// RecordFuseOp records one served FUSE op's latency under {operation, outcome}
// and bumps the fuse.ops counter, so the op rate and its latency/error split come
// from one call. start is when the op began; ok=false records outcome=error. A
// nil Registry is a no-op.
func (r *Registry) RecordFuseOp(op string, start time.Time, ok bool) {
	if r == nil {
		return
	}
	r.FuseOp(op)
	r.fuseOpDuration.Record(context.Background(), time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome(ok))))
}

// RecordMetaOp records one metadata-DB op's latency under {operation, outcome}
// and, on failure, bumps mlfs.meta.errors under {operation, error.kind}. start is
// when the op began; err nil means success. A nil Registry is a no-op. Callers
// wrap a query with a deferred call:
//
//	defer r.RecordMetaOp("read_slices", time.Now(), &err)
func (r *Registry) RecordMetaOp(op string, start time.Time, errp *error) {
	if r == nil {
		return
	}
	var err error
	if errp != nil {
		err = *errp
	}
	r.metaOpDuration.Record(context.Background(), time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome(err == nil))))
	if err != nil {
		r.metaErrors.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("operation", op), attribute.String("error.kind", ErrorKind(err))))
	}
}

// IntegrityError bumps the data-integrity canary for kind ("corrupt" /
// "not_found" / "range"). Caller classifies a read-path error with
// snapshot.IntegrityKind and calls this only when the kind is non-empty. A nil
// Registry is a no-op.
func (r *Registry) IntegrityError(kind string) {
	if r == nil || kind == "" {
		return
	}
	r.integrityErrors.Add(context.Background(), 1, metric.WithAttributes(attribute.String("error.kind", kind)))
}

// LeaseAcquired / LeaseLost record an ownership-lease transition (multi-node).
func (r *Registry) LeaseAcquired() {
	if r == nil {
		return
	}
	r.leaseAcquired.Add(context.Background(), 1)
}

func (r *Registry) LeaseLost() {
	if r == nil {
		return
	}
	r.leaseLost.Add(context.Background(), 1)
}

// FenceRejected records one metadata write rejected because the ownership
// generation moved out from under it (a stale-handle fence; multi-node).
func (r *Registry) FenceRejected() {
	if r == nil {
		return
	}
	r.fenceRejected.Add(context.Background(), 1)
}

// LeaderTransition records a GC-leader election transition. leader=true means
// this node just became leader; false means it stood down (multi-node).
func (r *Registry) LeaderTransition(leader bool) {
	if r == nil {
		return
	}
	role := "follower"
	if leader {
		role = "leader"
	}
	r.leaderTransition.Add(context.Background(), 1, metric.WithAttributes(attribute.String("to", role)))
}

// MountEvent records a mount-lifecycle event ("mount" / "unmount"); repeated
// cycling shows up as mount flap.
func (r *Registry) MountEvent(event string) {
	if r == nil {
		return
	}
	r.mountFlap.Add(context.Background(), 1, metric.WithAttributes(attribute.String("event", event)))
}

// outcome maps an ok flag to the OBSERVABILITY.md outcome label.
func outcome(ok bool) string {
	if ok {
		return "ok"
	}
	return "error"
}

// ErrorKind maps a metadata-DB error to the small bounded error.kind set in
// docs/OBSERVABILITY.md (never a raw error string — that is unbounded). It mirrors
// the casstore/blobstore/obs taxonomy so a meta error reads the same as a backing
// error on a dashboard. sql.ErrNoRows is not_found; context cancel/deadline are
// canceled/timeout; the rest fall back to string sniffs for the common Postgres
// conditions (serialization conflict, lock timeout, auth) and otherwise "other".
func ErrorKind(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, sql.ErrNoRows):
		return "not_found"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "duplicate key") || strings.Contains(s, "already exists"):
		return "already_exists"
	case strings.Contains(s, "serializ") || strings.Contains(s, "deadlock") || strings.Contains(s, "could not serialize"):
		return "conflict"
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline") || strings.Contains(s, "canceling statement"):
		return "timeout"
	case strings.Contains(s, "password") || strings.Contains(s, "authentication") || strings.Contains(s, "permission denied"):
		return "auth"
	default:
		return "other"
	}
}

// Handler returns an HTTP handler that serves the current metrics in Prometheus
// exposition format, for a /metrics scrape endpoint. Backed by the same registry
// as the in-mount virtual file. Returns nil for a nil Registry.
func (r *Registry) Handler() http.Handler {
	if r == nil {
		return nil
	}
	return promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{})
}

// RenderPrometheus returns the current snapshot in Prometheus text exposition
// format — the bytes the FUSE virtual file and any /metrics scrape serve.
func (r *Registry) RenderPrometheus() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	mfs, err := r.promReg.Gather()
	if err != nil {
		return nil, fmt.Errorf("metrics: gather: %w", err)
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			return nil, fmt.Errorf("metrics: encode: %w", err)
		}
	}
	return buf.Bytes(), nil
}

// Shutdown flushes and stops the meter provider.
func (r *Registry) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.provider.Shutdown(ctx)
}
