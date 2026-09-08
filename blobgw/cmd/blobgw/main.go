// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command blobgw runs the L1 object gateway over the casstore substrate.
//
// Backends are selected by flags so one binary serves both local dev and
// production:
//
//	-backend local|s3        chunk + manifest storage (default local)
//	-index   memory|postgres dedup index + object (blob_ref) index (default memory)
//	-staging none|memory|s3  stage-then-finalize upload backend (default memory)
//
// The zero-flag default (local + memory + memory) is a self-contained dev
// server. Production uses -backend s3 -index postgres -staging s3.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/gc"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/blobgw/pgindex"
	"github.com/scitrera/memorylayer-storage/blobgw/s3stage"
	"github.com/scitrera/memorylayer-storage/blobgw/server"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"go.opentelemetry.io/otel/attribute"
)

type options struct {
	addr         string
	domain       string
	packSize     int
	compression  string
	packFraming  string
	gcInterval   time.Duration
	gcSafetyWind time.Duration
	stagingTTL   time.Duration

	backend string
	dataDir string

	s3Bucket         string
	s3Region         string
	s3Endpoint       string
	s3Prefix         string
	s3ForcePathStyle bool

	index       string
	databaseURL string

	dbMaxOpenConns    int
	dbMaxIdleConns    int
	dbConnMaxLifetime time.Duration

	staging       string
	stagingBucket string
	stagingPrefix string

	// Control plane (NATS face, ADR §2.5/§2.9). All default OFF: when
	// controlPlane is false the daemon is the existing HTTP-only object server
	// and none of these are consulted.
	controlPlane       bool
	natsURL            string
	natsCreds          string
	natsNKey           string
	tenantConfig       string
	tenantSecretsFile  string
	credentialMode     string
	credentialCacheTTL time.Duration
	presignMaxTTL      time.Duration

	// Leader-elected per-tenant GC (ADR §2.4 #29, §4 I3). Only meaningful with
	// -control-plane (needs the per-tenant router + NATS). Default OFF.
	gcLeader             bool
	gcLeaderInterval     time.Duration
	gcLeaderSafetyWindow time.Duration
	gcLeaderTTL          time.Duration
	gcLeaderReplicas     int

	// Pack compaction (repack), run as the first step of each GC tenant pass.
	// Default OFF: compaction only kicks in when a trigger threshold is set
	// (gc-compact-min-fill > 0 or gc-compact-min-pack-bytes > 0).
	gcCompactMinFill         float64
	gcCompactMinPackBytes    int64
	gcCompactMaxBytesPerPass int64
	gcCompactDefrag          bool

	// Runtime metrics for the GC + compaction sweeper, per tenant. Off by default.
	metricsAddr         string
	otelEndpoint        string
	otelInsecure        bool
	metricsPushInterval time.Duration
	otelTraceSampling   float64

	manifestDSNFile string
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var o options
	flag.StringVar(&o.addr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&o.domain, "domain", "default", "dedup + ref namespace (the enterprise instance for internal storage)")
	flag.IntVar(&o.packSize, "pack-size", 0, "casstore pack target in bytes (0 = default ~16 MiB)")
	flag.StringVar(&o.compression, "compression", "zstd", "pack-blob compression for compressible content types: zstd|gzip|none (none disables compression entirely — rollback)")
	flag.StringVar(&o.packFraming, "pack-framing", "per-chunk", "framing for COMPRESSED packs: per-chunk (each chunk compressed independently behind an in-pack index, so compressed packs stay range-readable) | whole-pack (one codec stream per pack: better ratio, but any chunk read fetches+decompresses the whole pack — rollback). No effect on packs stored uncompressed (tensor/media classes)")
	flag.DurationVar(&o.gcInterval, "gc-interval", 10*time.Minute, "chunked-GC interval (0 disables)")
	flag.DurationVar(&o.gcSafetyWind, "gc-safety-window", time.Hour, "do not GC blobs younger than this (guards the GC↔in-flight-write race)")
	flag.DurationVar(&o.stagingTTL, "staging-ttl", 24*time.Hour, "reclaim minted-but-never-finalized staging slots older than this (0 disables the staging sweeper)")

	flag.StringVar(&o.backend, "backend", "local", "chunk+manifest storage: local|s3")
	flag.StringVar(&o.dataDir, "data-dir", "./blobgw-data", "local backend data directory")

	flag.StringVar(&o.s3Bucket, "s3-bucket", "", "S3 bucket for chunks+manifests (backend=s3)")
	flag.StringVar(&o.s3Region, "s3-region", "us-east-1", "S3 region")
	flag.StringVar(&o.s3Endpoint, "s3-endpoint", "", "S3 endpoint override (RustFS/R2); empty = AWS")
	flag.StringVar(&o.s3Prefix, "s3-prefix", "blobgw", "S3 key prefix for chunks+manifests")
	flag.BoolVar(&o.s3ForcePathStyle, "s3-force-path-style", false, "force S3 path-style addressing (RustFS)")

	flag.StringVar(&o.index, "index", "memory", "dedup + ref index: memory|postgres")
	flag.StringVar(&o.databaseURL, "database-url", os.Getenv("BLOBGW_DATABASE_URL"), "Postgres DSN (index=postgres); defaults to $BLOBGW_DATABASE_URL")
	// Connection-pool tuning for the Postgres index (index=postgres). Sane
	// defaults bound the pool so a burst of concurrent object ops can't exhaust
	// Postgres connections; override per deployment if needed.
	flag.IntVar(&o.dbMaxOpenConns, "db-max-open-conns", 25, "max open Postgres connections (index=postgres)")
	flag.IntVar(&o.dbMaxIdleConns, "db-max-idle-conns", 5, "max idle Postgres connections (index=postgres)")
	flag.DurationVar(&o.dbConnMaxLifetime, "db-conn-max-lifetime", 30*time.Minute, "max lifetime of a pooled Postgres connection (index=postgres)")

	flag.StringVar(&o.staging, "staging", "memory", "stage-then-finalize backend: none|memory|s3")
	flag.StringVar(&o.stagingBucket, "staging-bucket", "", "S3 bucket for staged uploads (staging=s3)")
	flag.StringVar(&o.stagingPrefix, "staging-prefix", "blobgw-staging", "S3 key prefix for staged uploads")

	// Control plane (NATS face). Defaults keep it OFF so existing HTTP-only
	// behavior is unchanged unless -control-plane is passed.
	flag.BoolVar(&o.controlPlane, "control-plane", false, "enable the NATS control-plane server (ADR §2.5): index/presign/GC RPCs + per-tenant binding")
	flag.StringVar(&o.natsURL, "nats-url", nats.DefaultURL, "NATS server URL (control-plane=true)")
	// Control-plane account selection (ADR-001 §7.7: account-per-domain on the
	// converged control plane). blobgw runs in a SHARED service account that
	// exports the blobgw.*.> service; each tenant account imports only its own
	// blobgw.<tenant>.> prefix, so a node in tenant A's account can reach blobgw
	// for tenant A ONLY and the account boundary — not just app validation —
	// blocks reaching tenant B. These select the credentials that place this
	// blobgw connection in the service account. Both empty (the default) keeps
	// the legacy single-account behavior: the connection authenticates with
	// whatever the URL/server config implies and subscribes to blobgw.*.> in one
	// account. See docs/blobgw-control-plane-accounts.md.
	flag.StringVar(&o.natsCreds, "nats-creds", "", "path to a NATS credentials file selecting the blobgw control-plane (service) account (control-plane=true; ADR §7.7). Empty = legacy single-account.")
	flag.StringVar(&o.natsNKey, "nats-nkey", "", "path to a NATS nkey seed file selecting the blobgw control-plane (service) account (control-plane=true; alternative to -nats-creds). Empty = legacy single-account.")
	flag.StringVar(&o.tenantConfig, "tenant-config", "", "path to the per-tenant resolver records (control-plane=true): EITHER a single JSON file OR a DIRECTORY of per-tenant *.json files (each hot-reloaded, one file per tenant for onboarding); see tenantconfig schema (ADR §2.9). Each record may carry an optional manifestDSN (per-tenant PG manifest routing), superseding -manifest-dsn-file")
	flag.StringVar(&o.tenantSecretsFile, "tenant-secrets-file", "", "optional JSON file mapping credentialRef → {accessKey,secretKey}; env BLOBGW_TENANT_KEY_*/SECRET_* take precedence (credential-mode=static only)")
	flag.StringVar(&o.credentialMode, "credential-mode", string(tenantconfig.CredentialModeStatic), "per-tenant credential source (control-plane=true): static (credentialRef = secret ref; dev + non-AWS prod) | aws-sts (credentialRef = IAM role ARN, STS-assumed via IRSA web-identity token; requires AWS_WEB_IDENTITY_TOKEN_FILE in the pod) (ADR §2.9)")
	flag.DurationVar(&o.credentialCacheTTL, "credential-cache-ttl", tenantconfig.DefaultCacheTTL, "per-tenant credential cache TTL; doubles as the secret-rotation pickup (ADR §2.9). In aws-sts mode the cache also refreshes before STS session expiry")
	flag.DurationVar(&o.presignMaxTTL, "presign-max-ttl", 15*time.Minute, "cap on minted presigned-URL validity (ADR §2.9); effective TTL = min(requested, cap)")

	// Leader-elected per-tenant GC (ADR §2.4 #29, §4 I3). Default OFF; only
	// meaningful with -control-plane (needs the per-tenant router + NATS/JetStream).
	flag.BoolVar(&o.gcLeader, "gc-leader", false, "enable the NATS-leader-elected per-tenant GC sweeper (ADR §2.4 #29); requires -control-plane")
	flag.DurationVar(&o.gcLeaderInterval, "gc-leader-interval", gc.DefaultInterval, "leader-elected GC sweep cadence (gc-leader=true)")
	flag.DurationVar(&o.gcLeaderSafetyWindow, "gc-leader-safety-window", gc.DefaultSafetyWindow, "conservative GC safety window (ADR §4 I3/HC1: hours, default 24h); never reclaim a pack younger than this (gc-leader=true)")
	flag.DurationVar(&o.gcLeaderTTL, "gc-leader-ttl", 30*time.Second, "GC leadership lease TTL over NATS JetStream KV (gc-leader=true)")
	flag.IntVar(&o.gcLeaderReplicas, "gc-leader-replicas", 3, "JetStream replication factor for the GC lease bucket (ADR §6.1 R3 HA; default 3). Single-node embedded JetStream (e.g. tests) caps this at the cluster size, effectively 1 (gc-leader=true)")
	flag.Float64Var(&o.gcCompactMinFill, "gc-compact-min-fill", 0, "repack: consolidate packs filled below this ratio of pack-size (0 = disabled). Runs as the first step of each GC tenant pass (gc-leader=true)")
	flag.Int64Var(&o.gcCompactMinPackBytes, "gc-compact-min-pack-bytes", 0, "repack: consolidate packs smaller than this many bytes regardless of fill (0 = disabled). Coalesces the legacy ~128 KiB streaming-write packs (gc-leader=true)")
	flag.Int64Var(&o.gcCompactMaxBytesPerPass, "gc-compact-max-bytes-per-pass", 0, "repack: cap on live bytes rewritten per tenant per pass (0 = unbounded). Bounds compaction I/O per sweep (gc-leader=true)")
	flag.BoolVar(&o.gcCompactDefrag, "gc-compact-defrag", false, "repack: consolidate in FILE (manifest) order instead of chunk-hash order, so a fragmented object's chunks are laid out contiguously and later sequential reads coalesce into fewer pack GETs (restores read locality dedup/legacy fragmentation destroyed). Costs more memory: holds the selected packs' raw bytes in flight, bounded by -gc-compact-max-bytes-per-pass (gc-leader=true)")
	flag.StringVar(&o.metricsAddr, "metrics-addr", "", "serve a Prometheus /metrics scrape endpoint for the GC+compaction sweeper on this address (e.g. :9101); empty = off (gc-leader=true)")
	flag.StringVar(&o.manifestDSNFile, "manifest-dsn-file", "", "OPTIONAL LEGACY override: path to a JSON map {tenant: postgres-DSN} routing those tenants' GC manifest reads to PostgreSQL — the converged mlfs -remote posture where slice_manifest lives in the tenant's meta DB, not S3 (gc-leader=true). Superseded by a per-record manifestDSN in -tenant-config (which wins when set); kept for back-compat during migration. Tenants absent from the map keep the S3 manifest store. Mount from a k8s Secret.")
	flag.StringVar(&o.otelEndpoint, "otel-endpoint", "", "push GC+compaction metrics to this OTLP/gRPC collector endpoint (host:port) for centralized per-tenant reporting; empty = off (gc-leader=true)")
	flag.BoolVar(&o.otelInsecure, "otel-insecure", false, "use an insecure (no-TLS) OTLP connection (in-cluster / dev collectors)")
	flag.DurationVar(&o.metricsPushInterval, "metrics-push-interval", 10*time.Second, "OTLP push cadence (-otel-endpoint)")
	flag.Float64Var(&o.otelTraceSampling, "otel-trace-sampling", 1.0, "head-sampling ratio for distributed traces (parent-based): 1.0 = every root trace, 0.0 = none. Only effective with -otel-endpoint (traces need a collector); a sampled upstream caller's trace is always continued regardless")
	flag.Parse()

	if o.gcLeader && !o.controlPlane {
		slog.Error("blobgw: -gc-leader requires -control-plane (needs the per-tenant router + NATS)")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	b, err := build(ctx, o)
	if err != nil {
		slog.Error("blobgw: init", "err", err)
		os.Exit(1)
	}
	defer b.cleanup()

	// Optionally bring up the NATS control-plane face (ADR §2.5/§2.9) alongside
	// the HTTP data path. Default OFF: when -control-plane is not passed the
	// daemon is the existing HTTP-only object server, unchanged.
	if o.controlPlane {
		stopCP, err := startControlPlane(ctx, o, b)
		if err != nil {
			slog.Error("blobgw: control-plane init", "err", err)
			os.Exit(1)
		}
		defer stopCP()
	}

	if o.gcInterval > 0 {
		go b.gc.Loop(ctx, o.gcInterval)
	}
	// The staging sweeper shares the chunked-GC interval; -staging-ttl gates how
	// old a pending slot must be before it is reclaimed.
	if b.stagingGC != nil && o.gcInterval > 0 && o.stagingTTL > 0 {
		go b.stagingGC.Loop(ctx, o.gcInterval)
	}

	// Admin maintenance endpoints (pack-size backfill + on-demand per-tenant GC)
	// are wired here, AFTER control-plane setup has populated b.router, so a real
	// per-tenant backfill/GC resolves the tenant's OWN backend through the router
	// (ADR §2.6) rather than the shared/placeholder store. Backfill needs the
	// pgindex Store's PackSizeRecorder (only the postgres index satisfies it);
	// GC needs the per-tenant router (only the -control-plane posture builds one).
	srvOpts := []server.Option{
		server.WithReadiness(b.ready),
		server.WithLogger(slog.Default()),
		server.WithMetrics(b.dataPath),
		server.WithUsage(b.usage),
	}
	if _, ok := b.dedup.(snapshot.PackSizeRecorder); ok {
		srvOpts = append(srvOpts, server.WithBackfill(pgBackfill{
			router: b.router, chunks: b.chunks, rec: b.dedup, logger: slog.Default(),
		}))
	}
	if b.router != nil {
		// Use the leader-elected safety window when configured; else the GC default
		// (never reclaim a pack younger than this — guards the GC↔in-flight-write race).
		safetyWindow := o.gcLeaderSafetyWindow
		if safetyWindow <= 0 {
			safetyWindow = gc.DefaultSafetyWindow
		}
		srvOpts = append(srvOpts, server.WithTenantGC(pgGC{
			router: b.router, safetyWindow: safetyWindow, logger: slog.Default(),
		}))
	}
	srv := &http.Server{Addr: o.addr, Handler: server.New(b.gw, srvOpts...)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("blobgw: shutdown", "err", err)
		}
	}()

	slog.Info("blobgw: listening", "addr", o.addr, "domain", o.domain,
		"backend", o.backend, "index", o.index, "staging", o.staging,
		"gc_interval", o.gcInterval, "control_plane", o.controlPlane)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("blobgw: serve", "err", err)
		os.Exit(1)
	}
	slog.Info("blobgw: stopped")
}

// build assembles the gateway + GC from the selected backends and returns a
// readiness probe (for GET /readyz) plus a cleanup function for any resources
// that need closing (e.g. the DB pool). The readiness probe pings the Postgres
// index (when index=postgres) and does a cheap chunk-backend reachability check.
func build(ctx context.Context, o options) (*built, error) {
	cleanups := []func(){}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(err error) (*built, error) {
		cleanup()
		return nil, err
	}

	// readiness probes accumulate as backends are wired; /readyz runs them all.
	var readyProbes []func(context.Context) error

	// --- runtime metrics (data path + GC/compaction share one provider) ---
	// Gated the same way the GC sweeper's metrics are: OFF unless -metrics-addr or
	// -otel-endpoint is set. When off, reg stays nil and the data-path instruments
	// are built over a no-op meter (Registry.Meter() handles the nil receiver), so
	// every wrap/record below runs unconditionally with zero exporter behind it.
	hostname, _ := os.Hostname()
	nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	var reg *metrics.Registry
	if o.metricsAddr != "" || o.otelEndpoint != "" {
		mopts := []metrics.Option{metrics.WithInstance(nodeID)}
		if o.otelEndpoint != "" {
			mopts = append(mopts, metrics.WithOTLP(o.otelEndpoint, o.otelInsecure, o.metricsPushInterval))
		}
		r, err := metrics.New(ctx, mopts...)
		if err != nil {
			return fail(fmt.Errorf("metrics: %w", err))
		}
		cleanups = append(cleanups, func() { _ = r.Shutdown(context.Background()) })
		reg = r
	}
	meter := reg.Meter() // no-op meter when reg is nil; never branches at call sites

	// --- distributed tracing (the fan-out spans + exemplars) ---
	// Gated more tightly than metrics: traces need a collector to land in, so they
	// require -otel-endpoint specifically (there is no Prometheus-scrape equivalent
	// for a trace). NewTracing registers the provider GLOBALLY and installs a W3C
	// propagator, so the casstore backing spans (casstore.backing.*, opened via the
	// global tracer) and blobgw's own HTTP/control-plane spans are exported and the
	// data-path histograms — recorded within request ctx — pick up trace exemplars.
	// When -otel-endpoint is unset NewTracing returns a nil *Tracing and leaves the
	// global provider as the default no-op, so every span site is a zero-overhead
	// no-op and there is no behavior change with tracing off.
	if o.otelEndpoint != "" {
		tp, err := metrics.NewTracing(ctx,
			metrics.WithInstance(nodeID),
			metrics.WithOTLP(o.otelEndpoint, o.otelInsecure, o.metricsPushInterval),
			metrics.WithTraceSampler(o.otelTraceSampling))
		if err != nil {
			return fail(fmt.Errorf("tracing: %w", err))
		}
		cleanups = append(cleanups, func() { _ = tp.Shutdown(context.Background()) })
	}
	dataPath, err := metrics.NewDataPath(meter)
	if err != nil {
		return fail(fmt.Errorf("metrics data path: %w", err))
	}

	// --- chunk + manifest storage ---
	var upstream snapshot.SnapshotStore
	var chunks blobstore.Storage
	switch o.backend {
	case "local":
		us, err := snapshot.NewLocalStore(filepath.Join(o.dataDir, "manifests"))
		if err != nil {
			return fail(fmt.Errorf("local manifest store: %w", err))
		}
		cs, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: filepath.Join(o.dataDir, "chunks")})
		if err != nil {
			return fail(fmt.Errorf("local chunk store: %w", err))
		}
		cleanups = append(cleanups, func() { _ = cs.Close(context.Background()) })
		// Backing-store RED: wrap the chunk store so every casstore IO records the
		// backing.op.duration / errors / bytes view (docs/OBSERVABILITY.md). The
		// HTTP data path is single-domain (o.domain), so the tenant attribute is the
		// configured domain. No-op meter when metrics are off.
		wrapped, werr := obs.Wrap(cs, meter, "local", attribute.String("tenant", o.domain))
		if werr != nil {
			return fail(fmt.Errorf("wrap local chunk store: %w", werr))
		}
		upstream, chunks = us, wrapped
	case "s3":
		if o.s3Bucket == "" {
			return fail(errors.New("backend=s3 requires -s3-bucket"))
		}
		us, err := snapshot.NewS3Store(ctx, snapshot.S3Config{
			Bucket: o.s3Bucket, Region: o.s3Region, Endpoint: o.s3Endpoint,
			Prefix: o.s3Prefix + "/manifests", ForcePathStyle: o.s3ForcePathStyle,
		})
		if err != nil {
			return fail(fmt.Errorf("s3 manifest store: %w", err))
		}
		// The kopia-backed blobstore wants a scheme-less endpoint (host:port)
		// plus an explicit TLS toggle, whereas the aws-sdk snapshot store above
		// wants the full URL. Translate the chunk-store endpoint accordingly.
		chunkEndpoint, chunkNoTLS := o.s3Endpoint, false
		if rest, ok := strings.CutPrefix(o.s3Endpoint, "http://"); ok {
			chunkEndpoint, chunkNoTLS = rest, true
		} else if rest, ok := strings.CutPrefix(o.s3Endpoint, "https://"); ok {
			chunkEndpoint, chunkNoTLS = rest, false
		}
		cs, err := blobstore.NewS3Storage(ctx, blobstore.S3Config{
			Bucket: o.s3Bucket, Region: o.s3Region, Endpoint: chunkEndpoint,
			Prefix: o.s3Prefix + "/chunks", DoNotUseTLS: chunkNoTLS,
		})
		if err != nil {
			return fail(fmt.Errorf("s3 chunk store: %w", err))
		}
		cleanups = append(cleanups, func() { _ = cs.Close(context.Background()) })
		// Backing-store RED: wrap the S3 chunk store. This is the single biggest
		// observability gap — an object gateway's latency and failures are dominated
		// by S3 (and the auth/error.kind canary catches the STS "token has expired"
		// GC failure). No-op meter when metrics are off.
		wrapped, werr := obs.Wrap(cs, meter, "s3", attribute.String("tenant", o.domain))
		if werr != nil {
			return fail(fmt.Errorf("wrap s3 chunk store: %w", werr))
		}
		upstream, chunks = us, wrapped
	default:
		return fail(fmt.Errorf("unknown -backend %q (want local|s3)", o.backend))
	}

	// Backend reachability probe for /readyz: a cheap blob listing under an
	// unlikely prefix. It exercises the chunk backend's auth + connectivity
	// without reading object data; an empty result is success.
	chunkBackend := chunks
	readyProbes = append(readyProbes, func(ctx context.Context) error {
		err := chunkBackend.ListBlobs(ctx, blobstore.ID("readyz-probe-"), func(blobstore.Metadata) error { return nil })
		if err != nil {
			return fmt.Errorf("chunk backend unreachable: %w", err)
		}
		return nil
	})

	// --- dedup index + ref store ---
	var dedup snapshot.DedupStore
	var refs gateway.RefStore
	// usageSrc backs GET /admin/usage; set only for the postgres index (the
	// memory index has no per-domain physical accounting → endpoint returns 501).
	var usageSrc server.DomainUsager
	switch o.index {
	case "memory":
		dedup = snapshot.NewMemoryDedupStore()
		refs = gateway.NewMemoryRefStore()
	case "postgres":
		if o.databaseURL == "" {
			return fail(errors.New("index=postgres requires -database-url or $BLOBGW_DATABASE_URL"))
		}
		db, err := sql.Open("pgx", o.databaseURL)
		if err != nil {
			return fail(fmt.Errorf("open postgres: %w", err))
		}
		// Bound the connection pool so a burst of concurrent object ops can't
		// exhaust Postgres connections (flag-overridable; sane defaults).
		db.SetMaxOpenConns(o.dbMaxOpenConns)
		db.SetMaxIdleConns(o.dbMaxIdleConns)
		db.SetConnMaxLifetime(o.dbConnMaxLifetime)
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return fail(fmt.Errorf("ping postgres: %w", err))
		}
		pg := pgindex.New(db, pgindex.WithMetrics(dataPath))
		if err := pg.Migrate(ctx); err != nil {
			_ = db.Close()
			return fail(fmt.Errorf("migrate postgres: %w", err))
		}
		// USE saturation view of the index pool (docs/OBSERVABILITY.md): in_use/idle/
		// wait_count gauges off sql.DB.Stats(). No-op when metrics are disabled.
		if err := dataPath.BindPool(meter, "index", db); err != nil {
			_ = db.Close()
			return fail(fmt.Errorf("bind index pool gauges: %w", err))
		}
		cleanups = append(cleanups, func() { _ = db.Close() })
		// Readiness probe: a live ping of the index DB.
		dbHandle := db
		readyProbes = append(readyProbes, func(ctx context.Context) error {
			if err := dbHandle.PingContext(ctx); err != nil {
				return fmt.Errorf("postgres index unreachable: %w", err)
			}
			return nil
		})
		dedup, refs = pg, pg
		usageSrc = pgUsage{pg}
		// Pack-size backfill + on-demand GC adapters are built at the server.New
		// call site (main), once b.router is populated by control-plane setup, so a
		// real per-tenant backfill/GC resolves the tenant's OWN backend through the
		// router. b.dedup (the pgindex Store) is the recorder for the backfill; it
		// satisfies snapshot.PackSizeRecorder (checked at that site).
	default:
		return fail(fmt.Errorf("unknown -index %q (want memory|postgres)", o.index))
	}

	// --- staging backend ---
	var staging gateway.StagingStore
	switch o.staging {
	case "none":
		staging = nil
	case "memory":
		staging = gateway.NewMemoryStagingStore()
	case "s3":
		bucket := o.stagingBucket
		if bucket == "" {
			bucket = o.s3Bucket // reuse the chunk bucket if a dedicated one isn't given
		}
		if bucket == "" {
			return fail(errors.New("staging=s3 requires -staging-bucket or -s3-bucket"))
		}
		st, err := s3stage.New(ctx, s3stage.Config{
			Bucket: bucket, Region: o.s3Region, Endpoint: o.s3Endpoint,
			Prefix: o.stagingPrefix, ForcePathStyle: o.s3ForcePathStyle,
		})
		if err != nil {
			return fail(fmt.Errorf("s3 staging: %w", err))
		}
		staging = st
	default:
		return fail(fmt.Errorf("unknown -staging %q (want none|memory|s3)", o.staging))
	}

	// Compression policy: compress generic/text-like content with the selected
	// codec while storing already-compressed and GPU-loadable content types
	// uncompressed (decided from the object's Content-Type). -compression=none
	// disables it entirely for rollback. Compression is dedup-safe: it only
	// shrinks the physical pack bytes; chunk hashing and dedup stay on the
	// original bytes.
	codec, err := snapshot.ParseCompressionAlgo(o.compression)
	if err != nil {
		return fail(fmt.Errorf("parse -compression: %w", err))
	}
	framing, err := snapshot.ParsePackCompressionMode(o.packFraming)
	if err != nil {
		return fail(fmt.Errorf("parse -pack-framing: %w", err))
	}
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes:   o.packSize,
		DedupDomain:       o.domain,
		Index:             snapshot.NewGlobalIndex(dedup, nil),
		CompressionPolicy: snapshot.NewContentTypePolicy(codec),
		PackCompression:   framing,
	})
	if err != nil {
		return fail(fmt.Errorf("chunked store: %w", err))
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = dedup
	gc.SafetyWindow = o.gcSafetyWind
	gw := gateway.New(cs, refs, staging, o.domain)

	// The staging sweeper only makes sense when a staging backend exists.
	var stagingGC *gateway.StagingGC
	if staging != nil {
		stagingGC = gateway.NewStagingGC(refs, staging, o.domain, o.stagingTTL, nil)
	}

	// Compose the readiness probe: ready only if every backend probe passes.
	probes := readyProbes
	ready := func(ctx context.Context) error {
		for _, p := range probes {
			if err := p(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	return &built{
		gw:        gw,
		gc:        gc,
		stagingGC: stagingGC,
		ready:     ready,
		cleanup:   cleanup,
		dedup:     dedup,
		refs:      refs,
		usage:     usageSrc,
		staging:   staging,
		chunks:    chunks,
		metrics:   reg,
		dataPath:  dataPath,
	}, nil
}

// built bundles the assembled HTTP data-path components (gw/gc/stagingGC/ready)
// with the shared index + ref + staging stores the control-plane wiring reuses.
// The control plane (when enabled) serves index RPCs against the same dedup
// store and routes per-tenant bindings through a TenantRouter over the same
// shared refs/staging — so both faces share one source of truth (ADR §2.5/§3).
type built struct {
	gw        *gateway.Gateway
	gc        *snapshot.ChunkedGC
	stagingGC *gateway.StagingGC
	ready     func(context.Context) error
	cleanup   func()

	dedup   snapshot.DedupStore
	refs    gateway.RefStore
	staging gateway.StagingStore

	// chunks is the DEFAULT/placeholder chunk (pack) store from build(): the
	// single wrapped store on the HTTP data path (single-tenant posture), or a
	// placeholder that the per-tenant router supersedes under -control-plane. The
	// backfill adapter uses it as the fallback when no router is wired (preserving
	// single-tenant behavior); with a router, per-tenant backfill/GC resolve the
	// tenant's OWN backend through the router instead.
	chunks blobstore.Storage

	// router is the per-tenant TenantRouter built during control-plane setup
	// (WithBindingProvider). Non-nil only under -control-plane; it lets the
	// backfill + GC admin adapters resolve each tenant's OWN backend (ADR §2.6),
	// exactly as the leader-elected GC sweeper does. nil in the HTTP-only /
	// single-tenant posture, in which case the adapters fall back to b.chunks.
	router *gateway.TenantRouter

	// usage backs GET /admin/usage. Non-nil only when the selected index exposes
	// per-domain storage accounting (the pgindex Store); nil for the in-memory
	// index, in which case the endpoint returns 501.
	usage server.DomainUsager

	// backfill backs POST /admin/backfill-packs. Non-nil only when the selected
	// index records per-pack sizes (the pgindex Store); nil for the in-memory
	// index, in which case the endpoint returns 501.
	backfill server.PackBackfiller

	// metrics is the daemon's shared instrument registry (GC/compaction + the
	// data-path meter). nil when metrics are disabled (no -metrics-addr/-otel-endpoint),
	// in which case dataPath is built over a no-op meter so call sites never branch.
	metrics  *metrics.Registry
	dataPath *metrics.DataPath
}

// pgUsage adapts *pgindex.Store to server.DomainUsager: it maps the store's
// pgindex.UsageStats to the server's structurally-identical server.DomainUsage,
// so the server package stays free of a pgindex import (it depends only on the
// narrow DomainUsager interface).
type pgUsage struct{ s *pgindex.Store }

func (p pgUsage) DomainUsage(ctx context.Context, domain string) (server.DomainUsage, error) {
	u, err := p.s.DomainUsage(ctx, domain)
	if err != nil {
		return server.DomainUsage{}, err
	}
	return server.DomainUsage{
		PhysicalBytes:       u.PhysicalBytes,
		LogicalDedupedBytes: u.LogicalDedupedBytes,
		ObjectApparentBytes: u.ObjectApparentBytes,
	}, nil
}

// pgBackfill adapts the postgres index + a chunk store to
// server.PackBackfiller: BackfillPacks lists a domain's pack blobs and records
// each pack's on-disk size into rec's PackSizeRecorder (the pgindex Store), so a
// backfill fills the same packs table the data path + GC record into. rec is
// `any` so this stays free of a concrete pgindex type at the field —
// snapshot.BackfillPackSizes type-asserts it to the recorder.
//
// Store selection is tenant-router-aware (ADR §2.6): when router is non-nil (the
// per-tenant-bucket control-plane posture) BackfillPacks resolves the tenant's
// OWN pack store through the router — otherwise it would list the shared
// placeholder backend and record 0 for a real tenant. When router is nil (the
// single-tenant HTTP-only posture) it uses chunks, the wrapped data-path store,
// preserving the pre-router behavior.
type pgBackfill struct {
	router *gateway.TenantRouter
	chunks blobstore.Storage
	rec    any
	logger *slog.Logger
}

func (p pgBackfill) BackfillPacks(ctx context.Context, domain string) (int, error) {
	store := p.chunks
	if p.router != nil {
		s, err := p.router.ChunksForTenant(ctx, domain)
		if err != nil {
			return 0, err
		}
		store = s
	}
	return snapshot.BackfillPackSizes(ctx, store, p.rec, domain, p.logger)
}

// pgGC adapts the per-tenant TenantRouter to server.TenantGCer: RunGCForDomain
// resolves the tenant's own ChunkedGC through the router (its OWN backend +
// the shared dedup index, ADR §2.6), stamps the configured safety window, runs a
// single scoped mark-and-sweep pass, and maps the snapshot.GCResult to the
// narrow server.GCSummary. This is the on-demand ops-maintenance twin of the
// leader-elected scheduled sweeper (which uses the same router.GCForTenant).
type pgGC struct {
	router       *gateway.TenantRouter
	safetyWindow time.Duration
	logger       *slog.Logger
}

func (p pgGC) RunGCForDomain(ctx context.Context, domain string) (server.GCSummary, error) {
	g, err := p.router.GCForTenant(ctx, domain)
	if err != nil {
		return server.GCSummary{}, err
	}
	g.SafetyWindow = p.safetyWindow
	res, err := g.RunOnceForDomain(ctx, domain)
	if err != nil {
		return server.GCSummary{}, err
	}
	return server.GCSummary{
		LiveChunks:      res.LiveChunks,
		ChunksReclaimed: res.ChunksReclaimed,
		BytesReclaimed:  res.BytesReclaimed,
	}, nil
}

// startControlPlane brings up the NATS control-plane face (ADR §2.5/§2.9): it
// connects to NATS, loads the per-tenant credential provider from the
// tenant-config file, starts a controlplane.Server over the shared dedup index,
// and constructs a per-tenant gateway.TenantRouter bound through the same
// provider (the per-tenant direct-I/O path of ADR §2.6). It returns a stop
// function that drains the server subscriptions and the NATS connection; the
// caller defers it for graceful shutdown.
//
// The HTTP data path (b.gw) is untouched — the control plane is an additive,
// opt-in second face sharing the same index + ref + staging stores (ADR §3).
func startControlPlane(ctx context.Context, o options, b *built) (func(), error) {
	if o.tenantConfig == "" {
		return nil, fmt.Errorf("control-plane requires -tenant-config")
	}

	mode, err := tenantconfig.ParseCredentialMode(o.credentialMode)
	if err != nil {
		return nil, err
	}
	// Watched variant: the provider polls the tenant-config file and atomically
	// swaps its tenant→Descriptor map on change, so tenant add/remove takes effect
	// WITHOUT restarting blobgw (the Helm checksum/tenants pod-roll is no longer
	// needed for the bind/presign data path). The initial load still fails fast on
	// a bad config; the polling goroutine is bound to ctx and exits on shutdown.
	provider, err := tenantconfig.LoadProviderModeWatched(ctx, mode, o.tenantConfig, o.tenantSecretsFile, o.credentialCacheTTL)
	if err != nil {
		return nil, fmt.Errorf("load tenant provider: %w", err)
	}

	// Parse the tenant-config file directly too (the provider hides it) so the
	// leader-elected GC sweeper, when enabled, can enumerate the tenant set. NOTE:
	// this GC-set enumeration is a static one-shot snapshot — the restart-free
	// reload above covers only the per-request bind/presign data path, not the
	// GC-leader tenant sweep set. Adding/removing a tenant's GC coverage still
	// requires a restart (GC leadership is a rare, single-replica concern).
	tenantFile, err := tenantconfig.LoadPath(o.tenantConfig)
	if err != nil {
		return nil, fmt.Errorf("load tenant config: %w", err)
	}

	connOpts := []nats.Option{
		nats.Name("blobgw-control-plane"),
		nats.MaxReconnects(-1),
	}
	// ADR §7.7: select the blobgw control-plane (service) account via creds/nkey.
	// When both are empty the connection stays in the legacy single-account model
	// (auth implied by the URL/server config). Mirrors mlfs coord's -nats-creds
	// account-selection seam (mlfs/cmd/mlfs/coord.go).
	acctOpts, err := natsAccountOptions(o.natsCreds, o.natsNKey)
	if err != nil {
		return nil, err
	}
	connOpts = append(connOpts, acctOpts...)

	nc, err := nats.Connect(o.natsURL, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats %q: %w", o.natsURL, err)
	}

	cp, err := controlplane.NewServer(nc, b.dedup, provider,
		controlplane.WithLogger(slog.Default()),
		controlplane.WithPresignMaxTTL(o.presignMaxTTL),
		controlplane.WithMetrics(b.dataPath),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("build control-plane server: %w", err)
	}
	if err := cp.Start(ctx); err != nil {
		nc.Close()
		return nil, fmt.Errorf("start control-plane server: %w", err)
	}

	// Build the per-tenant TenantRouter ONCE, here in control-plane setup where the
	// provider + manifest-DSN routing exist, and stash it on b.router so BOTH the
	// admin backfill/GC endpoints (server.New, on every control-plane daemon) AND
	// the leader-elected GC sweeper (below, gc-leader only) resolve each tenant's
	// OWN backend through the same router (ADR §2.6). It shares the daemon's
	// dedup/ref/staging stores, so it purges/records the same index the data path
	// writes through. Each tenant's S3 chunk store is wrapped for the backing-store
	// RED view (the daemon's shared meter; a no-op meter when metrics are off).
	//
	// Manifest DSN routing (converged mlfs -remote): tenants listed here have their
	// slice manifests in their own meta DB, not S3, so per-tenant GC reads the live
	// set from PostgreSQL. Absent tenants keep the S3 manifest store. Empty file =
	// all-S3 (the object-gateway posture).
	manifestDSNs, err := loadManifestDSNs(o.manifestDSNFile)
	if err != nil {
		if cerr := cp.Close(); cerr != nil {
			slog.Warn("blobgw: control-plane close", "err", cerr)
		}
		nc.Close()
		return nil, fmt.Errorf("load manifest DSNs: %w", err)
	}
	routerFraming, err := snapshot.ParsePackCompressionMode(o.packFraming)
	if err != nil {
		return nil, fmt.Errorf("parse -pack-framing: %w", err)
	}
	b.router = gateway.NewTenantRouter(nil, nil, b.dedup, b.refs, b.staging, o.packSize,
		gateway.WithBindingProvider(provider, manifestDSNs),
		gateway.WithPackCompression(routerFraming),
		gateway.WithBackingMeter(b.metrics.Meter()))

	// Optionally start the leader-elected per-tenant GC sweeper (ADR §2.4 #29,
	// §4 I3). It runs over the SAME per-tenant TenantRouter (b.router) built above,
	// sharing the daemon's dedup/ref/staging stores, so the GC purges the same
	// index the data path writes through. Only ONE replica sweeps at a time (NATS
	// leader election); followers stand by.
	var stopGC func()
	if o.gcLeader {
		stopGC, err = startGCRunner(ctx, o, b, tenantFile, nc)
		if err != nil {
			if cerr := cp.Close(); cerr != nil {
				slog.Warn("blobgw: control-plane close", "err", cerr)
			}
			nc.Close()
			return nil, fmt.Errorf("start gc runner: %w", err)
		}
	}

	slog.Info("blobgw: control-plane up", "nats_url", o.natsURL,
		"nats_account_creds", o.natsCreds != "" || o.natsNKey != "",
		"tenant_config", o.tenantConfig, "credential_mode", string(mode),
		"credential_cache_ttl", o.credentialCacheTTL,
		"presign_max_ttl", o.presignMaxTTL, "gc_leader", o.gcLeader)

	stop := func() {
		if stopGC != nil {
			stopGC()
		}
		// Release the per-tenant router's PG manifest DB pools (no-op for the S3-only
		// posture). The router is owned here now (built in control-plane setup and
		// shared by the admin endpoints + the GC sweeper), so it is closed here — the
		// GC runner no longer owns it.
		if cerr := b.router.Close(); cerr != nil {
			slog.Warn("blobgw: tenant router close", "err", cerr)
		}
		if err := cp.Close(); err != nil {
			slog.Warn("blobgw: control-plane close", "err", err)
		}
		// Drain flushes pending publishes and unsubscribes before closing, so no
		// in-flight reply is dropped and no goroutine is leaked. Fall back to
		// Close if Drain errors (e.g. partitioned NATS) to bound exit latency.
		if err := nc.Drain(); err != nil {
			slog.Warn("blobgw: nats drain", "err", err)
			nc.Close()
		}
	}
	return stop, nil
}

// startGCRunner brings up the leader-elected per-tenant GC sweeper (ADR §2.4
// #29, §4 I3). It REUSES the per-tenant TenantRouter (b.router) built in
// control-plane setup — sharing the daemon's dedup/ref/staging stores — elects a
// GC leader over NATS JetStream KV, and runs a gc.Runner that — only on the
// leader — sweeps each configured tenant's pack store with the conservative
// safety window (HC1, default 24h). It returns a stop function that cancels the
// runner (releasing the lease) and waits for it; the router itself is owned (and
// closed) by control-plane setup, not here.
//
// HC2 (node-side write-back-backlog fail-safe) is the mlfs-node-side dependency
// that makes the finite window sufficient (ADR §4); it is OUT of scope here.
func startGCRunner(ctx context.Context, o options, b *built,
	tenantFile *tenantconfig.File, nc *nats.Conn) (func(), error) {

	// Reuse the per-tenant router built once in control-plane setup (shared by the
	// admin backfill/GC endpoints), so GC purges the same dedup index writes go
	// through and there is a single per-tenant backend cache.
	router := b.router

	hostname, _ := os.Hostname()
	nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	// GC/compaction outcomes record onto the daemon's SHARED registry (built in
	// build(), tagged with this node's service.instance.id), so the data path and
	// the sweeper push to one OTLP collector / one Prometheus scrape. nil when
	// metrics are disabled — gc.RunnerConfig.Metrics is nil-safe.
	mreg := b.metrics

	leader, err := gc.NewLeader(ctx, nc, gc.LeaderConfig{
		Key:    "gc",
		NodeID: nodeID,
		TTL:    o.gcLeaderTTL,
		// Replicate the lease bucket for HA (ADR §6.1 R3): default 3 so a single
		// NATS node loss can't drop the GC leadership lease. Single-node embedded
		// JetStream (dev/test) effectively runs this at 1, since the replication
		// factor is capped at the cluster size.
		Replicas: o.gcLeaderReplicas,
	})
	if err != nil {
		return nil, fmt.Errorf("gc leader: %w", err)
	}

	runner, err := gc.NewRunner(leader, router.GCForTenant, tenantFile.TenantNames, gc.RunnerConfig{
		Interval:     o.gcLeaderInterval,
		SafetyWindow: o.gcLeaderSafetyWindow,
		Logger:       slog.Default(),
		// Repack runs as the first step of each tenant pass; it is a no-op unless a
		// trigger threshold is set (min-fill or min-pack-bytes), so wiring the
		// compactor is safe even when compaction is disabled. NewRunner copies the
		// GC safety window into Compact so freshly written consolidated packs are
		// never themselves re-selected.
		CompactFunc: router.CompactorForTenant,
		Compact: snapshot.CompactConfig{
			MinFillRatio:    o.gcCompactMinFill,
			MinPackBytes:    o.gcCompactMinPackBytes,
			MaxBytesPerPass: o.gcCompactMaxBytesPerPass,
			Defrag:          o.gcCompactDefrag,
		},
		Metrics: mreg,
	})
	if err != nil {
		// mreg is the daemon's shared registry (owned by build() cleanup) — don't
		// shut it down here; the caller's cleanup will.
		return nil, fmt.Errorf("gc runner: %w", err)
	}

	// Optional Prometheus /metrics scrape endpoint over the same registry.
	var stopHTTP func()
	if o.metricsAddr != "" {
		stopHTTP = startGCMetricsHTTP(o.metricsAddr, mreg)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runner.Run(runCtx)
	}()

	compactOn := o.gcCompactMinFill > 0 || o.gcCompactMinPackBytes > 0
	slog.Info("blobgw: gc runner up", "node_id", nodeID,
		"interval", o.gcLeaderInterval, "safety_window", o.gcLeaderSafetyWindow,
		"leader_ttl", o.gcLeaderTTL, "compact", compactOn,
		"compact_min_fill", o.gcCompactMinFill, "compact_min_pack_bytes", o.gcCompactMinPackBytes,
		"compact_defrag", o.gcCompactDefrag,
		"metrics_scrape", o.metricsAddr, "otlp_endpoint", o.otelEndpoint)

	stop := func() {
		cancel()
		<-done
		if stopHTTP != nil {
			stopHTTP()
		}
		// The metrics registry is owned (and Shutdown) by build()'s cleanup now that
		// the data path shares it — don't double-shutdown here. The per-tenant router
		// (b.router) is owned + closed by control-plane setup, so it is NOT closed here.
	}
	return stop, nil
}

// loadManifestDSNs reads a JSON map {tenant: postgres-DSN} from path. An empty
// path returns nil (the all-S3 posture: no tenant routes its manifests to PG).
//
// Each DSN is passed through os.ExpandEnv, so the mounted file may carry
// ${VAR}/$VAR credential placeholders (e.g. ${PGPASS}) injected at runtime from a
// Secret. This keeps the password out of git: the file can ship as a ConfigMap
// of {"tenant":"postgresql://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/db"} while
// the actual creds arrive via env (the same pattern blobgw uses for
// BLOBGW_DATABASE_URL). Expansion runs BEFORE the empty-DSN check so an unset or
// empty variable fails loudly rather than silently demoting a converged tenant to
// the S3 fallback (which would make GC read an empty live set and reclaim live
// chunks after the safety window).
func loadManifestDSNs(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse %s as {tenant: dsn} JSON: %w", path, err)
	}
	for tenant, dsn := range m {
		if tenant == "" {
			return nil, fmt.Errorf("%s: empty tenant key", path)
		}
		dsn = os.ExpandEnv(dsn)
		if dsn == "" {
			return nil, fmt.Errorf("%s: empty DSN for tenant %q (after env expansion — check the injected PG* vars)", path, tenant)
		}
		m[tenant] = dsn
	}
	return m, nil
}

// startGCMetricsHTTP serves a Prometheus /metrics scrape endpoint on addr from the
// GC+compaction registry. Returns a stop function that drains the server.
func startGCMetricsHTTP(addr string, mreg *metrics.Registry) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", mreg.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("blobgw: gc metrics http server", "addr", addr, "err", err)
		}
	}()
	slog.Info("blobgw: gc metrics scrape endpoint up", "addr", addr, "path", "/metrics")
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}
}

// natsAccountOptions builds the nats.Option set that selects the blobgw
// control-plane (service) account (ADR-001 §7.7). It mirrors mlfs coord's
// account-selection seam: -nats-creds points at a JWT+nkey credentials file
// (operator/JWT mode, the production path), -nats-nkey at a bare nkey seed file
// (signed-nkey accounts). At most one may be set. When both are empty it returns
// no options, leaving the connection in the legacy single-account model (auth
// implied by the URL / server config) for back-compat.
func natsAccountOptions(credsFile, nkeyFile string) ([]nats.Option, error) {
	switch {
	case credsFile != "" && nkeyFile != "":
		return nil, fmt.Errorf("control-plane: set only one of -nats-creds / -nats-nkey")
	case credsFile != "":
		if _, err := os.Stat(credsFile); err != nil {
			return nil, fmt.Errorf("control-plane: creds file %q: %w", credsFile, err)
		}
		return []nats.Option{nats.UserCredentials(credsFile)}, nil
	case nkeyFile != "":
		opt, err := nats.NkeyOptionFromSeed(nkeyFile)
		if err != nil {
			return nil, fmt.Errorf("control-plane: load nkey seed %q: %w", nkeyFile, err)
		}
		return []nats.Option{opt}, nil
	default:
		return nil, nil
	}
}
