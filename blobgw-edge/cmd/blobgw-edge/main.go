// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command blobgw-edge runs the L1.3 externally-facing capability edge in front
// of internal blobgw.
//
// This entrypoint wires the dev stack: a local-filesystem TenantRouter (so each
// tenant gets its own dedup domain over shared casstore backends), an Ed25519
// capability minter/verifier, header-asserted identity, and in-memory quota /
// rate-limit / revocation. Production swaps the identity provider for the
// real auth proxy, loads persistent Ed25519 keys, and points the router at S3
// + Postgres-backed blobgw backends.
package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/gc"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := flag.String("addr", ":8090", "HTTP listen address")
	dataDir := flag.String("data-dir", "./blobgw-edge-data", "local data directory (backend=local)")
	audience := flag.String("audience", "blobgw-edge", "capability audience (this edge's identity)")
	packSize := flag.Int("pack-size", 0, "casstore pack target in bytes (0 = default)")

	// --- data-path backend selection (mirrors blobgw's build(); same flag
	// names/help so the Helm chart can reuse the values). backend=local (default)
	// preserves today's dev path EXACTLY (local-filesystem shared-backend router).
	// backend=s3 builds the production per-tenant bucket-binding router over the
	// shared pgindex dedup+ref index + tenant-config provider (ADR §2.6). ---
	backend := flag.String("backend", "local", "data-path backend: local|s3 (local = today's dev path; s3 = per-tenant bucket-binding router over shared pgindex + tenant-config)")
	index := flag.String("index", "memory", "shared dedup + ref index (backend=s3): memory|postgres")
	indexDatabaseURL := flag.String("index-database-url", os.Getenv("BLOBGW_DATABASE_URL"), "Postgres DSN for the SHARED blobgw dedup+ref index (index=postgres); defaults to $BLOBGW_DATABASE_URL. Separate from -database-url (the edge POLICY DB), which may point at a different database; when this is empty in index=postgres, falls back to -database-url.")
	staging := flag.String("staging", "none", "stage-then-finalize backend (backend=s3): none|memory|s3 (edge uses the direct-Put path, so none is the default)")
	stagingBucket := flag.String("staging-bucket", "", "S3 bucket for staged uploads (staging=s3); empty reuses -s3-bucket")
	stagingPrefix := flag.String("staging-prefix", "blobgw-staging", "S3 key prefix for staged uploads (staging=s3)")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint override (RustFS/R2) for the staging store (backend=s3); empty = AWS")
	s3Bucket := flag.String("s3-bucket", "", "default S3 bucket reused by -staging-bucket when unset (backend=s3). Per-tenant chunk buckets come from -tenant-config, not this flag.")
	s3Region := flag.String("s3-region", "us-east-1", "S3 region for the staging store (backend=s3)")
	s3Prefix := flag.String("s3-prefix", "blobgw", "S3 key prefix (backend=s3; reserved for parity with blobgw, used by the staging store when a dedicated prefix is not set)")
	s3ForcePathStyle := flag.Bool("s3-force-path-style", false, "force S3 path-style addressing for the staging store (RustFS)")
	tenantConfig := flag.String("tenant-config", "", "path to the per-tenant resolver records binding each tenant to its own S3 bucket (backend=s3, REQUIRED): EITHER a single JSON file OR a DIRECTORY of per-tenant *.json files (each hot-reloaded, one file per tenant for onboarding); see tenantconfig schema (ADR §2.9). Each record may carry an optional manifestDSN (per-tenant PG manifest routing), superseding -manifest-dsn-file")
	tenantSecretsFile := flag.String("tenant-secrets-file", "", "optional JSON file mapping credentialRef → {accessKey,secretKey}; env BLOBGW_TENANT_KEY_*/SECRET_* take precedence (credential-mode=static only)")
	credentialMode := flag.String("credential-mode", string(tenantconfig.CredentialModeStatic), "per-tenant credential source (backend=s3): static (credentialRef = secret ref; dev + non-AWS prod) | aws-sts (credentialRef = IAM role ARN, STS-assumed via IRSA web-identity token) (ADR §2.9)")
	credentialCacheTTL := flag.Duration("credential-cache-ttl", tenantconfig.DefaultCacheTTL, "per-tenant credential cache TTL; doubles as the secret-rotation pickup (ADR §2.9)")
	manifestDSNFile := flag.String("manifest-dsn-file", "", "OPTIONAL LEGACY override: path to a JSON map {tenant: postgres-DSN} routing those tenants' manifest reads to PostgreSQL — the converged mlfs -remote posture (TECH_DEBT #50, backend=s3). Superseded by a per-record manifestDSN in -tenant-config (which wins when set); kept for back-compat during migration. Absent tenants keep the S3 manifest store. Mount from a k8s Secret.")
	dbMaxOpenConns := flag.Int("db-max-open-conns", 25, "max open Postgres connections for the shared index (index=postgres)")
	dbMaxIdleConns := flag.Int("db-max-idle-conns", 5, "max idle Postgres connections for the shared index (index=postgres)")
	dbConnMaxLifetime := flag.Duration("db-conn-max-lifetime", 30*time.Minute, "max lifetime of a pooled shared-index Postgres connection (index=postgres)")
	maxObjectSize := flag.Int64("max-object-size", 0, "global upload ceiling in bytes (0 = unlimited)")
	existenceMode := flag.String("existence-mode", edge.ModeTrusted, "trusted|confidential")
	defaultTTL := flag.Duration("default-ttl", 15*time.Minute, "default capability TTL")
	defaultRequireAuth := flag.Bool("default-require-auth", false, "auth-binding default when a mint request omits require_auth (THIS PHASE: false = shareable-by-default). The minter always stamps the resolved value onto the token explicitly.")
	shareableMaxTTL := flag.Duration("shareable-max-ttl", 5*time.Minute, "TTL clamp for shareable (require_auth=false) capabilities; a leaked bearer URL is short-lived by construction (0 disables the clamp)")
	authBoundMaxTTL := flag.Duration("auth-bound-max-ttl", 60*time.Minute, "TTL ceiling for auth-bound (require_auth=true) capabilities, which can safely carry longer TTLs (0 disables the clamp)")
	gcInterval := flag.Duration("gc-interval", 10*time.Minute, "chunked-GC interval (0 disables)")
	stagingGCTTL := flag.Duration("staging-gc-ttl", 1*time.Hour, "per-tenant StagingGC grace window (#33): a minted-but-never-finalized staged upload older than this is reclaimed (its pending ref row + staged bytes). Must exceed the longest legitimate mint→finalize gap. Only applies when the backend has a staging store (backend=local always; backend=s3 with -staging!=none).")
	stagingGCInterval := flag.Duration("staging-gc-interval", 15*time.Minute, "single-pass per-tenant StagingGC sweep cadence, DECOUPLED from -gc-interval. The edge defers chunk GC to blobgw (-gc-interval=0), but blobgw does NOT sweep the edge's staging area, so the staging sweep must run on its OWN cadence or minted-but-never-finalized staged objects leak forever. Only used on the non-leader (single-pass) path when a staging store exists; the -gc-leader path sweeps staging on the leader cadence. <=0 disables it.")
	gcLeader := flag.Bool("gc-leader", false, "enable the NATS-leader-elected per-tenant GC sweeper (mirrors blobgw): exactly one replica sweeps at a time. false = the single-pass in-process Loop (single-replica dev/default).")
	natsURL := flag.String("nats-url", nats.DefaultURL, "NATS server URL (gc-leader=true): the JetStream cluster holding the GC leadership lease")
	natsCreds := flag.String("nats-creds", "", "path to a NATS credentials file selecting the edge GC control-plane (service) account (gc-leader=true). Empty = legacy single-account.")
	natsNKey := flag.String("nats-nkey", "", "path to a NATS nkey seed file selecting the edge GC control-plane account (gc-leader=true; alternative to -nats-creds). Empty = legacy single-account.")
	gcLeaderTTL := flag.Duration("gc-leader-ttl", 30*time.Second, "GC leadership lease TTL over NATS JetStream KV (gc-leader=true)")
	gcLeaderInterval := flag.Duration("gc-leader-interval", gc.DefaultInterval, "leader-elected GC sweep cadence (gc-leader=true)")
	gcLeaderSafetyWindow := flag.Duration("gc-leader-safety-window", gc.DefaultSafetyWindow, "conservative GC safety window (hours, default 24h); never reclaim a pack younger than this (gc-leader=true)")
	gcLeaderReplicas := flag.Int("gc-leader-replicas", 3, "JetStream replication factor for the GC lease bucket (HA; default 3). Single-node embedded JetStream (e.g. dev) effectively caps this at the cluster size (gc-leader=true)")
	keySeedHex := flag.String("key-seed-hex", "", "32-byte hex Ed25519 seed (dev: stable keys across restarts; empty = ephemeral)")
	policy := flag.String("policy", "memory", "policy state backend (quota/rate-limit/revocation): memory|postgres")
	databaseURL := flag.String("database-url", os.Getenv("BLOBGW_EDGE_DATABASE_URL"), "Postgres DSN (policy=postgres); defaults to $BLOBGW_EDGE_DATABASE_URL")
	quotaMaxBytes := flag.Int64("quota-max-bytes", 0, "per-tenant storage ceiling in bytes (0 = unlimited)")
	quotaMaxObjects := flag.Int64("quota-max-objects", 0, "per-tenant object-count ceiling (0 = unlimited)")
	ratePerSec := flag.Float64("rate-per-sec", 0, "per-key token-bucket refill rate (0 disables rate limiting)")
	rateBurst := flag.Float64("rate-burst", 0, "per-key token-bucket burst capacity")
	revocationTTL := flag.Duration("revocation-ttl", 24*time.Hour, "how long a revoked jti is honored before pruning (>= max capability TTL)")
	metricsAddr := flag.String("metrics-addr", "", "serve a Prometheus /metrics scrape endpoint on this address (e.g. :9102); empty = off")
	otelEndpoint := flag.String("otel-endpoint", "", "push edge metrics + traces to this OTLP/gRPC collector endpoint (host:port); empty = off")
	otelInsecure := flag.Bool("otel-insecure", false, "use an insecure (no-TLS) OTLP connection (in-cluster / dev collectors)")
	metricsPushInterval := flag.Duration("metrics-push-interval", 10*time.Second, "OTLP metrics push cadence (-otel-endpoint)")
	otelTraceSampling := flag.Float64("otel-trace-sampling", 1.0, "head-sampling ratio for distributed traces (parent-based): 1.0 = every root trace, 0.0 = none. Only effective with -otel-endpoint (traces need a collector); a sampled upstream caller's trace is always continued regardless")
	flag.Parse()

	priv, kid, err := loadOrGenerateKey(*keySeedHex)
	if err != nil {
		slog.Error("blobgw-edge: key init", "err", err)
		os.Exit(1)
	}
	minter := capability.NewMinter(kid, priv, *audience)
	verifier := capability.NewVerifier(*audience)
	verifier.AddKey(kid, priv.Public().(ed25519.PublicKey))

	deps, pgRevocation, tenantLister, closePolicy, err := buildPolicy(context.Background(), policyOptions{
		backend:       *policy,
		databaseURL:   *databaseURL,
		maxBytes:      *quotaMaxBytes,
		maxObjects:    *quotaMaxObjects,
		ratePerSec:    *ratePerSec,
		rateBurst:     *rateBurst,
		revocationTTL: *revocationTTL,
	})
	if err != nil {
		slog.Error("blobgw-edge: policy init", "err", err)
		os.Exit(1)
	}
	defer closePolicy()
	deps.Identity = edge.HeaderIdentityProvider{}
	deps.Audit = edge.SlogAuditSink{Logger: slog.Default()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- observability (metrics + traces + exemplars) ---
	// Reuse blobgw's OTEL registry/tracer (blobgw-edge already imports blobgw), so
	// the edge emits onto the SAME provider shape as the layers behind it. Gated
	// exactly like blobgw: OFF unless -metrics-addr or -otel-endpoint is set. When
	// off, reg stays nil and the edge instruments are built over a no-op meter
	// (Registry.Meter() handles the nil receiver) + the global no-op tracer, so
	// every record/span site is a zero-overhead no-op with no behavior change.
	var shutdowns []func()
	hostname, _ := os.Hostname()
	nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	var reg *metrics.Registry
	if *metricsAddr != "" || *otelEndpoint != "" {
		mopts := []metrics.Option{metrics.WithInstance(nodeID), metrics.WithServiceName("blobgw-edge")}
		if *otelEndpoint != "" {
			mopts = append(mopts, metrics.WithOTLP(*otelEndpoint, *otelInsecure, *metricsPushInterval))
		}
		r, err := metrics.New(context.Background(), mopts...)
		if err != nil {
			slog.Error("blobgw-edge: metrics init", "err", err)
			os.Exit(1)
		}
		reg = r
		shutdowns = append(shutdowns, func() { _ = r.Shutdown(context.Background()) })
	}
	// Traces need a collector to land in, so they require -otel-endpoint
	// specifically (no Prometheus-scrape equivalent for a trace). NewTracing
	// registers the provider GLOBALLY + a W3C propagator, so the casstore backing
	// spans and the edge's own spans are exported and the request/mint histograms —
	// recorded within span ctx — pick up trace exemplars. Unset endpoint → nil
	// *Tracing, global no-op provider.
	if *otelEndpoint != "" {
		tp, err := metrics.NewTracing(context.Background(),
			metrics.WithInstance(nodeID),
			metrics.WithServiceName("blobgw-edge"),
			metrics.WithOTLP(*otelEndpoint, *otelInsecure, *metricsPushInterval),
			metrics.WithTraceSampler(*otelTraceSampling))
		if err != nil {
			slog.Error("blobgw-edge: tracing init", "err", err)
			os.Exit(1)
		}
		shutdowns = append(shutdowns, func() { _ = tp.Shutdown(context.Background()) })
	}
	edgeMetrics, err := edge.NewMetrics(reg.Meter()) // no-op meter when reg is nil
	if err != nil {
		slog.Error("blobgw-edge: edge instruments", "err", err)
		os.Exit(1)
	}

	// Assemble the data-path backend now that reg exists (the s3 posture wires the
	// per-tenant S3 chunk stores' backing meter off reg.Meter()). backend=local
	// (default) is unchanged from before; backend=s3 builds the production
	// per-tenant router over the shared pgindex + tenant-config provider. The index
	// DSN defaults to $BLOBGW_DATABASE_URL but falls back to the edge policy DSN
	// (-database-url) when unset, so a single-DSN deployment works while a split
	// (separate policy DB vs shared index DB) is still expressible.
	indexDSN := *indexDatabaseURL
	if indexDSN == "" {
		indexDSN = *databaseURL
	}
	b, err := buildBackend(ctx, backendOptions{
		backend:            *backend,
		dataDir:            *dataDir,
		packSize:           *packSize,
		index:              *index,
		indexDatabaseURL:   indexDSN,
		dbMaxOpenConns:     *dbMaxOpenConns,
		dbMaxIdleConns:     *dbMaxIdleConns,
		dbConnMaxLifetime:  *dbConnMaxLifetime,
		tenantConfig:       *tenantConfig,
		tenantSecretsFile:  *tenantSecretsFile,
		credentialMode:     *credentialMode,
		credentialCacheTTL: *credentialCacheTTL,
		manifestDSNFile:    *manifestDSNFile,
		staging:            *staging,
		stagingBucket:      *stagingBucket,
		stagingPrefix:      *stagingPrefix,
		s3Endpoint:         *s3Endpoint,
		s3Bucket:           *s3Bucket,
		s3Region:           *s3Region,
		s3Prefix:           *s3Prefix,
		s3ForcePathStyle:   *s3ForcePathStyle,
	}, reg)
	if err != nil {
		slog.Error("blobgw-edge: backend init", "err", err)
		os.Exit(1)
	}
	defer b.cleanup()

	// s3 mode is the per-tenant bucket-binding posture: there is no single shared
	// chunk/manifest store to sweep in-process, so router.GC() is nil and GC MUST
	// run leader-elected per tenant (router.GCForTenant). Fail fast rather than
	// silently never reclaiming.
	if !b.inProcessGC && *gcInterval > 0 && !*gcLeader {
		slog.Error("blobgw-edge: backend=s3 requires -gc-leader for GC (per-tenant posture has no in-process sweep); set -gc-leader or -gc-interval=0")
		os.Exit(1)
	}

	srv := edge.New(b.router, minter, verifier, deps, edge.Config{
		Audience:           *audience,
		MaxObjectSize:      *maxObjectSize,
		DefaultTTL:         *defaultTTL,
		ExistenceMode:      *existenceMode,
		DefaultRequireAuth: *defaultRequireAuth,
		ShareableMaxTTL:    *shareableMaxTTL,
		AuthBoundMaxTTL:    *authBoundMaxTTL,
	}, edge.WithMetrics(edgeMetrics))

	// Serve the Prometheus scrape endpoint (mirrors blobgw's startGCMetricsHTTP).
	if reg != nil && *metricsAddr != "" {
		stopMetricsHTTP := startMetricsHTTP(*metricsAddr, reg)
		shutdowns = append(shutdowns, stopMetricsHTTP)
	}
	// Flush/stop the OTEL providers (+ metrics HTTP) on daemon shutdown, in
	// reverse order of registration.
	defer func() {
		for i := len(shutdowns) - 1; i >= 0; i-- {
			shutdowns[i]()
		}
	}()
	// GC has two modes. -gc-leader=true elects a single sweeper over NATS
	// JetStream KV and runs a per-tenant gc.Runner (mirroring blobgw), so exactly
	// one replica sweeps at a time in a multi-replica deployment. The default
	// single-pass in-process Loop stays for dev/single-replica: it is correct only
	// with one replica (every replica would sweep), hence the leader mode for HA.
	//
	// The edge has NO static tenant list (tenants arrive dynamically via
	// capabilities), so the runner is fed a dynamic enumeration from the policy
	// quota store (edge.TenantLister) — the natural "tenants with data" source,
	// re-read each pass so a tenant that stored its first object mid-run is swept
	// on the next sweep.
	//
	// TECH_DEBT #33 (per-tenant StagingGC): NOW WIRED. The edge has adopted the
	// stage-then-finalize path (POST /staged → POST /finalize, op=STAGE/FINALIZE),
	// so a minted-but-never-finalized upload leaves a pending blob_ref row + a
	// staged object that leak without a sweep. A per-tenant gateway.StagingGC is
	// wired ALONGSIDE the chunked-pack GC below — under the SAME leader (so only
	// one replica sweeps), over the SAME dynamic TenantLister (one StagingGC.RunOnce
	// per tenant), and in the single-pass dev mode too. It is skipped only when the
	// backend has no staging store (backend=s3 -staging=none), where there is
	// nothing to reclaim (b.staging == nil).
	switch {
	case *gcLeader:
		stopGC, err := startGCRunner(ctx, gcRunnerOptions{
			router:       b.router,
			tenants:      tenantLister,
			reg:          reg,
			natsURL:      *natsURL,
			natsCreds:    *natsCreds,
			natsNKey:     *natsNKey,
			nodeID:       nodeID,
			ttl:          *gcLeaderTTL,
			interval:     *gcLeaderInterval,
			safetyWindow: *gcLeaderSafetyWindow,
			replicas:     *gcLeaderReplicas,
			// Per-tenant StagingGC (#33): swept on the SAME leader + TenantLister as
			// the chunked runner. nil staging store → no staging sweep.
			stagingRefs:  b.refs,
			stagingStore: b.staging,
			stagingTTL:   *stagingGCTTL,
		})
		if err != nil {
			slog.Error("blobgw-edge: gc leader init", "err", err)
			os.Exit(1)
		}
		shutdowns = append(shutdowns, stopGC)
	case *gcInterval > 0 && b.inProcessGC:
		slog.Info("blobgw-edge: single-pass in-process GC (single-replica; set -gc-leader for multi-replica HA)", "interval", *gcInterval)
		go b.router.GC().Loop(ctx, *gcInterval)
	}
	// StagingGC runs on its OWN cadence (-staging-gc-interval), DECOUPLED from the
	// chunk-GC -gc-interval: the edge sets -gc-interval=0 to defer chunk GC to blobgw
	// (which sweeps the shared chunk store), but blobgw does NOT sweep the edge's
	// staging area, so minted-but-never-finalized staged objects would leak forever.
	// The -gc-leader path already sweeps staging inside startGCRunner (on the leader);
	// this covers every non-leader (single-pass) replica whenever a staging store
	// exists - independent of whether chunk GC is enabled here.
	if !*gcLeader && b.staging != nil && *stagingGCInterval > 0 {
		go stagingGCLoop(ctx, b.refs, b.staging, tenantLister, *stagingGCTTL, *stagingGCInterval)
	}
	// Prune lapsed jti denylist entries under BOTH modes (leader or single-pass),
	// on the effective GC cadence, so the PG denylist table stays bounded
	// regardless of how GC runs.
	if pgRevocation != nil {
		pruneInterval := *gcInterval
		if *gcLeader {
			pruneInterval = *gcLeaderInterval
		}
		if pruneInterval > 0 {
			go revocationPruneLoop(ctx, pgRevocation, pruneInterval)
		}
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	slog.Info("blobgw-edge: listening", "addr", *addr, "audience", *audience,
		"existence_mode", *existenceMode, "kid", kid)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("blobgw-edge: serve", "err", err)
		os.Exit(1)
	}
	slog.Info("blobgw-edge: stopped")
}

type policyOptions struct {
	backend       string
	databaseURL   string
	maxBytes      int64
	maxObjects    int64
	ratePerSec    float64
	rateBurst     float64
	revocationTTL time.Duration
}

// buildPolicy selects the policy-state backend. backend=memory returns the
// in-memory impls (dev/tests). backend=postgres opens the DSN, migrates the
// edge policy schema, and returns Postgres-backed impls plus the *PgRevocation
// (for its prune loop). It also returns the quota store as an edge.TenantLister
// (both backends' quota stores enumerate tenants-with-data) so the leader GC
// runner can sweep exactly the tenants that have data. It returns a cleanup that
// closes any opened pool.
func buildPolicy(ctx context.Context, o policyOptions) (edge.Deps, *edge.PgRevocation, edge.TenantLister, func(), error) {
	switch o.backend {
	case "memory":
		quota := edge.NewMemoryQuotaStore(o.maxBytes, o.maxObjects)
		return edge.Deps{
			Quota:      quota,
			Rate:       edge.NewTokenBucketLimiter(o.ratePerSec, o.rateBurst),
			Revocation: edge.NewMemoryRevocation(),
		}, nil, quota, func() {}, nil
	case "postgres":
		if o.databaseURL == "" {
			return edge.Deps{}, nil, nil, func() {}, errors.New("policy=postgres requires -database-url or $BLOBGW_EDGE_DATABASE_URL")
		}
		db, err := sql.Open("pgx", o.databaseURL)
		if err != nil {
			return edge.Deps{}, nil, nil, func() {}, err
		}
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return edge.Deps{}, nil, nil, func() {}, err
		}
		if err := edge.PgMigrate(ctx, db); err != nil {
			_ = db.Close()
			return edge.Deps{}, nil, nil, func() {}, err
		}
		quota := edge.NewPgQuotaStore(db, o.maxBytes, o.maxObjects, slog.Default())
		rev := edge.NewPgRevocation(db, o.revocationTTL, slog.Default())
		return edge.Deps{
			Quota:      quota,
			Rate:       edge.NewPgRateLimiter(db, o.ratePerSec, o.rateBurst, slog.Default()),
			Revocation: rev,
		}, rev, quota, func() { _ = db.Close() }, nil
	default:
		return edge.Deps{}, nil, nil, func() {}, errors.New("unknown -policy (want memory|postgres)")
	}
}

// revocationPruneLoop runs Prune immediately and then every interval until ctx
// is done, keeping the jti denylist table bounded.
func revocationPruneLoop(ctx context.Context, rev *edge.PgRevocation, interval time.Duration) {
	prune := func() {
		if n, err := rev.Prune(ctx); err != nil {
			slog.ErrorContext(ctx, "blobgw-edge: revocation prune", "err", err)
		} else if n > 0 {
			slog.InfoContext(ctx, "blobgw-edge: revocation prune", "removed", n)
		}
	}
	prune()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prune()
		}
	}
}

// gcRunnerOptions bundles the inputs for the leader-elected GC sweeper.
type gcRunnerOptions struct {
	router       *gateway.TenantRouter // per-tenant router exposing GCForTenant
	tenants      edge.TenantLister     // dynamic "tenants with data" enumeration
	reg          *metrics.Registry     // shared OTEL registry (nil = metrics off; nil-safe)
	natsURL      string
	natsCreds    string
	natsNKey     string
	nodeID       string
	ttl          time.Duration
	interval     time.Duration
	safetyWindow time.Duration
	replicas     int

	// Per-tenant StagingGC (#33): reclaim minted-but-never-finalized staged
	// uploads. Swept on the SAME leader + TenantLister as the chunked runner.
	// stagingStore nil (backend has no staging store) disables the staging sweep.
	stagingRefs  gateway.RefStore
	stagingStore gateway.StagingStore
	stagingTTL   time.Duration
}

// startGCRunner brings up the leader-elected per-tenant GC sweeper (mirroring
// blobgw's startGCRunner). It connects NATS, elects a single GC leader over
// JetStream KV, and runs a gc.Runner that — only on the leader — sweeps each
// tenant returned by the dynamic TenantLister with the conservative safety
// window. Because the edge has no static tenant list, the runner's TenantsFunc
// closes over the policy quota store and is re-read every pass, so a tenant that
// stored data after start is swept on the next sweep. It returns a stop function
// that cancels the runner (releasing the lease), waits for it, and drains NATS.
func startGCRunner(ctx context.Context, o gcRunnerOptions) (func(), error) {
	// Select the edge GC control-plane account (creds/nkey), mirroring blobgw. Both
	// empty = legacy single-account (auth implied by the URL / server config).
	acctOpts, err := natsAccountOptions(o.natsCreds, o.natsNKey)
	if err != nil {
		return nil, err
	}
	connOpts := append([]nats.Option{
		nats.Name("blobgw-edge-gc"),
		nats.MaxReconnects(-1),
	}, acctOpts...)
	nc, err := nats.Connect(o.natsURL, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats %q: %w", o.natsURL, err)
	}

	leader, err := gc.NewLeader(ctx, nc, gc.LeaderConfig{
		Key:      "gc",
		NodeID:   o.nodeID,
		TTL:      o.ttl,
		Replicas: o.replicas,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("gc leader: %w", err)
	}

	// Dynamic tenant enumeration: read the quota store's tenants-with-data on every
	// pass. gc.TenantsFunc has no error/ctx return, so an enumeration failure is
	// logged and the pass sweeps nothing (never a stale set); a background ctx
	// bounds the query so a slow DB cannot wedge the sweep.
	tenantsFn := func() []string {
		lctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		names, err := o.tenants.Tenants(lctx)
		if err != nil {
			slog.ErrorContext(lctx, "blobgw-edge: gc tenant enumeration failed; skipping sweep", "err", err)
			return nil
		}
		return names
	}

	// The edge uses the direct-Put path over a SHARED-backend TenantRouter (no
	// per-tenant bucket binding), so there is no per-tenant compactor to wire —
	// GC-only, no CompactFunc. Metrics reuse the daemon's shared OTEL registry
	// (nil-safe when metrics are off), so GC outcomes land on the same collector as
	// the edge request/mint instruments.
	runner, err := gc.NewRunner(leader, o.router.GCForTenant, tenantsFn, gc.RunnerConfig{
		Interval:     o.interval,
		SafetyWindow: o.safetyWindow,
		Logger:       slog.Default(),
		Metrics:      o.reg,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("gc runner: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runner.Run(runCtx)
	}()

	// Per-tenant StagingGC (#33) under the SAME leader: a parallel loop that, only
	// while this replica holds the lease (leader.IsLeader()), sweeps each tenant's
	// stale pending staged uploads on the same cadence + TenantLister as the
	// chunked runner. It rechecks leadership per tenant (matching the chunked
	// runner's mid-sweep gate) so a lease lost during a long pass stops it
	// promptly. Skipped when the backend has no staging store.
	if o.stagingStore != nil {
		go stagingSweepLoop(runCtx, leader, o.stagingRefs, o.stagingStore, tenantsFn, o.stagingTTL, o.interval)
	}

	slog.Info("blobgw-edge: gc leader up", "node_id", o.nodeID, "nats_url", o.natsURL,
		"nats_account_creds", o.natsCreds != "" || o.natsNKey != "",
		"interval", o.interval, "safety_window", o.safetyWindow,
		"leader_ttl", o.ttl, "replicas", o.replicas, "staging_gc", o.stagingStore != nil)

	return func() {
		cancel()
		<-done // let the runner release its lease before we drain NATS
		if derr := nc.Drain(); derr != nil {
			slog.Warn("blobgw-edge: nats drain", "err", derr)
			nc.Close()
		}
	}, nil
}

// sweepStagingTenants runs one StagingGC pass per tenant, reclaiming each
// tenant's minted-but-never-finalized staged uploads older than ttl. It builds a
// fresh per-tenant gateway.StagingGC (domain=tenant) over the SHARED ref +
// staging stores — the same stores the data path writes through — so a stale
// pending row is reclaimed in the tenant's own domain and cross-tenant reach is
// structurally impossible (ListStalePending is domain-scoped). isLeader, when
// non-nil, is rechecked before EACH tenant so a lease lost mid-pass stops the
// sweep promptly (mirroring the chunked runner's per-tenant gate); a nil
// isLeader (single-replica dev path) always proceeds. A per-tenant failure is
// logged and does not abort the others — GC is operational, not service-fatal.
func sweepStagingTenants(ctx context.Context, isLeader func() bool, refs gateway.RefStore,
	stage gateway.StagingStore, tenants []string, ttl time.Duration) {
	for _, tenant := range tenants {
		if ctx.Err() != nil {
			return
		}
		if isLeader != nil && !isLeader() {
			return
		}
		sgc := gateway.NewStagingGC(refs, stage, tenant, ttl, slog.Default())
		if _, err := sgc.RunOnce(ctx); err != nil {
			slog.ErrorContext(ctx, "blobgw-edge: staging gc: tenant sweep failed", "tenant", tenant, "err", err)
		}
	}
}

// stagingSweepLoop drives the per-tenant StagingGC on the leader-elected path: a
// fixed-delay loop (next pass one interval after the previous COMPLETES, matching
// gc.Runner.Run) that sweeps only while leader.IsLeader(). The first pass fires
// one interval after start so a freshly-elected leader has settled. tenantsFn is
// the same dynamic TenantLister-backed enumeration the chunked runner uses.
func stagingSweepLoop(ctx context.Context, leader *gc.Leader, refs gateway.RefStore,
	stage gateway.StagingStore, tenantsFn func() []string, ttl, interval time.Duration) {
	if interval <= 0 {
		return
	}
	slog.InfoContext(ctx, "blobgw-edge: staging gc loop started (leader-gated)", "interval", interval, "ttl", ttl)
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if leader.IsLeader() {
				sweepStagingTenants(ctx, leader.IsLeader, refs, stage, tenantsFn(), ttl)
			}
			timer.Reset(interval)
		}
	}
}

// stagingGCLoop drives the per-tenant StagingGC on the single-pass dev path (no
// leader election, single replica). It runs one pass immediately and then every
// interval, enumerating tenants from the dynamic TenantLister on each pass so a
// tenant that staged its first upload mid-run is swept next pass. interval <= 0
// disables it.
func stagingGCLoop(ctx context.Context, refs gateway.RefStore, stage gateway.StagingStore,
	tenants edge.TenantLister, ttl, interval time.Duration) {
	if interval <= 0 {
		return
	}
	// tenantsFn re-reads the tenants-with-data set each pass (a background ctx
	// bounds the query so a slow DB can't wedge the sweep); an enumeration failure
	// logs and sweeps nothing that pass (never a stale set).
	tenantsFn := func() []string {
		lctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		names, err := tenants.Tenants(lctx)
		if err != nil {
			slog.ErrorContext(lctx, "blobgw-edge: staging gc tenant enumeration failed; skipping sweep", "err", err)
			return nil
		}
		return names
	}
	slog.InfoContext(ctx, "blobgw-edge: staging gc loop started (single-pass)", "interval", interval, "ttl", ttl)
	sweepStagingTenants(ctx, nil, refs, stage, tenantsFn(), ttl)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepStagingTenants(ctx, nil, refs, stage, tenantsFn(), ttl)
		}
	}
}

// natsAccountOptions builds the nats.Option set selecting the edge GC
// control-plane account (mirrors blobgw's natsAccountOptions): -nats-creds points
// at a JWT+nkey credentials file (operator/JWT mode), -nats-nkey at a bare nkey
// seed file. At most one may be set. Both empty returns no options, leaving the
// connection in the legacy single-account model for back-compat.
func natsAccountOptions(credsFile, nkeyFile string) ([]nats.Option, error) {
	switch {
	case credsFile != "" && nkeyFile != "":
		return nil, errors.New("gc-leader: set only one of -nats-creds / -nats-nkey")
	case credsFile != "":
		if _, err := os.Stat(credsFile); err != nil {
			return nil, fmt.Errorf("gc-leader: creds file %q: %w", credsFile, err)
		}
		return []nats.Option{nats.UserCredentials(credsFile)}, nil
	case nkeyFile != "":
		opt, err := nats.NkeyOptionFromSeed(nkeyFile)
		if err != nil {
			return nil, fmt.Errorf("gc-leader: load nkey seed %q: %w", nkeyFile, err)
		}
		return []nats.Option{opt}, nil
	default:
		return nil, nil
	}
}

// startMetricsHTTP serves a Prometheus /metrics scrape endpoint on addr from the
// edge's OTEL registry (mirrors blobgw's startGCMetricsHTTP). Returns a stop
// function that drains the server.
func startMetricsHTTP(addr string, reg *metrics.Registry) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("blobgw-edge: metrics http server", "addr", addr, "err", err)
		}
	}()
	slog.Info("blobgw-edge: metrics scrape endpoint up", "addr", addr, "path", "/metrics")
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}
}

// loadOrGenerateKey builds an Ed25519 private key from a hex seed (stable dev
// keys) or generates an ephemeral one. The key id is derived from the public
// key so it is stable for a given seed.
func loadOrGenerateKey(seedHex string) (ed25519.PrivateKey, string, error) {
	var priv ed25519.PrivateKey
	if seedHex != "" {
		seed, err := hex.DecodeString(seedHex)
		if err != nil {
			return nil, "", err
		}
		if len(seed) != ed25519.SeedSize {
			return nil, "", errors.New("key-seed-hex must be 32 bytes")
		}
		priv = ed25519.NewKeyFromSeed(seed)
	} else {
		_, p, err := capability.GenerateKey()
		if err != nil {
			return nil, "", err
		}
		priv = p
	}
	pub := priv.Public().(ed25519.PublicKey)
	kid := hex.EncodeToString(pub[:6])
	return priv, kid, nil
}
