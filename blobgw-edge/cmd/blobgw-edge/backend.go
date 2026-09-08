// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/blobgw/pgindex"
	"github.com/scitrera/memorylayer-storage/blobgw/s3stage"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// backendOptions bundles the data-path backend selection, mirroring the flag
// subset blobgw's build() consumes (same flag names/help so the Helm chart can
// reuse the same values). Only the fields relevant to the selected backend are
// consulted.
type backendOptions struct {
	backend  string // local|s3
	dataDir  string // backend=local
	packSize int

	// index (backend=s3): the shared blobgw dedup + ref index. memory is for
	// tests/dev-over-s3; postgres is production.
	index            string // memory|postgres
	indexDatabaseURL string // Postgres DSN (index=postgres)

	dbMaxOpenConns    int
	dbMaxIdleConns    int
	dbConnMaxLifetime time.Duration

	// tenant-config (backend=s3): the per-tenant S3 bucket-binding provider.
	tenantConfig       string
	tenantSecretsFile  string
	credentialMode     string
	credentialCacheTTL time.Duration
	manifestDSNFile    string

	// staging (backend=s3): stage-then-finalize upload backend.
	staging       string // none|memory|s3
	stagingBucket string
	stagingPrefix string

	// s3 flags (backend=s3, staging=s3): mirror blobgw's names.
	s3Endpoint       string
	s3Bucket         string
	s3Region         string
	s3Prefix         string
	s3ForcePathStyle bool
}

// backend is the assembled data-path posture: a per-tenant router plus a cleanup
// for any opened resources (DB pool, PG manifest pools). inProcessGC reports
// whether the single-pass in-process GC().Loop applies — true only for the
// shared-backend local posture; the per-tenant s3 posture has no single shared
// store to sweep in-process (router.GC() returns nil), so it requires
// -gc-leader (router.GCForTenant).
type backend struct {
	router      *gateway.TenantRouter
	cleanup     func()
	inProcessGC bool

	// refs + staging are the SHARED ref index and staging store the router was
	// built over (the router does not expose them). They feed the per-tenant
	// StagingGC (#33): now that the edge adopts the stage-then-finalize path
	// (POST /staged → POST /finalize), a minted-but-never-finalized upload leaves
	// a pending blob_ref row + a staged object that leak without a sweep. The GC
	// runner sweeps gateway.NewStagingGC(refs, staging, tenant, ttl) per tenant.
	// staging is nil when the backend has no staging store (backend=s3
	// -staging=none), in which case there is nothing to sweep and no StagingGC is
	// wired.
	refs    gateway.RefStore
	staging gateway.StagingStore
}

// buildBackend selects the data-path backend. backend=local (the default) is
// EXACTLY today's dev path: a local-filesystem shared-backend TenantRouter over
// in-memory dedup/ref/staging (gateway.NewLocalTenantRouter). backend=s3 mirrors
// blobgw's per-tenant assembly (cmd/blobgw startGCRunner): a per-tenant
// bucket-binding router (WithBindingProvider) over the SHARED pgindex dedup+ref
// index and a staging store, with WithBackingMeter so the per-tenant S3 chunk
// stores emit the casstore.backing.* RED view. reg is the daemon's shared OTEL
// registry (nil-safe: reg.Meter() is a no-op meter when metrics are off).
func buildBackend(ctx context.Context, o backendOptions, reg *metrics.Registry) (*backend, error) {
	switch o.backend {
	case "local":
		local, err := gateway.NewLocalTenantRouter(o.dataDir, o.packSize)
		if err != nil {
			return nil, fmt.Errorf("tenant router init: %w", err)
		}
		// Surface the shared in-memory ref + staging stores so the per-tenant
		// StagingGC can sweep un-finalized staged uploads (#33) on the dev path too.
		return &backend{
			router: local.Router, cleanup: func() {}, inProcessGC: true,
			refs: local.Refs, staging: local.Staging,
		}, nil
	case "s3":
		return buildS3Backend(ctx, o, reg)
	default:
		return nil, fmt.Errorf("unknown -backend %q (want local|s3)", o.backend)
	}
}

// buildS3Backend assembles the production per-tenant router: shared pgindex
// dedup+ref index, a staging store, and the tenant-config credential provider
// wired via WithBindingProvider — the SAME shape blobgw's startGCRunner builds,
// so the edge data path and the leader-elected GC (A4) purge the same index.
// Fails fast with clear errors on the flags s3 mode requires (mirrors blobgw's
// fail-style checks).
func buildS3Backend(ctx context.Context, o backendOptions, reg *metrics.Registry) (*backend, error) {
	cleanups := []func(){}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(err error) (*backend, error) {
		cleanup()
		return nil, err
	}

	if o.tenantConfig == "" {
		return fail(errors.New("backend=s3 requires -tenant-config"))
	}

	// The data-path RED recorder feeds pgindex op metrics + the index-pool USE
	// gauges. Built over the daemon's shared meter (no-op when metrics are off).
	meter := reg.Meter()
	dataPath, err := metrics.NewDataPath(meter)
	if err != nil {
		return fail(fmt.Errorf("metrics data path: %w", err))
	}

	// --- shared dedup + ref index (blobgw's pgindex) ---
	// In s3 mode the edge uses the SAME shared pgindex the internal blobgw writes
	// through, so GC (router.GCForTenant, A4) purges exactly what the data path
	// writes (mirror blobgw's "shared index" comment). memory is dev/test only.
	dedup, refs, err := buildIndex(ctx, o, dataPath, meter, &cleanups)
	if err != nil {
		return fail(err)
	}

	// --- staging store ---
	staging, err := buildStaging(ctx, o)
	if err != nil {
		return fail(err)
	}

	// --- per-tenant credential provider (tenant-config) ---
	mode, err := tenantconfig.ParseCredentialMode(o.credentialMode)
	if err != nil {
		return fail(err)
	}
	ttl := o.credentialCacheTTL
	if ttl <= 0 {
		ttl = tenantconfig.DefaultCacheTTL
	}
	// Watched variant: poll the tenant-config file and atomically swap the
	// tenant→Descriptor map on change, so tenant add/remove takes effect WITHOUT
	// restarting blobgw-edge (same restart-free bind/presign data path as blobgw).
	// The initial load still fails fast; the polling goroutine is bound to ctx.
	provider, err := tenantconfig.LoadProviderModeWatched(ctx, mode, o.tenantConfig, o.tenantSecretsFile, ttl)
	if err != nil {
		return fail(fmt.Errorf("load tenant provider: %w", err))
	}

	// Manifest DSN routing (converged mlfs -remote, TECH_DEBT #50): listed tenants
	// keep their slice manifests in their own meta DB, not S3.
	manifestDSNs, err := loadManifestDSNs(o.manifestDSNFile)
	if err != nil {
		return fail(fmt.Errorf("load manifest DSNs: %w", err))
	}

	// Per-tenant router over the shared index/ref/staging stores + the provider,
	// mirroring blobgw's startGCRunner. WithBackingMeter wraps each per-tenant S3
	// chunk store with the casstore backing-store RED adapter (obs.Wrap) so
	// casstore.backing.* metrics + spans light up — the seam A2 noted the local
	// path lacked; the s3 per-tenant path has it via WithBackingMeter. The shared
	// upstream/chunks are nil on the WithBindingProvider path (each tenant binds
	// its own S3 bucket). GCForTenant over this router is what A4's leader sweeps.
	router := gateway.NewTenantRouter(nil, nil, dedup, refs, staging, o.packSize,
		gateway.WithBindingProvider(provider, manifestDSNs),
		gateway.WithBackingMeter(meter))

	slog.Info("blobgw-edge: s3 backend up", "index", o.index,
		"tenant_config", o.tenantConfig, "credential_mode", string(mode),
		"staging", o.staging, "manifest_dsn_routing", manifestDSNs != nil)

	return &backend{
		router: router,
		cleanup: func() {
			// Release per-tenant PG manifest DB pools (no-op for the S3-only posture),
			// then the shared index pool + any staging resources.
			if cerr := router.Close(); cerr != nil {
				slog.Warn("blobgw-edge: router close", "err", cerr)
			}
			cleanup()
		},
		inProcessGC: false,
		// Surface the shared ref + staging stores for the per-tenant StagingGC (#33).
		// staging is nil when -staging=none (the edge then has no staged path to
		// sweep), so the GC wiring in main.go is skipped in that case.
		refs:    refs,
		staging: staging,
	}, nil
}

// buildIndex selects the shared dedup + ref index for s3 mode. memory is a
// self-contained dev/test index; postgres opens the DSN, migrates the shared
// blobgw index schema, bounds + instruments the pool, and returns the pgindex
// Store as both DedupStore and RefStore (the same handle blobgw uses).
func buildIndex(ctx context.Context, o backendOptions, dataPath *metrics.DataPath,
	meter metric.Meter, cleanups *[]func()) (snapshot.DedupStore, gateway.RefStore, error) {
	switch o.index {
	case "memory":
		return snapshot.NewMemoryDedupStore(), gateway.NewMemoryRefStore(), nil
	case "postgres":
		if o.indexDatabaseURL == "" {
			return nil, nil, errors.New("index=postgres requires -index-database-url or -database-url ($BLOBGW_DATABASE_URL)")
		}
		db, err := sql.Open("pgx", o.indexDatabaseURL)
		if err != nil {
			return nil, nil, fmt.Errorf("open postgres index: %w", err)
		}
		// Bound the pool so a burst of concurrent object ops can't exhaust Postgres
		// connections (flag-overridable; sane defaults, mirroring blobgw).
		db.SetMaxOpenConns(o.dbMaxOpenConns)
		db.SetMaxIdleConns(o.dbMaxIdleConns)
		db.SetConnMaxLifetime(o.dbConnMaxLifetime)
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("ping postgres index: %w", err)
		}
		pg := pgindex.New(db, pgindex.WithMetrics(dataPath))
		if err := pg.Migrate(ctx); err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("migrate postgres index: %w", err)
		}
		// USE saturation view of the index pool (in_use/idle/wait_count off
		// sql.DB.Stats()). No-op when metrics are disabled.
		if err := dataPath.BindPool(meter, "index", db); err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("bind index pool gauges: %w", err)
		}
		*cleanups = append(*cleanups, func() { _ = db.Close() })
		return pg, pg, nil
	default:
		return nil, nil, fmt.Errorf("unknown -index %q (want memory|postgres)", o.index)
	}
}

// buildStaging selects the stage-then-finalize backend for s3 mode. The edge
// uses the direct-Put path today, so the default is none; memory + s3 mirror
// blobgw for parity when the staged path is exercised.
func buildStaging(ctx context.Context, o backendOptions) (gateway.StagingStore, error) {
	switch o.staging {
	case "none", "":
		return nil, nil
	case "memory":
		return gateway.NewMemoryStagingStore(), nil
	case "s3":
		bucket := o.stagingBucket
		if bucket == "" {
			bucket = o.s3Bucket // reuse the chunk bucket if a dedicated one isn't given
		}
		if bucket == "" {
			return nil, errors.New("staging=s3 requires -staging-bucket or -s3-bucket")
		}
		st, err := s3stage.New(ctx, s3stage.Config{
			Bucket: bucket, Region: o.s3Region, Endpoint: o.s3Endpoint,
			Prefix: o.stagingPrefix, ForcePathStyle: o.s3ForcePathStyle,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 staging: %w", err)
		}
		return st, nil
	default:
		return nil, fmt.Errorf("unknown -staging %q (want none|memory|s3)", o.staging)
	}
}

// loadManifestDSNs reads a JSON map {tenant: postgres-DSN} from path (mirrors
// blobgw's loadManifestDSNs). An empty path returns nil (the all-S3 posture: no
// tenant routes its manifests to PG). Each DSN is passed through os.ExpandEnv so
// the mounted file may carry ${VAR}/$VAR credential placeholders injected at
// runtime from a k8s Secret, keeping the password out of git. Expansion runs
// BEFORE the empty-DSN check so an unset variable fails loudly rather than
// silently demoting a converged tenant to the S3 fallback.
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
