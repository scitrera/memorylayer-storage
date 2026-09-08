// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package gc is blobgw's leader-elected garbage collector for the converged
// storage platform (ADR-001 §2.4 #29, §4 I3). A single NATS-elected blobgw
// replica sweeps each tenant's pack store on a ticker, enforcing a conservative
// safety window so a sweep can never reclaim a pack in the gap between its PUT
// and its index Record.
//
// # Why GC lives on blobgw
//
// GC must run where the real per-tenant S3 stores + credentials are. blobgw's
// TenantRouter already binds each tenant to its own backend (the per-tenant
// bucket of ADR §2.6) and exposes GCForTenant(ctx, tenant) → *snapshot.ChunkedGC
// over that backend plus the shared dedup index. The Runner drives that seam: it
// is the leader-elected scheduler; ChunkedGC is the per-pass mark-and-sweep.
//
// # Safety window (ADR §4 I3 / HC1 / HC2)
//
// I3 requires the GC safety window to exceed the worst-case PUT→Record gap
// across nodes (plus clock skew + margin). Under the deferred claim/pending-
// announce protocol the gap can stretch under write-back-cache load, so HC1
// mandates a CONSERVATIVE window measured in HOURS — production default 24h, set
// here as DefaultSafetyWindow. The Runner stamps this window onto every
// per-tenant ChunkedGC (its SafetyWindow field), so no pack younger than the
// window is ever reclaimed.
//
// HC2 — the node-side fail-safe (a node must block writes + alarm when its
// write-back backlog approaches the window) — is what makes a finite window
// SUFFICIENT. It is an mlfs-NODE-side dependency and is OUT OF SCOPE for this
// package: the Runner enforces the window, but it relies on HC2 holding on the
// write path to guarantee the window is never breached. See ADR-001 §4 (I3 +
// HC2) and §7.5.
package gc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// DefaultSafetyWindow is the conservative GC safety window of ADR §4 HC1: in
// hours, not minutes, so a normal write-back drain can never approach it. It is
// stamped onto every per-tenant ChunkedGC unless overridden via RunnerConfig.
const DefaultSafetyWindow = 24 * time.Hour

// DefaultInterval is the default GC sweep cadence.
const DefaultInterval = time.Hour

// MinSafetyWindow is the hard absolute floor for the GC safety window. ADR §4
// HC1 mandates an HOURS-scale window so a normal write-back drain can never
// approach it; a positive-but-tiny window (e.g. a "1s" typo) would disarm that
// invariant and arm a data-loss config. NewRunner clamps any configured window
// below this floor UP to DefaultSafetyWindow and logs an ERROR, so a typo can
// never reduce the window below the hours scale HC1 requires.
const MinSafetyWindow = time.Hour

// GCFunc yields a per-tenant ChunkedGC over that tenant's own backend plus the
// shared dedup index. (*gateway.TenantRouter).GCForTenant satisfies it directly,
// so the Runner depends only on this narrow capability rather than the whole
// router — keeping the package decoupled and testable with a fake.
type GCFunc func(ctx context.Context, tenant string) (*snapshot.ChunkedGC, error)

// CompactFunc yields a per-tenant ChunkedStore for pack compaction (repack).
// (*gateway.TenantRouter).CompactorForTenant satisfies it. Optional: when nil
// (or no compaction trigger is configured) the Runner does GC only.
type CompactFunc func(ctx context.Context, tenant string) (*snapshot.ChunkedStore, error)

// TenantsFunc yields the current set of tenants to sweep. It is read on every
// sweep tick so a tenant added at runtime (e.g. a reloaded tenant-config) is
// picked up on the next pass. tenantconfig.File.Tenants is the production source;
// a closure over it satisfies this.
type TenantsFunc func() []string

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	// Interval is the sweep cadence: how often the leader iterates the tenant set
	// and runs a GC pass per tenant. Defaults to DefaultInterval when non-positive.
	Interval time.Duration
	// SafetyWindow is stamped onto every per-tenant ChunkedGC (ADR §4 I3/HC1).
	// Defaults to DefaultSafetyWindow (24h) when non-positive. Tests may set a
	// tiny window to exercise both sides of the cutoff.
	SafetyWindow time.Duration
	// Logger receives sweep lifecycle + per-tenant pass logs. Defaults to
	// slog.Default() when nil.
	Logger *slog.Logger

	// CompactFunc, when set, makes each tenant pass FIRST consolidate under-filled
	// /small packs (so the GC sweep then reclaims the orphaned old packs in the
	// same pass). Nil disables compaction (GC only). Typically
	// (*gateway.TenantRouter).CompactorForTenant.
	CompactFunc CompactFunc
	// Compact tunes compaction (fill/size thresholds, per-pass byte cap). It only
	// runs when CompactFunc is set AND a trigger (MinFillRatio or MinPackBytes) is
	// configured. The Runner stamps SafetyWindow onto it so compaction skips packs
	// younger than the GC window (don't race a fresh write).
	Compact snapshot.CompactConfig

	// Metrics, when set, records per-tenant GC + compaction outcomes (counters,
	// duration histograms, live-chunk gauge). nil disables instrumentation (all
	// record calls are nil-safe).
	Metrics *metrics.Registry
}

// Runner is the leader-elected GC scheduler. On each tick, IF this replica is
// the leader, it iterates the configured tenants and runs one scoped GC pass per
// tenant via GCForTenant → RunOnceForDomain, with the safety window enforced. A
// follower does nothing.
//
// Leadership is rechecked per-tenant: the IsLeader() gate is re-evaluated at the
// top of every tenant iteration, so a leader deposed mid-sweep (lease lost on a
// NATS partition) stops sweeping at the next tenant boundary rather than running
// the rest of the pass. This bounds — but does not by itself eliminate — the
// window in which a deposed leader and a freshly-elected one could overlap on a
// single tenant. Correctness under that brief overlap does NOT rest on the lock
// alone: casstore is content-addressed (concurrent passes converge on the same
// liveness decision) and the conservative safety window (ADR §4 I3/HC1) keeps
// any pack younger than the window off-limits to BOTH sweepers. Data safety is
// by design (content-addressing + safety window), with per-tenant leadership
// recheck as defense-in-depth — not by coincidence of a single sweeper.
type Runner struct {
	leader       *Leader
	gcFor        GCFunc
	tenants      TenantsFunc
	interval     time.Duration
	safetyWindow time.Duration
	logger       *slog.Logger
	compactFor   CompactFunc // nil = compaction off
	compactCfg   snapshot.CompactConfig
	metrics      *metrics.Registry // nil = no instrumentation (nil-safe)
}

// NewRunner constructs a Runner. leader is the NATS leader-election primitive
// (only the leader sweeps); gcFor yields a per-tenant ChunkedGC (typically
// router.GCForTenant); tenants yields the current tenant set; cfg tunes the
// interval, safety window, and logger. leader, gcFor and tenants are required.
func NewRunner(leader *Leader, gcFor GCFunc, tenants TenantsFunc, cfg RunnerConfig) (*Runner, error) {
	if leader == nil {
		return nil, errors.New("gc: nil leader")
	}
	if gcFor == nil {
		return nil, errors.New("gc: nil GCFunc")
	}
	if tenants == nil {
		return nil, errors.New("gc: nil TenantsFunc")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// Enforce the safety-window floor (ADR §4 I3/HC1). A non-positive window is
	// silently defaulted; a positive-but-tiny window is the dangerous case — it
	// passes type checks yet disarms the hours-scale invariant. Two-stage guard:
	//   - below MinSafetyWindow (incl. non-positive): clamp UP to the default and
	//     log ERROR — a typo must never arm a data-loss config.
	//   - between the floor and the default: honor it but log a loud WARN, since
	//     it is below the recommended conservative window.
	switch {
	case cfg.SafetyWindow < MinSafetyWindow:
		cfg.Logger.Error("gc: configured safety window below the hard minimum floor; clamping up to the default — a window this small disarms ADR §4 HC1",
			"configured", cfg.SafetyWindow, "minimum", MinSafetyWindow, "clamped_to", DefaultSafetyWindow)
		cfg.SafetyWindow = DefaultSafetyWindow
	case cfg.SafetyWindow < DefaultSafetyWindow:
		cfg.Logger.Warn("gc: configured safety window is below the recommended conservative default (ADR §4 HC1); proceeding but a larger window is safer",
			"configured", cfg.SafetyWindow, "recommended", DefaultSafetyWindow)
	}
	// Compaction respects the same conservative window as GC: never repack a pack
	// younger than the window (don't race a fresh write).
	cfg.Compact.SafetyWindow = cfg.SafetyWindow
	return &Runner{
		leader:       leader,
		gcFor:        gcFor,
		tenants:      tenants,
		interval:     cfg.Interval,
		safetyWindow: cfg.SafetyWindow,
		logger:       cfg.Logger,
		compactFor:   cfg.CompactFunc,
		compactCfg:   cfg.Compact,
		metrics:      cfg.Metrics,
	}, nil
}

// Run drives the GC loop until ctx is cancelled. It launches the leader election
// in a child goroutine and uses a fixed-delay timer in the calling goroutine: the
// next sweep is scheduled one full interval after the previous sweep COMPLETES,
// not on a fixed-rate clock. Only the current leader sweeps; followers skip. The
// first sweep fires one interval after start (not immediately) so a freshly-elected
// leader has had a renew cycle to settle. Returns when ctx is done.
func (r *Runner) Run(ctx context.Context) {
	r.logger.InfoContext(ctx, "blobgw gc: runner started",
		"interval", r.interval, "safety_window", r.safetyWindow)

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	defer cancelLeader()
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		r.leader.Run(leaderCtx)
	}()

	// Fixed-DELAY scheduling: a time.Timer reset AFTER each sweep completes, NOT a
	// fixed-rate Ticker. The sweep runs synchronously in this loop and a single pass
	// can outlast r.interval (a large tenant's compaction pass runs many minutes); a
	// Ticker buffers a pending tick during that overrun, so the next sweep would fire
	// with zero gap (back-to-back full re-scans). Resetting the timer only once the
	// sweep returns guarantees a real r.interval of rest between passes regardless of
	// how long a pass takes. The first sweep still fires one interval after start.
	timer := time.NewTimer(r.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.InfoContext(ctx, "blobgw gc: runner exiting", "err", ctx.Err())
			cancelLeader()
			<-leaderDone // let the leader release its lease before we return
			return
		case <-timer.C:
			r.sweepIfLeader(ctx)
			// Reset only after the sweep returns (timer.C is already drained by the
			// receive above, so Reset is safe here): schedules the next sweep one full
			// interval from NOW, not from the previous fire.
			timer.Reset(r.interval)
		}
	}
}

// sweepIfLeader runs one full sweep (every tenant) iff this replica is the
// leader. Exported-via-test-only behavior is kept here so tests can drive a
// single sweep without the ticker.
func (r *Runner) sweepIfLeader(ctx context.Context) {
	if !r.leader.IsLeader() {
		r.logger.DebugContext(ctx, "blobgw gc: not leader, skipping sweep")
		return
	}
	r.sweep(ctx)
}

// sweep iterates the current tenant set and runs one scoped GC pass per tenant.
// A failure on one tenant is logged and does not abort the others — GC is an
// operational concern, not service-fatal.
//
// Leadership is rechecked at the top of EACH tenant iteration: a single
// IsLeader() gate at sweep entry is insufficient because a long multi-tenant
// pass can outlive the lease (lost on a NATS partition mid-sweep), letting a
// deposed leader keep sweeping while a peer is elected — two concurrent
// sweepers. Re-evaluating the gate per tenant bails out of the rest of the pass
// as soon as leadership is lost, bounding any overlap to at most the in-flight
// tenant (see Runner doc for why that residual overlap is still data-safe).
func (r *Runner) sweep(ctx context.Context) {
	tenants := r.tenants()
	r.logger.InfoContext(ctx, "blobgw gc: sweep starting", "tenants", len(tenants))
	for _, tenant := range tenants {
		if ctx.Err() != nil {
			return
		}
		// Recheck leadership before every tenant: a lease lost mid-sweep must
		// stop the sweep here rather than continue purging on a deposed leader.
		if !r.leader.IsLeader() {
			r.logger.WarnContext(ctx, "blobgw gc: leadership lost mid-sweep, aborting remaining tenants",
				"tenant", tenant)
			return
		}
		r.sweepTenant(ctx, tenant)
	}
	r.logger.InfoContext(ctx, "blobgw gc: sweep complete", "tenants", len(tenants))
}

// sweepTenant runs a single safety-window-enforced GC pass over one tenant's
// backend. It resolves the per-tenant ChunkedGC via the GCFunc, stamps the
// conservative safety window onto it (ADR §4 I3/HC1), and runs a domain-scoped
// mark-and-sweep so the pass only ever touches that tenant's blobs.
func (r *Runner) sweepTenant(ctx context.Context, tenant string) {
	// Consolidate under-filled/small packs first (no-op when disabled) so the GC
	// sweep below reclaims the now-orphaned old packs in the same pass.
	r.compactTenant(ctx, tenant)

	g, err := r.gcFor(ctx, tenant)
	if err != nil {
		r.logger.WarnContext(ctx, "blobgw gc: resolve tenant gc failed",
			"tenant", tenant, "err", err)
		return
	}
	// Enforce the conservative safety window (ADR §4 I3/HC1): never reclaim a
	// pack younger than the window. This is the one knob that prevents a sweep
	// from collecting a pack in the PUT→Record gap.
	g.SafetyWindow = r.safetyWindow
	res, err := g.RunOnceForDomain(ctx, tenant)
	if err != nil {
		r.metrics.GCError(tenant)
		r.logger.WarnContext(ctx, "blobgw gc: tenant pass failed",
			"tenant", tenant, "err", err)
		return
	}
	r.metrics.RecordGC(tenant, res)
	// #50 over-deletion canary (docs/OBSERVABILITY.md): a pass that scanned chunks
	// (the tenant HAS pack blobs) yet found ZERO live is the single condition that
	// precedes GC reclaiming live data. RecordGC already bumped the
	// blobgw.gc.empty_live_set counter; log it PROMINENTLY here so it is impossible
	// to miss in operations. It should fire ~0 times forever.
	if metrics.EmptyLiveSet(res) {
		r.logger.ErrorContext(ctx, "blobgw gc: EMPTY LIVE SET while pack blobs exist — possible GC over-deletion (TECH_DEBT #50); investigate before the safety window expires",
			"tenant", tenant,
			"chunks_scanned", res.ChunksScanned,
			"manifests_scanned", res.ManifestsScanned,
			"live_chunks", res.LiveChunks,
		)
	}
	r.logger.InfoContext(ctx, "blobgw gc: tenant pass complete",
		"tenant", tenant,
		"chunks_scanned", res.ChunksScanned,
		"manifests_scanned", res.ManifestsScanned,
		"live_chunks", res.LiveChunks,
		"chunks_reclaimed", res.ChunksReclaimed,
		"bytes_reclaimed", res.BytesReclaimed,
		"chunks_deferred_young", res.ChunksDeferredYoung,
		"chunks_skipped_recent", res.ChunksSkippedRecent,
		"chunks_skipped_error", res.ChunksSkippedError,
		"duration", res.Duration,
	)
}

// compactTenant consolidates a tenant's under-filled/small packs into full ones
// before the GC sweep. No-op when compaction is disabled (CompactFunc nil) or no
// trigger (MinFillRatio/MinPackBytes) is configured. A failure is logged and
// never aborts the GC pass — compaction is best-effort.
func (r *Runner) compactTenant(ctx context.Context, tenant string) {
	if r.compactFor == nil || (r.compactCfg.MinFillRatio <= 0 && r.compactCfg.MinPackBytes <= 0) {
		return
	}
	cs, err := r.compactFor(ctx, tenant)
	if err != nil {
		r.metrics.CompactError(tenant)
		r.logger.WarnContext(ctx, "blobgw gc: resolve tenant compactor failed",
			"tenant", tenant, "err", err)
		return
	}
	res, err := cs.Compact(ctx, tenant, r.compactCfg)
	if err != nil {
		r.metrics.CompactError(tenant)
		r.logger.WarnContext(ctx, "blobgw gc: tenant compaction failed",
			"tenant", tenant, "err", err)
		return
	}
	r.metrics.RecordCompact(tenant, res)
	if res.PacksWritten > 0 || res.ManifestsRewritten > 0 {
		r.logger.InfoContext(ctx, "blobgw gc: tenant compaction",
			"tenant", tenant,
			"packs_scanned", res.PacksScanned,
			"packs_selected", res.PacksSelected,
			"packs_written", res.PacksWritten,
			"manifests_rewritten", res.ManifestsRewritten,
			"bytes_rewritten", res.BytesRewritten,
			"duration", res.Duration,
		)
	}
}
