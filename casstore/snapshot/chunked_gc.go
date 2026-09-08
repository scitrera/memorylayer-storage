// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// ChunkedGC implements mark-and-sweep garbage collection for chunks and
// pack blobs orphaned by deleted manifests. The architect review
// (.slop/blobstore-architect-review.md, "What's missing #1") argued
// strongly against refcounted-on-Delete because of unfixable races
// against concurrent writers; this is the recommended alternative.
//
// Algorithm:
//
//  1. **Mark snapshot**: enumerate every blob with prefix "chunk-" (v1)
//     OR "pack-" (v2) in the blobstore; record each blob's current
//     Timestamp as its "tag". Both prefixes are listed so a mixed-format
//     store (some snapshots written before packing was enabled, some
//     after) is handled correctly in the same pass.
//  2. **Live set**: walk every manifest in the snapshot store, parse
//     each. For v1 manifests, add "chunk-<tenant>-<hash>" to the live
//     set. For v2 manifests, add "pack-<tenant>-<pack_hash>" to the
//     live set. A pack blob is live if ANY of its chunks is referenced.
//  3. **Sweep**: for each marked blob not in the live set, re-fetch
//     its current Timestamp via GetMetadata. If the timestamp is
//     unchanged from the mark phase, DeleteBlob — the blob has not
//     been touched between mark and sweep so it is safe to reclaim.
//     If the timestamp changed (a concurrent writer touched it), skip
//     deletion; the blob will be re-evaluated on the next GC pass.
//
// Run on a fixed schedule, leader-only (so multiple replicas don't
// double-GC). See main.go wiring.

// PayloadWalker is an OPTIONAL SnapshotStore capability: stream every manifest's
// payload ALONGSIDE its metadata in one pass, so the GC's live-set phase parses
// chunk refs inline instead of issuing a Get per manifest (the M-individual-round-
// trips cost, TECH_DEBT #28). A PG-backed manifest store implements it as one
// streaming SELECT; stores that don't fall back to Walk + per-manifest Get. The
// payload handed to yield is the raw (decoded) manifest blob, valid only for the
// duration of the callback — implementations stream it row-by-row to bound memory.
type PayloadWalker interface {
	WalkPayloads(ctx context.Context, yield func(key SnapshotKey, version string, meta SnapshotMetadata, payload []byte) error) error
}

type ChunkedGC struct {
	manifests SnapshotStore     // the wrapped snapshot store (manifests live here)
	chunks    blobstore.Storage // chunk storage to GC
	logger    *slog.Logger

	// Metrics, when non-nil, receives the cumulative bytes/chunks reclaimed
	// by each GC pass. It is nil-safe: a nil Metrics (the default) emits
	// nothing. This is the seam that lets a host application (e.g.
	// sandbox-provider) plug its own OTel instruments in without casstore
	// depending on any particular telemetry stack. See GCMetrics.
	Metrics GCMetrics

	// Index, when non-nil, is the global dedup index whose entries must be
	// purged when their pack is reclaimed, so a later Put never dedups against
	// collected bytes. Set it to the same DedupStore the ChunkedStore writes
	// through (a DedupStore satisfies DedupGC). Leave nil for the
	// prior-manifest default — there is no external index, so nothing to
	// purge. See DedupGC and the package note on GC + global dedup.
	Index DedupGC

	// SafetyWindow, when > 0, refuses to reclaim a pack/chunk blob younger than
	// the window (by its storage timestamp). This closes the window where a
	// write has uploaded a new pack but not yet committed the manifest that
	// references it: a concurrent GC would otherwise see the pack as orphaned
	// and delete it. A few minutes covers any in-flight write; the plan uses
	// 24h in production. Default 0 = disabled (immediate reclaim).
	SafetyWindow time.Duration

	// Assoc, when non-nil, lets the live-set phase be answered by the ASSOCIATION
	// index (one query) instead of a walk over every manifest in the store — the
	// O(store) → O(query) win in docs/DESIGN_lore_evaluation.md §4.2.
	//
	// It applies only to a DOMAIN-SCOPED pass (RunOnceForDomain) whose domain the
	// association backfill has marked complete. Two deliberate restrictions:
	// associations are per-domain so an all-domains pass has nothing to enumerate,
	// and a domain that has not been backfilled holds no associations for its
	// pre-existing objects — trusting the association live set there would judge
	// the whole domain garbage. Both cases fall back to the manifest walk. Run
	// ChunkedStore.BackfillAssociations once per domain to enable the fast path.
	Assoc PackAssocStore

	// now is the clock for the safety-window check; overridable in tests.
	now func() time.Time
}

// DedupGC is the subset of dedup-index maintenance the GC needs: dropping
// index entries that point at packs being reclaimed. A DedupStore satisfies
// it. Kept separate so ChunkedGC depends only on the purge capability, not the
// full read/write store.
type DedupGC interface {
	// PurgePacks removes every index entry in domain whose location points to
	// one of the given pack hashes. Idempotent.
	PurgePacks(ctx context.Context, domain string, packHashes []string) error
}

// GCMetrics receives the cumulative counters produced by each GC pass.
// Implementations must be safe for concurrent use. casstore never holds a
// telemetry dependency itself; a host wires an adapter (for example, over
// its OTel Int64Counters) and assigns it to ChunkedGC.Metrics. All call
// sites guard against a nil ChunkedGC.Metrics, so leaving it unset is a
// no-op.
type GCMetrics interface {
	// AddReclaimedBytes adds n to the cumulative count of bytes reclaimed
	// across all GC passes. Called once per pass with the pass total.
	AddReclaimedBytes(ctx context.Context, n int64)
	// AddReclaimedChunks adds n to the cumulative count of chunks reclaimed
	// across all GC passes. Called once per pass with the pass total.
	AddReclaimedChunks(ctx context.Context, n int64)
}

// NewChunkedGC constructs a GC routine. Both arguments are required.
// logger may be nil (falls back to slog.Default). Telemetry is opt-in:
// assign ChunkedGC.Metrics after construction to receive reclaim counters.
func NewChunkedGC(manifests SnapshotStore, chunks blobstore.Storage, logger *slog.Logger) *ChunkedGC {
	if logger == nil {
		logger = slog.Default()
	}
	return &ChunkedGC{manifests: manifests, chunks: chunks, logger: logger, now: time.Now}
}

// SetClock overrides the clock used for the safety-window check (tests).
func (g *ChunkedGC) SetClock(now func() time.Time) { g.now = now }

func (g *ChunkedGC) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// GCResult summarises a single GC pass for metrics and logging.
type GCResult struct {
	// ChunksScanned is the total count of chunks in the blobstore at mark time.
	ChunksScanned int
	// ManifestsScanned is the total count of manifests walked.
	ManifestsScanned int
	// LiveChunks is the count of unique (tenant, hash) tuples referenced
	// by some manifest.
	LiveChunks int
	// ChunksReclaimed is the count of unreferenced chunks successfully deleted.
	ChunksReclaimed int
	// BytesReclaimed is the total size of reclaimed chunks.
	BytesReclaimed int64
	// ChunksSkippedRecent is the count of unreferenced chunks NOT deleted
	// because their Timestamp changed between mark and sweep (concurrent
	// writer). They are tried again on the next pass.
	ChunksSkippedRecent int
	// ChunksDeferredYoung is the count of unreferenced chunks NOT deleted
	// because they are younger than SafetyWindow (a write may still be about to
	// commit a manifest referencing them). Retried on a later pass.
	ChunksDeferredYoung int
	// ChunksSkippedError is the count of unreferenced chunks NOT deleted
	// because of a backend error during DeleteBlob or GetMetadata.
	ChunksSkippedError int
	// LiveFromAssociations reports whether the live set came from the association
	// index (fast path) rather than a manifest walk.
	LiveFromAssociations bool
	// Duration is the wall-clock time of the GC pass.
	Duration time.Duration
}

// RunOnce performs a single mark-and-sweep pass over ALL domains and returns
// its summary. The pass is cancel-safe: ctx.Done() aborts cleanly between
// chunks. It is the default for single-domain stores (e.g. sandbox-provider).
func (g *ChunkedGC) RunOnce(ctx context.Context) (GCResult, error) {
	return g.run(ctx, "")
}

// RunOnceForDomain performs a mark-and-sweep pass scoped to a single dedup
// domain: only that domain's chunk/pack blobs are listed, and only manifests
// stored under that domain contribute to the live set. Use it for
// per-tenant/per-domain GC schedules in multi-domain deployments. Blobs are
// domain-prefixed and a manifest only references its own domain's blobs, so a
// scoped pass is self-contained and never deletes another domain's data.
func (g *ChunkedGC) RunOnceForDomain(ctx context.Context, domain string) (GCResult, error) {
	return g.run(ctx, domain)
}

// run is the shared mark-and-sweep implementation. domainFilter == "" means
// all domains; otherwise the pass is scoped to that single domain.
func (g *ChunkedGC) run(ctx context.Context, domainFilter string) (GCResult, error) {
	start := time.Now()
	result := GCResult{}

	// Blob-ID prefixes to sweep: both v1 "chunk-" (standalone chunks) and v2
	// "pack-" (grouped packs) so a mixed-format store is handled in one pass,
	// narrowed to one domain when scoped.
	var prefixes []blobstore.ID
	if domainFilter == "" {
		prefixes = []blobstore.ID{"chunk-", "pack-"}
	} else {
		prefixes = []blobstore.ID{chunkTenantPrefix(domainFilter), packTenantPrefix(domainFilter)}
	}

	live, lerr := g.liveSet(ctx, domainFilter, &result)
	if lerr != nil {
		return result, lerr
	}
	result.LiveChunks = len(live)

	if serr := g.sweep(ctx, prefixes, live, &result); serr != nil {
		return result, serr
	}
	result.Duration = time.Since(start)
	return result, nil
}

// liveSet computes the set of blob IDs that must survive this pass, preferring
// the association index when it is available and trustworthy for this domain.
func (g *ChunkedGC) liveSet(ctx context.Context, domainFilter string, result *GCResult) (map[blobstore.ID]struct{}, error) {
	if live, ok, err := g.liveFromAssociations(ctx, domainFilter); err != nil {
		return nil, err
	} else if ok {
		result.LiveFromAssociations = true
		return live, nil
	}

	// For v1 manifests each chunk ref maps to a standalone chunk blob.
	// For v2 manifests each chunk ref maps to a pack blob (a pack is live
	// if ANY of its constituent chunks is referenced by a live manifest).
	live := make(map[blobstore.ID]struct{})
	// addLive parses one manifest payload and records the blob IDs it keeps alive.
	// Shared by the streaming-payload path and the per-manifest-Get fallback.
	addLive := func(key SnapshotKey, version string, payload []byte) {
		var m chunkManifest
		if err := json.Unmarshal(payload, &m); err != nil {
			g.logger.WarnContext(ctx, "chunked gc: manifest parse failed",
				"key", key, "version", version, "err", err)
			return
		}
		// Domain scoping: a manifest only references blobs in its own domain
		// (m.Tenant), so when scoped we ignore other domains' manifests.
		if domainFilter != "" && m.Tenant != domainFilter {
			return
		}
		result.ManifestsScanned++
		switch m.Format {
		case manifestFormatV1:
			for _, ref := range m.Chunks {
				live[chunkBlobID(m.Tenant, ref.Hash)] = struct{}{}
			}
		case manifestFormatV2:
			for _, ref := range m.Chunks {
				live[packBlobID(m.Tenant, ref.PackHash)] = struct{}{}
			}
		default:
			g.logger.WarnContext(ctx, "chunked gc: unknown manifest format, skipping",
				"key", key, "version", version, "format", m.Format)
		}
	}

	// Prefer the streaming-payload walk (one query, payload inline) when the
	// manifest store supports it — this removes the per-manifest Get (M individual
	// round trips, TECH_DEBT #28). Stores that don't (dev/local) fall back to the
	// Walk + per-manifest Get path below; both feed addLive identically.
	if pw, ok := g.manifests.(PayloadWalker); ok {
		if err := pw.WalkPayloads(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata, payload []byte) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			tag := meta.Tags["chunked_format"]
			if tag != manifestFormatV1 && tag != manifestFormatV2 {
				return nil // non-chunked snapshot: no chunk/pack refs
			}
			addLive(key, version, payload)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("chunked gc: walk payloads: %w", err)
		}
	} else {
		if err := g.manifests.Walk(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			tag := meta.Tags["chunked_format"]
			if tag != manifestFormatV1 && tag != manifestFormatV2 {
				return nil
			}
			// Fetch the raw manifest bytes via the SnapshotStore (not the
			// ChunkedStore, which would assemble the blob).
			rc, _, err := g.manifests.Get(ctx, key, version)
			if err != nil {
				g.logger.WarnContext(ctx, "chunked gc: skipping unreadable manifest",
					"key", key, "version", version, "err", err)
				return nil
			}
			data, readErr := io.ReadAll(rc)
			_ = rc.Close()
			if readErr != nil {
				g.logger.WarnContext(ctx, "chunked gc: manifest read failed",
					"key", key, "version", version, "err", readErr)
				return nil
			}
			addLive(key, version, data)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("chunked gc: walk manifests: %w", err)
		}
	}
	return live, nil
}

// liveFromAssociations answers the live set from the association index. ok=false
// means the fast path does not apply (no association store, an all-domains pass,
// or a domain that has not been backfilled) and the caller must walk manifests.
func (g *ChunkedGC) liveFromAssociations(ctx context.Context, domainFilter string) (map[blobstore.ID]struct{}, bool, error) {
	if g.Assoc == nil || domainFilter == "" {
		return nil, false, nil
	}
	done, err := g.Assoc.IsBackfilled(ctx, domainFilter)
	if err != nil {
		return nil, false, fmt.Errorf("chunked gc: check association backfill for %q: %w", domainFilter, err)
	}
	if !done {
		g.logger.InfoContext(ctx, "chunked gc: domain not association-backfilled; using manifest walk",
			"domain", domainFilter)
		return nil, false, nil
	}
	live := make(map[blobstore.ID]struct{})
	if err := g.Assoc.WalkLivePacks(ctx, domainFilter, func(packHash string) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		live[packBlobID(domainFilter, packHash)] = struct{}{}
		return nil
	}); err != nil {
		return nil, false, fmt.Errorf("chunked gc: walk live packs for %q: %w", domainFilter, err)
	}
	return live, true, nil
}

// sweep is phase 2: stream every blob and reclaim those not in the live set.
func (g *ChunkedGC) sweep(ctx context.Context, prefixes []blobstore.ID, live map[blobstore.ID]struct{}, result *GCResult) error {
	// Phase 2: STREAM every blob and reclaim those not in the live set. We do NOT
	// materialise an all-blobs map (the old "marked" phase) — memory is bounded by
	// the live set alone (TECH_DEBT #28). Listing AFTER building the live set does
	// not introduce a race: a blob written after the live-set build that appears in
	// this list is necessarily younger than the safety window (its manifest commits
	// moments after its upload), so sweepOrphan's window check defers it — the same
	// guarantee the original mark→live→sweep order relied on.
	// Backfill live packs' on-disk sizes as a side effect of the sweep: the list
	// below already visits every pack blob with its md.Length, so recording the
	// LIVE ones' sizes fills/refreshes the packs table for pre-existing packs that
	// were written before storage accounting was enabled (dead packs are dropped
	// anyway). Best-effort — the backfiller type-asserts g.Index to
	// PackSizeRecorder (nil-safe: a non-recording index → no-op) and a recording
	// failure is logged inside flush, so it NEVER fails the GC pass.
	bf := newPackSizeBackfiller(g.Index, g.logger)
	for _, prefix := range prefixes {
		if err := g.chunks.ListBlobs(ctx, prefix, func(md blobstore.Metadata) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			result.ChunksScanned++
			if _, isLive := live[md.BlobID]; isLive {
				// Live pack: also backfill its on-disk size (no-op for chunk blobs).
				bf.offer(ctx, md.BlobID, md.Length)
				return nil
			}
			g.sweepOrphan(ctx, md, result)
			return nil
		}); err != nil {
			return fmt.Errorf("chunked gc: sweep-list %s*: %w", prefix, err)
		}
	}
	bf.flush(ctx)
	return nil
}

// sweepOrphan reclaims one blob that the live-set phase found unreferenced. md is
// its list-time metadata. It re-verifies the timestamp hasn't changed (concurrent
// writer protection), defers blobs younger than the safety window, purges any
// dedup-index entries pointing at it, then deletes. Per-blob problems are logged +
// counted and skipped (the next pass retries); it never aborts the sweep.
func (g *ChunkedGC) sweepOrphan(ctx context.Context, md blobstore.Metadata, result *GCResult) {
	id := md.BlobID
	curMd, err := g.chunks.GetMetadata(ctx, id)
	if err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return // already gone (deleted between list and sweep)
		}
		g.logger.WarnContext(ctx, "chunked gc: skipping unreadable chunk metadata", "id", id, "err", err)
		result.ChunksSkippedError++
		return
	}
	if !curMd.Timestamp.Equal(md.Timestamp) {
		// Touched by a concurrent writer between the list and now → defer.
		result.ChunksSkippedRecent++
		return
	}
	// Safety window: never reclaim a blob younger than the window. A write can
	// upload a pack and only commit its referencing manifest moments later; without
	// this, a concurrent GC would treat that pack as orphaned and delete it.
	//
	// Clock-skew note (ADR §4 I3): this subtracts the GC node's wall clock
	// (g.clock()) from the S3-server-assigned object Timestamp (curMd.Timestamp).
	// The two clocks are independent, so a skew of up to a few seconds is expected
	// and is harmless only because the production window is on the hours scale (HC1)
	// — far larger than any realistic NTP skew. An operator who shrinks SafetyWindow
	// toward minutes/seconds MUST account for this skew, or a young blob could be
	// mis-judged old and reclaimed mid-write.
	if g.SafetyWindow > 0 && g.clock().Sub(curMd.Timestamp) < g.SafetyWindow {
		result.ChunksDeferredYoung++
		return
	}
	// Purge dedup-index entries for this pack BEFORE deleting the blob, so the
	// invariant "every index entry points to an existing pack" holds even if the
	// delete fails. Chunk (v1) blobs have no index entries, so only pack IDs are
	// purged. On purge failure, skip the delete and retry next pass.
	if g.Index != nil {
		if domain, packHash, ok := parsePackID(id); ok {
			if err := g.Index.PurgePacks(ctx, domain, []string{packHash}); err != nil {
				g.logger.WarnContext(ctx, "chunked gc: dedup-index purge failed; deferring blob delete", "id", id, "err", err)
				result.ChunksSkippedError++
				return
			}
		}
	}
	if err := g.chunks.DeleteBlob(ctx, id); err != nil {
		g.logger.WarnContext(ctx, "chunked gc: delete failed", "id", id, "err", err)
		result.ChunksSkippedError++
		return
	}
	result.ChunksReclaimed++
	result.BytesReclaimed += md.Length
}

// Loop runs RunOnce on every tick until ctx is cancelled. Designed to
// be launched as a goroutine from main.go behind leader election.
//
// On panic or any error during a pass the loop logs and continues — GC
// failures are operational concerns, not service-fatal.
func (g *ChunkedGC) Loop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		g.logger.InfoContext(ctx, "chunked gc: disabled (interval <= 0)")
		return
	}
	g.logger.InfoContext(ctx, "chunked gc: loop started", "interval", interval)

	// Run immediately on start so operators see GC activity right away
	// without waiting a full interval.
	g.runOnceLogged(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			g.logger.InfoContext(ctx, "chunked gc: loop exiting", "err", ctx.Err())
			return
		case <-ticker.C:
			g.runOnceLogged(ctx)
		}
	}
}

func (g *ChunkedGC) runOnceLogged(ctx context.Context) {
	res, err := g.RunOnce(ctx)
	if err != nil {
		g.logger.WarnContext(ctx, "chunked gc: pass failed", "err", err)
		atomic.StoreInt64(&lastGCBytesReclaimed, 0)
		return
	}
	g.logger.InfoContext(ctx, "chunked gc: pass complete",
		"chunks_scanned", res.ChunksScanned,
		"manifests_scanned", res.ManifestsScanned,
		"live_chunks", res.LiveChunks,
		"chunks_reclaimed", res.ChunksReclaimed,
		"bytes_reclaimed", res.BytesReclaimed,
		"chunks_skipped_recent", res.ChunksSkippedRecent,
		"chunks_deferred_young", res.ChunksDeferredYoung,
		"chunks_skipped_error", res.ChunksSkippedError,
		"duration", res.Duration,
	)
	atomic.StoreInt64(&lastGCBytesReclaimed, res.BytesReclaimed)
	atomic.StoreInt64(&lastGCChunksReclaimed, int64(res.ChunksReclaimed))
	atomic.StoreInt64(&lastGCChunksScanned, int64(res.ChunksScanned))
	atomic.StoreInt64(&lastGCLiveChunks, int64(res.LiveChunks))

	// Emit cumulative counters for this pass to the host's metrics sink, if
	// one is wired. A nil Metrics (the default) makes this a no-op.
	if g.Metrics != nil {
		if res.BytesReclaimed > 0 {
			g.Metrics.AddReclaimedBytes(ctx, res.BytesReclaimed)
		}
		if res.ChunksReclaimed > 0 {
			g.Metrics.AddReclaimedChunks(ctx, int64(res.ChunksReclaimed))
		}
	}
}

// Atomic counters published for OTel observable gauges. Read-only from
// outside this file; updated only by runOnceLogged.
var (
	lastGCBytesReclaimed  int64
	lastGCChunksReclaimed int64
	lastGCChunksScanned   int64
	lastGCLiveChunks      int64
)

// LastGCStats returns the most recent GC pass's metrics for OTel
// observable gauge callbacks. Safe to call concurrently with GC runs.
func LastGCStats() (bytesReclaimed, chunksReclaimed, chunksScanned, liveChunks int64) {
	return atomic.LoadInt64(&lastGCBytesReclaimed),
		atomic.LoadInt64(&lastGCChunksReclaimed),
		atomic.LoadInt64(&lastGCChunksScanned),
		atomic.LoadInt64(&lastGCLiveChunks)
}

// parsePackID extracts (domain, packHash) from a "pack-<domain>-<hash>" blob
// ID. The hash is a fixed-width 64-char sha256 hex suffix, so we parse from
// the right to stay robust to hyphens inside the domain. ok is false for any
// ID that is not a well-formed pack ID (e.g. a v1 "chunk-" blob, which carries
// no dedup-index entry to purge).
func parsePackID(id blobstore.ID) (domain, hash string, ok bool) {
	const prefix = "pack-"
	const hashLen = sha256HexLen
	s := string(id)
	if !strings.HasPrefix(s, prefix) {
		return "", "", false
	}
	rest := s[len(prefix):]
	if len(rest) < hashLen+1 {
		return "", "", false
	}
	sep := len(rest) - hashLen - 1
	if rest[sep] != '-' {
		return "", "", false
	}
	domain = rest[:sep]
	hash = rest[sep+1:]
	if !isHex(hash) {
		return "", "", false
	}
	return domain, hash, true
}

// sha256HexLen is the length of a sha256 digest in hex (32 bytes → 64 chars).
const sha256HexLen = 64

// isHex reports whether s is non-empty and entirely lowercase/uppercase hex
// digits. Used to validate the trailing hash when parsing a pack blob ID.
func isHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
