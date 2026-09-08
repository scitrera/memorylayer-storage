// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Association backfill.
//
// Associations are recorded on write, so a store that predates them holds none.
// An association-derived live set would therefore judge every pre-existing pack
// unreferenced and delete the entire store. BackfillAssociations closes that gap:
// it walks the manifests ONCE per domain — the same traversal the old GC did on
// every pass — records the associations those manifests imply, and sets a marker.
// ChunkedGC refuses the association-derived live set for any domain whose marker
// is unset, so the dangerous state is unreachable rather than merely documented.

// BackfillResult summarises one BackfillAssociations call.
type BackfillResult struct {
	// ManifestsScanned is the number of chunked manifests walked.
	ManifestsScanned int
	// AssociationsRecorded is the number of (pack, context) rows written.
	AssociationsRecorded int
	// ContextsSeen is the number of distinct owner contexts encountered.
	ContextsSeen int
	// Duration is the wall-clock time of the backfill.
	Duration time.Duration
}

// BackfillAssociations populates the association index for domain from the
// manifests already in the store, then marks the domain backfilled so the
// association-derived live set may be trusted.
//
// It is idempotent: Associate is idempotent per (pack, context), so re-running is
// safe and simply re-asserts the same rows. Run it once per domain when adopting
// associations, and after any bulk restore that bypassed the write path.
//
// domain == "" backfills every domain found in the walked manifests.
func (c *ChunkedStore) BackfillAssociations(ctx context.Context, domain string, logger *slog.Logger) (BackfillResult, error) {
	if logger == nil {
		logger = slog.Default()
	}
	store := c.packAssocStore()
	if store == nil {
		return BackfillResult{}, fmt.Errorf("backfill associations: configured dedup index has no association support")
	}

	res := BackfillResult{}
	start := time.Now()
	contexts := make(map[string]struct{})
	domains := make(map[string]struct{})

	// Buffer per (domain) so Associate is batched rather than row-at-a-time.
	pending := make(map[string][]PackAssociation)
	flush := func(d string) error {
		batch := pending[d]
		if len(batch) == 0 {
			return nil
		}
		if err := store.Associate(ctx, d, batch); err != nil {
			return fmt.Errorf("backfill associations: record %d rows in domain %q: %w", len(batch), d, err)
		}
		res.AssociationsRecorded += len(batch)
		pending[d] = batch[:0]
		return nil
	}

	// absorb turns one manifest into its association rows. The context is the
	// object's OwnerKey, matching contextFor's default — a backfilled association
	// must be indistinguishable from one the write path would have recorded.
	absorb := func(key SnapshotKey, version string, payload []byte) error {
		var m chunkManifest
		if err := json.Unmarshal(payload, &m); err != nil {
			logger.WarnContext(ctx, "backfill associations: manifest parse failed",
				"key", key, "version", version, "err", err)
			return nil
		}
		if m.Format != manifestFormatV2 {
			// v1 manifests reference standalone chunk blobs, which have no pack to
			// associate. They are swept by the manifest-walk path as before.
			return nil
		}
		if domain != "" && m.Tenant != domain {
			return nil
		}
		res.ManifestsScanned++
		domains[m.Tenant] = struct{}{}
		packContext := key.OwnerKey
		contexts[packContext] = struct{}{}
		seen := make(map[string]struct{}, len(m.Chunks))
		for _, ref := range m.Chunks {
			if ref.PackHash == "" {
				continue
			}
			if _, dup := seen[ref.PackHash]; dup {
				continue
			}
			seen[ref.PackHash] = struct{}{}
			pending[m.Tenant] = append(pending[m.Tenant], PackAssociation{
				PackHash: ref.PackHash,
				Context:  packContext,
			})
		}
		if len(pending[m.Tenant]) >= backfillBatchSize {
			return flush(m.Tenant)
		}
		return nil
	}

	if err := c.walkManifestPayloads(ctx, logger, absorb); err != nil {
		return res, err
	}
	for d := range pending {
		if err := flush(d); err != nil {
			return res, err
		}
	}

	// Mark every domain the walk touched (or the single requested domain, even if
	// it held no manifests — an empty domain is legitimately fully backfilled).
	if domain != "" {
		domains[domain] = struct{}{}
	}
	for d := range domains {
		if err := store.MarkBackfilled(ctx, d); err != nil {
			return res, fmt.Errorf("backfill associations: mark domain %q backfilled: %w", d, err)
		}
	}

	res.ContextsSeen = len(contexts)
	res.Duration = time.Since(start)
	logger.InfoContext(ctx, "backfill associations: complete",
		"domain", domain,
		"manifests_scanned", res.ManifestsScanned,
		"associations_recorded", res.AssociationsRecorded,
		"contexts_seen", res.ContextsSeen,
		"duration", res.Duration)
	return res, nil
}

// backfillBatchSize bounds how many association rows are buffered before a write.
const backfillBatchSize = 1000

// walkManifestPayloads streams every chunked manifest's payload, preferring the
// PayloadWalker fast path (one streaming query) and falling back to Walk plus a
// per-manifest Get. It is the shared traversal behind both the backfill and the
// GC's manifest-derived live set.
func (c *ChunkedStore) walkManifestPayloads(ctx context.Context, logger *slog.Logger, yield func(key SnapshotKey, version string, payload []byte) error) error {
	if pw, ok := c.upstream.(PayloadWalker); ok {
		return pw.WalkPayloads(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata, payload []byte) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !isChunkedManifest(meta) {
				return nil
			}
			return yield(key, version, payload)
		})
	}
	return c.upstream.Walk(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isChunkedManifest(meta) {
			return nil
		}
		rc, _, err := c.upstream.Get(ctx, key, version)
		if err != nil {
			logger.WarnContext(ctx, "backfill associations: skipping unreadable manifest",
				"key", key, "version", version, "err", err)
			return nil
		}
		payload, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			logger.WarnContext(ctx, "backfill associations: manifest read failed",
				"key", key, "version", version, "err", readErr)
			return nil
		}
		return yield(key, version, payload)
	})
}

// isChunkedManifest reports whether a snapshot's metadata marks it as a chunked
// manifest (v1 or v2) rather than an ordinary payload.
func isChunkedManifest(meta SnapshotMetadata) bool {
	tag := meta.Tags["chunked_format"]
	return tag == manifestFormatV1 || tag == manifestFormatV2
}
