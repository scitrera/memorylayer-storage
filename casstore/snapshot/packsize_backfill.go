// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"errors"
	"log/slog"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// packSizeBackfillFlushThreshold is the number of staged pack sizes at which the
// backfiller flushes a batch to the recorder. Bounds memory during a full-store
// sweep while keeping the per-flush round-trip count low.
const packSizeBackfillFlushThreshold = 512

// packSizeBackfiller batches per-pack physical (on-disk) sizes and records them
// into a PackSizeRecorder in bounded flushes. It is the shared engine behind both
// the GC sweep hook (Part B — records live packs' sizes as a side effect of the
// pass) and the one-time BackfillPackSizes (Part C — records every pack). It is
// nil-safe on the recorder axis: an index that does not implement
// PackSizeRecorder yields a nil rec, making every offer/flush a no-op, so callers
// need not branch. Best-effort throughout: a RecordPackSizes error is logged and
// swallowed (the batch is dropped, the sweep/backfill continues) so pack-size
// accounting never fails the operation it rides on.
type packSizeBackfiller struct {
	rec    PackSizeRecorder      // nil if the index doesn't support recording
	batch  map[string][]PackSize // domain -> pending pack sizes
	n      int                   // pending records since the last flush
	total  int                   // total records staged across the lifetime
	logger *slog.Logger
}

// newPackSizeBackfiller builds a backfiller over index's optional
// PackSizeRecorder capability. index may be any value (the GC's DedupGC, a
// DedupStore, nil, ...): it is type-asserted to PackSizeRecorder, so an index
// that does not record leaves rec nil and the backfiller is an inert no-op.
// logger may be nil (falls back to slog.Default).
func newPackSizeBackfiller(index any, logger *slog.Logger) *packSizeBackfiller {
	if logger == nil {
		logger = slog.Default()
	}
	rec, _ := index.(PackSizeRecorder)
	return &packSizeBackfiller{
		rec:    rec,
		batch:  make(map[string][]PackSize),
		logger: logger,
	}
}

// offer stages one blob's on-disk size for recording IF it is a pack blob. It is
// a no-op when the index doesn't record (rec nil) or when blobID is not a
// well-formed pack ID (a v1 "chunk-" blob carries no packs-table row). Flushes
// once the pending count reaches packSizeBackfillFlushThreshold.
func (b *packSizeBackfiller) offer(ctx context.Context, blobID blobstore.ID, length int64) {
	if b.rec == nil {
		return
	}
	domain, hash, ok := parsePackID(blobID)
	if !ok {
		return
	}
	b.batch[domain] = append(b.batch[domain], PackSize{PackHash: hash, CompressedBytes: length})
	b.n++
	b.total++
	if b.n >= packSizeBackfillFlushThreshold {
		b.flush(ctx)
	}
}

// flush records every staged batch, per domain, and resets the pending state. It
// is best-effort: a RecordPackSizes error is logged at WARN and the domain's
// batch is dropped so the caller (GC sweep or backfill) continues. A no-op when
// the index doesn't record or nothing is staged.
func (b *packSizeBackfiller) flush(ctx context.Context) {
	if b.rec == nil || b.n == 0 {
		return
	}
	for domain, sizes := range b.batch {
		if len(sizes) == 0 {
			continue
		}
		if err := b.rec.RecordPackSizes(ctx, domain, sizes); err != nil {
			b.logger.WarnContext(ctx, "pack-size backfill: record failed; dropping batch",
				"domain", domain, "count", len(sizes), "err", err)
		}
	}
	// Reset for the next batch (best-effort: even on a partial error we drop the
	// staged batch and move on; a later pass/backfill re-records).
	b.batch = make(map[string][]PackSize)
	b.n = 0
}

// BackfillPackSizes lists every pack blob (a domain's, or all domains when
// domain=="") and records each pack's on-disk size (md.Length) into the index's
// PackSizeRecorder. Read-only w.r.t. blobs — it reclaims nothing; it only
// fills/refreshes the packs table (e.g. after enabling storage accounting on a
// store with pre-existing data). Idempotent.
func BackfillPackSizes(ctx context.Context, chunks blobstore.Storage, index any, domain string, logger *slog.Logger) (recorded int, err error) {
	bf := newPackSizeBackfiller(index, logger)
	if bf.rec == nil {
		return 0, errors.New("index does not support pack-size recording")
	}
	prefix := blobstore.ID("pack-")
	if domain != "" {
		prefix = packTenantPrefix(domain)
	}
	if err := chunks.ListBlobs(ctx, prefix, func(md blobstore.Metadata) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		bf.offer(ctx, md.BlobID, md.Length)
		return nil
	}); err != nil {
		return bf.total, err
	}
	bf.flush(ctx)
	return bf.total, nil
}
