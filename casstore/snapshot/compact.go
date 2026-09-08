// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// CompactConfig tunes pack compaction (repack). A pack is a candidate when it is
// under-filled (live bytes / pack bytes below MinFillRatio) OR small (below
// MinPackBytes) — i.e. dead-space-heavy from CoW deletes, or the tiny per-write
// packs left by an un-batched streaming writer.
type CompactConfig struct {
	MinFillRatio    float64       // repack packs whose live-byte fill is below this (0 disables; e.g. 0.5)
	MinPackBytes    int64         // also repack packs smaller than this (0 disables; e.g. 4<<20)
	MaxBytesPerPass int64         // cap live bytes rewritten per pass so it never starves live I/O (0 = unbounded)
	SafetyWindow    time.Duration // skip packs younger than this (don't race a fresh write)

	// Defrag, when true, consolidates in FILE (manifest stream) order instead of
	// the default chunk-hash order: a fragmented file's chunks are written
	// contiguously into the new packs, so a later sequential read coalesces them
	// into one (or few) pack GET(s) instead of one per scattered source pack. It
	// restores the read locality that dedup-scatter / legacy fragmentation
	// destroyed; first-ingest writes are already laid out this way. Cost: it holds
	// the candidate packs' raw bytes in memory at once (bounded by MaxBytesPerPass)
	// to place a file's chunks even when they span source packs, and parses each
	// manifest's chunk order (one extra per-key list, ~the live-chunk set). Shared
	// (deduped) chunks land with whichever file places them first — unavoidable for
	// content-addressed storage, and benign because model weights dedup poorly.
	Defrag bool
}

// CompactResult summarizes one Compact pass.
type CompactResult struct {
	PacksScanned       int
	PacksSelected      int
	PacksWritten       int
	ManifestsRewritten int
	BytesRewritten     int64
	Duration           time.Duration
}

// Compact consolidates under-filled / small v2 packs in `domain` into full
// (~packTarget) packs and rewrites the referencing manifests to point at them,
// leaving the now-orphaned old packs for the existing safety-windowed GC to
// reclaim. It NEVER deletes a pack itself. Content-addressing makes every step
// idempotent, so a crashed pass is safe to re-run.
//
// Scope/safety: it only touches keys with a SINGLE version (e.g. mlfs slices,
// which are write-once) — rewriting one version of a multi-version key would
// require dropping history, so such keys are skipped (their packs simply aren't
// compacted). Concurrent writers to a compacted key would risk a lost update, so
// run this leader-only/per-domain on write-once data. Compaction is
// CODEC-PRESERVING: each source pack's codec is recovered from its self-describing
// header (no filename needed) and live chunks are consolidated into a per-codec
// buffer, so an uncompressed (model) class stays uncompressed (range-readable +
// mmap-able) while a compressible class is re-compressed on consolidation. A
// header-less legacy pack reads as CompressNone.
func (c *ChunkedStore) Compact(ctx context.Context, domain string, cfg CompactConfig) (CompactResult, error) {
	start := time.Now()
	res := CompactResult{}
	if c.packTargetBytes == packingDisabled {
		return res, nil // no packing, nothing to compact
	}
	target := c.packTargetBytes

	// 1. Inventory the domain's packs: stored size + timestamp.
	type packInfo struct {
		size int64
		ts   time.Time
	}
	packs := map[string]packInfo{}
	if err := c.chunks.ListBlobs(ctx, packTenantPrefix(domain), func(md blobstore.Metadata) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, ph, ok := parsePackID(md.BlobID); ok {
			packs[ph] = packInfo{size: md.Length, ts: md.Timestamp}
		}
		return nil
	}); err != nil {
		return res, fmt.Errorf("compact: list packs: %w", err)
	}
	res.PacksScanned = len(packs)
	if len(packs) == 0 {
		res.Duration = time.Since(start)
		return res, nil
	}

	// 2. Walk the LATEST manifests → per pack: deduped live chunks + referencing
	// keys. Only the latest version of each key is considered (see scope note).
	type liveChunk struct{ offset, size int }
	packLive := map[string]map[string]liveChunk{} // packHash -> chunkHash -> location
	packKeys := map[string]map[SnapshotKey]bool{} // packHash -> referencing keys
	// keyOrder (Defrag only) records each key's chunkRefs in MANIFEST stream order
	// (filtered to chunks in known packs), so consolidation can place a file's
	// chunks contiguously rather than scattered by hash.
	var keyOrder map[SnapshotKey][]chunkRef
	if cfg.Defrag {
		keyOrder = map[SnapshotKey][]chunkRef{}
	}
	seen := map[SnapshotKey]bool{}
	if err := c.upstream.Walk(ctx, func(key SnapshotKey, _ string, meta SnapshotMetadata) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if meta.Tags["chunked_format"] != manifestFormatV2 || seen[key] {
			return nil
		}
		seen[key] = true
		m, ok, err := c.latestManifest(ctx, key)
		if err != nil {
			return err
		}
		if !ok || m.Format != manifestFormatV2 || m.Tenant != domain {
			return nil
		}
		var ordered []chunkRef
		for _, ref := range m.Chunks {
			if ref.PackHash == "" {
				continue
			}
			if _, known := packs[ref.PackHash]; !known {
				continue
			}
			lm := packLive[ref.PackHash]
			if lm == nil {
				lm = map[string]liveChunk{}
				packLive[ref.PackHash] = lm
			}
			lm[ref.Hash] = liveChunk{offset: ref.Offset, size: ref.Size}
			km := packKeys[ref.PackHash]
			if km == nil {
				km = map[SnapshotKey]bool{}
				packKeys[ref.PackHash] = km
			}
			km[key] = true
			if cfg.Defrag {
				ordered = append(ordered, ref)
			}
		}
		if cfg.Defrag && len(ordered) > 0 {
			keyOrder[key] = ordered
		}
		return nil
	}); err != nil {
		return res, fmt.Errorf("compact: walk manifests: %w", err)
	}

	// 3+4. Select candidates (deterministic order; bounded per pass).
	order := make([]string, 0, len(packLive))
	for ph := range packLive {
		order = append(order, ph)
	}
	sort.Strings(order)
	var candidates []string
	var budget int64
	now := time.Now()
	for _, ph := range order {
		info := packs[ph]
		var liveBytes int64
		for _, lc := range packLive[ph] {
			liveBytes += int64(lc.size)
		}
		fill := 1.0
		if info.size > 0 {
			fill = float64(liveBytes) / float64(info.size)
		}
		underFilled := cfg.MinFillRatio > 0 && fill < cfg.MinFillRatio
		small := cfg.MinPackBytes > 0 && info.size < cfg.MinPackBytes
		if !underFilled && !small {
			continue
		}
		if cfg.SafetyWindow > 0 && now.Sub(info.ts) < cfg.SafetyWindow {
			continue
		}
		if cfg.MaxBytesPerPass > 0 && len(candidates) > 0 && budget+liveBytes > cfg.MaxBytesPerPass {
			break
		}
		candidates = append(candidates, ph)
		budget += liveBytes
	}

	// Worth-it guard: only compact when it actually REDUCES the pack count.
	// A lone sub-target pack (or a set whose live bytes still need as many packs)
	// would otherwise be rewritten into itself every pass. budget is the candidate
	// set's total live bytes; the consolidated set needs ~ceil(budget/target) packs
	// (over-estimate by ≤1, so a marginal merge may be deferred — fine).
	estNewPacks := int((budget + int64(target) - 1) / int64(target))
	if estNewPacks < 1 {
		estNewPacks = 1
	}
	if len(candidates) < 2 || estNewPacks >= len(candidates) {
		candidates = nil
	}
	res.PacksSelected = len(candidates)
	if len(candidates) == 0 {
		res.Duration = time.Since(start)
		return res, nil
	}
	selected := make(map[string]bool, len(candidates))
	for _, ph := range candidates {
		selected[ph] = true
	}

	// 5. Read selected packs' live chunks (each pack fetched + decompressed once)
	// and assemble new packs; relocate maps each chunk to its new location. A pack
	// carries exactly ONE codec, so consolidation is codec-preserving: each chunk
	// is accumulated into a per-codec buffer keyed by its SOURCE pack's codec
	// (recovered, header-only, by the codec-aware fetch) and each consolidated pack
	// is re-written with that codec. This keeps an uncompressed (model) class
	// range-readable + mmap-able and lets a compressible class actually shrink on
	// consolidation — the compactor never sees a filename (codec is self-describing
	// in the pack header). Per-codec buffering means candidates need not be
	// pre-sorted by codec; deterministic flush order keeps output reproducible.
	relocate := map[string]PackRef{}
	placed := map[string]bool{} // chunk hash → staged this pass (or already relocated)
	type stagedChunk struct {
		hash string
		off  int
		size int
	}
	type codecBuf struct {
		buf    []byte
		staged []stagedChunk
	}
	// psr records each rewritten pack's physical (compressed) size for storage
	// accounting. Optional (nil unless the index's durable store implements
	// PackSizeRecorder); best-effort — logged, never fails the compaction. The
	// old (candidate) packs' rows are dropped by PurgePacks below, and any
	// orphaned rewritten pack (never referenced) is later reclaimed by GC, which
	// purges its packs row too.
	psr := c.packSizeRecorder()

	bufs := map[CompressionAlgo]*codecBuf{}
	flush := func(algo CompressionAlgo, cb *codecBuf) error {
		if cb == nil || len(cb.buf) == 0 {
			return nil
		}
		sum := sha256.Sum256(cb.buf)
		ph := hex.EncodeToString(sum[:])
		stored, err := c.storePackBlob(cb.buf, packSpansFromEntries(len(cb.staged), func(i int) (int, int) {
			return cb.staged[i].off, cb.staged[i].size
		}), algo)
		if err != nil {
			return err
		}
		if err := c.deduper.PutIfAbsent(ctx, packBlobID(domain, ph), blobstore.BytesFromSlice(stored)); err != nil {
			return fmt.Errorf("compact: write pack: %w", err)
		}
		// Record the rewritten pack's physical (compressed) size — one row per
		// pack, best-effort.
		recordPackSize(ctx, psr, domain, ph, len(stored))
		for _, s := range cb.staged {
			relocate[s.hash] = PackRef{PackHash: ph, Offset: s.off, Size: s.size}
		}
		res.PacksWritten++
		res.BytesRewritten += int64(len(cb.buf))
		cb.buf = cb.buf[:0]
		cb.staged = cb.staged[:0]
		return nil
	}
	// emit copies one chunk's bytes into its codec buffer, recording the relocation
	// (via flush) and marking it placed; shared by the hash-order and file-order
	// consolidation paths.
	emit := func(algo CompressionAlgo, hash string, data []byte) error {
		cb := bufs[algo]
		if cb == nil {
			cb = &codecBuf{}
			bufs[algo] = cb
		}
		off := len(cb.buf)
		cb.buf = append(cb.buf, data...)
		cb.staged = append(cb.staged, stagedChunk{hash: hash, off: off, size: len(data)})
		placed[hash] = true
		if len(cb.buf) >= target {
			return flush(algo, cb)
		}
		return nil
	}

	if cfg.Defrag {
		// File-order: place each file's chunks contiguously, fetching+caching the
		// source packs (a file's chunks may span several). The cache is bounded by
		// the candidate budget (MaxBytesPerPass).
		type cachedPack struct {
			raw  []byte
			algo CompressionAlgo
		}
		rawCache := map[string]cachedPack{}
		fetch := func(ph string) (cachedPack, error) {
			if e, ok := rawCache[ph]; ok {
				return e, nil
			}
			raw, algo, err := c.fetchPackRaw(ctx, domain, ph)
			if err != nil {
				return cachedPack{}, err
			}
			e := cachedPack{raw: raw, algo: algo}
			rawCache[ph] = e
			return e, nil
		}
		keys := make([]SnapshotKey, 0, len(keyOrder))
		for k := range keyOrder {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].OwnerKey < keys[j].OwnerKey })
		for _, key := range keys {
			for _, ref := range keyOrder[key] {
				if !selected[ref.PackHash] || placed[ref.Hash] {
					continue
				}
				e, err := fetch(ref.PackHash)
				if err != nil {
					return res, err
				}
				if ref.Offset < 0 || ref.Offset+ref.Size > len(e.raw) {
					return res, fmt.Errorf("compact: pack %s chunk %s out of range", ref.PackHash, ref.Hash)
				}
				if err := emit(e.algo, ref.Hash, e.raw[ref.Offset:ref.Offset+ref.Size]); err != nil {
					return res, err
				}
			}
		}
	} else {
		// Hash-order (default): consolidate each candidate pack's live chunks.
		for _, ph := range candidates {
			raw, algo, err := c.fetchPackRaw(ctx, domain, ph)
			if err != nil {
				return res, err
			}
			hashes := make([]string, 0, len(packLive[ph]))
			for h := range packLive[ph] {
				hashes = append(hashes, h)
			}
			sort.Strings(hashes)
			for _, h := range hashes {
				if placed[h] {
					continue // dedup: chunk already placed in a (this or prior) new pack
				}
				lc := packLive[ph][h]
				if lc.offset < 0 || lc.offset+lc.size > len(raw) {
					return res, fmt.Errorf("compact: pack %s chunk %s out of range", ph, h)
				}
				if err := emit(algo, h, raw[lc.offset:lc.offset+lc.size]); err != nil {
					return res, err
				}
			}
		}
	}
	// Flush the residual per-codec buffers in a deterministic codec order.
	algos := make([]CompressionAlgo, 0, len(bufs))
	for algo := range bufs {
		algos = append(algos, algo)
	}
	sort.Slice(algos, func(i, j int) bool { return algos[i] < algos[j] })
	for _, algo := range algos {
		if err := flush(algo, bufs[algo]); err != nil {
			return res, err
		}
	}

	// 6. Repoint the dedup index BEFORE rewriting manifests: the relocated chunks now
	// live in the freshly-written consolidated packs, and the candidate packs are
	// headed for GC. If a durable index kept pointing these chunks at the old packs, a
	// later Put that dedups one of them would re-reference — and thus re-pin against
	// GC — a pack that should drain, so the old pack would never reclaim (TECH_DEBT
	// #51). Done before the manifest rewrite so a failure here is retry-safe: the old
	// packs stay in their latest manifests and are re-selected (and re-repointed,
	// idempotently) next pass, while the safety window protects the fresh consolidated
	// packs from premature GC in the gap. No-op for indexes with no durable
	// cross-object store (PriorManifestIndex) — step 7's rewrite is their repoint.
	if rp, ok := c.index.(dedupRepointer); ok && len(relocate) > 0 {
		newLocs := make([]ChunkLocation, 0, len(relocate))
		for h, ref := range relocate {
			newLocs = append(newLocs, ChunkLocation{ChunkHash: h, PackRef: ref})
		}
		if err := rp.Repoint(ctx, domain, newLocs, candidates); err != nil {
			return res, fmt.Errorf("compact: repoint dedup index: %w", err)
		}
	}

	// 7. Rewrite each referencing key's manifest to the new packs (single-version
	// keys only). The old version is dropped so the old pack becomes orphaned.
	rewritten := map[SnapshotKey]bool{}
	for _, ph := range candidates {
		for key := range packKeys[ph] {
			if rewritten[key] {
				continue
			}
			rewritten[key] = true
			changed, err := c.rewriteSelectedRefs(ctx, key, selected, relocate)
			if err != nil {
				return res, fmt.Errorf("compact: rewrite %v: %w", key, err)
			}
			if changed {
				res.ManifestsRewritten++
			}
		}
	}
	res.Duration = time.Since(start)
	return res, nil
}

// latestManifest fetches + parses the latest v1/v2 manifest for key. ok is false
// when there is no snapshot or the blob is not one of our manifests.
func (c *ChunkedStore) latestManifest(ctx context.Context, key SnapshotKey) (chunkManifest, bool, error) {
	var m chunkManifest
	rc, _, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return m, false, nil
		}
		return m, false, err
	}
	data, rerr := io.ReadAll(rc)
	_ = rc.Close()
	if rerr != nil {
		return m, false, fmt.Errorf("read manifest: %w", rerr)
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, false, nil // not a chunked manifest
	}
	return m, true, nil
}

// fetchPackRaw fetches a pack blob and decompresses it to its raw (uncompressed)
// content so chunks can be sliced out by their (offset, size). It also returns the
// codec the pack was STORED under, recovered from the self-describing pack header
// (NOT the manifest — a v2 manifest carries no per-pack codec), so the compactor
// can re-write each consolidated pack with its source codec (codec-preserving
// repack). Legacy header-less packs report CompressNone.
func (c *ChunkedStore) fetchPackRaw(ctx context.Context, domain, packHash string) ([]byte, CompressionAlgo, error) {
	buf := blobstore.NewOutputBuffer()
	if err := c.chunks.GetBlob(ctx, packBlobID(domain, packHash), 0, -1, buf); err != nil {
		return nil, "", fmt.Errorf("compact: fetch pack %s: %w", packHash, err)
	}
	framing, derr := detectPackFraming(buf.Bytes())
	if derr != nil {
		return nil, "", fmt.Errorf("compact: pack %s framing: %w", packHash, derr)
	}
	raw, err := decompressPackBlob(buf.Bytes())
	if err != nil {
		return nil, "", fmt.Errorf("compact: pack %s: %w", packHash, err)
	}
	return raw, framing.logicalCodec(), nil
}

// rewriteSelectedRefs rewrites key's manifest, relocating every chunkRef whose
// PackHash is in `selected` to relocate[hash], writes the new manifest version,
// and drops the prior version so the old pack can be reclaimed. Only single-
// version keys are rewritten (multi-version keys are left untouched). Returns
// false (no error) when nothing changed or the key was skipped.
func (c *ChunkedStore) rewriteSelectedRefs(ctx context.Context, key SnapshotKey, selected map[string]bool, relocate map[string]PackRef) (bool, error) {
	versions, err := c.upstream.List(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return false, nil
		}
		return false, err
	}
	if len(versions) != 1 {
		return false, nil // multi-version key: skip (would drop history)
	}
	oldVersion := versions[0].Version

	rc, meta, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return false, err
	}
	data, rerr := io.ReadAll(rc)
	_ = rc.Close()
	if rerr != nil {
		return false, fmt.Errorf("read manifest: %w", rerr)
	}
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return false, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Format != manifestFormatV2 {
		return false, nil
	}
	changed := false
	for i := range m.Chunks {
		if !selected[m.Chunks[i].PackHash] {
			continue
		}
		nl, ok := relocate[m.Chunks[i].Hash]
		if !ok {
			return false, fmt.Errorf("no relocation for chunk %s (pack %s)", m.Chunks[i].Hash, m.Chunks[i].PackHash)
		}
		m.Chunks[i].PackHash = nl.PackHash
		m.Chunks[i].Offset = nl.Offset
		// Hash + Size are unchanged by relocation.
		changed = true
	}
	if !changed {
		return false, nil
	}
	nb, err := json.Marshal(&m)
	if err != nil {
		return false, fmt.Errorf("marshal manifest: %w", err)
	}
	// Write the rewritten manifest (new version), THEN drop the old version so a
	// crash between the two leaves both readable (old pack still present) and the
	// next pass cleans up.
	if _, err := c.upstream.Put(ctx, key, meta, bytesReader(nb)); err != nil {
		return false, fmt.Errorf("put rewritten manifest: %w", err)
	}
	if err := c.upstream.Delete(ctx, key, oldVersion); err != nil {
		return false, fmt.Errorf("delete old manifest version: %w", err)
	}
	return true, nil
}
