// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// defaultBatchReadConcurrency bounds in-flight coalesced pack GETs in
// GetLatestBatch when the caller passes concurrency <= 0.
const defaultBatchReadConcurrency = 8

// BatchReadStats reports how a GetLatestBatch resolved its data: the number of
// backing DATA fetches it issued (ranged sub-runs + whole-pack fallbacks +
// standalone chunk blobs; framing-probe GETs are excluded) and the total bytes
// fetched. It is the field signal that coalescing reduced round trips — e.g.
// PackGets ≪ number of keys on dense packs.
type BatchReadStats struct {
	PackGets int64
	Bytes    int64
}

// batchStatsAcc accumulates BatchReadStats across the concurrent bucket workers.
type batchStatsAcc struct {
	packGets atomic.Int64
	bytes    atomic.Int64
}

func (a *batchStatsAcc) add(gets, bytes int64) {
	a.packGets.Add(gets)
	a.bytes.Add(bytes)
}

// BatchLatestEntry is one key's latest-version manifest, as returned by a
// BatchLatestGetter: the metadata plus the raw manifest payload bytes (the same
// bytes GetLatest's reader would consume before decoding).
type BatchLatestEntry struct {
	Meta    SnapshotMetadata
	Payload []byte
}

// BatchLatestGetter is an OPTIONAL SnapshotStore capability: resolve the latest
// version of many keys in far fewer round trips than N GetLatest calls (e.g. one
// SQL query for a PG-backed manifest store). It returns each key's manifest
// payload + metadata; a key with no snapshot is simply ABSENT from the result map
// (not an error), mirroring how GetLatest reports ErrNoSnapshot per key.
//
// ChunkedStore.GetLatestBatch uses this when its upstream implements it, and
// falls back to per-key GetLatest otherwise — so it is purely a round-trip
// optimization, never a correctness dependency. The returned payload is the raw
// (already-decompressed, if the store compresses manifests) manifest blob.
type BatchLatestGetter interface {
	GetLatestBatch(ctx context.Context, keys []SnapshotKey) (map[SnapshotKey]BatchLatestEntry, error)
}

// keyState accumulates one key's chunk bytes as the pack buckets are fetched.
type keyState struct {
	tenant string     // manifest tenant — the blob-ID prefix for this key's chunks/packs
	refs   []chunkRef // stream order
	chunks [][]byte   // filled per index; each slot written by exactly one bucket worker
	failed atomic.Bool
}

// chunkLoc points a bucket's chunk back to the (key, stream index) it belongs to.
type chunkLoc struct {
	key SnapshotKey
	idx int
	ref chunkRef
}

// packBucket is the set of chunk locations sharing one backing blob (a v2 pack,
// or a v1 standalone chunk blob) — the unit of a single coalesced fetch.
type packBucket struct {
	pid        blobstore.ID
	standalone bool // v1: the blob is a single chunk, not a pack
	locs       []chunkLoc
}

// GetLatestBatch resolves the latest manifest for each key and returns each key's
// full assembled bytes, fetching each backing PACK ONCE for all the in-batch
// slices it holds — the cross-key coalescing the per-key GetLatest cannot do
// (within one slice, same-pack refs are contiguous and loadPackRun already
// coalesces them; across slices they are not, so this buckets globally by pack).
// Output is byte-identical to calling GetLatest per key; only the fetch shape
// changes. concurrency bounds in-flight pack GETs (<=0 → default).
//
// BEST-EFFORT, prefetch-oriented: a key whose manifest is missing/malformed, or
// any of whose packs fails to fetch, is simply ABSENT from the result (the demand
// path re-fetches per slice and surfaces any real error). It returns a non-nil
// error only when manifest resolution itself fails wholesale. The BatchReadStats
// report the backing data GETs issued + bytes fetched (the coalescing signal).
func (c *ChunkedStore) GetLatestBatch(ctx context.Context, keys []SnapshotKey, concurrency int) (map[SnapshotKey][]byte, BatchReadStats, error) {
	out := make(map[SnapshotKey][]byte, len(keys))
	if len(keys) == 0 {
		return out, BatchReadStats{}, nil
	}
	if concurrency <= 0 {
		concurrency = defaultBatchReadConcurrency
	}

	payloads, err := c.resolveManifestPayloads(ctx, keys)
	if err != nil {
		return nil, BatchReadStats{}, err
	}

	// Parse manifests → per-key state; bucket every chunkRef by its backing blob.
	states := make(map[SnapshotKey]*keyState, len(payloads))
	buckets := make(map[blobstore.ID]*packBucket)
	for key, payload := range payloads {
		var m chunkManifest
		if jerr := json.Unmarshal(payload, &m); jerr != nil {
			continue // malformed manifest: drop (demand path surfaces it)
		}
		if m.Format != manifestFormatV1 && m.Format != manifestFormatV2 {
			continue // not a chunked manifest
		}
		st := &keyState{tenant: m.Tenant, refs: m.Chunks, chunks: make([][]byte, len(m.Chunks))}
		states[key] = st
		for idx, ref := range m.Chunks {
			var id blobstore.ID
			standalone := ref.PackHash == "" // v1 normalised: standalone chunk blob
			if standalone {
				id = chunkBlobID(m.Tenant, ref.Hash)
			} else {
				id = packBlobID(m.Tenant, ref.PackHash)
			}
			b := buckets[id]
			if b == nil {
				b = &packBucket{pid: id, standalone: standalone}
				buckets[id] = b
			}
			b.locs = append(b.locs, chunkLoc{key: key, idx: idx, ref: ref})
		}
	}

	// Fetch every bucket once, bounded by concurrency. Each bucket writes only into
	// pre-allocated per-(key,idx) slots (no slot is shared between buckets), and
	// marks its keys failed on error — so post-Wait reads need no locking.
	var acc batchStatsAcc
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, b := range buckets {
		wg.Add(1)
		sem <- struct{}{}
		go func(b *packBucket) {
			defer wg.Done()
			defer func() { <-sem }()
			var ferr error
			if b.standalone {
				ferr = c.fetchStandaloneBucket(ctx, b, states, &acc)
			} else {
				ferr = c.fetchPackBucket(ctx, b, states, &acc)
			}
			if ferr != nil {
				for _, l := range b.locs {
					states[l.key].failed.Store(true)
				}
			}
		}(b)
	}
	wg.Wait()

	// Assemble each non-failed key's bytes in stream order. A non-failed key had
	// every chunk slot filled (each ref belongs to exactly one successful bucket).
	for key, st := range states {
		if st.failed.Load() {
			continue
		}
		total := 0
		for _, ch := range st.chunks {
			total += len(ch)
		}
		buf := make([]byte, 0, total)
		for _, ch := range st.chunks {
			buf = append(buf, ch...)
		}
		out[key] = buf
	}
	return out, BatchReadStats{PackGets: acc.packGets.Load(), Bytes: acc.bytes.Load()}, nil
}

// resolveManifestPayloads returns the raw (decoded) manifest bytes per key,
// using the upstream's BatchLatestGetter when available (one round trip) and
// falling back to per-key GetLatest otherwise. Keys with no snapshot are absent.
func (c *ChunkedStore) resolveManifestPayloads(ctx context.Context, keys []SnapshotKey) (map[SnapshotKey][]byte, error) {
	payloads := make(map[SnapshotKey][]byte, len(keys))
	if bg, ok := c.upstream.(BatchLatestGetter); ok {
		entries, err := bg.GetLatestBatch(ctx, keys)
		if err != nil {
			return nil, fmt.Errorf("chunked get batch: resolve manifests: %w", err)
		}
		for k, e := range entries {
			payloads[k] = e.Payload
		}
		return payloads, nil
	}
	// Fallback: per-key GetLatest (deduped). The upstream manifest store handles
	// its own decompression; ErrNoSnapshot keys are simply omitted.
	seen := make(map[SnapshotKey]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		rc, _, err := c.upstream.GetLatest(ctx, k)
		if err != nil {
			if errors.Is(err, ErrNoSnapshot) {
				continue
			}
			return nil, fmt.Errorf("chunked get batch: get-latest %v: %w", k, err)
		}
		b, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			return nil, fmt.Errorf("chunked get batch: read manifest %v: %w", k, rerr)
		}
		payloads[k] = b
	}
	return payloads, nil
}

// fetchPackBucket fetches one v2 pack's in-batch chunks (cross-key) and writes
// each chunk's verified bytes into its key/idx slot. Mirrors loadPackRun's framing
// + EOF fallback, but generalized across keys AND guarded against dedup
// fragmentation: it sorts the bucket's chunks by offset and issues one ranged GET
// per CONTIGUOUS sub-run, so a window scattered across a pack (a chunk at offset 0
// and another at 15 MiB, nothing needed between) fetches two tight ranges instead
// of one 15 MiB union (the §4 sparsity guard — never worse than the per-slice
// path, and exactly one GET for a dense pack). Compressed/un-probeable packs and
// the EOF-boundary case fall back to a single whole-pack fetch that serves every
// chunk.
func (c *ChunkedStore) fetchPackBucket(ctx context.Context, b *packBucket, states map[SnapshotKey]*keyState, acc *batchStatsAcc) error {
	// Sort by offset so contiguous chunks are adjacent for sub-run grouping.
	sort.Slice(b.locs, func(i, j int) bool { return b.locs[i].ref.Offset < b.locs[j].ref.Offset })
	for _, l := range b.locs {
		if l.ref.Offset < 0 {
			return fmt.Errorf("chunked get batch: pack %s negative offset %d", b.pid, l.ref.Offset)
		}
	}

	framing, err := c.probeFraming(ctx, b.pid)
	if err != nil {
		return err
	}
	if framing.algo == packAlgoChunked {
		// Per-chunk framing: compressed AND range-readable, same sub-run grouping
		// as the uncompressed path.
		return c.fetchChunkedPackBucket(ctx, b, framing, states, acc)
	}
	if framing.algo != CompressNone {
		// Whole-pack compressed (or un-probeable): one whole-pack fetch serves the
		// bucket.
		return c.assignWholePack(ctx, b, states, acc)
	}

	// Uncompressed: one ranged GET per maximal contiguous sub-run.
	for _, group := range groupContiguousLocs(b.locs) {
		run := make([]chunkRef, len(group))
		minOff, maxEnd := group[0].ref.Offset, 0
		for i, l := range group {
			run[i] = l.ref
			if e := l.ref.Offset + l.ref.Size; e > maxEnd {
				maxEnd = e
			}
		}
		if maxEnd-minOff <= 0 {
			// All-zero-size sub-run: no bytes to fetch; the hash check still runs.
			chunks, serr := sliceRunFromContent(nil, b.pid, shiftRun(run, minOff))
			if serr != nil {
				return serr
			}
			assignGroup(group, chunks, states)
			continue
		}
		off := int64(framing.contentBase + minOff)
		length := int64(maxEnd - minOff)
		buf := blobstore.NewOutputBuffer()
		if gerr := c.chunks.GetBlob(ctx, b.pid, off, length, buf); gerr != nil {
			if errors.Is(gerr, blobstore.ErrBlobNotFound) {
				return fmt.Errorf("chunked get batch: pack %s missing — unrestorable: %w", b.pid, ErrMissingBlob)
			}
			// EOF-boundary (or other non-missing) failure: a sub-run's end can land
			// exactly on pack-content EOF (gliner2 #47). Fall back to a single
			// whole-pack fetch that serves the ENTIRE bucket, not just this group.
			return c.assignWholePack(ctx, b, states, acc)
		}
		acc.add(1, int64(len(buf.Bytes())))
		chunks, serr := sliceRunFromContent(buf.Bytes(), b.pid, shiftRun(run, minOff))
		if serr != nil {
			return serr
		}
		assignGroup(group, chunks, states)
	}
	return nil
}

// fetchChunkedPackBucket is the per-chunk-framed counterpart of the uncompressed
// ranged path: one ranged GET per maximal contiguous sub-run, mapped through the
// pack's index, with each chunk decompressed independently. Any non-missing fetch
// failure (the EOF-boundary case on a tail sub-run) falls back to a single
// whole-pack fetch serving the entire bucket, exactly as the uncompressed path does.
func (c *ChunkedStore) fetchChunkedPackBucket(ctx context.Context, b *packBucket, framing packFraming, states map[SnapshotKey]*keyState, acc *batchStatsAcc) error {
	for _, group := range groupContiguousLocs(b.locs) {
		run := make([]chunkRef, len(group))
		minOff, maxEnd := group[0].ref.Offset, 0
		for i, l := range group {
			run[i] = l.ref
			if e := l.ref.Offset + l.ref.Size; e > maxEnd {
				maxEnd = e
			}
		}
		if maxEnd-minOff <= 0 {
			chunks, serr := sliceRunFromContent(nil, b.pid, shiftRun(run, minOff))
			if serr != nil {
				return serr
			}
			assignGroup(group, chunks, states)
			continue
		}
		storedOff, storedLen, first, last, serr := framing.chunkedStoredSpan(minOff, maxEnd)
		if serr != nil {
			return fmt.Errorf("chunked get batch: pack %s: %w", b.pid, serr)
		}
		buf := blobstore.NewOutputBuffer()
		if gerr := c.chunks.GetBlob(ctx, b.pid, int64(framing.bodyBase+storedOff), int64(storedLen), buf); gerr != nil {
			if errors.Is(gerr, blobstore.ErrBlobNotFound) {
				return fmt.Errorf("chunked get batch: pack %s missing — unrestorable: %w", b.pid, ErrMissingBlob)
			}
			return c.assignWholePack(ctx, b, states, acc)
		}
		acc.add(1, int64(len(buf.Bytes())))
		content, merr := materializeChunkedWindow(framing.chunked, first, last, buf.Bytes(), storedOff)
		if merr != nil {
			return fmt.Errorf("chunked get batch: pack %s: %w", b.pid, merr)
		}
		chunks, cerr := sliceRunFromContent(content, b.pid, shiftRun(run, minOff))
		if cerr != nil {
			return cerr
		}
		assignGroup(group, chunks, states)
	}
	return nil
}

// assignWholePack fetches+decompresses the whole pack once and assigns every
// chunk in the bucket from it (offsets relative to full content, no shift). Used
// for compressed packs and the uncompressed EOF-boundary fallback.
func (c *ChunkedStore) assignWholePack(ctx context.Context, b *packBucket, states map[SnapshotKey]*keyState, acc *batchStatsAcc) error {
	raw, err := c.wholePackContent(ctx, b.pid)
	if err != nil {
		return err
	}
	acc.add(1, int64(len(raw)))
	run := make([]chunkRef, len(b.locs))
	for i, l := range b.locs {
		run[i] = l.ref
	}
	chunks, err := sliceRunFromContent(raw, b.pid, run)
	if err != nil {
		return err
	}
	assignGroup(b.locs, chunks, states)
	return nil
}

// assignGroup writes each fetched chunk (in group order) into its key/idx slot.
func assignGroup(group []chunkLoc, chunks [][]byte, states map[SnapshotKey]*keyState) {
	for i, l := range group {
		states[l.key].chunks[l.idx] = chunks[i]
	}
}

// groupContiguousLocs splits offset-sorted locs into maximal runs whose byte
// ranges are adjacent or overlapping (gap 0), so each run maps to one tight ranged
// GET with no fetched-but-unneeded bytes. A dense pack yields one group; a
// dedup-scattered window yields several (never more than the per-slice count).
func groupContiguousLocs(locs []chunkLoc) [][]chunkLoc {
	groups := make([][]chunkLoc, 0, 1)
	cur := []chunkLoc{locs[0]}
	curEnd := locs[0].ref.Offset + locs[0].ref.Size
	for _, l := range locs[1:] {
		if l.ref.Offset <= curEnd { // adjacent or overlapping (dup chunk)
			cur = append(cur, l)
			if e := l.ref.Offset + l.ref.Size; e > curEnd {
				curEnd = e
			}
		} else {
			groups = append(groups, cur)
			cur = []chunkLoc{l}
			curEnd = l.ref.Offset + l.ref.Size
		}
	}
	return append(groups, cur)
}

// fetchStandaloneBucket fetches a v1 standalone chunk blob once and assigns its
// verified bytes to every location referencing it (identical-hash refs dedup to
// one blob). Mirrors loadStandaloneChunk's decompress + size check.
func (c *ChunkedStore) fetchStandaloneBucket(ctx context.Context, b *packBucket, states map[SnapshotKey]*keyState, acc *batchStatsAcc) error {
	buf := blobstore.NewOutputBuffer()
	if err := c.chunks.GetBlob(ctx, b.pid, 0, -1, buf); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return fmt.Errorf("chunked get batch: chunk %s missing — unrestorable: %w", b.pid, ErrMissingBlob)
		}
		return fmt.Errorf("chunked get batch: fetch chunk %s: %w", b.pid, err)
	}
	acc.add(1, int64(len(buf.Bytes())))
	got, derr := decompressPackBlob(buf.Bytes())
	if derr != nil {
		return fmt.Errorf("chunked get batch: chunk %s: %w", b.pid, derr)
	}
	for _, l := range b.locs {
		if len(got) != l.ref.Size {
			return fmt.Errorf("chunked get batch: chunk %s size mismatch: manifest=%d got=%d: %w", b.pid, l.ref.Size, len(got), ErrCorrupt)
		}
		states[l.key].chunks[l.idx] = append([]byte(nil), got...) // own copy per slot
	}
	return nil
}

// wholePackContent fetches and decompresses a whole pack (the compressed-pack and
// EOF-fallback path). No single-slot cache: GetLatestBatch processes each pack
// once, so a per-pack buffer that lives only for this call is the right bound.
func (c *ChunkedStore) wholePackContent(ctx context.Context, pid blobstore.ID) ([]byte, error) {
	buf := blobstore.NewOutputBuffer()
	if err := c.chunks.GetBlob(ctx, pid, 0, -1, buf); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("chunked get batch: pack %s missing — unrestorable: %w", pid, ErrMissingBlob)
		}
		return nil, fmt.Errorf("chunked get batch: fetch pack %s: %w", pid, err)
	}
	raw, derr := decompressPackBlob(buf.Bytes())
	if derr != nil {
		return nil, fmt.Errorf("chunked get batch: pack %s: %w", pid, derr)
	}
	return raw, nil
}

// probeFraming returns pid's pack framing, consulting/populating the shared
// store-level cache. Mirrors chunkStreamReader.framingFor: a header-length probe
// that, if rejected, caches the forceWholePack sentinel so the caller takes the
// whole-pack path.
func (c *ChunkedStore) probeFraming(ctx context.Context, pid blobstore.ID) (packFraming, error) {
	if f, ok := c.framingCache.Load(pid); ok {
		return f.(packFraming), nil
	}
	framing, err := probePackFraming(ctx, c.chunks, pid, "chunked get batch")
	if err != nil {
		return packFraming{}, err
	}
	c.framingCache.Store(pid, framing)
	return framing, nil
}
