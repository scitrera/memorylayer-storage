// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio

// Sequential-read prefetch (readahead).
//
// The Linux kernel does its own FUSE readahead, but it is capped at 128 KiB
// (and never exceeds MaxWrite) — plenty to keep a warm/local mount saturated,
// but far too shallow to hide a cold backing-store round trip. On a sequential
// stream over the -remote (direct-S3) path every slice miss pays a ReadSlices
// (PG) query plus an object GET (~tens-to-hundreds of ms); reading a multi-GB
// model file one demand slice at a time serializes thousands of those.
//
// readAhead watches each inode's access pattern and, once a read looks
// sequential, proactively materializes the next window of slices into the disk
// cache on a bounded background pool — so the bytes are local before the
// application asks. It is strictly best-effort: any prefetch may be skipped or
// fail without affecting correctness (the demand read fetches the slice
// normally), so it never blocks a read, never propagates an error, and stays
// bounded in goroutines, in-flight fetches, and metadata-pool pressure.

import (
	"context"
	"sync"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

// localCacheChecker is an optional chunkstore.Store capability: report whether a
// slice is resident in the LOCAL cache without a backing-store round trip.
// Prefetch uses it to skip slices that are already cached (so it never
// re-fetches) — and to avoid an Exists() that would itself hit S3 in -remote
// mode. A store that cannot answer cheaply simply omits it (checker stays nil).
type localCacheChecker interface {
	CachedLocally(id uint64) bool
}

const (
	// defaultReadaheadTrigger is the sequential run length a stream must reach
	// before prefetch engages — long enough to skip one-shot/random reads, short
	// enough to kick in early on a real streaming load.
	defaultReadaheadTrigger = 2 << 20 // 2 MiB
	// defaultReadaheadConcurrency bounds concurrent prefetch backing fetches when
	// the caller does not specify one. It is the ceiling on the extra PG
	// (manifest/ReadSlices) + backing (S3 GET) load readahead adds, so on a cold
	// sequential read it is the real throughput knob: aggregate ≈ concurrency ×
	// per-slice rate. 8 matches the default metadata pool (-db-max-open-conns 8);
	// raise BOTH together on read-heavy pools (e.g. GPU model loading) — a higher
	// concurrency starved of metadata connections just queues on the PG pool.
	defaultReadaheadConcurrency = 8
	// maxReadaheadTrackers caps the per-inode state map so a workload that touches
	// millions of files cannot grow it without bound; over the cap we evict one
	// arbitrary entry per insert (losing only prefetch state, which self-rewarms).
	maxReadaheadTrackers = 8192
)

// readAhead prefetches upcoming slices into the disk cache on sequential reads.
type readAhead struct {
	window  int64 // bytes to keep warm ahead of the read cursor (0 = disabled)
	trigger int64 // sequential run bytes before prefetch engages
	files   *Files
	checker localCacheChecker // nil if the store can't answer local-presence cheaply
	metrics *metrics.Registry // nil-safe

	// ctx is a DETACHED daemon-lifetime context — never a per-request ctx, which
	// the kernel cancels the instant the FUSE read returns (prefetch outlives it).
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{} // admission token per prefetch unit (bounds concurrency)
	wg     sync.WaitGroup

	mu       sync.Mutex
	track    map[meta.Ino]*raState
	inflight map[uint64]struct{} // slice ids currently being prefetched (dedup)
}

// raState is one inode's sequential-access tracking.
type raState struct {
	lastEnd    int64 // end offset of the previous read (contiguity test)
	runBytes   int64 // length of the current uninterrupted sequential run
	prefetchTo int64 // file offset the prefetch frontier has already reached
}

// newReadAhead builds a prefetcher for f keeping window bytes warm ahead of a
// sequential read cursor, with at most concurrency in-flight backing fetches
// (≤0 uses defaultReadaheadConcurrency). metrics may be nil.
func newReadAhead(f *Files, window int64, concurrency int, m *metrics.Registry) *readAhead {
	if concurrency <= 0 {
		concurrency = defaultReadaheadConcurrency
	}
	ctx, cancel := context.WithCancel(context.Background())
	ra := &readAhead{
		window:   window,
		trigger:  defaultReadaheadTrigger,
		files:    f,
		metrics:  m,
		ctx:      ctx,
		cancel:   cancel,
		sem:      make(chan struct{}, concurrency),
		track:    make(map[meta.Ino]*raState),
		inflight: make(map[uint64]struct{}),
	}
	if c, ok := f.chunks.(localCacheChecker); ok {
		ra.checker = c
	}
	return ra
}

// close stops the prefetcher and waits for in-flight prefetches to drain.
func (ra *readAhead) close() {
	ra.cancel()
	ra.wg.Wait()
}

// note updates inode ino's sequential-access state after a demand read of
// [offset, readEnd) and, when the stream looks sequential and has run past the
// trigger, schedules prefetch of the next window so it is warm before the
// application reaches it. The locked sections are tiny; the actual prefetch
// (ReadSlices + backing GETs) runs on the bounded background pool.
func (ra *readAhead) note(ino meta.Ino, offset, readEnd, fileLen int64) {
	if ra.window <= 0 || readEnd >= fileLen {
		return // disabled, or nothing left of the file to prefetch
	}

	ra.mu.Lock()
	st := ra.track[ino]
	if st == nil {
		if len(ra.track) >= maxReadaheadTrackers {
			for k := range ra.track { // evict one arbitrary entry (bounded map)
				delete(ra.track, k)
				break
			}
		}
		st = &raState{}
		ra.track[ino] = st
	}
	seq := offset == st.lastEnd // contiguous with the previous read on this inode
	if seq {
		st.runBytes += readEnd - offset
	} else {
		st.runBytes = readEnd - offset
		st.prefetchTo = readEnd // a seek invalidates the old frontier
	}
	st.lastEnd = readEnd

	var lo, hi int64
	schedule := false
	if seq && st.runBytes >= ra.trigger {
		want := readEnd + ra.window
		if want > fileLen {
			want = fileLen
		}
		lo = st.prefetchTo
		if lo < readEnd {
			lo = readEnd
		}
		if want > lo {
			hi, schedule = want, true
			st.prefetchTo = want // advance the frontier; dropped slices stay best-effort
		}
	}
	ra.mu.Unlock()

	if !schedule {
		return
	}
	// Resolve + dispatch on a short-lived goroutine so the demand read never waits
	// on the prefetch's metadata queries or backing GETs.
	ra.wg.Add(1)
	go ra.run(ino, lo, hi)
}

// run resolves the slices covering [lo,hi) and warms each not-already-resident
// one into the cache via a bounded pool of CONCURRENT backing fetches — the key
// to getting ahead of a flat-out sequential reader: a single window's slices are
// pulled in parallel (≤ readaheadConcurrency at a time), not one blocking GET
// after another. Best-effort throughout: a saturated pool drops the slice (the
// demand read fetches it), and any error — cancelled ctx, PG, backing — is
// dropped silently. This goroutine returns once every slice is dispatched or
// dropped; the spawned fetches finish under the same WaitGroup so close() drains
// them.
func (ra *readAhead) run(ino meta.Ino, lo, hi int64) {
	defer ra.wg.Done()

	ids := ra.sliceIDs(ino, lo, hi)

	// Pack-coalescing path: when the store can batch-read, fetch the whole window
	// in ONE call so slices sharing a casstore pack cost one pack GET instead of
	// one per slice (the cold model-load fix). Concurrency (in-flight pack GETs)
	// is bounded inside ReadBatch by cap(ra.sem), preserving -readahead-concurrency.
	if br, ok := ra.files.chunks.(chunkstore.BatchReader); ok {
		ra.runBatch(br, ids)
		return
	}

	for _, id := range ids {
		select {
		case <-ra.ctx.Done():
			return
		default:
		}
		if !ra.claim(id) {
			continue // already resident locally or another fetch holds it
		}
		// Non-blocking admission per slice: when the pool is full, drop this one
		// (demand will fetch it) rather than block the dispatcher — this is what
		// bounds total in-flight prefetch GETs and prevents pile-up on a slow backing.
		select {
		case ra.sem <- struct{}{}:
		default:
			ra.release(id)
			return
		}
		ra.wg.Add(1)
		go func(id uint64) {
			defer ra.wg.Done()
			defer func() { <-ra.sem }()
			defer ra.release(id)
			data, err := ra.files.chunks.Read(ra.ctx, id) // populates the disk cache
			if err == nil {
				ra.metrics.Prefetch(int64(len(data)))
			}
		}(id)
	}
}

// runBatch prefetches a window via the backing's coalesced ReadBatch: it claims
// the not-already-resident ids (so it never re-fetches and dedups against other
// in-flight prefetches), issues ONE batch read whose internal concurrency is
// bounded by cap(ra.sem), and releases the claims. Best-effort: a failed/saturated
// batch drops those ids (the demand path re-fetches). ReadBatch populates the disk
// cache for every fetched slice, exactly like the per-slice Read path.
func (ra *readAhead) runBatch(br chunkstore.BatchReader, ids []uint64) {
	claimed := make([]uint64, 0, len(ids))
	for _, id := range ids {
		select {
		case <-ra.ctx.Done():
			for _, c := range claimed {
				ra.release(c)
			}
			return
		default:
		}
		if ra.claim(id) {
			claimed = append(claimed, id)
		}
	}
	if len(claimed) == 0 {
		return
	}
	defer func() {
		for _, id := range claimed {
			ra.release(id)
		}
	}()
	out, stats, err := br.ReadBatch(ra.ctx, claimed, cap(ra.sem))
	if err != nil {
		return // best-effort: demand path re-fetches
	}
	for _, data := range out {
		ra.metrics.Prefetch(int64(len(data))) // per-slice: warmed slices + bytes
	}
	ra.metrics.PrefetchPacks(stats.PackGets) // coalesced backing GETs for the batch
}

// sliceIDs returns the distinct non-hole slice ids covering [lo,hi), across
// however many chunks the range spans. It mirrors Read's per-chunk resolve but
// collects ids instead of copying bytes; on a metadata error it returns what it
// has so far (best-effort).
func (ra *readAhead) sliceIDs(ino meta.Ino, lo, hi int64) []uint64 {
	var ids []uint64
	seen := make(map[uint64]struct{})
	cur := lo
	for cur < hi {
		indx := uint32(cur / meta.ChunkSize)
		chunkBase := int64(indx) * meta.ChunkSize
		clo := uint32(cur - chunkBase)
		hiByte := chunkBase + meta.ChunkSize
		if hiByte > hi {
			hiByte = hi
		}
		chi := uint32(hiByte - chunkBase)
		slices, st := ra.files.meta.ReadSlices(ra.ctx, ino, indx)
		if st != 0 {
			return ids
		}
		for _, sg := range resolve(slices, clo, chi) {
			if sg.id == 0 {
				continue // hole
			}
			if _, dup := seen[sg.id]; dup {
				continue
			}
			seen[sg.id] = struct{}{}
			ids = append(ids, sg.id)
		}
		cur = hiByte
	}
	return ids
}

// claim reserves slice id for prefetch, returning false if it is already
// resident locally or another unit already holds it. The local-presence check
// (when the store supports it) avoids re-fetching cached slices — and avoids an
// Exists() that would hit S3 on the -remote path.
func (ra *readAhead) claim(id uint64) bool {
	if ra.checker != nil && ra.checker.CachedLocally(id) {
		return false
	}
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if _, busy := ra.inflight[id]; busy {
		return false
	}
	ra.inflight[id] = struct{}{}
	return true
}

func (ra *readAhead) release(id uint64) {
	ra.mu.Lock()
	delete(ra.inflight, id)
	ra.mu.Unlock()
}
