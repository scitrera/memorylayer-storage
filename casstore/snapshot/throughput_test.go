// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// Direct-casstore read throughput.
//
// # Why
//
// The only throughput number we have for the platform is 43 MiB/s, and that is a
// *FUSE* number (mlfs sequential read, TECH_DEBT #44). It says nothing about what
// casstore's own read path can do, because mlfs pays FUSE round trips, Postgres
// metadata lookups and a disk-cache hop that a direct library consumer does not.
// A snapshot-restore tier calling casstore in-process is a completely different
// path and has never been measured. This closes that.
//
// # What this measures, and what it does not
//
// The backend here is the LOCAL filesystem and the data is likely in page cache,
// so these numbers are the SOFTWARE ceiling of casstore's reassembly path — CPU,
// allocation, hashing and copying — not a measurement of disk or network. That is
// deliberate and it is the right first question: if the code path cannot reach
// GB/s when the bytes are already in RAM, no storage backend will rescue it. Disk
// and object-store behaviour are a separate axis measured separately.
//
// Note the reassembly path verifies the SHA-256 of every chunk on read
// (sliceRunFromContent), so these numbers include full integrity checking. That
// is a real cost and it is one of the things worth knowing.
//
// # Running
//
//	CASSTORE_THROUGHPUT_MB=2048 go test ./snapshot -run TestReadThroughput -v
//
// Skipped unless the variable is set, so it never slows the normal suite.

// TestReadThroughput reports MB/s for the sequential reader and for a parallel
// pack-fetching reader that prototypes Materializer.Fill.
func TestReadThroughput(t *testing.T) {
	mbStr := os.Getenv("CASSTORE_THROUGHPUT_MB")
	if mbStr == "" {
		t.Skip("set CASSTORE_THROUGHPUT_MB to run the throughput report")
	}
	mb, err := strconv.Atoi(mbStr)
	if err != nil || mb <= 0 {
		t.Fatalf("CASSTORE_THROUGHPUT_MB must be a positive integer, got %q", mbStr)
	}
	ctx := context.Background()
	size := int64(mb) << 20

	// Incompressible, like real tensor/GPU-memory payloads. Compression is off so
	// the measurement is of reassembly, not of zstd.
	payload := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	if _, err := rng.Read(payload); err != nil {
		t.Fatal(err)
	}

	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })

	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		DedupDomain:       "bench",
		Index:             NewGlobalIndex(NewMemoryDedupStore(), nil),
		CompressionPolicy: NewContentTypePolicy(CompressNone),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := SnapshotKey{Tenant: "bench", OwnerKey: "obj"}
	start := time.Now()
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	writeRate := rate(size, time.Since(start))

	m, ok, err := cs.latestManifest(ctx, key)
	if err != nil || !ok {
		t.Fatalf("latestManifest: ok=%v err=%v", ok, err)
	}
	packs := distinctPacks(m)

	t.Logf("payload %d MiB → %d chunks in %d packs; write %s",
		mb, len(m.Chunks), len(packs), writeRate)

	// --- 1. sequential reader (what exists today) ---------------------------
	start = time.Now()
	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	n, err := io.Copy(io.Discard, rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("sequential read: %v", err)
	}
	if n != size {
		t.Fatalf("sequential read returned %d bytes, want %d", n, size)
	}
	seqRate := rate(size, time.Since(start))

	// --- 2. parallel pack fetch (prototype of Materializer.Fill) ------------
	// Group chunks by pack, fetch packs concurrently, slice each chunk into its
	// place in one preallocated destination buffer. This is the shape a snapshot
	// restore would use: the destination would be pinned/hugepage memory.
	results := make([]string, 0, 4)
	for _, workers := range concurrencyLevels() {
		dst := make([]byte, size)
		start = time.Now()
		if err := fillParallel(ctx, chunks, m, dst, workers); err != nil {
			t.Fatalf("parallel fill (workers=%d): %v", workers, err)
		}
		d := time.Since(start)
		if !bytes.Equal(dst, payload) {
			t.Fatalf("parallel fill (workers=%d) produced wrong bytes", workers)
		}
		results = append(results, fmt.Sprintf("  parallel/%-3d %s", workers, rate(size, d)))
	}

	// --- 3. control: raw concurrent blob reads, no casstore processing -------
	// Same packs, same concurrency, but no decompress / slice / sha256 / copy.
	// The gap between this and parallel/N is casstore's CPU cost, which is the
	// number that says whether reassembly or IO is the limit.
	maxW := concurrencyLevels()[len(concurrencyLevels())-1]
	start = time.Now()
	if err := rawFetchPacks(ctx, chunks, m, packs, maxW); err != nil {
		t.Fatalf("raw fetch: %v", err)
	}
	rawRate := rate(size, time.Since(start))

	t.Logf("read throughput (local FS backend, likely page-cached; sha256 verified):")
	t.Logf("  sequential   %s", seqRate)
	for _, r := range results {
		t.Log(r)
	}
	t.Logf("  raw/%-3d      %s   <- IO ceiling, no decompress/verify/copy", maxW, rawRate)
}

// rawFetchPacks reads every pack concurrently and discards it: the IO ceiling for
// this backend at this concurrency, with none of casstore's per-chunk work.
func rawFetchPacks(ctx context.Context, store blobstore.Storage, m chunkManifest, packs []string, workers int) error {
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, ph := range packs {
		wg.Add(1)
		go func(ph string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			buf := blobstore.NewOutputBuffer()
			if err := store.GetBlob(ctx, packBlobID(m.Tenant, ph), 0, -1, buf); err != nil {
				recordErr(&mu, &firstErr, err)
			}
		}(ph)
	}
	wg.Wait()
	return firstErr
}

// benchWorkers is the fetch concurrency for the pack-size sweep. It defaults to
// NumCPU but is normally set lower: on a GPU node the cores belong to inference,
// so a realistic storage budget is a handful of workers, not the whole machine.
func benchWorkers() int {
	if v := os.Getenv("CASSTORE_THROUGHPUT_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// concurrencyLevels returns the worker counts to sweep, bounded by the machine.
func concurrencyLevels() []int {
	max := runtime.NumCPU()
	levels := []int{1, 4, 16}
	if max > 16 {
		levels = append(levels, max)
	}
	return levels
}

// fillParallel is the prototype materializer: it satisfies the whole object into
// dst using `workers` concurrent pack fetches. Every chunk's content hash is
// verified by sliceRunFromContent, exactly as the sequential path does, so the
// two are comparable.
func fillParallel(ctx context.Context, store blobstore.Storage, m chunkManifest, dst []byte, workers int) error {
	type packJob struct {
		packHash string
		refs     []chunkRef // manifest refs in this pack
		dstOffs  []int64    // where each ref's bytes belong in dst
	}

	// Build the destination offset for every chunk, then group by pack.
	byPack := map[string]*packJob{}
	var off int64
	for _, ref := range m.Chunks {
		j := byPack[ref.PackHash]
		if j == nil {
			j = &packJob{packHash: ref.PackHash}
			byPack[ref.PackHash] = j
		}
		j.refs = append(j.refs, ref)
		j.dstOffs = append(j.dstOffs, off)
		off += int64(ref.Size)
	}
	if off != int64(len(dst)) {
		return fmt.Errorf("manifest covers %d bytes, dst is %d", off, len(dst))
	}

	jobs := make([]*packJob, 0, len(byPack))
	for _, j := range byPack {
		// Slicing a pack requires its refs in ascending pack offset.
		idx := make([]int, len(j.refs))
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool { return j.refs[idx[a]].Offset < j.refs[idx[b]].Offset })
		refs := make([]chunkRef, len(idx))
		offs := make([]int64, len(idx))
		for i, ix := range idx {
			refs[i], offs[i] = j.refs[ix], j.dstOffs[ix]
		}
		j.refs, j.dstOffs = refs, offs
		jobs = append(jobs, j)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, j := range jobs {
		wg.Add(1)
		go func(j *packJob) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pid := packBlobID(m.Tenant, j.packHash)
			buf := blobstore.NewOutputBuffer()
			if err := store.GetBlob(ctx, pid, 0, -1, buf); err != nil {
				recordErr(&mu, &firstErr, fmt.Errorf("fetch pack %s: %w", pid, err))
				return
			}
			content, err := decompressPackBlob(buf.Bytes())
			if err != nil {
				recordErr(&mu, &firstErr, err)
				return
			}
			// Same verification the sequential reader performs.
			out, err := sliceRunFromContent(content, pid, j.refs)
			if err != nil {
				recordErr(&mu, &firstErr, err)
				return
			}
			for i, chunk := range out {
				copy(dst[j.dstOffs[i]:], chunk)
			}
		}(j)
	}
	wg.Wait()
	return firstErr
}

func recordErr(mu *sync.Mutex, dst *error, err error) {
	mu.Lock()
	if *dst == nil {
		*dst = err
	}
	mu.Unlock()
}

func distinctPacks(m chunkManifest) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, ref := range m.Chunks {
		if ref.PackHash == "" {
			continue
		}
		if _, dup := seen[ref.PackHash]; dup {
			continue
		}
		seen[ref.PackHash] = struct{}{}
		out = append(out, ref.PackHash)
	}
	return out
}

func rate(bytes int64, d time.Duration) string {
	mbps := float64(bytes) / (1 << 20) / d.Seconds()
	return fmt.Sprintf("%8.1f MiB/s (%.2f GiB/s, %s)", mbps, mbps/1024, d.Round(time.Millisecond))
}

// TestReadThroughputPackSize sweeps the casstore pack target at fixed max
// concurrency, to answer whether raising pack size (as a GPU-workload deployment
// does via -pack-size) actually raises restore throughput.
//
// The two effects pull in OPPOSITE directions and which one wins depends on the
// backend:
//
//   - Fewer, larger requests amortise per-request latency. This is the win on an
//     object store, where every GET carries tens of milliseconds of overhead.
//   - Fewer packs means fewer units of parallelism to distribute across workers,
//     and a longer tail when the count stops dividing evenly. This is the loss on
//     a local backend, where per-request overhead is already negligible.
//
// This test measures a LOCAL, page-cached backend, so it isolates the second
// effect. It is NOT a prediction for S3 — see the caveat in the design doc.
func TestReadThroughputPackSize(t *testing.T) {
	mbStr := os.Getenv("CASSTORE_THROUGHPUT_MB")
	if mbStr == "" {
		t.Skip("set CASSTORE_THROUGHPUT_MB to run the pack-size sweep")
	}
	mb, err := strconv.Atoi(mbStr)
	if err != nil || mb <= 0 {
		t.Fatalf("CASSTORE_THROUGHPUT_MB must be a positive integer, got %q", mbStr)
	}
	ctx := context.Background()
	size := int64(mb) << 20

	payload := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	if _, err := rng.Read(payload); err != nil {
		t.Fatal(err)
	}
	workers := benchWorkers()

	t.Logf("pack-size sweep at %d workers, %d MiB payload (local FS, page-cached):", workers, mb)
	for _, packMB := range []int{4, 16, 64, 128} {
		upstream, err := NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
			PackTargetBytes:   packMB << 20,
			DedupDomain:       "bench",
			Index:             NewGlobalIndex(NewMemoryDedupStore(), nil),
			CompressionPolicy: NewContentTypePolicy(CompressNone),
		})
		if err != nil {
			t.Fatal(err)
		}
		key := SnapshotKey{Tenant: "bench", OwnerKey: "obj"}
		if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put (pack=%dMiB): %v", packMB, err)
		}
		m, ok, err := cs.latestManifest(ctx, key)
		if err != nil || !ok {
			t.Fatalf("latestManifest: ok=%v err=%v", ok, err)
		}
		nPacks := len(distinctPacks(m))

		dst := make([]byte, size)
		start := time.Now()
		if err := fillParallel(ctx, chunks, m, dst, workers); err != nil {
			t.Fatalf("fill (pack=%dMiB): %v", packMB, err)
		}
		d := time.Since(start)
		if !bytes.Equal(dst, payload) {
			t.Fatalf("pack=%dMiB produced wrong bytes", packMB)
		}
		// Peak buffer memory: the prototype fetches whole packs, so in-flight
		// bytes scale with packSize × workers. That is a real deployment cost.
		inflight := int64(packMB<<20) * int64(workers)
		t.Logf("  pack=%3dMiB  %3d packs (%.1f/worker)  %s  in-flight≈%s",
			packMB, nPacks, float64(nPacks)/float64(workers), rate(size, d), humanBytes(inflight))
		_ = chunks.Close(ctx)
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
