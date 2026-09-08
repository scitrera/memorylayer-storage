// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// slowStore wraps a backing chunk store and injects a per-Read delay, modeling a
// high-latency object-store GET (the case readahead exists to hide). It also
// counts backing Reads so a test can see prefetch fetching ahead of demand.
type slowStore struct {
	chunkstore.Store
	delay time.Duration
	reads atomic.Int64
}

func (s *slowStore) Read(ctx context.Context, id uint64) ([]byte, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.reads.Add(1)
	return s.Store.Read(ctx, id)
}

// readaheadStack is a data path whose chunk store is a DiskCache over a
// latency-injecting backing, so reads that miss the local cache pay `delay`.
type readaheadStack struct {
	f     *fileio.Files
	e     *meta.Engine
	dc    *cache.DiskCache
	slow  *slowStore
	local *chunkstore.Local
}

// newReadaheadStack writes the file's slices through a warm cache, then returns a
// COLD cache (fresh dir) over the same backing so subsequent reads miss locally
// and fetch from the latency-injecting backing — the cold-read scenario.
func newReadaheadStack(t *testing.T, delay time.Duration, window int64, fileData [][]byte) (*readaheadStack, meta.Ino) {
	t.Helper()
	ctx := context.Background()
	e := meta.Open(testpg.DB(t), 7)
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	local, err := chunkstore.NewLocal(t.TempDir(), domain, 0)
	if err != nil {
		t.Fatalf("chunkstore: %v", err)
	}
	t.Cleanup(func() { _ = local.Chunks.Close(ctx) })

	ino, _, st := e.Create(ctx, meta.RootInode, "model", 0o644, meta.Background())
	if st != 0 {
		t.Fatalf("create: %v", st)
	}

	// Warm-write each 1 MiB region as its own slice through a throwaway cache, then
	// flush it to the backing so the cold read cache below must fetch from backing.
	warm, err := cache.New(local.Store, cache.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	wf := fileio.New(e, warm)
	off := int64(0)
	for _, region := range fileData {
		if _, err := wf.Write(ctx, ino, off, region, chunkstore.ClassDefault); err != nil {
			t.Fatalf("write @%d: %v", off, err)
		}
		off += int64(len(region))
	}
	if err := warm.Flush(ctx); err != nil { // push staged slices to the backing
		t.Fatalf("flush warm: %v", err)
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("close warm: %v", err)
	}

	// Cold read path: fresh cache dir (no clean entries) over the latency backing.
	slow := &slowStore{Store: local.Store, delay: delay}
	dc, err := cache.New(slow, cache.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("cold cache: %v", err)
	}
	t.Cleanup(func() { _ = dc.Close() })
	f := fileio.New(e, dc)
	if window > 0 {
		f.EnableReadahead(window, 0, nil) // 0 → default concurrency
	}
	t.Cleanup(func() { _ = f.Close() })
	return &readaheadStack{f: f, e: e, dc: dc, slow: slow, local: local}, ino
}

// sliceIDByPos maps each slice's start offset in chunk 0 to its id, so a test can
// assert which slices prefetch warmed.
func (s *readaheadStack) sliceIDByPos(t *testing.T, ino meta.Ino) map[int64]uint64 {
	t.Helper()
	refs, st := s.e.ReadSlices(context.Background(), ino, 0)
	if st != 0 {
		t.Fatalf("read slices: %v", st)
	}
	out := make(map[int64]uint64, len(refs))
	for _, r := range refs {
		out[int64(r.Pos)] = r.Id
	}
	return out
}

// TestReadaheadWarmsWindowAhead verifies that a sequential read warms the next
// window into the cache WITHOUT the demand read having touched those slices, and
// that it stops at the window boundary (does not prefetch the whole file).
func TestReadaheadWarmsWindowAhead(t *testing.T) {
	const mib = 1 << 20
	regions := make([][]byte, 8) // 8 × 1 MiB = 8 distinct slices in chunk 0
	for i := range regions {
		regions[i] = randomBytes(t, mib)
	}
	// 4 MiB window: a 3-read sequential prefix (0..3 MiB) should warm up to ~7 MiB.
	s, ino := newReadaheadStack(t, 0, 4*mib, regions)
	ids := s.sliceIDByPos(t, ino)
	ctx := context.Background()

	buf := make([]byte, mib)
	for i := int64(0); i < 3; i++ { // demand-read only the first 3 MiB
		if _, err := s.f.Read(ctx, ino, i*mib, buf); err != nil {
			t.Fatalf("read @%d MiB: %v", i, err)
		}
	}

	// Slices 3..6 MiB are never demand-read here, so their local residency can only
	// come from prefetch. Wait for the async prefetch to settle.
	for _, posMiB := range []int64{3, 4, 5, 6} {
		id := ids[posMiB*mib]
		if !waitCached(s.dc, id, 2*time.Second) {
			t.Fatalf("slice at %d MiB (id %d) was not prefetched into the cache", posMiB, id)
		}
	}
	// The slice at 7 MiB is past the prefetch frontier (window stops at ~7 MiB) and
	// was never demand-read, so it must NOT be resident — proving the window bounds
	// prefetch rather than greedily pulling the whole file.
	if s.dc.CachedLocally(ids[7*mib]) {
		t.Fatalf("slice at 7 MiB was prefetched but is beyond the readahead window")
	}
}

// TestReadaheadHidesBackingLatency proves the point of the feature: a sequential
// read over a high-latency backing completes materially faster with readahead on,
// because upcoming slices are fetched concurrently ahead of demand instead of one
// blocking GET at a time.
func TestReadaheadHidesBackingLatency(t *testing.T) {
	const mib = 1 << 20
	const delay = 20 * time.Millisecond
	regions := make([][]byte, 8)
	for i := range regions {
		regions[i] = randomBytes(t, mib)
	}

	seqRead := func(window int64) time.Duration {
		s, ino := newReadaheadStack(t, delay, window, regions)
		ctx := context.Background()
		buf := make([]byte, mib)
		start := time.Now()
		for i := int64(0); i < int64(len(regions)); i++ {
			if _, err := s.f.Read(ctx, ino, i*mib, buf); err != nil {
				t.Fatalf("read @%d MiB: %v", i, err)
			}
		}
		return time.Since(start)
	}

	off := seqRead(0)      // readahead disabled: ~8 serial GETs
	on := seqRead(8 * mib) // readahead on: GETs overlap ahead of demand

	t.Logf("sequential read of %d MiB @ %v/slice: readahead off=%v on=%v", len(regions), delay, off, on)
	// Off pays ~8×delay serially (~160 ms). On overlaps the GETs, so it should be
	// well under that. A loose bound keeps the assertion robust under CI load.
	if on >= off {
		t.Fatalf("readahead did not speed up the cold sequential read: off=%v on=%v", off, on)
	}
	if on > off*3/4 {
		t.Fatalf("readahead speedup too small to be the prefetch effect: off=%v on=%v", off, on)
	}
}

// waitCached polls CachedLocally until the slice is resident or the deadline
// passes (prefetch is asynchronous).
func waitCached(dc *cache.DiskCache, id uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dc.CachedLocally(id) {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return dc.CachedLocally(id)
}
