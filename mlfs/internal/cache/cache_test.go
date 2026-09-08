// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// memBatchStore is a memStore that also implements chunkstore.BatchWriter, so
// the cache routes drains through the batched path.
type memBatchStore struct {
	*memStore
	batchCalls atomic.Int32
}

func newMemBatchStore() *memBatchStore { return &memBatchStore{memStore: newMemStore()} }

func (m *memBatchStore) WriteBatch(ctx context.Context, items []chunkstore.BatchItem) error {
	m.batchCalls.Add(1)
	for _, it := range items {
		if err := m.memStore.Write(ctx, it.ID, it.Data, it.Class); err != nil {
			return err
		}
	}
	return nil
}

// memStore is an in-memory chunkstore.Store with write/read counters, so tests
// can assert when (and whether) data reaches the backing store.
func newMemStore() *memStore { return &memStore{m: map[uint64][]byte{}} }

// Optional instrumentation. writeDelay holds each Write for a beat so that
// concurrent drains overlap inside the backing store; inFlight/maxInFlight track
// how many Writes run at once; writeErr (if set) makes every Write fail.
type memStore struct {
	mu     sync.Mutex
	m      map[uint64][]byte
	writes int
	reads  int

	writeDelay  time.Duration
	writeErr    error
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (s *memStore) Write(_ context.Context, id uint64, data []byte, _ chunkstore.StoreClass) error {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		cur := s.maxInFlight.Load()
		if n <= cur || s.maxInFlight.CompareAndSwap(cur, n) {
			break
		}
	}
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	s.m[id] = append([]byte(nil), data...)
	s.writes++
	return nil
}

func (s *memStore) Read(_ context.Context, id uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.m[id]
	s.reads++
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), d...), nil
}

func (s *memStore) ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error) {
	d, err := s.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(d)) {
		return 0, nil
	}
	return copy(p, d[off:]), nil
}

func (s *memStore) Exists(_ context.Context, id uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[id]
	return ok, nil
}

func (s *memStore) Remove(_ context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
	return nil
}

func (s *memStore) counts() (w, r int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes, s.reads
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestWriteFlushRead(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	data := randomBytes(t, 256*1024)
	if err := c.Write(ctx, 7, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if ok, _ := back.Exists(ctx, 7); !ok {
		t.Fatal("data should have reached the backing store after Flush")
	}
	got, err := c.Read(ctx, 7)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Read mismatch: err=%v equal=%v", err, bytes.Equal(got, data))
	}
}

// TestCachedLocally checks the local-presence probe the readahead prefetcher
// relies on: it reports true ONLY when a slice is resident in the local cache or
// staging, and never triggers a backing fetch (unlike Exists, which falls
// through to the backing store on a local miss).
func TestCachedLocally(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	// Seed a slice that exists ONLY in the backing store.
	if err := back.Write(ctx, 99, randomBytes(t, 4096), chunkstore.ClassDefault); err != nil {
		t.Fatalf("seed backing: %v", err)
	}
	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// Backing-only slice: not local, and probing it must not read the backing.
	if c.CachedLocally(99) {
		t.Fatal("slice 99 lives only in the backing store; CachedLocally must be false")
	}
	if _, r := back.counts(); r != 0 {
		t.Fatalf("CachedLocally hit the backing store (%d reads); it must be local-only", r)
	}

	// A demand Read populates the clean cache → now locally resident.
	if _, err := c.Read(ctx, 99); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !c.CachedLocally(99) {
		t.Fatal("after a demand Read populated the cache, CachedLocally must be true")
	}
}

// TestCrashReplay is the acceptance: a write is acknowledged (fsync'd to
// staging) but NOT yet uploaded; the process "crashes"; on restart the cache
// recovers the staged write and replays it to the backing store with no loss.
func TestCrashReplay(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	back := newMemStore()

	// First cache: stage a write but never upload (AutoUpload off, no Flush).
	c1, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New c1: %v", err)
	}
	data := randomBytes(t, 512*1024)
	if err := c1.Write(ctx, 99, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Acknowledged, but not uploaded.
	if w, _ := back.counts(); w != 0 {
		t.Fatalf("backing should have 0 writes before recovery, got %d", w)
	}
	if ok, _ := back.Exists(ctx, 99); ok {
		t.Fatal("backing should not have the data yet")
	}
	// The staging file is durably present.
	if _, err := os.Stat(filepath.Join(dir, "staging", "99")); err != nil {
		t.Fatalf("staging file missing: %v", err)
	}
	// "crash": abandon c1 without Close/Flush.

	// Restart: a fresh cache over the same dir + backing replays staging.
	c2, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New c2 (recover): %v", err)
	}
	defer c2.Close()
	if ok, _ := back.Exists(ctx, 99); !ok {
		t.Fatal("staged write should have replayed to the backing store on restart")
	}
	got, err := c2.Read(ctx, 99)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("recovered data mismatch: err=%v equal=%v", err, bytes.Equal(got, data))
	}
}

// TestStagingDirSplit verifies the durable write-back (staging) can live on a
// SEPARATE directory from the clean cache, and that an acked write staged there
// still survives a crash + restart — the durability guarantee that lets the
// clean cache go on ephemeral storage.
func TestStagingDirSplit(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()   // clean cache (could be ephemeral)
	stagingDir := t.TempDir() // durable write-back (persistent)
	back := newMemStore()

	cfg := cache.Config{Dir: cacheDir, StagingDir: stagingDir, AutoUpload: false}
	c1, err := cache.New(back, cfg)
	if err != nil {
		t.Fatalf("New c1: %v", err)
	}
	data := randomBytes(t, 256*1024)
	if err := c1.Write(ctx, 7, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The durable copy lives under StagingDir, NOT under the clean-cache Dir.
	if _, err := os.Stat(filepath.Join(stagingDir, "staging", "7")); err != nil {
		t.Fatalf("staging file not under StagingDir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "staging", "7")); err == nil {
		t.Fatal("staging file leaked under the clean-cache Dir; should be under StagingDir")
	}
	if w, _ := back.counts(); w != 0 {
		t.Fatalf("backing should have 0 writes before recovery, got %d", w)
	}

	// "crash": abandon c1. A fresh cache over the SAME split dirs must replay the
	// staged write — durability is preserved across the split.
	c2, err := cache.New(back, cfg)
	if err != nil {
		t.Fatalf("New c2 (recover): %v", err)
	}
	defer c2.Close()
	if ok, _ := back.Exists(ctx, 7); !ok {
		t.Fatal("staged write under StagingDir should replay on restart")
	}
	got, err := c2.Read(ctx, 7)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("recovered data mismatch: err=%v equal=%v", err, bytes.Equal(got, data))
	}
}

func TestChecksumResilience(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	back := newMemStore()
	c, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	data := randomBytes(t, 64*1024)
	c.Write(ctx, 5, data, chunkstore.ClassDefault)
	c.Flush(ctx)
	// Read once to populate the clean cache.
	if _, err := c.Read(ctx, 5); err != nil {
		t.Fatalf("Read populate: %v", err)
	}
	// Corrupt the clean-cache file.
	cachePath := filepath.Join(dir, "cache", "5")
	corrupt, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	corrupt[len(corrupt)-1] ^= 0xFF
	if err := os.WriteFile(cachePath, corrupt, 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	// Read must detect corruption, drop the cache copy, and re-fetch correctly.
	got, err := c.Read(ctx, 5)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("corruption not handled: err=%v equal=%v", err, bytes.Equal(got, data))
	}
}

func TestLRUEviction(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: true, MaxItems: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	payloads := map[uint64][]byte{}
	for _, id := range []uint64{1, 2, 3} {
		d := randomBytes(t, 32*1024)
		payloads[id] = d
		if err := c.Write(ctx, id, d, chunkstore.ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
		if err := c.Flush(ctx); err != nil {
			t.Fatalf("Flush %d: %v", id, err)
		}
		// Read to ensure it is promoted into the clean LRU cache.
		if _, err := c.Read(ctx, id); err != nil {
			t.Fatalf("Read %d: %v", id, err)
		}
	}
	// With MaxItems=2, at most 2 clean cache files should remain on disk.
	// (Eviction deletes the local copy; the data is safe in the backing store.)
	// Every id must still read back correctly (evicted ones via the backing).
	_, readsBefore := back.counts()
	for id, want := range payloads {
		got, err := c.Read(ctx, id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Read %d after eviction: err=%v equal=%v", id, err, bytes.Equal(got, want))
		}
	}
	_, readsAfter := back.counts()
	if readsAfter <= readsBefore {
		t.Errorf("expected at least one backing read for an evicted entry (%d → %d)", readsBefore, readsAfter)
	}
}

// TestCorruptStagingQuarantined is the #2 regression: a staging entry that fails
// its checksum on drain must NOT be silently dropped (the Write was already
// acked durable). It is quarantined (bytes preserved), OnCorrupt fires, and it
// never reaches the backing store.
func TestCorruptStagingQuarantined(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	back := newMemStore()

	var gotID uint64
	var gotPath string
	c, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: false,
		OnCorrupt: func(id uint64, path string, cause error) { gotID, gotPath = id, path }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := c.Write(ctx, 42, randomBytes(t, 64*1024), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Corrupt the staged bytes on disk before they upload.
	sp := filepath.Join(dir, "staging", "42")
	raw, err := os.ReadFile(sp)
	if err != nil {
		t.Fatalf("read staging: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(sp, raw, 0o644); err != nil {
		t.Fatalf("corrupt staging: %v", err)
	}

	// Handler is set, so Flush swallows the alert (returns nil) but must quarantine.
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush with handler set should not error: %v", err)
	}
	if gotID != 42 {
		t.Fatalf("OnCorrupt not invoked for id 42 (got %d)", gotID)
	}
	if _, err := os.Stat(sp); !os.IsNotExist(err) {
		t.Errorf("staging entry should be moved out of staging, stat err=%v", err)
	}
	qb, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("quarantined bytes missing at %s: %v", gotPath, err)
	}
	if !bytes.Equal(qb, raw) {
		t.Error("quarantined file should preserve the exact (corrupt) staging bytes for recovery")
	}
	if ok, _ := back.Exists(ctx, 42); ok {
		t.Error("a corrupt entry must never reach the backing store")
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestCorruptStagingSurfacedWithoutHandler: with no OnCorrupt handler, Flush
// returns ErrStagingCorrupt rather than silently succeeding.
func TestCorruptStagingSurfacedWithoutHandler(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := cache.New(newMemStore(), cache.Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Write(ctx, 5, randomBytes(t, 8192), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sp := filepath.Join(dir, "staging", "5")
	raw, _ := os.ReadFile(sp)
	raw[len(raw)-1] ^= 0xFF
	_ = os.WriteFile(sp, raw, 0o644)

	if err := c.Flush(ctx); !errors.Is(err, cache.ErrStagingCorrupt) {
		t.Fatalf("want ErrStagingCorrupt, got %v", err)
	}
	_ = c.Close()
}

// TestCorruptStagingRecoveryDoesNotBlockMount: a corrupt staging file left by a
// crash must be quarantined on restart without failing cache open.
func TestCorruptStagingRecoveryDoesNotBlockMount(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	back := newMemStore()

	c1, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New c1: %v", err)
	}
	if err := c1.Write(ctx, 7, randomBytes(t, 4096), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// "crash": corrupt the staging file, never Close/Flush c1.
	sp := filepath.Join(dir, "staging", "7")
	raw, _ := os.ReadFile(sp)
	raw[0] ^= 0xFF
	_ = os.WriteFile(sp, raw, 0o644)

	// Restart with NO handler: recovery must tolerate the corruption (mount
	// must not be blocked) yet still quarantine the bytes.
	c2, err := cache.New(back, cache.Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New must tolerate corrupt staging on recovery, got: %v", err)
	}
	defer c2.Close()
	if _, err := os.Stat(sp); !os.IsNotExist(err) {
		t.Errorf("staging entry should be quarantined on recovery, stat err=%v", err)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "corrupt"))
	if len(ents) == 0 {
		t.Error("expected the preserved bytes in corrupt/ after recovery")
	}
}

// TestWriteBurstDoesNotFanOutDrains is the H1 regression: a burst of N writes
// under AutoUpload must NOT spawn N concurrent drainStaging runs (the old code
// did `go drainStaging` per write, racing the staging dir). Now Write only sends
// a coalescing non-blocking nudge, so the single upload-loop goroutine drains.
//
// Each backing Write is held open (writeDelay) so any overlapping drains would
// be caught by maxInFlight. We let the loop drain autonomously via nudges (a
// long ticker keeps the periodic tick out of the way, and we deliberately do NOT
// call Flush, which is a legitimately-concurrent drain) and poll the backing
// store for completion. The single-drain design keeps maxInFlight at 1.
func TestWriteBurstDoesNotFanOutDrains(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	back.writeDelay = 5 * time.Millisecond
	c, err := cache.New(back, cache.Config{
		Dir:            t.TempDir(),
		AutoUpload:     true,
		UploadInterval: time.Hour, // park the ticker; drive drains purely by nudge
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	const n = 64
	for id := uint64(0); id < n; id++ {
		if err := c.Write(ctx, id, randomBytes(t, 4096), chunkstore.ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}
	// Writes must not block on uploads. The whole burst only stages (fsync) and
	// nudges; it does NOT wait for the slow backing Writes. So by the time the
	// burst returns, the loop cannot have uploaded all n entries yet (that would
	// cost ~n*writeDelay of upload time the writer never waited for). Seeing
	// fewer than n backing Writes here proves Write returned without draining.
	if w, _ := back.counts(); w >= n {
		t.Errorf("all %d backing Writes already done after the burst; Write appears to block on uploads", n)
	}

	// Wait for the nudge-driven loop to drain all entries to the backing store.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if w, _ := back.counts(); w >= n {
			break
		}
		if time.Now().After(deadline) {
			w, _ := back.counts()
			t.Fatalf("loop drained only %d/%d entries before deadline", w, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	for id := uint64(0); id < n; id++ {
		if ok, _ := back.Exists(ctx, id); !ok {
			t.Fatalf("id %d never reached the backing store", id)
		}
	}
	// The core assertion: drains never overlapped. With the old per-write
	// goroutine fan-out, many drains would race the same staging dir and the
	// backing Writes would overlap, pushing maxInFlight well above 1.
	if mx := back.maxInFlight.Load(); mx != 1 {
		t.Errorf("backing Write concurrency was %d; nudge-coalesced drains must run one at a time", mx)
	}
}

// TestUploadConcurrencyBounded confirms UploadConcurrency uploads staged slices
// in parallel WITHIN a drain (so the high-latency -remote path isn't round-trip
// bound) but never exceeds the configured bound (no unbounded fan-out).
func TestUploadConcurrencyBounded(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	back.writeDelay = 10 * time.Millisecond // hold each upload so overlap is observable
	c, err := cache.New(back, cache.Config{
		Dir:               t.TempDir(),
		AutoUpload:        false, // drive the drain deterministically via Flush
		UploadConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	const n = 16
	for id := uint64(0); id < n; id++ {
		if err := c.Write(ctx, id, randomBytes(t, 4096), chunkstore.ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if w, _ := back.counts(); w != n {
		t.Errorf("drained %d/%d entries", w, n)
	}
	mx := back.maxInFlight.Load()
	if mx < 2 {
		t.Errorf("upload concurrency not engaged: maxInFlight=%d, want ≥2", mx)
	}
	if mx > 4 {
		t.Errorf("upload concurrency exceeded the bound: maxInFlight=%d, want ≤4", mx)
	}
}

// TestBatchDrainSharesAndDurable verifies the BatchWriter drain path: writes are
// coalesced into a single batched upload (not one per slice), every slice reaches
// the backing, and reads back byte-identical through the cache.
func TestBatchDrainSharesAndDurable(t *testing.T) {
	ctx := context.Background()
	back := newMemBatchStore()
	c, err := cache.New(back, cache.Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	const n = 32
	payloads := make(map[uint64][]byte, n)
	for id := uint64(0); id < n; id++ {
		p := randomBytes(t, 4096)
		payloads[id] = p
		if err := c.Write(ctx, id, p, chunkstore.ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}
	if err := c.Flush(ctx); err != nil { // -> drainBatched -> one WriteBatch
		t.Fatalf("Flush: %v", err)
	}

	// All slices coalesced into a single batched upload (under the byte budget).
	if got := back.batchCalls.Load(); got != 1 {
		t.Errorf("WriteBatch called %d times; want 1 (all slices in one batch)", got)
	}
	if w, _ := back.counts(); w != n {
		t.Errorf("backing has %d slices, want %d", w, n)
	}
	// Read back every slice through the cache.
	for id := uint64(0); id < n; id++ {
		got, err := c.Read(ctx, id)
		if err != nil {
			t.Fatalf("Read %d: %v", id, err)
		}
		if !bytes.Equal(got, payloads[id]) {
			t.Errorf("slice %d read-back mismatch", id)
		}
	}
}

// TestBackgroundDrainErrorSurfaced is the H1 error-handling regression: a drain
// driven by the upload loop / write nudge has no caller to return to, so its
// error must reach Config.OnError (or the log) instead of being swallowed by the
// old `_ = drainStaging(...)`.
func TestBackgroundDrainErrorSurfaced(t *testing.T) {
	ctx := context.Background()
	back := newMemStore()
	back.writeErr = errors.New("backing unavailable")

	errCh := make(chan error, 16)
	c, err := cache.New(back, cache.Config{
		Dir:            t.TempDir(),
		AutoUpload:     true,
		UploadInterval: 5 * time.Millisecond,
		OnError:        func(e error) { errCh <- e },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// Stage a write; its background drain will fail on the backing Write.
	if err := c.Write(ctx, 1, randomBytes(t, 1024), chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case e := <-errCh:
		if e == nil || !errors.Is(e, back.writeErr) {
			t.Fatalf("OnError got %v; want wrapped %v", e, back.writeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background drain error was swallowed; OnError never fired")
	}
}
