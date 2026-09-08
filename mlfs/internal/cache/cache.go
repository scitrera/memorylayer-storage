// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package cache is mlfs's local disk cache and durable write-back staging in
// front of the casstore-backed chunk store.
//
// A Write
// lands in a staging file that is fsync'd (file AND directory) BEFORE the call
// returns, so an acknowledged write survives a kill -9. A background uploader
// drains staging into the backing chunk store; on restart, recover() re-uploads
// any staging entries left behind by a crash. Clean (already-uploaded) cache
// entries are LRU-evictable; staging entries are never evicted (they are the
// only durable copy until uploaded). Every cache/staging file embeds a sha256
// header that is verified on read, so corruption is detected, not served.
//
// DiskCache implements chunkstore.Store, so it is a drop-in decorator.
package cache

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fsstat"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

// Config tunes the cache.
type Config struct {
	Dir string // clean read-cache root (the reclaimable, evictable cache/ tree)
	// StagingDir, when set, places the DURABLE write-back data — staging/ (the
	// only copy of an acked-but-not-yet-uploaded write) and corrupt/ (quarantined
	// acked writes) — on a separate filesystem from Dir. Empty = under Dir (the
	// original layout). Splitting them lets Dir live on fast/ephemeral storage
	// (instance-store NVMe) without weakening the "an acked write survives node
	// termination" guarantee: point StagingDir (and the WAL) at a persistent disk.
	StagingDir string
	MaxBytes   int64 // clean-cache size cap (0 = unlimited)
	MaxItems   int   // clean-cache item cap (0 = unlimited)
	// MinFreeFraction evicts the clean cache to keep at least this fraction of the
	// cache *filesystem* free (0 = disabled; e.g. 0.15 = treat the cache as full
	// once free space ≤ 15% of the volume). It lets the LRU self-size to each
	// node's disk without a hard-coded byte cap, and composes with MaxBytes (the
	// cache is trimmed to satisfy whichever bound bites first). Only the clean
	// cache is reclaimable — staging (undurable writes) is never evicted — so the
	// floor is best-effort when staging + other on-disk usage already exceed it.
	MinFreeFraction float64
	AutoUpload      bool          // run the background uploader (off → drain only on recover/Flush)
	UploadInterval  time.Duration // background drain cadence (default 1s)
	// UploadConcurrency bounds how many uploads a single drain runs in parallel
	// (≤1 = serial). It applies to BOTH drain paths: the batched path (backings
	// implementing chunkstore.BatchWriter) flushes up to this many ~pack-sized
	// batches at once; the per-slice fallback uploads up to this many slices at
	// once. On the high-latency -remote path serial uploads are round-trip-bound,
	// so a bounded pool multiplies throughput. This is INTRA-drain concurrency
	// only — the upload loop is still a single nudge-coalesced goroutine.
	UploadConcurrency int

	// OnCorrupt is invoked when a staging entry fails its checksum on drain.
	// The bytes are first quarantined (moved to corrupt/) so the acknowledged
	// write is preserved for forensics/manual recovery, then this fires with
	// the quarantine path. Use it to alert/log. If nil, Flush/Close surface
	// ErrStagingCorrupt instead; recovery on open never blocks the mount.
	OnCorrupt func(id uint64, quarantinePath string, cause error)

	// OnError is invoked when a background drain (driven by the upload loop or a
	// write nudge) fails. Those drains have no caller to return the error to, so
	// without a sink the failure would be swallowed. If nil, errors are logged
	// via slog.Default(). Synchronous drains (recover/Flush/Close) still return
	// their errors to the caller and do NOT route through OnError.
	OnError func(error)

	// Metrics, if set, records cache hit/miss and write-back upload counters and
	// exposes the live staging depth. nil disables instrumentation (all calls are
	// nil-safe).
	Metrics *metrics.Registry
}

// ErrStagingCorrupt reports that one or more acknowledged staging writes failed
// their checksum and were quarantined (not silently dropped). Returned by
// Flush/Close when no Config.OnCorrupt handler is set.
var ErrStagingCorrupt = errors.New("cache: corrupt staging entry quarantined")

var _ chunkstore.Store = (*DiskCache)(nil)

// DiskCache wraps a backing chunkstore.Store with durable staging + an LRU
// read cache.
type DiskCache struct {
	backing chunkstore.Store
	cfg     Config
	staging string
	cache   string
	corrupt string // quarantine dir for staging entries that fail checksum
	tmpSeq  atomic.Uint64

	// diskFree reports (availBytes, totalBytes) for the cache filesystem; used by
	// the MinFreeFraction eviction floor. A field so tests can simulate a full
	// disk; defaults to statfsBytes.
	diskFree func(path string) (avail, total int64, err error)

	// batchBudget bounds how much staged data one batched-drain upload holds in
	// memory before flushing (a field so tests can shrink it).
	batchBudget int

	// now is the clock used for the write-back oldest-unflushed-age gauge; a field
	// so tests can pin it. Defaults to time.Now.
	now func() time.Time

	mu       sync.Mutex
	lru      *list.List // *entry, most-recent at front (clean cache only)
	index    map[uint64]*list.Element
	curBytes int64

	stop  chan struct{}
	nudge chan struct{} // capacity-1 coalescing drain trigger (AutoUpload only)
	wg    sync.WaitGroup
}

type entry struct {
	id   uint64
	size int64
}

const checksumLen = sha256.Size

// New opens (or creates) the cache at cfg.Dir over backing, replays any
// crash-left staging entries, and starts the background uploader when
// cfg.AutoUpload is set.
func New(backing chunkstore.Store, cfg Config) (*DiskCache, error) {
	if cfg.UploadInterval <= 0 {
		cfg.UploadInterval = time.Second
	}
	// Durable write-back (staging + corrupt) goes under StagingDir when set, so it
	// can live on a persistent disk while the reclaimable clean cache lives on a
	// (possibly ephemeral) Dir. Empty StagingDir keeps the original under-Dir layout.
	stagingRoot := cfg.StagingDir
	if stagingRoot == "" {
		stagingRoot = cfg.Dir
	}
	c := &DiskCache{
		backing:     backing,
		cfg:         cfg,
		staging:     filepath.Join(stagingRoot, "staging"),
		cache:       filepath.Join(cfg.Dir, "cache"),
		corrupt:     filepath.Join(stagingRoot, "corrupt"),
		diskFree:    statfsBytes,
		batchBudget: defaultBatchBudget,
		now:         time.Now,
		lru:         list.New(),
		index:       make(map[uint64]*list.Element),
		stop:        make(chan struct{}),
	}
	for _, d := range []string{c.staging, c.cache, c.corrupt} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("cache: mkdir %s: %w", d, err)
		}
	}
	if err := c.scanCache(); err != nil {
		return nil, err
	}
	// Crash recovery: drain anything left in staging. A corrupt entry is
	// quarantined (its bytes preserved) and reported via OnCorrupt, but must
	// not block the mount — so ErrStagingCorrupt is tolerated here.
	if err := c.drainStaging(context.Background()); err != nil && !errors.Is(err, ErrStagingCorrupt) {
		return nil, fmt.Errorf("cache: recover staging: %w", err)
	}
	if cfg.Metrics != nil {
		// Lazy: the staging gauges call this only when metrics are scraped, so the
		// directory walk never lands on the drain path.
		cfg.Metrics.SetStagingObserver(c.stagingDepth)
		cfg.Metrics.SetOldestUnflushedObserver(c.stagingOldestAge)
	}
	if cfg.AutoUpload {
		c.nudge = make(chan struct{}, 1)
		c.wg.Add(1)
		go c.uploadLoop()
	}
	return c, nil
}

// stagingDepth reports the current write-back staging backlog (un-uploaded
// files and bytes). Used as the metrics staging-gauge observer.
func (c *DiskCache) stagingDepth() (files, bytes int64) {
	return fsstat.DirUsage(c.staging)
}

// stagingOldestAge reports how long (seconds) the oldest still-unflushed staged
// write has waited — the GC safety-window canary. A staging file's mtime is when
// the (fsync'd, durable) write was staged, so the oldest mtime ages the backlog.
// 0 when staging is empty. Used as the metrics oldest-unflushed-age observer
// (lazy: it stats the staging dir only when metrics are scraped, never on the
// drain path).
func (c *DiskCache) stagingOldestAge() float64 {
	oldest, ok := fsstat.OldestModTime(c.staging)
	if !ok {
		return 0
	}
	age := c.now().Sub(oldest).Seconds()
	if age < 0 {
		return 0 // clock skew / a write that just landed; never report negative
	}
	return age
}

// Close stops the uploader and drains remaining staging synchronously.
func (c *DiskCache) Close() error {
	close(c.stop)
	c.wg.Wait()
	return c.drainStaging(context.Background())
}

// Flush drains all pending staging entries to the backing store and waits.
func (c *DiskCache) Flush(ctx context.Context) error { return c.drainStaging(ctx) }

// --- chunkstore.Store ---

// Write stages data durably (fsync'd) and returns; the uploader moves it to the
// backing store. The acknowledged write survives a crash via the staging file.
// class is persisted WITH the staged bytes so a recovery drain (which has no
// inode context) packs the slice under the right codec.
func (c *DiskCache) Write(ctx context.Context, id uint64, data []byte, class chunkstore.StoreClass) error {
	if err := c.stage(id, data, class); err != nil {
		return err
	}
	if c.cfg.AutoUpload {
		// Nudge the upload loop to drain promptly. The send is non-blocking and
		// the channel has capacity 1, so a burst of writes coalesces into a
		// single pending drain request instead of spawning a goroutine per
		// write. The periodic ticker (and recover/Flush) remain the durability
		// net if a nudge is dropped because one is already pending.
		select {
		case c.nudge <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *DiskCache) Read(ctx context.Context, id uint64) ([]byte, error) {
	// Prefer the clean cache, then a not-yet-uploaded staging copy.
	if data, ok, err := c.readLocal(c.cachePath(id)); err != nil {
		return nil, err
	} else if ok {
		c.touch(id)
		c.cfg.Metrics.CacheHit()
		return data, nil
	}
	if _, data, ok, err := readStaged(c.stagingPath(id)); err != nil {
		if errors.Is(err, errCorrupt) {
			// A corrupt staging copy is not droppable here (it is the durable record
			// of an acked write); fall through to the backing/miss path and let the
			// drain quarantine it.
		} else {
			return nil, err
		}
	} else if ok {
		c.cfg.Metrics.CacheHit() // staging is local too — no backing fetch
		return data, nil
	}
	// Miss: fetch from backing and populate the clean cache.
	c.cfg.Metrics.CacheMiss()
	data, err := c.backing.Read(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := c.populate(id, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadBatch serves many slices at once: local hits are read from the cache, and
// misses are fetched in ONE coalesced call to the backing (when it is a
// chunkstore.BatchReader — casstore's cross-key pack coalescing) then populated
// into the clean cache exactly like a Read miss, so the LRU/checksum invariants
// are untouched. Falls back to per-id Read when the backing can't batch.
// Best-effort: an unreadable id is simply absent from the result.
func (c *DiskCache) ReadBatch(ctx context.Context, ids []uint64, concurrency int) (map[uint64][]byte, chunkstore.BatchReadStats, error) {
	out := make(map[uint64][]byte, len(ids))
	var stats chunkstore.BatchReadStats
	if len(ids) == 0 {
		return out, stats, nil
	}
	var misses []uint64
	for _, id := range ids {
		if data, ok := c.readLocalAny(id); ok {
			out[id] = data
			continue
		}
		misses = append(misses, id)
	}
	if len(misses) == 0 {
		return out, stats, nil // all local: no backing fetch
	}
	bw, ok := c.backing.(chunkstore.BatchReader)
	if !ok {
		// Backing can't coalesce: fetch misses CONCURRENTLY (bounded by concurrency),
		// each Read populating the cache. Preserves the latency-hiding the prefetcher
		// relies on — just without same-pack coalescing. Each Read ≈ one backing GET.
		conc := concurrency
		if conc < 1 {
			conc = 1
		}
		sem := make(chan struct{}, conc)
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, id := range misses {
			wg.Add(1)
			sem <- struct{}{}
			go func(id uint64) {
				defer wg.Done()
				defer func() { <-sem }()
				if data, err := c.Read(ctx, id); err == nil {
					mu.Lock()
					out[id] = data
					stats.PackGets++
					stats.Bytes += int64(len(data))
					mu.Unlock()
				}
			}(id)
		}
		wg.Wait()
		return out, stats, nil
	}
	fetched, bstats, err := bw.ReadBatch(ctx, misses, concurrency)
	stats = bstats
	if err != nil {
		return out, stats, err // partial result + error; the prefetch caller ignores err
	}
	for id, data := range fetched {
		c.cfg.Metrics.CacheMiss()
		_ = c.populate(id, data) // durable clean-cache write, same as Read's miss path
		out[id] = data
	}
	return out, stats, nil
}

// readLocalAny returns slice id's bytes if resident in the clean cache or the
// durable staging dir, with NO backing round trip. Mirrors Read's first two
// branches; used by ReadBatch to split hits from misses.
func (c *DiskCache) readLocalAny(id uint64) ([]byte, bool) {
	if data, ok, err := c.readLocal(c.cachePath(id)); err == nil && ok {
		c.touch(id)
		c.cfg.Metrics.CacheHit()
		return data, true
	}
	if _, data, ok, err := readStaged(c.stagingPath(id)); err == nil && ok {
		c.cfg.Metrics.CacheHit()
		return data, true
	}
	return nil, false
}

func (c *DiskCache) ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error) {
	data, err := c.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(data)) {
		return 0, nil
	}
	return copy(p, data[off:]), nil
}

func (c *DiskCache) Exists(ctx context.Context, id uint64) (bool, error) {
	if fileExists(c.cachePath(id)) || fileExists(c.stagingPath(id)) {
		return true, nil
	}
	return c.backing.Exists(ctx, id)
}

// CachedLocally reports whether slice id is resident in the local clean cache or
// the durable staging dir, with NO backing-store round trip (unlike Exists,
// which falls through to the backing store / S3 on a local miss). The readahead
// prefetcher uses it to skip slices that are already resident.
func (c *DiskCache) CachedLocally(id uint64) bool {
	return fileExists(c.cachePath(id)) || fileExists(c.stagingPath(id))
}

func (c *DiskCache) Remove(ctx context.Context, id uint64) error {
	c.mu.Lock()
	if el, ok := c.index[id]; ok {
		c.curBytes -= el.Value.(*entry).size
		c.lru.Remove(el)
		delete(c.index, id)
	}
	c.mu.Unlock()
	_ = os.Remove(c.cachePath(id))
	_ = os.Remove(c.stagingPath(id))
	return c.backing.Remove(ctx, id)
}

// --- staging + upload ---

func (c *DiskCache) stagingPath(id uint64) string {
	return filepath.Join(c.staging, fmt.Sprintf("%d", id))
}
func (c *DiskCache) cachePath(id uint64) string { return filepath.Join(c.cache, fmt.Sprintf("%d", id)) }

// stage atomically writes a checksummed staging file and fsyncs it + its dir.
// The staging file carries the slice's StoreClass in a staging-SPECIFIC framing
// (writeStaged) so the class survives a crash; the clean cache keeps the shared
// writeChecksummed format untouched (it has no use for the class).
func (c *DiskCache) stage(id uint64, data []byte, class chunkstore.StoreClass) error {
	tmp := filepath.Join(c.staging, fmt.Sprintf(".tmp-%d-%d", id, c.tmpSeq.Add(1)))
	if err := writeStaged(tmp, byte(class), data); err != nil {
		return fmt.Errorf("cache: stage %d: %w", id, err)
	}
	if err := os.Rename(tmp, c.stagingPath(id)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("cache: stage rename %d: %w", id, err)
	}
	if err := fsyncDir(c.staging); err != nil {
		return fmt.Errorf("cache: stage fsync dir: %w", err)
	}
	return nil
}

func (c *DiskCache) uploadLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.UploadInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.backgroundDrain()
		case <-c.nudge:
			c.backgroundDrain()
		}
	}
}

// backgroundDrain runs a drain for the upload loop (tick or write nudge). These
// drains have no caller to return the error to, so a failure must be surfaced
// (OnError if set, else slog.Default()) rather than swallowed. A quarantine-only
// drain (ErrStagingCorrupt) already alerted via OnCorrupt and is not re-reported.
func (c *DiskCache) backgroundDrain() {
	if err := c.drainStaging(context.Background()); err != nil && !errors.Is(err, ErrStagingCorrupt) {
		if c.cfg.OnError != nil {
			c.cfg.OnError(err)
			return
		}
		slog.Default().Error("cache: background drainStaging failed", "err", err)
	}
}

// drainStaging uploads every complete staging entry to the backing store and
// promotes it to the clean cache. Safe to call concurrently (per-entry work is
// idempotent; a missing entry just means another drain handled it).
func (c *DiskCache) drainStaging(ctx context.Context) error {
	ents, err := os.ReadDir(c.staging)
	if err != nil {
		return err
	}
	// When the backing store can write a BATCH in one packed operation (casstore
	// PutBatch → shared packs), prefer it: it collapses the per-slice S3-PUT storm
	// that bounds -remote write throughput. Falls through to per-slice uploads for
	// backings without the capability.
	if bw, ok := c.backing.(chunkstore.BatchWriter); ok {
		return c.drainBatched(ctx, ents, bw)
	}

	conc := c.cfg.UploadConcurrency
	if conc < 1 {
		conc = 1
	}

	var corrupt atomic.Int64
	var firstErr error
	var errMu sync.Mutex
	setErr := func(e error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}

	if conc == 1 {
		// Serial (default): preserve the single-in-flight behavior so Flush/Close
		// and the upload loop never overlap backing Writes.
		for _, de := range ents {
			cr, e := c.drainOne(ctx, de)
			if cr {
				corrupt.Add(1)
			}
			if e != nil {
				setErr(e)
				break // stop on first error; the entry stays staged for retry
			}
		}
	} else {
		// Bounded intra-drain concurrency: upload up to conc staged slices at once
		// (distinct ids, so per-entry work does not race). Still ONE drain
		// goroutine overall — this is not the old per-write fan-out.
		sem := make(chan struct{}, conc)
		var wg sync.WaitGroup
		for _, de := range ents {
			wg.Add(1)
			sem <- struct{}{}
			go func(de os.DirEntry) {
				defer wg.Done()
				defer func() { <-sem }()
				cr, e := c.drainOne(ctx, de)
				if cr {
					corrupt.Add(1)
				}
				if e != nil {
					setErr(e)
				}
			}(de)
		}
		wg.Wait()
	}

	if firstErr != nil {
		return firstErr // retry on next drain
	}
	// If anything was quarantined and there is no handler to absorb the alert,
	// surface it so an active Flush/Close caller cannot mistake corruption for
	// a clean drain. New() (recovery) tolerates this via errors.Is.
	if n := int(corrupt.Load()); n > 0 && c.cfg.OnCorrupt == nil {
		return fmt.Errorf("%w (%d entr%s)", ErrStagingCorrupt, n, plural(n))
	}
	return nil
}

// drainOne uploads a single staging entry to the backing store and promotes it
// to the clean cache. corrupt is true when the entry failed its checksum and was
// quarantined. Safe to run concurrently for distinct entries (per-entry work is
// idempotent; populate/evict take c.mu).
func (c *DiskCache) drainOne(ctx context.Context, de os.DirEntry) (corrupt bool, err error) {
	name := de.Name()
	if de.IsDir() || len(name) == 0 || name[0] == '.' { // skip .tmp-*
		return false, nil
	}
	id, perr := parseID(name)
	if perr != nil {
		return false, nil
	}
	path := c.stagingPath(id)
	class, data, ok, rerr := readStaged(path)
	if rerr != nil {
		// Corrupt staging entry. The caller was already told this Write was durable
		// (fsync returned), so dropping it would silently lose an acknowledged
		// write. Instead quarantine the bytes (preserve them for recovery) and
		// alert; only count it as drained once preserved.
		if qerr := c.quarantine(id, path, rerr); qerr != nil {
			return false, qerr // could not preserve — leave in staging, retry later
		}
		return true, nil
	}
	if !ok {
		return false, nil // vanished
	}
	if err := c.backing.Write(ctx, id, data, chunkstore.StoreClass(class)); err != nil {
		return false, err // retry on next drain
	}
	c.cfg.Metrics.Upload(int64(len(data)))
	// Promote to clean cache, then drop staging.
	_ = c.populate(id, data)
	_ = os.Remove(path)
	return false, nil
}

// defaultBatchBudget caps how much staged data one batched upload holds in
// memory before flushing — ~one pack target, so each batch fills roughly one
// casstore pack and UploadConcurrency batches run as ~that many parallel pack
// PUTs.
const defaultBatchBudget = 16 << 20 // 16 MiB

// drainBatched uploads staged slices to a BatchWriter backing in byte-bounded
// batches, so their chunks share casstore packs (one PUT per ~pack instead of
// one per ~128 KB slice). Batches are flushed with bounded concurrency
// (UploadConcurrency, ≤1 = serial), so on the high-latency -remote path packs
// upload in parallel. Staging entries are removed only after their batch
// commits, preserving the crash-durability contract; corrupt entries are
// quarantined out of the batch exactly as the per-slice path does. Reading
// staging is sequential (local, fast); only the uploads run concurrently.
func (c *DiskCache) drainBatched(ctx context.Context, ents []os.DirEntry, bw chunkstore.BatchWriter) error {
	conc := c.cfg.UploadConcurrency
	if conc < 1 {
		conc = 1
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	setErr := func(e error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}
	// flushAsync hands a batch off to the bounded upload pool. The batch/paths
	// slices are owned by the goroutine (the caller starts fresh ones), so they
	// are never reused under it.
	flushAsync := func(batch []chunkstore.BatchItem, paths []string) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := bw.WriteBatch(ctx, batch); err != nil {
				setErr(err) // staging untouched → retried on the next drain
				return
			}
			var up int64
			for i := range batch {
				up += int64(len(batch[i].Data))
				_ = c.populate(batch[i].ID, batch[i].Data)
				_ = os.Remove(paths[i])
			}
			c.cfg.Metrics.Upload(up) // one batched upload (~one packed PUT)
		}()
	}

	var (
		batch      []chunkstore.BatchItem
		batchPaths []string
		batchBytes int
		corrupt    int
	)
	for _, de := range ents {
		name := de.Name()
		if de.IsDir() || len(name) == 0 || name[0] == '.' { // skip .tmp-*
			continue
		}
		id, perr := parseID(name)
		if perr != nil {
			continue
		}
		path := c.stagingPath(id)
		class, data, ok, rerr := readStaged(path)
		if rerr != nil {
			if qerr := c.quarantine(id, path, rerr); qerr != nil {
				wg.Wait()
				return qerr
			}
			corrupt++
			continue
		}
		if !ok {
			continue // vanished
		}
		batch = append(batch, chunkstore.BatchItem{ID: id, Data: data, Class: chunkstore.StoreClass(class)})
		batchPaths = append(batchPaths, path)
		batchBytes += len(data)
		if batchBytes >= c.batchBudget {
			flushAsync(batch, batchPaths)
			batch, batchPaths, batchBytes = nil, nil, 0 // fresh slices; old ones are in flight
		}
	}
	if len(batch) > 0 {
		flushAsync(batch, batchPaths)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if corrupt > 0 && c.cfg.OnCorrupt == nil {
		return fmt.Errorf("%w (%d entr%s)", ErrStagingCorrupt, corrupt, plural(corrupt))
	}
	return nil
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// quarantine moves a checksum-failing staging file into the corrupt/ dir
// (preserving the acknowledged bytes for manual recovery) and fires OnCorrupt.
// Returns an error only if the bytes could not be preserved — in which case the
// caller leaves the entry in staging to retry rather than risk losing it.
func (c *DiskCache) quarantine(id uint64, path string, cause error) error {
	dst := filepath.Join(c.corrupt, fmt.Sprintf("%d-%d", id, c.tmpSeq.Add(1)))
	if err := os.Rename(path, dst); err != nil {
		return fmt.Errorf("cache: quarantine corrupt staging %d: %w", id, err)
	}
	if err := fsyncDir(c.corrupt); err != nil {
		return fmt.Errorf("cache: quarantine fsync dir: %w", err)
	}
	if c.cfg.OnCorrupt != nil {
		c.cfg.OnCorrupt(id, dst, cause)
	}
	return nil
}

// --- clean cache + LRU ---

func (c *DiskCache) scanCache() error {
	ents, err := os.ReadDir(c.cache)
	if err != nil {
		return err
	}
	for _, de := range ents {
		if de.IsDir() {
			continue
		}
		id, perr := parseID(de.Name())
		if perr != nil {
			continue
		}
		info, ierr := de.Info()
		if ierr != nil {
			continue
		}
		c.addLRU(id, info.Size())
	}
	c.evict()
	return nil
}

// populate writes data into the clean cache and updates the LRU.
func (c *DiskCache) populate(id uint64, data []byte) error {
	path := c.cachePath(id)
	if err := writeChecksummed(path, data); err != nil {
		return err
	}
	c.mu.Lock()
	c.addLRU(id, int64(len(data)+checksumLen))
	c.evict()
	c.mu.Unlock()
	return nil
}

func (c *DiskCache) addLRU(id uint64, size int64) {
	if el, ok := c.index[id]; ok {
		c.curBytes -= el.Value.(*entry).size
		c.lru.Remove(el)
	}
	el := c.lru.PushFront(&entry{id: id, size: size})
	c.index[id] = el
	c.curBytes += size
}

func (c *DiskCache) touch(id uint64) {
	c.mu.Lock()
	if el, ok := c.index[id]; ok {
		c.lru.MoveToFront(el)
	}
	c.mu.Unlock()
}

// evict trims the clean cache to the configured caps (MaxBytes / MaxItems) and
// the MinFreeFraction disk-headroom floor. Caller holds c.mu.
func (c *DiskCache) evict() {
	// Bytes of clean cache to drop so the filesystem regains its free headroom
	// (snapshot once via statfs; we count what we free against it). 0 when the
	// floor is disabled or already satisfied.
	spaceTarget := c.freeSpaceEvictBytes()
	var freedForSpace int64
	for {
		overBytes := c.cfg.MaxBytes > 0 && c.curBytes > c.cfg.MaxBytes
		overItems := c.cfg.MaxItems > 0 && c.lru.Len() > c.cfg.MaxItems
		overSpace := spaceTarget > 0 && freedForSpace < spaceTarget
		if !overBytes && !overItems && !overSpace {
			return
		}
		back := c.lru.Back()
		if back == nil {
			return
		}
		e := back.Value.(*entry)
		c.lru.Remove(back)
		delete(c.index, e.id)
		c.curBytes -= e.size
		_ = os.Remove(c.cachePath(e.id))
		freedForSpace += e.size
	}
}

// freeSpaceEvictBytes returns how many bytes of CLEAN cache to evict so the
// cache filesystem regains MinFreeFraction of free space (0 when disabled,
// statfs fails, or there is already enough free). The result is clamped to the
// clean cache's current size — the rest of the disk's usage (staging, other
// tenants, the OS) is not ours to reclaim, so the floor is best-effort. Caller
// holds c.mu.
func (c *DiskCache) freeSpaceEvictBytes() int64 {
	if c.cfg.MinFreeFraction <= 0 || c.diskFree == nil {
		return 0
	}
	avail, total, err := c.diskFree(c.cache)
	if err != nil || total <= 0 {
		return 0 // never evict blindly on a statfs error
	}
	want := int64(c.cfg.MinFreeFraction * float64(total))
	if avail >= want {
		return 0
	}
	deficit := want - avail
	if deficit > c.curBytes {
		deficit = c.curBytes
	}
	return deficit
}

// statfsBytes reports the available and total bytes of the filesystem holding
// path (best-effort; an unmounted/missing path returns an error and disables the
// free-space floor for that call).
func statfsBytes(path string) (avail, total int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := int64(st.Bsize)
	return int64(st.Bavail) * bs, int64(st.Blocks) * bs, nil
}

// readLocal reads a checksummed cache/staging file. ok=false if absent.
func (c *DiskCache) readLocal(path string) ([]byte, bool, error) {
	data, ok, err := readChecksummed(path)
	if err != nil {
		if errors.Is(err, errCorrupt) {
			// Drop a corrupt clean-cache copy and treat as a miss.
			_ = os.Remove(path)
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, ok, nil
}

// --- file helpers (checksummed, fsync'd) ---

var errCorrupt = errors.New("cache: checksum mismatch")

// writeChecksummed writes [sha256(32)][data] to path and fsyncs the file.
func writeChecksummed(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if _, err := f.Write(sum[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // the per-write fsync — durability before ack
		f.Close()
		return err
	}
	return f.Close()
}

// readChecksummed reads a file written by writeChecksummed, verifying the hash.
// ok=false (nil error) when the file does not exist.
func readChecksummed(path string) ([]byte, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(raw) < checksumLen {
		return nil, false, errCorrupt
	}
	body := raw[checksumLen:]
	want := sha256.Sum256(body)
	for i := 0; i < checksumLen; i++ {
		if want[i] != raw[i] {
			return nil, false, errCorrupt
		}
	}
	return body, true, nil
}

// stagedHashPrefix domain-separates the staging checksum from the clean-cache /
// legacy writeChecksummed checksum. Without it, a legacy file [sha256(data)][data]
// is indistinguishable from a staged file [sha256(class||body)][class][body] with
// class == data[0] (the hash inputs are byte-identical), so a legacy pending
// write would be mis-read with a garbage class. Mixing this constant into the
// staged hash makes the two formats unambiguous: a legacy file fails the staged
// checksum and falls through to the legacy reader (→ ClassDefault).
var stagedHashPrefix = []byte("mlfs-staged-v1\x00")

func stagedSum(class byte, data []byte) []byte {
	h := sha256.New()
	h.Write(stagedHashPrefix)
	h.Write([]byte{class})
	h.Write(data)
	return h.Sum(nil)
}

// writeStaged writes a staging file framed as [sha256(32) of (prefix||class||data)]
// [class(1)][data] and fsyncs it. This is the staging-SPECIFIC format: the class
// byte must survive a crash so a recovery drain (no inode context) packs the
// slice under the right codec. It is deliberately NOT writeChecksummed — that
// format also backs the clean cache (populate), which has no use for the class,
// so framing the class only here keeps the clean-cache on-disk format and its LRU
// size accounting unchanged.
func writeStaged(path string, class byte, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(stagedSum(class, data)); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write([]byte{class}); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // the per-write fsync — durability before ack
		f.Close()
		return err
	}
	return f.Close()
}

// readStaged reads a file written by writeStaged, verifying the hash and
// recovering the class byte. ok=false (nil error) when the file does not exist.
// For backward compatibility a file in the legacy writeChecksummed format
// ([sha256(32) of data][data], written before the class byte existed) is accepted
// and reported as ClassDefault (0) — so a staging entry pending across an in-place
// upgrade still drains instead of being quarantined.
func readStaged(path string) (class byte, data []byte, ok bool, err error) {
	raw, rerr := os.ReadFile(path)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return 0, nil, false, nil
		}
		return 0, nil, false, rerr
	}
	if len(raw) >= checksumLen+1 {
		cb := raw[checksumLen]
		body := raw[checksumLen+1:]
		if bytes.Equal(stagedSum(cb, body), raw[:checksumLen]) {
			return cb, body, true, nil
		}
	}
	// Not the staged framing: try the legacy writeChecksummed format.
	if body, lok, lerr := readChecksummed(path); lerr == nil && lok {
		return byte(chunkstore.ClassDefault), body, true, nil
	}
	return 0, nil, false, errCorrupt
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func parseID(name string) (uint64, error) {
	return strconv.ParseUint(name, 10, 64)
}
