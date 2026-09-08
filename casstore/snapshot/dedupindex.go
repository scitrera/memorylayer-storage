// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// PackRef locates a content-addressed chunk within a pack blob. It carries
// exactly what Get needs to fetch a chunk's bytes without re-reading the
// whole object: which pack holds it, where inside that pack, and how long it
// is. It is the value half of the dedup mapping (chunk content hash → where
// the chunk physically lives).
type PackRef struct {
	PackHash string // sha256 hex of the pack blob holding the chunk
	Offset   int    // byte offset of the chunk within the pack blob
	Size     int    // chunk byte length
}

// ChunkLocation pairs a chunk's content hash with its PackRef. It is the unit
// a DedupSession records and a DedupStore persists, and corresponds to one
// row of the plan's pack_manifest(domain, chunk_hash, pack_hash, offset,
// length) table.
type ChunkLocation struct {
	ChunkHash string
	PackRef
}

// DedupIndex decides, during a Put on the v2 (pack) write path, whether a
// freshly-split chunk already exists in the store — so its bytes can be
// reused instead of re-packed and re-uploaded — and remembers newly-packed
// chunks for future Puts.
//
// Two implementations ship with casstore:
//
//   - PriorManifestIndex (the default; also what a nil ChunkedConfig.Index
//     selects) dedups a new object only against the latest prior snapshot of
//     the SAME key. This is sandbox-provider's original behavior and needs no
//     external store — correct for snapshot version chains, but yields almost
//     no dedup for distinct-object workloads (every object is a new key).
//   - GlobalIndex dedups across ALL objects within a dedup domain, backed by
//     a caller-supplied DedupStore (e.g. Postgres for blobgw). This is what
//     gives "25 identical uploads store once".
//
// Dedup is always best-effort: a Lookup miss — or a backend error — simply
// means the chunk is packed and uploaded normally. It never blocks or fails a
// Put, so a flaky index degrades storage efficiency, never correctness.
type DedupIndex interface {
	// NewSession opens a per-Put dedup view for object `key` within `domain`.
	// One Put owns one session; a session is not used by multiple goroutines.
	NewSession(ctx context.Context, domain string, key SnapshotKey) (DedupSession, error)
}

// DedupSession is a single Put's working view over a DedupIndex. It is not
// safe for concurrent use.
type DedupSession interface {
	// Lookup reports whether chunkHash is already stored (and, if so, where).
	// A false result means "pack and upload this chunk".
	Lookup(chunkHash string) (PackRef, bool)

	// Record notes that chunkHash was just packed at the given location, so
	// future Puts (for durable indexes) can reuse it. Buffered until Commit.
	// Idempotent per hash.
	Record(loc ChunkLocation)

	// Commit durably persists everything Record-ed. It is called once, after
	// every pack for the object has been uploaded, so a recorded location
	// always points at bytes that already exist in the blob store. A no-op
	// for PriorManifestIndex (the manifest ChunkedStore writes is its record).
	Commit() error
}

// DedupStore is the durable backing a GlobalIndex persists chunk locations
// to. Callers supply the implementation: blobgw (L1) backs it with a Postgres
// pack_manifest table; tests and small deployments can use MemoryDedupStore.
//
// Every operation is scoped by `domain`: a chunk recorded in domain A is
// invisible from domain B. That isolation is the boundary that both prevents
// cross-domain dedup and closes the cross-tenant existence oracle, so the
// domain string MUST be a trust boundary the caller is comfortable sharing
// dedup within (the whole enterprise instance for trusted internal storage; a
// single tenant/workspace for untrusted multi-tenant SaaS).
//
// Implementations must be safe for concurrent use.
type DedupStore interface {
	// Lookup returns the stored location of chunkHash in domain, or ok=false
	// if it is absent.
	Lookup(ctx context.Context, domain, chunkHash string) (PackRef, bool, error)

	// LookupBatch resolves many chunk hashes in one round-trip. The returned
	// map contains only the hashes that were found; missing hashes are simply
	// absent. Order-independent.
	LookupBatch(ctx context.Context, domain string, chunkHashes []string) (map[string]PackRef, error)

	// Record idempotently persists chunk locations in domain. Recording a hash
	// that is already present is a no-op: the first writer of a content hash
	// wins, and later identical content maps to those same bytes (the same
	// content-addressed semantics as blobstore.PutIfAbsent).
	Record(ctx context.Context, domain string, locs []ChunkLocation) error

	// PurgePacks removes every index entry in domain whose location points to
	// one of the given pack hashes. GC calls this when it reclaims a pack, so
	// the index never hands out a reference to collected bytes. Idempotent;
	// purging an absent pack is a no-op. (Postgres: DELETE FROM pack_manifest
	// WHERE domain=$1 AND pack_hash = ANY($2).)
	PurgePacks(ctx context.Context, domain string, packHashes []string) error
}

// PackSize is the physical (compressed, on-disk) size of one pack blob.
type PackSize struct {
	PackHash        string
	CompressedBytes int64
}

// PackSizeRecorder is an OPTIONAL capability a DedupStore MAY implement to track
// per-pack physical sizes for storage accounting. The write + compaction paths
// call it when the store implements it; stores that don't lose only the accounting.
type PackSizeRecorder interface {
	RecordPackSizes(ctx context.Context, domain string, sizes []PackSize) error
}

// ---------------------------------------------------------------------------
// PriorManifestIndex — default, no external store
// ---------------------------------------------------------------------------

// PriorManifestIndex reproduces sandbox-provider's original dedup scope: a new
// object is deduped only against the latest prior snapshot of the SAME key.
// It holds no external state — each session loads the prior manifest from the
// upstream SnapshotStore. This is the default ChunkedStore behavior (a nil
// ChunkedConfig.Index is equivalent to one of these).
type PriorManifestIndex struct {
	upstream SnapshotStore
}

// NewPriorManifestIndex constructs a PriorManifestIndex over the same upstream
// SnapshotStore the ChunkedStore writes manifests to.
func NewPriorManifestIndex(upstream SnapshotStore) *PriorManifestIndex {
	return &PriorManifestIndex{upstream: upstream}
}

// NewSession loads the latest prior manifest for key (if any) into an
// in-memory chunk-hash → PackRef map. domain is unused: prior-manifest dedup
// is inherently scoped to the key's own version chain.
func (p *PriorManifestIndex) NewSession(ctx context.Context, _ string, key SnapshotKey) (DedupSession, error) {
	return &priorManifestSession{index: p.loadPriorIndex(ctx, key)}, nil
}

// loadPriorIndex fetches the latest manifest for key and returns a map from
// chunk hash → PackRef for fast dedup lookup. Only genuine v2 pack refs are
// included (non-empty PackHash that differs from the chunk hash); v1
// chunk-blob refs cannot be reused inside a v2 pack without re-reading the
// original blob, which defeats the purpose. Any error returns nil — dedup is
// best-effort, and a miss just means re-packing.
func (p *PriorManifestIndex) loadPriorIndex(ctx context.Context, key SnapshotKey) map[string]PackRef {
	rc, _, err := p.upstream.GetLatest(ctx, key)
	if err != nil {
		return nil
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil
	}
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	if m.Format != manifestFormatV2 {
		return nil
	}
	idx := make(map[string]PackRef, len(m.Chunks))
	for _, ref := range m.Chunks {
		if ref.PackHash == "" || ref.PackHash == ref.Hash {
			continue
		}
		idx[ref.Hash] = PackRef{PackHash: ref.PackHash, Offset: ref.Offset, Size: ref.Size}
	}
	return idx
}

// priorManifestSession answers lookups from the primed prior-manifest map.
// Record and Commit are no-ops: the durable record of where these chunks live
// is the manifest ChunkedStore writes, which the next Put reloads.
type priorManifestSession struct {
	index map[string]PackRef
}

func (s *priorManifestSession) Lookup(chunkHash string) (PackRef, bool) {
	ref, ok := s.index[chunkHash]
	return ref, ok
}

func (s *priorManifestSession) Record(ChunkLocation) {}

func (s *priorManifestSession) Commit() error { return nil }

// ---------------------------------------------------------------------------
// GlobalIndex — cross-object dedup within a domain, durable
// ---------------------------------------------------------------------------

// GlobalIndex dedups across every object in a dedup domain by consulting a
// durable DedupStore. It is the index enterprise wires (over Postgres) to get
// cross-workspace dedup of identical pages/renders/tensors.
//
// Concurrency note: two objects Put concurrently into the same domain may each
// miss the other's not-yet-committed chunks and both pack+upload them; because
// packs are content-addressed the loser merely wastes one upload, never
// corrupts data. Sequential Puts (and the common re-ingest case) see prior
// chunks via the store and dedup fully.
type GlobalIndex struct {
	store  DedupStore
	logger *slog.Logger
}

// NewGlobalIndex constructs a GlobalIndex over the supplied durable store.
// logger may be nil (falls back to slog.Default); it is used only to surface
// best-effort lookup/commit failures, which never fail a Put.
func NewGlobalIndex(store DedupStore, logger *slog.Logger) *GlobalIndex {
	if logger == nil {
		logger = slog.Default()
	}
	return &GlobalIndex{store: store, logger: logger}
}

// dedupRepointer is the optional DedupIndex capability pack compaction uses to keep
// a durable dedup index consistent after it consolidates packs: it moves the
// compacted chunks' index entries onto the new (consolidated) packs and drops the
// old ones, so a post-compaction dedup hit references the consolidated pack — which
// the rewritten manifests also reference — instead of re-pinning the old pack against
// GC (which, since GC liveness is manifest-only and the index is only purged at
// reclaim, would otherwise keep the old pack from ever draining; TECH_DEBT #51).
// GlobalIndex implements it; PriorManifestIndex does not — its "index" is the per-key
// manifest chain, already updated by compaction's manifest rewrite.
type dedupRepointer interface {
	Repoint(ctx context.Context, domain string, newLocs []ChunkLocation, deadPacks []string) error
}

var _ dedupRepointer = (*GlobalIndex)(nil)

// Repoint moves the durable index entries for a set of just-compacted chunks to
// their new pack locations and drops the packs compaction consolidated away. It is
// the index half of pack compaction (see dedupRepointer): deadPacks are the old
// (candidate) pack hashes — their entries, including the now-dead non-relocated
// chunks, are purged — and newLocs are the relocated live chunks at their
// consolidated-pack locations. Purge-then-record because DedupStore.Record is
// first-writer-wins and would otherwise keep the stale old-pack entry. A no-op when
// there is nothing to drop.
func (g *GlobalIndex) Repoint(ctx context.Context, domain string, newLocs []ChunkLocation, deadPacks []string) error {
	if len(deadPacks) == 0 {
		return nil
	}
	if err := g.store.PurgePacks(ctx, domain, deadPacks); err != nil {
		return fmt.Errorf("dedup repoint: purge consolidated packs: %w", err)
	}
	if len(newLocs) == 0 {
		return nil
	}
	if err := g.store.Record(ctx, domain, newLocs); err != nil {
		return fmt.Errorf("dedup repoint: record new locations: %w", err)
	}
	return nil
}

// packSizeRecorder exposes the durable store's optional PackSizeRecorder
// capability, or nil if the store does not implement it. It is the seam the
// pack-write and compaction paths use to record per-pack physical (compressed)
// sizes for storage accounting without adding a method to DedupStore (which
// would force every implementer — PriorManifestIndex, MemoryDedupStore, tests —
// to change). Best-effort: a nil result simply means no accounting.
func (g *GlobalIndex) packSizeRecorder() PackSizeRecorder {
	if r, ok := g.store.(PackSizeRecorder); ok {
		return r
	}
	return nil
}

// NewSession opens a domain-scoped session backed by the durable store.
func (g *GlobalIndex) NewSession(ctx context.Context, domain string, _ SnapshotKey) (DedupSession, error) {
	return &globalSession{
		ctx:    ctx,
		domain: domain,
		store:  g.store,
		logger: g.logger,
	}, nil
}

type globalSession struct {
	ctx     context.Context
	domain  string
	store   DedupStore
	logger  *slog.Logger
	pending []ChunkLocation
}

// Lookup queries the durable store for chunkHash within the domain. A backend
// error is logged and reported as a miss so the chunk is packed normally —
// dedup is best-effort and never blocks a Put.
func (s *globalSession) Lookup(chunkHash string) (PackRef, bool) {
	ref, ok, err := s.store.Lookup(s.ctx, s.domain, chunkHash)
	if err != nil {
		s.logger.WarnContext(s.ctx, "dedup global index: lookup failed; treating as miss",
			"domain", s.domain, "chunk", chunkHash, "err", err)
		return PackRef{}, false
	}
	return ref, ok
}

func (s *globalSession) Record(loc ChunkLocation) {
	s.pending = append(s.pending, loc)
}

// Commit persists every recorded chunk location in one batch. It runs after
// all packs are uploaded, so the locations always reference existing bytes.
func (s *globalSession) Commit() error {
	if len(s.pending) == 0 {
		return nil
	}
	return s.store.Record(s.ctx, s.domain, s.pending)
}

// ---------------------------------------------------------------------------
// MemoryDedupStore — in-memory reference DedupStore
// ---------------------------------------------------------------------------

// MemoryDedupStore is an in-memory, concurrency-safe DedupStore. It is the
// reference implementation used by tests and viable for single-process
// deployments; durable deployments (blobgw) supply a SQL-backed store with
// the same contract. Record is idempotent (first writer of a hash wins).
type MemoryDedupStore struct {
	mu    sync.RWMutex
	byDom map[string]map[string]PackRef // domain → chunkHash → location

	// assocMu guards assoc. It is deliberately a SEPARATE lock from mu: a Lookup
	// holds mu (read) and must consult pack lifecycle state to honour the
	// "never hand out a reclaiming pack" contract, so the two locks must be
	// independently acquirable. assoc is nil until the first association call, so
	// a store nobody associates with costs nothing.
	assocMu sync.Mutex
	assoc   map[string]*memAssoc // domain → associations + lifecycle
}

// NewMemoryDedupStore constructs an empty in-memory dedup store.
func NewMemoryDedupStore() *MemoryDedupStore {
	return &MemoryDedupStore{byDom: make(map[string]map[string]PackRef)}
}

// Lookup honours the PackAssocStore contract: a location in a pack that is
// obliterating or obliterated is reported as a MISS, so a Put never dedups into
// bytes on their way out (guard 1 in packassoc.go).
func (m *MemoryDedupStore) Lookup(_ context.Context, domain, chunkHash string) (PackRef, bool, error) {
	m.mu.RLock()
	ref, ok := m.byDom[domain][chunkHash]
	m.mu.RUnlock()
	if !ok {
		return PackRef{}, false, nil
	}
	if m.packUnavailable(domain, ref.PackHash) {
		return PackRef{}, false, nil
	}
	return ref, true, nil
}

// LookupBatch applies the same reclamation filter as Lookup.
func (m *MemoryDedupStore) LookupBatch(_ context.Context, domain string, chunkHashes []string) (map[string]PackRef, error) {
	m.mu.RLock()
	dom := m.byDom[domain]
	out := make(map[string]PackRef, len(chunkHashes))
	for _, h := range chunkHashes {
		if ref, ok := dom[h]; ok {
			out[h] = ref
		}
	}
	m.mu.RUnlock()
	for h, ref := range out {
		if m.packUnavailable(domain, ref.PackHash) {
			delete(out, h)
		}
	}
	return out, nil
}

func (m *MemoryDedupStore) Record(_ context.Context, domain string, locs []ChunkLocation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dom := m.byDom[domain]
	if dom == nil {
		dom = make(map[string]PackRef)
		m.byDom[domain] = dom
	}
	for _, loc := range locs {
		// First writer wins — idempotent, content-addressed.
		if _, exists := dom[loc.ChunkHash]; !exists {
			dom[loc.ChunkHash] = loc.PackRef
		}
	}
	return nil
}

func (m *MemoryDedupStore) PurgePacks(_ context.Context, domain string, packHashes []string) error {
	if len(packHashes) == 0 {
		return nil
	}
	purge := make(map[string]struct{}, len(packHashes))
	for _, ph := range packHashes {
		purge[ph] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dom := m.byDom[domain]
	for hash, ref := range dom {
		if _, ok := purge[ref.PackHash]; ok {
			delete(dom, hash)
		}
	}
	return nil
}

// Len reports the number of distinct chunk hashes recorded in a domain.
// Exposed for tests and admin/metrics; not part of the DedupStore contract.
func (m *MemoryDedupStore) Len(domain string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byDom[domain])
}
