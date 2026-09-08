// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package chunkstore stores file slices through casstore. File metadata maps
// logical chunk offsets to slices; content-addressed packs deduplicate their
// bytes within a domain. Copy-on-write clones reuse existing slices.
// A disk cache provides local reads and durable staging for write-back.
package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// StoreClass is a slice's compression class: it decides whether the backing
// casstore compresses the slice's pack or stores it uncompressed. mlfs sets it
// per file (by file type, at create) so a model/tensor file stays uncompressed
// (range-readable + mmap/zero-copy device loads) while generic data compresses —
// on the SAME tenant, instead of relying on a global CompressNone switch. The
// class is carried with the staged slice (crash-safe) so a recovery drain packs
// it correctly with no inode context. The zero value is ClassDefault.
type StoreClass uint8

const (
	// ClassDefault leaves the codec to the backing's CompressionPolicy (generic
	// content compresses; the policy still stores recognized already-compressed /
	// tensor content types uncompressed). The unchanged pre-classification
	// behavior.
	ClassDefault StoreClass = iota
	// ClassUncompressed forces the slice's pack to be stored uncompressed
	// regardless of policy — the model/tensor class that must stay byte-for-byte
	// on disk for ranged reads and mmap/zero-copy device loads.
	ClassUncompressed
)

// Store reads and writes slice data. Implementations are safe for concurrent
// use. Slice ids are allocated by the metadata engine (NewSlice) and are
// opaque, globally-unique handles here.
type Store interface {
	// Write stores the bytes of slice id under compression class class.
	// Idempotent for identical content (casstore dedups the underlying chunks;
	// the class only selects the physical codec, never the chunk identity).
	Write(ctx context.Context, id uint64, data []byte, class StoreClass) error
	// Read returns the full bytes of slice id.
	Read(ctx context.Context, id uint64) ([]byte, error)
	// ReadAt reads len(p) bytes of slice id starting at off, returning the
	// number of bytes read (io.ReadFull semantics).
	ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error)
	// Exists reports whether slice id has been written.
	Exists(ctx context.Context, id uint64) (bool, error)
	// Remove deletes slice id's manifest; its chunks are reclaimed by GC once
	// no slice references them.
	Remove(ctx context.Context, id uint64) error
}

// CasStore implements Store over a casstore ChunkedStore. The ChunkedStore
// should be configured with a GlobalIndex so identical content across distinct
// slice ids deduplicates.
type CasStore struct {
	cs *snapshot.ChunkedStore
}

var _ Store = (*CasStore)(nil)

// New wraps a casstore ChunkedStore as a slice data store.
func New(cs *snapshot.ChunkedStore) *CasStore { return &CasStore{cs: cs} }

// sliceKey maps a slice id to its casstore object key. The physical chunks
// beneath are content-addressed regardless of this key, so the key choice does
// not affect dedup.
func sliceKey(id uint64) snapshot.SnapshotKey {
	return snapshot.SnapshotKey{OwnerKey: "s/" + strconv.FormatUint(id, 10)}
}

func (s *CasStore) Write(ctx context.Context, id uint64, data []byte, class StoreClass) error {
	if _, err := s.cs.Put(ctx, sliceKey(id), metaForClass(class), bytes.NewReader(data)); err != nil {
		return fmt.Errorf("chunkstore: write slice %d: %w", id, err)
	}
	return nil
}

// metaForClass maps a slice's StoreClass to the casstore metadata that selects
// its codec. ClassUncompressed stamps the store_uncompressed tag (an
// unconditional per-object opt-out the policy honors first), so the uncompressed
// guarantee is a property of the slice itself rather than of the global policy
// default. ClassDefault leaves the metadata empty so the backing's
// CompressionPolicy decides. casstore packs a batch's chunks codec-homogeneously
// (it keys packs on (domain, codec)), so mixed-class slices in one WriteBatch
// still land in class-uniform packs.
func metaForClass(class StoreClass) snapshot.SnapshotMetadata {
	if class == ClassUncompressed {
		return snapshot.SnapshotMetadata{Tags: map[string]string{snapshot.TagStoreUncompressed: "1"}}
	}
	return snapshot.SnapshotMetadata{}
}

// BatchItem is one slice in a WriteBatch.
type BatchItem struct {
	ID    uint64
	Data  []byte
	Class StoreClass
}

// BatchWriter is an optional Store capability: write many slices in ONE packed
// operation so their chunks share casstore packs (far fewer object/S3 PUTs than
// one Write per slice). The disk-cache write-back drain uses it when the backing
// store implements it — the fix for the -remote per-128-KB-slice PUT storm.
type BatchWriter interface {
	WriteBatch(ctx context.Context, items []BatchItem) error
}

var _ BatchWriter = (*CasStore)(nil)

// WriteBatch stores many slices in one casstore PutBatch, packing their chunks
// into shared packs. Idempotent for identical content (casstore dedups).
func (s *CasStore) WriteBatch(ctx context.Context, items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	batch := make([]snapshot.BatchPutItem, len(items))
	for i, it := range items {
		batch[i] = snapshot.BatchPutItem{Key: sliceKey(it.ID), Meta: metaForClass(it.Class), Data: it.Data}
	}
	if _, err := s.cs.PutBatch(ctx, batch); err != nil {
		return fmt.Errorf("chunkstore: write batch (%d slices): %w", len(items), err)
	}
	return nil
}

// BatchReader is an optional Store capability: read many slices in ONE call so
// the backing can coalesce fetches of slices that share a casstore pack (far
// fewer backing/S3 GETs than one Read per slice — the cold sequential-read fix
// for model loading). concurrency bounds in-flight backing (pack) fetches
// (<=0 → backing default). It is BEST-EFFORT: a slice that can't be read is
// omitted from the result rather than failing the batch (the demand path
// re-fetches per slice and surfaces real errors). The sequential-read prefetcher
// uses it when the backing implements it.
// BatchReadStats aliases casstore's stats so cache/readahead can consume it
// through the chunkstore package without importing casstore directly.
type BatchReadStats = snapshot.BatchReadStats

type BatchReader interface {
	ReadBatch(ctx context.Context, ids []uint64, concurrency int) (map[uint64][]byte, BatchReadStats, error)
}

var _ BatchReader = (*CasStore)(nil)

// ReadBatch reads many slices via casstore's cross-key pack coalescing
// (GetLatestBatch): slices sharing a pack cost one pack GET, not one per slice.
// Returned bytes are per id; an unreadable slice is simply absent from the map.
// The BatchReadStats report the backing data GETs + bytes (the coalescing signal).
func (s *CasStore) ReadBatch(ctx context.Context, ids []uint64, concurrency int) (map[uint64][]byte, snapshot.BatchReadStats, error) {
	if len(ids) == 0 {
		return map[uint64][]byte{}, snapshot.BatchReadStats{}, nil
	}
	keys := make([]snapshot.SnapshotKey, len(ids))
	byKey := make(map[snapshot.SnapshotKey]uint64, len(ids))
	for i, id := range ids {
		k := sliceKey(id)
		keys[i] = k
		byKey[k] = id
	}
	res, stats, err := s.cs.GetLatestBatch(ctx, keys, concurrency)
	if err != nil {
		return nil, snapshot.BatchReadStats{}, fmt.Errorf("chunkstore: read batch (%d slices): %w", len(ids), err)
	}
	out := make(map[uint64][]byte, len(res))
	for k, b := range res {
		if id, ok := byKey[k]; ok {
			out[id] = b
		}
	}
	return out, stats, nil
}

// RegisterSlice writes slice id's manifest to reference an ORDERED list of
// already-existing casstore chunks, WITHOUT reading or writing any chunk/pack
// bytes. It is the mlfs (VFS→mlfs) half of the metadata-only bridge: a blob_ref's
// chunks (which already live in this tenant's pack set, same dedup domain) are laid
// into an mlfs slice purely by synthesising the slice's manifest over them.
//
// class selects the physical codec exactly as Write does, but since NO pack is
// written here it only stamps the manifest's metadata for consistency (the packs
// the chunks live in were already written by whoever ingested them). The chunks
// must be in original-stream order; the caller (the bridge) reads them from the
// source ref's manifest, which is already ordered.
func (s *CasStore) RegisterSlice(ctx context.Context, id uint64, chunks []snapshot.ChunkLocation, class StoreClass) error {
	if _, err := s.cs.RegisterManifest(ctx, sliceKey(id), metaForClass(class), chunks); err != nil {
		return fmt.Errorf("chunkstore: register slice %d: %w", id, err)
	}
	return nil
}

// SliceChunks returns the ordered casstore chunk list backing slice id's manifest,
// WITHOUT reading any chunk/pack bytes (it reads only the small manifest). It is
// the mlfs→VFS read half of the bridge: the coordinator gathers a file's slices'
// chunk lists to register a blob_ref over the SAME chunks. Returns an error
// (wrapping snapshot.ErrNoSnapshot) when the slice has no manifest.
func (s *CasStore) SliceChunks(ctx context.Context, id uint64) ([]snapshot.ChunkLocation, error) {
	locs, err := s.cs.ManifestChunks(ctx, sliceKey(id))
	if err != nil {
		return nil, fmt.Errorf("chunkstore: slice %d chunks: %w", id, err)
	}
	return locs, nil
}

func (s *CasStore) Read(ctx context.Context, id uint64) ([]byte, error) {
	rc, _, err := s.cs.GetLatest(ctx, sliceKey(id))
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return nil, fmt.Errorf("chunkstore: slice %d not found: %w", id, err)
		}
		return nil, fmt.Errorf("chunkstore: read slice %d: %w", id, err)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ReadAt is NOT the production read path: mlfs always mounts the durable
// write-back DiskCache in front (main.go), and DiskCache.ReadAt serves from a
// whole-slice Read + copy — it never calls this. This direct implementation
// exists only for cache-less callers (offline tools / tests), so it mirrors the
// cache's semantics rather than carrying a separate sequential-discard path.
//
// There is nothing finer than a whole-slice fetch to range to here: a FUSE slice
// is ≤1 MiB (the MaxWrite cap) and the casstore chunker targets ~1 MiB, so a
// slice is ~1 chunk (measured: 1 chunk ≤512 KiB, ~1.4 at 1 MiB), and a chunk is
// the atomic content-hash-verified unit. casstore's GetLatest reader already
// fetches only that chunk-run's bytes via a ranged pack GET (#18), so Read pulls
// just the covering chunk(s); we copy the requested window out of it.
func (s *CasStore) ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error) {
	data, err := s.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(data)) {
		return 0, nil // read at/after end of slice
	}
	return copy(p, data[off:]), nil
}

func (s *CasStore) Exists(ctx context.Context, id uint64) (bool, error) {
	_, err := s.cs.GetLatestMetadata(ctx, sliceKey(id))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, snapshot.ErrNoSnapshot) {
		return false, nil
	}
	return false, fmt.Errorf("chunkstore: exists slice %d: %w", id, err)
}

// WalkSliceIDs calls fn for each stored slice manifest, with the slice id and
// the manifest's creation time. GC uses this to find orphaned slices (those
// not in the live set) while respecting a safety window on young manifests.
// A slice with multiple versions yields fn once per version.
func (s *CasStore) WalkSliceIDs(ctx context.Context, fn func(id uint64, createdAt time.Time) error) error {
	return s.cs.Walk(ctx, func(key snapshot.SnapshotKey, _ string, m snapshot.SnapshotMetadata) error {
		id, ok := parseSliceKey(key.OwnerKey)
		if !ok {
			return nil // not a slice manifest (defensive; this store only writes s/<id>)
		}
		return fn(id, m.CreatedAt)
	})
}

// parseSliceKey is the inverse of sliceKey: "s/<id>" → id.
func parseSliceKey(ownerKey string) (uint64, bool) {
	rest, ok := strings.CutPrefix(ownerKey, "s/")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func (s *CasStore) Remove(ctx context.Context, id uint64) error {
	key := sliceKey(id)
	versions, err := s.cs.List(ctx, key)
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		return fmt.Errorf("chunkstore: remove slice %d: list: %w", id, err)
	}
	for _, v := range versions {
		if err := s.cs.Delete(ctx, key, v.Version); err != nil {
			return fmt.Errorf("chunkstore: remove slice %d: delete %s: %w", id, v.Version, err)
		}
	}
	return nil
}
