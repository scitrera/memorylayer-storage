// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package bridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/pgindex"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/manifeststore"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// ErrSplitManifestStore is returned when a Stack is constructed (or validated)
// with different manifest stores for the blobgw side and the mlfs side. The
// correctness of GC depends on a SINGLE manifest upstream that both sides share:
// ChunkedGC marks chunks live by walking ALL manifests it can see; if blobgw ref
// manifests live in a different store than mlfs slice manifests, a GC pass that
// walks only one store will reclaim packs the other side still references —
// silent data loss. NewStack enforces this structurally (one manifests pointer),
// and AssertGCCoversAllManifests exposes the same check for callers that build
// their own Stack components outside NewStack.
var ErrSplitManifestStore = errors.New("bridge: blobgw and mlfs sides must share the same manifest upstream for GC correctness; split manifest stores would cause silent data loss")

// Stack is a fully-wired bridge over a SINGLE shared Postgres and a SINGLE shared
// chunk blobstore, exactly modelling the production invariant: mlfs mount domain ==
// blobgw ref domain == tenant domain, both over the same PG. It is what the bridge
// CLI subcommand and the end-to-end integration test both construct, so the code
// path they exercise is the real one.
//
// Both sides' ChunkedStores share the ONE chunk blobstore and the ONE pgindex
// (which backs both blobgw's blob_ref/pack_manifest AND mlfs's dedup lookups), so a
// chunk mlfs writes is immediately visible to blobgw's RegisterRef validation via
// the same pack_manifest rows — the whole point of the bridge.
//
// GC correctness: the shared GC walks the SINGLE manifeststore that backs BOTH sides.
// This is the only configuration where GC is correct: if blobgw ref manifests and
// mlfs slice manifests lived in SEPARATE stores, a GC pass that only walks one store
// would reclaim packs the other side still references. NewStack enforces this
// structurally; call AssertGCCoversAllManifests to verify at any point.
type Stack struct {
	Bridge   *Bridge
	Gateway  *gateway.Gateway
	FS       *FS
	Engine   *meta.Engine
	PGIndex  *pgindex.Store
	Manifest *manifeststore.Store
	Chunks   blobstore.Storage
	// SliceStore is the mlfs-side chunk store (over the shared casstore) the FS
	// registers slices into; the data path (fileio) reads through the SAME store, so
	// a bridged slice is immediately readable. Exposed for callers/tests that drive
	// the mlfs data path directly.
	SliceStore *chunkstore.CasStore

	// GC is the shared casstore GC (index-aware) over the one chunk store; a caller
	// may schedule it. Both sides' chunks live here, so one GC reclaims for both.
	GC *snapshot.ChunkedGC

	// gcManifest is the manifest store the GC was given. It is compared against
	// Manifest (both sides' upstream) by AssertGCCoversAllManifests to detect a
	// split-manifest misconfiguration that would cause silent data loss under GC.
	gcManifest *manifeststore.Store
}

// AssertGCCoversAllManifests returns ErrSplitManifestStore if the stack's GC is
// not wired with the same manifest store that backs both the blobgw and mlfs sides.
// Call this after constructing a Stack (especially outside NewStack) to fail-close
// rather than silently misconfiguring GC. NewStack always passes; a hand-assembled
// Stack with split manifest stores fails.
func (s *Stack) AssertGCCoversAllManifests() error {
	if s.gcManifest != s.Manifest {
		return ErrSplitManifestStore
	}
	return nil
}

// StackConfig configures NewStack.
type StackConfig struct {
	// DB is the shared Postgres pool. It backs mlfs's meta engine + slice_manifest,
	// and blobgw's blob_ref + pack_manifest — one datastore, the tenant domain.
	DB *sql.DB
	// ChunkDir is the local-filesystem directory for the shared chunk blobstore.
	// In production this is S3; a local dir keeps the CLI/test self-contained while
	// exercising the identical ChunkedStore/dedup/GC code. Ignored when Chunks is set.
	ChunkDir string
	// Chunks, when non-nil, is the shared chunk blobstore to use directly instead of
	// constructing a local one from ChunkDir. Tests inject an instrumented store here
	// to assert the bridge moves ZERO chunk bytes; production leaves it nil.
	Chunks blobstore.Storage
	// Domain is the tenant domain shared by both sides (dedup domain, blob ref
	// namespace, mlfs mount domain). Must be identical for the bridge to operate.
	Domain string
	// PackTargetBytes is the casstore pack target (0 = default ~16 MiB); both sides
	// must agree so chunk identity is consistent.
	PackTargetBytes int
	// Region is the mlfs inode-allocation region prefix for the meta engine.
	Region uint8
}

// NewStack wires both bridge sides over the shared PG + chunk store and returns the
// coordinator plus the pieces a caller may need (GC, stores). It migrates every
// schema (mlfs meta, slice_manifest, blob_ref/pack_manifest) so a fresh database is
// ready. The caller owns the DB pool lifecycle.
func NewStack(ctx context.Context, cfg StackConfig) (*Stack, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("bridge stack: nil DB")
	}
	if cfg.Domain == "" {
		return nil, fmt.Errorf("bridge stack: empty domain")
	}

	// mlfs metadata engine (node/edge/slice_ref/…).
	engine := meta.Open(cfg.DB, cfg.Region)
	if err := engine.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("bridge stack: migrate meta: %w", err)
	}
	// mlfs slice-manifest store (casstore upstream for slice manifests).
	if err := manifeststore.Migrate(ctx, cfg.DB); err != nil {
		return nil, fmt.Errorf("bridge stack: migrate slice_manifest: %w", err)
	}
	manifests := manifeststore.New(cfg.DB, nil)

	// blobgw index: blob_ref (RefStore) + pack_manifest (DedupStore), one table set.
	pg := pgindex.New(cfg.DB)
	if err := pg.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("bridge stack: migrate pgindex: %w", err)
	}

	// The ONE shared chunk blobstore both sides read/write. Tests may inject an
	// instrumented store (cfg.Chunks) to prove the bridge moves zero chunk bytes.
	chunks := cfg.Chunks
	if chunks == nil {
		var cerr error
		chunks, cerr = blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: filepath.Join(cfg.ChunkDir, "chunks")})
		if cerr != nil {
			return nil, fmt.Errorf("bridge stack: chunk store: %w", cerr)
		}
	}

	// The ONE shared dedup index (pack_manifest via pgindex) both ChunkedStores use,
	// so a chunk written by mlfs is visible to blobgw and vice versa.
	index := snapshot.NewGlobalIndex(pg, nil)

	// blobgw side: casstore ChunkedStore over the mlfs... no — blobgw refs are
	// manifests too, but they need their OWN manifest upstream (blob_ref manifests
	// are distinct casstore keys from slice manifests). We store blobgw's ref
	// manifests in the SAME slice_manifest table via manifeststore: the OwnerKey
	// namespaces differ ("s/<id>" for slices, the ref string for refs), so they
	// coexist. Both sides therefore share the manifest upstream AND the chunk store
	// AND the dedup index — one substrate, two logical namespaces.
	blobCS, err := snapshot.NewChunkedStore(manifests, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: cfg.PackTargetBytes,
		DedupDomain:     cfg.Domain,
		Index:           index,
	})
	if err != nil {
		return nil, fmt.Errorf("bridge stack: blobgw chunked store: %w", err)
	}
	gw := gateway.New(blobCS, pg, nil, cfg.Domain, gateway.WithChunkValidator(pg))

	// mlfs side: its own ChunkedStore over the SAME manifests + chunks + index.
	mlfsCS, err := snapshot.NewChunkedStore(manifests, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: cfg.PackTargetBytes,
		DedupDomain:     cfg.Domain,
		Index:           index,
	})
	if err != nil {
		return nil, fmt.Errorf("bridge stack: mlfs chunked store: %w", err)
	}
	sliceStore := chunkstore.New(mlfsCS)
	fs := New(engine, sliceStore)

	br, err := NewBridge(fs, gw, cfg.Domain)
	if err != nil {
		return nil, err
	}

	gc := snapshot.NewChunkedGC(manifests, chunks, nil)
	gc.Index = pg

	st := &Stack{
		Bridge:     br,
		Gateway:    gw,
		FS:         fs,
		Engine:     engine,
		PGIndex:    pg,
		Manifest:   manifests,
		Chunks:     chunks,
		SliceStore: sliceStore,
		GC:         gc,
		gcManifest: manifests, // same pointer as Manifest → AssertGCCoversAllManifests passes
	}
	// Structural safety: NewStack constructs both sides from the same manifests
	// pointer, so this assertion always passes here. It is a canary: if a future
	// refactor accidentally splits the manifest stores, this will surface the bug
	// immediately rather than letting a GC pass silently reclaim live chunks.
	if err := st.AssertGCCoversAllManifests(); err != nil {
		return nil, fmt.Errorf("bridge stack: internal invariant violated: %w", err)
	}
	return st, nil
}

// Close releases the shared chunk blobstore. The DB pool is owned by the caller.
func (s *Stack) Close(ctx context.Context) error {
	if s.Chunks != nil {
		return s.Chunks.Close(ctx)
	}
	return nil
}
