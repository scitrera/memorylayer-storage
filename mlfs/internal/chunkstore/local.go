// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore

import (
	"context"
	"fmt"
	"path/filepath"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// Local bundles a CasStore with the casstore pieces behind it, for dev, tests,
// and single-node deployments: the GC (caller schedules it), the dedup index,
// and the chunk blob store (for inspecting physical blob counts).
type Local struct {
	Store  *CasStore
	GC     *snapshot.ChunkedGC
	Dedup  *snapshot.MemoryDedupStore
	Chunks blobstore.Storage
	// Compactor is the underlying casstore store, exposed so operator tooling
	// (mlfs-admin compact) can consolidate under-filled/small packs.
	Compactor *snapshot.ChunkedStore
}

// LocalOption tunes NewLocal.
type LocalOption func(*localOpts)

type localOpts struct {
	compression     snapshot.CompressionPolicy
	packCompression snapshot.PackCompressionMode
	meter           metric.Meter // nil = no backing-store instrumentation
}

// WithCompression overrides the local store's pack-blob compression policy
// (default: zstd via a ContentTypePolicy). Pass NewContentTypePolicy(CompressNone)
// — or rely on per-slice store_uncompressed tags — to store uncompressed. The
// per-slice StoreClass tag is always honored first regardless of this policy, so
// a compressing policy still keeps model/tensor slices uncompressed.
func WithCompression(p snapshot.CompressionPolicy) LocalOption {
	return func(o *localOpts) { o.compression = p }
}

// WithPackCompression selects the framing used for packs that ARE compressed:
// per-chunk (default, keeps compressed packs range-readable) or whole-pack (the
// rollback, better ratio but every chunk read fetches the whole pack). It has no
// effect on slices stored uncompressed — the model/tensor class carrying a
// store_uncompressed tag is framed identically either way.
func WithPackCompression(m snapshot.PackCompressionMode) LocalOption {
	return func(o *localOpts) { o.packCompression = m }
}

// WithMeter wraps the chunk blob store with the casstore backing-store OTEL
// adapter (obs.Wrap), so local-backend GET/PUT latency, errors, and bytes are
// recorded under backend="local". A nil meter (the default) skips wrapping. The
// dedup domain is attached as mlfs.domain so the series joins this daemon's
// identity stream.
func WithMeter(m metric.Meter) LocalOption {
	return func(o *localOpts) { o.meter = m }
}

// NewLocal wires a CasStore over local-filesystem casstore backends with a
// global dedup index (so identical slice content deduplicates). domain is the
// dedup domain; packTarget is the casstore pack target (0 = default).
func NewLocal(dir, domain string, packTarget int, opts ...LocalOption) (*Local, error) {
	// File data is arbitrary, so compress slices by default (zstd). mlfs slices
	// carry no content type, so the ContentTypePolicy sees an empty type and
	// compresses — the safe default for general file bytes. Already-compressed
	// media still chunks/dedups fine; we accept the minor CPU cost rather than
	// sniff file types at the slice layer. Files the bridge classifies as
	// model/tensor carry a per-slice store_uncompressed tag, which the policy
	// honors first, so they stay uncompressed even under this default. Rollback:
	// WithCompression(NewContentTypePolicy(CompressNone)).
	o := localOpts{compression: snapshot.NewContentTypePolicy(snapshot.CompressZstd)}
	for _, opt := range opts {
		opt(&o)
	}
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		return nil, fmt.Errorf("chunkstore local: manifest store: %w", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		return nil, fmt.Errorf("chunkstore local: chunk store: %w", err)
	}
	// Backing-store RED: wrap the blob store so its GET/PUT latency, errors, and
	// bytes are recorded (otherwise dark). The ChunkedStore + GC both read through
	// the wrapped handle, so every backing op is counted once.
	if o.meter != nil {
		wrapped, werr := obs.Wrap(chunks, o.meter, "local", attribute.String("mlfs.domain", domain))
		if werr != nil {
			return nil, fmt.Errorf("chunkstore local: wrap backing metrics: %w", werr)
		}
		chunks = wrapped
	}
	dedup := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes:   packTarget,
		DedupDomain:       domain,
		Index:             snapshot.NewGlobalIndex(dedup, nil),
		CompressionPolicy: o.compression,
		PackCompression:   o.packCompression,
	})
	if err != nil {
		return nil, fmt.Errorf("chunkstore local: chunked store: %w", err)
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = dedup
	return &Local{Store: New(cs), GC: gc, Dedup: dedup, Chunks: chunks, Compactor: cs}, nil
}
