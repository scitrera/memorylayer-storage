// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore

import (
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore/obs"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteblob"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remotepresign"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"
)

// Remote bundles a CasStore wired over the converged direct-S3 node data path:
// a global dedup index served by blobgw over NATS, presigned-URL S3 transfers,
// and an injected manifest store. Unlike Local it carries NO GC, dedup, or
// chunk-store handles: garbage collection is blobgw-owned (it holds the tenant
// credentials and the authoritative index), so the node never constructs one.
// Downstream consumers (cache→fileio→fuse) take *CasStore through the
// chunkstore.Store interface and are untouched by the remote vs local choice.
type Remote struct {
	Store *CasStore
}

// RemoteConfig configures NewRemote.
//
// Tenant is the casstore dedup domain (the blob-ID prefix and dedup isolation
// boundary) and the NATS subject tenant; it must be a valid NATS subject token
// (see ctlproto.ValidateTenant). Requester is the shared NATS transport that
// feeds BOTH the remote dedup index and the presign client. HTTP is the
// node-side S3 transfer client. Manifests is the snapshot manifest store,
// injected by the caller (cmd wires the PG manifeststore; tests may pass a
// local store) so this constructor stays storage-agnostic and PG-free. Logger
// is optional (passed to the GlobalIndex for best-effort dedup diagnostics).
// PresignTTL, when >0, overrides the requested presigned-URL validity.
type RemoteConfig struct {
	Tenant          string
	PackTargetBytes int
	Requester       remoteindex.Requester
	HTTP            *s3http.Client
	Manifests       snapshot.SnapshotStore
	Logger          *slog.Logger
	PresignTTL      time.Duration

	// Compression selects the casstore pack-blob compression policy. A nil
	// policy uses the remote default: UNCOMPRESSED (see NewRemote) — the correct
	// choice for the converged design's primary workload (large, largely
	// incompressible model weights such as .pt/.safetensors), because it
	// preserves mmap/zero-copy device loads per ADR-001 §6. Set it to
	// snapshot.NewContentTypePolicy(snapshot.CompressZstd) to opt compressible
	// workloads (e.g. workclaw scratch) back into zstd.
	//
	// Compressing no longer costs ranged reads: per-chunk pack framing (the
	// PackCompression default) keeps compressed packs range-readable, so the old
	// "compressed ⇒ whole-pack fetch" tradeoff is gone. Zero-copy mmap remains the
	// reason the model/tensor class stays uncompressed.
	Compression snapshot.CompressionPolicy

	// PackCompression selects the framing for packs that ARE compressed:
	// per-chunk (zero value, range-readable) or whole-pack (rollback). It has no
	// effect under the uncompressed default above — an uncompressed pack is framed
	// identically either way.
	PackCompression snapshot.PackCompressionMode

	// Meter, when non-nil, wraps the direct-S3 blob store with the casstore
	// backing-store OTEL adapter (obs.Wrap, backend="s3"), so cold-read and
	// write-back S3 GET/PUT latency, errors, and bytes are recorded — the RED view
	// that is otherwise dark on the converged path. The tenant is attached as
	// mlfs.domain so the series joins this daemon's identity stream.
	Meter metric.Meter
}

// NewRemote wires a CasStore over the remote (direct-S3) backends with a global
// dedup index so identical slice content deduplicates across distinct slice ids
// via blobgw's authoritative index. It constructs no GC: pack reclamation is a
// blobgw concern (see Remote).
func NewRemote(cfg RemoteConfig) (*Remote, error) {
	if cfg.Tenant == "" {
		return nil, fmt.Errorf("chunkstore remote: Tenant must not be empty")
	}
	if cfg.Requester == nil {
		return nil, fmt.Errorf("chunkstore remote: Requester must not be nil")
	}
	if cfg.HTTP == nil {
		return nil, fmt.Errorf("chunkstore remote: HTTP must not be nil")
	}
	if cfg.Manifests == nil {
		return nil, fmt.Errorf("chunkstore remote: Manifests must not be nil")
	}

	// The dedup index and presign client share the one NATS Requester (the
	// transport is multiplexed by subject — index.lookup/record vs presign).
	dedup := remoteindex.NewRemoteDedupStore(cfg.Requester)
	presign := remotepresign.NewClient(cfg.Requester)

	var blobOpts []remoteblob.Option
	if cfg.PresignTTL > 0 {
		blobOpts = append(blobOpts, remoteblob.WithPresignTTL(cfg.PresignTTL))
	}
	var chunks blobstore.Storage = remoteblob.NewBlobStore(presign, cfg.HTTP, cfg.Tenant, blobOpts...)
	// Backing-store RED: wrap the direct-S3 blob store so its GET/PUT latency,
	// errors, and bytes are recorded under backend="s3". The ChunkedStore reads
	// through the wrapped handle, so cold reads and write-back uploads are counted.
	if cfg.Meter != nil {
		wrapped, werr := obs.Wrap(chunks, cfg.Meter, "s3", attribute.String("mlfs.domain", cfg.Tenant))
		if werr != nil {
			return nil, fmt.Errorf("chunkstore remote: wrap backing metrics: %w", werr)
		}
		chunks = wrapped
	}

	// Remote default: store slices UNCOMPRESSED. The converged path's primary
	// payload is large, largely-incompressible model weights (.pt/.safetensors);
	// storing them uncompressed preserves mmap/zero-copy device loads per
	// ADR-001 §6, and they would barely shrink anyway. Unlike NewLocal
	// (dev/single-node, kept zstd), the remote path optimizes for the model-store
	// workload. Callers opt compressible workloads back into zstd via
	// RemoteConfig.Compression — which no longer forfeits ranged reads, since
	// per-chunk pack framing keeps compressed packs range-readable.
	compression := cfg.Compression
	if compression == nil {
		compression = snapshot.NewContentTypePolicy(snapshot.CompressNone)
	}

	cs, err := snapshot.NewChunkedStore(cfg.Manifests, chunks, snapshot.ChunkedConfig{
		PackTargetBytes:   cfg.PackTargetBytes,
		DedupDomain:       cfg.Tenant,
		Index:             snapshot.NewGlobalIndex(dedup, cfg.Logger),
		CompressionPolicy: compression,
		PackCompression:   cfg.PackCompression,
	})
	if err != nil {
		return nil, fmt.Errorf("chunkstore remote: chunked store: %w", err)
	}
	return &Remote{Store: New(cs)}, nil
}
