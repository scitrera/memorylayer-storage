// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// LocalStack bundles a Gateway with the casstore pieces needed to operate it:
// the GC (which the caller schedules) and the dedup store. It is returned by
// NewLocalStack for development, tests, and single-node deployments.
type LocalStack struct {
	Gateway *Gateway
	GC      *snapshot.ChunkedGC
	Dedup   *snapshot.MemoryDedupStore
	Refs    *MemoryRefStore
	Staging *MemoryStagingStore
}

// NewLocalStack wires a Gateway over local-filesystem casstore backends:
// manifests under dir/manifests, chunks under dir/chunks, an in-memory global
// dedup index, and in-memory ref + staging stores. Suitable for `blobgw`'s
// dev mode and for tests; production wires S3 chunk storage plus the
// Postgres-backed dedup and ref stores instead.
//
// packTarget is the casstore pack target in bytes (0 = casstore default ~16
// MiB). domain is the dedup + ref namespace. Optional opts tune the stack; the
// zero-option form keeps the historical defaults.
func NewLocalStack(dir, domain string, packTarget int, opts ...LocalStackOption) (*LocalStack, error) {
	o := localStackOpts{}
	for _, opt := range opts {
		opt(&o)
	}
	upstream, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		return nil, fmt.Errorf("blobgw local stack: manifest store: %w", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{
		Root: filepath.Join(dir, "chunks"),
	})
	if err != nil {
		return nil, fmt.Errorf("blobgw local stack: chunk store: %w", err)
	}
	dedup := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: packTarget,
		DedupDomain:     domain,
		Index:           snapshot.NewGlobalIndex(dedup, nil),
		// Compress generic/text-like content by default; already-compressed and
		// GPU-loadable content types are stored uncompressed (decided from the
		// object's Content-Type, which Gateway.Put carries into the metadata).
		// Mirrors the production wiring in cmd/blobgw/main.go.
		CompressionPolicy: snapshot.NewContentTypePolicy(snapshot.CompressZstd),
		PackCompression:   o.packCompression,
	})
	if err != nil {
		return nil, fmt.Errorf("blobgw local stack: chunked store: %w", err)
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = dedup
	refs := NewMemoryRefStore()
	staging := NewMemoryStagingStore()
	gw := New(cs, refs, staging, domain)
	return &LocalStack{Gateway: gw, GC: gc, Dedup: dedup, Refs: refs, Staging: staging}, nil
}

// LocalStackOption tunes NewLocalStack.
type LocalStackOption func(*localStackOpts)

type localStackOpts struct {
	packCompression snapshot.PackCompressionMode
}

// WithLocalPackCompression selects the framing used for COMPRESSED packs:
// per-chunk (default, range-readable) or whole-pack (rollback). Packs the
// compression policy stores uncompressed are framed identically either way.
func WithLocalPackCompression(m snapshot.PackCompressionMode) LocalStackOption {
	return func(o *localStackOpts) { o.packCompression = m }
}
