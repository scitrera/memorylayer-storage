// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/sharded"
)

// LocalConfig configures the filesystem-backed blob store. Mirrors the
// fields sandbox-provider operators actually use; advanced kopia options
// (file uid/gid override, throttling) are accessible by constructing the
// underlying filesystem.Options directly if needed.
type LocalConfig struct {
	// Root is the directory under which all blobs are stored. Created
	// (recursively) if absent. Required.
	Root string

	// DirectoryShards controls how many path segments are used to bucket
	// blobs. nil → default 1-level sharding by first byte (matches the
	// "<hash[:2]>/<hash>" pattern from the original BlobStore design).
	DirectoryShards []int

	// FileMode is the chmod mode for written blobs. 0 → kopia default.
	FileMode os.FileMode
}

// NewLocalStorage opens a filesystem-backed blob store under cfg.Root.
// The directory is created if it doesn't exist; this matches the
// existing snapshot.NewLocalStore convention.
func NewLocalStorage(ctx context.Context, cfg LocalConfig) (Storage, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("blobstore: LocalConfig.Root is required")
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, fmt.Errorf("blobstore: mkdir local root %q: %w", cfg.Root, err)
	}
	// Resolve to absolute so the on-disk paths kopia constructs are stable
	// across cwd changes.
	abs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("blobstore: resolve local root: %w", err)
	}
	shards := cfg.DirectoryShards
	if shards == nil {
		shards = []int{2} // 1-level sharding: aa/aabbccdd...
	}
	opts := &filesystem.Options{
		Path:     abs,
		FileMode: cfg.FileMode,
		Options: sharded.Options{
			DirectoryShards: shards,
		},
	}
	st, err := filesystem.New(ctx, opts, true /* isCreate */)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open filesystem backend: %w", err)
	}
	return st, nil
}
