// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// SetDefaultStoreClass sets the compression class applied to a created file whose
// type is not recognized as a model/tensor. cmd/mlfs wires it from a flag: a
// model-store mount passes ClassUncompressed (so even unrecognized files stay
// uncompressed — range-readable + mmap-safe); a mixed-content mount leaves the
// default ClassDefault so generic data compresses. Call once before serving.
func (b *Bridge) SetDefaultStoreClass(class uint8) { b.defaultStoreClass = class }

// modelExtensions are file types that must be stored uncompressed: tensor /
// model-weight formats loaded into device memory via mmap / zero-copy, where a
// compressed-at-rest pack would force a host decompress hop and defeat ranged
// reads. The list is the semantic signal (extension), which — unlike an entropy
// test — correctly keeps a compressible-but-tensor file uncompressed. Magic-byte
// sniffing for extension-less / .bin shards is a later refinement.
var modelExtensions = map[string]bool{
	".safetensors": true,
	".pt":          true,
	".pth":         true,
	".bin":         true, // HF pytorch_model.bin / consolidated.bin shards
	".npy":         true,
	".npz":         true,
	".gguf":        true,
	".ggml":        true,
	".onnx":        true,
	".ckpt":        true,
}

// classifyStoreClass picks a file's compression class from its name: a recognized
// model/tensor extension is ClassUncompressed; anything else takes the mount's
// default (dflt). Returned as the chunkstore.StoreClass numeric value (stored as a
// uint8 on the inode).
func classifyStoreClass(name string, dflt uint8) uint8 {
	ext := strings.ToLower(filepath.Ext(name))
	if modelExtensions[ext] {
		return uint8(chunkstore.ClassUncompressed)
	}
	return dflt
}

// storeClassFor returns inode's compression class for the write path, preferring
// the in-memory cache and falling back to a single metadata read (then caching
// it). A read failure degrades to ClassDefault rather than failing the write —
// the class only selects a codec, never correctness.
func (b *Bridge) storeClassFor(ctx context.Context, ino meta.Ino) chunkstore.StoreClass {
	if v, ok := b.classCache.Load(uint64(ino)); ok {
		return chunkstore.StoreClass(v.(uint8))
	}
	a, st := b.meta.GetAttr(ctx, ino)
	if st != 0 || a == nil {
		return chunkstore.ClassDefault
	}
	b.classCache.Store(uint64(ino), a.StoreClass)
	return chunkstore.StoreClass(a.StoreClass)
}
