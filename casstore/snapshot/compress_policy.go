// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import "strings"

// CompressionPolicy decides, per object, whether the ChunkedStore should
// compress that object's pack blobs and with which codec. The decision is made
// from the caller-supplied content type (SnapshotMetadata.Format, which blobgw
// populates from the HTTP Content-Type) plus an optional explicit override tag.
//
// The decision is per-PUT: every pack blob freshly created by one Put inherits
// that Put's codec. Deduped chunks that reference a pack created by an earlier
// Put are read back with whatever codec that pack was stored under — the codec
// is self-describing in the pack blob's header (see packCodecHeader), so a
// mixed-codec store reads correctly regardless of policy changes over time.
//
// Crucially, compression happens AFTER chunking and dedup-keying: chunk content
// hashes (and therefore dedup and pack identity) are always computed over the
// ORIGINAL bytes, so enabling compression never changes which chunks dedup
// against each other. Compression only shrinks the physical pack bytes written
// to the blob store.
type CompressionPolicy interface {
	// CodecFor returns the codec to apply to pack blobs for an object with the
	// given content type. Return CompressNone to store the object's packs
	// uncompressed. contentType is the raw value the caller provided (may
	// include parameters like "text/plain; charset=utf-8" or be empty).
	CodecFor(contentType string) CompressionAlgo
}

// TagStoreUncompressed is a SnapshotMetadata.Tags key callers set to force a
// store-uncompressed decision regardless of content type. Set it to any
// non-empty value to opt an object out of compression — the escape hatch for
// "GPU-loadable" payloads the caller knows must stay byte-for-byte on disk for
// zero-copy / mmap loads even when the content type doesn't reveal that. mlfs
// stamps it on its uncompressed (model/tensor) class slice writes so the
// uncompressed guarantee is a per-object property, not a global-policy default.
const TagStoreUncompressed = "store_uncompressed"

// ContentTypePolicy is the default CompressionPolicy. It compresses
// generic/text-like content with the configured codec (zstd by default) and
// stores already-compressed and GPU-loadable content classes uncompressed,
// because:
//
//   - Already-compressed bytes (zstd, gzip, png, jpeg, mp4, ...) do not shrink
//     further; re-compressing them only burns CPU and can even grow the blob.
//   - GPU-loadable tensor formats (.safetensors, .npy, .pt, ...) are loaded into
//     device memory via mmap / zero-copy. Storing them compressed would force a
//     decompress-to-temp on every load and defeat zero-copy entirely — this is
//     the "reserve-now, store uncompressed" content class the storage plan
//     calls out. Tensor files are also largely incompressible (dense fp16/bf16),
//     so there is little to gain anyway.
//
// The classification is deliberately small and conservative: anything not
// recognised as an uncompressed class is compressed (file/object data is
// arbitrary and usually benefits). Callers needing finer control can set the
// store_uncompressed tag per object or supply their own CompressionPolicy.
type ContentTypePolicy struct {
	// Codec is applied to content the policy decides to compress. The zero
	// value ("") is treated as CompressZstd. Set to CompressNone to make the
	// whole policy a no-op (compress nothing) — the rollback knob.
	Codec CompressionAlgo
}

// NewContentTypePolicy returns a ContentTypePolicy. codec is applied to
// compressible content; pass CompressNone to disable compression globally
// (every object stored uncompressed) as a rollback path.
func NewContentTypePolicy(codec CompressionAlgo) ContentTypePolicy {
	if codec == "" {
		codec = CompressZstd
	}
	return ContentTypePolicy{Codec: codec}
}

// CodecFor implements CompressionPolicy.
func (p ContentTypePolicy) CodecFor(contentType string) CompressionAlgo {
	if p.Codec == CompressNone {
		return CompressNone
	}
	codec := p.Codec
	if codec == "" {
		codec = CompressZstd
	}
	if isStoreUncompressedContentType(contentType) {
		return CompressNone
	}
	return codec
}

// isStoreUncompressedContentType reports whether contentType names a content
// class that must be stored uncompressed: either already-compressed media
// (recompression is wasted CPU) or a GPU-loadable tensor format (compression
// breaks zero-copy / mmap loads). Matching is case-insensitive and ignores any
// "; param" suffix.
func isStoreUncompressedContentType(contentType string) bool {
	ct := contentType
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.ToLower(strings.TrimSpace(ct))
	if ct == "" {
		// Unknown content type: compress by default (arbitrary bytes usually
		// benefit). mlfs file slices fall here and are compressed.
		return false
	}

	// Whole-type families that are inherently already-compressed.
	if strings.HasPrefix(ct, "image/") ||
		strings.HasPrefix(ct, "video/") ||
		strings.HasPrefix(ct, "audio/") {
		return true
	}

	// Specific already-compressed or GPU-loadable application types. SVG and
	// other text-ish image/* would be over-matched by the image/ prefix above,
	// but they are rare in this store and the cost of not compressing them is
	// negligible, so we keep the prefix rule simple.
	switch ct {
	case
		// Already-compressed container/codec formats.
		"application/zstd",
		"application/gzip",
		"application/x-gzip",
		"application/x-bzip2",
		"application/x-xz",
		"application/x-7z-compressed",
		"application/zip",
		"application/x-zstd",
		"application/x-lz4",
		// GPU-loadable / zero-copy tensor & array formats. Compressing these
		// breaks mmap/zero-copy device loads (the "reserve-now, store
		// uncompressed" class).
		"application/x-safetensors",
		"application/vnd.safetensors",
		"application/x-pytorch",
		"application/x-numpy",
		"application/x-npy",
		"application/x-npz",
		"application/octet-stream-tensor",
		"tensor/gpu-loadable":
		return true
	}
	return false
}
