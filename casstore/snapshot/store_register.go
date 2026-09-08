// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// RegisterManifest writes a v2 manifest for key that references an ORDERED list
// of already-existing pack chunks, WITHOUT reading or writing a single chunk or
// pack blob. It is the metadata-only manifest-synthesis primitive the mlfs↔VFS
// bridge is built on: a file already chunked+packed under one casstore key (an
// mlfs slice, a blobgw ref) can be re-referenced under a NEW key purely by
// synthesising a manifest that points at the same pack_manifest chunks. Dedup is
// preserved by construction — no new pack blob is emitted, so the new key and the
// original share the exact same physical bytes.
//
// # Byte-identical manifest guarantee
//
// The manifest this produces is structurally IDENTICAL to what Put would have
// written for the same ordered chunks: it is the same chunkManifest struct
// (Format = manifestFormatV2, Tenant = the effective dedup domain, one chunkRef
// per input chunk carrying Hash/PackHash/Offset/Size), marshalled by the same
// writeManifest path Put uses. Every field a chunkRef carries after a Put is set
// here from the ChunkLocation, in the same encoding, so the existing read path
// (openManifest → chunkStreamReader) reassembles it byte-for-byte and each
// chunk's content-hash check passes exactly as for a Put-authored manifest. The
// only bytes written are the manifest blob itself (in the upstream SnapshotStore);
// the chunk/pack blobstore is never touched.
//
// # Ordering and integrity
//
// chunks MUST be supplied in original-stream order: the reader concatenates them
// in manifest order to recover the object, so the caller is responsible for
// preserving order (both bridge directions read an existing ordered manifest, so
// order is inherited, never invented). The manifest's TotalSize is computed as the
// sum of the chunk sizes; a caller that also knows the object's whole-content size
// should assert it equals that sum before calling (the bridge does).
//
// The returned SnapshotMetadata carries the upstream-generated Version and, as for
// any Put, a SizeBytes equal to the MANIFEST blob's byte length (not the object's
// content length — that is the manifest's TotalSize, recovered on read). Callers
// that need the object length track it out of band (blob_ref.size_b / node.length).
//
// The chunks are trusted to exist in the domain's pack set (they came from an
// existing manifest in the SAME dedup domain); this primitive does not re-verify
// their presence — that is the bridge coordinator's responsibility (it validates
// against pack_manifest before calling here). A zero-length chunks slice writes an
// empty-object manifest (TotalSize 0), which the reader restores as zero bytes.
func (c *ChunkedStore) RegisterManifest(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, chunks []ChunkLocation) (SnapshotMetadata, error) {
	domain := c.domainFor(key)
	m := chunkManifest{
		Format: manifestFormatV2,
		Tenant: domain,
		Chunks: make([]chunkRef, len(chunks)),
	}
	for i, loc := range chunks {
		if loc.PackHash == "" {
			return SnapshotMetadata{}, fmt.Errorf("register manifest: chunk %d (%s) has empty pack hash — only v2 pack chunks can be registered", i, loc.ChunkHash)
		}
		if loc.Size <= 0 || loc.Offset < 0 {
			// Reject Size==0 chunks as a defence-in-depth measure: casstore never emits
			// zero-size chunks, so a zero Size here indicates a caller bug. Allowing it
			// would create boundary-attribution ambiguity in chunksForWindow (bridgefs.go)
			// where a chunk with Size==0 has no clear window assignment.
			return SnapshotMetadata{}, fmt.Errorf("register manifest: chunk %d (%s) has invalid offset/size (offset=%d size=%d); size must be > 0", i, loc.ChunkHash, loc.Offset, loc.Size)
		}
		m.Chunks[i] = chunkRef{
			Hash:     loc.ChunkHash,
			PackHash: loc.PackHash,
			Offset:   loc.Offset,
			Size:     loc.Size,
		}
		m.TotalSize += int64(loc.Size)
	}
	return c.writeManifest(ctx, key, meta, &m, manifestFormatV2)
}

// ManifestChunks returns the ordered chunk list backing the LATEST manifest for
// key, WITHOUT reading any chunk/pack bytes — it reads only the (small) manifest
// blob. It is the read half of the bridge: the mlfs→VFS direction reads an mlfs
// slice's chunk list to hand to blobgw RegisterRef, and the VFS→mlfs direction
// reads a ref's chunk list to lay into mlfs slices.
//
// The returned ChunkLocations are in original-stream order and carry each chunk's
// (ChunkHash, PackHash, Offset, Size) — everything RegisterManifest needs to
// reproduce the same manifest elsewhere. v1 (legacy, chunk-per-blob) manifests
// are rejected: a v1 chunk blob cannot be re-referenced inside a v2 pack manifest
// without re-reading its bytes, which would violate the zero-byte invariant.
// Returns ErrNoSnapshot when no snapshot exists for key.
func (c *ChunkedStore) ManifestChunks(ctx context.Context, key SnapshotKey) ([]ChunkLocation, error) {
	rc, _, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return nil, err // includes ErrNoSnapshot
	}
	manifestBytes, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		return nil, fmt.Errorf("manifest chunks: read manifest: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("manifest chunks: close manifest: %w", closeErr)
	}
	var m chunkManifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return nil, fmt.Errorf("manifest chunks: parse manifest: %w", err)
	}
	switch m.Format {
	case manifestFormatV2:
		// expected
	case manifestFormatV1:
		return nil, fmt.Errorf("manifest chunks: key %v is a v1 (chunk-per-blob) manifest; only v2 pack manifests can be bridged", key)
	default:
		return nil, fmt.Errorf("manifest chunks: not a chunked manifest (format=%q)", m.Format)
	}
	out := make([]ChunkLocation, len(m.Chunks))
	for i, ref := range m.Chunks {
		if ref.PackHash == "" {
			return nil, fmt.Errorf("manifest chunks: key %v chunk %d (%s) has empty pack hash (v1-normalised); cannot bridge", key, i, ref.Hash)
		}
		out[i] = ChunkLocation{
			ChunkHash: ref.Hash,
			PackRef:   PackRef{PackHash: ref.PackHash, Offset: ref.Offset, Size: ref.Size},
		}
	}
	return out, nil
}
