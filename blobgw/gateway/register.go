// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// ErrChunkNotInDomain is returned by RegisterRef when a supplied chunk is not
// present in the domain's pack index (pack_manifest). It signals a bridge bug or
// a cross-domain attempt: RegisterRef synthesises a manifest over EXISTING chunks
// and must never mint a ref that references bytes the domain doesn't hold.
var ErrChunkNotInDomain = errors.New("blobgw: chunk not present in domain pack index")

// ErrChunkPackMismatch is returned by RegisterRef when a supplied chunk hash is
// present in the domain's pack index but the caller-supplied PackRef (PackHash /
// Offset / Size) disagrees with the index's authoritative value. This would mint
// a manifest that binds the wrong pack position and is silently unreadable (the
// object body would fail casstore's per-chunk hash check on first read). The fix
// is to always source ChunkLocations from ManifestChunks or FileChunks — never
// to hand-construct PackRef values.
var ErrChunkPackMismatch = errors.New("blobgw: chunk PackRef disagrees with domain pack index")

// ChunkValidator reports whether chunk hashes are present in a dedup domain's
// pack index. It is exactly the read half of snapshot.DedupStore (pgindex.Store
// satisfies it), supplied to the Gateway so RegisterRef can prove every chunk it
// is asked to reference already exists in THIS domain before binding a ref to it.
// Optional: a Gateway without one skips validation (the caller vouches for the
// chunks), which suits tests and single-writer setups.
type ChunkValidator interface {
	LookupBatch(ctx context.Context, domain string, chunkHashes []string) (map[string]snapshot.PackRef, error)
}

// WithChunkValidator wires a ChunkValidator so RegisterRef validates that every
// supplied chunk exists in the domain's pack index before binding the ref. In
// production this is the same pgindex.Store that backs the dedup index, so the
// check reads the very pack_manifest rows casstore wrote. Pass nil (or omit) to
// skip validation.
func WithChunkValidator(v ChunkValidator) Option {
	return func(g *Gateway) { g.validator = v }
}

// RegisterRef binds ref to an object whose bytes ALREADY exist in this domain's
// casstore pack set, WITHOUT staging, chunking, or moving a single byte. It is the
// metadata-only analog of Finalize and the blobgw (mlfs→VFS) half of the bridge: a
// file written through mlfs is already chunked+packed+deduped in the tenant domain,
// so a blob_ref for it is pure manifest synthesis over the shared chunks.
//
// It (1) validates — when a ChunkValidator is wired — that every chunk exists in
// the domain's pack index, refusing to mint a dangling ref; (2) calls the casstore
// RegisterManifest primitive to write the ref's manifest referencing those existing
// chunks (no pack blob written); and (3) records the blob_ref with the supplied
// contentHash/size/contentType and the manifest's version. Re-registering an
// existing ref supersedes it, like a re-Put.
//
// contentHash MUST be the sha256 hex of the whole object (the mlfs node's
// content_hash, byte-identical to what a Put through this gateway would compute),
// and size its byte length; the bridge carries both from the source so the ref's
// metadata is consistent with the mlfs file it mirrors. chunks MUST be in
// original-stream order (the bridge reads them from the source manifest, which is
// already ordered). userMeta is attached durably like PutWithMeta.
//
// # content_hash trust boundary
//
// contentHash is accepted verbatim — RegisterRef does NOT re-hash the chunk bytes
// (that would defeat the zero-copy guarantee). A wrong contentHash at the call site
// is stored as-is and propagates to every downstream reader of the ref's metadata.
// The only enforcement is the caller's responsibility (MlfsToRef reads content_hash
// from the mlfs node, which mlfs stamped at flush time; RefToMlfs copies it from
// the ref). Callers that need end-to-end verification may call Bridge.VerifyRefContent
// after RegisterRef; it streams all bytes once (not zero-copy) and is opt-in only.
//
// # Concurrent-writer / serialization note (item 7)
//
// RegisterRef is intended as an OFFLINE/ADMIN operation: it is called by the bridge
// coordinator for a file whose mlfs side has been quiesced (fully written + hash
// stamped) before the bridge op. It is NOT designed for concurrent calls on the same
// ref key from multiple writers.
//
// Under a concurrent race (e.g. a RegisterRef and a normal Put/Finalize on the SAME
// ref key racing to completion), the manifeststore Put is not serialised with the
// refs.Put: two manifest versions could be written and GetLatest resolves the most
// recent one by version string ordering. The ref row is an upsert (last writer wins
// on CreatedAt). A reader that calls refs.Get + store.GetLatest in sequence may
// observe the ref row pointing at Version V1 while GetLatest returns Version V2
// (or vice versa), producing a metadata/body skew for that one read.
//
// This is acceptable because:
//  1. RegisterRef is admin/offline: no concurrent Put is expected on the same ref
//     during a bridge operation (the bridge holds the "quiesced" invariant).
//  2. If a race does occur, the skew is transient: the next refs.Put will update the
//     version pointer and subsequent reads are consistent again.
//  3. GetLatest always returns the LATEST manifest version, so the body bytes are
//     always correct; only the ref row's Version field can lag briefly.
//
// Callers that CANNOT guarantee quiescence must serialize RegisterRef and concurrent
// Puts externally (e.g. a per-ref advisory lock). This constraint is documented here
// rather than enforced, since the bridge use case always satisfies it naturally.
func (g *Gateway) RegisterRef(ctx context.Context, ref string, chunks []snapshot.ChunkLocation, contentHash string, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error) {
	if ref == "" {
		return ObjectInfo{}, ErrInvalidRef
	}
	if err := g.validateAndNormaliseChunks(ctx, chunks); err != nil {
		return ObjectInfo{}, err
	}
	// Metadata-only manifest synthesis: writes the manifest blob referencing the
	// existing pack chunks; no chunk/pack bytes are read or written.
	sm, err := g.store.RegisterManifest(ctx, g.key(ref), snapshot.SnapshotMetadata{
		Format: contentType,
		Tags:   encodeUserTags(userMeta),
	}, chunks)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw register-ref %q: register manifest: %w", ref, err)
	}
	info := ObjectInfo{
		Ref:         ref,
		Domain:      g.domain,
		ContentHash: contentHash,
		Size:        size,
		ContentType: contentType,
		CreatedAt:   g.now().UTC(),
		Version:     sm.Version,
		UserMeta:    cloneMeta(userMeta),
	}
	if err := g.refs.Put(ctx, info); err != nil {
		// Roll back the manifest version we just wrote so a retry does not leak an
		// orphaned manifest that pins its pack chunks forever. This mirrors the same
		// pattern Finalize uses on an integrity-check failure. Best-effort: a rollback
		// failure is logged but does not mask the original refs.Put error.
		if delErr := g.store.Delete(ctx, g.key(ref), sm.Version); delErr != nil {
			return ObjectInfo{}, fmt.Errorf("blobgw register-ref %q: ref store failed (%w) and manifest rollback also failed: %v", ref, err, delErr)
		}
		return ObjectInfo{}, fmt.Errorf("blobgw register-ref %q: ref store: %w", ref, err)
	}
	return info, nil
}

// RefChunks returns the ordered casstore chunk list backing ref's LATEST manifest,
// WITHOUT reading any chunk/pack bytes (only the small manifest). It is the read
// half of the VFS→mlfs bridge direction: the coordinator hands these chunks to the
// mlfs side to lay into an inode over the SAME chunks. Returns ErrNotFound for an
// unknown or still-pending ref.
func (g *Gateway) RefChunks(ctx context.Context, ref string) ([]snapshot.ChunkLocation, error) {
	info, ok, err := g.refs.Get(ctx, g.domain, ref)
	if err != nil {
		return nil, err
	}
	if !ok || info.Pending {
		return nil, ErrNotFound
	}
	locs, err := g.store.ManifestChunks(ctx, g.key(ref))
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("blobgw ref-chunks %q: %w", ref, err)
	}
	return locs, nil
}

// validateAndNormaliseChunks confirms every supplied chunk hash is present in
// the domain's pack index AND overwrites each chunk's PackRef with the index's
// authoritative value. Both checks are required for correctness:
//
//   - Presence check: a chunk absent from the index would produce a dangling ref
//     (ErrChunkNotInDomain). The caller must not mint a ref whose bytes the domain
//     does not hold.
//
//   - PackRef normalisation: LookupBatch returns the domain's authoritative
//     PackHash/Offset/Size for each chunk. We REPLACE the caller-supplied loc with
//     the index value before handing the slice to RegisterManifest. Accepting a
//     caller-supplied PackRef without verifying it would silently mint a manifest
//     that binds the WRONG pack position — the object body would fail casstore's
//     per-chunk hash check on first read (ErrCorrupt) yet RegisterRef would succeed.
//     Using the index value is strictly safer: the index owns pack_manifest, so its
//     PackRef is always correct (ErrChunkPackMismatch signals a caller bug or a
//     cross-domain attempt so callers can surface the right error message rather than
//     a silent accept-then-corrupt path, but the normalisation itself would also be
//     safe to do silently).
//
// It is a no-op when no ChunkValidator is wired (the caller vouches for the
// chunks, e.g. in tests or a single-writer admin path). chunks is modified
// in-place; the caller should not reuse the original slice after this returns nil.
func (g *Gateway) validateAndNormaliseChunks(ctx context.Context, chunks []snapshot.ChunkLocation) error {
	if g.validator == nil || len(chunks) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(chunks))
	seen := make(map[string]struct{}, len(chunks))
	for _, c := range chunks {
		if _, dup := seen[c.ChunkHash]; dup {
			continue
		}
		seen[c.ChunkHash] = struct{}{}
		hashes = append(hashes, c.ChunkHash)
	}
	found, err := g.validator.LookupBatch(ctx, g.domain, hashes)
	if err != nil {
		return fmt.Errorf("blobgw register-ref: validate chunks: %w", err)
	}
	for i, c := range chunks {
		authoritative, ok := found[c.ChunkHash]
		if !ok {
			return fmt.Errorf("blobgw register-ref: chunk %s absent in domain %q: %w", c.ChunkHash, g.domain, ErrChunkNotInDomain)
		}
		if c.PackRef != authoritative {
			return fmt.Errorf("blobgw register-ref: chunk %s PackRef mismatch: caller supplied {pack=%s off=%d size=%d} but index has {pack=%s off=%d size=%d}: %w",
				c.ChunkHash,
				c.PackRef.PackHash, c.PackRef.Offset, c.PackRef.Size,
				authoritative.PackHash, authoritative.Offset, authoritative.Size,
				ErrChunkPackMismatch)
		}
		// Overwrite with the authoritative value (currently a no-op after the check,
		// but makes the intent explicit: the manifest is always built from index truth).
		chunks[i].PackRef = authoritative
	}
	return nil
}
