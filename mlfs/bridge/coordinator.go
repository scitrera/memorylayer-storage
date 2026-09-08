// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// ErrContentHashMismatch is returned by VerifyContent when the sha256 computed by
// streaming all chunks of a ref or mlfs file disagrees with its declared content_hash.
// It always wraps a detail string with the expected and computed values.
var ErrContentHashMismatch = errors.New("bridge: content hash mismatch")

// Bridge is the mlfs↔VFS coordinator: it drives both directions of the
// metadata-only bridge over a shared casstore domain, moving files between an mlfs
// filesystem and a blobgw object gateway WITHOUT reading or writing a single file
// byte. It owns the hard cross-side invariant — mlfs mount domain == blobgw ref
// domain == the tenant domain, both over the same Postgres — and refuses to operate
// when the two sides disagree on the domain.
//
// Both directions are pure metadata synthesis over the SAME casstore pack_manifest
// chunks: MlfsToRef reads an mlfs file's chunk list and registers a blob_ref over
// it; RefToMlfs reads a ref's chunk list and lays it into a new mlfs inode. The ref
// and the mlfs file end up referencing the identical packs — dedup preserved, no new
// bytes, and each reads back byte-identical to the other.
//
// # content_hash trust boundary
//
// The bridge carries content_hash VERBATIM from source to destination (mlfs node
// content_hash → ref content_hash in MlfsToRef; ref content_hash → mlfs node content_hash
// in RefToMlfs). It does NOT re-hash the object bytes by default — that would require
// reading every chunk, defeating the zero-copy guarantee. The bridge therefore TRUSTS
// that the source's content_hash is correct. Consequences:
//
//   - A stale or wrong content_hash at the source propagates to the destination. The
//     only downstream effect is a wrong ETag/checksum on the ref side; the bytes are
//     still served correctly (they come from the chunks, not the hash field).
//   - Callers that need end-to-end hash verification (e.g. a one-time admin repair
//     pass) may call VerifyContent after bridging; it streams all chunks once and
//     asserts the sha256 matches the declared hash. This is the only exception to
//     zero-copy and is opt-in.
//
// # Single-writer / quiescence requirement
//
// MlfsToRef reads content_hash, size, and chunk list across THREE separate
// non-transactional metadata reads (ContentHashAndSize → FileChunks). A concurrent
// whole-slice overwrite mid-bridge could produce an inconsistent snapshot: the hash
// from one version, the chunks from another. Callers MUST ensure the file is
// quiesced (no concurrent writers) for the duration of MlfsToRef. RefToMlfs is safe
// under concurrent readers (it creates a new inode atomically before slices are
// registered) but the directory parent must not be concurrently deleted. For the
// typical admin/maintenance use case (the file has been finalised and its hash
// stamped before bridging) this requirement is naturally satisfied.
type Bridge struct {
	fs     *FS
	gw     *gateway.Gateway
	domain string
}

// NewBridge constructs the coordinator over an mlfs bridge surface and a blobgw
// gateway that MUST share the dedup/ref domain. It returns an error if the gateway's
// domain differs from the supplied tenant domain, since a cross-domain bridge would
// reference chunks the other side's domain does not hold (the chunks are isolated by
// domain in pack_manifest).
func NewBridge(fs *FS, gw *gateway.Gateway, domain string) (*Bridge, error) {
	if fs == nil || gw == nil {
		return nil, fmt.Errorf("bridge: nil fs or gateway")
	}
	if gw.Domain() != domain {
		return nil, fmt.Errorf("bridge: domain mismatch: gateway domain %q != tenant domain %q (mlfs mount domain, blobgw ref domain, and tenant domain must be identical)", gw.Domain(), domain)
	}
	return &Bridge{fs: fs, gw: gw, domain: domain}, nil
}

// MlfsToRef registers a blob_ref named ref for the mlfs file at path, WITHOUT moving
// any bytes: it reads the file's whole-file content hash + size and its ordered
// casstore chunk list, then binds the ref over the SAME chunks via the gateway's
// metadata-only RegisterRef. The ref is immediately readable through blobgw (and an
// edge presigned download URL) and reads back byte-identical to the mlfs file.
//
// The file MUST have a persisted content hash (mlfs stamps it on flush/close, C0);
// a file never flushed with a hash is refused, because the ref's content_hash must
// be authoritative for client-side idempotency/dedup. contentType is carried onto
// the ref's manifest (drives casstore's compression policy for any FUTURE writes to
// the ref; the existing packs are untouched). userMeta is attached durably.
func (b *Bridge) MlfsToRef(ctx context.Context, path, ref, contentType string, userMeta map[string]string) (gateway.ObjectInfo, error) {
	ino, err := b.fs.Resolve(ctx, path)
	if err != nil {
		return gateway.ObjectInfo{}, err
	}
	contentHash, size, err := b.fs.ContentHashAndSize(ctx, ino)
	if err != nil {
		return gateway.ObjectInfo{}, err
	}
	if contentHash == "" {
		return gateway.ObjectInfo{}, fmt.Errorf("bridge: mlfs file %q has no content hash (not yet flushed); cannot register a ref without an authoritative content hash", path)
	}
	chunks, err := b.fs.FileChunks(ctx, ino)
	if err != nil {
		return gateway.ObjectInfo{}, err
	}
	// Defence-in-depth: the chunk sizes must sum to the file's declared length, or
	// the ref's content_hash would not match its bytes.
	if got := sumChunkSizes(chunks); got != size {
		return gateway.ObjectInfo{}, fmt.Errorf("bridge: mlfs file %q chunk sizes sum to %d but file length is %d", path, got, size)
	}
	info, err := b.gw.RegisterRef(ctx, ref, chunks, contentHash, size, contentType, userMeta)
	if err != nil {
		return gateway.ObjectInfo{}, err
	}
	return info, nil
}

// RefToMlfs creates an mlfs file at path from the blob_ref ref, WITHOUT moving any
// bytes: it reads the ref's metadata (content hash + size) and its ordered casstore
// chunk list, then lays those chunks into a new inode via the metadata-only mlfs
// side, linking it under path's parent directory. The new file reads back
// byte-identical to the ref.
//
// path's parent directory MUST already exist (the bridge does not mkdir -p); the
// leaf name is the new file. class selects the (already-materialised) compression
// class. Returns the new inode.
//
// All-or-nothing guarantee: if CreateInodeFromChunks succeeds in creating the inode
// but a subsequent step fails (e.g. slice registration or content-hash stamp), the
// bridge unlinks the partially-created inode before returning the error. Callers
// see either a fully-constructed file or no file — never a partial inode left under
// path. The unlink is best-effort; if it itself fails, both errors are returned
// (wrapped) so the caller can surface the unlink failure alongside the original.
func (b *Bridge) RefToMlfs(ctx context.Context, ref, path string, class StoreClass) (meta.Ino, error) {
	parentPath, name, err := splitParent(path)
	if err != nil {
		return 0, err
	}
	parent, err := b.fs.Resolve(ctx, parentPath)
	if err != nil {
		return 0, err
	}
	info, err := b.gw.Head(ctx, ref)
	if err != nil {
		return 0, fmt.Errorf("bridge: head ref %q: %w", ref, err)
	}
	chunks, err := b.gw.RefChunks(ctx, ref)
	if err != nil {
		return 0, fmt.Errorf("bridge: ref %q chunks: %w", ref, err)
	}
	inode, err := b.fs.CreateInodeFromChunks(ctx, parent, name, chunks, info.ContentHash, info.Size, class)
	if err != nil {
		// inode > 0 means the Create call inside CreateInodeFromChunks succeeded but
		// a later step (slice registration, content-hash stamp, …) failed. Unlink the
		// partial inode so the caller sees no file rather than an incomplete one.
		if inode != 0 {
			if ulErr := b.fs.Unlink(ctx, parent, name); ulErr != nil {
				return 0, fmt.Errorf("bridge: ref-to-mlfs %q→%q: create failed (%w) and unlink of partial inode also failed: %v", ref, path, err, ulErr)
			}
		}
		return 0, fmt.Errorf("bridge: ref-to-mlfs %q→%q: %w", ref, path, err)
	}
	return inode, nil
}

// VerifyRefContent is the opt-in content-hash verification for the blobgw side:
// it streams the ref's full body through the gateway (reading ALL chunk bytes) and
// asserts that the computed sha256 matches the ref's declared ContentHash.
//
// This is NOT zero-copy: it reads every chunk byte once. Use it for admin/repair
// passes where end-to-end hash fidelity must be confirmed (e.g. after bridging from
// an mlfs source whose content_hash was suspect). Do NOT call it in the hot path.
//
// Returns ErrContentHashMismatch (wrapping a detail string) if the hashes disagree,
// nil if they match, or a wrapped I/O error if the ref could not be read.
func (b *Bridge) VerifyRefContent(ctx context.Context, ref string) error {
	info, err := b.gw.Head(ctx, ref)
	if err != nil {
		return fmt.Errorf("bridge: verify-ref %q: head: %w", ref, err)
	}
	rc, _, err := b.gw.Get(ctx, ref)
	if err != nil {
		return fmt.Errorf("bridge: verify-ref %q: get: %w", ref, err)
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return fmt.Errorf("bridge: verify-ref %q: read: %w", ref, err)
	}
	computed := hex.EncodeToString(h.Sum(nil))
	if computed != info.ContentHash {
		return fmt.Errorf("bridge: verify-ref %q: expected %s got %s: %w", ref, info.ContentHash, computed, ErrContentHashMismatch)
	}
	return nil
}

// VerifyMlfsContent is the opt-in content-hash verification for the mlfs side: it
// streams the inode's full content through the mlfs data path and asserts the sha256
// matches the inode's persisted content_hash.
//
// Same caveats as VerifyRefContent: not zero-copy, opt-in only, admin/repair use.
// Returns ErrContentHashMismatch if hashes disagree, ErrNoContentHash if the inode
// has no persisted hash (cannot verify), or a wrapped I/O error.
//
// fileio is a ReadAt-style function matching the mlfs data path signature:
// func(ctx, inode, offset int64, buf []byte) (int, error). Pass fileio.New(...).Read.
func (b *Bridge) VerifyMlfsContent(ctx context.Context, inode meta.Ino, read func(ctx context.Context, inode meta.Ino, off int64, buf []byte) (int, error)) error {
	contentHash, size, err := b.fs.ContentHashAndSize(ctx, inode)
	if err != nil {
		return fmt.Errorf("bridge: verify-mlfs inode %d: getattr: %w", inode, err)
	}
	if contentHash == "" {
		return fmt.Errorf("bridge: verify-mlfs inode %d: no content hash stamped (file never flushed): %w", inode, ErrNoContentHash)
	}
	h := sha256.New()
	buf := make([]byte, 256*1024)
	var off int64
	for off < size {
		n, err := read(ctx, inode, off, buf)
		if n > 0 {
			h.Write(buf[:n])
			off += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("bridge: verify-mlfs inode %d: read at %d: %w", inode, off, err)
		}
	}
	computed := hex.EncodeToString(h.Sum(nil))
	if computed != contentHash {
		return fmt.Errorf("bridge: verify-mlfs inode %d: expected %s got %s: %w", inode, contentHash, computed, ErrContentHashMismatch)
	}
	return nil
}

// ErrNoContentHash is returned by VerifyMlfsContent when the inode has no persisted
// whole-file content hash (the file was never flushed with SetContentHash). Bridging
// such a file is also refused by MlfsToRef for the same reason.
var ErrNoContentHash = errors.New("bridge: inode has no persisted content hash")

// sumChunkSizes totals the byte sizes of an ordered chunk list.
func sumChunkSizes(chunks []snapshot.ChunkLocation) int64 {
	var n int64
	for _, c := range chunks {
		n += int64(c.Size)
	}
	return n
}

// splitParent splits an absolute path into (parentDir, leafName). A path with no
// parent component (e.g. "/file") yields ("/", "file"). An empty leaf (trailing
// slash) or empty path is an error — the bridge needs a concrete target file name.
func splitParent(path string) (parent, name string, err error) {
	comps := splitPath(path)
	if len(comps) == 0 {
		return "", "", fmt.Errorf("bridge: empty target path")
	}
	name = comps[len(comps)-1]
	parent = "/" + strings.Join(comps[:len(comps)-1], "/")
	return parent, name, nil
}
