// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sync"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// contenthash.go maintains mlfs's whole-file content hash on the write path: a
// running sha256 that agrees, byte-for-byte, with blobgw's ContentHash (the
// lowercase-hex sha256 of the whole object). This is the foundation for the
// mlfs↔VFS bridge (a bridged blob_ref's content_hash must equal this) and closes
// mlfs's integrity gap of storing no whole-file digest.
//
// The design is one running hash + a hashed_len watermark per OPEN file, kept in
// memory (never persisted — only the FINAL hash lands on the inode):
//
//   - Fast path (sequential/append, the common workload): a Write whose offset
//     equals hashed_len feeds its bytes straight into the running sha256 and
//     advances the watermark. This is a cheap per-write hash update — no extra
//     I/O — so it does not regress the write-throughput design (the OTEL spans /
//     metrics on the write path are untouched; the update happens after the data
//     write returns, on the bytes already in hand).
//
//   - Recompute fallback (true random overwrite/backfill): a Write landing BELOW
//     the watermark rewrites already-hashed bytes, so the running digest can no
//     longer be trusted; we mark it invalid. On flush/close the bridge then
//     recomputes the whole-file sha256 by streaming the assembled file in order
//     (fileio.HashFile, which reuses the read path), which also covers
//     sparse/out-of-order writes correctly. A write at offset ABOVE the watermark
//     (a hole ahead) also invalidates: the skipped bytes are zeros the running
//     hash never absorbed, so only a recompute reflects the sparse content.
//
// Concurrency: mlfs serializes a file's writes per fd via the kernel, but a
// stateless-handle bridge can still see concurrent Writes to one inode from
// different fds/threads. Each hashState carries its own mutex so the running
// digest and watermark update atomically — the running hash is never corrupted
// by interleaved writers, matching the meta engine's per-inode write fence.

// hashState is the per-open incremental whole-file hash accumulator for one
// inode. Access is serialized by mu so concurrent Writes to the same inode never
// interleave into the running digest.
type hashState struct {
	mu        sync.Mutex
	running   hash.Hash // running sha256 over [0, hashedLen)
	hashedLen int64     // watermark: bytes absorbed into running, in order from 0
	valid     bool      // false once a behind/ahead-watermark write forced recompute
}

// newHashState starts a fresh accumulator: an empty running sha256 valid from
// offset 0 (an unwritten file's hash is sha256("")).
func newHashState() *hashState {
	return &hashState{running: sha256.New(), valid: true}
}

// note folds a completed write of data at offset into the running hash. It runs
// AFTER the data write succeeds, on the bytes already in hand, so it adds only a
// sha256 update on the fast path and never any I/O. A write exactly at the
// watermark (sequential/append) advances it; anything else (a backfill below, or
// a gap above) invalidates the running digest so flush recomputes.
func (h *hashState) note(offset int64, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.valid {
		return // already committed to a recompute; no point tracking further
	}
	switch {
	case offset == h.hashedLen:
		_, _ = h.running.Write(data)
		h.hashedLen += int64(len(data))
	case offset < h.hashedLen:
		// Behind-watermark overwrite: already-hashed bytes changed.
		h.valid = false
	default: // offset > h.hashedLen
		// Gap ahead: the [hashedLen, offset) bytes are unhashed (a hole/zeros or a
		// not-yet-arrived earlier write); the running digest can't represent it.
		h.valid = false
	}
}

// invalidate forces the recompute path for this inode's next finalize.
func (h *hashState) invalidate() {
	h.mu.Lock()
	h.valid = false
	h.mu.Unlock()
}

// finalize returns the whole-file hash and whether the running digest is usable.
// When ok, digest is the fast-path result and equals sha256 of [0, hashedLen);
// the caller must confirm hashedLen == file size before trusting it (a file
// truncated shorter, or extended by a sparse SetAttr, makes the watermark diverge
// from the real length). When !ok, the caller must recompute via fileio.HashFile.
func (h *hashState) finalize() (digest string, hashedLen int64, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.valid {
		return "", h.hashedLen, false
	}
	return hex.EncodeToString(h.running.Sum(nil)), h.hashedLen, true
}

// hashStateFor returns the accumulator for ino, creating it on first use. Returns
// nil when the feature is disabled, so the write path skips all hashing.
func (b *Bridge) hashStateFor(ino meta.Ino) *hashState {
	if b.hashDisabled {
		return nil
	}
	if v, ok := b.hashStates.Load(uint64(ino)); ok {
		return v.(*hashState)
	}
	v, _ := b.hashStates.LoadOrStore(uint64(ino), newHashState())
	return v.(*hashState)
}

// noteWriteHash folds a completed write into ino's running hash (no-op when the
// feature is disabled). Called from Write after the data lands.
func (b *Bridge) noteWriteHash(ino meta.Ino, offset int64, data []byte) {
	if h := b.hashStateFor(ino); h != nil {
		h.note(offset, data)
	}
}

// invalidateHash marks ino's running digest stale so the next finalize recomputes
// (called from paths that change bytes outside the sequential write flow, e.g.
// truncate/setattr-size). No-op when disabled or untracked (an untracked inode
// re-derives from scratch on its first Write or recomputes on finalize).
func (b *Bridge) invalidateHash(ino meta.Ino) {
	if b.hashDisabled {
		return
	}
	if v, ok := b.hashStates.Load(uint64(ino)); ok {
		v.(*hashState).invalidate()
	}
}

// finalizeHash computes ino's whole-file content hash and persists it on the
// inode. It uses the running digest when it is valid AND its watermark matches
// the current file length (the pure sequential/append case); otherwise it
// recomputes by streaming the assembled file (fileio.HashFile). Called on
// flush/close. It never fails the FUSE op: a hashing or persist error is logged
// by the caller path but does not surface to the workload (the hash is best-effort
// integrity metadata, not data). Returns the digest and any error.
//
// The inode's current length is read once here and compared to the watermark so a
// file written sequentially then truncated shorter (watermark now past EOF) falls
// back to recompute rather than trusting a digest over bytes that no longer exist.
func (b *Bridge) finalizeHash(ctx context.Context, ino meta.Ino) (string, error) {
	if b.hashDisabled {
		return "", nil
	}
	v, ok := b.hashStates.Load(uint64(ino))
	if !ok {
		return "", nil // never written through this mount since open — nothing to stamp
	}
	h := v.(*hashState)

	digest, hashedLen, fast := h.finalize()
	if fast {
		// Confirm the watermark still equals the live file length; a truncate or a
		// size-extending SetAttr since the last write would make the running digest
		// cover the wrong byte range.
		a, st := b.meta.GetAttr(ctx, ino)
		if st != 0 || int64(a.Length) != hashedLen {
			fast = false
		}
	}
	if !fast {
		var err error
		digest, err = b.files.HashFile(ctx, ino)
		if err != nil {
			return "", err
		}
	}
	if st := b.meta.SetContentHash(ctx, ino, digest); st != 0 {
		return digest, st // syscall.Errno implements error
	}
	return digest, nil
}
