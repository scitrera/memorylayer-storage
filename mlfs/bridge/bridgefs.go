// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package bridge is the EXPORTED mlfs↔VFS metadata-only bridge (handoff Item C):
// it moves files between mlfs and blobgw by SYNTHESISING metadata over the shared
// casstore chunks — no file bytes are ever read or written in either direction.
//
// This file (the mlfs-side surface, type FS) provides:
//
//   - CreateInodeFromChunks (VFS→mlfs, "create-inode-from-manifest") lays a
//     blob_ref's already-existing casstore chunks into a new mlfs inode's slices by
//     synthesising each slice's manifest over those chunks, then links the inode
//     into a directory. No download, no chunking, no byte copy.
//   - FileChunks (mlfs→VFS) gathers a file's slices' ordered casstore chunk lists so
//     the coordinator can register a blob_ref over the SAME chunks.
//
// The coordinator (type Bridge, in coordinator.go) drives both directions end to
// end. The package is exported (not internal/) so callers outside mlfs can wire it,
// but it lives IN the mlfs module because only mlfs-internal code can construct the
// meta engine + chunk store it operates over. The zero-byte-movement and
// same-chunk-dedup invariants are enforced here (only metadata-only
// chunkstore.RegisterSlice + meta row writes are ever called) and asserted by tests.
package bridge

import (
	"context"
	"fmt"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// FS is the metadata-only bridge surface over an mlfs metadata engine + chunk
// store. Construct with New using the SAME engine and CasStore the running mlfs
// daemon uses (so the bridged inode is a first-class file the FUSE path serves and
// GC accounts for). It is safe for concurrent use to the same degree the
// underlying engine/store are.
type FS struct {
	meta   *meta.Engine
	chunks *chunkstore.CasStore
}

// New wraps an mlfs metadata engine and its casstore-backed chunk store as the
// bridge surface. Both must point at the SAME tenant domain and Postgres as the
// blobgw side of the bridge (the hard cross-side invariant: mlfs mount domain ==
// blobgw ref domain == tenant domain, one PG).
func New(m *meta.Engine, chunks *chunkstore.CasStore) *FS {
	return &FS{meta: m, chunks: chunks}
}

// Ino re-exports mlfs's inode type so bridge callers can name it without importing
// mlfs internals.
type Ino = meta.Ino

// RootInode is the fixed inode of the mlfs root, re-exported for path resolution.
const RootInode = meta.RootInode

// Resolve walks an absolute, slash-separated path from the mlfs root to its inode.
// A leading "/" is optional; "" and "/" resolve to the root. It is the path→inode
// step of the mlfs→VFS bridge direction. Returns an error (wrapping the engine's
// errno) if any component is missing or not a directory.
func (fs *FS) Resolve(ctx context.Context, path string) (meta.Ino, error) {
	ino := meta.RootInode
	for _, name := range splitPath(path) {
		next, _, st := fs.meta.Lookup(ctx, ino, name)
		if st != 0 {
			return 0, fmt.Errorf("bridgefs: resolve %q: component %q: errno %v", path, name, st)
		}
		ino = next
	}
	return ino, nil
}

// Unlink removes the directory entry name under parent. It is the rollback path
// for RefToMlfs: if CreateInodeFromChunks creates an inode but a subsequent step
// fails, the coordinator calls Unlink to remove the partial inode so no incomplete
// file is left under path. Returns an error wrapping the engine's errno on failure.
func (fs *FS) Unlink(ctx context.Context, parent meta.Ino, name string) error {
	if st := fs.meta.Unlink(ctx, parent, name, meta.Background()); st != 0 {
		return fmt.Errorf("bridgefs: unlink %q under %d: errno %v", name, parent, st)
	}
	return nil
}

// ContentHashAndSize returns a file's persisted whole-file content hash and byte
// length — the two values the mlfs→VFS direction stamps onto the blob_ref. An
// empty hash means the file was never flushed with a hash; the coordinator refuses
// to bridge such a file (the ref's content_hash must be authoritative).
func (fs *FS) ContentHashAndSize(ctx context.Context, inode meta.Ino) (string, int64, error) {
	attr, st := fs.meta.GetAttr(ctx, inode)
	if st != 0 {
		return "", 0, fmt.Errorf("bridgefs: getattr %d: errno %v", inode, st)
	}
	return attr.ContentHash, int64(attr.Length), nil
}

// splitPath splits an absolute path into non-empty components, tolerating leading/
// trailing/duplicate slashes. "" and "/" yield no components (the root).
func splitPath(path string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '/' {
			if i > start {
				out = append(out, path[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// StoreClass selects a bridged file's compression class, re-exported so the
// bridge module can pass it without importing mlfs internals. It maps 1:1 to
// chunkstore.StoreClass.
type StoreClass uint8

const (
	// ClassDefault leaves the codec to the backing policy (generic content
	// compresses).
	ClassDefault StoreClass = StoreClass(chunkstore.ClassDefault)
	// ClassUncompressed forces the slice's pack to be stored uncompressed
	// (model/tensor data). Only meaningful on a fresh write; for a bridged inode
	// the packs already exist, so the class only stamps the synthesised manifests
	// for consistency.
	ClassUncompressed StoreClass = StoreClass(chunkstore.ClassUncompressed)
)

// CreateInodeFromChunks creates a new regular file named name under directory
// parent whose bytes are the concatenation of chunks (an ordered casstore chunk
// list, e.g. a blob_ref's manifest chunks that already live in this tenant's pack
// set), links it into the directory, and stamps its whole-file content hash — all
// WITHOUT reading or writing a single chunk/pack byte.
//
// # How the metadata-only inode is built
//
// mlfs models a file as fixed-size 64 MiB chunks (meta.ChunkSize), each holding
// slices; a slice's bytes live in a casstore object keyed by its slice id. This
// lays the ref's chunks into ONE mlfs slice per 64 MiB file-chunk:
//
//   - We walk the ordered casstore chunks, accumulating the file byte offset. Each
//     mlfs 64 MiB chunk gets its own slice whose casstore manifest is REGISTERED
//     (chunkstore.RegisterSlice → casstore RegisterManifest) over the casstore
//     chunks that overlap that mlfs chunk. No pack is written; the slice's manifest
//     just points at the shared chunks.
//   - A casstore chunk that STRADDLES a 64 MiB boundary is referenced WHOLE by both
//     the slice before and the slice after the boundary; each slice's soff/slen read
//     window (meta.Slice.Off/Len) selects only the bytes that belong to its mlfs
//     chunk. This keeps every casstore chunk intact (its content hash is unchanged
//     and both slices dedup to the exact same pack bytes) while the visible file
//     bytes are byte-correct.
//
// contentHash MUST be the ref's whole-object sha256 hex (byte-identical to what
// mlfs's own SetContentHash records and to blobgw's ContentHash), and size the
// object's byte length; both are stamped onto the new node so the bridged file is
// indistinguishable from a natively-written one. class selects the slices'
// (already-materialised) compression class.
//
// It returns the new inode on success. When the inode is created (inode > 0) but a
// subsequent step fails, the returned inode is non-zero so the coordinator can
// unlink it — Bridge.RefToMlfs does exactly this (via FS.Unlink), giving
// all-or-nothing semantics. A create/link conflict (name already exists) is surfaced
// as errno wrapped in an error with inode == 0.
func (fs *FS) CreateInodeFromChunks(ctx context.Context, parent meta.Ino, name string, chunks []snapshot.ChunkLocation, contentHash string, size int64, class StoreClass) (meta.Ino, error) {
	if size < 0 {
		return 0, fmt.Errorf("bridgefs: negative size %d", size)
	}
	// Validate the chunk list sums to size before we mutate anything, so a
	// mismatched manifest is rejected up front (the file bytes must equal the
	// declared size for the content hash to hold).
	var total int64
	for i, c := range chunks {
		if c.PackHash == "" {
			return 0, fmt.Errorf("bridgefs: chunk %d (%s) has empty pack hash; only registered v2 pack chunks can be bridged", i, c.ChunkHash)
		}
		if c.Size <= 0 {
			// Size==0 is rejected (not just negative) for the same reason as in
			// RegisterManifest: a zero-size chunk creates boundary-attribution ambiguity
			// in chunksForWindow and cannot represent a meaningful byte range.
			return 0, fmt.Errorf("bridgefs: chunk %d (%s) has non-positive size %d; size must be > 0", i, c.ChunkHash, c.Size)
		}
		total += int64(c.Size)
	}
	if total != size {
		return 0, fmt.Errorf("bridgefs: chunk sizes sum to %d but declared size is %d", total, size)
	}

	inode, _, st := fs.meta.Create(ctx, parent, name, 0o644, meta.Background())
	if st != 0 {
		return 0, fmt.Errorf("bridgefs: create %q under %d: errno %v", name, parent, st)
	}

	if err := fs.layChunks(ctx, inode, chunks, class); err != nil {
		return inode, err
	}

	// Stamp the class and whole-file hash so the bridged inode matches a natively
	// written file (the class drives future writes' codec; the hash lets a later
	// mlfs→VFS bridge of this same file skip re-hashing).
	if class != ClassDefault {
		if st := fs.meta.SetStoreClass(ctx, inode, uint8(class)); st != 0 {
			return inode, fmt.Errorf("bridgefs: set store class on %d: errno %v", inode, st)
		}
	}
	if contentHash != "" {
		if st := fs.meta.SetContentHash(ctx, inode, contentHash); st != 0 {
			return inode, fmt.Errorf("bridgefs: set content hash on %d: errno %v", inode, st)
		}
	}
	return inode, nil
}

// layChunks assigns the ordered casstore chunks to per-64-MiB-mlfs-chunk slices
// and records each slice's manifest + slice_ref row. See CreateInodeFromChunks for
// the straddle handling. It is driven off each chunk's ABSOLUTE file offset
// (prefix sums) so the mapping is explicit and boundary-exact.
//
// For mlfs chunk index k covering file bytes [k*ChunkSize, (k+1)*ChunkSize):
//   - The slice references every casstore chunk that OVERLAPS that window, WHOLE
//     (so no chunk is split — content hashes and dedup are preserved). A chunk that
//     straddles the lower boundary is shared with mlfs chunk k-1; one that straddles
//     the upper boundary is shared with k+1.
//   - The slice's casstore object is exactly those overlapping chunks; its read
//     window is soff = (k*ChunkSize - firstOverlapChunkStart) .. and slen = the
//     bytes of this mlfs chunk (clamped to file end), so the visible bytes are
//     byte-correct even though the object may carry a straddler's out-of-window head
//     and/or tail.
func (fs *FS) layChunks(ctx context.Context, inode meta.Ino, chunks []snapshot.ChunkLocation, class StoreClass) error {
	if len(chunks) == 0 {
		return nil // empty file: no slices (Create already made a zero-length node)
	}
	const chunkSize = int64(meta.ChunkSize)

	// starts[i] = absolute file offset of chunks[i]; starts[len] = total file size.
	starts := make([]int64, len(chunks)+1)
	for i, c := range chunks {
		starts[i+1] = starts[i] + int64(c.Size)
	}
	total := starts[len(chunks)]

	nMlfsChunks := (total + chunkSize - 1) / chunkSize
	for k := int64(0); k < nMlfsChunks; k++ {
		lo := k * chunkSize
		hi := lo + chunkSize
		if hi > total {
			hi = total
		}
		// Find the casstore chunks overlapping [lo, hi): first with end > lo, last
		// with start < hi. starts is monotonic, so a linear pair of scans is fine
		// (chunk counts per 64 MiB mlfs-chunk are ~64 at 1 MiB targets).
		first := -1
		last := -1
		for i := range chunks {
			cStart, cEnd := starts[i], starts[i+1]
			if cEnd <= lo || cStart >= hi {
				continue
			}
			if first == -1 {
				first = i
			}
			last = i
		}
		if first == -1 {
			// Should not happen for a non-empty mlfs chunk; defensive.
			return fmt.Errorf("bridgefs: no casstore chunks overlap mlfs chunk %d ([%d,%d))", k, lo, hi)
		}
		assigned := chunks[first : last+1]
		objectStart := starts[first]
		objectSize := starts[last+1] - objectStart

		soff := lo - objectStart        // >= 0; skips a lower-straddler's head
		slen := hi - lo                 // this mlfs chunk's visible byte count
		pos := uint32(lo - k*chunkSize) // always 0 for whole-chunk slices, but explicit

		if err := fs.registerAndWriteSlice(ctx, inode, uint32(k), pos, assigned, objectSize, soff, slen, class); err != nil {
			return err
		}
	}
	return nil
}

// registerAndWriteSlice registers a slice's casstore manifest over `assigned`
// (metadata only) and records its slice_ref row so the data path resolves it.
func (fs *FS) registerAndWriteSlice(ctx context.Context, inode meta.Ino, indx, pos uint32, assigned []snapshot.ChunkLocation, objectSize, soff, slen int64, class StoreClass) error {
	sliceID, err := fs.meta.NewSlice(ctx)
	if err != nil {
		return fmt.Errorf("bridgefs: new slice: %w", err)
	}
	if err := fs.chunks.RegisterSlice(ctx, sliceID, assigned, chunkstore.StoreClass(class)); err != nil {
		return err
	}
	if st := fs.meta.Write(ctx, inode, indx, pos, meta.Slice{
		Id:   sliceID,
		Size: uint32(objectSize),
		Off:  uint32(soff),
		Len:  uint32(slen),
	}); st != 0 {
		return fmt.Errorf("bridgefs: record slice %d on inode %d chunk %d: errno %v", sliceID, inode, indx, st)
	}
	return nil
}

// FileChunks returns the ordered casstore chunk list for the whole file at inode,
// gathered from its slices, WITHOUT reading any chunk/pack bytes. It is the
// mlfs→VFS read half of the bridge: the coordinator hands the returned chunks to
// blobgw RegisterRef to synthesise a blob_ref over the SAME chunks.
//
// It walks the file's chunks in order (0..ceil(length/ChunkSize)), resolves each
// mlfs chunk's visible slices (newest-wins overlay, exactly like the data path), and
// emits each visible casstore chunk ONCE — attributed to the window its bytes begin
// in — so a chunk that straddles a 64 MiB mlfs-chunk boundary (shared whole by the
// slices on both sides via their soff/slen windows) is not double-counted. A file
// whose slices were each a whole casstore object (the common case, including any
// inode this package created) yields its chunk list directly; the overlay resolution
// makes it correct even for natively-written files with overlapping whole-slice
// writes.
//
// Constraint: the visible ranges must fall on casstore-chunk boundaries (they do for
// bridge-created inodes and for whole-slice writes). A sub-chunk overwrite that
// leaves a PARTIAL casstore chunk visible cannot be bridged byte-exactly (a partial
// chunk has no whole chunk to reference); FileChunks detects this via a byte-
// accounting guard (the emitted chunks must sum to the file length) and refuses it,
// so such a file must be re-materialised (recompacted) first. This is a deferred
// edge case per the handoff. A hole (sparse range) is likewise refused.
func (fs *FS) FileChunks(ctx context.Context, inode meta.Ino) ([]snapshot.ChunkLocation, error) {
	attr, st := fs.meta.GetAttr(ctx, inode)
	if st != 0 {
		return nil, fmt.Errorf("bridgefs: getattr %d: errno %v", inode, st)
	}
	if attr.Typ != meta.TypeFile {
		return nil, fmt.Errorf("bridgefs: inode %d is not a regular file", inode)
	}
	length := int64(attr.Length)
	const chunkSize = int64(meta.ChunkSize)

	var out []snapshot.ChunkLocation
	for fileOff := int64(0); fileOff < length; {
		indx := uint32(fileOff / chunkSize)
		chunkBase := int64(indx) * chunkSize
		chunkEnd := chunkBase + chunkSize
		if chunkEnd > length {
			chunkEnd = length
		}
		visStart := fileOff
		visEnd := chunkEnd

		chunks, err := fs.resolveChunkRange(ctx, inode, indx, uint32(visStart-chunkBase), uint32(visEnd-chunkBase))
		if err != nil {
			return nil, err
		}
		out = append(out, chunks...)
		fileOff = chunkEnd
	}
	// Byte-accounting guard: the emitted chunks must reconstruct exactly the file's
	// bytes. A mismatch means a slice's visible window did not fall on casstore-chunk
	// boundaries (a sub-chunk overwrite) — such a file cannot be bridged byte-exactly
	// and must be recompacted first (deferred edge case per the handoff).
	var total int64
	for _, c := range out {
		total += int64(c.Size)
	}
	if total != length {
		return nil, fmt.Errorf("bridgefs: inode %d chunk accounting mismatch: chunks sum to %d but file length is %d (a sub-chunk overwrite prevents byte-exact bridging; recompact first)", inode, total, length)
	}
	return out, nil
}

// visSeg is a resolved, non-overlapping visible piece of a chunk read window: the
// chunk-relative bytes [start,end) are served by slice id, read from the slice's
// own byte stream starting at srcOff.
type visSeg struct {
	start, end uint32
	id         uint64
	srcOff     uint32
}

// resolveChunkRange resolves mlfs chunk indx's write-ordered slices over the
// chunk-relative window [lo,hi) newest-wins (the same overlay semantics as the data
// path), then maps each visible segment to the WHOLE casstore chunks it covers. It
// errors if a visible segment does not fall on casstore-chunk boundaries (a
// sub-chunk overwrite), since a partial chunk cannot be bridged byte-exactly.
func (fs *FS) resolveChunkRange(ctx context.Context, inode meta.Ino, indx uint32, lo, hi uint32) ([]snapshot.ChunkLocation, error) {
	if lo >= hi {
		return nil, nil
	}
	refs, st := fs.meta.ReadSlices(ctx, inode, indx)
	if st != 0 {
		return nil, fmt.Errorf("bridgefs: read slices inode %d chunk %d: errno %v", inode, indx, st)
	}
	segs := resolveVisible(refs, lo, hi)

	// Cache each slice's casstore chunk list (metadata only) so a slice touched by
	// several visible segments is read once.
	sliceChunks := make(map[uint64][]snapshot.ChunkLocation)
	var out []snapshot.ChunkLocation
	for _, s := range segs {
		if s.id == 0 {
			return nil, fmt.Errorf("bridgefs: inode %d chunk %d has a hole in [%d,%d) — a sparse/partly-written file cannot be bridged byte-exactly", inode, indx, s.start, s.end)
		}
		locs, ok := sliceChunks[s.id]
		if !ok {
			var err error
			locs, err = fs.chunks.SliceChunks(ctx, s.id)
			if err != nil {
				return nil, err
			}
			sliceChunks[s.id] = locs
		}
		out = append(out, chunksForWindow(locs, int64(s.srcOff), int64(s.end-s.start))...)
	}
	return out, nil
}

// resolveVisible overlays a chunk's write-ordered slices within [lo,hi), newest
// wins, returning ordered non-overlapping visible segments. It mirrors the data
// path's resolver (internal/fileio.resolve) but emits the source read offset for
// each segment so the caller can map it to casstore chunks. Uncovered ranges
// (holes) are emitted with id 0.
func resolveVisible(slices []meta.SliceRef, lo, hi uint32) []visSeg {
	if lo >= hi {
		return nil
	}
	type iv struct{ a, b uint32 }
	var covered []iv
	var out []visSeg

	subtract := func(a, b uint32) []iv {
		var res []iv
		cur := a
		for _, c := range covered {
			if c.b <= cur || c.a >= b {
				continue
			}
			if c.a > cur {
				res = append(res, iv{cur, minU32(c.a, b)})
			}
			if c.b > cur {
				cur = c.b
			}
			if cur >= b {
				break
			}
		}
		if cur < b {
			res = append(res, iv{cur, b})
		}
		return res
	}
	addCovered := func(a, b uint32) {
		covered = append(covered, iv{a, b})
		// Insertion sort by interval start (covered is tiny per chunk).
		for i := 1; i < len(covered); i++ {
			for j := i; j > 0 && covered[j].a < covered[j-1].a; j-- {
				covered[j], covered[j-1] = covered[j-1], covered[j]
			}
		}
		merged := covered[:0:0]
		for _, c := range covered {
			if len(merged) > 0 && c.a <= merged[len(merged)-1].b {
				if c.b > merged[len(merged)-1].b {
					merged[len(merged)-1].b = c.b
				}
				continue
			}
			merged = append(merged, c)
		}
		covered = merged
	}

	for i := len(slices) - 1; i >= 0; i-- { // newest first
		s := slices[i]
		a, b := s.Pos, s.Pos+s.Len
		if a < lo {
			a = lo
		}
		if b > hi {
			b = hi
		}
		if a >= b {
			continue
		}
		for _, g := range subtract(a, b) {
			out = append(out, visSeg{start: g.a, end: g.b, id: s.Id, srcOff: s.Off + (g.a - s.Pos)})
			addCovered(g.a, g.b)
		}
	}
	for _, g := range subtract(lo, hi) {
		out = append(out, visSeg{start: g.a, end: g.b}) // hole (id 0)
	}
	sortVisSegs(out)
	return out
}

// chunksForWindow returns the casstore chunks of a slice's ordered chunk list whose
// bytes fall in the slice-relative visible window [off, off+length). A chunk is
// EMITTED iff its START lies within the window; a chunk whose start is before `off`
// was already emitted by the PREVIOUS window/slice (the boundary straddler shared
// across a 64 MiB mlfs-chunk boundary — or, for a slice reused across chunks, its
// leading bytes) and is skipped here. A chunk starting inside the window may extend
// past its END: that is the straddler shared with the NEXT window, emitted once here
// (where it begins). This attributes every casstore chunk to exactly one window.
//
// Correctness is guarded at the FileChunks level: the emitted chunks' total size must
// equal the file length. Any misalignment (a genuine sub-chunk overwrite that leaves
// a partial chunk visible with no whole chunk to represent it) shows up there as a
// size mismatch and is refused — a file with such overlaps must be re-materialised
// (recompacted) before it can be bridged (a deferred edge case per the handoff).
func chunksForWindow(locs []snapshot.ChunkLocation, off, length int64) []snapshot.ChunkLocation {
	if length == 0 {
		return nil
	}
	end := off + length
	var cur int64
	var out []snapshot.ChunkLocation
	for _, c := range locs {
		cStart := cur
		cur += int64(c.Size)
		if cStart < off || cStart >= end {
			continue // starts in a prior window (already emitted) or a later one
		}
		out = append(out, c) // may extend past `end` (straddler for the next window)
	}
	return out
}

func minU32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func sortVisSegs(s []visSeg) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].start < s[j-1].start; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
