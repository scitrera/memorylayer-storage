// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package fileio is mlfs's file data path: it turns POSIX read/write at byte
// offsets into slice writes (chunk store) + slice records (meta engine), and
// resolves overlapping slices on read so newer writes win.
//
// A file is a sequence of fixed-size chunks (meta.ChunkSize). A Write produces
// one new slice per touched chunk, whose bytes go to the chunk store under a
// freshly-allocated slice id; a slice_ref row records where it lands. A Read
// gathers a chunk's slice history (write order) and overlays them — the
// overlaid slice ranges: the newest slice wins,
// uncovered ranges read as zeros (holes). Copy-on-write (Clone) is a pure
// metadata slice-row copy, sharing the underlying chunks.
package fileio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/wal"
)

// tracer opens the data-path child spans (mlfs.read.cold) under the FUSE op span
// the bridge starts; it is the GLOBAL OTEL tracer, a no-op until a TracerProvider
// is installed (only with an OTLP endpoint configured), so the warm path pays
// nothing.
var tracer = otel.Tracer("github.com/scitrera/memorylayer-storage/mlfs/internal/fileio")

// Files is the data path over a metadata engine + chunk store.
type Files struct {
	meta   *meta.Engine
	chunks chunkstore.Store
	wal    *wal.WAL   // optional local data write-ahead log (L2.4); nil disables it
	ra     *readAhead // optional sequential-read prefetcher (nil = disabled)
}

// New constructs the data path with no write-ahead log (crash recovery of
// uncommitted metadata writes is then unavailable — used by tests and tools).
func New(m *meta.Engine, chunks chunkstore.Store) *Files {
	return &Files{meta: m, chunks: chunks}
}

// NewWithWAL constructs the data path backed by a local data WAL: each chunk
// write is logged (fsync'd) AFTER the data is staged but BEFORE the metadata
// commit, so a crash in that window is recovered by ReplayWAL on the next start
// instead of losing an acknowledged write. Pass the same WAL to ReplayWAL at
// startup before serving.
func NewWithWAL(m *meta.Engine, chunks chunkstore.Store, w *wal.WAL) *Files {
	return &Files{meta: m, chunks: chunks, wal: w}
}

// EnableReadahead turns on sequential-read prefetch: on a detected sequential
// stream, window bytes ahead of the read cursor are proactively materialized
// into the disk cache, hiding the per-slice backing (S3) + metadata (PG) round
// trips the kernel's ≤128 KiB FUSE readahead can't cover (e.g. cold model
// loading). concurrency bounds in-flight backing fetches (≤0 = default); on a
// cold sequential read this is the throughput knob (aggregate ≈ concurrency ×
// per-slice rate), so scale it with the metadata pool on read-heavy pools.
// window ≤ 0 disables it; metrics may be nil. Call once before serving and pair
// with Close to stop the background prefetcher.
func (f *Files) EnableReadahead(window int64, concurrency int, m *metrics.Registry) {
	if window <= 0 {
		return
	}
	f.ra = newReadAhead(f, window, concurrency, m)
}

// Close stops the background readahead prefetcher (if enabled) and waits for
// in-flight prefetches to finish. Safe to call when readahead was never enabled.
func (f *Files) Close() error {
	if f.ra != nil {
		f.ra.close()
	}
	return nil
}

// Write writes data at byte offset in file ino, returning bytes written. It
// splits across chunk boundaries, allocating one slice per touched chunk. class
// is the file's compression class (caller-supplied; the bridge reads it from the
// inode), carried onto every slice so model/tensor files stay uncompressed and
// generic files compress.
func (f *Files) Write(ctx context.Context, ino meta.Ino, offset int64, data []byte, class chunkstore.StoreClass) (int, error) {
	written := 0
	for len(data) > 0 {
		indx := uint32(offset / meta.ChunkSize)
		pos := uint32(offset % meta.ChunkSize)
		n := len(data)
		if room := int(meta.ChunkSize - int64(pos)); n > room {
			n = room
		}
		id, err := f.meta.NewSlice(ctx)
		if err != nil {
			return written, fmt.Errorf("fileio: new slice: %w", err)
		}
		if err := f.chunks.Write(ctx, id, data[:n], class); err != nil {
			return written, err
		}
		// Log the slice (fsync'd) before the metadata commit: if we crash here the
		// data is already staged and ReplayWAL re-runs this meta.Write on restart.
		if f.wal != nil {
			if _, err := f.wal.Append(wal.Record{
				Op: wal.OpWrite, Ino: uint64(ino), Indx: indx, Off: pos,
				SliceID: id, Size: uint32(n), SliceOff: 0, SliceLen: uint32(n),
			}); err != nil {
				return written, fmt.Errorf("fileio: wal append: %w", err)
			}
		}
		if st := f.meta.Write(ctx, ino, indx, pos, meta.Slice{Id: id, Size: uint32(n), Off: 0, Len: uint32(n)}); st != 0 {
			return written, fmt.Errorf("fileio: meta write: errno %v", st)
		}
		written += n
		offset += int64(n)
		data = data[n:]
	}
	return written, nil
}

// Read fills buf with the file's bytes starting at offset, returning the count
// (clamped to file length; holes read as zeros). Returns 0 at/after EOF.
func (f *Files) Read(ctx context.Context, ino meta.Ino, offset int64, buf []byte) (int, error) {
	attr, st := f.meta.GetAttr(ctx, ino)
	if st != 0 {
		return 0, fmt.Errorf("fileio: getattr: errno %v", st)
	}
	fileLen := int64(attr.Length)
	if offset >= fileLen || len(buf) == 0 {
		return 0, nil
	}
	end := offset + int64(len(buf))
	if end > fileLen {
		end = fileLen
	}
	n := int(end - offset)
	for i := 0; i < n; i++ { // holes default to zero
		buf[i] = 0
	}
	// Child span for the data fetch: the manifest resolve (meta.ReadSlices → PG)
	// and the chunk reads (chunks.ReadAt → cache; on a miss the casstore backing
	// leaf span over S3) nest under it via spanCtx, so a slow cold read visibly
	// decomposes into manifest(PG) + pack(S3) under the parent FUSE read span. It
	// is cheap (one span start/end) and a no-op until a TracerProvider is installed;
	// a warm cache-hit read produces it with no backing children, which is fine.
	spanCtx, span := tracer.Start(ctx, "mlfs.read.cold", trace.WithAttributes(
		attribute.Int64("mlfs.inode", int64(ino)),
		attribute.Int64("mlfs.offset", offset),
		attribute.Int("mlfs.size", n)))
	cur := offset
	for cur < end {
		indx := uint32(cur / meta.ChunkSize)
		chunkBase := int64(indx) * meta.ChunkSize
		lo := uint32(cur - chunkBase)
		hiByte := chunkBase + meta.ChunkSize
		if hiByte > end {
			hiByte = end
		}
		hi := uint32(hiByte - chunkBase)

		slices, st := f.meta.ReadSlices(spanCtx, ino, indx)
		if st != 0 {
			err := fmt.Errorf("fileio: read slices: errno %v", st)
			span.RecordError(err)
			span.SetStatus(codes.Error, "read slices")
			span.End()
			return 0, err
		}
		for _, sg := range resolve(slices, lo, hi) {
			if sg.id == 0 {
				continue // hole — already zero
			}
			dst := int(chunkBase + int64(sg.start) - offset)
			length := int(sg.end - sg.start)
			if _, err := f.chunks.ReadAt(spanCtx, sg.id, int64(sg.srcOff), buf[dst:dst+length]); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "chunk read")
				span.End()
				return 0, err
			}
		}
		cur = hiByte
	}
	span.End()
	// Feed the sequential-read prefetcher with this read's extent so it can warm
	// the next window ahead of demand (no-op when readahead is disabled).
	if f.ra != nil {
		f.ra.note(ino, offset, end, fileLen)
	}
	return n, nil
}

// hashStreamSize is the buffer HashFile streams the file through: large enough
// to amortize the per-Read slice-manifest resolve, small enough to bound memory
// on a huge file. It need not align to ChunkSize — Read handles arbitrary
// offsets/lengths.
const hashStreamSize = 1 << 20 // 1 MiB

// HashFile computes the whole-file sha256 of ino as lowercase hex (no algorithm
// prefix), matching blobgw's ContentHash encoding. It streams the file's bytes in
// order through the normal Read path, so it resolves overlapping slices, reads
// holes as zeros, and works over either the disk cache or the casstore backing —
// giving the correct digest regardless of how the writes arrived (sequential,
// random backfill, sparse). This is the recompute fallback the bridge uses when
// the incremental running hash was invalidated by a behind-watermark write; it is
// also the definition the incremental fast path must agree with. An empty file
// hashes to sha256("").
func (f *Files) HashFile(ctx context.Context, ino meta.Ino) (string, error) {
	attr, st := f.meta.GetAttr(ctx, ino)
	if st != 0 {
		return "", fmt.Errorf("fileio: hash getattr: errno %v", st)
	}
	h := sha256.New()
	fileLen := int64(attr.Length)
	buf := make([]byte, hashStreamSize)
	for off := int64(0); off < fileLen; {
		want := fileLen - off
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		n, err := f.Read(ctx, ino, off, buf[:want])
		if err != nil {
			return "", fmt.Errorf("fileio: hash read at %d: %w", off, err)
		}
		if n == 0 {
			break // defensive: no progress (should not happen below fileLen)
		}
		_, _ = h.Write(buf[:n])
		off += int64(n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Truncate sets the file length. Shrink is honored on read (bytes past the new
// length are not returned); reclaiming now-unreachable slice rows is a later
// refinement. Grow makes the tail sparse (reads as zeros).
func (f *Files) Truncate(ctx context.Context, ino meta.Ino, size int64) error {
	// Log the length change before the commit (same crash-window guarantee as
	// Write). The new length is split across the record's Indx (high 32) / Off
	// (low 32), per the wal.Record OpTruncate encoding.
	if f.wal != nil {
		if _, err := f.wal.Append(wal.Record{
			Op: wal.OpTruncate, Ino: uint64(ino),
			Indx: uint32(uint64(size) >> 32), Off: uint32(uint64(size)),
		}); err != nil {
			return fmt.Errorf("fileio: wal append (truncate): %w", err)
		}
	}
	if st := f.meta.Truncate(ctx, ino, size); st != 0 {
		return fmt.Errorf("fileio: truncate: errno %v", st)
	}
	return nil
}

// Clone copy-on-writes src onto dst (metadata-only; shares chunks).
func (f *Files) Clone(ctx context.Context, src, dst meta.Ino) error {
	if st := f.meta.Clone(ctx, src, dst); st != 0 {
		return fmt.Errorf("fileio: clone: errno %v", st)
	}
	return nil
}

// CopyFileRange copies length bytes at srcOff in src to dstOff in dst, as
// copy_file_range(2): it returns the number of bytes copied, clamped to src's
// length. When the whole copied run is chunk-aligned (both offsets and the run
// length land on meta.ChunkSize boundaries, the common case for a streaming
// cp(1)) it shares the underlying chunks copy-on-write — zero data movement,
// the same blob-sharing as Clone. Any non-aligned head/tail (mid-chunk offsets
// or a final partial chunk) falls back to a plain read+write copy, so the
// result is always byte-correct regardless of alignment.
func (f *Files) CopyFileRange(ctx context.Context, src, dst meta.Ino, srcOff, dstOff int64, length int) (int, error) {
	if length <= 0 {
		return 0, nil
	}
	sa, st := f.meta.GetAttr(ctx, src)
	if st != 0 {
		return 0, fmt.Errorf("fileio: copy_file_range getattr src: errno %v", st)
	}
	// Clamp the request to src's length (copy_file_range never reads past EOF).
	if srcOff >= int64(sa.Length) {
		return 0, nil
	}
	if srcOff+int64(length) > int64(sa.Length) {
		length = int(int64(sa.Length) - srcOff)
	}
	end := srcOff + int64(length)

	const cs = int64(meta.ChunkSize)
	copied := 0
	cur := srcOff
	dcur := dstOff
	for cur < end {
		remaining := end - cur
		// Fast path: aligned offsets and at least a full chunk to go (and the
		// run reaches a chunk boundary or src EOF). Share whole chunks.
		if cur%cs == 0 && dcur%cs == 0 && remaining >= cs {
			nChunks := remaining / cs
			srcIdx := uint32(cur / cs)
			dstIdx := uint32(dcur / cs)
			if st := f.meta.CloneRange(ctx, src, dst, srcIdx, dstIdx, uint32(nChunks)); st != 0 {
				return copied, fmt.Errorf("fileio: copy_file_range clone: errno %v", st)
			}
			span := nChunks * cs
			cur += span
			dcur += span
			copied += int(span)
			continue
		}
		// Slow path: copy up to the next chunk boundary on either side via
		// read+write so partial/misaligned regions stay byte-correct.
		step := remaining
		if toSrcBound := cs - (cur % cs); toSrcBound < step {
			step = toSrcBound
		}
		if toDstBound := cs - (dcur % cs); toDstBound < step {
			step = toDstBound
		}
		buf := make([]byte, step)
		rn, err := f.Read(ctx, src, cur, buf)
		if err != nil {
			return copied, err
		}
		if rn == 0 {
			break
		}
		wn, err := f.Write(ctx, dst, dcur, buf[:rn], chunkstore.ClassDefault)
		if err != nil {
			return copied, err
		}
		cur += int64(wn)
		dcur += int64(wn)
		copied += wn
		if rn < int(step) {
			break // hit src EOF mid-window
		}
	}
	// Ensure dst is at least as long as the copy end (the fast path does not
	// touch length; a hole-only fast copy still needs the length recorded).
	if copied > 0 {
		if da, st := f.meta.GetAttr(ctx, dst); st == 0 && int64(da.Length) < dstOff+int64(copied) {
			if st := f.meta.Truncate(ctx, dst, dstOff+int64(copied)); st != 0 {
				return copied, fmt.Errorf("fileio: copy_file_range extend dst: errno %v", st)
			}
		}
	}
	return copied, nil
}

// --- slice overlay resolution ---

// seg is a resolved, non-overlapping piece of a chunk read window: bytes
// [start,end) come from chunk-store slice id at [srcOff, srcOff+(end-start)).
// id == 0 means a hole (zeros).
type seg struct {
	start, end uint32
	id         uint64
	srcOff     uint32
}

type iv struct{ a, b uint32 }

// resolve overlays a chunk's write-ordered slices within window [lo,hi),
// newest-wins, and returns ordered non-overlapping segments (holes as id 0).
func resolve(slices []meta.SliceRef, lo, hi uint32) []seg {
	if lo >= hi {
		return nil
	}
	var covered []iv // sorted, merged, disjoint
	var out []seg
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
		for _, g := range subtract(a, b, covered) {
			out = append(out, seg{start: g.a, end: g.b, id: s.Id, srcOff: s.Off + (g.a - s.Pos)})
			covered = addCovered(covered, g.a, g.b)
		}
	}
	for _, g := range subtract(lo, hi, covered) {
		out = append(out, seg{start: g.a, end: g.b}) // hole (id 0)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out
}

// subtract returns the parts of [a,b) not in any covered interval (sorted,
// disjoint).
func subtract(a, b uint32, covered []iv) []iv {
	var res []iv
	cur := a
	for _, c := range covered {
		if c.b <= cur || c.a >= b {
			continue
		}
		if c.a > cur {
			res = append(res, iv{cur, min32(c.a, b)})
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

// addCovered inserts [a,b) and returns a sorted, merged interval set.
func addCovered(covered []iv, a, b uint32) []iv {
	covered = append(covered, iv{a, b})
	sort.Slice(covered, func(i, j int) bool { return covered[i].a < covered[j].a })
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
	return merged
}

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
