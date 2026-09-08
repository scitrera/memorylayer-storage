// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// CompressionAlgo identifies a compression scheme that may be applied to
// snapshot blobs as they flow through a CompressingStore.
type CompressionAlgo string

const (
	// CompressNone disables compression. Streams flow through untouched and
	// no compression tag is stamped on metadata.
	CompressNone CompressionAlgo = "none"
	// CompressGzip uses stdlib compress/gzip. Universally compatible; modest
	// ratio; medium CPU.
	CompressGzip CompressionAlgo = "gzip"
	// CompressZstd uses github.com/klauspost/compress/zstd. Better ratio and
	// faster than gzip at the default level. Default.
	CompressZstd CompressionAlgo = "zstd"
)

// tagCompression is the SnapshotMetadata.Tags key written by Put to record
// which compression algorithm was applied. Get/GetLatest read this tag to
// decide how to decompress on read.
//
// A snapshot whose metadata lacks this tag is treated as uncompressed — this
// preserves backward compatibility with snapshots written before the
// compression decorator existed.
const tagCompression = "compression"

// ParseCompressionAlgo accepts the env-style spelling of an algorithm and
// returns the typed value. Empty input maps to CompressZstd (default). Unknown
// inputs return an error.
func ParseCompressionAlgo(s string) (CompressionAlgo, error) {
	switch s {
	case "":
		return CompressZstd, nil
	case "none", "off", "disabled":
		return CompressNone, nil
	case "gzip":
		return CompressGzip, nil
	case "zstd":
		return CompressZstd, nil
	default:
		return "", fmt.Errorf("snapshot: unknown compression algorithm %q (valid: none, gzip, zstd)", s)
	}
}

// CompressingStore wraps an upstream SnapshotStore with stream-based
// compression on Put and matching decompression on Get / GetLatest. It is the
// natural outermost wrapper — placing it above a CacheStore means cached
// blobs on disk are also compressed.
//
// Backward compatibility: on Get the algorithm is read from
// SnapshotMetadata.Tags["compression"]; absence is treated as "none" so
// snapshots written before this decorator continue to work.
//
// CompressingStore does not buffer; both directions stream through io.Pipe
// (Put) and a wrapping ReadCloser (Get).
type CompressingStore struct {
	upstream SnapshotStore
	algo     CompressionAlgo
}

// NewCompressingStore returns a CompressingStore that applies algo to every
// blob written via Put. If algo == CompressNone, the returned store is a
// pure pass-through (no tag stamped, no wrapping on Get) — callers that want
// to skip the decorator entirely should do so at construction time instead.
func NewCompressingStore(upstream SnapshotStore, algo CompressionAlgo) (*CompressingStore, error) {
	switch algo {
	case CompressNone, CompressGzip, CompressZstd:
		// ok
	default:
		return nil, fmt.Errorf("snapshot: invalid compression algorithm %q", algo)
	}
	return &CompressingStore{upstream: upstream, algo: algo}, nil
}

// Put streams r through the configured compressor into the upstream store.
// The compression tag is stamped on a *copy* of meta.Tags so the caller's map
// is not mutated.
func (c *CompressingStore) Put(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	if c.algo == CompressNone {
		return c.upstream.Put(ctx, key, meta, r)
	}

	// Stamp the algorithm on a copy of the tags map so callers can reuse the
	// meta value safely.
	tags := make(map[string]string, len(meta.Tags)+1)
	for k, v := range meta.Tags {
		tags[k] = v
	}
	tags[tagCompression] = string(c.algo)
	meta.Tags = tags

	pr, pw := io.Pipe()

	// Compress raw → pipe in a goroutine while upstream.Put drains the read side.
	errCh := make(chan error, 1)
	go func() {
		cw, cerr := newCompressor(pw, c.algo)
		if cerr != nil {
			pw.CloseWithError(cerr)
			errCh <- cerr
			return
		}
		_, copyErr := io.Copy(cw, r)
		closeErr := cw.Close()
		if copyErr == nil {
			copyErr = closeErr
		}
		pw.CloseWithError(copyErr)
		errCh <- copyErr
	}()

	stored, putErr := c.upstream.Put(ctx, key, meta, pr)
	compErr := <-errCh

	if compErr != nil {
		return SnapshotMetadata{}, fmt.Errorf("snapshot/compress: compress stream: %w", compErr)
	}
	if putErr != nil {
		return SnapshotMetadata{}, putErr
	}
	return stored, nil
}

// Get reads the upstream blob and, if the metadata records a non-none
// compression tag, returns a ReadCloser that transparently decompresses.
// Snapshots without the tag are returned as-is (backward compatibility).
func (c *CompressingStore) Get(ctx context.Context, key SnapshotKey, version string) (io.ReadCloser, SnapshotMetadata, error) {
	rc, meta, err := c.upstream.Get(ctx, key, version)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}
	return c.wrapReader(rc, meta)
}

// GetLatest mirrors Get for the highest-version snapshot.
func (c *CompressingStore) GetLatest(ctx context.Context, key SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	rc, meta, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}
	return c.wrapReader(rc, meta)
}

// GetLatestMetadata is a pure pass-through; no blob is opened.
func (c *CompressingStore) GetLatestMetadata(ctx context.Context, key SnapshotKey) (SnapshotMetadata, error) {
	return c.upstream.GetLatestMetadata(ctx, key)
}

// List is a pure pass-through; the compression tag remains visible to callers.
func (c *CompressingStore) List(ctx context.Context, key SnapshotKey) ([]SnapshotMetadata, error) {
	return c.upstream.List(ctx, key)
}

// Walk is a pure pass-through.
func (c *CompressingStore) Walk(ctx context.Context, yield func(SnapshotKey, string, SnapshotMetadata) error) error {
	return c.upstream.Walk(ctx, yield)
}

// Delete is a pure pass-through.
func (c *CompressingStore) Delete(ctx context.Context, key SnapshotKey, version string) error {
	return c.upstream.Delete(ctx, key, version)
}

// wrapReader inspects the metadata's compression tag and returns a
// decompressing reader when needed. It closes upstreamRC on construction
// errors so the caller does not have to.
func (c *CompressingStore) wrapReader(upstreamRC io.ReadCloser, meta SnapshotMetadata) (io.ReadCloser, SnapshotMetadata, error) {
	algo := CompressionAlgo(meta.Tags[tagCompression])
	if algo == "" {
		algo = CompressNone
	}
	if algo == CompressNone {
		return upstreamRC, meta, nil
	}
	dec, closer, err := newDecompressor(upstreamRC, algo)
	if err != nil {
		_ = upstreamRC.Close()
		return nil, SnapshotMetadata{}, fmt.Errorf("snapshot/compress: open decompressor (%s): %w", algo, err)
	}
	return &chainReadCloser{Reader: dec, closers: []io.Closer{closer, upstreamRC}}, meta, nil
}

// chainReadCloser is an io.ReadCloser that closes a chain of underlying
// closers on Close. Closers are invoked in slice order; the first error is
// returned but every closer is still called.
type chainReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (c *chainReadCloser) Close() error {
	var firstErr error
	for _, cl := range c.closers {
		if cl == nil {
			continue
		}
		if err := cl.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- compressor / decompressor factories ------------------------------------

func newCompressor(w io.Writer, algo CompressionAlgo) (io.WriteCloser, error) {
	switch algo {
	case CompressGzip:
		return gzip.NewWriter(w), nil
	case CompressZstd:
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return nil, err
		}
		return zw, nil
	default:
		return nil, fmt.Errorf("snapshot/compress: no compressor for %q", algo)
	}
}

// newDecompressor returns a Reader plus a Closer for the decompressor's own
// resources (gzip.Reader implements io.Closer; zstd.Decoder needs Close()
// called to release its goroutine).
func newDecompressor(r io.Reader, algo CompressionAlgo) (io.Reader, io.Closer, error) {
	switch algo {
	case CompressGzip:
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return gr, gr, nil
	case CompressZstd:
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return zr, zstdCloser{zr}, nil
	default:
		return nil, nil, fmt.Errorf("snapshot/compress: no decompressor for %q", algo)
	}
}

// zstdCloser adapts *zstd.Decoder (whose Close() returns nothing) to io.Closer.
type zstdCloser struct{ dec *zstd.Decoder }

func (z zstdCloser) Close() error {
	z.dec.Close()
	return nil
}

// --- self-describing pack-blob compression ----------------------------------
//
// ChunkedStore compresses each PACK blob (not the whole object) so dedup still
// keys on uncompressed chunk content. The codec must be recoverable on read
// WITHOUT a schema/manifest change, because a v2 manifest can reference packs
// created by other Puts (via cross-object dedup) whose codec is not in the
// current manifest. We therefore make the codec self-describing: every pack
// blob this store writes carries a fixed-size header naming its codec, and the
// reader strips the header and decompresses accordingly. Legacy packs (written
// before compression existed) have no header — their first bytes are arbitrary
// pack content — so the reader treats a missing/short magic as "uncompressed,
// no header" and returns the bytes untouched (backward compatible).

// packMagic is the 4-byte sentinel prefixing every compression-aware pack
// header. Chosen to be extremely unlikely to occur at the start of a legacy
// (header-less) pack: "CPK" + version byte 1.
var packMagic = [4]byte{'C', 'P', 'K', 1}

// packHeaderLen is packMagic (4 bytes) + one codec byte.
const packHeaderLen = len(packMagic) + 1

const (
	packCodecNone byte = 0
	packCodecZstd byte = 1
	packCodecGzip byte = 2
	// packCodecChunked marks a pack whose chunks are each compressed
	// INDEPENDENTLY, with a self-describing index mapping uncompressed content
	// ranges to stored byte ranges. See "per-chunk pack framing" below.
	packCodecChunked byte = 3
)

func packCodecByte(algo CompressionAlgo) byte {
	switch algo {
	case CompressGzip:
		return packCodecGzip
	case CompressZstd:
		return packCodecZstd
	default:
		return packCodecNone
	}
}

func packAlgoFromByte(b byte) (CompressionAlgo, bool) {
	switch b {
	case packCodecNone:
		return CompressNone, true
	case packCodecZstd:
		return CompressZstd, true
	case packCodecGzip:
		return CompressGzip, true
	default:
		return CompressNone, false
	}
}

// compressPackBlob returns the on-disk bytes for a pack whose UNCOMPRESSED
// content is raw, applying algo. The returned bytes are framed with a
// self-describing header (packMagic + codec byte) so the reader can recover the
// codec. When algo == CompressNone the body is raw with a none-codec header —
// this keeps newly-written packs uniformly framed while remaining cheap. The
// pack's identity (its hash / blob ID) is computed by the caller over raw,
// BEFORE this call, so compression never affects dedup or GC.
func compressPackBlob(raw []byte, algo CompressionAlgo) ([]byte, error) {
	header := make([]byte, packHeaderLen)
	copy(header, packMagic[:])
	header[len(packMagic)] = packCodecByte(algo)

	if algo == CompressNone {
		return append(header, raw...), nil
	}

	var buf bytes.Buffer
	buf.Grow(len(raw)/2 + packHeaderLen)
	buf.Write(header)
	cw, err := newCompressor(&buf, algo)
	if err != nil {
		return nil, fmt.Errorf("snapshot/compress: pack compressor (%s): %w", algo, err)
	}
	if _, err := cw.Write(raw); err != nil {
		_ = cw.Close()
		return nil, fmt.Errorf("snapshot/compress: write pack (%s): %w", algo, err)
	}
	if err := cw.Close(); err != nil {
		return nil, fmt.Errorf("snapshot/compress: close pack compressor (%s): %w", algo, err)
	}
	return buf.Bytes(), nil
}

// packFraming describes how a stored pack blob is framed, recovered by
// inspecting only its leading header bytes. It lets the ranged-read path decide,
// from a tiny prefix, whether it may slice chunk bytes directly out of a ranged
// GET (uncompressed packs) or must fall back to a whole-pack fetch + decompress
// (compressed packs, whose on-disk bytes are a single contiguous stream that
// cannot be sliced by uncompressed-content offset).
type packFraming struct {
	// algo is the codec the pack body is stored under. CompressNone means the
	// body is the raw uncompressed pack content and may be sliced directly;
	// packAlgoChunked means per-chunk framing (see chunked/bodyBase below).
	algo CompressionAlgo
	// contentBase is the on-disk byte offset at which the UNCOMPRESSED pack
	// content begins. For a self-describing (magic) header it is packHeaderLen;
	// for a legacy header-less uncompressed pack it is 0. Only meaningful when
	// algo == CompressNone.
	contentBase int
	// chunkedCodec is the algorithm a per-chunk-framed pack was compressed under,
	// recovered from the header's logical codec byte. Distinct from the per-entry
	// codecs (an entry falls back to raw when compression does not shrink it) and
	// from algo (which is the packAlgoChunked marker). Only meaningful when
	// algo == packAlgoChunked.
	chunkedCodec CompressionAlgo
	// indexLen is the byte length of the per-chunk index. Only meaningful when
	// algo == packAlgoChunked; set by detectPackFraming from the header so the
	// caller knows how many bytes to fetch for the index itself.
	indexLen int
	// bodyBase is the on-disk byte offset at which the chunk bodies begin
	// (packChunkedHeaderLen + indexLen). Entry storedOffsets are relative to it.
	// Only meaningful when algo == packAlgoChunked.
	bodyBase int
	// chunked is the parsed per-chunk index, in ascending uncompressed-offset
	// order. Non-nil only for a fully-resolved per-chunk framing; detectPackFraming
	// leaves it nil (it sees only the header) and the reader fills it in after
	// fetching indexLen bytes at packChunkedHeaderLen.
	chunked []packChunkEntry
}

// detectPackFraming inspects the leading bytes of a stored pack to recover its
// framing without decompressing. prefix must contain at least the first
// packHeaderLen bytes of the stored blob when the blob is that long; a shorter
// prefix is only valid for blobs that are themselves shorter than the header
// (legacy header-less packs). It mirrors decompressPackBlob's header rules so
// the ranged and whole-pack paths agree byte-for-byte on every pack.
func detectPackFraming(prefix []byte) (packFraming, error) {
	if len(prefix) < packHeaderLen || !bytes.Equal(prefix[:len(packMagic)], packMagic[:]) {
		// No recognised header: a legacy, header-less, uncompressed pack whose
		// content begins at byte 0.
		return packFraming{algo: CompressNone, contentBase: 0}, nil
	}
	if prefix[len(packMagic)] == packCodecChunked {
		// Per-chunk framing: the header carries the index length, so the caller
		// knows how many more bytes to fetch before it can map offsets.
		if len(prefix) < packChunkedHeaderLen {
			return packFraming{}, errPackChunkedShortPrefix
		}
		logical, ok := packAlgoFromByte(prefix[packHeaderLen])
		if !ok {
			return packFraming{}, fmt.Errorf("snapshot/compress: unknown per-chunk pack logical codec byte %d", prefix[packHeaderLen])
		}
		indexLen := int(binary.BigEndian.Uint32(prefix[packHeaderLen+1 : packChunkedHeaderLen]))
		if indexLen < 0 || indexLen%packChunkEntryLen != 0 {
			return packFraming{}, fmt.Errorf("snapshot/compress: per-chunk pack index length %d is not a multiple of %d", indexLen, packChunkEntryLen)
		}
		return packFraming{
			algo:         packAlgoChunked,
			chunkedCodec: logical,
			indexLen:     indexLen,
			bodyBase:     packChunkedHeaderLen + indexLen,
		}, nil
	}
	algo, ok := packAlgoFromByte(prefix[len(packMagic)])
	if !ok {
		return packFraming{}, fmt.Errorf("snapshot/compress: unknown pack codec byte %d", prefix[len(packMagic)])
	}
	// Framed pack: uncompressed content (or the compressed body) starts right
	// after the fixed-size header.
	return packFraming{algo: algo, contentBase: packHeaderLen}, nil
}

// decompressPackBlob is the inverse of compressPackBlob. It inspects the stored
// bytes for the self-describing header and returns the original UNCOMPRESSED
// pack content. Bytes lacking the header (legacy packs written before pack
// compression existed) are returned unchanged — backward compatible.
func decompressPackBlob(stored []byte) ([]byte, error) {
	if len(stored) < packHeaderLen || !bytes.Equal(stored[:len(packMagic)], packMagic[:]) {
		// No recognised header: a legacy, header-less, uncompressed pack.
		return stored, nil
	}
	if stored[len(packMagic)] == packCodecChunked {
		return decompressChunkedPackBlob(stored)
	}
	algo, ok := packAlgoFromByte(stored[len(packMagic)])
	if !ok {
		return nil, fmt.Errorf("snapshot/compress: unknown pack codec byte %d", stored[len(packMagic)])
	}
	body := stored[packHeaderLen:]
	if algo == CompressNone {
		// Framed-but-uncompressed pack: strip the header, return the body. Copy
		// so callers own a buffer independent of the fetched blob's backing.
		out := make([]byte, len(body))
		copy(out, body)
		return out, nil
	}
	dec, closer, err := newDecompressor(bytes.NewReader(body), algo)
	if err != nil {
		return nil, fmt.Errorf("snapshot/compress: open pack decompressor (%s): %w", algo, err)
	}
	defer closer.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("snapshot/compress: decompress pack (%s): %w", algo, err)
	}
	return out, nil
}

// --- per-chunk pack framing (A1) --------------------------------------------
//
// A whole-pack codec stream cannot be indexed by an uncompressed-content offset,
// so a compressed pack forces a whole-pack fetch + decompress to serve any chunk
// (TECH_DEBT #18). Per-chunk framing removes that: each chunk in the pack is
// compressed INDEPENDENTLY and the pack carries an index mapping each chunk's
// uncompressed content range to its stored byte range, so a run of chunks maps to
// one contiguous ranged GET that decompresses chunk-by-chunk.
//
// The index has to live in the pack (not the manifest) for the same reason the
// codec byte does: a v2 manifest can reference packs written by OTHER Puts via
// cross-object dedup, so the reader must recover the physical layout from the
// pack blob alone.
//
// Layout:
//
//	offset 0                    packMagic {'C','P','K',1}     (4 bytes)
//	offset 4                    packCodecChunked              (1 byte)
//	offset 5                    logical codec byte            (1 byte)
//	offset 6                    u32be indexLen                (4 bytes)
//	offset 10                   index: indexLen bytes, packChunkEntryLen each,
//	                            ascending by uncompressed offset
//	offset 10+indexLen          chunk bodies, back-to-back, same order
//
// The logical codec byte records the algorithm the pack was compressed under, as
// distinct from the per-entry codecs (an individual entry falls back to raw when
// compression does not shrink it). It exists so codec-preserving consumers — the
// compactor's repack in particular — can recover the pack's compression class
// even when every entry happens to be stored raw.
//
// Pack identity is unaffected: the pack hash, the blob ID, every dedup-index
// entry and the GC live set are all still computed over the UNCOMPRESSED pack
// content, exactly as for the whole-pack codecs. Only the stored bytes change
// shape.

// packChunkedHeaderLen is the fixed prefix of a per-chunk-framed pack:
// packMagic + codec byte + logical codec byte + u32 index length.
const packChunkedHeaderLen = packHeaderLen + 1 + 4

// packChunkEntryLen is the wire size of one index entry:
// u64 uncompressed offset, u32 uncompressed size, u64 stored offset,
// u32 stored size, u8 per-entry codec.
const packChunkEntryLen = 8 + 4 + 8 + 4 + 1

// packAlgoChunked is an in-memory marker for per-chunk framing. It is never
// written to a blob (the blob carries packCodecChunked in its header byte) and is
// never passed to the compressor/decompressor factories; it exists so packFraming
// can distinguish "per-chunk framed" from the whole-body codecs.
const packAlgoChunked CompressionAlgo = "__per_chunk__"

// errPackChunkedShortPrefix reports that detectPackFraming saw the per-chunk codec
// but was handed fewer than packChunkedHeaderLen bytes, so it could not read the
// index length. The caller re-probes with a longer prefix.
var errPackChunkedShortPrefix = errors.New("snapshot/compress: per-chunk pack header needs more bytes")

// packSpan is one chunk's extent within the UNCOMPRESSED pack content. The pack
// writers already track exactly this (putV2/putBatchV2 packEntries, compact's
// stagedChunk), so building the index costs nothing extra.
type packSpan struct {
	offset int
	size   int
}

// packChunkEntry is one parsed index entry.
type packChunkEntry struct {
	uncOffset int // offset of this chunk in the uncompressed pack content
	uncSize   int // length of this chunk uncompressed
	stoOffset int // offset of the stored body, RELATIVE to bodyBase
	stoSize   int // length of the stored body
	codec     byte
}

// compressPackBlobChunked returns the on-disk bytes for a pack whose UNCOMPRESSED
// content is raw, compressing each span independently under algo and emitting the
// self-describing per-chunk index described above.
//
// spans must tile raw exactly: ascending, contiguous, starting at 0 and ending at
// len(raw). That is what every caller produces (a pack buffer is built by
// appending whole chunks back-to-back), and it is what makes whole-pack
// reassembly in decompressChunkedPackBlob byte-exact.
//
// A span whose compressed form is not smaller than its raw form is stored raw
// under packCodecNone, so the worst case is bounded by index overhead rather than
// by codec expansion.
func compressPackBlobChunked(raw []byte, spans []packSpan, algo CompressionAlgo) ([]byte, error) {
	if algo == CompressNone {
		// Nothing to gain: an uncompressed pack is already range-readable through
		// the plain framed path, and that path costs no index.
		return compressPackBlob(raw, algo)
	}
	if err := validatePackSpans(raw, spans); err != nil {
		return nil, err
	}

	entries := make([]packChunkEntry, len(spans))
	bodies := make([][]byte, len(spans))
	stoOff := 0
	for i, sp := range spans {
		chunk := raw[sp.offset : sp.offset+sp.size]
		body, codec, err := compressChunkBody(chunk, algo)
		if err != nil {
			return nil, err
		}
		entries[i] = packChunkEntry{
			uncOffset: sp.offset,
			uncSize:   sp.size,
			stoOffset: stoOff,
			stoSize:   len(body),
			codec:     codec,
		}
		bodies[i] = body
		stoOff += len(body)
	}

	indexLen := len(entries) * packChunkEntryLen
	out := make([]byte, 0, packChunkedHeaderLen+indexLen+stoOff)
	out = append(out, packMagic[:]...)
	out = append(out, packCodecChunked)
	out = append(out, packCodecByte(algo))
	out = binary.BigEndian.AppendUint32(out, uint32(indexLen))
	for _, e := range entries {
		out = binary.BigEndian.AppendUint64(out, uint64(e.uncOffset))
		out = binary.BigEndian.AppendUint32(out, uint32(e.uncSize))
		out = binary.BigEndian.AppendUint64(out, uint64(e.stoOffset))
		out = binary.BigEndian.AppendUint32(out, uint32(e.stoSize))
		out = append(out, e.codec)
	}
	for _, b := range bodies {
		out = append(out, b...)
	}
	return out, nil
}

// validatePackSpans enforces the exact-tiling contract of compressPackBlobChunked.
func validatePackSpans(raw []byte, spans []packSpan) error {
	if len(spans) == 0 {
		if len(raw) != 0 {
			return fmt.Errorf("snapshot/compress: per-chunk pack has %d bytes but no spans", len(raw))
		}
		return nil
	}
	next := 0
	for i, sp := range spans {
		if sp.size < 0 || sp.offset != next {
			return fmt.Errorf("snapshot/compress: per-chunk pack span %d (offset=%d size=%d) does not tile at %d",
				i, sp.offset, sp.size, next)
		}
		next += sp.size
	}
	if next != len(raw) {
		return fmt.Errorf("snapshot/compress: per-chunk pack spans cover %d bytes, pack has %d", next, len(raw))
	}
	return nil
}

// compressChunkBody compresses one chunk, falling back to the raw bytes (codec
// none) whenever compression does not actually shrink it.
func compressChunkBody(chunk []byte, algo CompressionAlgo) ([]byte, byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(chunk)/2 + 1)
	cw, err := newCompressor(&buf, algo)
	if err != nil {
		return nil, 0, fmt.Errorf("snapshot/compress: per-chunk compressor (%s): %w", algo, err)
	}
	if _, err := cw.Write(chunk); err != nil {
		_ = cw.Close()
		return nil, 0, fmt.Errorf("snapshot/compress: write per-chunk body (%s): %w", algo, err)
	}
	if err := cw.Close(); err != nil {
		return nil, 0, fmt.Errorf("snapshot/compress: close per-chunk compressor (%s): %w", algo, err)
	}
	if buf.Len() >= len(chunk) {
		return chunk, packCodecNone, nil
	}
	return buf.Bytes(), packCodecByte(algo), nil
}

// parsePackChunkIndex decodes indexLen bytes of per-chunk index and validates that
// the entries tile the uncompressed content contiguously from 0 and that stored
// bodies are ascending and non-overlapping. Both invariants are what let the
// reader translate an uncompressed window into one contiguous byte range.
func parsePackChunkIndex(b []byte) ([]packChunkEntry, error) {
	if len(b)%packChunkEntryLen != 0 {
		return nil, fmt.Errorf("snapshot/compress: per-chunk index length %d is not a multiple of %d", len(b), packChunkEntryLen)
	}
	n := len(b) / packChunkEntryLen
	entries := make([]packChunkEntry, n)
	nextUnc, nextSto := 0, 0
	for i := range n {
		off := i * packChunkEntryLen
		e := packChunkEntry{
			uncOffset: int(binary.BigEndian.Uint64(b[off : off+8])),
			uncSize:   int(binary.BigEndian.Uint32(b[off+8 : off+12])),
			stoOffset: int(binary.BigEndian.Uint64(b[off+12 : off+20])),
			stoSize:   int(binary.BigEndian.Uint32(b[off+20 : off+24])),
			codec:     b[off+24],
		}
		if e.uncOffset != nextUnc || e.uncSize < 0 {
			return nil, fmt.Errorf("snapshot/compress: per-chunk index entry %d (offset=%d size=%d) does not tile at %d: %w",
				i, e.uncOffset, e.uncSize, nextUnc, ErrCorrupt)
		}
		if e.stoOffset != nextSto || e.stoSize < 0 {
			return nil, fmt.Errorf("snapshot/compress: per-chunk index entry %d stored range (offset=%d size=%d) is not contiguous at %d: %w",
				i, e.stoOffset, e.stoSize, nextSto, ErrCorrupt)
		}
		if _, ok := packAlgoFromByte(e.codec); !ok {
			return nil, fmt.Errorf("snapshot/compress: per-chunk index entry %d has unknown codec byte %d: %w", i, e.codec, ErrCorrupt)
		}
		nextUnc += e.uncSize
		nextSto += e.stoSize
		entries[i] = e
	}
	return entries, nil
}

// decompressChunkedPackBlob reassembles the full UNCOMPRESSED content of a
// per-chunk-framed pack. This is what keeps every whole-pack consumer (GC,
// compaction, integrity verification, the EOF-boundary fallback) working against
// the new codec without changes.
func decompressChunkedPackBlob(stored []byte) ([]byte, error) {
	framing, err := detectPackFraming(stored)
	if err != nil {
		return nil, err
	}
	if len(stored) < framing.bodyBase {
		return nil, fmt.Errorf("snapshot/compress: per-chunk pack truncated: %d bytes, index needs %d: %w",
			len(stored), framing.bodyBase, ErrCorrupt)
	}
	entries, err := parsePackChunkIndex(stored[packChunkedHeaderLen:framing.bodyBase])
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	body := stored[framing.bodyBase:]
	total := entries[len(entries)-1].uncOffset + entries[len(entries)-1].uncSize
	out := make([]byte, total)
	for i, e := range entries {
		if err := inflateChunkEntry(e, body, out[e.uncOffset:e.uncOffset+e.uncSize], i); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// inflateChunkEntry decodes one index entry's stored body (found at e.stoOffset
// within body) into dst, which must be exactly e.uncSize long.
func inflateChunkEntry(e packChunkEntry, body []byte, dst []byte, idx int) error {
	end := e.stoOffset + e.stoSize
	if e.stoOffset < 0 || end > len(body) {
		return fmt.Errorf("snapshot/compress: per-chunk entry %d stored range [%d,%d) exceeds body length %d: %w",
			idx, e.stoOffset, end, len(body), ErrCorrupt)
	}
	src := body[e.stoOffset:end]
	algo, ok := packAlgoFromByte(e.codec)
	if !ok {
		return fmt.Errorf("snapshot/compress: per-chunk entry %d unknown codec byte %d: %w", idx, e.codec, ErrCorrupt)
	}
	if algo == CompressNone {
		if len(src) != len(dst) {
			return fmt.Errorf("snapshot/compress: per-chunk entry %d raw body is %d bytes, index says %d: %w",
				idx, len(src), len(dst), ErrCorrupt)
		}
		copy(dst, src)
		return nil
	}
	dec, closer, derr := newDecompressor(bytes.NewReader(src), algo)
	if derr != nil {
		return fmt.Errorf("snapshot/compress: open per-chunk decompressor (%s): %w", algo, derr)
	}
	defer closer.Close()
	if _, rerr := io.ReadFull(dec, dst); rerr != nil {
		return fmt.Errorf("snapshot/compress: decompress per-chunk entry %d (%s): %w", idx, algo, rerr)
	}
	// A well-formed entry decodes to exactly uncSize bytes; trailing bytes mean
	// the index disagrees with the body.
	var probe [1]byte
	if n, _ := dec.Read(probe[:]); n != 0 {
		return fmt.Errorf("snapshot/compress: per-chunk entry %d decodes to more than the indexed %d bytes: %w",
			idx, len(dst), ErrCorrupt)
	}
	return nil
}

// logicalCodec returns the pack's compression CLASS — what a codec-preserving
// consumer (the compactor's repack) must re-apply. For per-chunk framing that is
// the header's logical codec byte rather than the packAlgoChunked marker; for
// every other framing it is the body codec itself.
func (f packFraming) logicalCodec() CompressionAlgo {
	if f.algo == packAlgoChunked {
		return f.chunkedCodec
	}
	return f.algo
}

// chunkedStoredSpan maps an uncompressed content window [minOff, maxEnd) onto the
// contiguous STORED byte range covering it, plus the index range of the entries
// involved. The window must begin and end on entry boundaries — which it always
// does, because every chunkRef names a whole chunk and every entry is a whole
// chunk.
func (f packFraming) chunkedStoredSpan(minOff, maxEnd int) (storedOff, storedLen, first, last int, err error) {
	first = -1
	for i, e := range f.chunked {
		if e.uncOffset == minOff {
			first = i
			break
		}
	}
	if first < 0 {
		return 0, 0, 0, 0, fmt.Errorf("snapshot/compress: per-chunk window start %d is not an entry boundary: %w", minOff, ErrCorrupt)
	}
	last = first
	for last < len(f.chunked) && f.chunked[last].uncOffset < maxEnd {
		last++
	}
	if last == first {
		return 0, 0, first, first, nil
	}
	endEntry := f.chunked[last-1]
	if endEntry.uncOffset+endEntry.uncSize != maxEnd {
		return 0, 0, 0, 0, fmt.Errorf("snapshot/compress: per-chunk window end %d is not an entry boundary: %w", maxEnd, ErrCorrupt)
	}
	storedOff = f.chunked[first].stoOffset
	storedLen = endEntry.stoOffset + endEntry.stoSize - storedOff
	return storedOff, storedLen, first, last, nil
}

// materializeChunkedWindow decodes entries[first:last] out of storedWindow — the
// bytes fetched for the range chunkedStoredSpan returned — into one contiguous
// buffer of uncompressed content, based at entries[first].uncOffset.
func materializeChunkedWindow(entries []packChunkEntry, first, last int, storedWindow []byte, storedBase int) ([]byte, error) {
	if first >= last {
		return nil, nil
	}
	base := entries[first].uncOffset
	total := entries[last-1].uncOffset + entries[last-1].uncSize - base
	out := make([]byte, total)
	for i := first; i < last; i++ {
		e := entries[i]
		// Rebase the entry onto the fetched window before decoding.
		shifted := e
		shifted.stoOffset = e.stoOffset - storedBase
		dstOff := e.uncOffset - base
		if err := inflateChunkEntry(shifted, storedWindow, out[dstOff:dstOff+e.uncSize], i); err != nil {
			return nil, err
		}
	}
	return out, nil
}
