// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/kopia/kopia/repo/splitter"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// ChunkedStore is a SnapshotStore decorator that splits each incoming blob
// into content-addressed chunks via a rolling-hash splitter (Buzhash32 at
// 1 MiB target by default), stores each unique chunk in a BlobStore, and
// writes a small manifest (chunk-hash list) as the snapshot's blob in the
// wrapped SnapshotStore.
//
// Restore reads the manifest, fetches each referenced chunk, and streams
// them in order to reassemble the original bytes.
//
// Dedup happens at the chunk level: identical chunks across different
// snapshots of the same tenant share storage in the BlobStore. For
// long-lived sandboxes where most Python heap pages don't change between
// snapshots, this produces large storage savings.
//
// # Pack files (v2)
//
// By default, chunks are grouped into ~16 MiB pack blobs before upload.
// This amortises S3 PUT costs: rather than one PUT per ~1 MiB chunk, a
// 50 MiB snapshot emits ~3 pack PUTs instead of ~50 chunk PUTs — roughly
// 16x fewer PUTs per snapshot at large scale.
//
// Each pack blob has ID "pack-<tenant>-<pack_sha256>". The manifest
// records (pack_hash, offset, length) per chunk so Get can fetch only
// the needed byte ranges. This is the v2 manifest format; v1 manifests
// (written when pack mode was disabled or by older code) are read
// transparently by treating each v1 chunk as a pack-of-one.
//
// Dedup scope is pluggable via ChunkedConfig.Index (a DedupIndex):
//   - PriorManifestIndex (default): before adding a chunk to the current
//     pack, the store checks the latest prior manifest for the same
//     owner_key. If the chunk hash is already recorded there, its existing
//     (pack_id, offset, length) is reused and the bytes are not re-packed.
//     This gives dedup within the snapshot history of a single owner_key
//     (the common case: consecutive checkpoints of one sandbox share most
//     memory pages), with no external store.
//   - GlobalIndex: consults a durable DedupStore so a chunk is reused if any
//     object in the dedup domain has already stored it — cross-owner-key /
//     cross-object dedup (the enterprise distinct-object case). The chunk→pack
//     index this requires lives in the caller-supplied DedupStore.
//
// The dedup domain (ChunkedConfig.DedupDomain) is the isolation boundary and
// the prefix on every chunk/pack blob ID; it defaults to key.Tenant.
//
// Disabling packing: set ChunkedConfig.PackTargetBytes = packingDisabled
// (or env SANDBOX_SNAPSHOT_CHUNKED_PACK_SIZE=0) to revert to v1 behavior
// — one chunk blob per chunk — useful as a rollback path if packing
// causes issues in production.
//
// v1 scope (unchanged):
//   - Per-tenant prefix isolation — chunks/packs for tenant A live under
//     "chunk-A-..." / "pack-A-..." and are never visible to tenant B.
//   - Manifest format: JSON, versioned. Switch to protobuf later if size
//     becomes a problem; manifests for typical workloads are <100 KB.
//   - No automatic garbage collection in this decorator. Use
//     ChunkedStoreGC (separate package file) on a schedule.
//
// Backward compatibility: ChunkedStore is OPT-IN via main.go wiring.
// Snapshots written without ChunkedStore are stored as opaque blobs in
// the upstream SnapshotStore and remain readable. Snapshots written WITH
// ChunkedStore are NOT readable by callers that bypass ChunkedStore
// (their blobs are manifests, not the original bytes).
type ChunkedStore struct {
	upstream        SnapshotStore
	chunks          blobstore.Storage
	deduper         *blobstore.InflightDeduper // coalesces concurrent identical pack/chunk uploads
	splitter        splitter.Factory
	chunkSize       int // approximate target, exposed for tests
	packTargetBytes int // packingDisabled = disabled (v1 behavior)

	// dedupDomain overrides key.Tenant as the blob-ID prefix and dedup-index
	// scope. Empty means per-tenant (key.Tenant), which is the original
	// behavior. See ChunkedConfig.DedupDomain.
	dedupDomain string
	// index is the dedup strategy consulted on the v2 write path. Never nil
	// after NewChunkedStore (defaults to a PriorManifestIndex). See DedupIndex.
	index DedupIndex
	// policy decides, per Put, whether pack/chunk blobs are compressed and with
	// which codec, from the object's content type. Never nil after
	// NewChunkedStore (defaults to a no-compression policy for backward
	// compatibility — compression is opt-in via ChunkedConfig.CompressionPolicy).
	// Compression is applied to pack BLOB bytes only; chunk content hashing,
	// dedup, pack identity, and GC are all computed over the uncompressed bytes,
	// so enabling compression never changes which chunks dedup. See
	// CompressionPolicy and compressPackBlob.
	policy CompressionPolicy
	// packCompression selects the framing used for compressed packs (per-chunk
	// by default, whole-pack as the rollback). See PackCompressionMode.
	packCompression PackCompressionMode

	// framingCache memoizes each pack's header framing decision keyed by pack
	// blob ID (which embeds the content-addressed, immutable pack hash). Because
	// a pack hash is permanently bound to one set of bytes, its framing never
	// changes, so the tiny header probe (framingFor) runs at most once per pack
	// per process — across ALL readers and all Get/ReadAt calls — instead of
	// once per reader. Readers consult this shared cache before probing.
	//
	// Growth bound: one small entry (a packFraming is a string + int) per
	// distinct pack hash ever read by this process. Pack hashes are bounded by
	// the domain's stored pack set, so a plain map is acceptable here; we do not
	// evict. A reader-local fallback map still backs the per-reader path when a
	// store is absent (direct newChunkStreamReader construction in tests).
	framingCache sync.Map // map[blobstore.ID]packFraming
}

// ChunkedConfig configures a ChunkedStore. SplitterName names a kopia
// splitter factory (see kopia/repo/splitter/splitter.go for the registered
// names). "DYNAMIC-1M-BUZHASH" is the recommended default.
//
// PackTargetBytes controls pack-file grouping. The zero value uses the
// default (16 MiB). Set to packingDisabled (-1) to disable packing
// entirely and write the v1 single-chunk-per-blob format. The public
// env-var interface uses 0 to mean "use default" and a negative value to
// mean "disabled"; see main.go for the translation.
type ChunkedConfig struct {
	SplitterName    string // default: "DYNAMIC-1M-BUZHASH"
	PackTargetBytes int    // 0 = use default (16 MiB); packingDisabled (-1) = no packing

	// DedupDomain is the isolation boundary for dedup and the prefix on every
	// chunk/pack blob ID. The zero value means per-tenant (the blob ID prefix
	// is key.Tenant, matching the original behavior) — leave it empty for
	// byte-identical, isolation-first storage. Set it to a wider scope (e.g.
	// "the enterprise instance") to dedup identical content across tenants
	// within a single trust domain. Distinct domains never dedup against each
	// other and never share blobs.
	DedupDomain string

	// Index selects the dedup strategy on the v2 (pack) write path. A nil
	// Index uses the default PriorManifestIndex (dedup only against the latest
	// prior snapshot of the same key) — the original behavior. Supply a
	// GlobalIndex (backed by a DedupStore) to dedup across all objects in the
	// dedup domain.
	Index DedupIndex

	// CompressionPolicy decides, per Put, whether the object's pack blobs are
	// compressed and with which codec, from the object's content type
	// (SnapshotMetadata.Format) plus the optional store_uncompressed tag. A nil
	// policy disables compression entirely (every pack stored uncompressed) —
	// the original behavior, kept as the default for backward compatibility.
	// Supply NewContentTypePolicy(CompressZstd) to compress generic/text-like
	// content while storing already-compressed and GPU-loadable classes
	// uncompressed. Compression is dedup-safe: it shrinks only the physical pack
	// bytes; chunk hashing, dedup, and pack identity stay on the original bytes.
	CompressionPolicy CompressionPolicy

	// PackCompression selects the physical framing used when a pack IS
	// compressed (it has no effect on packs the policy stores uncompressed). The
	// zero value selects per-chunk framing, which keeps compressed packs
	// range-readable. See PackCompressionMode.
	PackCompression PackCompressionMode
}

// PackCompressionMode selects how a compressed pack is framed on disk. Both
// modes are dedup-safe and produce identical uncompressed content; they differ
// only in whether a compressed pack can be ranged into.
//
// The reader understands every mode regardless of which one the writer emits, so
// switching is a pure configuration change with no migration: existing packs stay
// readable and newly written packs simply take the new framing.
type PackCompressionMode int

const (
	// PackCompressPerChunk compresses each chunk in the pack independently and
	// records a self-describing index, so a run of chunks can be served by one
	// ranged GET instead of a whole-pack fetch + decompress (TECH_DEBT #18). It
	// trades a little compression ratio (no cross-chunk redundancy, plus ~25
	// bytes of index per chunk) for range-readability. Default.
	PackCompressPerChunk PackCompressionMode = 0
	// PackCompressWholePack compresses the whole pack as one codec stream — the
	// original framing. Better ratio, but no ranged read is possible: every chunk
	// read fetches and decompresses the entire pack. Kept as the rollback path.
	PackCompressWholePack PackCompressionMode = 1
)

// String implements fmt.Stringer so daemons can log the resolved mode.
func (m PackCompressionMode) String() string {
	switch m {
	case PackCompressWholePack:
		return "whole-pack"
	default:
		return "per-chunk"
	}
}

// ParsePackCompressionMode accepts the env/flag spelling of a pack-compression
// framing and returns the typed value. Empty input maps to PackCompressPerChunk
// (the default). Unknown input is an error. Mirrors ParseCompressionAlgo.
//
// Note this only affects packs the CompressionPolicy decided to COMPRESS. Packs
// stored uncompressed (the GPU-loadable tensor and already-compressed media
// classes, or a store_uncompressed tag) are framed identically either way.
func ParsePackCompressionMode(s string) (PackCompressionMode, error) {
	switch s {
	case "", "per-chunk", "perchunk", "chunk":
		return PackCompressPerChunk, nil
	case "whole-pack", "wholepack", "whole", "pack":
		return PackCompressWholePack, nil
	default:
		return 0, fmt.Errorf("snapshot: unknown pack compression mode %q (valid: per-chunk, whole-pack)", s)
	}
}

// noCompressionPolicy is the default policy: it never compresses, preserving
// the pre-compression behavior for callers that don't opt in.
type noCompressionPolicy struct{}

func (noCompressionPolicy) CodecFor(string) CompressionAlgo { return CompressNone }

// defaultPackTargetBytes is ~16 MiB, matching the architect's recommendation
// and xet-core's "xorb" target size. At 1 MiB average chunk size this
// groups roughly 16 chunks per pack, cutting S3 PUTs ~16x.
const defaultPackTargetBytes = 16 * 1024 * 1024

// packingDisabled is the unexported sentinel for ChunkedConfig.PackTargetBytes
// meaning "no pack grouping; write v1 single-chunk-per-blob format". The zero
// value of ChunkedConfig.PackTargetBytes means "use the default" instead, so
// we need this explicit sentinel.
const packingDisabled = -1

// PackingDisabled is the exported value callers (main.go env parsing) should
// pass as ChunkedConfig.PackTargetBytes to disable pack grouping entirely and
// fall back to the v1 single-chunk-per-blob format. Equal to -1.
const PackingDisabled = packingDisabled

// NewChunkedStore constructs a ChunkedStore. Returns an error if the
// splitter name is unknown.
func NewChunkedStore(upstream SnapshotStore, chunks blobstore.Storage, cfg ChunkedConfig) (*ChunkedStore, error) {
	name := cfg.SplitterName
	if name == "" {
		name = "DYNAMIC-1M-BUZHASH"
	}
	factory := splitter.GetFactory(name)
	if factory == nil {
		return nil, fmt.Errorf("chunked store: unknown splitter %q (valid examples: DYNAMIC-1M-BUZHASH, DYNAMIC-256K-BUZHASH, FIXED-1M)", name)
	}
	packTarget := cfg.PackTargetBytes
	if packTarget == 0 {
		packTarget = defaultPackTargetBytes
	}
	index := cfg.Index
	if index == nil {
		// Default: dedup only against the latest prior snapshot of the same
		// key — the original sandbox-provider behavior, no external store.
		index = NewPriorManifestIndex(upstream)
	}
	policy := cfg.CompressionPolicy
	if policy == nil {
		// Default: no compression, preserving pre-compression behavior. Callers
		// opt in via ChunkedConfig.CompressionPolicy.
		policy = noCompressionPolicy{}
	}
	return &ChunkedStore{
		upstream:        upstream,
		chunks:          chunks,
		deduper:         blobstore.NewInflightDeduper(chunks),
		splitter:        factory,
		chunkSize:       factory().MaxSegmentSize(), // approximate
		packTargetBytes: packTarget,
		dedupDomain:     cfg.DedupDomain,
		index:           index,
		policy:          policy,
		packCompression: cfg.PackCompression,
	}, nil
}

// storePackBlob frames a pack for storage under codec, honouring the configured
// PackCompressionMode. spans are the chunk extents within raw (required for
// per-chunk framing; ignored otherwise). It is the single seam every pack writer
// goes through, so the framing decision lives in exactly one place.
//
// Pack identity is computed by the caller over raw BEFORE this call, so the
// framing choice never affects dedup, the dedup index, or GC.
func (c *ChunkedStore) storePackBlob(raw []byte, spans []packSpan, codec CompressionAlgo) ([]byte, error) {
	if codec == CompressNone || c.packCompression == PackCompressWholePack {
		return compressPackBlob(raw, codec)
	}
	return compressPackBlobChunked(raw, spans, codec)
}

// packSpansFromEntries builds the chunk-extent list per-chunk pack framing needs
// from whatever shape the caller already tracks, via an (offset, size) accessor.
// Every pack writer records those two numbers per buffered chunk already, so this
// adapter keeps the framing seam free of writer-specific entry types.
func packSpansFromEntries(n int, at func(i int) (offset, size int)) []packSpan {
	spans := make([]packSpan, n)
	for i := range n {
		off, size := at(i)
		spans[i] = packSpan{offset: off, size: size}
	}
	return spans
}

// codecFor resolves the compression codec to apply to this Put's pack/chunk
// blobs. The store_uncompressed tag is an unconditional opt-out (the caller
// knows the payload must stay byte-for-byte on disk, e.g. a GPU-loadable
// tensor the content type doesn't reveal); otherwise the policy decides from
// the content type, which callers carry in SnapshotMetadata.Format (blobgw
// populates it from the HTTP Content-Type).
func (c *ChunkedStore) codecFor(meta SnapshotMetadata) CompressionAlgo {
	if meta.Tags[TagStoreUncompressed] != "" {
		return CompressNone
	}
	return c.policy.CodecFor(meta.Format)
}

// domainFor returns the effective dedup domain for key: the configured
// DedupDomain when set, otherwise the key's tenant (the original per-tenant
// behavior). This single value prefixes every chunk/pack blob ID and scopes
// the dedup index, so it is also what gets stored in the manifest's Tenant
// field and used by the reader and GC to reconstruct blob IDs.
func (c *ChunkedStore) domainFor(key SnapshotKey) string {
	if c.dedupDomain != "" {
		return c.dedupDomain
	}
	return key.Tenant
}

// packSizeRecorderProvider is implemented by a DedupIndex (GlobalIndex) whose
// durable store can record per-pack physical (compressed) sizes for storage
// accounting. It is an OPTIONAL, local seam: the pack-write and compaction
// paths type-assert c.index for it and record best-effort when present, so the
// PriorManifestIndex path (and any index without a durable PackSizeRecorder
// store) is entirely unaffected.
type packSizeRecorderProvider interface {
	packSizeRecorder() PackSizeRecorder
}

// packSizeRecorder returns the durable store's PackSizeRecorder when the
// configured index exposes one, else nil. Callers record per-pack physical
// sizes best-effort (log-and-continue on error), mirroring how a dedup Record
// failure is treated: it never fails a Put.
func (c *ChunkedStore) packSizeRecorder() PackSizeRecorder {
	if p, ok := c.index.(packSizeRecorderProvider); ok {
		return p.packSizeRecorder()
	}
	return nil
}

// manifestFormatV1 is the legacy single-chunk-per-blob format.
const manifestFormatV1 = "sandbox-chunked-v1"

// manifestFormatV2 is the pack-file format. Chunk refs include pack
// blob ID (hash), offset within the pack, and length.
const manifestFormatV2 = "sandbox-chunked-v2"

// chunkManifest is the per-snapshot manifest written to the upstream
// SnapshotStore. It lists chunk references in original-stream order so
// Get can concatenate them to recover the source bytes.
//
// The Format field distinguishes v1 (no pack) and v2 (pack) layouts.
type chunkManifest struct {
	// Format is "sandbox-chunked-v1" or "sandbox-chunked-v2".
	Format string `json:"format"`

	// TotalSize is the original blob size in bytes, BEFORE chunking.
	TotalSize int64 `json:"total_size"`

	// Tenant is the per-tenant prefix all chunk/pack blob IDs use.
	Tenant string `json:"tenant"`

	// Chunks lists every chunk in original-stream order.
	Chunks []chunkRef `json:"chunks"`
}

// chunkRef describes one chunk within a manifest.
//
// v1 format: only Hash and Size are set. The chunk blob ID is
// "chunk-<tenant>-<Hash>" and the chunk lives at offset 0.
//
// v2 format: all four fields are set. PackHash is the sha256 of the
// pack blob; the full blob ID is "pack-<tenant>-<PackHash>". Offset is
// the byte offset within the pack, Size is the chunk byte length.
type chunkRef struct {
	Hash     string `json:"h"`           // sha256 hex of chunk content
	PackHash string `json:"p,omitempty"` // sha256 hex of pack blob (v2 only)
	Offset   int    `json:"o,omitempty"` // byte offset within pack (v2 only)
	Size     int    `json:"s"`           // chunk byte length
}

// packBlobID encodes the (tenant, packHash) pair as a per-tenant BlobID.
// Format: pack-<tenant>-<hash>
func packBlobID(tenant, hash string) blobstore.ID {
	if tenant == "" {
		tenant = "_"
	}
	return blobstore.ID("pack-" + tenant + "-" + hash)
}

// packTenantPrefix returns the ListBlobs prefix for enumerating one
// tenant's pack blobs.
func packTenantPrefix(tenant string) blobstore.ID {
	if tenant == "" {
		tenant = "_"
	}
	return blobstore.ID("pack-" + tenant + "-")
}

// chunkBlobID encodes the (tenant, hash) pair as a per-tenant BlobID.
// We deliberately use a hyphen-delimited flat encoding (no slashes) so
// kopia's sharded filesystem backend can prefix-list within a tenant
// using a single string comparison — slashes are interpreted as path
// separators by the sharded backend, which complicates prefix walks.
//
// Format: chunk-<tenant>-<hash>
// Listing prefix: chunk-<tenant>- (lists all chunks belonging to tenant)
//
// Tenant scoping prevents cross-tenant dedup AND the side-channel /
// compliance issues that would follow (see architect review §Q3).
func chunkBlobID(tenant, hash string) blobstore.ID {
	if tenant == "" {
		tenant = "_"
	}
	return blobstore.ID("chunk-" + tenant + "-" + hash)
}

// chunkTenantPrefix returns the ListBlobs prefix for enumerating one
// tenant's chunks. Mark-and-sweep GC and per-tenant accounting use this.
func chunkTenantPrefix(tenant string) blobstore.ID {
	if tenant == "" {
		tenant = "_"
	}
	return blobstore.ID("chunk-" + tenant + "-")
}

// Put implements SnapshotStore. Streams r through the splitter, groups
// chunks into pack blobs (when packing is enabled), writes each unique
// pack to the BlobStore, then writes the manifest as the snapshot's blob.
//
// Dedup: before assembling a new pack, we load the latest prior manifest
// for this owner_key (if any) to build an in-memory chunk-hash→chunkRef
// index. Chunks already present in that prior manifest are reused
// (existing pack ID + offset) without re-packing. This gives effective
// dedup for consecutive checkpoints of the same sandbox (the common case).
// Cross-owner-key dedup requires a global index and is deferred to Phase 2.
func (c *ChunkedStore) Put(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	if c.packTargetBytes == packingDisabled {
		return c.putV1(ctx, key, meta, r)
	}
	return c.putV2(ctx, key, meta, r)
}

// putV1 writes chunks as individual blobs (v1 format). Used when packing
// is explicitly disabled (packingDisabled sentinel).
func (c *ChunkedStore) putV1(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	domain := c.domainFor(key)
	codec := c.codecFor(meta)
	m := chunkManifest{
		Format: manifestFormatV1,
		Tenant: domain,
	}

	sp := c.splitter()
	defer sp.Close()

	const readBufSize = 256 * 1024
	readBuf := make([]byte, readBufSize)
	var current []byte // accumulates the chunk being assembled

	emit := func() error {
		if len(current) == 0 {
			return nil
		}
		sum := sha256.Sum256(current)
		hash := hex.EncodeToString(sum[:])
		id := chunkBlobID(domain, hash)
		// Compress the physical blob; the ID/hash above stay on the original
		// bytes so dedup and GC are unaffected. The codec is self-describing in
		// the blob header so the reader recovers it without a manifest change.
		stored, cerr := compressPackBlob(current, codec)
		if cerr != nil {
			return cerr
		}
		if err := c.deduper.PutIfAbsent(ctx, id, blobstore.BytesFromSlice(stored)); err != nil {
			return err
		}
		m.Chunks = append(m.Chunks, chunkRef{Hash: hash, Size: len(current)})
		m.TotalSize += int64(len(current))
		current = current[:0]
		return nil
	}

	// Buffered read loop following kopia's canonical pattern
	// (repo/object/object_writer.go::Write): the splitter is stateful and
	// "consumes" bytes from each slice we pass; we maintain a separate
	// `current` buffer that accumulates chunk content since the last
	// boundary. When the splitter signals a boundary at offset n in the
	// supplied slice, we append [0:n] to `current`, emit the chunk, then
	// continue with data[n:].
	for {
		n, err := r.Read(readBuf)
		if n > 0 {
			data := readBuf[:n]
			for len(data) > 0 {
				cut := sp.NextSplitPoint(data)
				if cut < 0 {
					current = append(current, data...)
					break
				}
				current = append(current, data[:cut]...)
				if emitErr := emit(); emitErr != nil {
					return SnapshotMetadata{}, fmt.Errorf("chunked put: %w", emitErr)
				}
				data = data[cut:]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return SnapshotMetadata{}, fmt.Errorf("chunked put: read input: %w", err)
		}
	}
	// Final partial chunk: whatever's left after EOF.
	if err := emit(); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put: tail chunk: %w", err)
	}

	return c.writeManifest(ctx, key, meta, &m, manifestFormatV1)
}

// putV2 groups chunks into pack blobs (v2 format).
func (c *ChunkedStore) putV2(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	domain := c.domainFor(key)
	codec := c.codecFor(meta)
	m := chunkManifest{
		Format: manifestFormatV2,
		Tenant: domain,
	}

	// Open a dedup session for this Put. The configured DedupIndex decides the
	// dedup scope: the default PriorManifestIndex dedups only against the
	// latest prior snapshot of the same key, while a GlobalIndex dedups across
	// every object in the domain. A chunk the session already knows is reused
	// (its existing pack ref) without re-packing or re-uploading its bytes;
	// newly-packed chunks are recorded and committed once their packs land.
	sess, err := c.index.NewSession(ctx, domain, key)
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put v2: open dedup session: %w", err)
	}

	// psr records each pack's physical (compressed) size for storage accounting.
	// Optional: nil unless the configured index's durable store implements
	// PackSizeRecorder. Recording is best-effort — a failure is logged and the
	// Put continues (it only loses the accounting, never correctness).
	psr := c.packSizeRecorder()

	sp := c.splitter()
	defer sp.Close()

	const readBufSize = 256 * 1024
	readBuf := make([]byte, readBufSize)
	var current []byte // accumulates bytes of the chunk being assembled

	// packBuf accumulates chunks waiting to be flushed as a pack blob.
	// packEntries records, for each chunk currently buffered in packBuf, its
	// offset/size within packBuf AND the index of its (placeholder) chunkRef in
	// m.Chunks. Crucially, m.Chunks is appended in strict original-stream order
	// at emit time — including a placeholder for each not-yet-flushed packed
	// chunk — and flushPack only back-fills the now-known PackHash into those
	// placeholders. This preserves stream order even when deduped chunks (which
	// resolve immediately) are interleaved with packed chunks (which resolve at
	// flush). Appending packed-chunk refs at flush time instead would reorder
	// the manifest and silently corrupt reads (see
	// TestGlobalIndex_PartialMidStreamDedupPreservesOrder).
	type packEntry struct {
		manifestIdx int // index of this chunk's placeholder ref in m.Chunks
		offset      int // byte offset of the chunk within packBuf
		size        int
	}
	var packBuf []byte
	var packEntries []packEntry

	flushPack := func() error {
		if len(packBuf) == 0 {
			return nil
		}
		sum := sha256.Sum256(packBuf)
		ph := hex.EncodeToString(sum[:])
		pid := packBlobID(domain, ph)
		// Compress the physical pack blob. ph (and thus the blob ID and every
		// dedup/manifest reference) is the hash of the UNCOMPRESSED pack, so
		// dedup, the dedup index, and GC are all unaffected — only the bytes
		// written to the blob store shrink. The codec is recorded in a
		// self-describing header so the reader can recover it for deduped
		// cross-Put references without a manifest/schema change.
		stored, cerr := c.storePackBlob(packBuf, packSpansFromEntries(len(packEntries), func(i int) (int, int) {
			return packEntries[i].offset, packEntries[i].size
		}), codec)
		if cerr != nil {
			return cerr
		}
		if err := c.deduper.PutIfAbsent(ctx, pid, blobstore.BytesFromSlice(stored)); err != nil {
			return fmt.Errorf("flush pack: %w", err)
		}
		// Record the pack's physical (compressed) on-disk size — len(stored),
		// after compression — for per-domain storage accounting. One row per pack
		// (never per chunk). Best-effort: log-and-continue, never fail the Put.
		recordPackSize(ctx, psr, domain, ph, len(stored))
		for _, e := range packEntries {
			// Back-fill the pack hash into the placeholder ref appended in
			// stream order at emit time. Hash/Offset/Size were already set.
			m.Chunks[e.manifestIdx].PackHash = ph
			// Record the chunk's location now that its pack is uploaded, so a
			// later object in this domain can reuse it (no-op for the default
			// prior-manifest index). Committed after the final flush.
			sess.Record(ChunkLocation{
				ChunkHash: m.Chunks[e.manifestIdx].Hash,
				PackRef:   PackRef{PackHash: ph, Offset: e.offset, Size: e.size},
			})
		}
		packBuf = packBuf[:0]
		packEntries = packEntries[:0]
		return nil
	}

	emitChunk := func() error {
		if len(current) == 0 {
			return nil
		}
		sum := sha256.Sum256(current)
		hash := hex.EncodeToString(sum[:])

		// Dedup check: if the session already knows this chunk, reuse its
		// existing location (pack_hash, offset, size) without repacking or
		// re-uploading. Scope depends on the configured index (prior-manifest
		// or global).
		if ref, ok := sess.Lookup(hash); ok && ref.PackHash != "" && ref.PackHash != hash {
			m.Chunks = append(m.Chunks, chunkRef{
				Hash:     hash,
				PackHash: ref.PackHash,
				Offset:   ref.Offset,
				Size:     ref.Size,
			})
			m.TotalSize += int64(ref.Size)
			current = current[:0]
			return nil
		}

		// New chunk: append to the current pack buffer AND append its ref to
		// m.Chunks now (in stream order) with the pack hash left blank; flushPack
		// fills it in once the pack is sealed. Offset/Size are known immediately.
		offset := len(packBuf)
		packBuf = append(packBuf, current...)
		m.Chunks = append(m.Chunks, chunkRef{Hash: hash, Offset: offset, Size: len(current)})
		m.TotalSize += int64(len(current))
		packEntries = append(packEntries, packEntry{manifestIdx: len(m.Chunks) - 1, offset: offset, size: len(current)})
		current = current[:0]

		// Flush when the pack buffer reaches the target size.
		if len(packBuf) >= c.packTargetBytes {
			return flushPack()
		}
		return nil
	}

	// Buffered read loop — same kopia pattern as putV1.
	for {
		n, err := r.Read(readBuf)
		if n > 0 {
			data := readBuf[:n]
			for len(data) > 0 {
				cut := sp.NextSplitPoint(data)
				if cut < 0 {
					current = append(current, data...)
					break
				}
				current = append(current, data[:cut]...)
				if emitErr := emitChunk(); emitErr != nil {
					return SnapshotMetadata{}, fmt.Errorf("chunked put v2: %w", emitErr)
				}
				data = data[cut:]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return SnapshotMetadata{}, fmt.Errorf("chunked put v2: read input: %w", err)
		}
	}
	// Final partial chunk: whatever's left after EOF.
	if err := emitChunk(); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put v2: tail chunk: %w", err)
	}
	// Flush the final partial pack (may be under target size — that's fine).
	if err := flushPack(); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put v2: final flush: %w", err)
	}

	// Durably persist the dedup index now that every pack has been uploaded,
	// so a recorded chunk location always references bytes that already exist.
	if err := sess.Commit(); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put v2: commit dedup index: %w", err)
	}

	// Record this object's ownership of every pack it references, BEFORE the
	// manifest exists. A refusal (ErrPackObliterating) means a pack this Put
	// deduped into is being reclaimed, so the Put fails cleanly rather than
	// writing a manifest that points at bytes on their way out; a retry re-packs.
	if err := c.associatePacks(ctx, domain, contextFor(key, meta), m.Chunks); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put v2: associate packs: %w", err)
	}

	return c.writeManifest(ctx, key, meta, &m, manifestFormatV2)
}

// BatchPutItem is one object in a PutBatch: its key/metadata and full content.
type BatchPutItem struct {
	Key  SnapshotKey
	Meta SnapshotMetadata
	Data []byte
}

// PutBatch stores multiple objects, PACKING their chunks into SHARED pack blobs
// (one ~packTarget pack spanning many objects) rather than one pack per object.
// It is the throughput fix for streaming many small writes: the per-object Put
// path can never fill a pack, so each small object becomes its own tiny pack →
// one blob/S3-PUT; PutBatch coalesces them across objects. Each object still gets
// its own manifest + dedup session, so the read path and dedup are unchanged.
// Items that differ in dedup domain or compression codec are simply split into
// separate packs (a pack is uniform in both). Returns one SnapshotMetadata per
// input item, in order. With packing disabled it falls back to per-item v1 Puts.
func (c *ChunkedStore) PutBatch(ctx context.Context, items []BatchPutItem) ([]SnapshotMetadata, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if c.packTargetBytes == packingDisabled {
		out := make([]SnapshotMetadata, len(items))
		for i := range items {
			md, err := c.putV1(ctx, items[i].Key, items[i].Meta, bytes.NewReader(items[i].Data))
			if err != nil {
				return nil, err
			}
			out[i] = md
		}
		return out, nil
	}
	return c.putBatchV2(ctx, items)
}

func (c *ChunkedStore) putBatchV2(ctx context.Context, items []BatchPutItem) ([]SnapshotMetadata, error) {
	type itemState struct {
		manifest chunkManifest
		sess     DedupSession
		domain   string
		codec    CompressionAlgo
	}
	states := make([]itemState, len(items))
	for i := range items {
		domain := c.domainFor(items[i].Key)
		sess, err := c.index.NewSession(ctx, domain, items[i].Key)
		if err != nil {
			return nil, fmt.Errorf("chunked put batch: open dedup session: %w", err)
		}
		states[i] = itemState{
			manifest: chunkManifest{Format: manifestFormatV2, Tenant: domain},
			sess:     sess,
			domain:   domain,
			codec:    c.codecFor(items[i].Meta),
		}
	}

	// psr records each pack's physical (compressed) size for storage accounting.
	// Optional (nil unless the index's durable store implements PackSizeRecorder);
	// best-effort — a failure is logged and the batch continues.
	psr := c.packSizeRecorder()

	// Shared pack buffer spanning items; each entry back-fills one item's slot.
	// A pack holds chunks of a single (domain, codec) — flushed before mixing.
	type packEntry struct {
		item        int
		manifestIdx int
		offset      int
		size        int
	}
	var (
		packBuf     []byte
		packEntries []packEntry
		packDomain  string
		packCodec   CompressionAlgo
	)
	flushPack := func() error {
		if len(packBuf) == 0 {
			return nil
		}
		sum := sha256.Sum256(packBuf)
		ph := hex.EncodeToString(sum[:])
		pid := packBlobID(packDomain, ph)
		stored, cerr := c.storePackBlob(packBuf, packSpansFromEntries(len(packEntries), func(i int) (int, int) {
			return packEntries[i].offset, packEntries[i].size
		}), packCodec)
		if cerr != nil {
			return cerr
		}
		if err := c.deduper.PutIfAbsent(ctx, pid, blobstore.BytesFromSlice(stored)); err != nil {
			return fmt.Errorf("flush pack: %w", err)
		}
		// Record the shared pack's physical (compressed) size once, under its
		// (uniform) domain — one row per pack, best-effort.
		recordPackSize(ctx, psr, packDomain, ph, len(stored))
		for _, e := range packEntries {
			st := &states[e.item]
			st.manifest.Chunks[e.manifestIdx].PackHash = ph
			st.sess.Record(ChunkLocation{
				ChunkHash: st.manifest.Chunks[e.manifestIdx].Hash,
				PackRef:   PackRef{PackHash: ph, Offset: e.offset, Size: e.size},
			})
		}
		packBuf = packBuf[:0]
		packEntries = packEntries[:0]
		return nil
	}

	const readBufSize = 256 * 1024
	readBuf := make([]byte, readBufSize)

	processItem := func(i int) error {
		st := &states[i]
		sp := c.splitter()
		defer sp.Close()
		var current []byte // accumulates the chunk being assembled

		emitChunk := func() error {
			if len(current) == 0 {
				return nil
			}
			sum := sha256.Sum256(current)
			hash := hex.EncodeToString(sum[:])
			// Dedup against this item's session (prior-manifest or global scope) —
			// unchanged from putV2.
			if ref, ok := st.sess.Lookup(hash); ok && ref.PackHash != "" && ref.PackHash != hash {
				st.manifest.Chunks = append(st.manifest.Chunks, chunkRef{
					Hash: hash, PackHash: ref.PackHash, Offset: ref.Offset, Size: ref.Size,
				})
				st.manifest.TotalSize += int64(ref.Size)
				current = current[:0]
				return nil
			}
			// A pack is uniform in (domain, codec); flush before mixing across items.
			if len(packBuf) > 0 && (st.domain != packDomain || st.codec != packCodec) {
				if err := flushPack(); err != nil {
					return err
				}
			}
			if len(packBuf) == 0 {
				packDomain, packCodec = st.domain, st.codec
			}
			offset := len(packBuf)
			packBuf = append(packBuf, current...)
			st.manifest.Chunks = append(st.manifest.Chunks, chunkRef{Hash: hash, Offset: offset, Size: len(current)})
			st.manifest.TotalSize += int64(len(current))
			packEntries = append(packEntries, packEntry{item: i, manifestIdx: len(st.manifest.Chunks) - 1, offset: offset, size: len(current)})
			current = current[:0]
			if len(packBuf) >= c.packTargetBytes {
				return flushPack()
			}
			return nil
		}

		r := bytes.NewReader(items[i].Data)
		for {
			n, err := r.Read(readBuf)
			if n > 0 {
				data := readBuf[:n]
				for len(data) > 0 {
					cut := sp.NextSplitPoint(data)
					if cut < 0 {
						current = append(current, data...)
						break
					}
					current = append(current, data[:cut]...)
					if emitErr := emitChunk(); emitErr != nil {
						return emitErr
					}
					data = data[cut:]
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}
		}
		return emitChunk() // final partial chunk
	}

	for i := range items {
		if err := processItem(i); err != nil {
			return nil, fmt.Errorf("chunked put batch: item %d: %w", i, err)
		}
	}
	// Final pack (spans whatever items contributed its tail chunks).
	if err := flushPack(); err != nil {
		return nil, fmt.Errorf("chunked put batch: final flush: %w", err)
	}

	out := make([]SnapshotMetadata, len(items))
	for i := range items {
		if err := states[i].sess.Commit(); err != nil {
			return nil, fmt.Errorf("chunked put batch: commit dedup index: %w", err)
		}
		if err := c.associatePacks(ctx, states[i].domain, contextFor(items[i].Key, items[i].Meta), states[i].manifest.Chunks); err != nil {
			return nil, fmt.Errorf("chunked put batch: associate packs: %w", err)
		}
		md, err := c.writeManifest(ctx, items[i].Key, items[i].Meta, &states[i].manifest, manifestFormatV2)
		if err != nil {
			return nil, fmt.Errorf("chunked put batch: write manifest: %w", err)
		}
		out[i] = md
	}
	return out, nil
}

// writeManifest JSON-encodes m and writes it as the snapshot's blob in
// the upstream store, tagging the metadata with the format string.
func (c *ChunkedStore) writeManifest(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, m *chunkManifest, formatTag string) (SnapshotMetadata, error) {
	manifestBytes, err := json.Marshal(m)
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked put: marshal manifest: %w", err)
	}
	if meta.Tags == nil {
		meta.Tags = make(map[string]string, 2)
	}
	tags := make(map[string]string, len(meta.Tags)+1)
	for k, v := range meta.Tags {
		tags[k] = v
	}
	tags["chunked_format"] = formatTag
	meta.Tags = tags
	return c.upstream.Put(ctx, key, meta, bytesReader(manifestBytes))
}

// UpdateLatestMeta rewrites ONLY the latest manifest for key with an overlaid
// tag set, without re-reading, re-chunking, or re-uploading any chunk/pack
// blob. It is the cheap path for metadata-only updates (e.g. blobgw's post-hoc
// S3 ETag): the prior fix re-streamed the whole object through Put just to
// rewrite the manifest, paying a full chunk re-read + re-pack; this reuses the
// existing manifest's chunkRefs verbatim so the only physical write is the new
// manifest blob in the upstream SnapshotStore.
//
// Merge semantics: each entry in tags is overlaid onto the latest manifest's
// existing SnapshotMetadata.Tags. A value of "" deletes that key (so callers
// can drop a tag without re-listing). Keys absent from tags are preserved,
// which keeps casstore's own internal tags (e.g. chunked_format) intact. The
// manifest body (chunkRefs + format) is copied through byte-for-byte, so the
// object reads back identically and GC sees the exact same live chunk/pack set
// after the update — dedup and reclamation are entirely unaffected.
//
// It writes a NEW snapshot version (the upstream Put generates one) that
// supersedes the prior latest, mirroring Put's version-chain semantics; the
// old version's manifest remains until deleted/GC'd like any other version.
// Returns ErrNoSnapshot if no snapshot exists for key, and an error if the
// latest snapshot is not a ChunkedStore manifest (refusing to rewrite an
// opaque/non-chunked blob it did not author).
func (c *ChunkedStore) UpdateLatestMeta(ctx context.Context, key SnapshotKey, tags map[string]string) (SnapshotMetadata, error) {
	rc, meta, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return SnapshotMetadata{}, err // includes ErrNoSnapshot
	}
	manifestBytes, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked update meta: read manifest: %w", readErr)
	}
	if closeErr != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked update meta: close manifest: %w", closeErr)
	}

	// Validate the latest snapshot is a manifest we authored, so we never
	// rewrite (and thereby reinterpret) an opaque non-chunked blob. We only
	// inspect the format; the chunkRefs are passed through untouched.
	var m chunkManifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("chunked update meta: parse manifest: %w", err)
	}
	if m.Format != manifestFormatV1 && m.Format != manifestFormatV2 {
		return SnapshotMetadata{}, fmt.Errorf("chunked update meta: not a chunked manifest (format=%q)", m.Format)
	}

	// Overlay the requested tags onto the existing set. "" deletes a key.
	merged := make(map[string]string, len(meta.Tags)+len(tags))
	for k, v := range meta.Tags {
		merged[k] = v
	}
	for k, v := range tags {
		if v == "" {
			delete(merged, k)
			continue
		}
		merged[k] = v
	}
	meta.Tags = merged

	// Re-write the SAME manifest bytes under a fresh version. No chunk/pack
	// blob is read or written — only this manifest blob in the upstream store.
	return c.upstream.Put(ctx, key, meta, bytesReader(manifestBytes))
}

// Get implements SnapshotStore. Reads the manifest from the upstream
// store, fetches each chunk from the BlobStore, and concatenates them.
func (c *ChunkedStore) Get(ctx context.Context, key SnapshotKey, version string) (io.ReadCloser, SnapshotMetadata, error) {
	rc, meta, err := c.upstream.Get(ctx, key, version)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}
	return c.openManifest(ctx, rc, meta)
}

// GetLatest implements SnapshotStore.
func (c *ChunkedStore) GetLatest(ctx context.Context, key SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	rc, meta, err := c.upstream.GetLatest(ctx, key)
	if err != nil {
		return nil, SnapshotMetadata{}, err
	}
	return c.openManifest(ctx, rc, meta)
}

// GetLatestMetadata is a pass-through. We never need the manifest to
// answer metadata queries.
func (c *ChunkedStore) GetLatestMetadata(ctx context.Context, key SnapshotKey) (SnapshotMetadata, error) {
	return c.upstream.GetLatestMetadata(ctx, key)
}

// List is a pass-through.
func (c *ChunkedStore) List(ctx context.Context, key SnapshotKey) ([]SnapshotMetadata, error) {
	return c.upstream.List(ctx, key)
}

// Walk is a pass-through. The chunked-GC routine uses Walk to enumerate
// every manifest, parse each one to compute the live chunk set.
func (c *ChunkedStore) Walk(ctx context.Context, yield func(SnapshotKey, string, SnapshotMetadata) error) error {
	return c.upstream.Walk(ctx, yield)
}

// Delete removes only the manifest. Chunk/pack reclamation is the GC's job —
// see chunked_gc.go (separate file). Eager refcount-based delete is
// known to race against concurrent writers (see architect review §1) so
// we deliberately don't do it here.
func (c *ChunkedStore) Delete(ctx context.Context, key SnapshotKey, version string) error {
	return c.upstream.Delete(ctx, key, version)
}

// openManifest decodes a manifest blob and returns a streaming reader
// that fetches and concatenates the referenced chunks.
func (c *ChunkedStore) openManifest(ctx context.Context, manifestRC io.ReadCloser, meta SnapshotMetadata) (io.ReadCloser, SnapshotMetadata, error) {
	defer manifestRC.Close()
	manifestBytes, err := io.ReadAll(manifestRC)
	if err != nil {
		return nil, SnapshotMetadata{}, fmt.Errorf("chunked get: read manifest: %w", err)
	}
	var m chunkManifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return nil, SnapshotMetadata{}, fmt.Errorf("chunked get: parse manifest: %w", err)
	}
	switch m.Format {
	case manifestFormatV1:
		// Backward compat: normalise v1 refs to the v2 shape so the reader
		// path is format-agnostic. For v1, the chunk blob is its own "pack"
		// at offset 0. We use a distinct marker (PackHash = "" for v1
		// normalised) — the reader then uses chunkBlobID instead of packBlobID.
		// We leave PackHash empty to signal "use chunkBlobID path".
		// (PackHash != Hash guard in loadPriorIndex handles this too.)
		m.Format = manifestFormatV2 // reader uses v2 path from here on
	case manifestFormatV2:
		// already in the expected shape
	default:
		return nil, SnapshotMetadata{}, fmt.Errorf("chunked get: not a chunked manifest (format=%q)", m.Format)
	}
	return newChunkStreamReader(ctx, c.chunks, m, &c.framingCache), meta, nil
}

// chunkStreamReader implements io.ReadCloser by fetching pack bytes lazily from
// the BlobStore and slicing out the per-chunk byte ranges as Read drains them.
//
// # Ranged reads (#18)
//
// Rather than fetching each chunk's WHOLE pack (a 16 MiB GET to serve a ~1 MiB
// chunk) and slicing locally, the reader issues RANGED GetBlob calls that fetch
// only the bytes a run of chunks needs. This is the node-side win in ADR-001 §5:
// remoteblob turns a length>0 GetBlob into a presigned ranged S3 GET, so a node
// pulls just the requested byte range instead of an entire pack.
//
// # Same-pack coalescing (locality)
//
// To avoid turning one whole-pack GET into N tiny per-chunk GETs when many
// consecutive chunks live in ONE pack (the dense-pack common case), the reader
// COALESCES a maximal run of consecutive manifest entries that share a PackHash
// into a single ranged GetBlob covering only that run's content
// [contentBase+minOff, contentBase+maxEnd) — then slices each chunk out of that
// one buffer. A run never fetches the pack's unused tail, and a slice that spans
// many same-pack chunks still costs a single data GET.
//
// # Header framing probe
//
// A pack blob this store writes is framed (packMagic + codec byte); legacy packs
// (pre-compression) are header-less. The reader must know the framing to compute
// the on-disk byte range for an uncompressed-content offset, and must request an
// EXACT byte count (kopia's filesystem backend rejects a ranged read that asks
// for more bytes than the blob holds). So the first time a pack is touched the
// reader issues a tiny ranged GET of the header bytes, decides the framing, and
// caches it; subsequent runs in the same pack reuse the cached decision and skip
// straight to an exact data GET.
//
// # Compressed packs
//
// A compressed pack's on-disk bytes are a single contiguous codec stream, so an
// uncompressed-content offset cannot index into them and a ranged slice is
// impossible. For such packs the reader transparently falls back to a whole-pack
// GetBlob + decompress (the pre-#18 behavior), caching the decompressed content
// in one slot so the run still slices from a single fetch. Uncompressed packs
// (legacy header-less OR framed with the none codec) take the ranged path. The
// chunk content-hash check runs on the recovered bytes in BOTH paths, so ranged
// output is byte-identical to the whole-pack path.
//
// # Cache
//
// One decompressed-pack slot is retained for the compressed-pack fallback so
// consecutive runs in the same compressed pack don't re-fetch; the ranged path
// holds no whole-pack buffer, keeping memory bounded.
type chunkStreamReader struct {
	ctx       context.Context
	store     blobstore.Storage
	manifest  chunkManifest
	cursorIdx int    // index of the next chunk to emit
	current   []byte // buffer of the current chunk; drained as the caller reads

	// single-slot DECOMPRESSED-pack cache, used only for the compressed-pack
	// fallback path so consecutive runs in one compressed pack reuse one fetch.
	cachedPackID blobstore.ID
	cachedPack   []byte

	// framingByPack caches the per-pack header framing decision so a pack touched
	// by multiple runs is probed at most once. It is the SHARED, store-level cache
	// (keyed by pack blob ID, which embeds the immutable pack hash) when the reader
	// is built by openManifest, so the header probe runs at most once per pack per
	// process across all readers; for readers built directly (tests) it is a
	// reader-local sync.Map. Only the uncompressed (ranged) path consults it;
	// compressed packs land in the cachedPack slot instead.
	framingByPack *sync.Map // map[blobstore.ID]packFraming

	// queued holds chunk byte slices for the chunks of an already-fetched run,
	// in stream order, waiting to be handed to the caller one Read at a time.
	queued [][]byte
}

// newChunkStreamReader builds a streaming reader. framingCache is the shared
// per-pack-hash framing cache to consult/populate; pass nil for a reader-local
// cache (used by direct-construction tests). The cache being shared at the
// ChunkedStore level is what makes the header probe run at most once per pack
// per process rather than once per reader.
func newChunkStreamReader(ctx context.Context, store blobstore.Storage, m chunkManifest, framingCache *sync.Map) *chunkStreamReader {
	if framingCache == nil {
		framingCache = &sync.Map{}
	}
	return &chunkStreamReader{ctx: ctx, store: store, manifest: m, framingByPack: framingCache}
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	if len(r.current) == 0 {
		if len(r.queued) == 0 {
			if r.cursorIdx >= len(r.manifest.Chunks) {
				return 0, io.EOF
			}
			if err := r.loadNextRun(); err != nil {
				return 0, err
			}
		}
		// Hand out the next chunk from the fetched run.
		r.current = r.queued[0]
		r.queued = r.queued[1:]
	}
	n := copy(p, r.current)
	r.current = r.current[n:]
	return n, nil
}

// loadNextRun fetches the next run of chunks starting at cursorIdx and fills
// r.queued with their bytes in stream order, advancing cursorIdx past the run.
//
// v1 refs (after openManifest normalisation) have empty PackHash and are fetched
// one standalone chunk blob at a time. v2 refs are coalesced into per-pack runs
// served by a single ranged (or, for compressed packs, whole-pack) fetch.
func (r *chunkStreamReader) loadNextRun() error {
	ref := r.manifest.Chunks[r.cursorIdx]

	if ref.PackHash == "" {
		// v1 normalised ref: standalone chunk blob. There is no pack to coalesce
		// across, so each chunk is its own (whole-blob) fetch.
		r.cursorIdx++
		chunk, err := r.loadStandaloneChunk(ref)
		if err != nil {
			return err
		}
		r.queued = append(r.queued[:0], chunk)
		return nil
	}

	// v2: gather the maximal run of consecutive entries sharing this PackHash.
	start := r.cursorIdx
	end := start + 1
	for end < len(r.manifest.Chunks) && r.manifest.Chunks[end].PackHash == ref.PackHash {
		end++
	}
	run := r.manifest.Chunks[start:end]
	r.cursorIdx = end

	chunks, err := r.loadPackRun(ref.PackHash, run)
	if err != nil {
		return err
	}
	r.queued = append(r.queued[:0], chunks...)
	return nil
}

// loadStandaloneChunk fetches a v1 standalone chunk blob and returns its
// recovered (decompressed) bytes after the manifest size check.
func (r *chunkStreamReader) loadStandaloneChunk(ref chunkRef) ([]byte, error) {
	id := chunkBlobID(r.manifest.Tenant, ref.Hash)
	buf := blobstore.NewOutputBuffer()
	if err := r.store.GetBlob(r.ctx, id, 0, -1, buf); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("chunked get: chunk %s missing — snapshot is unrestorable: %w", ref.Hash, ErrMissingBlob)
		}
		return nil, fmt.Errorf("chunked get: fetch chunk %s: %w", ref.Hash, err)
	}
	// Strip the self-describing compression header (no-op for legacy
	// header-less blobs) to recover the original chunk bytes before the
	// size check / caller hand-off.
	got, derr := decompressPackBlob(buf.Bytes())
	if derr != nil {
		return nil, fmt.Errorf("chunked get: chunk %s: %w", ref.Hash, derr)
	}
	if len(got) != ref.Size {
		return nil, fmt.Errorf("chunked get: chunk %s size mismatch: manifest=%d got=%d: %w", ref.Hash, ref.Size, len(got), ErrCorrupt)
	}
	return got, nil
}

// loadPackRun returns, in stream order, the bytes of every chunk in run (all of
// which share packHash). It issues a single exact-length ranged GetBlob for
// uncompressed packs (slicing each chunk out of the fetched window) and falls
// back to a whole-pack fetch + decompress for compressed packs. Every returned
// chunk has had its content hash verified against the manifest.
func (r *chunkStreamReader) loadPackRun(packHash string, run []chunkRef) ([][]byte, error) {
	pid := packBlobID(r.manifest.Tenant, packHash)

	// If this pack was already fully decompressed into the one-slot cache (a
	// compressed pack touched by an earlier run), slice straight from it.
	if r.cachedPackID == pid && r.cachedPack != nil {
		return sliceRunFromContent(r.cachedPack, pid, run)
	}

	// The run's required uncompressed-content window is [minOff, maxEnd).
	minOff := run[0].Offset
	maxEnd := 0
	for _, ref := range run {
		if ref.Offset < minOff {
			minOff = ref.Offset
		}
		if e := ref.Offset + ref.Size; e > maxEnd {
			maxEnd = e
		}
	}
	if minOff < 0 || maxEnd < minOff {
		return nil, fmt.Errorf("chunked get: pack %s invalid run window (minOff=%d maxEnd=%d)", pid, minOff, maxEnd)
	}

	// Degenerate (all-zero-size) run: nothing to fetch, so skip the framing probe
	// entirely — there is no byte range to map and the header framing is
	// irrelevant. The hash check in sliceRunFromContent still runs (sha256 of
	// empty matches a zero-size ref). This MUST precede framingFor so a zero-length
	// run never pays a header GET.
	if maxEnd-minOff <= 0 {
		return sliceRunFromContent(nil, pid, shiftRun(run, minOff))
	}

	// Recover (or probe) this pack's framing so we can map an uncompressed-content
	// offset to an exact on-disk byte range.
	framing, err := r.framingFor(pid)
	if err != nil {
		return nil, err
	}

	if framing.algo == packAlgoChunked {
		// Per-chunk framing: compressed AND range-readable. Map the run's
		// uncompressed window through the index to one contiguous stored range.
		chunks, cerr := r.loadChunkedPackRun(pid, framing, run, minOff, maxEnd)
		if cerr == nil {
			return chunks, nil
		}
		if errors.Is(cerr, ErrMissingBlob) || errors.Is(cerr, ErrCorrupt) {
			return nil, cerr
		}
		// Same EOF-boundary hazard as the uncompressed path below (a tail run's
		// stored window ends exactly at blob EOF): fall back to the whole-pack read.
		return r.loadCompressedPackRun(pid, run)
	}

	if framing.algo != CompressNone {
		// Compressed pack (or a probe that couldn't determine framing safely, which
		// reports the forceWholePack sentinel): a ranged slice cannot index a codec
		// stream. Fetch the whole pack, decompress, cache the content, and slice the
		// run from it.
		return r.loadCompressedPackRun(pid, run)
	}

	// Uncompressed pack: fetch the run's content window. content begins at on-disk
	// byte framing.contentBase, so the run occupies on-disk bytes
	// [contentBase+minOff, contentBase+maxEnd). When the run's last chunk ends at
	// the pack content tail, that window reaches the blob's EOF exactly
	// (off+length == blob length) — and a strict backend rejects it: kopia requires
	// the read to return exactly `length` bytes, and an S3 ranged GET whose end
	// touches the object likewise short-reads. A single-file payload's final
	// partial pack hits this (its tail chunk lands on the pack-content boundary),
	// which is the gliner2 model-loading EIO/SIGBUS. So: try the windowed read, and
	// on ANY fetch failure that is not a missing pack, fall back to the whole-pack
	// path — an open-ended GET (no length check), sliced and hash-verified
	// identically. Clamping the length cannot help (it is already exact); only the
	// whole-pack read avoids the boundary. Correctness is unchanged: a genuinely
	// short/truncated pack still fails the bounds+hash check in sliceRunFromContent,
	// not here. Only a boundary (tail) run pays this; interior runs keep the fast
	// windowed read.
	off := int64(framing.contentBase + minOff)
	length := int64(maxEnd - minOff) // > 0: the zero-length run was short-circuited before framingFor
	buf := blobstore.NewOutputBuffer()
	if err := r.store.GetBlob(r.ctx, pid, off, length, buf); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("chunked get: pack %s missing — snapshot is unrestorable: %w", pid, ErrMissingBlob)
		}
		return r.loadCompressedPackRun(pid, run)
	}
	// The fetched window starts at content offset minOff, so rebase each chunk's
	// offset by minOff before slicing out of it.
	return sliceRunFromContent(buf.Bytes(), pid, shiftRun(run, minOff))
}

// forceWholePack is a sentinel CompressionAlgo used only as an in-memory signal
// from framingFor that the pack's framing could not be probed safely and the
// caller must take the whole-pack (decompress) path. It is never written to a
// blob and never compared by the codec helpers.
const forceWholePack CompressionAlgo = "__force_whole_pack__"

// framingFor returns the cached framing for pid, probing the pack header with a
// tiny ranged GET on first touch and caching the result. The probe reads exactly
// packHeaderLen bytes. If the backend rejects that read (a pathological legacy
// pack shorter than the header), framingFor reports the forceWholePack sentinel
// so the caller falls back to a whole-pack fetch — that path uses
// decompressPackBlob, which correctly returns short legacy bytes untouched.
func (r *chunkStreamReader) framingFor(pid blobstore.ID) (packFraming, error) {
	if f, ok := r.framingByPack.Load(pid); ok {
		return f.(packFraming), nil
	}
	framing, err := probePackFraming(r.ctx, r.store, pid, "chunked get")
	if err != nil {
		return packFraming{}, err
	}
	r.framingByPack.Store(pid, framing)
	return framing, nil
}

// probePackFraming reads a pack's framing from its header, resolving the
// per-chunk index when the header names the per-chunk codec. Shared by the
// streaming reader (framingFor) and the batch reader (probeFraming) so both agree
// byte-for-byte on every pack; each owns its own caching.
//
// The probe asks for packChunkedHeaderLen bytes, which is what per-chunk framing
// needs and a harmless over-read for the others. A backend that rejects it (a pack
// shorter than that, i.e. legacy or near-empty) is re-probed at packHeaderLen, and
// only if THAT is rejected does the caller fall back to a whole-pack read.
func probePackFraming(ctx context.Context, store blobstore.Storage, pid blobstore.ID, opName string) (packFraming, error) {
	head := blobstore.NewOutputBuffer()
	if err := store.GetBlob(ctx, pid, 0, int64(packChunkedHeaderLen), head); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return packFraming{}, fmt.Errorf("%s: pack %s missing — snapshot is unrestorable: %w", opName, pid, ErrMissingBlob)
		}
		// Too short for the long header: retry at the short one. A pack that is
		// shorter than packChunkedHeaderLen cannot be per-chunk framed, so the short
		// probe is sufficient to classify it.
		head = blobstore.NewOutputBuffer()
		if err := store.GetBlob(ctx, pid, 0, int64(packHeaderLen), head); err != nil {
			if errors.Is(err, blobstore.ErrBlobNotFound) {
				return packFraming{}, fmt.Errorf("%s: pack %s missing — snapshot is unrestorable: %w", opName, pid, ErrMissingBlob)
			}
			// Pathological pack shorter than even the short header: route through
			// the whole-pack path, which handles short legacy bytes untouched.
			return packFraming{algo: forceWholePack}, nil //nolint:nilerr // intentional whole-pack signal
		}
	}
	framing, derr := detectPackFraming(head.Bytes())
	if derr != nil {
		return packFraming{}, fmt.Errorf("%s: pack %s: %w", opName, pid, derr)
	}
	if framing.algo != packAlgoChunked {
		return framing, nil
	}

	// Per-chunk framing: fetch and parse the index so offsets can be mapped.
	if framing.indexLen == 0 {
		framing.chunked = []packChunkEntry{}
		return framing, nil
	}
	idx := blobstore.NewOutputBuffer()
	if err := store.GetBlob(ctx, pid, int64(packChunkedHeaderLen), int64(framing.indexLen), idx); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return packFraming{}, fmt.Errorf("%s: pack %s missing — snapshot is unrestorable: %w", opName, pid, ErrMissingBlob)
		}
		return packFraming{}, fmt.Errorf("%s: pack %s: fetch per-chunk index: %w", opName, pid, err)
	}
	entries, perr := parsePackChunkIndex(idx.Bytes())
	if perr != nil {
		return packFraming{}, fmt.Errorf("%s: pack %s: %w", opName, pid, perr)
	}
	framing.chunked = entries
	return framing, nil
}

// loadChunkedPackRun serves a run out of a per-chunk-framed pack: it maps the
// run's uncompressed window [minOff, maxEnd) through the index to one contiguous
// stored byte range, issues a single ranged GET for it, decompresses each chunk
// independently, and hash-verifies via the shared sliceRunFromContent. This is the
// point of A1 — a COMPRESSED pack served by a ranged read.
func (r *chunkStreamReader) loadChunkedPackRun(pid blobstore.ID, framing packFraming, run []chunkRef, minOff, maxEnd int) ([][]byte, error) {
	storedOff, storedLen, first, last, err := framing.chunkedStoredSpan(minOff, maxEnd)
	if err != nil {
		return nil, fmt.Errorf("chunked get: pack %s: %w", pid, err)
	}
	if storedLen == 0 {
		return sliceRunFromContent(nil, pid, shiftRun(run, minOff))
	}
	buf := blobstore.NewOutputBuffer()
	if gerr := r.store.GetBlob(r.ctx, pid, int64(framing.bodyBase+storedOff), int64(storedLen), buf); gerr != nil {
		if errors.Is(gerr, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("chunked get: pack %s missing — snapshot is unrestorable: %w", pid, ErrMissingBlob)
		}
		return nil, gerr
	}
	content, merr := materializeChunkedWindow(framing.chunked, first, last, buf.Bytes(), storedOff)
	if merr != nil {
		return nil, fmt.Errorf("chunked get: pack %s: %w", pid, merr)
	}
	return sliceRunFromContent(content, pid, shiftRun(run, minOff))
}

// loadCompressedPackRun is the fallback for compressed packs: it fetches and
// decompresses the WHOLE pack, caches the decompressed content in the one-slot
// cache, and slices the run out of it. This preserves pre-#18 behavior for the
// compressed-pack case (and the pathological short-legacy-pack case).
func (r *chunkStreamReader) loadCompressedPackRun(pid blobstore.ID, run []chunkRef) ([][]byte, error) {
	buf := blobstore.NewOutputBuffer()
	if err := r.store.GetBlob(r.ctx, pid, 0, -1, buf); err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("chunked get: pack %s missing — snapshot is unrestorable: %w", pid, ErrMissingBlob)
		}
		return nil, fmt.Errorf("chunked get: fetch pack %s: %w", pid, err)
	}
	raw, derr := decompressPackBlob(buf.Bytes())
	if derr != nil {
		return nil, fmt.Errorf("chunked get: pack %s: %w", pid, derr)
	}
	r.cachedPackID = pid
	r.cachedPack = raw
	return sliceRunFromContent(raw, pid, run)
}

// shiftRun returns a copy of run with every chunk's Offset reduced by base, so
// the refs index into a buffer that starts at content offset base (the start of
// a ranged window) rather than at content offset 0. Returns run unchanged when
// base is 0.
func shiftRun(run []chunkRef, base int) []chunkRef {
	if base == 0 {
		return run
	}
	out := make([]chunkRef, len(run))
	for i, ref := range run {
		ref.Offset -= base
		out[i] = ref
	}
	return out
}

// sliceRunFromContent slices each chunk of run out of the uncompressed pack
// content, verifies each chunk's content hash, and returns owned copies in
// stream order. content must cover every chunk's [Offset, Offset+Size).
func sliceRunFromContent(content []byte, pid blobstore.ID, run []chunkRef) ([][]byte, error) {
	out := make([][]byte, 0, len(run))
	for _, ref := range run {
		end := ref.Offset + ref.Size
		if ref.Offset < 0 || end > len(content) {
			return nil, fmt.Errorf("chunked get: chunk %s out of bounds in pack %s (offset=%d size=%d contentLen=%d): %w",
				ref.Hash, pid, ref.Offset, ref.Size, len(content), ErrCorrupt)
		}
		chunk := content[ref.Offset:end]

		// Integrity check: verify the chunk hash matches what the manifest
		// promised. This runs on the ranged bytes too, so a ranged read that
		// returned the wrong window would be caught here.
		sum := sha256.Sum256(chunk)
		got := hex.EncodeToString(sum[:])
		if got != ref.Hash {
			return nil, fmt.Errorf("chunked get: chunk %s hash mismatch (got %s) — data corruption in pack %s: %w",
				ref.Hash, got, pid, ErrCorrupt)
		}

		// Copy so the caller owns a buffer independent of the fetched blob's
		// backing (which may be reused/evicted).
		out = append(out, bytes.Clone(chunk))
	}
	return out, nil
}

func (r *chunkStreamReader) Close() error {
	r.current = nil
	r.cachedPack = nil
	r.queued = nil
	r.cursorIdx = len(r.manifest.Chunks)
	return nil
}

// recordPackSize records one pack's physical (compressed) on-disk size for
// per-domain storage accounting. It is a best-effort, log-and-continue helper:
// psr==nil (the store has no PackSizeRecorder capability) is a silent no-op, and
// a recorder error is logged but never propagated — losing the accounting for a
// pack must never fail the Put/compaction, exactly like a dedup Record failure.
// One call per pack (never per chunk), so a domain's physical SUM counts each
// pack's compressed bytes exactly once.
func recordPackSize(ctx context.Context, psr PackSizeRecorder, domain, packHash string, compressedBytes int) {
	if psr == nil {
		return
	}
	if err := psr.RecordPackSizes(ctx, domain, []PackSize{{PackHash: packHash, CompressedBytes: int64(compressedBytes)}}); err != nil {
		slog.WarnContext(ctx, "casstore: record pack size failed; storage accounting will be off for this pack",
			"domain", domain, "pack", packHash, "err", err)
	}
}

// bytesReader wraps a []byte as an io.Reader without dragging in
// bytes.NewReader → io.NopCloser at the call site.
func bytesReader(b []byte) io.Reader {
	return &byteSliceReader{data: b}
}

type byteSliceReader struct {
	data []byte
	pos  int
}

func (b *byteSliceReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
