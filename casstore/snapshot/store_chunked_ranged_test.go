// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// strictRangeStorage rejects a ranged read that reaches the blob's end
// (offset+length >= blob length), mimicking the contract the production -remote
// backend (and a strict S3 ranged GET) enforces. The kopia *filesystem* backend
// the default tests use happens to allow reading exactly to EOF, so the pack-tail
// boundary goes untested there — this wrapper reproduces it. rejected counts the
// boundary rejections so a test can prove the fallback actually engaged.
type strictRangeStorage struct {
	blobstore.Storage
	rejected atomic.Int64
}

func (s *strictRangeStorage) GetBlob(ctx context.Context, id blobstore.ID, offset, length int64, out blobstore.OutputBuffer) error {
	if length > 0 {
		if md, err := s.Storage.GetMetadata(ctx, id); err == nil && offset+length >= md.Length {
			s.rejected.Add(1)
			return fmt.Errorf("strictRangeStorage: ranged read [%d,%d) reaches blob %s EOF (len=%d)", offset, offset+length, id, md.Length)
		}
	}
	return s.Storage.GetBlob(ctx, id, offset, length, out)
}

// TestRangedRead_PackTailBoundary reproduces the gliner2 model-loading failure: a
// single-file uncompressed payload's slice run ends exactly at the pack-content
// tail, so the windowed GET reaches blob EOF and a strict backend rejects it.
// loadPackRun must fall back to the whole-pack fetch and still return byte-correct
// data (the read is EIO/SIGBUS on the GPU without this).
func TestRangedRead_PackTailBoundary(t *testing.T) {
	ctx := context.Background()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close(ctx) })
	strict := &strictRangeStorage{Storage: raw}

	cs, err := NewChunkedStore(upstream, strict, ChunkedConfig{
		CompressionPolicy: NewContentTypePolicy(CompressNone), // uncompressed → the ranged-read path
	})
	if err != nil {
		t.Fatal(err)
	}

	// One Put → one pack whose content IS the slice, so the read window covers
	// [0,len): off+length == pack EOF — the boundary that breaks gliner2.
	payload := make([]byte, 300*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	key := SnapshotKey{Tenant: "t1", OwnerKey: "s/1"}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	got, rerr := io.ReadAll(rc)
	rc.Close()
	if rerr != nil {
		t.Fatalf("ReadAll (the gliner2 EIO): %v", rerr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("tail-boundary read mismatch (%d vs %d bytes)", len(got), len(payload))
	}
	// Prove the test actually hit the boundary (so it can't pass trivially): the
	// strict backend must have rejected at least one tail read, and the fallback
	// recovered it.
	if strict.rejected.Load() == 0 {
		t.Fatal("strict backend never rejected a tail read — the boundary was not exercised")
	}
}

// sha256Hex returns the lowercase hex sha256 of b — the chunk/pack hash form the
// manifest uses.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// countingStorage wraps a blobstore.Storage and records every GetBlob call's
// (id, offset, length) so a test can assert the read path issues RANGED reads
// (length > 0) and coalesces consecutive same-pack chunks into a single GET.
// All non-GetBlob methods are promoted from the embedded interface unchanged.
type countingStorage struct {
	blobstore.Storage

	mu    sync.Mutex
	calls []getCall
}

type getCall struct {
	id     blobstore.ID
	offset int64
	length int64
}

func (c *countingStorage) GetBlob(ctx context.Context, id blobstore.ID, offset, length int64, out blobstore.OutputBuffer) error {
	c.mu.Lock()
	c.calls = append(c.calls, getCall{id: id, offset: offset, length: length})
	c.mu.Unlock()
	return c.Storage.GetBlob(ctx, id, offset, length, out)
}

func (c *countingStorage) snapshotCalls() []getCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]getCall, len(c.calls))
	copy(out, c.calls)
	return out
}

func (c *countingStorage) reset() {
	c.mu.Lock()
	c.calls = nil
	c.mu.Unlock()
}

// packGetCalls returns only the GetBlob calls targeting pack blobs (the chunk
// read path), split into framing metadata reads and chunk-payload fetches.
//
// Framing reads are the header probe (offset 0, one of the two header lengths)
// and, for a per-chunk-framed pack, the index fetch that follows it. They are
// classified against each pack's ACTUAL framing — read out of band, through the
// wrapped storage so the lookup does not pollute the counts — rather than by a
// length heuristic, which could not tell an index fetch from a chunk fetch.
func (c *countingStorage) packGetCalls() (probes, data []getCall) {
	for _, call := range c.snapshotCalls() {
		if !bytesHasPrefix(string(call.id), "pack-") {
			continue
		}
		if c.isFramingRead(call) {
			probes = append(probes, call)
			continue
		}
		data = append(data, call)
	}
	return probes, data
}

// isFramingRead reports whether call fetched framing metadata rather than chunk
// payload.
func (c *countingStorage) isFramingRead(call getCall) bool {
	if call.offset == 0 && (call.length == int64(packHeaderLen) || call.length == int64(packChunkedHeaderLen)) {
		return true
	}
	framing, ok := c.framingOf(call.id)
	if !ok || framing.algo != packAlgoChunked {
		return false
	}
	return call.offset == int64(packChunkedHeaderLen) && call.length == int64(framing.indexLen)
}

// framingOf reads a pack's framing directly from the wrapped storage (bypassing
// the counter) so call classification can consult the real on-disk layout.
func (c *countingStorage) framingOf(id blobstore.ID) (packFraming, bool) {
	buf := blobstore.NewOutputBuffer()
	if err := c.Storage.GetBlob(context.Background(), id, 0, -1, buf); err != nil {
		return packFraming{}, false
	}
	framing, err := detectPackFraming(buf.Bytes())
	if err != nil {
		return packFraming{}, false
	}
	return framing, true
}

func bytesHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// chunkedRangedStack builds a ChunkedStore whose chunk blobstore is wrapped in a
// countingStorage, with an explicit pack target and compression policy so each
// test can shape the layout it needs. The returned countingStorage observes
// every GetBlob the reader issues.
func chunkedRangedStack(t *testing.T, packTarget int, policy CompressionPolicy) (*ChunkedStore, *countingStorage) {
	t.Helper()
	return chunkedRangedStackMode(t, packTarget, policy, PackCompressPerChunk)
}

// chunkedRangedStackMode is chunkedRangedStack with an explicit pack-compression
// framing, so a test can pin either the default per-chunk framing or the
// whole-pack rollback.
func chunkedRangedStackMode(t *testing.T, packTarget int, policy CompressionPolicy, mode PackCompressionMode) (*ChunkedStore, *countingStorage) {
	t.Helper()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	local, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = local.Close(context.Background()) })
	counting := &countingStorage{Storage: local}
	cs, err := NewChunkedStore(upstream, counting, ChunkedConfig{
		PackTargetBytes:   packTarget,
		CompressionPolicy: policy,
		PackCompression:   mode,
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, counting
}

// TestRanged_ByteIdenticalAcrossLayouts is the load-bearing correctness test: it
// proves the ranged-read reader returns byte-identical output to the original
// content across every layout that matters — multi-pack slices, many chunks in
// one pack (coalescing), pack-boundary chunks, a slice larger than one pack, and
// both compressed (zstd) and uncompressed storage. Full reads, partial ReadAt-
// style reads at varied offsets/lengths, and v1 (no-pack) are all covered.
func TestRanged_ByteIdenticalAcrossLayouts(t *testing.T) {
	type layout struct {
		name       string
		packTarget int
		policy     CompressionPolicy
		size       int
		fill       func([]byte)
	}

	randomFill := func(b []byte) {
		if _, err := rand.Read(b); err != nil {
			t.Fatalf("rand: %v", err)
		}
	}
	repetitiveFill := func(b []byte) {
		pat := []byte("the quick brown fox jumps over the lazy dog 0123456789\n")
		for i := range b {
			b[i] = pat[i%len(pat)]
		}
	}

	layouts := []layout{
		// Many small packs → a slice spanning MANY packs. Uncompressed.
		{"multi-pack-uncompressed", 256 << 10, NewContentTypePolicy(CompressNone), 5 << 20, randomFill},
		// Big pack target → many chunks in ONE pack (exercises coalescing).
		{"dense-single-pack-uncompressed", 64 << 20, NewContentTypePolicy(CompressNone), 8 << 20, randomFill},
		// Slice larger than one pack, uncompressed.
		{"slice-larger-than-pack", 1 << 20, NewContentTypePolicy(CompressNone), 6 << 20, randomFill},
		// Compressed (zstd) content must still be byte-identical via the fallback.
		{"multi-pack-compressed", 1 << 20, NewContentTypePolicy(CompressZstd), 6 << 20, repetitiveFill},
		{"dense-single-pack-compressed", 64 << 20, NewContentTypePolicy(CompressZstd), 6 << 20, repetitiveFill},
		// Default 16 MiB pack target, mixed realistic content.
		{"default-target-random", 0, NewContentTypePolicy(CompressNone), 9 << 20, randomFill},
		// v1 (no packing): standalone chunk blobs.
		{"v1-no-pack-uncompressed", packingDisabled, NewContentTypePolicy(CompressNone), 3 << 20, randomFill},
		{"v1-no-pack-compressed", packingDisabled, NewContentTypePolicy(CompressZstd), 3 << 20, repetitiveFill},
		// Tiny payloads / edge sizes.
		{"empty", 1 << 20, NewContentTypePolicy(CompressNone), 0, randomFill},
		{"single-byte", 1 << 20, NewContentTypePolicy(CompressNone), 1, randomFill},
	}

	for _, lay := range layouts {
		t.Run(lay.name, func(t *testing.T) {
			cs, _ := chunkedRangedStack(t, lay.packTarget, lay.policy)
			ctx := context.Background()
			key := SnapshotKey{Tenant: "t", OwnerKey: lay.name}

			payload := make([]byte, lay.size)
			lay.fill(payload)

			if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
				t.Fatalf("Put: %v", err)
			}

			// 1) Full sequential read must equal the original bytes exactly.
			got := readAllVia(t, cs, key)
			if !bytes.Equal(got, payload) {
				t.Fatalf("full read mismatch: got %d bytes want %d (first-diff @ %d)",
					len(got), len(payload), firstDiffOffset(got, payload))
			}

			// 2) Partial reads at varied offsets/lengths (ReadAt semantics) must
			//    each match the corresponding window of the original — this is the
			//    path mlfs chunkstore.ReadAt drives.
			if lay.size > 0 {
				for _, frac := range []struct{ off, length int }{
					{0, 1},
					{0, lay.size},
					{lay.size / 2, lay.size / 4},
					{lay.size - 1, 1},
					{lay.size / 3, lay.size - lay.size/3},
					{maxInt(0, lay.size-1024), minInt(1024, lay.size)},
				} {
					off, length := frac.off, frac.length
					if off >= lay.size {
						continue
					}
					if off+length > lay.size {
						length = lay.size - off
					}
					window := readWindow(t, cs, key, off, length)
					want := payload[off : off+length]
					if !bytes.Equal(window, want) {
						t.Fatalf("partial read [%d,%d) mismatch (first-diff @ %d)",
							off, off+length, firstDiffOffset(window, want))
					}
				}
			}
		})
	}
}

// readWindow drives the streaming reader the way chunkstore.ReadAt does: skip to
// off, then read exactly length bytes.
func readWindow(t *testing.T, cs *ChunkedStore, key SnapshotKey, off, length int) []byte {
	t.Helper()
	rc, _, err := cs.GetLatest(context.Background(), key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	if off > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(off)); err != nil {
			t.Fatalf("seek to %d: %v", off, err)
		}
	}
	out := make([]byte, length)
	n, err := io.ReadFull(rc, out)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		t.Fatalf("read window [%d,%d): %v", off, off+length, err)
	}
	return out[:n]
}

// TestRanged_IssuesRangedNotWholePack asserts the chunk read path NEVER fetches
// a whole pack (length == -1) for an uncompressed store, and that the bytes it
// does fetch are RANGED (length > 0). This is the core #18 behavior: remoteblob
// turns these length>0 GetBlobs into presigned ranged S3 GETs.
func TestRanged_IssuesRangedNotWholePack(t *testing.T) {
	cs, counting := chunkedRangedStack(t, 1<<20, NewContentTypePolicy(CompressNone))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "ranged"}

	payload := make([]byte, 6<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	counting.reset()
	got := readAllVia(t, cs, key)
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch")
	}

	for _, call := range counting.snapshotCalls() {
		if !bytesHasPrefix(string(call.id), "pack-") {
			continue
		}
		if call.length < 0 {
			t.Fatalf("uncompressed read path issued a WHOLE-pack GetBlob (length=%d) for %s — expected ranged reads only",
				call.length, call.id)
		}
		if call.length == 0 {
			t.Fatalf("read path issued a zero-length GetBlob for %s (invalid range)", call.id)
		}
	}

	probes, data := counting.packGetCalls()
	if len(data) == 0 {
		t.Fatal("expected at least one ranged data GetBlob on the pack read path")
	}
	t.Logf("uncompressed ranged read: %d header probes + %d ranged data GETs", len(probes), len(data))
}

// TestRanged_CoalescesSamePackRun proves the reader COALESCES a run of
// consecutive chunks that share a pack into a single ranged data GET: with a
// large pack target a multi-chunk payload lands mostly in one pack, so the
// number of ranged data GETs must be far fewer than the chunk count.
func TestRanged_CoalescesSamePackRun(t *testing.T) {
	// 64 MiB pack target: an 8 MiB payload (~8 chunks) packs into ONE pack.
	cs, counting := chunkedRangedStack(t, 64<<20, NewContentTypePolicy(CompressNone))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "coalesce"}

	payload := make([]byte, 8<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// How many chunks and packs does the manifest describe?
	chunkCount, packHashes := manifestStats(t, cs, key)
	if len(packHashes) == 0 {
		t.Fatal("expected at least one pack")
	}
	if chunkCount <= len(packHashes) {
		t.Skipf("layout produced %d chunks across %d packs — not enough chunks-per-pack to exercise coalescing",
			chunkCount, len(packHashes))
	}

	counting.reset()
	got := readAllVia(t, cs, key)
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch")
	}

	probes, data := counting.packGetCalls()
	// One coalesced data GET per pack (a maximal same-pack run). Each pack is
	// probed once for framing. So data GETs == number of packs, and that is
	// strictly fewer than the chunk count when chunks share packs.
	if len(data) != len(packHashes) {
		t.Fatalf("expected one coalesced ranged data GET per pack (%d), got %d", len(packHashes), len(data))
	}
	if len(data) >= chunkCount {
		t.Fatalf("coalescing failed: %d ranged data GETs for %d chunks (expected far fewer)", len(data), chunkCount)
	}
	if len(probes) != len(packHashes) {
		t.Fatalf("expected one header probe per pack (%d), got %d", len(packHashes), len(probes))
	}
	t.Logf("coalescing: %d chunks across %d pack(s) served by %d ranged data GET(s) + %d probe(s)",
		chunkCount, len(packHashes), len(data), len(probes))
}

// TestRanged_PartialReadSkipsLaterPacks proves the locality win for partial
// reads spanning MULTIPLE packs: a read that stops inside the first pack must
// never fetch the later packs at all (the reader is lazy — it only fetches a run
// when Read advances into it). This is the #18 behavior mlfs ReadAt relies on:
// reading a small window of a large multi-pack slice pulls only the packs it
// touches, not the whole object.
func TestRanged_PartialReadSkipsLaterPacks(t *testing.T) {
	// Small pack target so the payload spans many packs.
	cs, counting := chunkedRangedStack(t, 512<<10, NewContentTypePolicy(CompressNone))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "partial-multi-pack"}

	payload := make([]byte, 8<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	chunkCount, packHashes := manifestStats(t, cs, key)
	if len(packHashes) < 3 {
		t.Skipf("need >=3 packs to prove later packs are skipped; got %d packs / %d chunks", len(packHashes), chunkCount)
	}

	counting.reset()
	// Read only the first 4 KiB — within the first chunk of the first pack.
	window := readWindow(t, cs, key, 0, 4096)
	if !bytes.Equal(window, payload[:4096]) {
		t.Fatalf("first-window mismatch")
	}

	// Only ONE pack's run should have been fetched; the later packs must be
	// untouched. (The reader fetches a coalesced run only when Read reaches it.)
	probes, data := counting.packGetCalls()
	touched := map[blobstore.ID]struct{}{}
	for _, c := range data {
		touched[c.id] = struct{}{}
	}
	if len(touched) != 1 {
		t.Fatalf("partial read into the first pack fetched data from %d packs; expected 1 (later packs must be skipped)", len(touched))
	}
	if len(data) != 1 {
		t.Fatalf("expected exactly one ranged data GET for the first pack's run, got %d", len(data))
	}
	// That single data GET must be far smaller than the whole object — it covers
	// only the first pack's run, never the later packs' bytes.
	if data[0].length >= int64(len(payload)) {
		t.Fatalf("ranged read fetched whole-object-sized window: length=%d payload=%d", data[0].length, len(payload))
	}
	t.Logf("partial read of first 4 KiB across %d packs fetched %d pack(s): %d-byte ranged data GET + %d probe(s)",
		len(packHashes), len(touched), data[0].length, len(probes))
}

// TestRanged_WholePackFramingFallsBackToWholePack verifies the rollback framing
// (PackCompressWholePack): a whole-pack codec stream cannot be range-sliced, so
// the reader fetches the whole pack (length == -1) and decompresses — while still
// returning byte-identical output. The DEFAULT framing avoids this entirely; see
// TestRanged_PerChunkCompressedIsRangeReadable.
func TestRanged_WholePackFramingFallsBackToWholePack(t *testing.T) {
	cs, counting := chunkedRangedStackMode(t, 64<<20, NewContentTypePolicy(CompressZstd), PackCompressWholePack)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "compressed"}

	// Highly compressible so the pack is actually stored compressed.
	payload := bytes.Repeat([]byte("compressible-block-content-"), 300000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	counting.reset()
	got := readAllVia(t, cs, key)
	if !bytes.Equal(got, payload) {
		t.Fatalf("compressed round-trip mismatch")
	}

	_, data := counting.packGetCalls()
	sawWhole := false
	for _, call := range data {
		if call.length < 0 {
			sawWhole = true
		}
	}
	if !sawWhole {
		t.Fatal("expected a whole-pack GetBlob (length=-1) for a compressed pack fallback")
	}
}

// TestRanged_PerChunkCompressedIsRangeReadable is the A1 acceptance test: with the
// default per-chunk framing, a COMPRESSED pack is served by ranged GETs — never a
// whole-pack fetch — while returning byte-identical content. This is the half of
// TECH_DEBT #18 that the whole-pack codec could not deliver.
func TestRanged_PerChunkCompressedIsRangeReadable(t *testing.T) {
	// One big pack so a whole-pack fetch would be conspicuous, and a partial read
	// so only a fraction of the pack is actually needed.
	cs, counting := chunkedRangedStack(t, 64<<20, NewContentTypePolicy(CompressZstd))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "per-chunk-compressed"}

	payload := bytes.Repeat([]byte("compressible-block-content-"), 300000)
	if _, err := cs.Put(ctx, key, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Full read first: correctness of the per-chunk ranged path end to end.
	counting.reset()
	got := readAllVia(t, cs, key)
	if !bytes.Equal(got, payload) {
		t.Fatal("per-chunk compressed round-trip mismatch")
	}
	_, data := counting.packGetCalls()
	if len(data) == 0 {
		t.Fatal("expected ranged data GETs for a per-chunk-framed pack")
	}
	for _, call := range data {
		if call.length < 0 {
			t.Fatalf("per-chunk framing must not fall back to a whole-pack GET (offset=%d length=%d)", call.offset, call.length)
		}
	}

	// Partial read: the reader must fetch far less than the whole object. This is
	// the win — under whole-pack framing this same read pulls the entire pack.
	counting.reset()
	head := readWindow(t, cs, key, 0, 4<<10)
	if !bytes.Equal(head, payload[:4<<10]) {
		t.Fatal("per-chunk compressed partial read mismatch")
	}
	_, data = counting.packGetCalls()
	var fetched int64
	for _, call := range data {
		if call.length < 0 {
			t.Fatalf("partial read fell back to a whole-pack GET (offset=%d)", call.offset)
		}
		fetched += call.length
	}
	if fetched >= int64(len(payload))/2 {
		t.Fatalf("partial read fetched %d bytes of a %d-byte object — not a ranged read", fetched, len(payload))
	}
	t.Logf("4 KiB partial read of a %d-byte compressed object fetched %d stored bytes across %d ranged GET(s)",
		len(payload), fetched, len(data))
}

// TestRanged_RunStartingMidPack deterministically exercises a coalesced run
// whose first chunk starts at a NON-ZERO pack offset (the cross-object-dedup
// shape, where a manifest references chunks in the middle of an existing pack).
// It validates the contentBase+minOff ranged offset and the shiftRun rebasing:
// the reader must fetch only [minOff, maxEnd) of the pack content and slice each
// chunk correctly. Built directly so the layout is exact, not splitter-dependent.
func TestRanged_RunStartingMidPack(t *testing.T) {
	ctx := context.Background()
	local, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = local.Close(ctx) })
	counting := &countingStorage{Storage: local}

	const tenant = "t"
	// Build a pack with three chunks A|B|C. The manifest will reference only B
	// then C — a run that starts at chunk B's non-zero offset.
	a := bytes.Repeat([]byte{0x11}, 1000)
	b := bytes.Repeat([]byte{0x22}, 2000)
	c := bytes.Repeat([]byte{0x33}, 1500)
	raw := append(append(append([]byte{}, a...), b...), c...)

	stored, err := compressPackBlob(raw, CompressNone) // framed, none codec
	if err != nil {
		t.Fatalf("compressPackBlob: %v", err)
	}
	packHash := sha256Hex(raw)
	pid := packBlobID(tenant, packHash)
	if err := local.PutBlob(ctx, pid, blobstore.BytesFromSlice(stored), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob pack: %v", err)
	}

	offB, offC := len(a), len(a)+len(b)
	m := chunkManifest{
		Format:    manifestFormatV2,
		TotalSize: int64(len(b) + len(c)),
		Tenant:    tenant,
		Chunks: []chunkRef{
			{Hash: sha256Hex(b), PackHash: packHash, Offset: offB, Size: len(b)},
			{Hash: sha256Hex(c), PackHash: packHash, Offset: offC, Size: len(c)},
		},
	}

	counting.reset()
	rd := newChunkStreamReader(ctx, counting, m, nil)
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	_ = rd.Close()

	want := append(append([]byte{}, b...), c...)
	if !bytes.Equal(got, want) {
		t.Fatalf("mid-pack run mismatch: got %d bytes want %d", len(got), len(want))
	}

	_, data := counting.packGetCalls()
	if len(data) != 1 {
		t.Fatalf("expected one coalesced ranged data GET for the B|C run, got %d", len(data))
	}
	// The ranged GET must start at the header + chunk B's offset and cover only
	// B|C — it must NOT include chunk A's leading bytes.
	wantOff := int64(packHeaderLen + offB)
	wantLen := int64(len(b) + len(c))
	if data[0].offset != wantOff || data[0].length != wantLen {
		t.Fatalf("ranged GET window wrong: got [off=%d len=%d] want [off=%d len=%d]",
			data[0].offset, data[0].length, wantOff, wantLen)
	}
}

// TestRanged_SharedFramingCacheSkipsReprobe proves PART A: the per-pack-hash
// framing cache lives at the ChunkedStore level (not per-reader), so a SECOND
// Get of the same object issues ZERO header probes — the first Get already
// probed and cached every pack's framing, and pack hashes are immutable, so the
// cached framing is reused across readers/Gets for the lifetime of the process.
func TestRanged_SharedFramingCacheSkipsReprobe(t *testing.T) {
	// Small pack target so the payload spans multiple packs — each pack is a
	// distinct cache entry, and we assert NONE are re-probed on the second read.
	cs, counting := chunkedRangedStack(t, 512<<10, NewContentTypePolicy(CompressNone))
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "shared-framing-cache"}

	payload := make([]byte, 6<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := cs.Put(ctx, key, SnapshotMetadata{}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// First Get: cold cache. The reader probes each pack's framing exactly once.
	counting.reset()
	got1 := readAllVia(t, cs, key)
	if !bytes.Equal(got1, payload) {
		t.Fatalf("first read mismatch")
	}
	probes1, data1 := counting.packGetCalls()
	if len(probes1) == 0 {
		t.Fatal("first read should have issued header probes on a cold cache")
	}
	if len(data1) == 0 {
		t.Fatal("first read should have issued ranged data GETs")
	}

	// Second Get of the SAME object via a fresh reader. The framing cache is at
	// the ChunkedStore level, so every pack is already cached — NO new probe.
	counting.reset()
	got2 := readAllVia(t, cs, key)
	if !bytes.Equal(got2, payload) {
		t.Fatalf("second read mismatch")
	}
	probes2, data2 := counting.packGetCalls()
	if len(probes2) != 0 {
		t.Fatalf("second Get of the same pack(s) issued %d header probe(s); expected 0 (shared per-pack-hash cache should skip re-probing)", len(probes2))
	}
	// Data GETs still happen — only the framing probe is cached, not the bytes.
	if len(data2) == 0 {
		t.Fatal("second read should still issue ranged data GETs (only the framing probe is cached, not the data)")
	}
	t.Logf("first read: %d probes + %d data GETs; second read: %d probes (cached) + %d data GETs",
		len(probes1), len(data1), len(probes2), len(data2))
}

// manifestStats parses the raw manifest for key and returns the chunk count and
// the set of distinct pack hashes it references.
func manifestStats(t *testing.T, cs *ChunkedStore, key SnapshotKey) (chunkCount int, packHashes map[string]struct{}) {
	t.Helper()
	rc, _, err := cs.upstream.GetLatest(context.Background(), key)
	if err != nil {
		t.Fatalf("GetLatest manifest: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	packHashes = make(map[string]struct{})
	for _, ref := range m.Chunks {
		if ref.PackHash != "" {
			packHashes[ref.PackHash] = struct{}{}
		}
	}
	return len(m.Chunks), packHashes
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
