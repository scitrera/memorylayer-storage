// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// spansOf builds the tiling packSpan list for a sequence of chunks and returns it
// alongside the concatenated pack content.
func spansOf(chunks ...[]byte) ([]byte, []packSpan) {
	var raw []byte
	spans := make([]packSpan, 0, len(chunks))
	for _, c := range chunks {
		spans = append(spans, packSpan{offset: len(raw), size: len(c)})
		raw = append(raw, c...)
	}
	return raw, spans
}

// TestParsePackCompressionMode covers the flag/env surface every daemon parses.
func TestParsePackCompressionMode(t *testing.T) {
	for in, want := range map[string]PackCompressionMode{
		"":           PackCompressPerChunk,
		"per-chunk":  PackCompressPerChunk,
		"perchunk":   PackCompressPerChunk,
		"chunk":      PackCompressPerChunk,
		"whole-pack": PackCompressWholePack,
		"wholepack":  PackCompressWholePack,
		"whole":      PackCompressWholePack,
		"pack":       PackCompressWholePack,
	} {
		got, err := ParsePackCompressionMode(in)
		if err != nil {
			t.Fatalf("ParsePackCompressionMode(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("ParsePackCompressionMode(%q) = %v, want %v", in, got, want)
		}
	}
	for _, bad := range []string{"none", "zstd", "true", "off"} {
		if _, err := ParsePackCompressionMode(bad); err == nil {
			t.Fatalf("ParsePackCompressionMode(%q) accepted an invalid mode", bad)
		}
	}
	if got := PackCompressPerChunk.String(); got != "per-chunk" {
		t.Fatalf("PackCompressPerChunk.String() = %q", got)
	}
	if got := PackCompressWholePack.String(); got != "whole-pack" {
		t.Fatalf("PackCompressWholePack.String() = %q", got)
	}
}

// TestPerChunk_RoundTripsByteIdentical is the base correctness property: whatever
// the span layout, decompressPackBlob must reproduce the uncompressed pack content
// exactly. This is what keeps every whole-pack consumer (GC, compaction, integrity
// verification, the EOF fallback) correct against the new codec.
func TestPerChunk_RoundTripsByteIdentical(t *testing.T) {
	incompressible := make([]byte, 4096)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		chunks [][]byte
	}{
		{"empty", nil},
		{"single", [][]byte{bytes.Repeat([]byte("a"), 5000)}},
		{"many", [][]byte{
			bytes.Repeat([]byte("a"), 1000),
			bytes.Repeat([]byte("b"), 2500),
			bytes.Repeat([]byte("c"), 17),
		}},
		{"mixed-compressibility", [][]byte{
			bytes.Repeat([]byte("compressible"), 900),
			incompressible,
			bytes.Repeat([]byte("z"), 64),
		}},
		{"zero-length-chunk", [][]byte{
			bytes.Repeat([]byte("a"), 100),
			{},
			bytes.Repeat([]byte("b"), 100),
		}},
	}
	for _, tc := range cases {
		for _, algo := range []CompressionAlgo{CompressZstd, CompressGzip} {
			t.Run(tc.name+"/"+string(algo), func(t *testing.T) {
				raw, spans := spansOf(tc.chunks...)
				stored, err := compressPackBlobChunked(raw, spans, algo)
				if err != nil {
					t.Fatalf("compressPackBlobChunked: %v", err)
				}
				framing, derr := detectPackFraming(stored)
				if derr != nil {
					t.Fatalf("detectPackFraming: %v", derr)
				}
				if framing.algo != packAlgoChunked {
					t.Fatalf("framing.algo = %q, want per-chunk marker", framing.algo)
				}
				if got := framing.logicalCodec(); got != algo {
					t.Fatalf("logicalCodec = %q, want %q", got, algo)
				}
				got, gerr := decompressPackBlob(stored)
				if gerr != nil {
					t.Fatalf("decompressPackBlob: %v", gerr)
				}
				if !bytes.Equal(got, raw) {
					t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(raw))
				}
			})
		}
	}
}

// TestPerChunk_IncompressibleChunkStoredRaw proves the per-entry fallback: a chunk
// the codec cannot shrink is stored raw under packCodecNone, so the worst case is
// bounded by index overhead rather than by codec expansion.
func TestPerChunk_IncompressibleChunkStoredRaw(t *testing.T) {
	incompressible := make([]byte, 32*1024)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatal(err)
	}
	raw, spans := spansOf(incompressible)
	stored, err := compressPackBlobChunked(raw, spans, CompressZstd)
	if err != nil {
		t.Fatalf("compressPackBlobChunked: %v", err)
	}
	framing, derr := detectPackFraming(stored)
	if derr != nil {
		t.Fatalf("detectPackFraming: %v", derr)
	}
	entries, perr := parsePackChunkIndex(stored[packChunkedHeaderLen:framing.bodyBase])
	if perr != nil {
		t.Fatalf("parsePackChunkIndex: %v", perr)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if entries[0].codec != packCodecNone {
		t.Fatalf("incompressible chunk stored under codec %d, want raw (%d)", entries[0].codec, packCodecNone)
	}
	if entries[0].stoSize != len(incompressible) {
		t.Fatalf("raw entry stored size = %d, want %d", entries[0].stoSize, len(incompressible))
	}
	// The pack must not have grown beyond the header + index.
	if maxWant := packChunkedHeaderLen + packChunkEntryLen + len(incompressible); len(stored) != maxWant {
		t.Fatalf("stored pack is %d bytes, want %d (no codec expansion)", len(stored), maxWant)
	}
}

// TestPerChunk_SpansMustTile locks the writer contract that makes whole-pack
// reassembly exact: spans must tile the pack content with no gap, overlap, or
// shortfall.
func TestPerChunk_SpansMustTile(t *testing.T) {
	raw := bytes.Repeat([]byte("x"), 300)
	cases := []struct {
		name  string
		spans []packSpan
	}{
		{"gap", []packSpan{{offset: 0, size: 100}, {offset: 150, size: 150}}},
		{"overlap", []packSpan{{offset: 0, size: 200}, {offset: 100, size: 200}}},
		{"short", []packSpan{{offset: 0, size: 100}}},
		{"does-not-start-at-zero", []packSpan{{offset: 10, size: 290}}},
		{"no-spans-for-content", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := compressPackBlobChunked(raw, tc.spans, CompressZstd); err == nil {
				t.Fatal("expected a tiling error, got nil")
			}
		})
	}
}

// TestPerChunk_CorruptIndexIsRejected proves corruption surfaces as an explicit
// error rather than wrong bytes: a tampered index must fail, never silently return
// mis-sliced content.
func TestPerChunk_CorruptIndexIsRejected(t *testing.T) {
	raw, spans := spansOf(
		bytes.Repeat([]byte("a"), 1000),
		bytes.Repeat([]byte("b"), 1000),
	)
	stored, err := compressPackBlobChunked(raw, spans, CompressZstd)
	if err != nil {
		t.Fatalf("compressPackBlobChunked: %v", err)
	}

	// Corrupt the SECOND entry's uncompressed offset so the entries no longer tile.
	tampered := bytes.Clone(stored)
	tampered[packChunkedHeaderLen+packChunkEntryLen] ^= 0xFF
	if _, derr := decompressPackBlob(tampered); derr == nil {
		t.Fatal("expected an error for a tampered per-chunk index, got nil")
	} else if !errors.Is(derr, ErrCorrupt) {
		t.Fatalf("tampered index error = %v, want ErrCorrupt", derr)
	}

	// Truncating below the indexed body must also fail rather than short-read.
	if _, derr := decompressPackBlob(stored[:len(stored)-10]); derr == nil {
		t.Fatal("expected an error for a truncated per-chunk pack, got nil")
	}
}

// TestPerChunk_DedupIdentityUnchanged is the dedup-safety invariant, the per-chunk
// analogue of PackHashUnchangedAcrossCodecs: the framing is a physical storage
// choice, so a store writing under per-chunk framing must produce exactly the same
// chunk and pack hashes as one writing whole-pack framing. If it did not, flipping
// the flag would silently split the dedup domain in two.
func TestPerChunk_DedupIdentityUnchanged(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("dedup-identity-block-"), 200000)
	key := SnapshotKey{Tenant: "t", OwnerKey: "identity"}

	manifestFor := func(mode PackCompressionMode) chunkManifest {
		cs, _ := chunkedRangedStackMode(t, 1<<20, NewContentTypePolicy(CompressZstd), mode)
		if _, err := cs.Put(ctx, key, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put (mode %d): %v", mode, err)
		}
		m, ok, err := cs.latestManifest(ctx, key)
		if err != nil || !ok {
			t.Fatalf("latestManifest (mode %d): ok=%v err=%v", mode, ok, err)
		}
		return m
	}

	perChunk := manifestFor(PackCompressPerChunk)
	wholePack := manifestFor(PackCompressWholePack)

	if len(perChunk.Chunks) != len(wholePack.Chunks) {
		t.Fatalf("chunk count differs across framings: per-chunk=%d whole-pack=%d",
			len(perChunk.Chunks), len(wholePack.Chunks))
	}
	for i := range perChunk.Chunks {
		a, b := perChunk.Chunks[i], wholePack.Chunks[i]
		if a.Hash != b.Hash || a.PackHash != b.PackHash || a.Offset != b.Offset || a.Size != b.Size {
			t.Fatalf("chunk %d differs across framings:\n per-chunk=%+v\n whole-pack=%+v", i, a, b)
		}
	}
}

// TestPerChunk_UncompressedClassesAreByteIdentical is the guarantee that matters
// for the model/tensor workload: per-chunk framing applies ONLY to packs the
// policy decided to compress. A store-uncompressed class (GPU-loadable tensors,
// already-compressed media, or an explicit store_uncompressed tag) still produces
// the original plain framing — no index, no per-chunk bodies — and the stored pack
// bytes are IDENTICAL under both modes. Those packs were already range-readable
// and mmap-friendly; A1 must not perturb them.
func TestPerChunk_UncompressedClassesAreByteIdentical(t *testing.T) {
	ctx := context.Background()
	// Dense random bytes, like real tensor data.
	payload := make([]byte, 3<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		meta SnapshotMetadata
	}{
		{"safetensors", SnapshotMetadata{Format: "application/x-safetensors"}},
		{"gpu-loadable", SnapshotMetadata{Format: "tensor/gpu-loadable"}},
		{"already-compressed-media", SnapshotMetadata{Format: "video/mp4"}},
		{"store-uncompressed-tag", SnapshotMetadata{
			Format: "application/octet-stream",
			Tags:   map[string]string{TagStoreUncompressed: "1"},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := SnapshotKey{Tenant: "t", OwnerKey: "uncompressed/" + tc.name}

			// storedPacks returns every pack blob the manifest references, keyed by
			// pack hash, for a store built in the given framing mode.
			storedPacks := func(mode PackCompressionMode) map[string][]byte {
				cs, chunks := chunkedRangedStackMode(t, 1<<20, NewContentTypePolicy(CompressZstd), mode)
				if _, err := cs.Put(ctx, key, tc.meta, bytes.NewReader(payload)); err != nil {
					t.Fatalf("Put (mode %d): %v", mode, err)
				}
				m, ok, err := cs.latestManifest(ctx, key)
				if err != nil || !ok {
					t.Fatalf("latestManifest (mode %d): ok=%v err=%v", mode, ok, err)
				}
				out := map[string][]byte{}
				for _, ref := range m.Chunks {
					if ref.PackHash == "" || out[ref.PackHash] != nil {
						continue
					}
					buf := blobstore.NewOutputBuffer()
					if err := chunks.GetBlob(ctx, packBlobID(m.Tenant, ref.PackHash), 0, -1, buf); err != nil {
						t.Fatalf("GetBlob pack %s: %v", ref.PackHash, err)
					}
					out[ref.PackHash] = bytes.Clone(buf.Bytes())
				}
				if len(out) == 0 {
					t.Fatalf("no packs written (mode %d)", mode)
				}
				return out
			}

			perChunk := storedPacks(PackCompressPerChunk)
			wholePack := storedPacks(PackCompressWholePack)

			if len(perChunk) != len(wholePack) {
				t.Fatalf("pack count differs: per-chunk=%d whole-pack=%d", len(perChunk), len(wholePack))
			}
			for hash, got := range perChunk {
				want, ok := wholePack[hash]
				if !ok {
					t.Fatalf("pack %s missing under whole-pack framing", hash)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("pack %s stored bytes differ across framing modes (%d vs %d bytes) — "+
						"an uncompressed class must be untouched by per-chunk framing", hash, len(got), len(want))
				}
				// And it must be plainly framed: no per-chunk index at all.
				framing, derr := detectPackFraming(got)
				if derr != nil {
					t.Fatalf("detectPackFraming(%s): %v", hash, derr)
				}
				if framing.algo != CompressNone {
					t.Fatalf("uncompressed-class pack %s framed as %q, want plain uncompressed framing", hash, framing.algo)
				}
			}
		})
	}
}

// TestPerChunk_CrossPutDedupReference covers the case that forces the index to live
// in the PACK rather than the manifest: a second object dedups against chunks in a
// pack written by an earlier Put, so its manifest references a pack whose physical
// layout it never recorded. The reader must recover that layout from the pack alone.
func TestPerChunk_CrossPutDedupReference(t *testing.T) {
	ctx := context.Background()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	local, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close(ctx) })

	// A GlobalIndex makes dedup cross-object, which is what puts a foreign pack
	// reference into the second object's manifest.
	cs, err := NewChunkedStore(upstream, local, ChunkedConfig{
		DedupDomain:       "d",
		Index:             NewGlobalIndex(NewMemoryDedupStore(), nil),
		CompressionPolicy: NewContentTypePolicy(CompressZstd),
	})
	if err != nil {
		t.Fatal(err)
	}

	shared := bytes.Repeat([]byte("shared-content-block-"), 300000)
	first := SnapshotKey{Tenant: "t", OwnerKey: "first"}
	if _, err := cs.Put(ctx, first, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(shared)); err != nil {
		t.Fatalf("Put first: %v", err)
	}

	// Second object: a distinct prefix followed by the same content, so its tail
	// chunks dedup into the first object's per-chunk-framed packs.
	second := SnapshotKey{Tenant: "t", OwnerKey: "second"}
	payload2 := append(bytes.Repeat([]byte("prefix-"), 1000), shared...)
	if _, err := cs.Put(ctx, second, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(payload2)); err != nil {
		t.Fatalf("Put second: %v", err)
	}

	got := readAllVia(t, cs, second)
	if !bytes.Equal(got, payload2) {
		t.Fatalf("cross-Put dedup read mismatch: got %d bytes want %d", len(got), len(payload2))
	}

	// And the first object still reads back correctly.
	if got := readAllVia(t, cs, first); !bytes.Equal(got, shared) {
		t.Fatal("first object read mismatch after cross-Put dedup")
	}
}

// TestPerChunk_BatchReadMatchesStreamingRead pins the batch reader to the streaming
// reader under per-chunk framing: both must return byte-identical content for the
// same keys, so the two independent ranged paths cannot drift.
func TestPerChunk_BatchReadMatchesStreamingRead(t *testing.T) {
	ctx := context.Background()
	cs, _ := chunkedRangedStack(t, 1<<20, NewContentTypePolicy(CompressZstd))

	want := map[SnapshotKey][]byte{}
	keys := make([]SnapshotKey, 0, 3)
	for i, n := range []int{200000, 50000, 400000} {
		k := SnapshotKey{Tenant: "t", OwnerKey: string(rune('a'+i)) + "/obj"}
		body := bytes.Repeat([]byte("batch-block-content-"), n/20)
		if _, err := cs.Put(ctx, k, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(body)); err != nil {
			t.Fatalf("Put %v: %v", k, err)
		}
		want[k] = body
		keys = append(keys, k)
	}

	got, _, err := cs.GetLatestBatch(ctx, keys, 3)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	for _, k := range keys {
		if !bytes.Equal(got[k], want[k]) {
			t.Fatalf("batch read mismatch for %v: got %d bytes want %d", k, len(got[k]), len(want[k]))
		}
		if streamed := readAllVia(t, cs, k); !bytes.Equal(streamed, want[k]) {
			t.Fatalf("streaming read mismatch for %v", k)
		}
	}
}

// TestPerChunk_RangedReadStartingMidPack is the per-chunk analogue of
// TestRanged_RunStartingMidPack: a run whose first chunk sits at a non-zero pack
// offset must fetch only that run's STORED bytes, mapped through the index, and
// slice correctly. Built directly so the layout is exact.
func TestPerChunk_RangedReadStartingMidPack(t *testing.T) {
	ctx := context.Background()
	local, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close(ctx) })
	counting := &countingStorage{Storage: local}

	const tenant = "t"
	a := bytes.Repeat([]byte{0x11}, 4000)
	b := bytes.Repeat([]byte{0x22}, 8000)
	c := bytes.Repeat([]byte{0x33}, 6000)
	raw, spans := spansOf(a, b, c)

	stored, err := compressPackBlobChunked(raw, spans, CompressZstd)
	if err != nil {
		t.Fatalf("compressPackBlobChunked: %v", err)
	}
	packHash := sha256Hex(raw)
	pid := packBlobID(tenant, packHash)
	if err := local.PutBlob(ctx, pid, blobstore.BytesFromSlice(stored), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
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
	got, rerr := io.ReadAll(rd)
	if rerr != nil {
		t.Fatalf("ReadAll: %v", rerr)
	}
	_ = rd.Close()

	if want := append(append([]byte{}, b...), c...); !bytes.Equal(got, want) {
		t.Fatalf("mid-pack per-chunk run mismatch: got %d bytes want %d", len(got), len(want))
	}

	_, data := counting.packGetCalls()
	if len(data) != 1 {
		t.Fatalf("expected one coalesced ranged data GET for the B|C run, got %d", len(data))
	}
	// The window must cover only B|C's STORED bytes — chunk A's stored body must
	// not be fetched.
	framing, derr := detectPackFraming(stored)
	if derr != nil {
		t.Fatal(derr)
	}
	entries, perr := parsePackChunkIndex(stored[packChunkedHeaderLen:framing.bodyBase])
	if perr != nil {
		t.Fatal(perr)
	}
	wantOff := int64(framing.bodyBase + entries[1].stoOffset)
	wantLen := int64(entries[1].stoSize + entries[2].stoSize)
	if data[0].offset != wantOff || data[0].length != wantLen {
		t.Fatalf("ranged GET window wrong: got [off=%d len=%d] want [off=%d len=%d]",
			data[0].offset, data[0].length, wantOff, wantLen)
	}
}
