// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// mixedCodecStackWithPackSize builds a ChunkedStore with a ContentTypePolicy so
// objects classified as GPU-loadable/tensor (e.g. application/x-safetensors) are
// stored UNCOMPRESSED while generic content is zstd-compressed — i.e. one tenant
// holding both codec classes at once, the scenario Phase 0 repack must preserve.
func mixedCodecStackWithPackSize(t *testing.T, packTarget int) (*ChunkedStore, SnapshotStore, blobstore.Storage) {
	t.Helper()
	upstream, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(context.Background(), blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(context.Background()) })

	cs, err := NewChunkedStore(upstream, chunks, ChunkedConfig{
		PackTargetBytes:   packTarget,
		CompressionPolicy: NewContentTypePolicy(CompressZstd),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return cs, upstream, chunks
}

// packCodecOf recovers the on-disk codec of the pack backing key's first chunk by
// reading the pack blob's self-describing header — the same signal the reader and
// the repacker use. It proves a key's data physically lives in a pack of the
// expected codec class.
func packCodecOf(t *testing.T, cs *ChunkedStore, chunks blobstore.Storage, domain string, key SnapshotKey) CompressionAlgo {
	t.Helper()
	ctx := context.Background()
	m, ok, err := cs.latestManifest(ctx, key)
	if err != nil || !ok {
		t.Fatalf("latestManifest %v: ok=%v err=%v", key, ok, err)
	}
	if len(m.Chunks) == 0 || m.Chunks[0].PackHash == "" {
		t.Fatalf("key %v has no packed chunk", key)
	}
	buf := blobstore.NewOutputBuffer()
	if err := chunks.GetBlob(ctx, packBlobID(domain, m.Chunks[0].PackHash), 0, -1, buf); err != nil {
		t.Fatalf("get pack %s: %v", m.Chunks[0].PackHash, err)
	}
	fr, err := detectPackFraming(buf.Bytes())
	if err != nil {
		t.Fatalf("detectPackFraming: %v", err)
	}
	// The compression CLASS, which is what codec-preserving repack keys on — a
	// per-chunk-framed pack reports it from the header's logical codec byte.
	return fr.logicalCodec()
}

// TestCompact_CodecPreserving is the Phase 0 guarantee: with a tenant holding BOTH
// an uncompressed (model/tensor) class and a compressible (generic) class, repack
// consolidates EACH class within its own codec — model packs stay uncompressed
// (range-readable + mmap-able), generic packs stay zstd — instead of collapsing
// everything to one codec. Every object still round-trips byte-identical, before
// and after GC reclaims the orphaned source packs.
func TestCompact_CodecPreserving(t *testing.T) {
	const packTarget = 1 << 20 // 1 MiB
	cs, _, chunks := mixedCodecStackWithPackSize(t, packTarget)
	ctx := context.Background()
	const domain = "t1"

	const (
		nModel   = 8
		nGeneric = 8
		sz       = 200 * 1024 // each Put → one small (< target) pack
	)
	want := map[SnapshotKey][]byte{}
	modelKeys := make([]SnapshotKey, nModel)
	genericKeys := make([]SnapshotKey, nGeneric)

	// Model objects: incompressible random bytes, declared as a tensor content
	// type → the policy stores their packs UNCOMPRESSED.
	for i := 0; i < nModel; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		k := SnapshotKey{Tenant: domain, OwnerKey: fmt.Sprintf("model/%d", i)}
		modelKeys[i] = k
		want[k] = b
		if _, err := cs.Put(ctx, k, SnapshotMetadata{Format: "application/x-safetensors"}, bytes.NewReader(b)); err != nil {
			t.Fatalf("Put model %d: %v", i, err)
		}
	}
	// Generic objects: compressible content, generic content type → zstd packs.
	for i := 0; i < nGeneric; i++ {
		b := make([]byte, sz)
		for j := range b {
			b[j] = byte('A' + (i+j/64)%16) // highly compressible, distinct per object
		}
		k := SnapshotKey{Tenant: domain, OwnerKey: fmt.Sprintf("generic/%d", i)}
		genericKeys[i] = k
		want[k] = b
		if _, err := cs.Put(ctx, k, SnapshotMetadata{Format: "text/plain"}, bytes.NewReader(b)); err != nil {
			t.Fatalf("Put generic %d: %v", i, err)
		}
	}

	// Sanity: pre-compaction, the two classes already live in different codecs.
	if got := packCodecOf(t, cs, chunks, domain, modelKeys[0]); got != CompressNone {
		t.Fatalf("pre-compaction model pack codec = %q, want none", got)
	}
	if got := packCodecOf(t, cs, chunks, domain, genericKeys[0]); got != CompressZstd {
		t.Fatalf("pre-compaction generic pack codec = %q, want zstd", got)
	}

	before, err := countTenantBlobs(ctx, chunks, domain)
	if err != nil {
		t.Fatal(err)
	}

	res, err := cs.Compact(ctx, domain, CompactConfig{MinPackBytes: packTarget})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.PacksSelected != nModel+nGeneric {
		t.Errorf("PacksSelected = %d, want %d", res.PacksSelected, nModel+nGeneric)
	}
	if res.ManifestsRewritten != nModel+nGeneric {
		t.Errorf("ManifestsRewritten = %d, want %d", res.ManifestsRewritten, nModel+nGeneric)
	}
	if res.PacksWritten >= nModel+nGeneric {
		t.Errorf("no consolidation: wrote %d packs for %d objects", res.PacksWritten, nModel+nGeneric)
	}

	// The crux: after repack each class still lives in its own codec — model packs
	// stay uncompressed, generic packs stay zstd. A naive repack collapses both to
	// one codec; codec-preserving repack does not.
	for i, k := range modelKeys {
		if got := packCodecOf(t, cs, chunks, domain, k); got != CompressNone {
			t.Errorf("post-compaction model[%d] pack codec = %q, want none (must stay mmap/range-readable)", i, got)
		}
	}
	for i, k := range genericKeys {
		if got := packCodecOf(t, cs, chunks, domain, k); got != CompressZstd {
			t.Errorf("post-compaction generic[%d] pack codec = %q, want zstd", i, got)
		}
	}

	// Every object still reads back byte-identical across the repack.
	readAll := func(tag string) {
		for k, w := range want {
			rc, _, err := cs.GetLatest(ctx, k)
			if err != nil {
				t.Fatalf("%s GetLatest %v: %v", tag, k, err)
			}
			got, rerr := io.ReadAll(rc)
			rc.Close()
			if rerr != nil {
				t.Fatalf("%s ReadAll %v: %v", tag, k, rerr)
			}
			if !bytes.Equal(got, w) {
				t.Fatalf("%s object %v round-trip mismatch (%d vs %d bytes, first-diff %d)",
					tag, k, len(got), len(w), firstDiffOffset(got, w))
			}
		}
	}
	readAll("post-compact")

	// GC reclaims the orphaned source packs; reads STILL work from the new ones.
	gc := NewChunkedGC(cs.upstream, chunks, slog.Default())
	gc.SafetyWindow = 0
	if _, err := gc.RunOnceForDomain(ctx, domain); err != nil {
		t.Fatalf("GC: %v", err)
	}
	after, err := countTenantBlobs(ctx, chunks, domain)
	if err != nil {
		t.Fatal(err)
	}
	if after != res.PacksWritten {
		t.Errorf("after GC: %d packs remain, want %d (the consolidated set)", after, res.PacksWritten)
	}
	if after >= before {
		t.Errorf("compaction+GC did not reduce pack count: before=%d after=%d", before, after)
	}
	readAll("post-gc")
}
