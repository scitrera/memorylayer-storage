// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// TestLocal_ClassControlsCompression proves the end-to-end payoff: under a
// COMPRESSING local store (the default zstd policy), a slice written
// ClassUncompressed stays uncompressed on disk (≈ original size, range-readable +
// mmap-safe) while an identical-size ClassDefault slice is compressed — so one
// store holds both classes, selected per slice. Both round-trip byte-identical.
func TestLocal_ClassControlsCompression(t *testing.T) {
	ctx := context.Background()
	// Highly compressible payload so the codec choice is unmistakable in stored size.
	data := bytes.Repeat([]byte("abcdefgh"), 64*1024) // 512 KiB → ~1 chunk/pack

	storedBytes := func(t *testing.T, class StoreClass) (stored int64, readBack []byte) {
		t.Helper()
		l, err := NewLocal(t.TempDir(), "d", 0) // default zstd policy
		if err != nil {
			t.Fatalf("NewLocal: %v", err)
		}
		t.Cleanup(func() { _ = l.Chunks.Close(ctx) })
		if err := l.Store.Write(ctx, 1, data, class); err != nil {
			t.Fatalf("Write class %d: %v", class, err)
		}
		for _, p := range []blobstore.ID{blobstore.ID("chunk-d-"), blobstore.ID("pack-d-")} {
			if err := l.Chunks.ListBlobs(ctx, p, func(md blobstore.Metadata) error {
				stored += md.Length
				return nil
			}); err != nil {
				t.Fatalf("ListBlobs: %v", err)
			}
		}
		got, err := l.Store.Read(ctx, 1)
		if err != nil {
			t.Fatalf("Read class %d: %v", class, err)
		}
		return stored, got
	}

	uncBytes, uncRead := storedBytes(t, ClassUncompressed)
	defBytes, defRead := storedBytes(t, ClassDefault)

	if !bytes.Equal(uncRead, data) || !bytes.Equal(defRead, data) {
		t.Fatalf("round-trip mismatch (unc=%v def=%v)", bytes.Equal(uncRead, data), bytes.Equal(defRead, data))
	}
	if defBytes >= uncBytes {
		t.Errorf("default-class stored %d bytes, expected < uncompressed-class %d (compressible data must shrink under the policy)", defBytes, uncBytes)
	}
	// The uncompressed-class blob must be ~the original size (a small pack-header
	// overhead aside), confirming the store_uncompressed tag overrode the policy.
	if uncBytes < int64(len(data))/2 {
		t.Errorf("uncompressed-class stored only %d bytes for a %d-byte slice — it was compressed despite the class", uncBytes, len(data))
	}
}
