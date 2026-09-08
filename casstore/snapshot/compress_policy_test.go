// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import "testing"

func TestContentTypePolicy_CodecFor(t *testing.T) {
	p := NewContentTypePolicy(CompressZstd)
	cases := []struct {
		ct   string
		want CompressionAlgo
	}{
		// Compressible / generic.
		{"", CompressZstd},
		{"text/plain", CompressZstd},
		{"text/plain; charset=utf-8", CompressZstd},
		{"application/json", CompressZstd},
		{"application/octet-stream", CompressZstd},
		{"application/pdf", CompressZstd},
		// Already-compressed media families.
		{"image/png", CompressNone},
		{"image/jpeg", CompressNone},
		{"IMAGE/PNG", CompressNone}, // case-insensitive
		{"video/mp4", CompressNone},
		{"audio/mpeg", CompressNone},
		// Already-compressed application types.
		{"application/zstd", CompressNone},
		{"application/gzip", CompressNone},
		{"application/zip", CompressNone},
		{"application/x-xz", CompressNone},
		// GPU-loadable tensor formats.
		{"application/x-safetensors", CompressNone},
		{"application/x-numpy", CompressNone},
		{"tensor/gpu-loadable", CompressNone},
	}
	for _, c := range cases {
		if got := p.CodecFor(c.ct); got != c.want {
			t.Errorf("CodecFor(%q) = %q, want %q", c.ct, got, c.want)
		}
	}
}

func TestContentTypePolicy_GzipCodec(t *testing.T) {
	p := NewContentTypePolicy(CompressGzip)
	if got := p.CodecFor("text/plain"); got != CompressGzip {
		t.Errorf("CodecFor(text/plain) = %q, want gzip", got)
	}
	if got := p.CodecFor("image/png"); got != CompressNone {
		t.Errorf("CodecFor(image/png) = %q, want none", got)
	}
}

func TestContentTypePolicy_NoneDisablesEverything(t *testing.T) {
	p := NewContentTypePolicy(CompressNone)
	for _, ct := range []string{"", "text/plain", "application/json", "image/png"} {
		if got := p.CodecFor(ct); got != CompressNone {
			t.Errorf("disabled policy CodecFor(%q) = %q, want none", ct, got)
		}
	}
}

func TestPackBlobRoundTrip(t *testing.T) {
	raw := []byte("the quick brown fox jumps over the lazy dog, repeated for compressibility. ")
	for i := 0; i < 8; i++ {
		raw = append(raw, raw...)
	}
	for _, algo := range []CompressionAlgo{CompressNone, CompressZstd, CompressGzip} {
		stored, err := compressPackBlob(raw, algo)
		if err != nil {
			t.Fatalf("compressPackBlob(%s): %v", algo, err)
		}
		got, err := decompressPackBlob(stored)
		if err != nil {
			t.Fatalf("decompressPackBlob(%s): %v", algo, err)
		}
		if string(got) != string(raw) {
			t.Fatalf("%s: round-trip mismatch", algo)
		}
		if algo != CompressNone && len(stored) >= len(raw) {
			t.Fatalf("%s: stored %d not smaller than raw %d", algo, len(stored), len(raw))
		}
	}
}

// TestDecompressLegacyHeaderlessPack verifies a pack written before compression
// existed (no header) is returned unchanged.
func TestDecompressLegacyHeaderlessPack(t *testing.T) {
	// Arbitrary bytes that do not start with packMagic.
	legacy := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06}
	got, err := decompressPackBlob(legacy)
	if err != nil {
		t.Fatalf("decompressPackBlob(legacy): %v", err)
	}
	if string(got) != string(legacy) {
		t.Fatalf("legacy pack altered: got %v want %v", got, legacy)
	}
}
