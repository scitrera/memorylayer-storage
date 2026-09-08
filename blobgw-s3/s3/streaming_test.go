// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// buildChunkedBody encodes data into a valid aws-chunked body whose chunk
// signatures chain from seedSig using signingKey/amzDate/credScope — the exact
// inverse of chunkReader, so a round-trip proves the decoder.
func buildChunkedBody(data []byte, chunkSize int, signingKey []byte, amzDate, credScope, seedSig string) []byte {
	var b bytes.Buffer
	prev := seedSig
	emit := func(payload []byte) {
		sts := strings.Join([]string{
			streamingChunkAlgo, amzDate, credScope, prev, emptyStringSHA256, hexSHA256(payload),
		}, "\n")
		sig := hmacHex(signingKey, sts)
		prev = sig
		fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(payload), sig)
		b.Write(payload)
		b.WriteString("\r\n")
	}
	for off := 0; off < len(data); off += chunkSize {
		end := min(off+chunkSize, len(data))
		emit(data[off:end])
	}
	emit(nil) // terminating zero-length chunk
	return b.Bytes()
}

func TestChunkReaderRoundTrip(t *testing.T) {
	signingKey := signingKey("secret", "20260602", "us-east-1", "s3")
	const (
		amzDate   = "20260602T000000Z"
		credScope = "20260602/us-east-1/s3/aws4_request"
		seed      = "0000000000000000000000000000000000000000000000000000000000000000"
	)
	payload := bytes.Repeat([]byte("memorylayer-dedup-"), 5000) // multi-chunk

	body := buildChunkedBody(payload, 64*1024, signingKey, amzDate, credScope, seed)
	cr := newChunkReader(bytes.NewReader(body), signingKey, amzDate, credScope, seed)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("decoded %d bytes, want %d", len(got), len(payload))
	}
}

func TestChunkReaderRejectsTamper(t *testing.T) {
	signingKey := signingKey("secret", "20260602", "us-east-1", "s3")
	const (
		amzDate   = "20260602T000000Z"
		credScope = "20260602/us-east-1/s3/aws4_request"
		seed      = "1111111111111111111111111111111111111111111111111111111111111111"
	)
	payload := []byte("the quick brown fox")
	body := buildChunkedBody(payload, 8, signingKey, amzDate, credScope, seed)

	// Flip a payload byte: the chunk's recomputed signature no longer matches.
	idx := bytes.IndexByte(body, 'q')
	if idx < 0 {
		t.Fatal("setup: payload byte not found")
	}
	body[idx] = 'Q'

	cr := newChunkReader(bytes.NewReader(body), signingKey, amzDate, credScope, seed)
	if _, err := io.ReadAll(cr); err != errSignatureDoesNotMatch {
		t.Errorf("tampered chunk: got %v, want SignatureDoesNotMatch", err)
	}
}

// TestParseChunkHeaderRejectsOversize asserts an aws-chunked header declaring a
// chunk larger than maxChunkSize is rejected with errEntityTooLarge at parse
// time — i.e. BEFORE readChunk would allocate make([]byte, size) — closing the
// unbounded-allocation DoS. The boundary value (exactly maxChunkSize) is still
// accepted.
func TestParseChunkHeaderRejectsOversize(t *testing.T) {
	// Exactly maxChunkSize is allowed.
	okHeader := fmt.Sprintf("%x;chunk-signature=abc", maxChunkSize)
	if size, _, err := parseChunkHeader(okHeader); err != nil || size != maxChunkSize {
		t.Fatalf("parseChunkHeader(%q): got (%d,%v) want (%d,nil)", okHeader, size, err, maxChunkSize)
	}

	// One byte over is rejected with errEntityTooLarge, and the chunk reader
	// surfaces that without allocating the oversize buffer.
	overHeader := fmt.Sprintf("%x;chunk-signature=abc", maxChunkSize+1)
	if _, _, err := parseChunkHeader(overHeader); err != errEntityTooLarge {
		t.Fatalf("parseChunkHeader(%q): got %v want errEntityTooLarge", overHeader, err)
	}

	// Drive it through the reader: an oversize first chunk fails fast with
	// EntityTooLarge rather than attempting a 1 GiB+ allocation.
	body := fmt.Sprintf("%x;chunk-signature=abc\r\n", 1<<30) // 1 GiB declared
	cr := newChunkReader(strings.NewReader(body), nil, "", "", "")
	if _, err := io.ReadAll(cr); err != errEntityTooLarge {
		t.Fatalf("chunkReader oversize: got %v want errEntityTooLarge", err)
	}
}

func TestParseChunkHeader(t *testing.T) {
	tests := []struct {
		in       string
		wantSize int
		wantSig  string
		wantErr  bool
	}{
		{"400;chunk-signature=abc123", 1024, "abc123", false},
		{"0;chunk-signature=deadbeef", 0, "deadbeef", false},
		{"400", 0, "", true},                  // no signature
		{"zz;chunk-signature=x", 0, "", true}, // bad hex size
		// A chunk declaring more than maxChunkSize (16 MiB) must be rejected at the
		// header — before readChunk's make([]byte, size) can allocate it.
		{"1000001;chunk-signature=x", 0, "", true}, // 16 MiB + 1, oversize
	}
	for _, tc := range tests {
		size, sig, err := parseChunkHeader(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseChunkHeader(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseChunkHeader(%q): %v", tc.in, err)
			continue
		}
		if size != tc.wantSize || sig != tc.wantSig {
			t.Errorf("parseChunkHeader(%q): got (%d,%q) want (%d,%q)", tc.in, size, sig, tc.wantSize, tc.wantSig)
		}
	}
}
