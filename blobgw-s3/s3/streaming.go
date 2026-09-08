// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bufio"
	"crypto/hmac"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// chunkReader decodes an aws-chunked (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) request
// body and verifies the per-chunk signature chain as it streams. It implements
// io.Reader so the handler can pass it straight into Gateway.Put — the bytes are
// chunked+deduped by casstore as they are decoded, never fully buffered.
//
// Wire format (per chunk):
//
//	<hex-size>;chunk-signature=<hex-sig>\r\n
//	<chunk-data of hex-size bytes>\r\n
//
// terminated by a zero-length chunk. Each chunk signature is chained:
//
//	stringToSign = "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n"
//	             + prevSignature + "\n" + SHA256("") + "\n" + SHA256(chunkData)
//	chunkSig     = HMAC-SHA256(signingKey, stringToSign)
//
// where prevSignature seeds from the request's seed signature. A mismatch
// surfaces as SignatureDoesNotMatch via Read's error, which the handler maps to
// the S3 error response.
type chunkReader struct {
	br         *bufio.Reader
	signingKey []byte
	amzDate    string
	credScope  string
	prevSig    string

	pending []byte // decoded bytes of the current chunk not yet returned
	done    bool   // final (zero-length) chunk consumed
	err     error  // sticky terminal error
}

// newChunkReader wraps body. seedSig is the request's verified seed signature;
// signingKey/amzDate/credScope are the same values used for the seed, reused to
// chain each chunk.
func newChunkReader(body io.Reader, signingKey []byte, amzDate, credScope, seedSig string) *chunkReader {
	return &chunkReader{
		br:         bufio.NewReader(body),
		signingKey: signingKey,
		amzDate:    amzDate,
		credScope:  credScope,
		prevSig:    seedSig,
	}
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for len(c.pending) == 0 {
		if c.done {
			return 0, io.EOF
		}
		if err := c.readChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// readChunk reads one chunk header + data, verifies the chunk signature, and
// stages the decoded bytes in c.pending. A zero-length chunk marks the stream
// done.
func (c *chunkReader) readChunk() error {
	header, err := c.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("s3 chunk: read header: %w", err)
	}
	header = strings.TrimRight(header, "\r\n")
	size, chunkSig, err := parseChunkHeader(header)
	if err != nil {
		return err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(c.br, data); err != nil {
		return fmt.Errorf("s3 chunk: read data: %w", err)
	}
	// Each chunk (including the final empty one) is followed by CRLF.
	if err := c.expectCRLF(); err != nil {
		return err
	}

	want := c.chunkSignature(data)
	if !hmac.Equal([]byte(want), []byte(chunkSig)) {
		return errSignatureDoesNotMatch
	}
	c.prevSig = want

	if size == 0 {
		c.done = true
		return nil
	}
	c.pending = data
	return nil
}

func (c *chunkReader) expectCRLF() error {
	cr, err := c.br.ReadByte()
	if err != nil {
		return fmt.Errorf("s3 chunk: trailing CR: %w", err)
	}
	lf, err := c.br.ReadByte()
	if err != nil {
		return fmt.Errorf("s3 chunk: trailing LF: %w", err)
	}
	if cr != '\r' || lf != '\n' {
		return errors.New("s3 chunk: malformed chunk terminator")
	}
	return nil
}

// chunkSignature computes the expected signature for a chunk's data, chained
// from the previous signature.
func (c *chunkReader) chunkSignature(data []byte) string {
	sts := strings.Join([]string{
		streamingChunkAlgo,
		c.amzDate,
		c.credScope,
		c.prevSig,
		emptyStringSHA256,
		hexSHA256(data),
	}, "\n")
	return hmacHex(c.signingKey, sts)
}

// maxChunkSize bounds the declared size of a single aws-chunked chunk. The
// decoder allocates a buffer of exactly the declared size before reading the
// data, so an attacker-controlled header could otherwise force an unbounded
// allocation (DoS). 16 MiB comfortably exceeds the SDKs' chunk sizes while
// capping the per-chunk buffer; an over-large declaration is rejected before any
// allocation and surfaces as EntityTooLarge (HTTP 400).
const maxChunkSize = 16 << 20 // 16 MiB

// parseChunkHeader splits "<hex-size>;chunk-signature=<sig>" into its parts.
func parseChunkHeader(header string) (int, string, error) {
	sizeStr, rest, ok := strings.Cut(header, ";")
	if !ok {
		return 0, "", errors.New("s3 chunk: header missing chunk-signature")
	}
	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 {
		return 0, "", fmt.Errorf("s3 chunk: bad size %q", sizeStr)
	}
	if size > maxChunkSize {
		// Reject before readChunk's make([]byte, size): an oversize declared chunk
		// must never drive an allocation.
		return 0, "", errEntityTooLarge
	}
	sig, ok := strings.CutPrefix(strings.TrimSpace(rest), "chunk-signature=")
	if !ok {
		return 0, "", errors.New("s3 chunk: header missing chunk-signature")
	}
	return int(size), sig, nil
}
