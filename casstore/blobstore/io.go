// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"bytes"
	"io"
)

// BytesFromSlice wraps a byte slice in the Bytes interface kopia's PutBlob
// expects. kopia's own gather.Bytes is the production implementation and
// handles non-contiguous buffers for zero-copy IO; this simple version is
// fine for everything sandbox-provider does (chunks are small + already
// contiguous before they leave the chunker).
func BytesFromSlice(b []byte) Bytes {
	return &byteSliceBytes{data: b}
}

type byteSliceBytes struct{ data []byte }

func (b *byteSliceBytes) Length() int { return len(b.data) }
func (b *byteSliceBytes) Reader() io.ReadSeekCloser {
	return &readSeekCloser{r: bytes.NewReader(b.data)}
}
func (b *byteSliceBytes) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.data)
	return int64(n), err
}

type readSeekCloser struct{ r *bytes.Reader }

func (rsc *readSeekCloser) Read(p []byte) (int, error) { return rsc.r.Read(p) }
func (rsc *readSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return rsc.r.Seek(offset, whence)
}
func (rsc *readSeekCloser) Close() error { return nil }

// NewOutputBuffer returns an OutputBuffer suitable for GetBlob. The
// returned buffer accumulates bytes in memory; pair with ReadOutputBuffer
// to retrieve the contents.
func NewOutputBuffer() *OutputBufferImpl { return &OutputBufferImpl{} }

// OutputBufferImpl is the concrete OutputBuffer used by GetBlob in our
// adapter. Public so callers can also use ReadAll on it without further
// wrapping.
type OutputBufferImpl struct{ buf bytes.Buffer }

func (o *OutputBufferImpl) Write(p []byte) (int, error) { return o.buf.Write(p) }
func (o *OutputBufferImpl) Length() int                 { return o.buf.Len() }
func (o *OutputBufferImpl) Reset()                      { o.buf.Reset() }

// Bytes returns the accumulated bytes as a defensive copy. Use for one-shot
// reads; for streaming use Reader.
func (o *OutputBufferImpl) Bytes() []byte { return append([]byte(nil), o.buf.Bytes()...) }

// Reader returns a streaming reader over the accumulated bytes. The
// returned reader shares memory with the buffer; do not Reset while a
// caller still holds the reader.
func (o *OutputBufferImpl) Reader() io.Reader { return bytes.NewReader(o.buf.Bytes()) }
