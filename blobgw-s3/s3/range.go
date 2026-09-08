// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"strconv"
	"strings"
)

// byteRange is a resolved, satisfiable byte range over an object of a known
// size: [start, end] inclusive, 0 <= start <= end < size.
type byteRange struct {
	start int64
	end   int64 // inclusive
}

// length returns the number of bytes the range covers.
func (r byteRange) length() int64 { return r.end - r.start + 1 }

// parseRange interprets a single HTTP Range header value against an object of
// size bytes. It supports the three S3-honored forms:
//
//	bytes=N-M   first N..M inclusive
//	bytes=N-    from N to the end
//	bytes=-S    the last S bytes (suffix)
//
// It returns (zero, false, nil) when the header is absent or not a byte-range
// (the caller serves the whole object), a resolved range with ok=true when the
// range is satisfiable, and ok=false with satisfiable=false when the range is
// syntactically valid but unsatisfiable for this size (caller answers 416).
// A malformed header is treated as "no range" per RFC 7233 (ignored), so a
// garbled header never downgrades a whole-object GET to an error.
func parseRange(header string, size int64) (rng byteRange, ok bool, satisfiable bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(header), "bytes=")
	if !found {
		return byteRange{}, false, true
	}
	// We honor only a single range; a multi-range header (comma-separated) is
	// ignored and the whole object is served (acceptable per the spec).
	if strings.Contains(spec, ",") {
		return byteRange{}, false, true
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return byteRange{}, false, true // malformed → ignore
	}
	startStr, endStr := spec[:dash], spec[dash+1:]

	switch {
	case startStr == "" && endStr == "":
		return byteRange{}, false, true // "bytes=-" → malformed, ignore

	case startStr == "":
		// Suffix range: last endStr bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n < 0 {
			return byteRange{}, false, true
		}
		if n == 0 {
			// "bytes=-0" requests the last zero bytes — unsatisfiable.
			return byteRange{}, true, false
		}
		if n >= size {
			n = size // clamp: whole object
		}
		if size == 0 {
			return byteRange{}, true, false
		}
		return byteRange{start: size - n, end: size - 1}, true, true

	case endStr == "":
		// "bytes=N-" → N to end.
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return byteRange{}, false, true
		}
		if start >= size {
			return byteRange{}, true, false // start past EOF → unsatisfiable
		}
		return byteRange{start: start, end: size - 1}, true, true

	default:
		// "bytes=N-M".
		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end < 0 || start > end {
			return byteRange{}, false, true // malformed → ignore
		}
		if start >= size {
			return byteRange{}, true, false // start past EOF → unsatisfiable
		}
		if end >= size {
			end = size - 1 // clamp to last byte
		}
		return byteRange{start: start, end: end}, true, true
	}
}
