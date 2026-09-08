// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import "errors"

// Integrity sentinels wrap read-path faults where the stored bytes no longer match
// what a manifest promises. Consumers errors.Is() these to raise the data-integrity
// canary (docs/OBSERVABILITY.md) and to distinguish an unrecoverable read from a
// transient backend error. casstore itself stays telemetry-free; these are plain
// typed errors wrapped (via %w) onto the existing detailed messages, so no caller
// that only formats the error sees any change.
var (
	// ErrCorrupt: a chunk failed its content-hash check, a manifest offset/size was
	// out of the pack's bounds, or a pack would not decompress — the bytes are not
	// what the manifest describes.
	ErrCorrupt = errors.New("casstore: data integrity (corrupt)")

	// ErrMissingBlob: a chunk/pack that a live manifest references is absent from
	// the backing store — the canary for GC over-deletion (TECH_DEBT #50 class) or
	// an out-of-band delete.
	ErrMissingBlob = errors.New("casstore: referenced blob missing")
)

// IntegrityKind classifies err for the integrity counter's error.kind attribute:
// "corrupt", "not_found", or "" when err is not an integrity fault. Consumers use
// it to bump their data-integrity instrument without re-deriving the taxonomy.
func IntegrityKind(err error) string {
	switch {
	case errors.Is(err, ErrCorrupt):
		return "corrupt"
	case errors.Is(err, ErrMissingBlob):
		return "not_found"
	default:
		return ""
	}
}
