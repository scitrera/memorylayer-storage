// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import "errors"

// Kind is a stable, wire-agnostic classification of a gateway error. It is the
// single source of truth for the sentinel→category decision so each HTTP
// front end (blobgw's JSON server, the edge, the S3 layer) maps ONE classifier
// to its own status codes and message/policy, instead of re-deriving the
// sentinel switch independently. The classifier decides the category only; each
// caller keeps its own wire formatting (JSON vs S3-XML), its own messages, and
// its own policy (e.g. the edge's confidential-mode existence hiding).
type Kind int

const (
	// KindInternal is the default for any error that is not one of the gateway
	// sentinels below (mapped to 5xx / InternalError by callers).
	KindInternal Kind = iota
	// KindNotFound classifies ErrNotFound (the ref is unknown or still pending).
	KindNotFound
	// KindInvalidRef classifies ErrInvalidRef (empty/malformed ref).
	KindInvalidRef
	// KindNotStaged classifies ErrNotStaged (Finalize on a ref with no pending
	// staged upload).
	KindNotStaged
	// KindIntegrity classifies ErrIntegrity (staged bytes failed the finalize
	// integrity gate).
	KindIntegrity
)

// Classify maps a gateway error to its stable Kind. It checks the sentinels in
// (errors.Is) and returns KindInternal for anything else (including nil). It is
// the centralized sentinel→category decision shared by every server's error
// mapper; the wire status/code/message stays each caller's own concern.
func Classify(err error) Kind {
	switch {
	case errors.Is(err, ErrNotFound):
		return KindNotFound
	case errors.Is(err, ErrInvalidRef):
		return KindInvalidRef
	case errors.Is(err, ErrNotStaged):
		return KindNotStaged
	case errors.Is(err, ErrIntegrity):
		return KindIntegrity
	default:
		return KindInternal
	}
}
