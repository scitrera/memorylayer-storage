// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"errors"
	"strings"
)

// ErrorKind maps a raw error to the small bounded error.kind taxonomy of
// docs/OBSERVABILITY.md (not_found/already_exists/timeout/auth/range/conflict/
// canceled/other). It is the blobgw-side twin of casstore/blobstore/obs.errorKind,
// kept consistent so a dashboard's error.kind label means the same thing whether
// the failure surfaced at the backing store (casstore) or at a blobgw data-path
// boundary (PG index, control-plane RPC, HTTP). Bounded by construction: every
// branch returns one of the fixed labels, never a raw error string.
//
// Sentinels match first; the string fallbacks classify the common S3/PG/NATS
// conditions (notably auth/credential expiry — the "token has expired" prod GC
// failure) that have no typed error reaching this layer. not_found/already_exists
// are well-defined responses, not failures — dashboards exclude them from failure
// alerts.
func ErrorKind(err error) string {
	if err == nil {
		return "ok"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no rows") || strings.Contains(s, "not found") ||
		strings.Contains(s, "NoSuchKey") || strings.Contains(s, "NotFound"):
		return "not_found"
	case strings.Contains(s, "already exists") || strings.Contains(s, "duplicate") ||
		strings.Contains(s, "AlreadyExists"):
		return "already_exists"
	case strings.Contains(s, "InvalidRange") || strings.Contains(s, "invalid range"):
		return "range"
	case strings.Contains(s, "expired") || strings.Contains(s, "AccessDenied") ||
		strings.Contains(s, "ExpiredToken") || strings.Contains(s, "InvalidAccessKeyId") ||
		strings.Contains(s, "Unauthorized") || strings.Contains(s, "permission denied"):
		return "auth"
	case strings.Contains(s, "conflict") || strings.Contains(s, "Conflict") ||
		strings.Contains(s, "serialize") || strings.Contains(s, "serialization") ||
		strings.Contains(s, "deadlock"):
		return "conflict"
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return "timeout"
	default:
		return "other"
	}
}
