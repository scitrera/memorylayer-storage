// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"context"
	"errors"
	"strings"

	kblob "github.com/kopia/kopia/repo/blob"
)

// PutIfAbsent writes the blob at id if and only if no blob currently exists
// there. The semantics are equivalent to PUT-with-If-None-Match-* in HTTP
// terms. Returns nil on a successful write OR on the "already exists" case
// — for content-addressed Puts (key = hash of content) the "already exists"
// case is a successful no-op.
//
// Backend support varies:
//
//   - S3-compatible backends honor PutOptions.DoNotRecreate natively via
//     S3 conditional writes; that's the fast path (one network round-trip
//     either way).
//   - Filesystem and other backends reject DoNotRecreate with
//     ErrUnsupportedPutBlobOption; for those we fall back to a check-
//     then-write sequence: GetMetadata first, skip Put if present.
//
// The fallback is RACY by definition — between the check and the write,
// another writer can put the same key. For the sandbox-provider use case
// this is acceptable because:
//   - All chunked-snapshot writes happen on the leader replica (the
//     reaper and periodic snapshotter are leader-gated).
//   - The chunk content is content-addressed by hash; if two writers race
//     to put the same content, the result is identical bytes either way.
//     The "loser" wastes one network round-trip but does not corrupt the
//     blob.
//
// Returns the result of PutBlob (or nil for the already-exists case).
func PutIfAbsent(ctx context.Context, st Storage, id ID, data Bytes) error {
	// Fast path: backend honors DoNotRecreate natively (S3, etc.)
	err := st.PutBlob(ctx, id, data, PutOptions{DoNotRecreate: true})
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrBlobAlreadyExists) {
		return nil
	}
	if !isUnsupportedDoNotRecreate(err) {
		return err
	}

	// Fallback: backend doesn't support DoNotRecreate. HEAD-then-PUT.
	if _, getErr := st.GetMetadata(ctx, id); getErr == nil {
		return nil
	} else if !errors.Is(getErr, ErrBlobNotFound) {
		return getErr
	}
	return st.PutBlob(ctx, id, data, PutOptions{})
}

// isUnsupportedDoNotRecreate reports whether err originates from a backend
// that doesn't honor PutOptions.DoNotRecreate. We can't errors.Is against
// the concrete sentinel because kopia wraps it with errors.Wrap which
// loses errors.Is compatibility, so we sniff for the marker string the
// kopia wrap produces.
func isUnsupportedDoNotRecreate(err error) bool {
	if errors.Is(err, kblob.ErrUnsupportedPutBlobOption) {
		return true
	}
	// kopia wraps the sentinel with errors.Wrap("do-not-recreate", ...),
	// which produces an error whose Error() contains the wrapped string.
	return strings.Contains(err.Error(), "do-not-recreate") &&
		strings.Contains(err.Error(), "unsupported put-blob option")
}
