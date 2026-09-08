// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
)

// errFenced is the in-transaction sentinel meaning "this node no longer owns the
// scope at the expected generation" — i.e. ownership was stolen out from under
// an in-flight write. Op-level callers map it to syscall.ESTALE (a stale handle:
// the caller re-resolves and retries, by which point a handoff has settled).
var errFenced = errors.New("mlfs meta: fenced (ownership generation moved)")

// acquireScope resolves the ownership scope for inode and ensures this node
// holds the lease, returning the scope key and the generation to fence the
// write on. It runs BEFORE the write transaction (it may itself open a tx to
// acquire the lease); checkFence then re-validates the generation atomically
// inside the write tx, closing the acquire→write race.
//
// In single-node mode (owner disabled) it is a zero-cost no-op: no scope walk,
// no lease call — the returned scope/gen are unused because checkFence is also
// a no-op. This keeps the pre-L2.8 single-node path byte-for-byte unchanged.
func (e *Engine) acquireScope(ctx context.Context, inode Ino) (scope string, gen int64, st syscall.Errno) {
	if !e.owner.Enabled() {
		return "", 0, 0
	}
	s, err := e.scopeOf(ctx, inode)
	if err != nil {
		return "", 0, e.eio(ctx, "scopeOf", err)
	}
	g, held, err := e.owner.Ensure(ctx, s)
	if err != nil {
		return "", 0, e.eio(ctx, "ensureOwner", err)
	}
	if !held {
		// Another live node owns this scope; surface a retryable stale error.
		return "", 0, syscall.ESTALE
	}
	return s, g, 0
}

// fenceInTx is the one-line guard called at the top of every mutating withTx
// closure. On a fence rejection it sets *out = ESTALE and returns errAbort (so
// the tx rolls back and the op reports a stale handle); a genuine SQL error is
// returned verbatim; nil means proceed. A no-op in single-node mode.
func (e *Engine) fenceInTx(ctx context.Context, tx *sql.Tx, scope string, gen int64, out *syscall.Errno) error {
	err := e.checkFence(ctx, tx, scope, gen)
	if err == nil {
		return nil
	}
	if err == errFenced {
		// Multi-node canary: a write hit a generation that moved out from under it
		// (ownership stolen/handed off). Count it; the op returns a retryable stale
		// handle.
		if e.rec != nil {
			e.rec.FenceRejected()
		}
		*out = syscall.ESTALE
		return errAbort
	}
	return err
}

// checkFence verifies, inside the write transaction, that this node still owns
// scope at the given generation. SELECT ... FOR SHARE serializes against a
// concurrent steal (AcquireOrResume takes FOR UPDATE on the same row and bumps
// generation), so a write can never commit under a generation that has already
// moved. Returns errFenced when ownership has shifted; a no-op in single-node
// mode.
func (e *Engine) checkFence(ctx context.Context, tx *sql.Tx, scope string, gen int64) error {
	if !e.owner.Enabled() {
		return nil
	}
	var ownerNode string
	var cur int64
	err := tx.QueryRowContext(ctx,
		`SELECT owner_node, generation FROM ownership WHERE scope_key=$1 FOR SHARE`,
		scope).Scan(&ownerNode, &cur)
	if err == sql.ErrNoRows {
		return errFenced // scope no longer owned at all
	}
	if err != nil {
		return err
	}
	if ownerNode != e.nodeID || cur != gen {
		return errFenced
	}
	return nil
}
