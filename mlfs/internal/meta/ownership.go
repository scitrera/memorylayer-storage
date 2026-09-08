// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
)

// Lease is the state of one scope's ownership row. Generation is the fencing
// token: it strictly increases each time ownership changes hands, so a stale
// owner's writes (guarded by `AND generation = $held`) are rejected after the
// scope has been stolen. SQL is the system of record; the lease is a
// write-affinity hint, and generation is the correctness primitive (L2.8
// threads it through every metadata UPDATE).
type Lease struct {
	ScopeKey      string
	OwnerNode     string
	ExpiresUnixMs int64
	Generation    int64
}

// AcquireOrResume takes (or renews) the lease on scopeKey for node, for ttlMs.
// It succeeds if the scope is unowned, expired, or already owned by node;
// taking it from an expired owner bumps the generation. It returns the lease
// and whether node now holds it. A live lease held by another node is returned
// with held=false (no change).
func (e *Engine) AcquireOrResume(ctx context.Context, scopeKey, node string, ttlMs int64) (lease Lease, held bool, err error) {
	defer e.recordOp("lease_acquire", e.now(), &err)
	nowMs := e.now().UnixMilli()
	expires := nowMs + ttlMs
	err = e.withTx(ctx, func(tx *sql.Tx) error {
		var cur Lease
		cur.ScopeKey = scopeKey
		serr := tx.QueryRowContext(ctx,
			`SELECT owner_node, lease_expires_unix_ms, generation FROM ownership WHERE scope_key=$1 FOR UPDATE`,
			scopeKey).Scan(&cur.OwnerNode, &cur.ExpiresUnixMs, &cur.Generation)
		switch {
		case serr == sql.ErrNoRows:
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO ownership (scope_key, owner_node, lease_expires_unix_ms, generation) VALUES ($1,$2,$3,1)`,
				scopeKey, node, expires); err != nil {
				return err
			}
			lease, held = Lease{scopeKey, node, expires, 1}, true
			return nil
		case serr != nil:
			return serr
		}
		switch {
		case cur.OwnerNode == node:
			// Renew in place; generation unchanged (no ownership change).
			if _, err := tx.ExecContext(ctx,
				`UPDATE ownership SET lease_expires_unix_ms=$2 WHERE scope_key=$1`, scopeKey, expires); err != nil {
				return err
			}
			lease, held = Lease{scopeKey, node, expires, cur.Generation}, true
		case cur.ExpiresUnixMs < nowMs:
			// Expired: steal it and bump the fence.
			gen := cur.Generation + 1
			if _, err := tx.ExecContext(ctx,
				`UPDATE ownership SET owner_node=$2, lease_expires_unix_ms=$3, generation=$4 WHERE scope_key=$1`,
				scopeKey, node, expires, gen); err != nil {
				return err
			}
			lease, held = Lease{scopeKey, node, expires, gen}, true
		default:
			// Live lease held by another node.
			lease, held = cur, false
		}
		return nil
	})
	return lease, held, err
}

// Refresh extends node's lease on scopeKey only if it still owns it at the
// given generation. Returns false if the scope was stolen (fenced out).
func (e *Engine) Refresh(ctx context.Context, scopeKey, node string, generation, ttlMs int64) (ok bool, err error) {
	defer e.recordOp("lease_refresh", e.now(), &err)
	expires := e.now().UnixMilli() + ttlMs
	res, rerr := e.db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms=$2 WHERE scope_key=$1 AND owner_node=$3 AND generation=$4`,
		scopeKey, expires, node, generation)
	if rerr != nil {
		err = rerr
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// ReleaseOwnership voluntarily gives up scopeKey by expiring its lease, but
// only if this node still holds it at generation. A successor's AcquireOrResume
// then sees the expired lease and steals it (bumping the generation), which
// fences any late write from this node. Used by the ownership-handoff path
// (L2.8.4): the prior owner releases so the requester can take over immediately
// rather than waiting out the TTL. Returns true if the release applied.
func (e *Engine) ReleaseOwnership(ctx context.Context, scopeKey, node string, generation int64) (bool, error) {
	res, err := e.db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms=0 WHERE scope_key=$1 AND owner_node=$2 AND generation=$3`,
		scopeKey, node, generation)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// Fenced reports whether scopeKey is still at the given generation — the check
// a write performs (via "AND generation=$held") to know it has not been
// fenced out. A missing scope reports false.
func (e *Engine) Fenced(ctx context.Context, scopeKey string, generation int64) (bool, error) {
	var cur int64
	err := e.db.QueryRowContext(ctx, `SELECT generation FROM ownership WHERE scope_key=$1`, scopeKey).Scan(&cur)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return cur == generation, nil
}
