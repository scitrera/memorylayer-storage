// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"strconv"
)

// Ownership scopes (L2.8) partition the namespace into independently-leasable
// subtrees. A scope is keyed by the inode of the FIRST-LEVEL directory under
// root that contains the target (the file/dir's top-level ancestor). Using the
// inode — not the path name — makes the key stable across renames WITHIN a
// subtree; only a cross-scope move changes a node's scope.
//
// scope_key encoding:
//   - the root inode itself  -> "root"
//   - anything under root    -> "ino:<first-level-ancestor-inode>"
//
// Granularity is fixed at depth 1 for the first cut; a configurable depth is a
// later refinement (see docs/L2.8_IMPLEMENTATION.md §3).

const rootScopeKey = "root"

func scopeKeyForInode(first Ino) string {
	return "ino:" + strconv.FormatUint(uint64(first), 10)
}

// scopeOf returns the ownership scope key for inode, memoized. It walks
// node.parent up to the root and keys on the first-level ancestor. The walk is
// deterministic, so the cache is a pure memoization; the only mutation of the
// mapping is a cross-scope rename, which calls flushScopeCache.
func (e *Engine) scopeOf(ctx context.Context, inode Ino) (string, error) {
	if inode == RootInode {
		return rootScopeKey, nil
	}
	if v, ok := e.scopeCache.Load(inode); ok {
		return v.(string), nil
	}
	cur := inode
	for {
		var parent int64
		err := e.db.QueryRowContext(ctx,
			`SELECT parent FROM node WHERE inode=$1`, int64(cur)).Scan(&parent)
		if err == sql.ErrNoRows {
			// Inode vanished mid-walk (concurrent delete): fall back to keying on
			// the inode itself rather than failing the caller.
			key := scopeKeyForInode(inode)
			return key, nil
		}
		if err != nil {
			return "", err
		}
		if Ino(parent) == RootInode || Ino(parent) == cur {
			// cur is a first-level node (its parent is root), or we hit the root's
			// self-parent: cur is the scope anchor.
			key := scopeKeyForInode(cur)
			e.scopeCache.Store(inode, key)
			return key, nil
		}
		cur = Ino(parent)
	}
}

// ScopeOf returns the ownership scope key for inode. Exported for the
// coordinator (lease handoff, admin tooling) and tests; the write path uses the
// unexported scopeOf directly.
func (e *Engine) ScopeOf(ctx context.Context, inode Ino) (string, error) {
	return e.scopeOf(ctx, inode)
}

// flushScopeCache clears the inode→scope memo. Called after a cross-scope rename
// (which can move a whole subtree to a different first-level ancestor), so the
// rare correctness case is handled by a cheap full flush rather than precise
// per-descendant invalidation.
func (e *Engine) flushScopeCache() {
	e.scopeCache.Range(func(k, _ any) bool {
		e.scopeCache.Delete(k)
		return true
	})
}
