// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// LiveSliceIDs returns the set of slice ids currently referenced by some file
// (the distinct slice_id values in slice_ref). This is the live set for
// refcount-free, mark-and-sweep GC: any stored slice NOT in this set is an
// orphan (its last referencing inode was deleted) and may be reclaimed once it
// is older than the GC safety window. Copy-on-write shares a slice_id across
// inodes, so a slice stays live until its last reference is gone — exactly what
// DISTINCT captures.
func (e *Engine) LiveSliceIDs(ctx context.Context) (map[uint64]struct{}, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT DISTINCT slice_id FROM slice_ref`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	live := make(map[uint64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		live[uint64(id)] = struct{}{}
	}
	return live, rows.Err()
}

// LiveSliceIDsIn returns the subset of ids that are currently referenced by some
// file (have a slice_ref row). It is the batched form of LiveSliceIDs: a cursored
// GC checks one window of stored slice ids at a time instead of loading the entire
// live set into memory (TECH_DEBT #28), so peak RAM is bounded by the batch, not
// the total slice count. An empty input returns an empty set.
func (e *Engine) LiveSliceIDsIn(ctx context.Context, ids []uint64) (map[uint64]struct{}, error) {
	live := make(map[uint64]struct{}, len(ids))
	if len(ids) == 0 {
		return live, nil
	}
	// SELECT DISTINCT slice_id FROM slice_ref WHERE slice_id IN ($1,...,$N)
	args := make([]any, len(ids))
	var in strings.Builder
	for i, id := range ids {
		if i > 0 {
			in.WriteByte(',')
		}
		fmt.Fprintf(&in, "$%d", i+1)
		args[i] = int64(id)
	}
	rows, err := e.db.QueryContext(ctx,
		`SELECT DISTINCT slice_id FROM slice_ref WHERE slice_id IN (`+in.String()+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		live[uint64(id)] = struct{}{}
	}
	return live, rows.Err()
}

// ReapOrphanedInodes deletes crash-orphaned ("zombie") inodes: node rows whose
// link count has reached zero AND that no edge points at, i.e. a file that was
// unlinked while open ("sustained") whose holder crashed before the final Close
// could delete it. Such a node has no name and no live handle, but its slice_ref
// rows still pin its chunks (LiveSliceIDs counts them as live), so slice GC can
// never reclaim them until the node itself is removed. This calls
// deleteInodeRows for each, which clears node/symlink/slice_ref/xattr in the
// same transaction; freed slices then become reclaimable on the next GC pass.
//
// RootInode is never reaped (it is parentless by design). Safe offline only:
// like ClearAllLocks it assumes no live mount is mid-sustain (single-node), so
// it must run from mlfs-admin against a quiesced filesystem, never a live one.
func (e *Engine) ReapOrphanedInodes(ctx context.Context) (int, error) {
	var reaped int
	err := e.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			`SELECT n.inode FROM node n
			   WHERE n.nlink <= 0
			     AND n.inode <> $1
			     AND NOT EXISTS (SELECT 1 FROM edge e WHERE e.inode = n.inode)`,
			int64(RootInode))
		if err != nil {
			return fmt.Errorf("scan orphaned inodes: %w", err)
		}
		var zombies []Ino
		for rows.Next() {
			var ino int64
			if err := rows.Scan(&ino); err != nil {
				rows.Close()
				return fmt.Errorf("scan orphaned inode row: %w", err)
			}
			zombies = append(zombies, Ino(ino))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("scan orphaned inodes: %w", err)
		}
		rows.Close()
		for _, ino := range zombies {
			if err := deleteInodeRows(ctx, tx, ino); err != nil {
				return fmt.Errorf("delete orphaned inode %d: %w", uint64(ino), err)
			}
			reaped++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reaped, nil
}
