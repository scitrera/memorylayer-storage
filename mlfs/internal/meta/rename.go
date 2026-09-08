// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"syscall"
)

// Rename moves srcName under parentSrc to dstName under parentDst, atomically
// in one SQL transaction (so a rename across ownership scopes is a single
// commit — the plan's "rename across owners works as one SQL transaction").
// An existing destination is replaced: a file is unlinked, an empty directory
// is removed; a non-empty directory yields ENOTEMPTY. RenameNoReplace fails
// with EEXIST if the destination exists. RenameExchange atomically swaps the
// two named entries (both must exist). RenameWhiteout (overlayfs) is not
// supported and returns ENOSYS.
func (e *Engine) Rename(ctx context.Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, c *Context) (Ino, *Attr, syscall.Errno) {
	if flags&RenameExchange != 0 {
		// RENAME_EXCHANGE is mutually exclusive with the other flags in the
		// kernel; any extra bit (NOREPLACE/WHITEOUT) alongside it is invalid.
		if flags&^RenameExchange != 0 {
			return 0, nil, syscall.EINVAL
		}
		return e.renameExchange(ctx, parentSrc, nameSrc, parentDst, nameDst, c)
	}
	if flags&^RenameNoReplace != 0 {
		return 0, nil, syscall.ENOSYS // WHITEOUT not implemented
	}
	// A rename can span two ownership scopes (src vs dst parent); hold and fence
	// both. Acquired in scope-key order would avoid cross-rename deadlock, but
	// acquisition here is non-blocking (lease CAS), so request order is safe.
	scopeSrc, genSrc, st := e.acquireScope(ctx, parentSrc)
	if st != 0 {
		return 0, nil, st
	}
	scopeDst, genDst, st := e.acquireScope(ctx, parentDst)
	if st != 0 {
		return 0, nil, st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	var srcIno int64
	var srcType int

	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scopeSrc, genSrc, &out); ferr != nil {
			return ferr
		}
		if scopeDst != scopeSrc {
			if ferr := e.fenceInTx(ctx, tx, scopeDst, genDst, &out); ferr != nil {
				return ferr
			}
		}
		// Source must exist.
		if err := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`,
			int64(parentSrc), []byte(nameSrc)).Scan(&srcIno, &srcType); err == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if err != nil {
			return err
		}

		// Per-parent nlink deltas (dir add/remove changes a parent's link count).
		delta := map[Ino]int{}

		// Destination, if present.
		var dstIno int64
		var dstType int
		dstErr := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`,
			int64(parentDst), []byte(nameDst)).Scan(&dstIno, &dstType)
		switch {
		case dstErr == sql.ErrNoRows:
			// no destination
		case dstErr != nil:
			return dstErr
		default:
			if flags&RenameNoReplace != 0 {
				out = syscall.EEXIST
				return errAbort
			}
			if dstIno == srcIno {
				return nil // same entry, no-op
			}
			srcIsDir := uint8(srcType) == TypeDirectory
			dstIsDir := uint8(dstType) == TypeDirectory
			if dstIsDir && !srcIsDir {
				out = syscall.EISDIR
				return errAbort
			}
			if !dstIsDir && srcIsDir {
				out = syscall.ENOTDIR
				return errAbort
			}
			if dstIsDir {
				var cnt int
				if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM edge WHERE parent=$1`, dstIno).Scan(&cnt); err != nil {
					return err
				}
				if cnt > 0 {
					out = syscall.ENOTEMPTY
					return errAbort
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM node WHERE inode=$1`, dstIno); err != nil {
					return err
				}
				delta[parentDst]-- // lost a subdirectory
			} else {
				// Replace a file: drop a link, delete node if it hits zero.
				var nlink int64
				if err := tx.QueryRowContext(ctx,
					`UPDATE node SET nlink=nlink-1, ctime=$2, ctimensec=$3 WHERE inode=$1 RETURNING nlink`,
					dstIno, ts, int(nsec)).Scan(&nlink); err != nil {
					return err
				}
				if nlink <= 0 {
					if _, err := tx.ExecContext(ctx, `DELETE FROM node WHERE inode=$1`, dstIno); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, `DELETE FROM symlink WHERE inode=$1`, dstIno); err != nil {
						return err
					}
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM edge WHERE parent=$1 AND name=$2`, int64(parentDst), []byte(nameDst)); err != nil {
				return err
			}
		}

		// Move the edge: remove source, insert destination.
		if _, err := tx.ExecContext(ctx, `DELETE FROM edge WHERE parent=$1 AND name=$2`, int64(parentSrc), []byte(nameSrc)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO edge (parent, name, inode, type) VALUES ($1,$2,$3,$4)`,
			int64(parentDst), []byte(nameDst), srcIno, srcType); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET parent=$2, ctime=$3, ctimensec=$4 WHERE inode=$1`,
			srcIno, int64(parentDst), ts, int(nsec)); err != nil {
			return err
		}
		if uint8(srcType) == TypeDirectory && parentSrc != parentDst {
			delta[parentSrc]--
			delta[parentDst]++
		}

		// Apply nlink deltas + bump mtime/ctime on both parents.
		for p, d := range delta {
			if _, err := tx.ExecContext(ctx, `UPDATE node SET nlink=nlink+$2 WHERE inode=$1`, int64(p), d); err != nil {
				return err
			}
		}
		for _, p := range distinctParents(parentSrc, parentDst) {
			if _, err := tx.ExecContext(ctx,
				`UPDATE node SET mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`,
				int64(p), ts, int(nsec)); err != nil {
				return err
			}
		}
		return e.appendChangelog(ctx, tx, Ino(srcIno), opRename,
			map[string]any{"src_parent": parentSrc, "src_name": nameSrc, "dst_parent": parentDst, "dst_name": nameDst})
	})
	if out != 0 {
		return 0, nil, out
	}
	if txErr != nil {
		return 0, nil, e.eio(ctx, "rename", txErr)
	}
	if scopeSrc != scopeDst {
		// A subtree moved between first-level ancestors: its descendants' cached
		// scope is now stale. Flush the (rare-path) memo.
		e.flushScopeCache()
	}
	return Ino(srcIno), e.GetAttrOrNil(ctx, Ino(srcIno)), 0
}

// renameExchange atomically swaps the entries (parentSrc,nameSrc) and
// (parentDst,nameDst): each name keeps its slot but points at the other's
// inode. Both must exist (ENOENT otherwise). Their stored parent + ctime are
// updated, parent dir nlinks are adjusted when a directory crosses parents, and
// both parents' mtime/ctime are bumped — all in one transaction.
func (e *Engine) renameExchange(ctx context.Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, c *Context) (Ino, *Attr, syscall.Errno) {
	scopeSrc, genSrc, st := e.acquireScope(ctx, parentSrc)
	if st != 0 {
		return 0, nil, st
	}
	scopeDst, genDst, st := e.acquireScope(ctx, parentDst)
	if st != 0 {
		return 0, nil, st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	var srcIno int64
	var srcType int

	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scopeSrc, genSrc, &out); ferr != nil {
			return ferr
		}
		if scopeDst != scopeSrc {
			if ferr := e.fenceInTx(ctx, tx, scopeDst, genDst, &out); ferr != nil {
				return ferr
			}
		}
		// Both entries must exist.
		var dstIno int64
		var dstType int
		if err := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`,
			int64(parentSrc), []byte(nameSrc)).Scan(&srcIno, &srcType); err == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`,
			int64(parentDst), []byte(nameDst)).Scan(&dstIno, &dstType); err == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if err != nil {
			return err
		}
		if srcIno == dstIno {
			return nil // swapping an entry with itself is a no-op
		}

		// Point each edge at the other inode (names/slots unchanged). The edge's
		// type column tracks its inode, so swap it too.
		if _, err := tx.ExecContext(ctx,
			`UPDATE edge SET inode=$3, type=$4 WHERE parent=$1 AND name=$2`,
			int64(parentSrc), []byte(nameSrc), dstIno, dstType); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE edge SET inode=$3, type=$4 WHERE parent=$1 AND name=$2`,
			int64(parentDst), []byte(nameDst), srcIno, srcType); err != nil {
			return err
		}

		// Update each node's stored parent + ctime to its new home.
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET parent=$2, ctime=$3, ctimensec=$4 WHERE inode=$1`,
			srcIno, int64(parentDst), ts, int(nsec)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET parent=$2, ctime=$3, ctimensec=$4 WHERE inode=$1`,
			dstIno, int64(parentSrc), ts, int(nsec)); err != nil {
			return err
		}

		// A directory moving across parents shifts that parent's link count.
		if parentSrc != parentDst {
			delta := map[Ino]int{}
			if uint8(srcType) == TypeDirectory {
				delta[parentSrc]--
				delta[parentDst]++
			}
			if uint8(dstType) == TypeDirectory {
				delta[parentDst]--
				delta[parentSrc]++
			}
			for p, d := range delta {
				if d == 0 {
					continue
				}
				if _, err := tx.ExecContext(ctx, `UPDATE node SET nlink=nlink+$2 WHERE inode=$1`, int64(p), d); err != nil {
					return err
				}
			}
		}

		// Bump mtime/ctime on the affected parents.
		for _, p := range distinctParents(parentSrc, parentDst) {
			if _, err := tx.ExecContext(ctx,
				`UPDATE node SET mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`,
				int64(p), ts, int(nsec)); err != nil {
				return err
			}
		}
		return e.appendChangelog(ctx, tx, Ino(srcIno), opRename,
			map[string]any{"exchange": true, "src_parent": parentSrc, "src_name": nameSrc, "dst_parent": parentDst, "dst_name": nameDst})
	})
	if out != 0 {
		return 0, nil, out
	}
	if txErr != nil {
		return 0, nil, e.eio(ctx, "renameExchange", txErr)
	}
	if scopeSrc != scopeDst {
		e.flushScopeCache()
	}
	return Ino(srcIno), e.GetAttrOrNil(ctx, Ino(srcIno)), 0
}

func distinctParents(a, b Ino) []Ino {
	if a == b {
		return []Ino{a}
	}
	return []Ino{a, b}
}
