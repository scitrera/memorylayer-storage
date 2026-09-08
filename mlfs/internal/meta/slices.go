// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"syscall"
)

// SliceRef is a raw slice_ref row: a slice at chunk-relative byte position Pos,
// sourced from chunk-store slice ID at [Off, Off+Len). Pos is what the data
// path needs to overlay slices (newer wins); meta.Slice (Id/Size/Off/Len)
// omits it.
type SliceRef struct {
	Pos uint32
	Slice
}

// ReadSlices returns the slice_ref rows for a chunk in WRITE order (ascending
// id). The data path resolves overlap; this returns the raw history.
func (e *Engine) ReadSlices(ctx context.Context, inode Ino, indx uint32) ([]SliceRef, syscall.Errno) {
	var opErr error // the underlying DB error, for the meta RED recorder
	defer e.recordOp("read_slices", e.now(), &opErr)
	rows, err := e.db.QueryContext(ctx,
		`SELECT pos, slice_id, size, soff, slen FROM slice_ref WHERE inode=$1 AND indx=$2 ORDER BY id`,
		int64(inode), int(indx))
	if err != nil {
		opErr = err
		return nil, e.eio(ctx, "readSlices", err)
	}
	defer rows.Close()
	var out []SliceRef
	for rows.Next() {
		var pos, size, soff, slen int
		var id int64
		if err := rows.Scan(&pos, &id, &size, &soff, &slen); err != nil {
			opErr = err
			return nil, e.eio(ctx, "readSlices", err)
		}
		out = append(out, SliceRef{
			Pos:   uint32(pos),
			Slice: Slice{Id: uint64(id), Size: uint32(size), Off: uint32(soff), Len: uint32(slen)},
		})
	}
	if err := rows.Err(); err != nil {
		opErr = err
		return nil, e.eio(ctx, "readSlices", err)
	}
	return out, 0
}

// Truncate sets the file length to size. On shrink it destroys data beyond
// size — slices entirely past it are deleted, a slice straddling it is trimmed
// — so a later grow reads zeros (POSIX semantics), not the old bytes. Grow is
// metadata-only (the tail is sparse).
func (e *Engine) Truncate(ctx context.Context, inode Ino, size int64) syscall.Errno {
	scope, gen, st := e.acquireScope(ctx, inode)
	if st != 0 {
		return st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scope, gen, &out); ferr != nil {
			return ferr
		}
		cur, gerr := e.getAttr(ctx, tx, inode)
		if gerr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if gerr != nil {
			return gerr
		}
		if cur.Typ == TypeDirectory {
			out = syscall.EISDIR
			return errAbort
		}
		if size < int64(cur.Length) {
			const cs = int64(ChunkSize)
			// Drop slices that start at or beyond the new size.
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM slice_ref WHERE inode=$1 AND (indx::bigint*$2 + pos) >= $3`,
				int64(inode), cs, size); err != nil {
				return err
			}
			// Trim a slice that straddles the new size down to the kept bytes.
			if _, err := tx.ExecContext(ctx,
				`UPDATE slice_ref SET slen = ($3 - (indx::bigint*$2 + pos))::int
				 WHERE inode=$1 AND (indx::bigint*$2 + pos) < $3 AND (indx::bigint*$2 + pos + slen) > $3`,
				int64(inode), cs, size); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET length=$2, mtime=$3, ctime=$3, mtimensec=$4, ctimensec=$4 WHERE inode=$1`,
			int64(inode), size, ts, int(nsec)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, inode, opSetattr, map[string]any{"truncate": size})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "truncate", txErr)
	}
	return 0
}

// CloneRange copies-on-write a chunk-aligned, whole-chunk run from srcIno into
// dstIno: it replaces dstIno's slice rows for chunk indices
// [dstIdx, dstIdx+nChunks) with copies of srcIno's rows for chunk indices
// [srcIdx, srcIdx+nChunks), referencing the SAME physical slice ids (zero data
// movement, shared chunks — same GC liveness rule as Clone). It does NOT touch
// dstIno's length; the caller (the data path) extends it to the copy end. Used
// by copy_file_range for the fast path where both offsets and the length sit on
// chunk boundaries. nChunks == 0 is a no-op.
func (e *Engine) CloneRange(ctx context.Context, srcIno, dstIno Ino, srcIdx, dstIdx, nChunks uint32) syscall.Errno {
	if nChunks == 0 {
		return 0
	}
	scope, gen, st := e.acquireScope(ctx, dstIno)
	if st != 0 {
		return st
	}
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scope, gen, &out); ferr != nil {
			return ferr
		}
		if _, serr := e.getAttr(ctx, tx, srcIno); serr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if serr != nil {
			return serr
		}
		dst, derr := e.getAttr(ctx, tx, dstIno)
		if derr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if derr != nil {
			return derr
		}
		if dst.Typ != TypeFile {
			out = syscall.EINVAL
			return errAbort
		}
		// Clear the destination chunk range, then copy the source rows over,
		// remapping each source chunk index to its destination index.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM slice_ref WHERE inode=$1 AND indx >= $2 AND indx < $3`,
			int64(dstIno), int(dstIdx), int(dstIdx+nChunks)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO slice_ref (inode, indx, pos, slice_id, size, soff, slen)
			 SELECT $1, indx + $2, pos, slice_id, size, soff, slen FROM slice_ref
			 WHERE inode=$3 AND indx >= $4 AND indx < $5
			 ON CONFLICT (inode, slice_id) DO NOTHING`,
			int64(dstIno), int(dstIdx)-int(srcIdx), int64(srcIno), int(srcIdx), int(srcIdx+nChunks)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, dstIno, opWrite,
			map[string]any{"clone_range_from": srcIno, "src_indx": srcIdx, "dst_indx": dstIdx, "n_chunks": nChunks})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "cloneRange", txErr)
	}
	return 0
}

// Clone implements copy-on-write: it copies every slice_ref row of srcIno onto
// dstIno (referencing the SAME physical slice ids — zero data movement) and
// sets dstIno's length, all in one transaction. The shared chunks stay live
// because dst's new slice rows reference them; mark-and-sweep GC only collects
// a chunk once NO slice row (in any file) references it. dstIno should be a
// freshly created, empty regular file.
func (e *Engine) Clone(ctx context.Context, srcIno, dstIno Ino) syscall.Errno {
	scope, gen, st := e.acquireScope(ctx, dstIno)
	if st != 0 {
		return st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scope, gen, &out); ferr != nil {
			return ferr
		}
		src, serr := e.getAttr(ctx, tx, srcIno)
		if serr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if serr != nil {
			return serr
		}
		if _, derr := e.getAttr(ctx, tx, dstIno); derr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		} else if derr != nil {
			return derr
		}
		if src.Typ != TypeFile {
			out = syscall.EINVAL
			return errAbort
		}
		// Copy slice rows verbatim (same slice_id) under the destination inode.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO slice_ref (inode, indx, pos, slice_id, size, soff, slen)
			 SELECT $1, indx, pos, slice_id, size, soff, slen FROM slice_ref WHERE inode=$2
			 ON CONFLICT (inode, slice_id) DO NOTHING`,
			int64(dstIno), int64(srcIno)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET length=$2, mtime=$3, ctime=$3, mtimensec=$4, ctimensec=$4 WHERE inode=$1`,
			int64(dstIno), int64(src.Length), ts, int(nsec)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, dstIno, opWrite,
			map[string]any{"clone_from": srcIno, "length": src.Length})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "clone", txErr)
	}
	return 0
}
