// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
)

// errAbort rolls back a transaction while a syscall.Errno is reported out-of-band.
var errAbort = errors.New("mlfs meta: tx abort")

// Rename flags (subset of renameat2). Values match the kernel's
// RENAME_* (uapi/linux/fs.h) so the FUSE bridge can forward in.Flags verbatim.
const (
	RenameNoReplace = 0x1 // RENAME_NOREPLACE: fail with EEXIST if dst exists
	RenameExchange  = 0x2 // RENAME_EXCHANGE: atomically swap src and dst
	RenameWhiteout  = 0x4 // RENAME_WHITEOUT: overlayfs; not supported (ENOSYS)
)

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (e *Engine) getAttr(ctx context.Context, q rowQuerier, inode Ino) (*Attr, error) {
	return scanAttr(q.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM node WHERE inode = $1`, int64(inode)).Scan)
}

// GetAttr returns a node's attributes.
func (e *Engine) GetAttr(ctx context.Context, inode Ino) (*Attr, syscall.Errno) {
	var opErr error // only a real failure; a missing inode (ENOENT) is not an error
	defer e.recordOp("get_attr", e.now(), &opErr)
	a, err := e.getAttr(ctx, e.db, inode)
	if err == sql.ErrNoRows {
		return nil, syscall.ENOENT
	}
	if err != nil {
		opErr = err
		return nil, e.eio(ctx, "getAttr", err)
	}
	return a, 0
}

// SetStoreClass records a file's compression class (see Attr.StoreClass). The
// bridge calls it once, right after creating a file, when its type classifies as
// non-default; the class is then inherited by every slice the file writes. It is
// a plain attribute UPDATE (no changelog / fence): the class is set by the
// creating client on a brand-new inode before any data is written, so there is no
// concurrent writer to fence against. A no-op (0 rows) on a missing inode returns
// ENOENT.
func (e *Engine) SetStoreClass(ctx context.Context, inode Ino, class uint8) syscall.Errno {
	res, err := e.db.ExecContext(ctx,
		`UPDATE node SET store_class=$2 WHERE inode=$1`, int64(inode), int(class))
	if err != nil {
		return e.eio(ctx, "setStoreClass", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return syscall.ENOENT
	}
	return 0
}

// SetContentHash records a file's whole-file content hash (see Attr.ContentHash):
// the lowercase-hex sha256 of the file's bytes, matching blobgw's ContentHash
// encoding. The bridge calls it on flush/close once the running or recomputed
// digest is finalized. It is a plain attribute UPDATE (no changelog / fence): the
// value is derived from the file's already-committed data, so a stale-generation
// writer would have failed its own data writes' fence first; this only stamps the
// resulting digest. A no-op (0 rows) on a missing inode returns ENOENT.
func (e *Engine) SetContentHash(ctx context.Context, inode Ino, hash string) syscall.Errno {
	res, err := e.db.ExecContext(ctx,
		`UPDATE node SET content_hash=$2 WHERE inode=$1`, int64(inode), hash)
	if err != nil {
		return e.eio(ctx, "setContentHash", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return syscall.ENOENT
	}
	return 0
}

// ContentHash returns a file's persisted whole-file content hash (empty until the
// file is first flushed). The mlfs↔VFS bridge reads it to register a blob_ref
// without re-hashing. A missing inode returns ENOENT.
func (e *Engine) ContentHash(ctx context.Context, inode Ino) (string, syscall.Errno) {
	a, st := e.GetAttr(ctx, inode)
	if st != 0 {
		return "", st
	}
	return a.ContentHash, 0
}

// Lookup resolves a name in a directory to its inode + attributes.
func (e *Engine) Lookup(ctx context.Context, parent Ino, name string) (Ino, *Attr, syscall.Errno) {
	var opErr error // a missing name (ENOENT) is normal control flow, not an error
	defer e.recordOp("lookup", e.now(), &opErr)
	var inode int64
	err := e.db.QueryRowContext(ctx,
		`SELECT inode FROM edge WHERE parent = $1 AND name = $2`, int64(parent), []byte(name)).Scan(&inode)
	if err == sql.ErrNoRows {
		return 0, nil, syscall.ENOENT
	}
	if err != nil {
		opErr = err
		return 0, nil, e.eio(ctx, "lookup", err)
	}
	a, gerr := e.getAttr(ctx, e.db, Ino(inode))
	if gerr != nil {
		opErr = gerr
		return 0, nil, e.eio(ctx, "lookup", gerr)
	}
	return Ino(inode), a, 0
}

// create is the shared node-creation path for Mknod/Mkdir/Create/Symlink.
func (e *Engine) create(ctx context.Context, parent Ino, name string, typ uint8,
	mode uint16, rdev uint32, target string, op uint8, c *Context) (Ino, *Attr, syscall.Errno) {

	inode, err := e.nextInode(ctx)
	if err != nil {
		return 0, nil, e.eio(ctx, "create", err)
	}
	scope, gen, st := e.acquireScope(ctx, parent)
	if st != 0 {
		return 0, nil, st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	var attr *Attr

	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scope, gen, &out); ferr != nil {
			return ferr
		}
		pa, perr := e.getAttr(ctx, tx, parent)
		if perr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if perr != nil {
			return perr
		}
		if pa.Typ != TypeDirectory {
			out = syscall.ENOTDIR
			return errAbort
		}
		res, eerr := tx.ExecContext(ctx,
			`INSERT INTO edge (parent, name, inode, type) VALUES ($1, $2, $3, $4)
			 ON CONFLICT (parent, name) DO NOTHING`,
			int64(parent), []byte(name), int64(inode), int(typ))
		if eerr != nil {
			return eerr
		}
		if n, _ := res.RowsAffected(); n == 0 {
			out = syscall.EEXIST
			return errAbort
		}
		nlink := uint32(1)
		var length uint64
		if typ == TypeDirectory {
			nlink = 2
		}
		if typ == TypeSymlink {
			length = uint64(len(target))
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO node (inode, type, mode, uid, gid, atime, mtime, ctime,
			    atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
			 VALUES ($1,$2,$3,$4,$5,$6,$6,$6,$7,$7,$7,$8,$9,$10,$11)`,
			int64(inode), int(typ), int(mode), int64(c.Uid), int64(c.Gid),
			ts, int(nsec), int64(nlink), int64(length), int64(rdev), int64(parent)); err != nil {
			return err
		}
		if typ == TypeSymlink {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO symlink (inode, target) VALUES ($1, $2)`, int64(inode), []byte(target)); err != nil {
				return err
			}
		}
		// Bump parent: mtime/ctime always; nlink+1 when adding a subdirectory.
		pq := `UPDATE node SET mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`
		if typ == TypeDirectory {
			pq = `UPDATE node SET nlink=nlink+1, mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`
		}
		if _, err := tx.ExecContext(ctx, pq, int64(parent), ts, int(nsec)); err != nil {
			return err
		}
		if err := e.appendChangelog(ctx, tx, inode, op,
			map[string]any{"parent": parent, "name": name, "type": typ}); err != nil {
			return err
		}
		attr = &Attr{
			Typ: typ, Mode: mode, Uid: c.Uid, Gid: c.Gid,
			Atime: ts, Mtime: ts, Ctime: ts, Atimensec: nsec, Mtimensec: nsec, Ctimensec: nsec,
			Nlink: nlink, Length: length, Rdev: rdev, Parent: parent, Full: true,
		}
		return nil
	})
	if out != 0 {
		return 0, nil, out
	}
	if txErr != nil {
		return 0, nil, e.eio(ctx, "create", txErr)
	}
	return inode, attr, 0
}

// Mkdir creates a subdirectory.
func (e *Engine) Mkdir(ctx context.Context, parent Ino, name string, mode uint16, c *Context) (Ino, *Attr, syscall.Errno) {
	return e.create(ctx, parent, name, TypeDirectory, mode, 0, "", opMkdir, c)
}

// Mknod creates a non-directory node (file, fifo, device, socket).
func (e *Engine) Mknod(ctx context.Context, parent Ino, name string, typ uint8, mode uint16, rdev uint32, c *Context) (Ino, *Attr, syscall.Errno) {
	return e.create(ctx, parent, name, typ, mode, rdev, "", opMknod, c)
}

// Create creates a regular file and returns its inode (ready to open).
func (e *Engine) Create(ctx context.Context, parent Ino, name string, mode uint16, c *Context) (Ino, *Attr, syscall.Errno) {
	return e.create(ctx, parent, name, TypeFile, mode, 0, "", opMknod, c)
}

// Symlink creates a symbolic link with the given target.
func (e *Engine) Symlink(ctx context.Context, parent Ino, name, target string, c *Context) (Ino, *Attr, syscall.Errno) {
	return e.create(ctx, parent, name, TypeSymlink, 0o777, 0, target, opSymlink, c)
}

// ReadLink returns a symlink's target.
func (e *Engine) ReadLink(ctx context.Context, inode Ino) ([]byte, syscall.Errno) {
	var target []byte
	err := e.db.QueryRowContext(ctx, `SELECT target FROM symlink WHERE inode = $1`, int64(inode)).Scan(&target)
	if err == sql.ErrNoRows {
		return nil, syscall.ENOENT
	}
	if err != nil {
		return nil, e.eio(ctx, "readlink", err)
	}
	return target, 0
}

// Unlink removes a file entry; the node (and its symlink row) is deleted when
// its link count reaches zero AND it is not open; an open file is kept
// (sustained) and deleted on the last Close (POSIX open-but-unlinked).
func (e *Engine) Unlink(ctx context.Context, parent Ino, name string, c *Context) syscall.Errno {
	scope, gen, st := e.acquireScope(ctx, parent)
	if st != 0 {
		return st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	var sustain Ino // set (non-zero) when the inode must be retained until close
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if ferr := e.fenceInTx(ctx, tx, scope, gen, &out); ferr != nil {
			return ferr
		}
		var inode int64
		var typ int
		err := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`, int64(parent), []byte(name)).Scan(&inode, &typ)
		if err == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if err != nil {
			return err
		}
		if uint8(typ) == TypeDirectory {
			out = syscall.EISDIR
			return errAbort
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM edge WHERE parent=$1 AND name=$2`, int64(parent), []byte(name)); err != nil {
			return err
		}
		var nlink int64
		if err := tx.QueryRowContext(ctx,
			`UPDATE node SET nlink=nlink-1, ctime=$2, ctimensec=$3 WHERE inode=$1 RETURNING nlink`,
			inode, ts, int(nsec)).Scan(&nlink); err != nil {
			return err
		}
		if nlink <= 0 {
			if e.isOpen(Ino(inode)) {
				// Still open: detach now (edge gone, nlink 0), delete on last Close.
				sustain = Ino(inode)
			} else if err := deleteInodeRows(ctx, tx, Ino(inode)); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`,
			int64(parent), ts, int(nsec)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, Ino(inode), opUnlink, map[string]any{"parent": parent, "name": name})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "unlink", txErr)
	}
	if sustain != 0 {
		// Commit succeeded and the inode is open: retain until last Close.
		e.markSustained(sustain)
	}
	return 0
}

// Rmdir removes an empty subdirectory.
func (e *Engine) Rmdir(ctx context.Context, parent Ino, name string, c *Context) syscall.Errno {
	scope, gen, st := e.acquireScope(ctx, parent)
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
		var inode int64
		var typ int
		err := tx.QueryRowContext(ctx,
			`SELECT inode, type FROM edge WHERE parent=$1 AND name=$2`, int64(parent), []byte(name)).Scan(&inode, &typ)
		if err == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if err != nil {
			return err
		}
		if uint8(typ) != TypeDirectory {
			out = syscall.ENOTDIR
			return errAbort
		}
		var cnt int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM edge WHERE parent=$1`, inode).Scan(&cnt); err != nil {
			return err
		}
		if cnt > 0 {
			out = syscall.ENOTEMPTY
			return errAbort
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM edge WHERE parent=$1 AND name=$2`, int64(parent), []byte(name)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM node WHERE inode=$1`, inode); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM xattr WHERE inode=$1`, inode); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET nlink=nlink-1, mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`,
			int64(parent), ts, int(nsec)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, Ino(inode), opRmdir, map[string]any{"parent": parent, "name": name})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "rmdir", txErr)
	}
	return 0
}

// Link creates a hard link newName under newParent pointing at an existing
// non-directory inode, bumping its link count. Directory hardlinks are refused
// (EPERM), matching POSIX. Returns the linked inode's updated attributes.
func (e *Engine) Link(ctx context.Context, inode, newParent Ino, newName string, c *Context) (*Attr, syscall.Errno) {
	scope, gen, st := e.acquireScope(ctx, newParent)
	if st != 0 {
		return nil, st
	}
	now := e.now()
	ts, nsec := now.Unix(), uint32(now.Nanosecond())
	var out syscall.Errno
	var attr *Attr
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
			out = syscall.EPERM // no hard links to directories
			return errAbort
		}
		pa, perr := e.getAttr(ctx, tx, newParent)
		if perr == sql.ErrNoRows {
			out = syscall.ENOENT
			return errAbort
		}
		if perr != nil {
			return perr
		}
		if pa.Typ != TypeDirectory {
			out = syscall.ENOTDIR
			return errAbort
		}
		res, eerr := tx.ExecContext(ctx,
			`INSERT INTO edge (parent, name, inode, type) VALUES ($1, $2, $3, $4)
			 ON CONFLICT (parent, name) DO NOTHING`,
			int64(newParent), []byte(newName), int64(inode), int(cur.Typ))
		if eerr != nil {
			return eerr
		}
		if n, _ := res.RowsAffected(); n == 0 {
			out = syscall.EEXIST
			return errAbort
		}
		var nlink int64
		if err := tx.QueryRowContext(ctx,
			`UPDATE node SET nlink=nlink+1, ctime=$2, ctimensec=$3 WHERE inode=$1 RETURNING nlink`,
			int64(inode), ts, int(nsec)).Scan(&nlink); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET mtime=$2, ctime=$2, mtimensec=$3, ctimensec=$3 WHERE inode=$1`,
			int64(newParent), ts, int(nsec)); err != nil {
			return err
		}
		cur.Nlink = uint32(nlink)
		cur.Ctime, cur.Ctimensec = ts, nsec
		attr = cur
		return e.appendChangelog(ctx, tx, inode, opLink, map[string]any{"parent": newParent, "name": newName})
	})
	if out != 0 {
		return nil, out
	}
	if txErr != nil {
		return nil, e.eio(ctx, "link", txErr)
	}
	return attr, 0
}

// Readdir returns a directory's entries (including "." and ".."); attributes
// are filled when plus is true.
func (e *Engine) Readdir(ctx context.Context, inode Ino, plus bool) ([]*Entry, syscall.Errno) {
	self, gerr := e.getAttr(ctx, e.db, inode)
	if gerr == sql.ErrNoRows {
		return nil, syscall.ENOENT
	}
	if gerr != nil {
		return nil, e.eio(ctx, "readdir", gerr)
	}
	if self.Typ != TypeDirectory {
		return nil, syscall.ENOTDIR
	}
	entries := []*Entry{{Inode: inode, Name: "."}, {Inode: self.Parent, Name: ".."}}
	rows, err := e.db.QueryContext(ctx,
		`SELECT name, inode FROM edge WHERE parent=$1 ORDER BY name`, int64(inode))
	if err != nil {
		return nil, e.eio(ctx, "readdir", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name []byte
		var ino int64
		if err := rows.Scan(&name, &ino); err != nil {
			return nil, e.eio(ctx, "readdir", err)
		}
		entries = append(entries, &Entry{Inode: Ino(ino), Name: string(name)})
	}
	if err := rows.Err(); err != nil {
		return nil, e.eio(ctx, "readdir", err)
	}
	if plus {
		for _, en := range entries {
			if a, aerr := e.getAttr(ctx, e.db, en.Inode); aerr == nil {
				en.Attr = a
			}
		}
	}
	return entries, 0
}

// SetAttr updates the selected attributes (set mask) and bumps ctime. The
// caller c authorizes ownership-changing fields: only the owner or root may
// chmod/chgrp, and only root may chown to a different uid (EPERM otherwise).
func (e *Engine) SetAttr(ctx context.Context, inode Ino, set uint16, attr *Attr, c *Context) (*Attr, syscall.Errno) {
	scope, gen, est := e.acquireScope(ctx, inode)
	if est != 0 {
		return nil, est
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
		if st := authorizeSetAttr(c, cur, set, attr); st != 0 {
			out = st
			return errAbort
		}
		// Apply selected fields onto a copy, then write the row back.
		if set&SetMode != 0 {
			cur.Mode = attr.Mode
		}
		if set&SetUID != 0 {
			cur.Uid = attr.Uid
		}
		if set&SetGID != 0 {
			cur.Gid = attr.Gid
		}
		if set&SetSize != 0 {
			cur.Length = attr.Length // L2.1: metadata length only; slice truncation is L2.x
		}
		if set&SetAtime != 0 {
			cur.Atime, cur.Atimensec = attr.Atime, attr.Atimensec
		}
		if set&SetMtime != 0 {
			cur.Mtime, cur.Mtimensec = attr.Mtime, attr.Mtimensec
		}
		cur.Ctime, cur.Ctimensec = ts, nsec
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET mode=$2, uid=$3, gid=$4, length=$5, atime=$6, atimensec=$7,
			    mtime=$8, mtimensec=$9, ctime=$10, ctimensec=$11 WHERE inode=$1`,
			int64(inode), int(cur.Mode), int64(cur.Uid), int64(cur.Gid), int64(cur.Length),
			cur.Atime, int(cur.Atimensec), cur.Mtime, int(cur.Mtimensec), cur.Ctime, int(cur.Ctimensec)); err != nil {
			return err
		}
		out = 0
		return e.appendChangelog(ctx, tx, inode, opSetattr, map[string]any{"set": set})
	})
	if out != 0 {
		return nil, out
	}
	if txErr != nil {
		return nil, e.eio(ctx, "setattr", txErr)
	}
	return e.GetAttrOrNil(ctx, inode), 0
}

// GetAttrOrNil returns the attr or nil (helper for return paths).
func (e *Engine) GetAttrOrNil(ctx context.Context, inode Ino) *Attr {
	a, _ := e.GetAttr(ctx, inode)
	return a
}

// Write records a slice over chunk indx at byte offset off and extends the
// file length if needed.
func (e *Engine) Write(ctx context.Context, inode Ino, indx uint32, off uint32, slice Slice) syscall.Errno {
	var opErr error // a real DB failure; a fence/ENOENT/EINVAL is normal control flow
	defer e.recordOp("write_slice", e.now(), &opErr)
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
		if cur.Typ != TypeFile {
			out = syscall.EINVAL
			return errAbort
		}
		// Idempotent on slice_id: a replayed (or duplicate) write whose slice
		// already landed is a no-op — skip the length bump + changelog so WAL
		// replay (L2.4) is safe.
		res, ierr := tx.ExecContext(ctx,
			`INSERT INTO slice_ref (inode, indx, pos, slice_id, size, soff, slen) VALUES ($1,$2,$3,$4,$5,$6,$7)
			 ON CONFLICT (inode, slice_id) DO NOTHING`,
			int64(inode), int(indx), int(off), int64(slice.Id), int(slice.Size), int(slice.Off), int(slice.Len))
		if ierr != nil {
			return ierr
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil // slice already present — idempotent no-op
		}
		newLen := uint64(indx)*ChunkSize + uint64(off) + uint64(slice.Len)
		length := cur.Length
		if newLen > length {
			length = newLen
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE node SET length=$2, mtime=$3, ctime=$3, mtimensec=$4, ctimensec=$4 WHERE inode=$1`,
			int64(inode), int64(length), ts, int(nsec)); err != nil {
			return err
		}
		return e.appendChangelog(ctx, tx, inode, opWrite, map[string]any{"indx": indx, "off": off, "len": slice.Len})
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		opErr = txErr
		return e.eio(ctx, "write", txErr)
	}
	return 0
}

// Read returns the slices written to chunk indx, in write order (overlay
// resolution into the visible slice list is a follow-up port of slice.go).
func (e *Engine) Read(ctx context.Context, inode Ino, indx uint32) ([]Slice, syscall.Errno) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT slice_id, size, soff, slen FROM slice_ref WHERE inode=$1 AND indx=$2 ORDER BY id`,
		int64(inode), int(indx))
	if err != nil {
		return nil, e.eio(ctx, "read", err)
	}
	defer rows.Close()
	var slices []Slice
	for rows.Next() {
		var id int64
		var size, soff, slen int
		if err := rows.Scan(&id, &size, &soff, &slen); err != nil {
			return nil, e.eio(ctx, "read", err)
		}
		slices = append(slices, Slice{Id: uint64(id), Size: uint32(size), Off: uint32(soff), Len: uint32(slen)})
	}
	if err := rows.Err(); err != nil {
		return nil, e.eio(ctx, "read", err)
	}
	return slices, 0
}
