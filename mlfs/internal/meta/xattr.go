// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"syscall"
)

// SetXAttr flags (match linux <sys/xattr.h>).
const (
	XattrCreate  uint32 = 0x1 // pure create: fail with EEXIST if the name exists
	XattrReplace uint32 = 0x2 // pure replace: fail with ENODATA if it does not
)

// GetXAttr returns the value of extended attribute name on inode, or ENODATA if
// it is not set. (We do not distinguish a missing inode from a missing name on
// the read path; the kernel's preceding lookup already validated the inode.)
func (e *Engine) GetXAttr(ctx context.Context, inode Ino, name string) ([]byte, syscall.Errno) {
	var v []byte
	err := e.db.QueryRowContext(ctx,
		`SELECT value FROM xattr WHERE inode=$1 AND name=$2`, int64(inode), []byte(name)).Scan(&v)
	if err == sql.ErrNoRows {
		return nil, syscall.ENODATA
	}
	if err != nil {
		return nil, e.eio(ctx, "getxattr", err)
	}
	return v, 0
}

// ListXAttr returns the names of all extended attributes on inode (sorted).
func (e *Engine) ListXAttr(ctx context.Context, inode Ino) ([]string, syscall.Errno) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT name FROM xattr WHERE inode=$1 ORDER BY name`, int64(inode))
	if err != nil {
		return nil, e.eio(ctx, "listxattr", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n []byte
		if err := rows.Scan(&n); err != nil {
			return nil, e.eio(ctx, "listxattr", err)
		}
		names = append(names, string(n))
	}
	if err := rows.Err(); err != nil {
		return nil, e.eio(ctx, "listxattr", err)
	}
	return names, 0
}

// SetXAttr sets extended attribute name=value on inode, honoring the
// create/replace flags. ENOENT if the inode is gone, EEXIST/ENODATA on a flag
// violation.
func (e *Engine) SetXAttr(ctx context.Context, inode Ino, name string, value []byte, flags uint32) syscall.Errno {
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		var one int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM node WHERE inode=$1`, int64(inode)).Scan(&one); err != nil {
			if err == sql.ErrNoRows {
				out = syscall.ENOENT
				return errAbort
			}
			return err
		}
		var exists bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM xattr WHERE inode=$1 AND name=$2)`,
			int64(inode), []byte(name)).Scan(&exists); err != nil {
			return err
		}
		if flags&XattrCreate != 0 && exists {
			out = syscall.EEXIST
			return errAbort
		}
		if flags&XattrReplace != 0 && !exists {
			out = syscall.ENODATA
			return errAbort
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO xattr (inode, name, value) VALUES ($1,$2,$3)
			 ON CONFLICT (inode, name) DO UPDATE SET value = EXCLUDED.value`,
			int64(inode), []byte(name), value)
		return err
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "setxattr", txErr)
	}
	return 0
}

// RemoveXAttr deletes extended attribute name from inode, or ENODATA if unset.
func (e *Engine) RemoveXAttr(ctx context.Context, inode Ino, name string) syscall.Errno {
	res, err := e.db.ExecContext(ctx,
		`DELETE FROM xattr WHERE inode=$1 AND name=$2`, int64(inode), []byte(name))
	if err != nil {
		return e.eio(ctx, "removexattr", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return syscall.ENODATA
	}
	return 0
}
