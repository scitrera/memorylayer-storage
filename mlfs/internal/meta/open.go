// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"syscall"
)

// OpenRef registers one open handle on inode (no I/O). The FUSE bridge calls
// this on Open/Create so the inode is retained if it is unlinked while open.
func (e *Engine) OpenRef(inode Ino) {
	e.omu.Lock()
	e.openRefs[inode]++
	e.omu.Unlock()
}

// Open verifies the inode exists, registers an open handle, and returns attrs.
func (e *Engine) Open(ctx context.Context, inode Ino) (*Attr, syscall.Errno) {
	a, st := e.GetAttr(ctx, inode)
	if st != 0 {
		return nil, st
	}
	e.OpenRef(inode)
	return a, 0
}

// Close drops one open handle. If it was the last handle on an inode that is
// already unlinked (sustained), the inode and its data rows are finally
// deleted — POSIX open-but-unlinked semantics.
func (e *Engine) Close(ctx context.Context, inode Ino) syscall.Errno {
	e.omu.Lock()
	if e.openRefs[inode] > 0 {
		e.openRefs[inode]--
	}
	last := e.openRefs[inode] == 0
	if last {
		delete(e.openRefs, inode)
	}
	_, sustainedNow := e.sustained[inode]
	doDelete := last && sustainedNow
	if doDelete {
		delete(e.sustained, inode)
	}
	e.omu.Unlock()

	if !doDelete {
		return 0
	}
	if err := e.withTx(ctx, func(tx *sql.Tx) error {
		return deleteInodeRows(ctx, tx, inode)
	}); err != nil {
		return e.eio(ctx, "close", err)
	}
	return 0
}

// isOpen reports whether inode currently has open handles.
func (e *Engine) isOpen(inode Ino) bool {
	e.omu.Lock()
	defer e.omu.Unlock()
	return e.openRefs[inode] > 0
}

// markSustained records that inode is unlinked but still open, so the last
// Close deletes it.
func (e *Engine) markSustained(inode Ino) {
	e.omu.Lock()
	e.sustained[inode] = struct{}{}
	e.omu.Unlock()
}

// deleteInodeRows removes a node and ALL of its data rows (symlink target,
// every slice_ref, and any xattrs). Deleting the slice_ref rows is essential:
// GC computes the live chunk set from slice_ref, so leaving them would pin a
// deleted file's chunks forever.
func deleteInodeRows(ctx context.Context, tx *sql.Tx, inode Ino) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM node WHERE inode=$1`, int64(inode)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM symlink WHERE inode=$1`, int64(inode)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM slice_ref WHERE inode=$1`, int64(inode)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM xattr WHERE inode=$1`, int64(inode)); err != nil {
		return err
	}
	return nil
}
