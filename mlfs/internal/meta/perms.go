// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"syscall"
)

// Access permission bits (match the low POSIX mode bits).
const (
	MayExec  uint8 = 1
	MayWrite uint8 = 2
	MayRead  uint8 = 4
)

// accessAttr checks caller c against an inode's attributes for the requested
// r/w/x mask. Root (uid 0) and a nil context bypass. Group matching considers
// only the caller's primary gid (supplementary groups aren't modeled here —
// the kernel's default_permissions does the full check for the FUSE path; this
// is coarse defense for non-FUSE callers).
func accessAttr(c *Context, a *Attr, mask uint8) syscall.Errno {
	if c == nil || c.Uid == 0 {
		return 0
	}
	var perm uint16
	switch {
	case c.Uid == a.Uid:
		perm = (a.Mode >> 6) & 7
	case c.Gid == a.Gid:
		perm = (a.Mode >> 3) & 7
	default:
		perm = a.Mode & 7
	}
	if uint16(mask)&perm == uint16(mask) {
		return 0
	}
	return syscall.EACCES
}

// Access reports whether caller c may access inode with the given r/w/x mask.
func (e *Engine) Access(ctx context.Context, inode Ino, mask uint8, c *Context) syscall.Errno {
	a, st := e.GetAttr(ctx, inode)
	if st != 0 {
		return st
	}
	return accessAttr(c, a, mask)
}

// setidBits are S_ISUID|S_ISGID — the set-id bits the kernel strips on a write by
// a non-owner (file_remove_privs). A mode change that ONLY clears these is a
// kernel-forced privilege drop, not a user chmod, and must be allowed regardless
// of ownership: it can only reduce privilege (never escalate), and under
// default_permissions the kernel has already rejected any non-owner USER chmod
// before it reaches the engine — so the only non-owner SetMode we ever see IS the
// forced kill-priv. Rejecting it (the old behavior) broke writes to set-id files
// by a non-owner (pjdfstest chmod/12).
const setidBits = 0o6000

// authorizeSetAttr enforces the ownership rules POSIX places on attribute
// changes (independent of file access perms): only the owner or root may
// chmod/chgrp; only root may chown to a different uid. cur is the inode's
// current attributes; set/attr are the requested change. Returns EPERM if
// disallowed.
func authorizeSetAttr(c *Context, cur *Attr, set uint16, attr *Attr) syscall.Errno {
	if c == nil || c.Uid == 0 {
		return 0 // root
	}
	owner := c.Uid == cur.Uid
	if set&SetMode != 0 && !owner {
		// Allow only a pure set-id clear (kernel kill-priv): no bits added, and the
		// only bits removed are set-id bits.
		privDropOnly := attr.Mode&^cur.Mode == 0 && (cur.Mode&^attr.Mode)&^uint16(setidBits) == 0
		if !privDropOnly {
			return syscall.EPERM
		}
	}
	if set&SetUID != 0 && attr.Uid != cur.Uid {
		return syscall.EPERM // chown to a different uid requires root
	}
	if set&SetGID != 0 && !owner {
		return syscall.EPERM
	}
	return 0
}
