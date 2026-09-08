// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"syscall"
	"testing"
)

// TestAuthorizeSetAttr_KillPriv covers the chmod ownership rules, in particular
// the kernel's set-id-clear-on-write (file_remove_privs): a NON-owner mode change
// that only clears S_ISUID/S_ISGID must be allowed (it is a forced privilege
// drop, not a user chmod), while any other non-owner mode change stays EPERM.
func TestAuthorizeSetAttr_KillPriv(t *testing.T) {
	const ownerUID = 1000
	cur := func(mode uint16) *Attr { return &Attr{Mode: mode, Uid: ownerUID} }
	nonOwner := &Context{Uid: 65534}
	owner := &Context{Uid: ownerUID}
	root := &Context{Uid: 0}

	cases := []struct {
		name string
		c    *Context
		cur  *Attr
		set  uint16
		attr *Attr
		want syscall.Errno
	}{
		{"non-owner clears suid (kill-priv)", nonOwner, cur(0o4777), SetMode, &Attr{Mode: 0o0777}, 0},
		{"non-owner clears sgid (kill-priv)", nonOwner, cur(0o2777), SetMode, &Attr{Mode: 0o0777}, 0},
		{"non-owner clears suid+sgid (kill-priv)", nonOwner, cur(0o6777), SetMode, &Attr{Mode: 0o0777}, 0},
		{"non-owner real chmod (perm bits) → EPERM", nonOwner, cur(0o0777), SetMode, &Attr{Mode: 0o0642}, syscall.EPERM},
		{"non-owner ADDS suid → EPERM", nonOwner, cur(0o0777), SetMode, &Attr{Mode: 0o4777}, syscall.EPERM},
		{"non-owner clears suid but also flips a perm bit → EPERM", nonOwner, cur(0o4777), SetMode, &Attr{Mode: 0o0775}, syscall.EPERM},
		{"owner chmod allowed", owner, cur(0o0777), SetMode, &Attr{Mode: 0o4755}, 0},
		{"root chmod allowed", root, cur(0o0777), SetMode, &Attr{Mode: 0o4755}, 0},
		{"non-owner chown gid → EPERM", nonOwner, cur(0o0777), SetGID, &Attr{Gid: 7}, syscall.EPERM},
	}
	for _, tc := range cases {
		if got := authorizeSetAttr(tc.c, tc.cur, tc.set, tc.attr); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
