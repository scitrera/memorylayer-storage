// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package acl implements just enough POSIX.1e ACL handling for default-ACL
// INHERITANCE on file creation. Enforcement of explicit ACLs is the kernel's
// job (the mount advertises CAP_POSIX_ACL and reads/writes the
// system.posix_acl_access / system.posix_acl_default xattrs through mlfs's
// generic xattr store). But under raw FUSE the kernel delegates default-ACL
// inheritance to the filesystem, so on create we must compute the child's
// access ACL + mode from the parent directory's default ACL. This is a
// POSIX ACL creation and inheritance rules
// (fs/posix_acl.c), operating on the on-disk xattr binary format.
package acl

import "encoding/binary"

// xattr binary format (uapi/linux/posix_acl_xattr.h):
//
//	header: u32 version
//	entry:  u16 tag, u16 perm, u32 id   (repeated)
const (
	xattrVersion = 2
	headerLen    = 4
	entryLen     = 8
)

// Entry tags (uapi/linux/posix_acl.h).
const (
	TagUserObj  uint16 = 0x01
	TagUser     uint16 = 0x02
	TagGroupObj uint16 = 0x04
	TagGroup    uint16 = 0x08
	TagMask     uint16 = 0x10
	TagOther    uint16 = 0x20
)

// undefinedID marks *_OBJ / MASK / OTHER entries (no specific uid/gid).
const undefinedID = 0xFFFFFFFF

// Entry is one ACL entry. Perm is the low 3 bits (rwx).
type Entry struct {
	Tag  uint16
	Perm uint16
	ID   uint32
}

// ACL is an ordered list of entries (as stored in the xattr).
type ACL []Entry

// Unmarshal parses the system.posix_acl_* binary format. A zero-length blob (or
// header-only) yields an empty ACL.
func Unmarshal(b []byte) (ACL, bool) {
	if len(b) < headerLen || (len(b)-headerLen)%entryLen != 0 {
		return nil, false
	}
	if binary.LittleEndian.Uint32(b) != xattrVersion {
		return nil, false
	}
	n := (len(b) - headerLen) / entryLen
	out := make(ACL, n)
	for i := 0; i < n; i++ {
		off := headerLen + i*entryLen
		out[i] = Entry{
			Tag:  binary.LittleEndian.Uint16(b[off:]),
			Perm: binary.LittleEndian.Uint16(b[off+2:]),
			ID:   binary.LittleEndian.Uint32(b[off+4:]),
		}
	}
	return out, true
}

// Marshal renders the ACL back to the xattr binary format.
func (a ACL) Marshal() []byte {
	b := make([]byte, headerLen+len(a)*entryLen)
	binary.LittleEndian.PutUint32(b, xattrVersion)
	for i, e := range a {
		off := headerLen + i*entryLen
		binary.LittleEndian.PutUint16(b[off:], e.Tag)
		binary.LittleEndian.PutUint16(b[off+2:], e.Perm)
		binary.LittleEndian.PutUint32(b[off+4:], e.ID)
	}
	return b
}

func (a ACL) clone() ACL {
	c := make(ACL, len(a))
	copy(c, a)
	return c
}

// Create computes a child's inherited state from a parent directory's default
// ACL and the requested mode:
//   - newMode is the child's effective mode after masking by the default ACL.
//   - access is the child's system.posix_acl_access ACL, or nil when the ACL is
//     equivalent to the plain mode bits (then mode alone suffices).
//
// The caller additionally copies the parent default ACL onto a child DIRECTORY's
// system.posix_acl_default (handled outside this function).
func Create(def ACL, mode uint16) (access ACL, newMode uint16) {
	acl := def.clone()
	notEquiv, m := createMasq(acl, mode)
	if !notEquiv {
		return nil, m // trivial ACL → mode bits carry it
	}
	return acl, m
}

// createMasq is posix_acl_create_masq: it masks the cloned default ACL by the
// requested mode in place and returns the adjusted mode plus whether the result
// is non-trivial (has named users/groups or a mask → needs a stored ACL).
func createMasq(acl ACL, mode uint16) (notEquiv bool, newMode uint16) {
	const (
		rwxU uint16 = 0o700
		rwxG uint16 = 0o070
		rwxO uint16 = 0o007
	)
	m := mode
	var groupObj, maskObj *Entry
	for i := range acl {
		switch acl[i].Tag {
		case TagUserObj:
			acl[i].Perm &= (m >> 6) | ^rwxO
			m &= (acl[i].Perm << 6) | ^rwxU
		case TagUser, TagGroup:
			notEquiv = true
		case TagGroupObj:
			groupObj = &acl[i]
		case TagOther:
			acl[i].Perm &= m | ^rwxO
			m &= acl[i].Perm | ^rwxO
		case TagMask:
			maskObj = &acl[i]
			notEquiv = true
		}
	}
	if maskObj != nil {
		maskObj.Perm &= (m >> 3) | ^rwxO
		m &= (maskObj.Perm << 3) | ^rwxG
	} else if groupObj != nil {
		groupObj.Perm &= (m >> 3) | ^rwxO
		m &= (groupObj.Perm << 3) | ^rwxG
	}
	newMode = (mode &^ 0o777) | m
	return notEquiv, newMode
}
