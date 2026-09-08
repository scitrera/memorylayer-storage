// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package acl

import (
	"reflect"
	"testing"
)

func TestMarshalRoundTrip(t *testing.T) {
	in := ACL{
		{Tag: TagUserObj, Perm: 7, ID: undefinedID},
		{Tag: TagUser, Perm: 7, ID: 1000},
		{Tag: TagGroupObj, Perm: 5, ID: undefinedID},
		{Tag: TagMask, Perm: 7, ID: undefinedID},
		{Tag: TagOther, Perm: 5, ID: undefinedID},
	}
	got, ok := Unmarshal(in.Marshal())
	if !ok {
		t.Fatal("unmarshal failed")
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	if _, ok := Unmarshal([]byte{1, 2, 3}); ok { // too short / bad length
		t.Fatal("expected rejection of malformed blob")
	}
	if _, ok := Unmarshal([]byte{9, 0, 0, 0}); ok { // wrong version
		t.Fatal("expected rejection of wrong version")
	}
}

// TestCreateNonTrivial: a default ACL with a named user + mask is non-trivial,
// so Create returns an access ACL and masks perms by the requested mode.
func TestCreateNonTrivial(t *testing.T) {
	def := ACL{
		{Tag: TagUserObj, Perm: 7, ID: undefinedID},
		{Tag: TagUser, Perm: 7, ID: 1000},
		{Tag: TagGroupObj, Perm: 5, ID: undefinedID},
		{Tag: TagMask, Perm: 7, ID: undefinedID},
		{Tag: TagOther, Perm: 5, ID: undefinedID},
	}
	access, mode := Create(def, 0o666)
	if access == nil {
		t.Fatal("expected a non-nil access ACL for a named-user default ACL")
	}
	// USER_OBJ perm masked by (mode>>6)=6 → owner mode bits become 6 (rw-).
	if mode&0o700 != 0o600 {
		t.Fatalf("owner mode bits = %o, want 600", mode&0o700)
	}
	// The named user entry is preserved (still present in the inherited ACL).
	var sawUser bool
	for _, e := range access {
		if e.Tag == TagUser && e.ID == 1000 {
			sawUser = true
		}
	}
	if !sawUser {
		t.Fatalf("named user entry lost in inherited ACL: %+v", access)
	}
}

// TestCreateTrivial: a 3-entry default ACL equivalent to plain mode bits yields
// no stored access ACL (mode alone carries it).
func TestCreateTrivial(t *testing.T) {
	def := ACL{
		{Tag: TagUserObj, Perm: 7, ID: undefinedID},
		{Tag: TagGroupObj, Perm: 5, ID: undefinedID},
		{Tag: TagOther, Perm: 5, ID: undefinedID},
	}
	access, mode := Create(def, 0o775)
	if access != nil {
		t.Fatalf("expected nil access ACL for a trivial default ACL, got %+v", access)
	}
	if mode&0o777 != 0o755 {
		t.Fatalf("mode = %o, want 755 (masked by default ACL)", mode&0o777)
	}
}
