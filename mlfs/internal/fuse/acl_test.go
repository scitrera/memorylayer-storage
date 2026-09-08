// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runACL runs setfacl/getfacl, skipping the test if the tools are missing.
func runACL(t *testing.T, name string, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not installed", name)
	}
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

// TestPOSIXACLRoundTrip verifies the kernel enforces POSIX ACLs over mlfs's
// xattr store (mount EnableAcl, L2.8.7): a named-user ACL set with setfacl is
// stored and reported back by getfacl.
func TestPOSIXACLRoundTrip(t *testing.T) {
	_, mnt := mountRig(t)
	f := filepath.Join(mnt, "f")
	if err := os.WriteFile(f, []byte("data"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if out, err := runACL(t, "setfacl", "-m", "u:12345:rwx", f); err != nil {
		t.Fatalf("setfacl: %v\n%s", err, out)
	}
	out, err := runACL(t, "getfacl", "-c", f) // -c: omit the comment header
	if err != nil {
		t.Fatalf("getfacl: %v\n%s", err, out)
	}
	if !strings.Contains(out, "user:12345:rwx") {
		t.Fatalf("ACL entry not reflected by getfacl:\n%s", out)
	}
	if !strings.Contains(out, "mask::") {
		t.Fatalf("expected an auto-generated mask entry:\n%s", out)
	}
}

// TestPOSIXACLDefaultInheritance exercises the kernel's CAP_POSIX_ACL path
// through mlfs end to end: a directory's default ACL is inherited by a newly
// created child, which only works if the kernel is reading/writing our ACL
// xattrs and applying inheritance on create.
func TestPOSIXACLDefaultInheritance(t *testing.T) {
	_, mnt := mountRig(t)
	dir := filepath.Join(mnt, "d")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if out, err := runACL(t, "setfacl", "-d", "-m", "u:12345:rwx", dir); err != nil {
		t.Fatalf("setfacl default: %v\n%s", err, out)
	}
	child := filepath.Join(dir, "child")
	if err := os.WriteFile(child, []byte("x"), 0o644); err != nil {
		t.Fatalf("create child: %v", err)
	}
	out, err := runACL(t, "getfacl", "-c", child)
	if err != nil {
		t.Fatalf("getfacl child: %v\n%s", err, out)
	}
	if !strings.Contains(out, "user:12345:") {
		t.Fatalf("child did not inherit the default ACL:\n%s", out)
	}
}
