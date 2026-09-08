// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fsstat

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
	}
	for _, c := range cases {
		if got := HumanBytes(c.n); got != c.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestDirUsage(t *testing.T) {
	root := t.TempDir()
	// Two files at the top, one nested — DirUsage must recurse and skip dirs.
	mustWrite(t, filepath.Join(root, "a"), 100)
	mustWrite(t, filepath.Join(root, "b"), 200)
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(root, "sub", "c"), 300)

	files, bytes := DirUsage(root)
	if files != 3 || bytes != 600 {
		t.Errorf("DirUsage = (%d files, %d bytes), want (3, 600)", files, bytes)
	}

	// A missing directory yields (0,0), never an error/panic.
	if f, b := DirUsage(filepath.Join(root, "does-not-exist")); f != 0 || b != 0 {
		t.Errorf("DirUsage(missing) = (%d, %d), want (0, 0)", f, b)
	}
	if f, b := DirUsage(""); f != 0 || b != 0 {
		t.Errorf("DirUsage(\"\") = (%d, %d), want (0, 0)", f, b)
	}
}

func TestStatfsBytes(t *testing.T) {
	avail, total := StatfsBytes(t.TempDir())
	if total <= 0 || avail < 0 || avail > total {
		t.Errorf("StatfsBytes returned implausible (avail=%d total=%d)", avail, total)
	}
	if a, to := StatfsBytes(""); a != 0 || to != 0 {
		t.Errorf("StatfsBytes(\"\") = (%d, %d), want (0, 0)", a, to)
	}
}

func TestOldestModTime(t *testing.T) {
	root := t.TempDir()

	// Empty dir → ok=false.
	if _, ok := OldestModTime(root); ok {
		t.Errorf("OldestModTime(empty) ok=true, want false")
	}
	// Missing / empty path → ok=false, never panics.
	if _, ok := OldestModTime(filepath.Join(root, "nope")); ok {
		t.Errorf("OldestModTime(missing) ok=true, want false")
	}
	if _, ok := OldestModTime(""); ok {
		t.Errorf(`OldestModTime("") ok=true, want false`)
	}

	mustWrite(t, filepath.Join(root, "new"), 10)
	mustWrite(t, filepath.Join(root, "old"), 10)
	// A dotfile (tmp) and a subdir must be skipped, even if oldest.
	mustWrite(t, filepath.Join(root, ".tmp-x"), 10)
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	want := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	for _, name := range []string{"old", ".tmp-x"} {
		if err := os.Chtimes(filepath.Join(root, name), want, want); err != nil {
			t.Fatal(err)
		}
	}

	got, ok := OldestModTime(root)
	if !ok {
		t.Fatal("OldestModTime ok=false, want true")
	}
	// "old" is the oldest non-dotfile; the back-dated ".tmp-x" must be ignored, so
	// the result equals "old"'s mtime, not the dotfile's (same time here) — assert
	// it picked a regular file at the back-dated time.
	if got.Truncate(time.Second) != want {
		t.Errorf("OldestModTime = %v, want %v (the oldest regular file)", got, want)
	}
}

func mustWrite(t *testing.T, path string, n int) {
	t.Helper()
	if err := os.WriteFile(path, make([]byte, n), 0o644); err != nil {
		t.Fatal(err)
	}
}
