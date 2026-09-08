// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"syscall"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// TestContentHashSetGet verifies the whole-file content-hash column: it defaults
// to empty on a fresh file, round-trips through SetContentHash / ContentHash and
// GetAttr, and reports ENOENT for a missing inode.
func TestContentHashSetGet(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()

	ino, a, st := e.Create(ctx, RootInode, "f.bin", 0o644, c)
	if st != 0 {
		t.Fatalf("Create: %v", st)
	}
	if a.ContentHash != "" {
		t.Fatalf("fresh file ContentHash = %q, want empty", a.ContentHash)
	}

	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if st := e.SetContentHash(ctx, ino, want); st != 0 {
		t.Fatalf("SetContentHash: %v", st)
	}

	got, st := e.ContentHash(ctx, ino)
	if st != 0 {
		t.Fatalf("ContentHash: %v", st)
	}
	if got != want {
		t.Fatalf("ContentHash = %q, want %q", got, want)
	}
	// Also visible through GetAttr (the bridge reads it from the attr).
	if a2, st := e.GetAttr(ctx, ino); st != 0 || a2.ContentHash != want {
		t.Fatalf("GetAttr ContentHash = %q (st=%v), want %q", a2.ContentHash, st, want)
	}

	// Missing inode → ENOENT on both the setter and the accessor.
	if st := e.SetContentHash(ctx, Ino(1<<40), "deadbeef"); st != syscall.ENOENT {
		t.Fatalf("SetContentHash(missing) = %v, want ENOENT", st)
	}
	if _, st := e.ContentHash(ctx, Ino(1<<40)); st != syscall.ENOENT {
		t.Fatalf("ContentHash(missing) = %v, want ENOENT", st)
	}
}

// TestContentHashSurvivesReopen confirms the persisted hash outlives a restart:
// a second Engine over the SAME database (schema pinned on the connection) reads
// the value written by the first — it lives in the node row, not in memory.
func TestContentHashSurvivesReopen(t *testing.T) {
	db := testpg.DB(t) // one DB (private schema); shared by both engines
	e := Open(db, 1)
	ctx, c := ctxBg(), Background()
	if err := e.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ino, _, st := e.Create(ctx, RootInode, "persist.bin", 0o644, c)
	if st != 0 {
		t.Fatalf("Create: %v", st)
	}
	const want = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if st := e.SetContentHash(ctx, ino, want); st != 0 {
		t.Fatalf("SetContentHash: %v", st)
	}

	// "Reopen": a fresh Engine over the same DB (no re-migrate needed).
	e2 := Open(db, 1)
	got, st := e2.ContentHash(ctx, ino)
	if st != 0 {
		t.Fatalf("reopened ContentHash: %v", st)
	}
	if got != want {
		t.Fatalf("after reopen ContentHash = %q, want %q", got, want)
	}
}
