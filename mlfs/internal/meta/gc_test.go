// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"testing"
)

// TestReapOrphanedInodes reproduces the crash-orphan leak (#31): a file unlinked
// while open is "sustained" (kept until the final Close). If the holder crashes
// mid-sustain, the node survives with nlink=0 and no edge but is never Close'd,
// so its slice_ref rows pin its chunks forever (slice GC sees them as live).
// ReapOrphanedInodes removes such zombies; live files and root are untouched.
func TestReapOrphanedInodes(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()

	// A zombie: a created+written file that we orphan by dropping its edge and
	// zeroing nlink, mimicking sustain-then-crash (Unlink-while-open, no Close).
	zombie, _, st := e.Create(ctx, RootInode, "zombie", 0o644, c)
	if st != 0 {
		t.Fatalf("Create zombie: %v", st)
	}
	if st := e.Write(ctx, zombie, 0, 0, Slice{Id: 9100, Size: 16, Off: 0, Len: 16}); st != 0 {
		t.Fatalf("Write zombie: %v", st)
	}
	if _, err := e.db.ExecContext(ctx, `DELETE FROM edge WHERE inode=$1`, int64(zombie)); err != nil {
		t.Fatalf("drop zombie edge: %v", err)
	}
	if _, err := e.db.ExecContext(ctx, `UPDATE node SET nlink=0 WHERE inode=$1`, int64(zombie)); err != nil {
		t.Fatalf("zero zombie nlink: %v", err)
	}

	// A live file that must survive the reap untouched.
	live, _, st := e.Create(ctx, RootInode, "live", 0o644, c)
	if st != 0 {
		t.Fatalf("Create live: %v", st)
	}
	if st := e.Write(ctx, live, 0, 0, Slice{Id: 9101, Size: 8, Off: 0, Len: 8}); st != 0 {
		t.Fatalf("Write live: %v", st)
	}

	n, err := e.ReapOrphanedInodes(ctx)
	if err != nil {
		t.Fatalf("ReapOrphanedInodes: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped count: got %d want 1", n)
	}

	// Zombie node + its slice_ref rows are gone.
	if got := countRows(t, e, `SELECT count(*) FROM node WHERE inode=$1`, int64(zombie)); got != 0 {
		t.Errorf("zombie node should be reaped: %d rows remain", got)
	}
	if got := countRows(t, e, `SELECT count(*) FROM slice_ref WHERE inode=$1`, int64(zombie)); got != 0 {
		t.Errorf("zombie slice_ref should be reaped: %d rows remain", got)
	}

	// Live file is untouched.
	if got := countRows(t, e, `SELECT count(*) FROM node WHERE inode=$1`, int64(live)); got != 1 {
		t.Errorf("live node should survive: %d rows", got)
	}
	if got := countRows(t, e, `SELECT count(*) FROM slice_ref WHERE inode=$1`, int64(live)); got != 1 {
		t.Errorf("live slice_ref should survive: %d rows", got)
	}

	// A second reap is a no-op.
	if n, err := e.ReapOrphanedInodes(ctx); err != nil || n != 0 {
		t.Errorf("second reap: n=%d err=%v want 0, nil", n, err)
	}
}

// TestReapOrphanedInodesSparesRoot guards against ever reaping the root inode
// even if it momentarily looks orphan-shaped (parentless, no inbound edge).
func TestReapOrphanedInodesSparesRoot(t *testing.T) {
	e := openTestEngine(t)
	ctx := ctxBg()
	// Force root to nlink<=0 to make the query's other predicates the only thing
	// protecting it; the inode<>RootInode guard must still spare it.
	if _, err := e.db.ExecContext(ctx, `UPDATE node SET nlink=0 WHERE inode=$1`, int64(RootInode)); err != nil {
		t.Fatalf("zero root nlink: %v", err)
	}
	if n, err := e.ReapOrphanedInodes(ctx); err != nil {
		t.Fatalf("ReapOrphanedInodes: %v", err)
	} else if n != 0 {
		t.Fatalf("root must never be reaped: reaped %d", n)
	}
	if _, st := e.GetAttr(ctx, RootInode); st != 0 {
		t.Errorf("root should still exist: %v", st)
	}
}

func countRows(t *testing.T, e *Engine, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(ctxBg(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}
