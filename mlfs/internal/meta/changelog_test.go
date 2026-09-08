// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"fmt"
	"testing"
	"time"
)

// changelogLen counts the entries currently in fs_changelog.
func changelogLen(t *testing.T, e *Engine) int {
	t.Helper()
	n := 0
	if err := e.ScanChangelog(ctxBg(), 0, func(ChangeEntry) error { n++; return nil }); err != nil {
		t.Fatalf("scan changelog: %v", err)
	}
	return n
}

func TestChangelogRetentionByLSNAndCount(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()
	for i := 0; i < 10; i++ {
		if _, _, st := e.Mkdir(ctx, RootInode, fmt.Sprintf("d%d", i), 0o755, c); st != 0 {
			t.Fatalf("mkdir %d: errno %v", i, st)
		}
	}
	if got := changelogLen(t, e); got != 10 {
		t.Fatalf("want 10 entries, got %d", got)
	}

	horizon, err := e.ChangelogHorizon(ctx)
	if err != nil {
		t.Fatalf("horizon: %v", err)
	}
	if horizon == 0 {
		t.Fatal("horizon should be non-zero with 10 entries")
	}

	// Keep the newest 4; the other 6 go.
	del, err := e.TrimChangelogKeepLast(ctx, 4)
	if err != nil {
		t.Fatalf("keep-last: %v", err)
	}
	if del != 6 || changelogLen(t, e) != 4 {
		t.Fatalf("keep-last 4: deleted=%d remaining=%d", del, changelogLen(t, e))
	}

	// Trimming up to the old horizon clears everything that remains.
	if _, err := e.TrimChangelog(ctx, horizon); err != nil {
		t.Fatalf("trim up-to-horizon: %v", err)
	}
	if got := changelogLen(t, e); got != 0 {
		t.Fatalf("want 0 after trim up-to-horizon, got %d", got)
	}

	// No-ops are safe.
	if del, _ := e.TrimChangelog(ctx, 0); del != 0 {
		t.Errorf("TrimChangelog(0) should delete nothing, got %d", del)
	}
	if del, _ := e.TrimChangelogKeepLast(ctx, 0); del != 0 {
		t.Errorf("TrimChangelogKeepLast(0) should delete nothing, got %d", del)
	}
}

func TestChangelogRetentionByAge(t *testing.T) {
	e := openTestEngine(t)
	ctx, c := ctxBg(), Background()

	base := time.Unix(1_700_000_000, 0)
	e.SetClock(func() time.Time { return base }) // "old" entries
	for i := 0; i < 3; i++ {
		if _, _, st := e.Mkdir(ctx, RootInode, fmt.Sprintf("old%d", i), 0o755, c); st != 0 {
			t.Fatalf("mkdir old%d: errno %v", i, st)
		}
	}
	e.SetClock(func() time.Time { return base.Add(2 * time.Hour) }) // "new" entries
	for i := 0; i < 2; i++ {
		if _, _, st := e.Mkdir(ctx, RootInode, fmt.Sprintf("new%d", i), 0o755, c); st != 0 {
			t.Fatalf("mkdir new%d: errno %v", i, st)
		}
	}

	// now == base+2h; cutoff at 1h ago drops the base-stamped entries only.
	del, err := e.TrimChangelogOlderThan(ctx, time.Hour)
	if err != nil {
		t.Fatalf("trim older-than: %v", err)
	}
	if del != 3 {
		t.Fatalf("want 3 old entries deleted, got %d", del)
	}
	if got := changelogLen(t, e); got != 2 {
		t.Fatalf("want 2 recent entries kept, got %d", got)
	}
	if del, _ := e.TrimChangelogOlderThan(ctx, 0); del != 0 {
		t.Errorf("TrimChangelogOlderThan(0) should delete nothing, got %d", del)
	}
}
