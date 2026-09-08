// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"testing"
	"time"
)

// fakeReaper is an in-memory LockReaper for testing the reap logic without a DB.
type fakeReaper struct {
	sids    []uint64
	cleared []uint64
}

func (f *fakeReaper) ListLockSIDs(context.Context) ([]uint64, error) { return f.sids, nil }
func (f *fakeReaper) ClearLocksForSID(_ context.Context, sid uint64) error {
	f.cleared = append(f.cleared, sid)
	return nil
}

func embeddedNATS(t *testing.T) *NATS {
	t.Helper()
	n, err := StartEmbedded(EmbeddedConfig{NodeName: "test", StoreDir: t.TempDir()})
	if err != nil {
		t.Skipf("embedded nats unavailable: %v", err)
	}
	t.Cleanup(n.Close)
	return n
}

// TestReaperClearsOnlyDeadSIDs: a live node's sid is kept; a sid absent from the
// node registry (a crashed peer) is reaped.
func TestReaperClearsOnlyDeadSIDs(t *testing.T) {
	n := embeddedNATS(t)
	ctx := context.Background()
	reg, err := NewRegistry(ctx, n, "nodeA", 1001, 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if err := reg.Heartbeat(ctx); err != nil { // nodeA (sid 1001) is alive
		t.Fatalf("heartbeat: %v", err)
	}
	// Lock tables hold rows for the live sid 1001 and a dead sid 9999.
	fr := &fakeReaper{sids: []uint64{1001, 9999}}
	reaped, err := reg.ReapDeadLocks(ctx, fr)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if reaped != 1 || len(fr.cleared) != 1 || fr.cleared[0] != 9999 {
		t.Fatalf("expected only sid 9999 reaped, got reaped=%d cleared=%v", reaped, fr.cleared)
	}
}

// TestLeaderSingleWinner: two campaigners over one bucket, exactly one wins; the
// loser becomes leader only after the winner releases.
func TestLeaderSingleWinner(t *testing.T) {
	n := embeddedNATS(t)
	ctx := context.Background()
	la, err := NewLeader(ctx, n, "gc", "nodeA", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("leader A: %v", err)
	}
	lb, err := NewLeader(ctx, n, "gc", "nodeB", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("leader B: %v", err)
	}
	la.campaign(ctx)
	lb.campaign(ctx)
	if !la.IsLeader() || lb.IsLeader() {
		t.Fatalf("expected exactly nodeA leader: A=%v B=%v", la.IsLeader(), lb.IsLeader())
	}
	// nodeA steps down; nodeB can now win.
	if err := la.kv.Delete(ctx, la.key); err != nil {
		t.Fatalf("release: %v", err)
	}
	lb.campaign(ctx)
	if !lb.IsLeader() {
		t.Fatalf("nodeB should take leadership after release")
	}
}
