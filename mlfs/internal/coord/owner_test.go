// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"database/sql"
	"syscall"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// twoNATSNodes wires two meta engines (shared PG schema) each driven by a
// NATSOwner over ONE embedded JetStream — the multi-node setup: shared SoR,
// shared liveness plane, independent node identities.
func twoNATSNodes(t *testing.T) (db *sql.DB, a, b *meta.Engine, oa, ob *NATSOwner, n *NATS) {
	t.Helper()
	db = testpg.DB(t) // skips when MLFS_TEST_DATABASE_URL is unset
	n, err := StartEmbedded(EmbeddedConfig{NodeName: "test", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(n.Close)

	ctx := context.Background()
	a = meta.Open(db, 1)
	b = meta.Open(db, 2)
	if err := a.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	oa, err = NewNATSOwner(ctx, n, a, "nodeA", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("owner A: %v", err)
	}
	ob, err = NewNATSOwner(ctx, n, b, "nodeB", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("owner B: %v", err)
	}
	a.SetCoordinator("nodeA", oa)
	b.SetCoordinator("nodeB", ob)
	return db, a, b, oa, ob, n
}

func ds(id uint64) meta.Slice { return meta.Slice{Id: id, Size: 16, Off: 0, Len: 16} }

// setup creates /s1/f via nodeA and returns f's inode and scope key. nodeA owns
// the scope afterward.
func setupFile(t *testing.T, a *meta.Engine) (f meta.Ino, scope string) {
	t.Helper()
	ctx, c := context.Background(), meta.Background()
	if _, _, st := a.Mkdir(ctx, meta.RootInode, "s1", 0o755, c); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	s1, _, st := a.Lookup(ctx, meta.RootInode, "s1")
	if st != 0 {
		t.Fatalf("lookup: %v", st)
	}
	f, _, st = a.Create(ctx, s1, "f", 0o644, c)
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := a.Write(ctx, f, 0, 0, ds(1)); st != 0 {
		t.Fatalf("write A: %v", st)
	}
	scope, err := a.ScopeOf(ctx, f)
	if err != nil {
		t.Fatalf("scopeOf: %v", err)
	}
	return f, scope
}

// TestNATSLivePeerBlocksSteal: while nodeA holds a live lease (NATS liveness key
// present), nodeB must NOT be able to write the scope — Ensure sees a live peer
// and the write is fenced with ESTALE, with no SQL steal attempted.
func TestNATSLivePeerBlocksSteal(t *testing.T) {
	_, a, b, _, _, _ := twoNATSNodes(t)
	ctx := context.Background()
	f, _ := setupFile(t, a)

	if st := b.Write(ctx, f, 0, 0, ds(2)); st != syscall.ESTALE {
		t.Fatalf("nodeB write against live nodeA: got %v, want ESTALE", st)
	}
	// nodeA, the live owner, still writes fine.
	if st := a.Write(ctx, f, 0, 0, ds(3)); st != 0 {
		t.Fatalf("nodeA write: %v", st)
	}
}

// TestNATSStealFencesStaleWriter: once nodeA's lease is gone (NATS key removed +
// SQL expired), nodeB steals (generation bumps) and writes; nodeA's next write —
// still trusting its cached old generation — is fenced with ESTALE.
func TestNATSStealFencesStaleWriter(t *testing.T) {
	db, a, b, oa, _, _ := twoNATSNodes(t)
	ctx := context.Background()
	f, scope := setupFile(t, a)

	// Simulate nodeA's death from the cluster's view: drop its liveness key and
	// expire the SQL lease so nodeB legitimately steals.
	if err := oa.kv.Delete(ctx, natsKey(scope)); err != nil {
		t.Fatalf("delete live key: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE ownership SET lease_expires_unix_ms = 0 WHERE scope_key = $1`, scope); err != nil {
		t.Fatalf("expire sql lease: %v", err)
	}

	// nodeB steals and writes.
	if st := b.Write(ctx, f, 0, 0, ds(4)); st != 0 {
		t.Fatalf("nodeB steal write: %v", st)
	}
	// nodeA still believes it owns the old generation → fenced.
	if st := a.Write(ctx, f, 0, 0, ds(5)); st != syscall.ESTALE {
		t.Fatalf("nodeA stale write: got %v, want ESTALE", st)
	}
}

// TestNATSRefreshKeepsLeaseLive: with nodeA's Run loop active, its liveness key
// is continuously renewed, so a peer keeps seeing it as a live owner.
func TestNATSRefreshKeepsLeaseLive(t *testing.T) {
	_, a, b, oa, _, _ := twoNATSNodes(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f, scope := setupFile(t, a)
	go oa.Run(ctx)

	// Liveness key is present and owned by nodeA.
	if _, err := oa.kv.Get(ctx, natsKey(scope)); err != nil {
		t.Fatalf("liveness key missing: %v", err)
	}
	// Peer still blocked.
	if st := b.Write(ctx, f, 0, 0, ds(6)); st != syscall.ESTALE {
		t.Fatalf("peer should be blocked while owner alive: got %v", st)
	}
}
