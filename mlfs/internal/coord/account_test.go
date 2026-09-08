// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// grantStore is a fake OwnershipStore that always grants — it isolates this test
// to the NATS-account boundary (no Postgres needed); the SQL fence is covered
// elsewhere.
type grantStore struct{}

func (grantStore) AcquireOrResume(_ context.Context, scope, node string, _ int64) (meta.Lease, bool, error) {
	return meta.Lease{ScopeKey: scope, OwnerNode: node, Generation: 1}, true, nil
}
func (grantStore) Refresh(context.Context, string, string, int64, int64) (bool, error) {
	return true, nil
}
func (grantStore) ReleaseOwnership(context.Context, string, string, int64) (bool, error) {
	return true, nil
}

// accountsNATS starts an embedded NATS with two JetStream-enabled accounts
// (a/b), each with a user, listening on a random port (auth applies, unlike the
// in-process pipe). Returns the client URL.
func accountsNATS(t *testing.T) string {
	t.Helper()
	store := t.TempDir()
	conf := filepath.Join(t.TempDir(), "nats.conf")
	cfg := `
host: "127.0.0.1"
port: -1
jetstream { store_dir: "` + store + `" }
accounts {
  A { jetstream { max_mem: 32MB, max_file: 256MB }, users: [ { user: "a", password: "pa" } ] }
  B { jetstream { max_mem: 32MB, max_file: 256MB }, users: [ { user: "b", password: "pb" } ] }
}
`
	if err := os.WriteFile(conf, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(conf)
	if err != nil {
		t.Skipf("nats config unsupported here: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// TestAccountPerDomainIsolation proves the multi-tenant model: two dedup domains
// sharing ONE NATS cluster but connecting with DIFFERENT account credentials get
// fully isolated coordination — the same bucket name ("mlfs_live") and the same
// scope key ("ino:42") do not collide, because each account has its own
// JetStream. Domain A's lease is invisible to domain B and vice versa.
func TestAccountPerDomainIsolation(t *testing.T) {
	url := accountsNATS(t)
	ctx := context.Background()

	nA, err := Connect(url, nats.UserInfo("a", "pa"))
	if err != nil {
		t.Fatalf("connect A: %v", err)
	}
	t.Cleanup(nA.Close)
	nB, err := Connect(url, nats.UserInfo("b", "pb"))
	if err != nil {
		t.Fatalf("connect B: %v", err)
	}
	t.Cleanup(nB.Close)

	oA, err := NewNATSOwner(ctx, nA, grantStore{}, "nodeA", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("owner A: %v", err)
	}
	oB, err := NewNATSOwner(ctx, nB, grantStore{}, "nodeB", 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("owner B: %v", err)
	}

	const scope = "ino:42" // identical scope key in both domains
	if _, held, err := oA.Ensure(ctx, scope); err != nil || !held {
		t.Fatalf("A ensure: held=%v err=%v", held, err)
	}

	// Cross-account KV isolation: A's liveness key exists in A's bucket but NOT
	// in B's (same bucket name, different account → different physical bucket).
	if _, err := oA.kv.Get(ctx, natsKey(scope)); err != nil {
		t.Fatalf("A should see its own liveness key: %v", err)
	}
	if _, err := oB.kv.Get(ctx, natsKey(scope)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("B must NOT see A's liveness key (account isolation breached): err=%v", err)
	}

	// And B independently owns its identically-named scope without seeing A as a
	// live foreign owner (no fencing across domains).
	if _, held, err := oB.Ensure(ctx, scope); err != nil || !held {
		t.Fatalf("B ensure (should be free in B's account): held=%v err=%v", held, err)
	}
}
