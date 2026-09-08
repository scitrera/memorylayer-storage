// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/coord"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	fusebridge "github.com/scitrera/memorylayer-storage/mlfs/internal/fuse"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/testpg"
)

// node is one live mlfs mount in a multi-node integration test: its own engine,
// cache, FUSE mount, and ownership coordinator — sharing the cluster's metadata
// DB, backing chunk store, and NATS plane with its peers.
type node struct {
	e     *meta.Engine
	dc    *cache.DiskCache
	owner *coord.NATSOwner
	mnt   string
}

// newNode builds and mounts one node against the shared db / NATS / casDir.
func newNode(t *testing.T, ctx context.Context, db *sql.DB, n *coord.NATS, casDir, nodeID string, region uint8) *node {
	t.Helper()
	e := meta.Open(db, region)
	if err := e.Migrate(ctx); err != nil { // idempotent; both nodes share one schema
		t.Fatalf("%s migrate: %v", nodeID, err)
	}
	local, err := chunkstore.NewLocal(casDir, "mlfs", 0) // SHARED backing store
	if err != nil {
		t.Fatalf("%s chunkstore: %v", nodeID, err)
	}
	dc, err := cache.New(local.Store, cache.Config{Dir: filepath.Join(t.TempDir(), "cache"), AutoUpload: true})
	if err != nil {
		t.Fatalf("%s cache: %v", nodeID, err)
	}
	owner, err := coord.NewNATSOwner(ctx, n, e, nodeID, 30*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatalf("%s owner: %v", nodeID, err)
	}
	e.SetCoordinator(nodeID, owner)

	files := fileio.New(e, dc)
	bridge := fusebridge.New(e, files, dc)
	mnt := t.TempDir()
	srv, err := fusebridge.Mount(bridge, mnt, &fuse.MountOptions{Name: "mlfs", FsName: "mlfs"}, false)
	if err != nil {
		t.Fatalf("%s mount: %v", nodeID, err)
	}
	t.Cleanup(func() {
		_ = srv.Unmount()
		_ = dc.Close()
		_ = local.Chunks.Close(context.Background())
	})
	return &node{e: e, dc: dc, owner: owner, mnt: mnt}
}

// TestTwoNodeLiveMount mounts two coordinated mlfs filesystems over a shared
// metadata DB, backing store, and NATS plane, then verifies (1) a write on one
// node is visible through the other's mount, and (2) the ownership fence blocks
// the peer from writing a scope the first node owns — end-to-end through FUSE.
func TestTwoNodeLiveMount(t *testing.T) {
	db := testpg.DB(t) // skips without MLFS_TEST_DATABASE_URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nats, err := coord.StartEmbedded(coord.EmbeddedConfig{NodeName: "itest", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	defer nats.Close()

	casDir := t.TempDir() // the shared L0 backing store both nodes read/write
	a := newNode(t, ctx, db, nats, casDir, "nodeA", 1)
	b := newNode(t, ctx, db, nats, casDir, "nodeB", 2)

	go a.owner.Run(ctx)
	go b.owner.Run(ctx)

	// nodeA establishes a subtree and writes a file (acquiring scope ino:<s1>).
	if err := os.Mkdir(filepath.Join(a.mnt, "s1"), 0o755); err != nil {
		t.Fatalf("nodeA mkdir: %v", err)
	}
	data := []byte("hello from nodeA")
	if err := os.WriteFile(filepath.Join(a.mnt, "s1", "f"), data, 0o644); err != nil {
		t.Fatalf("nodeA write: %v", err)
	}
	// Push nodeA's write-back data into the shared backing store so a peer can
	// read it (the cross-node data substrate is the shared casDir).
	if err := a.dc.Flush(ctx); err != nil {
		t.Fatalf("nodeA flush: %v", err)
	}

	// (1) Cross-node visibility: nodeB reads the same path and bytes.
	got, err := os.ReadFile(filepath.Join(b.mnt, "s1", "f"))
	if err != nil {
		t.Fatalf("nodeB read: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("nodeB read mismatch: got %q want %q", got, data)
	}

	// (2) Fence end-to-end: nodeA owns scope ino:<s1>, so nodeB writing into it
	// must fail (the fenced engine returns ESTALE; FUSE surfaces a write error).
	if err := os.WriteFile(filepath.Join(b.mnt, "s1", "f"), []byte("evil"), 0o644); err == nil {
		t.Fatal("nodeB write into nodeA-owned scope unexpectedly succeeded (fence breach)")
	}

	// nodeA, the owner, still writes its scope fine.
	if err := os.WriteFile(filepath.Join(a.mnt, "s1", "g"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("nodeA write (owner): %v", err)
	}
}
