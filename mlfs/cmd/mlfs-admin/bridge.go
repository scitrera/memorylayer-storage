// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/scitrera/memorylayer-storage/mlfs/bridge"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// runBridge is the thin CLI over the mlfs↔VFS metadata-only bridge. Two directions:
//
//	mlfs-admin bridge to-ref   -path /a/b -ref my-ref     (mlfs file  → blob_ref)
//	mlfs-admin bridge to-mlfs  -ref my-ref -path /a/b      (blob_ref   → mlfs file)
//
// It wires BOTH sides over a SINGLE shared Postgres (-meta-dsn) and a SINGLE shared
// chunk directory (-data-dir) at the SAME domain (-domain), so the operation is a
// pure metadata synthesis over the shared casstore chunks — no bytes are moved. It
// is offline tooling for testing/manual ops; a live daemon should not run against
// the same data-dir concurrently.
func runBridge(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: mlfs-admin bridge <to-ref|to-mlfs> [flags]")
	}
	dir := args[0]
	rest := args[1:]

	fs := flag.NewFlagSet("bridge "+dir, flag.ExitOnError)
	metaDSN := fs.String("meta-dsn", os.Getenv("MLFS_META_DSN"), "shared PostgreSQL DSN (mlfs meta + slice_manifest + blob_ref/pack_manifest)")
	dataDir := fs.String("data-dir", "/var/lib/mlfs/data", "shared local chunk store directory")
	domain := fs.String("domain", "mlfs", "tenant domain (mlfs mount == blobgw ref == dedup domain)")
	packTarget := fs.Int("pack-target-bytes", 0, "casstore pack target (0 = default; must match the daemon's)")
	path := fs.String("path", "", "mlfs file path (source for to-ref, target for to-mlfs)")
	ref := fs.String("ref", "", "blob_ref name (target for to-ref, source for to-mlfs)")
	contentType := fs.String("content-type", "application/octet-stream", "content type recorded on the ref (to-ref)")
	uncompressed := fs.Bool("uncompressed", false, "mark the created mlfs file's slices uncompressed class (to-mlfs)")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if *metaDSN == "" {
		return fmt.Errorf("-meta-dsn (or $MLFS_META_DSN) is required")
	}
	if *path == "" || *ref == "" {
		return fmt.Errorf("both -path and -ref are required")
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", *metaDSN)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	stack, err := bridge.NewStack(ctx, bridge.StackConfig{
		DB:              db,
		ChunkDir:        *dataDir,
		Domain:          *domain,
		PackTargetBytes: *packTarget,
	})
	if err != nil {
		return err
	}
	defer func() { _ = stack.Close(ctx) }()

	switch dir {
	case "to-ref":
		info, err := stack.Bridge.MlfsToRef(ctx, *path, *ref, *contentType, nil)
		if err != nil {
			return err
		}
		fmt.Printf("bridge to-ref: registered ref=%q content_hash=%s size=%d version=%s (no bytes moved)\n",
			info.Ref, info.ContentHash, info.Size, info.Version)
		return nil
	case "to-mlfs":
		class := bridge.ClassDefault
		if *uncompressed {
			class = bridge.StoreClass(chunkstore.ClassUncompressed)
		}
		ino, err := stack.Bridge.RefToMlfs(ctx, *ref, *path, class)
		if err != nil {
			return err
		}
		fmt.Printf("bridge to-mlfs: created inode=%d at %q from ref=%q (no bytes moved)\n", ino, *path, *ref)
		return nil
	default:
		return fmt.Errorf("unknown bridge direction %q (want to-ref|to-mlfs)", dir)
	}
}
