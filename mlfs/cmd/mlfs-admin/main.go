// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command mlfs-admin is the L2 operations CLI. Implemented subcommands:
//
//	gc   refcount-free mark-and-sweep garbage collection (slice_ref → orphan
//	     slice manifests → casstore blob sweep), with a safety window.
//	fsck  reap crash-orphaned ("zombie") inodes (unlinked-while-open whose
//	      holder crashed before the final Close), optionally clear stale locks,
//	      then run a GC pass to reclaim the freed slices. Offline/single-node.
//	stats read-only inspector of the metadata DB and the local node's on-disk
//	      cache/casstore (capacity, write-back backlog, dedup), with an optional
//	      -watch live feed. Safe against a live mount.
//	bench POSIX throughput + small-file latency against a directory under a
//	      mounted mlfs. Touches only the mountpoint; the same benchmark also
//	      ships as the standalone mlfs-bench binary for workload pods.
//	compact consolidate under-filled/small casstore packs (repack), rewriting
//	      the referencing manifests; a following gc reclaims the orphaned old
//	      packs. Offline (rewrites manifests); needs no metadata DB.
//
// gc and fsck assume a quiesced filesystem. They refuse to run while a live mlfs
// daemon holds the data-dir lock file (it would otherwise race the daemon and
// risk reaping an open-but-unlinked inode → data loss); a stale lock file (dead
// PID / unparseable) is ignored with a warning, and -force-online overrides the
// guard for operators certain no mount is active (DANGEROUS). stats and bench
// are read/POSIX-only and carry no such guard.
//
// Planned: ownership, changelog.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/gc"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/mountlock"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "gc":
		if err := runGC(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin gc:", err)
			os.Exit(1)
		}
	case "fsck":
		if err := runFsck(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin fsck:", err)
			os.Exit(1)
		}
	case "stats":
		if err := runStats(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin stats:", err)
			os.Exit(1)
		}
	case "bench":
		if err := runBench(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin bench:", err)
			os.Exit(1)
		}
	case "compact":
		if err := runCompact(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin compact:", err)
			os.Exit(1)
		}
	case "bridge":
		if err := runBridge(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-admin bridge:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "mlfs-admin: unknown subcommand %q\n", os.Args[1])
		usage()
	}
}

// warnf prints a guard warning (stale-lock notice, force-online override) to
// stderr, prefixed for the operator.
func warnf(msg string) { fmt.Fprintln(os.Stderr, "mlfs-admin: warning:", msg) }

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mlfs-admin <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "subcommands:")
	fmt.Fprintln(os.Stderr, "  gc     garbage-collect orphaned slice data (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  fsck   reap crash-orphaned inodes + stale locks, then GC (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  stats  read-only filesystem inspector: metadata + on-disk usage (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  bench  POSIX throughput + small-file latency on a mounted path (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  compact  consolidate under-filled/small packs (repack); reclaim via gc (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  bridge   metadata-only mlfs↔blobgw bridge: to-ref | to-mlfs (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  (planned: ownership, changelog)")
	os.Exit(2)
}

func runGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	metaDSN := fs.String("meta-dsn", os.Getenv("MLFS_META_DSN"), "PostgreSQL DSN for the metadata engine (pgx)")
	dataDir := fs.String("data-dir", "/var/lib/mlfs/data", "local casstore backend directory")
	domain := fs.String("domain", "mlfs", "casstore dedup domain (must match the daemon's)")
	packTarget := fs.Int("pack-target-bytes", 0, "casstore pack target (0 = default; must match the daemon's)")
	safety := fs.Duration("safety-window", 24*time.Hour, "minimum age before an orphan is reclaimed")
	force := fs.Bool("force", false, "reclaim immediately (safety-window=0); for tests/manual ops")
	forceOnline := fs.Bool("force-online", false, "DANGEROUS: run even if a live mlfs daemon holds the data-dir lock (can cause data loss)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *metaDSN == "" {
		return fmt.Errorf("-meta-dsn (or $MLFS_META_DSN) is required")
	}
	if err := mountlock.CheckQuiesced(*dataDir, *forceOnline, warnf); err != nil {
		return err
	}
	if *force {
		*safety = 0
	}
	ctx := context.Background()

	db, err := sql.Open("pgx", *metaDSN)
	if err != nil {
		return fmt.Errorf("open meta db: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping meta db: %w", err)
	}
	engine := meta.Open(db, 0) // region irrelevant to GC (no inode allocation)
	if err := engine.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate meta schema: %w", err)
	}
	local, err := chunkstore.NewLocal(*dataDir, *domain, *packTarget)
	if err != nil {
		return fmt.Errorf("chunk store: %w", err)
	}

	g := gc.New(engine, local)
	g.SetSafetyWindow(*safety)
	res, err := g.Run(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("gc: live_slices=%d scanned=%d orphans_deleted=%d deferred_young=%d | "+
		"chunks_reclaimed=%d bytes_reclaimed=%d chunks_deferred_young=%d duration=%s\n",
		res.LiveSlices, res.SlicesScanned, res.SlicesDeleted, res.SlicesDeferredYoung,
		res.Cas.ChunksReclaimed, res.Cas.BytesReclaimed, res.Cas.ChunksDeferredYoung, res.Cas.Duration)
	return nil
}

// runFsck reaps crash-orphaned ("zombie") inodes — files unlinked while open
// whose holder crashed before the final Close — then optionally clears stale
// locks and runs a forced GC pass so the freed slices are reclaimed in one go.
// All three steps are offline/single-node operations (no live mount).
func runFsck(args []string) error {
	fs := flag.NewFlagSet("fsck", flag.ExitOnError)
	metaDSN := fs.String("meta-dsn", os.Getenv("MLFS_META_DSN"), "PostgreSQL DSN for the metadata engine (pgx)")
	dataDir := fs.String("data-dir", "/var/lib/mlfs/data", "local casstore backend directory")
	domain := fs.String("domain", "mlfs", "casstore dedup domain (must match the daemon's)")
	packTarget := fs.Int("pack-target-bytes", 0, "casstore pack target (0 = default; must match the daemon's)")
	reapLocks := fs.Bool("reap-locks", true, "clear all stale advisory locks (single-node only)")
	runGCPass := fs.Bool("gc", true, "run a forced GC pass afterward to reclaim freed slices")
	safety := fs.Duration("safety-window", 0, "GC minimum orphan age (0 = reclaim immediately, the fsck default)")
	forceOnline := fs.Bool("force-online", false, "DANGEROUS: run even if a live mlfs daemon holds the data-dir lock (can cause data loss)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *metaDSN == "" {
		return fmt.Errorf("-meta-dsn (or $MLFS_META_DSN) is required")
	}
	if err := mountlock.CheckQuiesced(*dataDir, *forceOnline, warnf); err != nil {
		return err
	}
	ctx := context.Background()

	db, err := sql.Open("pgx", *metaDSN)
	if err != nil {
		return fmt.Errorf("open meta db: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping meta db: %w", err)
	}
	engine := meta.Open(db, 0) // region irrelevant to fsck (no inode allocation)
	if err := engine.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate meta schema: %w", err)
	}

	reaped, err := engine.ReapOrphanedInodes(ctx)
	if err != nil {
		return fmt.Errorf("reap orphaned inodes: %w", err)
	}

	if *reapLocks {
		fmt.Fprintln(os.Stderr, "fsck: clearing all advisory locks (single-node only; do not run against a live mount)")
		if err := engine.ClearAllLocks(ctx); err != nil {
			return fmt.Errorf("clear stale locks: %w", err)
		}
	}

	if !*runGCPass {
		fmt.Printf("fsck: zombies_reaped=%d locks_reaped=%t gc=skipped\n", reaped, *reapLocks)
		return nil
	}

	local, err := chunkstore.NewLocal(*dataDir, *domain, *packTarget)
	if err != nil {
		return fmt.Errorf("chunk store: %w", err)
	}
	g := gc.New(engine, local)
	g.SetSafetyWindow(*safety)
	res, err := g.Run(ctx)
	if err != nil {
		return fmt.Errorf("gc pass: %w", err)
	}
	fmt.Printf("fsck: zombies_reaped=%d locks_reaped=%t | gc: orphans_deleted=%d "+
		"chunks_reclaimed=%d bytes_reclaimed=%d\n",
		reaped, *reapLocks, res.SlicesDeleted, res.Cas.ChunksReclaimed, res.Cas.BytesReclaimed)
	return nil
}
