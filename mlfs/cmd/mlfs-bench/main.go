// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command mlfs-bench is a standalone probe for an mlfs mount, meant to be
// deployed onto any pod that mounts mlfs storage. It depends only on the POSIX
// mountpoint — no metadata-DB credentials, no node-side directories — so it
// builds into a small binary independent of the operator tooling in mlfs-admin
// (which carries the same bench plus the DB-backed `stats` inspector).
//
// Subcommands:
//
//	bench  POSIX throughput + small-file latency against a directory under the mount.
//	info   client-side capacity (statfs) and, with -walk, a tree size summary —
//	       the view a workload has of its own mount, with no privileged access.
//	stats  live, dstat-style feed of the daemon's counters, polled from the
//	       <mount>/.mlfs/metrics virtual file (no DB or node access needed).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/bench"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fsstat"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "bench":
		if err := runBench(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-bench bench:", err)
			os.Exit(1)
		}
	case "info":
		if err := runInfo(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-bench info:", err)
			os.Exit(1)
		}
	case "stats":
		if err := runStats(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "mlfs-bench stats:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "mlfs-bench: unknown subcommand %q\n", os.Args[1])
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mlfs-bench <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "subcommands:")
	fmt.Fprintln(os.Stderr, "  bench  POSIX throughput + small-file latency on a mounted path (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  info   client-side capacity (statfs) + optional tree summary (run -h for flags)")
	fmt.Fprintln(os.Stderr, "  stats  live dstat-style feed from <mount>/.mlfs/metrics (run -h for flags)")
	os.Exit(2)
}

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cfg := bench.Flags(fs)
	asJSON := fs.Bool("json", false, "emit the result as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res, err := bench.Run(context.Background(), *cfg)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	res.Print(os.Stdout)
	return nil
}

// runInfo reports what a workload can see of its own mount: filesystem capacity
// via statfs, and — with -walk — a recursive file count and byte total. It needs
// no privileges beyond read access to the path.
func runInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	path := fs.String("path", "", "a path under the mount to inspect (required)")
	walk := fs.Bool("walk", false, "recursively sum file count and bytes under -path (can be slow on large trees)")
	asJSON := fs.Bool("json", false, "emit the report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("-path is required (a path under a mounted mlfs)")
	}
	if _, err := os.Stat(*path); err != nil {
		return fmt.Errorf("stat %s: %w", *path, err)
	}

	avail, total := fsstat.StatfsBytes(*path)
	out := struct {
		Path       string `json:"path"`
		AvailBytes int64  `json:"avail_bytes"`
		TotalBytes int64  `json:"total_bytes"`
		Walked     bool   `json:"walked"`
		Files      int64  `json:"files"`
		Bytes      int64  `json:"bytes"`
	}{Path: *path, AvailBytes: avail, TotalBytes: total, Walked: *walk}
	if *walk {
		out.Files, out.Bytes = fsstat.DirUsage(*path)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	usedPct := "—"
	if total > 0 {
		usedPct = fmt.Sprintf("%.0f%%", 100*float64(total-avail)/float64(total))
	}
	fmt.Printf("mlfs info: %s\n", *path)
	fmt.Printf("  capacity: %s free of %s (%s used)\n",
		fsstat.HumanBytes(avail), fsstat.HumanBytes(total), usedPct)
	if *walk {
		fmt.Printf("  tree:     %d files, %s\n", out.Files, fsstat.HumanBytes(out.Bytes))
	}
	return nil
}
