// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/bench"
)

// runBench is the operator-side entry to the same POSIX benchmark the standalone
// mlfs-bench binary ships. It is offered here for convenience on nodes that
// already have mlfs-admin; it touches only the mountpoint (no DB), so unlike
// gc/fsck it is safe against a live mount.
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
