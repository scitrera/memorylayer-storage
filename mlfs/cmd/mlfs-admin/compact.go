// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/mountlock"
)

// runCompact consolidates a tenant/domain's under-filled and small packs into
// full ones (the operator one-shot of the blobgw GC runner's repack step). It
// rewrites the referencing manifests to point at the consolidated packs, leaving
// the old packs orphaned; a subsequent GC pass (`mlfs-admin gc`) reclaims them.
//
// Offline only: like gc/fsck it rewrites manifests, so it refuses to run against
// a live mount (which could read a manifest version mid-rewrite). It needs no
// metadata DB — compaction operates purely on the casstore pack/manifest store.
func runCompact(args []string) error {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dataDir := fs.String("data-dir", "/var/lib/mlfs/data", "local casstore backend directory")
	domain := fs.String("domain", "mlfs", "casstore dedup domain (must match the daemon's)")
	packTarget := fs.Int("pack-target-bytes", 0, "casstore pack target (0 = default; must match the daemon's)")
	minFill := fs.Float64("min-fill", 0.5, "consolidate packs filled below this ratio of the pack target (0 = disabled)")
	minPackBytes := fs.Int64("min-pack-bytes", 0, "also consolidate packs smaller than this many bytes regardless of fill (0 = disabled)")
	maxBytesPerPass := fs.Int64("max-bytes-per-pass", 0, "cap on live bytes rewritten in this pass (0 = unbounded)")
	defrag := fs.Bool("defrag", false, "consolidate in FILE (manifest) order instead of chunk-hash order, so a fragmented file's chunks are laid out contiguously and a later sequential read coalesces into fewer pack GETs (restores read locality dedup/legacy fragmentation destroyed). Costs more memory: holds the selected packs' raw bytes in flight (bounded by -max-bytes-per-pass)")
	safety := fs.Duration("safety-window", 0, "skip packs younger than this; 0 = compact everything (safe offline, the default for this tool)")
	forceOnline := fs.Bool("force-online", false, "DANGEROUS: run even if a live mlfs daemon holds the data-dir lock (can race a manifest read)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *minFill <= 0 && *minPackBytes <= 0 {
		return fmt.Errorf("nothing to do: set -min-fill and/or -min-pack-bytes")
	}
	if err := mountlock.CheckQuiesced(*dataDir, *forceOnline, warnf); err != nil {
		return err
	}
	ctx := context.Background()

	local, err := chunkstore.NewLocal(*dataDir, *domain, *packTarget)
	if err != nil {
		return fmt.Errorf("chunk store: %w", err)
	}
	res, err := local.Compactor.Compact(ctx, *domain, snapshot.CompactConfig{
		MinFillRatio:    *minFill,
		MinPackBytes:    *minPackBytes,
		MaxBytesPerPass: *maxBytesPerPass,
		SafetyWindow:    *safety,
		Defrag:          *defrag,
	})
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	fmt.Printf("compact: packs_scanned=%d packs_selected=%d packs_written=%d "+
		"manifests_rewritten=%d bytes_rewritten=%d duration=%s\n",
		res.PacksScanned, res.PacksSelected, res.PacksWritten,
		res.ManifestsRewritten, res.BytesRewritten, res.Duration.Round(time.Millisecond))
	if res.ManifestsRewritten > 0 {
		fmt.Fprintln(os.Stderr, "compact: old packs are now orphaned — run `mlfs-admin gc -force` to reclaim them")
	}
	return nil
}
