// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command dedupprobe measures how much PHYSICAL storage (and therefore how much
// network transfer) it costs to ingest one blob after another into a single
// casstore dedup domain.
//
// # Why this exists
//
// A snapshot-restore tier built on casstore is only worth building if a
// re-snapshot of the same workload mostly deduplicates against the previous one.
// If it does, restoring a changed snapshot pulls a small delta instead of tens of
// gigabytes; if it does not, the whole premise collapses back to "a slow copy".
//
// The measurement is deliberately PHYSICAL, not logical: it reports the bytes the
// chunk blobstore actually grew by, which is the number that governs fetch cost.
// It does not consult casstore internals, so it cannot flatter itself.
//
// # Modes
//
//	dedupprobe -files a.bin,b.bin,c.bin
//	    Ingest each file in order into one domain and report the incremental
//	    physical cost of each. Use this on REAL consecutive snapshots.
//
//	dedupprobe -base model.safetensors -synthetic
//	    Ingest the base, then a matrix of perturbations that model the specific
//	    ways a re-snapshot might differ from its predecessor, and report the
//	    incremental cost of each. Use this to BOUND the risk when real snapshot
//	    pairs are not available.
//
// The synthetic perturbations model distinct failure modes:
//
//   - identical    — a byte-identical re-snapshot. Establishes the floor.
//   - shift        — every byte displaced by a fixed offset, as if an allocation
//     base address moved. This is the case content-defined chunking
//     is supposed to absorb and fixed-size chunking cannot.
//   - sparse-edit  — a small fraction of bytes changed in place, as if a subset of
//     weights or embedded pointer values differed.
//   - realloc      — a region resized in the middle, displacing everything after
//     it. Models one allocation changing size between runs.
//   - permute      — fixed-size blocks reordered, as if the allocator handed back
//     the same buffers in a different order. This is the WORST case
//     and the one that would actually sink the design.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

func main() {
	var (
		filesArg  = flag.String("files", "", "comma-separated blobs to ingest in order")
		base      = flag.String("base", "", "base blob for -synthetic mode")
		synthetic = flag.Bool("synthetic", false, "run the perturbation matrix against -base")
		packSize  = flag.Int("pack-size", 0, "casstore pack target bytes (0 = default ~16 MiB)")
		seed      = flag.Int64("seed", 1, "PRNG seed for synthetic perturbations (deterministic)")
	)
	flag.Parse()

	if err := run(*filesArg, *base, *synthetic, *packSize, *seed); err != nil {
		fmt.Fprintf(os.Stderr, "dedupprobe: %v\n", err)
		os.Exit(1)
	}
}

func run(filesArg, base string, synthetic bool, packSize int, seed int64) error {
	var variants []variant
	switch {
	case synthetic:
		if base == "" {
			return fmt.Errorf("-synthetic requires -base")
		}
		raw, err := os.ReadFile(base)
		if err != nil {
			return fmt.Errorf("read base: %w", err)
		}
		variants = syntheticMatrix(raw, seed)
	case filesArg != "":
		for _, p := range strings.Split(filesArg, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			raw, err := os.ReadFile(p)
			if err != nil {
				return fmt.Errorf("read %s: %w", p, err)
			}
			variants = append(variants, variant{name: filepath.Base(p), data: raw})
		}
	default:
		return fmt.Errorf("supply -files or -base with -synthetic")
	}
	if len(variants) == 0 {
		return fmt.Errorf("nothing to ingest")
	}

	ctx := context.Background()
	root, err := os.MkdirTemp("", "dedupprobe-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	chunkDir := filepath.Join(root, "chunks")
	upstream, err := snapshot.NewLocalStore(filepath.Join(root, "manifests"))
	if err != nil {
		return err
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: chunkDir})
	if err != nil {
		return err
	}
	defer chunks.Close(ctx)

	// GlobalIndex is required: cross-OBJECT dedup is the entire premise. The
	// default PriorManifestIndex would only dedup a key against its own prior
	// version and would silently report a far better number than production.
	dedup := snapshot.NewMemoryDedupStore()
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: packSize,
		DedupDomain:     "probe",
		Index:           snapshot.NewGlobalIndex(dedup, nil),
		// Store uncompressed: a GPU/tensor snapshot class is stored raw in
		// production (mmap/zero-copy), and compression would confound the
		// measurement by mixing "did it dedup" with "did it compress".
		CompressionPolicy: snapshot.NewContentTypePolicy(snapshot.CompressNone),
	})
	if err != nil {
		return err
	}

	// A blob smaller than a handful of chunks cannot produce a meaningful reuse
	// number: at one chunk per object ANY change rewrites the whole thing and
	// every variant reports 0%, which reads as "dedup does not work" when it
	// actually means "the sample is too small to chunk".
	const minMeaningful = 8 << 20
	if n := len(variants[0].data); n < minMeaningful {
		fmt.Fprintf(os.Stderr,
			"WARNING: base is %s; below ~%s the object spans too few chunks for the\n"+
				"         reuse column to mean anything (expect 0%% everywhere). Use a larger blob.\n\n",
			humanize(int64(n)), humanize(minMeaningful))
	}

	fmt.Printf("%-16s %12s %14s %14s %9s %8s\n",
		"VARIANT", "LOGICAL", "PHYS-DELTA", "PHYS-TOTAL", "REUSE", "CHUNKS")
	prevPhysical := int64(0)
	prevChunks := 0
	for _, v := range variants {
		key := snapshot.SnapshotKey{Tenant: "probe", OwnerKey: v.name}
		if _, err := cs.Put(ctx, key, snapshot.SnapshotMetadata{}, bytes.NewReader(v.data)); err != nil {
			return fmt.Errorf("ingest %s: %w", v.name, err)
		}
		// Verify the round trip: a dedup number is meaningless if the bytes do
		// not come back intact.
		if err := verify(ctx, cs, key, v.data); err != nil {
			return fmt.Errorf("verify %s: %w", v.name, err)
		}

		physical, err := dirBytes(chunkDir)
		if err != nil {
			return err
		}
		delta := physical - prevPhysical
		nChunks := dedup.Len("probe")

		logical := int64(len(v.data))
		reuse := 0.0
		if logical > 0 {
			reuse = 100 * (1 - float64(delta)/float64(logical))
		}
		if reuse < 0 {
			reuse = 0
		}
		fmt.Printf("%-16s %12s %14s %14s %8.1f%% %8d\n",
			v.name, humanize(logical), humanize(delta), humanize(physical), reuse, nChunks-prevChunks)

		prevPhysical = physical
		prevChunks = nChunks
	}
	fmt.Printf("\nREUSE = 1 - (physical bytes added / logical bytes ingested).\n")
	fmt.Printf("It is the fraction of a re-snapshot that would NOT need to be fetched.\n")
	return nil
}

type variant struct {
	name string
	data []byte
}

// syntheticMatrix builds the perturbation set described in the package comment.
// Every perturbation is deterministic given seed so runs are comparable.
func syntheticMatrix(base []byte, seed int64) []variant {
	rng := rand.New(rand.NewSource(seed))
	out := []variant{{name: "base", data: base}}

	out = append(out, variant{name: "identical", data: append([]byte(nil), base...)})

	// Allocation base address moved: everything displaced by a non-aligned amount.
	shift := make([]byte, 0, len(base)+4096)
	shift = append(shift, bytes.Repeat([]byte{0xA5}, 4093)...)
	shift = append(shift, base...)
	out = append(out, variant{name: "shift-4093B", data: shift})

	// A small fraction of values differ in place (weight deltas, pointer values).
	for _, pct := range []float64{0.1, 1} {
		edited := append([]byte(nil), base...)
		n := int(float64(len(edited)) * pct / 100)
		for i := 0; i < n; i++ {
			edited[rng.Intn(len(edited))] ^= 0xFF
		}
		out = append(out, variant{name: fmt.Sprintf("sparse-edit-%.1f%%", pct), data: edited})
	}

	// The SAME volume of change as sparse-edit, but CONTIGUOUS. The contrast
	// between this and sparse-edit is the whole question: scattered differences
	// touch every chunk and destroy dedup, localized differences touch few.
	for _, pct := range []float64{1, 5} {
		edited := append([]byte(nil), base...)
		n := int(float64(len(edited)) * pct / 100)
		start := len(edited)/3 - n/2
		if start < 0 {
			start = 0
		}
		for i := start; i < start+n && i < len(edited); i++ {
			edited[i] ^= 0xFF
		}
		out = append(out, variant{name: fmt.Sprintf("local-edit-%.0f%%", pct), data: edited})
	}

	// One allocation in the middle changed size, displacing the tail.
	if len(base) > 1<<16 {
		mid := len(base) / 2
		grown := make([]byte, 0, len(base)+65536)
		grown = append(grown, base[:mid]...)
		grown = append(grown, bytes.Repeat([]byte{0x5A}, 65536)...)
		grown = append(grown, base[mid:]...)
		out = append(out, variant{name: "realloc-mid-64K", data: grown})
	}

	// Worst case: the allocator returned the same buffers in a different order.
	for _, blk := range []int{1 << 20, 1 << 22, 1 << 26} {
		if len(base) < blk*4 {
			continue
		}
		out = append(out, variant{
			name: fmt.Sprintf("permute-%s", humanize(int64(blk))),
			data: permuteBlocks(base, blk, rng),
		})
	}
	return out
}

// permuteBlocks shuffles fixed-size blocks, preserving every byte but destroying
// their global order.
func permuteBlocks(base []byte, blk int, rng *rand.Rand) []byte {
	n := (len(base) + blk - 1) / blk
	order := rng.Perm(n)
	out := make([]byte, 0, len(base))
	for _, i := range order {
		start := i * blk
		end := start + blk
		if end > len(base) {
			end = len(base)
		}
		out = append(out, base[start:end]...)
	}
	return out
}

// verify reads the object back and compares it byte-for-byte.
func verify(ctx context.Context, cs *snapshot.ChunkedStore, key snapshot.SnapshotKey, want []byte) error {
	rc, _, err := cs.GetLatest(ctx, key)
	if err != nil {
		return err
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("round-trip mismatch: got %d bytes want %d", len(got), len(want))
	}
	return nil
}

// dirBytes sums the apparent size of every file under root — the physical cost of
// what has been stored so far.
func dirBytes(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func humanize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
