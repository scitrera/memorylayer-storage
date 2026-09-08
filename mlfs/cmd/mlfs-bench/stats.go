// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/fsstat"
)

// runStats is the live, dstat-style feed. It polls the daemon's virtual metrics
// file (<mount>/.mlfs/metrics, Prometheus text) every interval and prints the
// per-second deltas. Reading a file inside the mount is all it needs — no DB, no
// node access, no network — so it works from any pod that mounts mlfs. The
// Prometheus text is parsed by a tiny line scanner rather than a full client
// library, keeping this probe binary small.
func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	mount := fs.String("path", "", "a mounted mlfs root (required); reads <path>/.mlfs/metrics")
	interval := fs.Duration("interval", time.Second, "sampling interval")
	count := fs.Int("count", 0, "number of samples to print (0 = until interrupted)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *mount == "" {
		return fmt.Errorf("-path is required (a mounted mlfs root)")
	}
	metricsPath := filepath.Join(*mount, ".mlfs", "metrics")
	if _, err := readMetrics(metricsPath); err != nil {
		return fmt.Errorf("read %s: %w (is this an mlfs mount with -metrics enabled?)", metricsPath, err)
	}

	prev, err := readMetrics(metricsPath)
	if err != nil {
		return err
	}
	prevAt := time.Now()
	tick := time.NewTicker(*interval)
	defer tick.Stop()

	const header = "    time     fuse/s    read/s   write/s   cache%   upload/s    up-bytes/s   staging"
	printed := 0
	for now := range tick.C {
		cur, err := readMetrics(metricsPath)
		if err != nil {
			return err
		}
		if printed%20 == 0 {
			fmt.Println(header)
		}
		secs := now.Sub(prevAt).Seconds()
		printStatsRow(now, secs, prev, cur)
		prev, prevAt = cur, now
		printed++
		if *count > 0 && printed >= *count {
			return nil
		}
	}
	return nil
}

// sample is the subset of counters/gauges the feed renders.
type sample struct {
	fuseOps     float64
	readBytes   float64
	writeBytes  float64
	cacheHits   float64
	cacheMisses float64
	uploadOps   float64
	uploadBytes float64
	stagingFile float64
	stagingByte float64
}

// readMetrics parses the Prometheus-text virtual file into a sample. fuse_ops is
// summed across its per-op label variants; the rest are single series.
func readMetrics(path string) (sample, error) {
	var s sample
	raw, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		val, err := strconv.ParseFloat(strings.TrimSpace(line[sp+1:]), 64)
		if err != nil {
			continue
		}
		name := line[:sp]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i] // drop labels; fuse_ops variants collapse to one base
		}
		switch name {
		case "mlfs_fuse_ops_total":
			s.fuseOps += val
		case "mlfs_read_bytes_total":
			s.readBytes = val
		case "mlfs_write_bytes_total":
			s.writeBytes = val
		case "mlfs_cache_hits_total":
			s.cacheHits = val
		case "mlfs_cache_misses_total":
			s.cacheMisses = val
		case "mlfs_upload_ops_total":
			s.uploadOps = val
		case "mlfs_upload_bytes_total":
			s.uploadBytes = val
		case "mlfs_staging_files":
			s.stagingFile = val
		case "mlfs_staging_bytes":
			s.stagingByte = val
		}
	}
	return s, nil
}

func printStatsRow(now time.Time, secs float64, prev, cur sample) {
	if secs <= 0 {
		secs = 1
	}
	rate := func(a, b float64) float64 { return (b - a) / secs }
	hits := cur.cacheHits - prev.cacheHits
	misses := cur.cacheMisses - prev.cacheMisses
	cachePct := "    —"
	if hits+misses > 0 {
		cachePct = fmt.Sprintf("%4.0f%%", 100*hits/(hits+misses))
	}
	perSec := func(n int64) string { return fsstat.HumanBytes(n) + "/s" }
	fmt.Printf("%s  %8.0f  %10s  %10s  %6s  %8.0f  %12s   %d/%s\n",
		now.Format("15:04:05"),
		rate(prev.fuseOps, cur.fuseOps),
		perSec(int64(rate(prev.readBytes, cur.readBytes))),
		perSec(int64(rate(prev.writeBytes, cur.writeBytes))),
		cachePct,
		rate(prev.uploadOps, cur.uploadOps),
		perSec(int64(rate(prev.uploadBytes, cur.uploadBytes))),
		int64(cur.stagingFile), fsstat.HumanBytes(int64(cur.stagingByte)),
	)
}
