// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package bench is a POSIX filesystem benchmark for an mlfs mount. It runs only
// standard file operations against a target directory, so it measures the mount
// exactly as a workload sees it — no metadata-DB or chunk-store coupling — which
// lets it ship in a small standalone binary deployable onto any pod that mounts
// mlfs storage. It exercises the two profiles that dominate real use: one large
// sequential file (write-back + read-back throughput) and many small files
// (create/stat/read latency, i.e. the metadata path).
package bench

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/fsstat"
)

// Config controls a benchmark run. Bind it to a flag set with Flags.
type Config struct {
	Dir        string        // parent directory (under the mount); a temp work dir is created here
	BigSize    int64         // total bytes for the large-file phase
	BlockSize  int           // I/O block size for the large-file phase
	SmallFiles int           // number of small files for the metadata phase
	SmallSize  int           // bytes per small file
	Fsync      bool          // fsync after each write (measure the durable path)
	DropCache  bool          // fadvise DONTNEED before the read phase (force a cold read)
	Keep       bool          // keep the work dir instead of removing it
	Timeout    time.Duration // overall deadline (0 = none)
}

// Flags registers the benchmark flags on fs and returns a Config populated after
// fs.Parse runs. Shared so every binary exposes an identical surface.
func Flags(fs *flag.FlagSet) *Config {
	c := &Config{}
	fs.StringVar(&c.Dir, "dir", "", "target directory under a mounted mlfs (required); a temp work dir is created here")
	fs.Int64Var(&c.BigSize, "big-size", 1<<30, "large-file phase: total bytes written then read back")
	fs.IntVar(&c.BlockSize, "block-size", 1<<20, "large-file phase: I/O block size")
	fs.IntVar(&c.SmallFiles, "small-files", 200, "metadata phase: number of small files")
	fs.IntVar(&c.SmallSize, "small-size", 4<<10, "metadata phase: bytes per small file")
	fs.BoolVar(&c.Fsync, "fsync", true, "fsync after each write (measure the durable path; false = page-cache speed)")
	fs.BoolVar(&c.DropCache, "drop-cache", true, "drop page cache before the read phase so reads are cold (fadvise DONTNEED)")
	fs.BoolVar(&c.Keep, "keep", false, "keep the work directory instead of deleting it")
	fs.DurationVar(&c.Timeout, "timeout", 0, "overall deadline (0 = none)")
	return c
}

// Result holds the measured throughput and rates of one run.
type Result struct {
	WorkDir string `json:"work_dir"`

	BigBytes     int64   `json:"big_bytes"`
	WriteSeconds float64 `json:"write_seconds"`
	ReadSeconds  float64 `json:"read_seconds"`
	WriteMiBps   float64 `json:"write_mibps"`
	ReadMiBps    float64 `json:"read_mibps"`

	SmallFiles    int     `json:"small_files"`
	SmallSize     int     `json:"small_size"`
	CreateSeconds float64 `json:"create_seconds"`
	StatSeconds   float64 `json:"stat_seconds"`
	ReadSeconds2  float64 `json:"small_read_seconds"`
	CreateIOPS    float64 `json:"create_iops"`
	StatIOPS      float64 `json:"stat_iops"`
	ReadIOPS      float64 `json:"small_read_iops"`

	Fsync bool `json:"fsync"`
}

const mib = 1 << 20

// Run executes the benchmark and returns its measurements. It creates (and,
// unless Keep, removes) a temp work directory under cfg.Dir.
func Run(ctx context.Context, cfg Config) (Result, error) {
	var res Result
	if cfg.Dir == "" {
		return res, fmt.Errorf("bench: -dir is required (a directory under a mounted mlfs)")
	}
	if cfg.BigSize < 0 || cfg.SmallSize < 0 {
		return res, fmt.Errorf("bench: sizes must be non-negative")
	}
	if cfg.BigSize > 0 && cfg.BlockSize <= 0 {
		return res, fmt.Errorf("bench: block-size must be > 0 for the large-file phase")
	}
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	work, err := os.MkdirTemp(cfg.Dir, "mlfs-bench-")
	if err != nil {
		return res, fmt.Errorf("bench: create work dir under %s: %w", cfg.Dir, err)
	}
	res.WorkDir = work
	if !cfg.Keep {
		defer os.RemoveAll(work)
	}

	res.Fsync = cfg.Fsync
	res.BigBytes = cfg.BigSize
	res.SmallFiles = cfg.SmallFiles
	res.SmallSize = cfg.SmallSize

	if cfg.BigSize > 0 {
		if err := runBigPhase(ctx, &res, cfg, work); err != nil {
			return res, err
		}
	}
	if cfg.SmallFiles > 0 {
		if err := runSmallPhase(ctx, &res, cfg, work); err != nil {
			return res, err
		}
	}
	return res, nil
}

// runBigPhase writes one large file sequentially, then reads it back. Each block
// carries its index in the first bytes so no two blocks are identical — otherwise
// content-addressed dedup would collapse the writes and inflate throughput.
func runBigPhase(ctx context.Context, res *Result, cfg Config, work string) error {
	path := filepath.Join(work, "big.dat")
	buf := make([]byte, cfg.BlockSize)
	for i := range buf {
		buf[i] = byte(i * 31)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("bench: create big file: %w", err)
	}
	start := time.Now()
	var written int64
	for block := 0; written < cfg.BigSize; block++ {
		if err := ctx.Err(); err != nil {
			f.Close()
			return err
		}
		stampBlock(buf, block)
		n := cfg.BlockSize
		if rem := cfg.BigSize - written; rem < int64(n) {
			n = int(rem)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			f.Close()
			return fmt.Errorf("bench: write big file: %w", err)
		}
		written += int64(n)
	}
	if cfg.Fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("bench: fsync big file: %w", err)
		}
	}
	if cfg.DropCache {
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("bench: close big file: %w", err)
	}
	res.WriteSeconds = time.Since(start).Seconds()
	res.WriteMiBps = mibps(written, res.WriteSeconds)

	rf, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("bench: open big file for read: %w", err)
	}
	if cfg.DropCache {
		_ = unix.Fadvise(int(rf.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
	start = time.Now()
	read, err := io.CopyBuffer(io.Discard, rf, buf)
	rf.Close()
	if err != nil {
		return fmt.Errorf("bench: read big file: %w", err)
	}
	res.ReadSeconds = time.Since(start).Seconds()
	res.ReadMiBps = mibps(read, res.ReadSeconds)
	return nil
}

// runSmallPhase creates, stats, then reads many small files — the metadata path
// (one slice + node row per file), which the large-file phase does not exercise.
func runSmallPhase(ctx context.Context, res *Result, cfg Config, work string) error {
	dir := filepath.Join(work, "small")
	if err := os.Mkdir(dir, 0o755); err != nil {
		return fmt.Errorf("bench: mkdir small: %w", err)
	}
	payload := make([]byte, cfg.SmallSize)
	for i := range payload {
		payload[i] = byte(i*7 + 1)
	}
	paths := make([]string, cfg.SmallFiles)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("f-%06d", i))
	}

	start := time.Now()
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := os.Create(p)
		if err != nil {
			return fmt.Errorf("bench: create small file: %w", err)
		}
		if _, err := f.Write(payload); err != nil {
			f.Close()
			return fmt.Errorf("bench: write small file: %w", err)
		}
		if cfg.Fsync {
			if err := f.Sync(); err != nil {
				f.Close()
				return fmt.Errorf("bench: fsync small file: %w", err)
			}
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("bench: close small file: %w", err)
		}
	}
	res.CreateSeconds = time.Since(start).Seconds()
	res.CreateIOPS = iops(len(paths), res.CreateSeconds)

	start = time.Now()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("bench: stat small file: %w", err)
		}
	}
	res.StatSeconds = time.Since(start).Seconds()
	res.StatIOPS = iops(len(paths), res.StatSeconds)

	start = time.Now()
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("bench: read small file: %w", err)
		}
		if len(b) != cfg.SmallSize {
			return fmt.Errorf("bench: short read %s: %d != %d", p, len(b), cfg.SmallSize)
		}
	}
	res.ReadSeconds2 = time.Since(start).Seconds()
	res.ReadIOPS = iops(len(paths), res.ReadSeconds2)
	return nil
}

// stampBlock writes the block index into the first 8 bytes so each block differs.
func stampBlock(buf []byte, block int) {
	for i := 0; i < 8 && i < len(buf); i++ {
		buf[i] = byte(block >> (8 * i))
	}
}

func mibps(bytes int64, secs float64) float64 {
	if secs <= 0 {
		return 0
	}
	return float64(bytes) / mib / secs
}

func iops(n int, secs float64) float64 {
	if secs <= 0 {
		return 0
	}
	return float64(n) / secs
}

// Print renders the result as an aligned, human-readable report to w.
func (r Result) Print(w io.Writer) {
	dur := func(s float64) string {
		return time.Duration(s * float64(time.Second)).Round(time.Millisecond).String()
	}
	fmt.Fprintf(w, "mlfs bench  (work dir: %s, fsync=%t)\n", r.WorkDir, r.Fsync)
	if r.BigBytes > 0 {
		fmt.Fprintf(w, "  large file (%s)\n", fsstat.HumanBytes(r.BigBytes))
		fmt.Fprintf(w, "    write: %8.1f MiB/s  (%s)\n", r.WriteMiBps, dur(r.WriteSeconds))
		fmt.Fprintf(w, "    read:  %8.1f MiB/s  (%s)\n", r.ReadMiBps, dur(r.ReadSeconds))
	}
	if r.SmallFiles > 0 {
		fmt.Fprintf(w, "  small files (%d × %s)\n", r.SmallFiles, fsstat.HumanBytes(int64(r.SmallSize)))
		fmt.Fprintf(w, "    create: %8.0f files/s  (%s)\n", r.CreateIOPS, dur(r.CreateSeconds))
		fmt.Fprintf(w, "    stat:   %8.0f files/s  (%s)\n", r.StatIOPS, dur(r.StatSeconds))
		fmt.Fprintf(w, "    read:   %8.0f files/s  (%s)\n", r.ReadIOPS, dur(r.ReadSeconds2))
	}
}
