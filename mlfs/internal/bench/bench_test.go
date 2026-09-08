// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package bench

import (
	"context"
	"os"
	"testing"
)

// TestRun exercises both phases against a local temp dir (the benchmark is
// filesystem-agnostic, so a plain tmpfs/ext4 dir is a valid target) and checks
// every measurement is populated and the work dir is cleaned up.
func TestRun(t *testing.T) {
	cfg := Config{
		Dir:        t.TempDir(),
		BigSize:    1 << 20, // 1 MiB — small but multi-block at 64 KiB
		BlockSize:  64 << 10,
		SmallFiles: 16,
		SmallSize:  512,
		Fsync:      false, // keep the unit test fast; the durable path is covered by the flag
		DropCache:  true,
	}
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.WriteMiBps <= 0 || res.ReadMiBps <= 0 {
		t.Errorf("big-file throughput not measured: write=%.2f read=%.2f", res.WriteMiBps, res.ReadMiBps)
	}
	if res.BigBytes != cfg.BigSize {
		t.Errorf("BigBytes = %d, want %d", res.BigBytes, cfg.BigSize)
	}
	if res.CreateIOPS <= 0 || res.StatIOPS <= 0 || res.ReadIOPS <= 0 {
		t.Errorf("small-file rates not measured: create=%.0f stat=%.0f read=%.0f",
			res.CreateIOPS, res.StatIOPS, res.ReadIOPS)
	}
	if _, err := os.Stat(res.WorkDir); !os.IsNotExist(err) {
		t.Errorf("work dir not cleaned up: %s (err=%v)", res.WorkDir, err)
	}
}

func TestRun_Keep(t *testing.T) {
	cfg := Config{Dir: t.TempDir(), BigSize: 0, SmallFiles: 4, SmallSize: 128, Keep: true}
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(res.WorkDir); err != nil {
		t.Errorf("work dir should be kept: %v", err)
	}
	os.RemoveAll(res.WorkDir)
}

func TestRun_RequiresDir(t *testing.T) {
	if _, err := Run(context.Background(), Config{}); err == nil {
		t.Fatal("expected error when -dir is empty")
	}
}

// TestRun_BigBlocksAreUnique guards the stamping that defeats dedup: two adjacent
// blocks must not be byte-identical, or content-addressed stores would collapse
// them and the write number would be meaningless.
func TestRun_BigBlocksAreUnique(t *testing.T) {
	buf := make([]byte, 32)
	stampBlock(buf, 1)
	a := append([]byte(nil), buf...)
	stampBlock(buf, 2)
	if string(a) == string(buf) {
		t.Fatal("stampBlock produced identical blocks for different indices")
	}
}
