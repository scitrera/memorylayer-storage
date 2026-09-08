// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReadMetrics checks the hand-rolled Prometheus-text scanner: comments are
// skipped, single series are read, and fuse_ops is summed across its op labels.
func TestReadMetrics(t *testing.T) {
	body := `# HELP mlfs_fuse_ops_total FUSE operations served, by op
# TYPE mlfs_fuse_ops_total counter
mlfs_fuse_ops_total{op="read"} 10
mlfs_fuse_ops_total{op="write"} 5
mlfs_read_bytes_total 4096
mlfs_write_bytes_total 1024
mlfs_cache_hits_total 9
mlfs_cache_misses_total 1
mlfs_upload_ops_total 3
mlfs_upload_bytes_total 1.048576e+06
mlfs_staging_files 2
mlfs_staging_bytes 8192
`
	path := filepath.Join(t.TempDir(), "metrics")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := readMetrics(path)
	if err != nil {
		t.Fatalf("readMetrics: %v", err)
	}
	if s.fuseOps != 15 {
		t.Errorf("fuseOps = %v, want 15 (summed across op labels)", s.fuseOps)
	}
	if s.readBytes != 4096 || s.writeBytes != 1024 {
		t.Errorf("read/write bytes = %v/%v, want 4096/1024", s.readBytes, s.writeBytes)
	}
	if s.cacheHits != 9 || s.cacheMisses != 1 {
		t.Errorf("cache hits/misses = %v/%v, want 9/1", s.cacheHits, s.cacheMisses)
	}
	if s.uploadOps != 3 || s.uploadBytes != 1048576 {
		t.Errorf("upload ops/bytes = %v/%v, want 3/1048576", s.uploadOps, s.uploadBytes)
	}
	if s.stagingFile != 2 || s.stagingByte != 8192 {
		t.Errorf("staging files/bytes = %v/%v, want 2/8192", s.stagingFile, s.stagingByte)
	}
}

func TestReadMetricsMissingFile(t *testing.T) {
	if _, err := readMetrics(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("expected error for missing metrics file")
	}
}
