// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

var errNop = errors.New("nopBacking: not present")

// nopBacking is a chunkstore.Store that drops writes — enough to construct a
// DiskCache for the staging-age unit test (which never drains). It deliberately
// does NOT implement chunkstore.BatchWriter, so New()'s recovery drain has
// nothing to batch.
type nopBacking struct{}

func (nopBacking) Write(_ context.Context, _ uint64, _ []byte, _ chunkstore.StoreClass) error {
	return nil
}
func (nopBacking) Read(_ context.Context, _ uint64) ([]byte, error) { return nil, errNop }
func (nopBacking) ReadAt(_ context.Context, _ uint64, _ int64, _ []byte) (int, error) {
	return 0, errNop
}
func (nopBacking) Exists(_ context.Context, _ uint64) (bool, error) { return false, nil }
func (nopBacking) Remove(_ context.Context, _ uint64) error         { return nil }

// TestStagingOldestAge verifies the write-back oldest-unflushed-age signal (the
// GC safety-window canary): 0 when staging is empty, and the age of the OLDEST
// staged file (not the newest) once writes are pending. AutoUpload is off so the
// staged entries persist for the assertion.
func TestStagingOldestAge(t *testing.T) {
	c, err := New(nopBacking{}, Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// Empty staging → 0.
	if age := c.stagingOldestAge(); age != 0 {
		t.Fatalf("empty staging age = %v, want 0", age)
	}

	// Stage two writes; back-date the oldest staging file's mtime by 90s so the
	// gauge must report the oldest, not the newest.
	if err := c.stage(1, []byte("oldest"), chunkstore.ClassDefault); err != nil {
		t.Fatalf("stage 1: %v", err)
	}
	if err := c.stage(2, []byte("newer"), chunkstore.ClassDefault); err != nil {
		t.Fatalf("stage 2: %v", err)
	}
	base := time.Now()
	old := base.Add(-90 * time.Second)
	if err := os.Chtimes(c.stagingPath(1), old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	// Pin the clock so the age is deterministic.
	c.now = func() time.Time { return base }

	age := c.stagingOldestAge()
	if age < 89 || age > 91 {
		t.Errorf("oldest-unflushed age = %v, want ~90s (oldest staged file)", age)
	}
}
