// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

func corruptLastByte(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("corruptLastByte: empty file")
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// classRecBacking is a chunkstore.Store + BatchWriter that records the StoreClass
// each slice arrived under, so a test can prove the class survived the staging
// file (crash-safe) and reached the backing through the drain.
type classRecBacking struct {
	mu      sync.Mutex
	data    map[uint64][]byte
	class   map[uint64]chunkstore.StoreClass
	batches int
	single  int // non-batch Write calls (drainOne path)
}

func newClassRecBacking() *classRecBacking {
	return &classRecBacking{data: map[uint64][]byte{}, class: map[uint64]chunkstore.StoreClass{}}
}

func (r *classRecBacking) record(id uint64, data []byte, class chunkstore.StoreClass) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[id] = append([]byte(nil), data...)
	r.class[id] = class
}

func (r *classRecBacking) Write(_ context.Context, id uint64, data []byte, class chunkstore.StoreClass) error {
	r.mu.Lock()
	r.single++
	r.mu.Unlock()
	r.record(id, data, class)
	return nil
}

func (r *classRecBacking) WriteBatch(_ context.Context, items []chunkstore.BatchItem) error {
	r.mu.Lock()
	r.batches++
	r.mu.Unlock()
	for _, it := range items {
		r.record(it.ID, it.Data, it.Class)
	}
	return nil
}

func (r *classRecBacking) Read(_ context.Context, id uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.data[id]...), nil
}
func (r *classRecBacking) ReadAt(_ context.Context, id uint64, off int64, p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.data[id]
	if off >= int64(len(d)) {
		return 0, nil
	}
	return copy(p, d[off:]), nil
}
func (r *classRecBacking) Exists(_ context.Context, id uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.data[id]
	return ok, nil
}
func (r *classRecBacking) Remove(_ context.Context, id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, id)
	delete(r.class, id)
	return nil
}

func (r *classRecBacking) classOf(id uint64) chunkstore.StoreClass {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.class[id]
}

// classRecPerSlice is classRecBacking WITHOUT BatchWriter, so the cache drains it
// via the per-slice drainOne path (exercises that path's class recovery too).
type classRecPerSlice struct{ *classRecBacking }

func (classRecPerSlice) WriteBatch() {} // shadow to ensure it does NOT satisfy BatchWriter

// TestCache_ClassSurvivesDrain proves a slice's StoreClass set at Write reaches
// the backing store through the (batched) drain — i.e. the class is persisted in
// the staging file and recovered on drain, not silently dropped to ClassDefault.
func TestCache_ClassSurvivesDrain(t *testing.T) {
	ctx := context.Background()
	rec := newClassRecBacking()
	c, err := New(rec, Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	data := bytes.Repeat([]byte("x"), 4096)
	if err := c.Write(ctx, 1, data, chunkstore.ClassUncompressed); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := c.Write(ctx, 2, data, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if rec.batches == 0 {
		t.Fatalf("expected the batched drain path to be used")
	}
	if got := rec.classOf(1); got != chunkstore.ClassUncompressed {
		t.Errorf("slice 1 reached backing as class %d, want ClassUncompressed", got)
	}
	if got := rec.classOf(2); got != chunkstore.ClassDefault {
		t.Errorf("slice 2 reached backing as class %d, want ClassDefault", got)
	}
}

// TestCache_ClassSurvivesCrashRecovery is the durability crux: a slice staged
// (fsync'd, acked) but NOT yet uploaded must, after a process restart with NO
// inode context, drain under its original class. We simulate the restart by
// constructing a SECOND cache over the same dirs — its New() recovery drain reads
// the class back out of the staging file.
func TestCache_ClassSurvivesCrashRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rec := newClassRecBacking()

	// First "process": stage under ClassUncompressed, never flush/upload.
	c1, err := New(rec, Config{Dir: dir, AutoUpload: false, UploadInterval: time.Hour})
	if err != nil {
		t.Fatalf("New c1: %v", err)
	}
	if err := c1.Write(ctx, 7, bytes.Repeat([]byte("m"), 2048), chunkstore.ClassUncompressed); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Crash: the staging file is on disk; rec has seen nothing yet.
	if _, ok := rec.class[7]; ok {
		t.Fatal("slice reached backing before any drain — test setup wrong")
	}

	// Second "process": recovery drain on New must recover the class from staging.
	c2, err := New(rec, Config{Dir: dir, AutoUpload: false})
	if err != nil {
		t.Fatalf("New c2 (recovery): %v", err)
	}
	defer c2.Close()
	if got := rec.classOf(7); got != chunkstore.ClassUncompressed {
		t.Errorf("recovered slice reached backing as class %d, want ClassUncompressed", got)
	}
}

// TestCache_ClassSurvivesPerSliceDrain covers the non-batch backing path
// (drainOne), where the class is recovered from staging and passed to
// backing.Write directly.
func TestCache_ClassSurvivesPerSliceDrain(t *testing.T) {
	ctx := context.Background()
	rec := newClassRecBacking()
	back := classRecPerSlice{rec}
	c, err := New(back, Config{Dir: t.TempDir(), AutoUpload: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	if err := c.Write(ctx, 9, bytes.Repeat([]byte("z"), 1024), chunkstore.ClassUncompressed); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if rec.single == 0 {
		t.Fatalf("expected the per-slice drainOne path to be used (BatchWriter must not be satisfied)")
	}
	if got := rec.classOf(9); got != chunkstore.ClassUncompressed {
		t.Errorf("slice 9 reached backing as class %d, want ClassUncompressed", got)
	}
}

// TestStagedFraming round-trips writeStaged → readStaged for both classes and
// confirms a legacy writeChecksummed file (no class byte) reads back as
// ClassDefault rather than being quarantined — so a staging entry pending across
// an in-place upgrade still drains.
func TestStagedFraming(t *testing.T) {
	dir := t.TempDir()
	data := []byte("hello staging world")

	for _, class := range []byte{byte(chunkstore.ClassDefault), byte(chunkstore.ClassUncompressed)} {
		p := filepath.Join(dir, "staged")
		if err := writeStaged(p, class, data); err != nil {
			t.Fatalf("writeStaged class %d: %v", class, err)
		}
		gotClass, gotData, ok, err := readStaged(p)
		if err != nil || !ok {
			t.Fatalf("readStaged class %d: ok=%v err=%v", class, ok, err)
		}
		if gotClass != class || !bytes.Equal(gotData, data) {
			t.Fatalf("round-trip class %d: got class %d data=%q", class, gotClass, gotData)
		}
	}

	// Legacy writeChecksummed file → ClassDefault, not corrupt.
	legacy := filepath.Join(dir, "legacy")
	if err := writeChecksummed(legacy, data); err != nil {
		t.Fatalf("writeChecksummed: %v", err)
	}
	gotClass, gotData, ok, err := readStaged(legacy)
	if err != nil || !ok {
		t.Fatalf("readStaged(legacy): ok=%v err=%v", ok, err)
	}
	if gotClass != byte(chunkstore.ClassDefault) || !bytes.Equal(gotData, data) {
		t.Errorf("legacy staging: got class %d data=%q, want ClassDefault + original", gotClass, gotData)
	}

	// A genuinely corrupt file (random bytes) is reported corrupt.
	bad := filepath.Join(dir, "bad")
	if err := writeChecksummed(bad, data); err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the body to break both the staged- and legacy-format checksums.
	corruptLastByte(t, bad)
	if _, _, _, err := readStaged(bad); !errors.Is(err, errCorrupt) {
		t.Errorf("corrupt staging: err=%v, want errCorrupt", err)
	}
}
