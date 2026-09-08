// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"os"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// fakeBacking is a minimal in-memory chunkstore.Store for the internal
// free-space test (which must set the unexported diskFree seam).
type fakeBacking struct{ m map[uint64][]byte }

func (f *fakeBacking) Write(_ context.Context, id uint64, d []byte, _ chunkstore.StoreClass) error {
	f.m[id] = append([]byte(nil), d...)
	return nil
}
func (f *fakeBacking) Read(_ context.Context, id uint64) ([]byte, error) {
	d, ok := f.m[id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), d...), nil
}
func (f *fakeBacking) ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error) {
	d, err := f.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(d)) {
		return 0, nil
	}
	return copy(p, d[off:]), nil
}
func (f *fakeBacking) Exists(_ context.Context, id uint64) (bool, error) {
	_, ok := f.m[id]
	return ok, nil
}
func (f *fakeBacking) Remove(_ context.Context, id uint64) error {
	delete(f.m, id)
	return nil
}

// TestMinFreeFractionEviction drives the clean cache past the MinFreeFraction
// disk-headroom floor with a simulated filesystem (diskFree returns
// total-other-curBytes, so evicting actually frees space) and asserts the LRU
// self-trims to maintain the floor — independent of any MaxBytes cap.
func TestMinFreeFractionEviction(t *testing.T) {
	ctx := context.Background()
	back := &fakeBacking{m: map[uint64][]byte{}}

	c, err := New(back, Config{Dir: t.TempDir(), MinFreeFraction: 0.20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	const total = int64(1_000_000)
	const other = int64(600_000) // staging/OS/other usage not ours to reclaim
	// avail shrinks as our clean cache grows; evicting our entries frees it.
	c.diskFree = func(string) (int64, int64, error) {
		return total - other - c.curBytes, total, nil
	}

	// Fill the clean cache via read-misses (each ~50 KiB).
	const n = 20
	const sz = 50_000
	for id := uint64(1); id <= n; id++ {
		back.m[id] = make([]byte, sz)
		if _, err := c.Read(ctx, id); err != nil {
			t.Fatalf("read %d: %v", id, err)
		}
	}

	// Floor wants ≥20% of 1 MB free; with 600 KB fixed other-usage that caps our
	// clean cache near 200 KB. It must have trimmed (not grown to ~1 MB) but not
	// emptied.
	want := int64(float64(total) * 0.20) // 200_000 free target
	avail := total - other - c.curBytes
	if avail < want-int64(sz)-checksumLen {
		t.Errorf("free-space floor not maintained: avail=%d want≥~%d (curBytes=%d)", avail, want, c.curBytes)
	}
	if c.curBytes == 0 {
		t.Errorf("cache fully evicted; expected it to retain entries up to the floor")
	}
	if c.curBytes > want+2*(sz+checksumLen) {
		t.Errorf("cache exceeded the free-space floor: curBytes=%d (target ~%d)", c.curBytes, want)
	}
}

// TestMinFreeFractionDisabled confirms the floor is off by default (no statfs
// eviction), so the cache is bounded only by MaxBytes/MaxItems.
func TestMinFreeFractionDisabled(t *testing.T) {
	ctx := context.Background()
	back := &fakeBacking{m: map[uint64][]byte{}}
	c, err := New(back, Config{Dir: t.TempDir()}) // MinFreeFraction default 0
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// A diskFree that always reports the disk as nearly full must be IGNORED when
	// the floor is disabled.
	c.diskFree = func(string) (int64, int64, error) { return 1, 1_000_000, nil }

	const n = 10
	const sz = 10_000
	for id := uint64(1); id <= n; id++ {
		back.m[id] = make([]byte, sz)
		if _, err := c.Read(ctx, id); err != nil {
			t.Fatalf("read %d: %v", id, err)
		}
	}
	if c.lru.Len() != n {
		t.Errorf("disabled floor evicted entries: lru.Len()=%d, want %d", c.lru.Len(), n)
	}
}
