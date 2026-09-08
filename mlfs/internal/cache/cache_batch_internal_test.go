// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// inflightBatch is a chunkstore.Store + BatchWriter that records the max number
// of WriteBatch calls in flight at once, so the test can assert the batched
// drain honors UploadConcurrency.
type inflightBatch struct {
	mu          sync.Mutex
	m           map[uint64][]byte
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	delay       time.Duration
}

func newInflightBatch() *inflightBatch { return &inflightBatch{m: map[uint64][]byte{}} }

func (s *inflightBatch) put(id uint64, d []byte) {
	s.mu.Lock()
	s.m[id] = append([]byte(nil), d...)
	s.mu.Unlock()
}
func (s *inflightBatch) Write(_ context.Context, id uint64, d []byte, _ chunkstore.StoreClass) error {
	s.put(id, d)
	return nil
}
func (s *inflightBatch) Read(_ context.Context, id uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.m[id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), d...), nil
}
func (s *inflightBatch) ReadAt(ctx context.Context, id uint64, off int64, p []byte) (int, error) {
	d, err := s.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(d)) {
		return 0, nil
	}
	return copy(p, d[off:]), nil
}
func (s *inflightBatch) Exists(_ context.Context, id uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[id]
	return ok, nil
}
func (s *inflightBatch) Remove(_ context.Context, id uint64) error {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
	return nil
}
func (s *inflightBatch) WriteBatch(_ context.Context, items []chunkstore.BatchItem) error {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		cur := s.maxInFlight.Load()
		if n <= cur || s.maxInFlight.CompareAndSwap(cur, n) {
			break
		}
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	for _, it := range items {
		s.put(it.ID, it.Data)
	}
	return nil
}

// TestDrainBatchedConcurrencyBounded checks the batched drain uploads batches in
// parallel up to UploadConcurrency but no more, and that everything is durable.
func TestDrainBatchedConcurrencyBounded(t *testing.T) {
	ctx := context.Background()
	back := newInflightBatch()
	back.delay = 10 * time.Millisecond // hold each batch so overlap is observable
	c, err := New(back, Config{Dir: t.TempDir(), AutoUpload: false, UploadConcurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.batchBudget = 1 // force one batch per slice → many parallel batches

	const n = 16
	for id := uint64(0); id < n; id++ {
		if err := c.Write(ctx, id, make([]byte, 3000), chunkstore.ClassDefault); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}
	if err := c.Flush(ctx); err != nil { // -> drainBatched (concurrent)
		t.Fatalf("Flush: %v", err)
	}

	if mx := back.maxInFlight.Load(); mx < 2 {
		t.Errorf("batched concurrency not engaged: maxInFlight=%d, want ≥2", mx)
	} else if mx > 4 {
		t.Errorf("batched concurrency exceeded the bound: maxInFlight=%d, want ≤4", mx)
	}
	for id := uint64(0); id < n; id++ {
		if ok, _ := back.Exists(ctx, id); !ok {
			t.Errorf("slice %d not durable after batched drain", id)
		}
	}
}
