// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// countingStore embeds a real Storage and counts only the *body* writes (the
// fallback PutBlob without DoNotRecreate), so the test measures redundant
// uploads precisely — the DoNotRecreate probe that the local backend rejects
// doesn't store anything and isn't counted.
type countingStore struct {
	blobstore.Storage
	writes atomic.Int64
	delay  time.Duration
}

func (c *countingStore) PutBlob(ctx context.Context, id blobstore.ID, data blobstore.Bytes, opts blobstore.PutOptions) error {
	if !opts.DoNotRecreate {
		c.writes.Add(1)
		if c.delay > 0 {
			time.Sleep(c.delay) // widen the race window so concurrency overlaps
		}
	}
	return c.Storage.PutBlob(ctx, id, data, opts)
}

// TestInflightDeduperCoalesces is the #3 regression: concurrent PutIfAbsent of
// the same content collapse to a single underlying upload, where the raw path
// would upload once per racer.
func TestInflightDeduperCoalesces(t *testing.T) {
	ctx := context.Background()
	const n = 16
	id := blobstore.ID("chunk-t-deadbeef")
	payload := bytes.Repeat([]byte("x"), 4096)

	runConcurrent := func(put func() error) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _ = put() }()
		}
		wg.Wait()
	}

	// Raw path: each racer pays its own upload.
	raw := &countingStore{Storage: newTestStorage(t), delay: 25 * time.Millisecond}
	runConcurrent(func() error {
		return blobstore.PutIfAbsent(ctx, raw, id, blobstore.BytesFromSlice(payload))
	})
	if got := raw.writes.Load(); got < 2 {
		t.Fatalf("raw path: expected multiple redundant uploads under contention, got %d", got)
	}

	// Deduped path: the same workload costs exactly one upload.
	deduped := &countingStore{Storage: newTestStorage(t), delay: 25 * time.Millisecond}
	d := blobstore.NewInflightDeduper(deduped)
	runConcurrent(func() error {
		return d.PutIfAbsent(ctx, id, blobstore.BytesFromSlice(payload))
	})
	if got := deduped.writes.Load(); got != 1 {
		t.Fatalf("deduper: expected exactly 1 upload for %d concurrent identical puts, got %d", n, got)
	}

	// The stored bytes are correct, and a later put is a no-op (already present).
	if got := getBytes(t, ctx, deduped, id); !bytes.Equal(got, payload) {
		t.Fatalf("stored content mismatch: %d bytes", len(got))
	}
	if err := d.PutIfAbsent(ctx, id, blobstore.BytesFromSlice(payload)); err != nil {
		t.Fatalf("idempotent re-put: %v", err)
	}
	if got := deduped.writes.Load(); got != 1 {
		t.Fatalf("re-put of existing blob should not upload again, writes=%d", got)
	}
}

// TestInflightDeduperDistinctIDs confirms different blob IDs are NOT coalesced.
func TestInflightDeduperDistinctIDs(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{Storage: newTestStorage(t)}
	d := blobstore.NewInflightDeduper(cs)
	if err := d.PutIfAbsent(ctx, "chunk-t-aaaa", blobstore.BytesFromSlice([]byte("a"))); err != nil {
		t.Fatal(err)
	}
	if err := d.PutIfAbsent(ctx, "chunk-t-bbbb", blobstore.BytesFromSlice([]byte("b"))); err != nil {
		t.Fatal(err)
	}
	if got := cs.writes.Load(); got != 2 {
		t.Fatalf("distinct IDs should each upload once, got %d", got)
	}
}
