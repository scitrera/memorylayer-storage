// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

// TestCasStore_ReadBatch checks the batch read maps slice ids ↔ casstore keys
// correctly, returns bytes identical to per-id Read, and omits missing ids.
func TestCasStore_ReadBatch(t *testing.T) {
	ctx := context.Background()
	l, err := chunkstore.NewLocal(t.TempDir(), "d1", 1<<20)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	t.Cleanup(func() { _ = l.Chunks.Close(ctx) })

	const n, sz = 12, 200 * 1024
	want := make(map[uint64][]byte, n)
	ids := make([]uint64, n)
	items := make([]chunkstore.BatchItem, n)
	for i := 0; i < n; i++ {
		b := make([]byte, sz)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		id := uint64(i + 1)
		ids[i], want[id] = id, b
		items[i] = chunkstore.BatchItem{ID: id, Data: b}
	}
	if err := l.Store.WriteBatch(ctx, items); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	got, _, err := l.Store.ReadBatch(ctx, ids, 4)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Errorf("slice %d: ReadBatch bytes (%d) != want (%d)", id, len(got[id]), len(w))
		}
		per, err := l.Store.Read(ctx, id)
		if err != nil {
			t.Fatalf("Read %d: %v", id, err)
		}
		if !bytes.Equal(got[id], per) {
			t.Errorf("slice %d: ReadBatch != per-id Read", id)
		}
	}

	// A missing id is absent from the result.
	g2, _, err := l.Store.ReadBatch(ctx, []uint64{99999, ids[0]}, 0)
	if err != nil {
		t.Fatalf("ReadBatch (missing): %v", err)
	}
	if _, ok := g2[99999]; ok {
		t.Errorf("missing id must be absent")
	}
	if !bytes.Equal(g2[ids[0]], want[ids[0]]) {
		t.Errorf("present id wrong in mixed batch")
	}

	// Empty input → empty (non-nil) map.
	if m, _, err := l.Store.ReadBatch(ctx, nil, 0); err != nil || m == nil || len(m) != 0 {
		t.Errorf("ReadBatch(nil) = (%v, %v), want (empty, nil)", m, err)
	}
}
