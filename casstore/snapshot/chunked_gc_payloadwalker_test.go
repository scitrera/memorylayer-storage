// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
)

// getCountingStore wraps a SnapshotStore and counts Get calls made THROUGH it, so
// a test can see whether the GC's live-set phase issued a per-manifest Get.
type getCountingStore struct {
	SnapshotStore
	gets atomic.Int64
}

func (s *getCountingStore) Get(ctx context.Context, key SnapshotKey, version string) (io.ReadCloser, SnapshotMetadata, error) {
	s.gets.Add(1)
	return s.SnapshotStore.Get(ctx, key, version)
}

// pwCountingStore is a getCountingStore that ALSO implements PayloadWalker — its
// WalkPayloads reads payloads via the embedded (uncounted) Get, so a GC that uses
// the streaming path issues ZERO counted Gets.
type pwCountingStore struct {
	getCountingStore
}

func (s *pwCountingStore) WalkPayloads(ctx context.Context, yield func(SnapshotKey, string, SnapshotMetadata, []byte) error) error {
	return s.SnapshotStore.Walk(ctx, func(key SnapshotKey, version string, meta SnapshotMetadata) error {
		rc, _, err := s.SnapshotStore.Get(ctx, key, version) // embedded → not counted
		if err != nil {
			return err
		}
		data, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			return rerr
		}
		return yield(key, version, meta, data)
	})
}

// TestChunkedGC_PayloadWalker proves the GC uses the streaming-payload walk when
// the manifest store supports it — issuing ZERO per-manifest Gets — and reclaims
// exactly the same orphans as the per-manifest-Get fallback.
func TestChunkedGC_PayloadWalker(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, usePW bool) (reclaimed, gets int64, remaining int) {
		t.Helper()
		cs, upstream, chunks := chunkedTestStack(t)
		// Write 4 objects; delete 2 of them so their blobs are orphaned.
		keys := make([]SnapshotKey, 4)
		vers := make([]string, 4)
		for i := 0; i < 4; i++ {
			k := SnapshotKey{Tenant: "t1", OwnerKey: string(rune('a' + i))}
			keys[i] = k
			payload := bytes.Repeat([]byte{byte('A' + i)}, 300_000)
			md, err := cs.Put(ctx, k, SnapshotMetadata{}, bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
			vers[i] = md.Version
		}
		for _, i := range []int{1, 3} { // orphan 2 of the 4
			if err := cs.Delete(ctx, keys[i], vers[i]); err != nil {
				t.Fatalf("Delete %d: %v", i, err)
			}
		}

		var manifests SnapshotStore
		var counter *getCountingStore
		if usePW {
			pw := &pwCountingStore{getCountingStore: getCountingStore{SnapshotStore: upstream}}
			manifests, counter = pw, &pw.getCountingStore
		} else {
			gc := &getCountingStore{SnapshotStore: upstream}
			manifests, counter = gc, gc
		}

		gc := NewChunkedGC(manifests, chunks, nil)
		res, err := gc.RunOnce(ctx)
		if err != nil {
			t.Fatalf("GC.RunOnce(usePW=%v): %v", usePW, err)
		}
		rem, _ := countTenantBlobs(ctx, chunks, "t1")
		return int64(res.ChunksReclaimed), counter.gets.Load(), rem
	}

	pwReclaimed, pwGets, pwRemaining := run(t, true)
	fbReclaimed, fbGets, fbRemaining := run(t, false)

	// Same reclaim outcome via both paths.
	if pwReclaimed != fbReclaimed || pwRemaining != fbRemaining {
		t.Errorf("PayloadWalker vs fallback diverged: reclaimed %d/%d, remaining %d/%d",
			pwReclaimed, fbReclaimed, pwRemaining, fbRemaining)
	}
	if pwReclaimed == 0 {
		t.Errorf("expected some orphan blobs reclaimed, got 0")
	}
	// PayloadWalker path issues NO per-manifest Get; the fallback issues one per
	// (surviving) chunked manifest.
	if pwGets != 0 {
		t.Errorf("PayloadWalker GC issued %d per-manifest Gets, want 0", pwGets)
	}
	if fbGets == 0 {
		t.Errorf("fallback GC issued 0 Gets — expected one per manifest")
	}
	t.Logf("reclaimed=%d remaining=%d; gets pw=%d fallback=%d", pwReclaimed, pwRemaining, pwGets, fbGets)
}
