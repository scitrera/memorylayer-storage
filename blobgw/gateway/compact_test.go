// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// TestTenantRouter_CompactConsolidates exercises the full blobgw compaction path:
// many small per-object packs are coalesced via CompactorForTenant → Compact,
// the orphaned old packs are reclaimed by GCForTenant, and every object still
// reads back byte-identical through the gateway.
func TestTenantRouter_CompactConsolidates(t *testing.T) {
	ltr, err := NewLocalTenantRouter(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewLocalTenantRouter: %v", err)
	}
	ctx := context.Background()
	gw, err := ltr.Router.For("acme")
	if err != nil {
		t.Fatalf("Router.For: %v", err)
	}

	countPacks := func() int {
		n := 0
		if err := ltr.Chunks.ListBlobs(ctx, blobstore.ID("pack-acme-"), func(blobstore.Metadata) error {
			n++
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
		return n
	}

	const n = 12
	want := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 100*1024) // 100 KiB, random ⇒ incompressible ⇒ one ~100 KiB pack each
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		ref := fmt.Sprintf("obj-%d", i)
		want[ref] = b
		if _, err := gw.Put(ctx, ref, "application/octet-stream", bytes.NewReader(b)); err != nil {
			t.Fatalf("Put %s: %v", ref, err)
		}
	}
	before := countPacks()
	if before < n {
		t.Fatalf("expected ≥%d packs before compaction, got %d", n, before)
	}

	// Compact: the ~100 KiB packs are small (< 1 MiB), so they coalesce.
	cs, err := ltr.Router.CompactorForTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("CompactorForTenant: %v", err)
	}
	res, err := cs.Compact(ctx, "acme", snapshot.CompactConfig{MinPackBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.PacksSelected != n || res.ManifestsRewritten != n {
		t.Errorf("Compact selected=%d rewritten=%d, want %d each", res.PacksSelected, res.ManifestsRewritten, n)
	}
	if res.PacksWritten >= n {
		t.Errorf("no consolidation: wrote %d packs for %d objects", res.PacksWritten, n)
	}

	// GC reclaims the now-orphaned old packs (safety window 0 for the test).
	gc, err := ltr.Router.GCForTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("GCForTenant: %v", err)
	}
	gc.SafetyWindow = 0
	if _, err := gc.RunOnceForDomain(ctx, "acme"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	after := countPacks()
	if after >= before {
		t.Errorf("compaction+GC did not reduce packs: before=%d after=%d", before, after)
	}
	if after != res.PacksWritten {
		t.Errorf("after GC: %d packs, want the %d consolidated", after, res.PacksWritten)
	}

	// Every object still reads back byte-identical (from the consolidated packs).
	for ref, payload := range want {
		rc, _, err := gw.Get(ctx, ref)
		if err != nil {
			t.Fatalf("Get %s: %v", ref, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, payload) {
			t.Errorf("object %s mismatch after compaction (%d vs %d bytes)", ref, len(got), len(payload))
		}
	}
}
