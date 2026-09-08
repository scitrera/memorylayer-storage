// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// compressStack builds a Gateway whose ChunkedStore applies the content-type
// compression policy (zstd), so PUTs are compressed per the object's
// Content-Type. Returns the gateway and its chunk blobstore for measuring
// physical bytes.
func compressStack(t *testing.T) (*Gateway, blobstore.Storage) {
	t.Helper()
	ctx := context.Background()
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewLocalStorage: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		DedupDomain:       testDomain,
		Index:             snapshot.NewGlobalIndex(snapshot.NewMemoryDedupStore(), nil),
		CompressionPolicy: snapshot.NewContentTypePolicy(snapshot.CompressZstd),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	return New(cs, NewMemoryRefStore(), NewMemoryStagingStore(), testDomain), chunks
}

func physicalBytes(t *testing.T, chunks blobstore.Storage) int64 {
	t.Helper()
	var total int64
	for _, p := range []blobstore.ID{blobstore.ID("pack-" + testDomain + "-"), blobstore.ID("chunk-" + testDomain + "-")} {
		if err := chunks.ListBlobs(context.Background(), p, func(md blobstore.Metadata) error {
			total += md.Length
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs: %v", err)
		}
	}
	return total
}

func getBody(t *testing.T, gw *Gateway, ref string) []byte {
	t.Helper()
	rc, _, err := gw.Get(context.Background(), ref)
	if err != nil {
		t.Fatalf("Get %q: %v", ref, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll %q: %v", ref, err)
	}
	return b
}

// TestGateway_CompressesTextContent verifies a text/plain PUT is physically
// compressed by the gateway's policy and round-trips byte-identically.
func TestGateway_CompressesTextContent(t *testing.T) {
	gw, chunks := compressStack(t)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("compress me: the quick brown fox. "), 200000) // ~6.6 MiB
	if _, err := gw.Put(ctx, "doc.txt", "text/plain", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := getBody(t, gw, "doc.txt"); !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch")
	}
	phys := physicalBytes(t, chunks)
	if phys >= int64(len(payload)) {
		t.Fatalf("text content was not compressed: physical=%d input=%d", phys, len(payload))
	}
	t.Logf("text/plain via gateway: %d input -> %d physical bytes", len(payload), phys)
}

// TestTenantRouter_CompressesAndDedupsViaRouterPath exercises the router path
// (as opposed to a hand-built Gateway) end-to-end: two tenants each store the
// same highly-compressible text payload, and the test asserts that:
//
//  1. Physical bytes are smaller than the original (compression is active).
//  2. The same payload stored twice under the same tenant dedups to one
//     physical copy (blob count does not grow on the second Put).
//  3. Both objects round-trip byte-identically via Get.
//
// This mirrors TestGateway_CompressesTextContent but goes through the
// TenantRouter so it covers the For() → ChunkedConfig.CompressionPolicy path
// that was previously missing (tech-debt #40).
func TestTenantRouter_CompressesAndDedupsViaRouterPath(t *testing.T) {
	ltr, err := NewLocalTenantRouter(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewLocalTenantRouter: %v", err)
	}
	ctx := context.Background()

	payload := bytes.Repeat([]byte("compress me: the quick brown fox. "), 200000) // ~6.6 MiB

	// --- Compression assertion for tenant "alpha" ---
	gwA, err := ltr.Router.For("alpha")
	if err != nil {
		t.Fatalf("Router.For alpha: %v", err)
	}
	if _, err := gwA.Put(ctx, "doc.txt", "text/plain", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put alpha/doc.txt: %v", err)
	}

	// physicalBytes scoped to the given domain prefix.
	domainPhysical := func(domain string) int64 {
		var total int64
		for _, pfx := range []blobstore.ID{
			blobstore.ID("pack-" + domain + "-"),
			blobstore.ID("chunk-" + domain + "-"),
		} {
			if err := ltr.Chunks.ListBlobs(ctx, pfx, func(md blobstore.Metadata) error {
				total += md.Length
				return nil
			}); err != nil {
				t.Fatalf("ListBlobs %q: %v", pfx, err)
			}
		}
		return total
	}

	physAfterFirst := domainPhysical("alpha")
	if physAfterFirst == 0 {
		t.Fatal("expected physical blobs after first put")
	}
	if physAfterFirst >= int64(len(payload)) {
		t.Fatalf("text content was not compressed via router path: physical=%d input=%d",
			physAfterFirst, len(payload))
	}
	t.Logf("router path text/plain: %d input -> %d physical bytes (alpha)", len(payload), physAfterFirst)

	// --- Dedup assertion: same payload again under a distinct ref, same tenant ---
	if _, err := gwA.Put(ctx, "doc2.txt", "text/plain", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put alpha/doc2.txt: %v", err)
	}
	physAfterSecond := domainPhysical("alpha")
	if physAfterSecond != physAfterFirst {
		t.Errorf("dedup failed via router path: blob bytes grew %d -> %d on identical content",
			physAfterFirst, physAfterSecond)
	}

	// --- Round-trip assertion ---
	if got := getBody(t, gwA, "doc.txt"); !bytes.Equal(got, payload) {
		t.Fatal("router path round-trip mismatch for doc.txt")
	}
	if got := getBody(t, gwA, "doc2.txt"); !bytes.Equal(got, payload) {
		t.Fatal("router path round-trip mismatch for doc2.txt")
	}

	// --- Cross-tenant isolation: tenant "beta" gets its own compressed domain ---
	gwB, err := ltr.Router.For("beta")
	if err != nil {
		t.Fatalf("Router.For beta: %v", err)
	}
	if _, err := gwB.Put(ctx, "doc.txt", "text/plain", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put beta/doc.txt: %v", err)
	}
	physBeta := domainPhysical("beta")
	if physBeta == 0 {
		t.Fatal("expected physical blobs for tenant beta")
	}
	if physBeta >= int64(len(payload)) {
		t.Fatalf("beta content was not compressed: physical=%d input=%d", physBeta, len(payload))
	}
	if got := getBody(t, gwB, "doc.txt"); !bytes.Equal(got, payload) {
		t.Fatal("router path round-trip mismatch for beta/doc.txt")
	}
}

// TestGateway_StoresGPULoadableUncompressed verifies a GPU-loadable tensor
// content type is stored uncompressed (so zero-copy/mmap device loads are not
// broken) and still round-trips. Uses highly compressible content so that
// accidental compression would be obvious in the physical size.
func TestGateway_StoresGPULoadableUncompressed(t *testing.T) {
	gw, chunks := compressStack(t)
	ctx := context.Background()

	payload := bytes.Repeat([]byte{0x00}, 4<<20) // 4 MiB zeros — would shrink ~1000x if compressed
	if _, err := gw.Put(ctx, "model.safetensors", "application/x-safetensors", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := getBody(t, gw, "model.safetensors"); !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch")
	}
	phys := physicalBytes(t, chunks)
	if phys < int64(len(payload)) {
		t.Fatalf("GPU-loadable type was compressed (physical=%d < input=%d) — breaks zero-copy load", phys, len(payload))
	}
}
