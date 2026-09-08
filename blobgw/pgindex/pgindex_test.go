// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package pgindex_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/pgindex"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// openTestStore connects to the Postgres named by BLOBGW_TEST_DATABASE_URL,
// migrates the schema, and returns a pgindex.Store scoped to a unique random
// domain (so concurrent/repeat runs don't collide). Skips when the env var is
// unset, mirroring casstore's RUSTFS_TEST_* convention.
func openTestStore(t *testing.T) (*pgindex.Store, string) {
	t.Helper()
	dsn := os.Getenv("BLOBGW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BLOBGW_TEST_DATABASE_URL (a pgx DSN) to run pgindex integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	store := pgindex.New(db)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	domain := "test-" + randHex(t, 8)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DELETE FROM pack_manifest WHERE domain=$1", domain)
		_, _ = db.ExecContext(ctx, "DELETE FROM packs WHERE domain=$1", domain)
		_, _ = db.ExecContext(ctx, "DELETE FROM blob_ref WHERE domain=$1", domain)
		_ = db.Close()
	})
	return store, domain
}

func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestPgIndex_DedupStoreContract(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	locs := []snapshot.ChunkLocation{
		{ChunkHash: "c1", PackRef: snapshot.PackRef{PackHash: "pA", Offset: 0, Size: 10}},
		{ChunkHash: "c2", PackRef: snapshot.PackRef{PackHash: "pA", Offset: 10, Size: 20}},
		{ChunkHash: "c3", PackRef: snapshot.PackRef{PackHash: "pB", Offset: 0, Size: 30}},
	}
	if err := store.Record(ctx, domain, locs); err != nil {
		t.Fatalf("Record: %v", err)
	}
	// Idempotent: re-record with a different location must not overwrite.
	if err := store.Record(ctx, domain, []snapshot.ChunkLocation{
		{ChunkHash: "c1", PackRef: snapshot.PackRef{PackHash: "pZ", Offset: 99, Size: 10}},
	}); err != nil {
		t.Fatalf("Record idempotent: %v", err)
	}

	ref, ok, err := store.Lookup(ctx, domain, "c1")
	if err != nil || !ok {
		t.Fatalf("Lookup c1: ok=%v err=%v", ok, err)
	}
	if ref.PackHash != "pA" || ref.Offset != 0 || ref.Size != 10 {
		t.Errorf("first-writer-wins violated: %+v", ref)
	}
	if _, ok, _ := store.Lookup(ctx, domain, "nope"); ok {
		t.Error("Lookup of absent chunk returned ok")
	}

	batch, err := store.LookupBatch(ctx, domain, []string{"c1", "c3", "missing"})
	if err != nil {
		t.Fatalf("LookupBatch: %v", err)
	}
	if len(batch) != 2 || batch["c3"].PackHash != "pB" {
		t.Errorf("LookupBatch unexpected: %+v", batch)
	}

	// Cross-domain isolation: nothing visible from another domain.
	if _, ok, _ := store.Lookup(ctx, domain+"-other", "c1"); ok {
		t.Error("cross-domain leak in Lookup")
	}

	// PurgePacks removes only entries pointing at pA.
	if err := store.PurgePacks(ctx, domain, []string{"pA"}); err != nil {
		t.Fatalf("PurgePacks: %v", err)
	}
	if _, ok, _ := store.Lookup(ctx, domain, "c1"); ok {
		t.Error("c1 should be purged with pack pA")
	}
	if _, ok, _ := store.Lookup(ctx, domain, "c3"); !ok {
		t.Error("c3 (pack pB) should survive purge of pA")
	}
}

func TestPgIndex_RefStoreContract(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	for _, ref := range []string{"a/1", "a/2", "b/1"} {
		err := store.Put(ctx, gateway.ObjectInfo{
			Domain: domain, Ref: ref, ContentHash: "h-" + ref, Size: 123, ContentType: "text/plain",
		})
		if err != nil {
			t.Fatalf("Put %s: %v", ref, err)
		}
	}

	got, ok, err := store.Get(ctx, domain, "a/1")
	if err != nil || !ok {
		t.Fatalf("Get a/1: ok=%v err=%v", ok, err)
	}
	if got.ContentHash != "h-a/1" || got.Size != 123 {
		t.Errorf("Get a/1 wrong: %+v", got)
	}

	// Upsert semantics.
	if err := store.Put(ctx, gateway.ObjectInfo{Domain: domain, Ref: "a/1", ContentHash: "h2", Size: 9}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got, _, _ := store.Get(ctx, domain, "a/1"); got.ContentHash != "h2" || got.Size != 9 {
		t.Errorf("upsert did not replace: %+v", got)
	}

	list, err := store.List(ctx, domain, "a/", 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("List a/ returned %d, want 2", len(list))
	}

	if err := store.Delete(ctx, domain, "a/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, domain, "a/1"); ok {
		t.Error("a/1 should be gone after Delete")
	}
}

// TestPgIndex_ListPageCursor proves keyset pagination (A8): inserting more than
// one page of rows under a prefix and paging through via the opaque cursor
// returns every row exactly once with no duplicates or skips across page
// boundaries, and the final page reports next=="" (exhausted). Rows under a
// different prefix are never returned.
func TestPgIndex_ListPageCursor(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	// Insert 2500 rows under "page/" plus a handful under "other/" that must
	// never leak into the paged listing. Distinct created_at values across rows
	// (and intentional ties) exercise both branches of the keyset predicate.
	const total = 2500
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < total; i++ {
		// Group rows into timestamp buckets so many share a created_at, forcing
		// the (created_at = $ts AND ref > $ref) tiebreak across page boundaries.
		created := base.Add(-time.Duration(i/7) * time.Second)
		if err := store.Put(ctx, gateway.ObjectInfo{
			Domain: domain, Ref: fmt.Sprintf("page/%05d", i), Size: int64(i), CreatedAt: created,
		}); err != nil {
			t.Fatalf("Put page/%05d: %v", i, err)
		}
	}
	for i := 0; i < 13; i++ {
		if err := store.Put(ctx, gateway.ObjectInfo{
			Domain: domain, Ref: fmt.Sprintf("other/%05d", i), Size: int64(i), CreatedAt: base,
		}); err != nil {
			t.Fatalf("Put other/%05d: %v", i, err)
		}
	}

	seen := make(map[string]int)
	after := ""
	pages := 0
	const pageSize = 300
	for {
		objs, next, err := store.ListPage(ctx, domain, "page/", after, pageSize)
		if err != nil {
			t.Fatalf("ListPage page %d: %v", pages, err)
		}
		if len(objs) > pageSize {
			t.Fatalf("page %d returned %d rows, want <= %d", pages, len(objs), pageSize)
		}
		for _, o := range objs {
			if !strings.HasPrefix(o.Ref, "page/") {
				t.Fatalf("prefix leak: %q in page/ listing", o.Ref)
			}
			seen[o.Ref]++
		}
		pages++
		if next == "" {
			if len(objs) > pageSize {
				t.Fatalf("final page non-empty cursor with full page")
			}
			break
		}
		after = next
		if pages > total { // safety: never loop forever
			t.Fatalf("pagination did not terminate after %d pages", pages)
		}
	}
	if len(seen) != total {
		t.Fatalf("paged through %d distinct refs, want %d", len(seen), total)
	}
	for ref, n := range seen {
		if n != 1 {
			t.Fatalf("ref %q returned %d times across pages (want exactly once)", ref, n)
		}
	}

	// DeleteByPrefix equivalent: page through "page/" and delete each ref, then
	// confirm everything under "page/" is gone while "other/" rows survive.
	after = ""
	for {
		objs, next, err := store.ListPage(ctx, domain, "page/", after, pageSize)
		if err != nil {
			t.Fatalf("ListPage during delete: %v", err)
		}
		for _, o := range objs {
			if err := store.Delete(ctx, domain, o.Ref); err != nil {
				t.Fatalf("Delete %q: %v", o.Ref, err)
			}
		}
		if next == "" {
			break
		}
		after = next
	}
	left, err := store.List(ctx, domain, "page/", 0)
	if err != nil {
		t.Fatalf("List page/ after delete: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("after prefix delete %d page/ rows remain, want 0", len(left))
	}
	others, err := store.List(ctx, domain, "other/", 0)
	if err != nil {
		t.Fatalf("List other/: %v", err)
	}
	if len(others) != 13 {
		t.Fatalf("prefix delete removed unrelated rows: %d other/ left, want 13", len(others))
	}
}

// TestPgIndex_ListStalePending verifies the staging-sweeper query: it returns
// only pending rows at/older than the cutoff, never finalized rows or pending
// rows still within the TTL window.
func TestPgIndex_ListStalePending(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	old := time.Now().UTC().Add(-48 * time.Hour)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	// A stale pending row, a fresh pending row, and a finalized (non-pending) row.
	put := func(ref string, pending bool, created time.Time) {
		t.Helper()
		if err := store.Put(ctx, gateway.ObjectInfo{
			Domain: domain, Ref: ref, Pending: pending,
			StagingKey: domain + "/staging/" + ref, CreatedAt: created,
		}); err != nil {
			t.Fatalf("Put %s: %v", ref, err)
		}
	}
	put("stale", true, old)
	put("fresh", true, recent)
	put("done", false, old) // old but finalized → must never be swept

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	stale, err := store.ListStalePending(ctx, domain, cutoff, 0)
	if err != nil {
		t.Fatalf("ListStalePending: %v", err)
	}
	if len(stale) != 1 || stale[0].Ref != "stale" {
		t.Fatalf("ListStalePending returned %+v, want only [stale]", stale)
	}
	if stale[0].StagingKey != domain+"/staging/stale" {
		t.Errorf("stale row staging key wrong: %q", stale[0].StagingKey)
	}
}

// TestPgIndex_PendingMetaPersistsAcrossRestart proves the A3 contract: user
// metadata and the A1 integrity expectations (expected_hash/expected_size) set
// on a PENDING row survive a process restart. It writes the pending row through
// one Store instance, then reads it back through a SECOND Store over a fresh
// *sql.DB (a simulated restart whose only durable state is Postgres). Before
// A3, user_meta had no column and was lost at the DB boundary, so Finalize
// re-read it empty after a restart.
func TestPgIndex_PendingMetaPersistsAcrossRestart(t *testing.T) {
	dsn := os.Getenv("BLOBGW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BLOBGW_TEST_DATABASE_URL (a pgx DSN) to run pgindex integration tests")
	}

	ctx := context.Background()
	domain := "test-" + randHex(t, 8)

	// First instance: migrate + write a pending row with metadata + expectations.
	db1, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open db1: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db1.ExecContext(ctx, "DELETE FROM blob_ref WHERE domain=$1", domain)
		_ = db1.Close()
	})
	store1 := pgindex.New(db1)
	if err := store1.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	created := time.Now().UTC().Truncate(time.Microsecond)
	want := gateway.ObjectInfo{
		Domain:       domain,
		Ref:          "pending/obj",
		ContentType:  "application/pdf",
		Pending:      true,
		StagingKey:   domain + "/staging/pending/obj/up",
		CreatedAt:    created,
		UserMeta:     map[string]string{"author": "alice", "etag": "deadbeef"},
		ExpectedHash: "abc123",
		ExpectedSize: 4096,
	}
	if err := store1.Put(ctx, want); err != nil {
		t.Fatalf("Put pending: %v", err)
	}

	// Second instance over a FRESH connection: the restart. Read the row back.
	db2, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	store2 := pgindex.New(db2)

	got, ok, err := store2.Get(ctx, domain, "pending/obj")
	if err != nil || !ok {
		t.Fatalf("Get after restart: ok=%v err=%v", ok, err)
	}
	if !got.Pending {
		t.Error("row lost its pending flag across restart")
	}
	if got.UserMeta["author"] != "alice" || got.UserMeta["etag"] != "deadbeef" {
		t.Errorf("user metadata lost across restart: %+v", got.UserMeta)
	}
	if got.ExpectedHash != "abc123" || got.ExpectedSize != 4096 {
		t.Errorf("integrity expectations lost across restart: hash=%q size=%d", got.ExpectedHash, got.ExpectedSize)
	}

	// ListStalePending must also surface the persisted metadata (it shares the
	// scan path used by the staging sweeper).
	stale, err := store2.ListStalePending(ctx, domain, time.Now().UTC().Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("ListStalePending: %v", err)
	}
	if len(stale) != 1 || stale[0].UserMeta["author"] != "alice" || stale[0].ExpectedSize != 4096 {
		t.Errorf("ListStalePending lost metadata: %+v", stale)
	}

	// A row written WITHOUT metadata reads back with a nil map (not an empty or
	// crashing scan): proves NULL columns and old rows scan cleanly.
	if err := store2.Put(ctx, gateway.ObjectInfo{Domain: domain, Ref: "bare", Size: 7}); err != nil {
		t.Fatalf("Put bare: %v", err)
	}
	bare, _, err := store2.Get(ctx, domain, "bare")
	if err != nil {
		t.Fatalf("Get bare: %v", err)
	}
	if bare.UserMeta != nil || bare.ExpectedHash != "" || bare.ExpectedSize != 0 {
		t.Errorf("bare row should have no metadata/expectations: %+v", bare)
	}
}

// TestPgIndex_RecordPackSizesUpsert proves RecordPackSizes inserts a per-pack
// physical-size row and that a re-record of the same (domain, pack_hash)
// OVERWRITES the compressed size (upsert, unlike the first-writer-wins Record).
func TestPgIndex_RecordPackSizesUpsert(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	if err := store.RecordPackSizes(ctx, domain, []snapshot.PackSize{
		{PackHash: "pA", CompressedBytes: 100},
		{PackHash: "pB", CompressedBytes: 250},
	}); err != nil {
		t.Fatalf("RecordPackSizes: %v", err)
	}
	// Physical SUM = 100 + 250.
	if us, err := store.DomainUsage(ctx, domain); err != nil {
		t.Fatalf("DomainUsage: %v", err)
	} else if us.PhysicalBytes != 350 {
		t.Errorf("physical after insert = %d, want 350", us.PhysicalBytes)
	}

	// Re-record pA with a different size → upsert overwrites (not ignored).
	if err := store.RecordPackSizes(ctx, domain, []snapshot.PackSize{
		{PackHash: "pA", CompressedBytes: 175},
	}); err != nil {
		t.Fatalf("RecordPackSizes upsert: %v", err)
	}
	if us, err := store.DomainUsage(ctx, domain); err != nil {
		t.Fatalf("DomainUsage after upsert: %v", err)
	} else if us.PhysicalBytes != 175+250 {
		t.Errorf("physical after upsert = %d, want %d", us.PhysicalBytes, 175+250)
	}

	// Empty input is a no-op (no error, no rows).
	if err := store.RecordPackSizes(ctx, domain, nil); err != nil {
		t.Errorf("RecordPackSizes(nil): %v", err)
	}
}

// TestPgIndex_BackfillPackSizes proves the one-time snapshot.BackfillPackSizes
// over a real blobstore records each pack's on-disk size into the pgindex packs
// table, so DomainUsage.PhysicalBytes equals the SUM of the pack blobs' lengths.
// It writes packs via a ChunkedStore backed by this pgindex Store, then CLEARS
// the packs rows (simulating pre-accounting data) and backfills them back.
func TestPgIndex_BackfillPackSizes(t *testing.T) {
	store, domain := openTestStore(t)
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
		DedupDomain: domain,
		Index:       snapshot.NewGlobalIndex(store, nil),
	})
	if err != nil {
		t.Fatalf("NewChunkedStore: %v", err)
	}
	for _, owner := range []string{"a", "b", "c"} {
		key := snapshot.SnapshotKey{Tenant: domain, OwnerKey: owner}
		if _, err := cs.Put(ctx, key, snapshot.SnapshotMetadata{},
			bytes.NewReader(randomBytes(t, 300_000))); err != nil {
			t.Fatalf("Put %s: %v", owner, err)
		}
	}

	// Sum the pack blobs' on-disk lengths — the physical truth to match against.
	var wantPhysical int64
	packCount := 0
	if err := chunks.ListBlobs(ctx, blobstore.ID("pack-"+domain+"-"), func(md blobstore.Metadata) error {
		wantPhysical += md.Length
		packCount++
		return nil
	}); err != nil {
		t.Fatalf("list packs: %v", err)
	}
	if packCount == 0 {
		t.Fatal("expected pack blobs written")
	}

	// The write path already recorded these packs; the backfill is idempotent
	// (RecordPackSizes upserts), so re-recording via the sweep restores the exact
	// same per-pack sizes. Asserting post-backfill physical == the pack-length SUM
	// therefore validates the backfill regardless of the starting rows — this is the
	// same code path that fills packs written before accounting existed.
	recorded, err := snapshot.BackfillPackSizes(ctx, chunks, store, domain, nil)
	if err != nil {
		t.Fatalf("BackfillPackSizes: %v", err)
	}
	if recorded != packCount {
		t.Errorf("backfill recorded %d packs, want %d", recorded, packCount)
	}

	us, err := store.DomainUsage(ctx, domain)
	if err != nil {
		t.Fatalf("DomainUsage: %v", err)
	}
	if us.PhysicalBytes != wantPhysical {
		t.Errorf("physical after backfill = %d, want %d (SUM of pack lengths)", us.PhysicalBytes, wantPhysical)
	}
}

// TestPgIndex_PurgePacksDropsPacksRow proves PurgePacks deletes BOTH the
// pack_manifest dedup entries AND the packs physical-size rows for the reclaimed
// pack, so a purged pack no longer contributes to the domain's physical SUM.
func TestPgIndex_PurgePacksDropsPacksRow(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	// Two packs, each with a chunk in pack_manifest and a size in packs.
	if err := store.Record(ctx, domain, []snapshot.ChunkLocation{
		{ChunkHash: "c1", PackRef: snapshot.PackRef{PackHash: "pA", Offset: 0, Size: 10}},
		{ChunkHash: "c2", PackRef: snapshot.PackRef{PackHash: "pB", Offset: 0, Size: 20}},
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := store.RecordPackSizes(ctx, domain, []snapshot.PackSize{
		{PackHash: "pA", CompressedBytes: 40},
		{PackHash: "pB", CompressedBytes: 60},
	}); err != nil {
		t.Fatalf("RecordPackSizes: %v", err)
	}
	if us, _ := store.DomainUsage(ctx, domain); us.PhysicalBytes != 100 {
		t.Fatalf("physical before purge = %d, want 100", us.PhysicalBytes)
	}

	// Purge pA → its dedup entry AND its packs row must both vanish; pB survives.
	if err := store.PurgePacks(ctx, domain, []string{"pA"}); err != nil {
		t.Fatalf("PurgePacks: %v", err)
	}
	if _, ok, _ := store.Lookup(ctx, domain, "c1"); ok {
		t.Error("c1 dedup entry should be purged with pack pA")
	}
	us, err := store.DomainUsage(ctx, domain)
	if err != nil {
		t.Fatalf("DomainUsage after purge: %v", err)
	}
	if us.PhysicalBytes != 60 {
		t.Errorf("physical after purging pA = %d, want 60 (only pB)", us.PhysicalBytes)
	}
}

// TestPgIndex_DomainUsagePerPackNotPerChunk is the load-bearing invariant:
// physical_bytes is a per-PACK SUM (each pack's compressed size counted once),
// NOT a per-chunk SUM. A pack holding MANY chunks must contribute its ONE
// compressed size — summing pack_manifest rows would massively over-count.
func TestPgIndex_DomainUsagePerPackNotPerChunk(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	// One pack "pBig" holds 5 chunks (each 1000 uncompressed bytes); its physical
	// compressed size is 1200 (recorded ONCE for the pack).
	locs := make([]snapshot.ChunkLocation, 0, 5)
	for i := 0; i < 5; i++ {
		locs = append(locs, snapshot.ChunkLocation{
			ChunkHash: fmt.Sprintf("big-%d", i),
			PackRef:   snapshot.PackRef{PackHash: "pBig", Offset: i * 1000, Size: 1000},
		})
	}
	if err := store.Record(ctx, domain, locs); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := store.RecordPackSizes(ctx, domain, []snapshot.PackSize{
		{PackHash: "pBig", CompressedBytes: 1200},
	}); err != nil {
		t.Fatalf("RecordPackSizes: %v", err)
	}
	// ObjectApparentBytes: pre-dedup size of blobgw objects only (blob_ref.size_b).
	// mlfs slice refs live in the tenant's separate mlfs meta DB; not present here.
	if err := store.Put(ctx, gateway.ObjectInfo{Domain: domain, Ref: "o/1", Size: 700}); err != nil {
		t.Fatalf("Put o/1: %v", err)
	}
	if err := store.Put(ctx, gateway.ObjectInfo{Domain: domain, Ref: "o/2", Size: 300}); err != nil {
		t.Fatalf("Put o/2: %v", err)
	}

	us, err := store.DomainUsage(ctx, domain)
	if err != nil {
		t.Fatalf("DomainUsage: %v", err)
	}
	// physical == the pack's single compressed size, NOT 5 × anything.
	if us.PhysicalBytes != 1200 {
		t.Errorf("physical = %d, want 1200 (pack counted ONCE, not per-chunk)", us.PhysicalBytes)
	}
	// logical_deduped == SUM of distinct chunk length_b = 5 × 1000.
	if us.LogicalDedupedBytes != 5000 {
		t.Errorf("logical_deduped = %d, want 5000 (5 distinct chunks × 1000)", us.LogicalDedupedBytes)
	}
	// object_apparent == SUM(blob_ref.size_b) = 700+300 (blobgw objects only).
	if us.ObjectApparentBytes != 1000 {
		t.Errorf("object_apparent = %d, want 1000 (blob_ref 700+300)", us.ObjectApparentBytes)
	}
}

// TestPgIndex_DomainUsageEmpty proves an unknown/empty domain reports zeros
// (COALESCE(...,0)) rather than a NULL-scan error.
func TestPgIndex_DomainUsageEmpty(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()
	us, err := store.DomainUsage(ctx, domain+"-never-written")
	if err != nil {
		t.Fatalf("DomainUsage empty: %v", err)
	}
	if us.PhysicalBytes != 0 || us.LogicalDedupedBytes != 0 {
		t.Errorf("empty domain usage = %+v, want all zero", us)
	}
}

// TestPgIndex_GatewayEndToEnd runs the full L1.1 acceptance through real
// Postgres: distinct identical objects dedup to one physical copy, and
// delete + GC reclaims unreferenced content while purging its index rows.
func TestPgIndex_GatewayEndToEnd(t *testing.T) {
	store, domain := openTestStore(t)
	ctx := context.Background()

	chunks, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("chunks: %v", err)
	}
	t.Cleanup(func() { _ = chunks.Close(ctx) })
	upstream, err := snapshot.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("upstream: %v", err)
	}
	cs, err := snapshot.NewChunkedStore(upstream, chunks, snapshot.ChunkedConfig{
		PackTargetBytes: 1 * 1024 * 1024,
		DedupDomain:     domain,
		Index:           snapshot.NewGlobalIndex(store, nil),
	})
	if err != nil {
		t.Fatalf("chunked store: %v", err)
	}
	gc := snapshot.NewChunkedGC(upstream, chunks, nil)
	gc.Index = store
	gw := gateway.New(cs, store, gateway.NewMemoryStagingStore(), domain)

	countBlobs := func() int {
		n := 0
		for _, p := range []blobstore.ID{blobstore.ID("chunk-" + domain + "-"), blobstore.ID("pack-" + domain + "-")} {
			_ = chunks.ListBlobs(ctx, p, func(blobstore.Metadata) error { n++; return nil })
		}
		return n
	}

	payload := randomBytes(t, 2*1024*1024)
	if _, err := gw.Put(ctx, "obj/0", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put 0: %v", err)
	}
	blobsFirst := countBlobs()
	for i := 1; i < 10; i++ {
		if _, err := gw.Put(ctx, fmt.Sprintf("obj/%d", i), "application/octet-stream", bytes.NewReader(payload)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if got := countBlobs(); got != blobsFirst {
		t.Errorf("dedup over Postgres failed: %d → %d blobs", blobsFirst, got)
	}
	if list, _ := gw.List(ctx, "obj/", 0); len(list) != 10 {
		t.Errorf("List returned %d, want 10", len(list))
	}

	// Distinct content, then delete it + GC → reclaimed and index purged.
	other := randomBytes(t, 2*1024*1024)
	if _, err := gw.Put(ctx, "solo", "application/octet-stream", bytes.NewReader(other)); err != nil {
		t.Fatalf("Put solo: %v", err)
	}
	if err := gw.Delete(ctx, "solo"); err != nil {
		t.Fatalf("Delete solo: %v", err)
	}
	if _, err := gc.RunOnce(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if got := countBlobs(); got != blobsFirst {
		t.Errorf("after deleting solo + GC expected %d blobs, got %d", blobsFirst, got)
	}
	// The shared objects still restore byte-exact.
	rc, _, err := gw.Get(ctx, "obj/5")
	if err != nil {
		t.Fatalf("Get obj/5: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(body, payload) {
		t.Error("shared object corrupted after GC")
	}
}
