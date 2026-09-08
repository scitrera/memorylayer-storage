// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package manifeststore_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// TestGetLatestBatch_ResolvesLatestPerKey covers the batched manifest resolution
// used by read-side pack coalescing: each key resolves to its LATEST version's
// payload + metadata in one query, missing keys are absent (not errors), repeated
// keys are de-duplicated, and keys in different partitions still resolve.
func TestGetLatestBatch_ResolvesLatestPerKey(t *testing.T) {
	store := openTestStore(t)

	mlfsKey := func(id string) snapshot.SnapshotKey {
		return snapshot.SnapshotKey{OwnerKey: "s/" + id} // tenant/workspace/user empty (mlfs slice keys)
	}

	// Two single-version slice keys + one key written TWICE (latest must win).
	k1, k2, kMulti := mlfsKey("1"), mlfsKey("2"), mlfsKey("9")
	if _, err := store.Put(ctx(), k1, snapshot.SnapshotMetadata{Format: "v2"}, bytes.NewReader([]byte("payload-1"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx(), k2, snapshot.SnapshotMetadata{Format: "v2"}, bytes.NewReader([]byte("payload-2"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx(), kMulti, snapshot.SnapshotMetadata{}, bytes.NewReader([]byte("old"))); err != nil {
		t.Fatal(err)
	}
	newest, err := store.Put(ctx(), kMulti, snapshot.SnapshotMetadata{Format: "newest"}, bytes.NewReader([]byte("new-payload")))
	if err != nil {
		t.Fatal(err)
	}

	// A key in a different partition (non-empty tenant) to prove partition grouping.
	kOther := snapshot.SnapshotKey{Tenant: "t2", Workspace: "ws", User: "u", OwnerKey: "s/1"}
	if _, err := store.Put(ctx(), kOther, snapshot.SnapshotMetadata{}, bytes.NewReader([]byte("other-tenant"))); err != nil {
		t.Fatal(err)
	}

	missing := mlfsKey("404")
	req := []snapshot.SnapshotKey{k1, k2, kMulti, kMulti /* dup */, kOther, missing}

	got, err := store.GetLatestBatch(ctx(), req)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}

	// Missing key absent; all others present.
	if _, ok := got[missing]; ok {
		t.Errorf("absent key should not be in result")
	}
	want := map[snapshot.SnapshotKey]string{
		k1:     "payload-1",
		k2:     "payload-2",
		kMulti: "new-payload", // latest version wins
		kOther: "other-tenant",
	}
	for k, wantPayload := range want {
		e, ok := got[k]
		if !ok {
			t.Errorf("key %v missing from batch result", k)
			continue
		}
		if string(e.Payload) != wantPayload {
			t.Errorf("key %v payload = %q, want %q", k, e.Payload, wantPayload)
		}
		if e.Meta.Key != k {
			t.Errorf("key %v meta.Key = %v, want %v", k, e.Meta.Key, k)
		}
	}
	// The multi-version key's metadata must be the newest version's.
	if e := got[kMulti]; e.Meta.Version != newest.Version {
		t.Errorf("kMulti version = %q, want newest %q", e.Meta.Version, newest.Version)
	}
	if e := got[kMulti]; e.Meta.Format != "newest" {
		t.Errorf("kMulti format = %q, want \"newest\"", e.Meta.Format)
	}

	// Empty input → empty (non-nil) map, no error.
	if m, err := store.GetLatestBatch(ctx(), nil); err != nil || m == nil || len(m) != 0 {
		t.Errorf("GetLatestBatch(nil) = (%v, %v), want (empty map, nil)", m, err)
	}
}

// TestWalkPayloads streams every manifest's payload + metadata in one pass and
// cross-checks each against Put — the GC's per-manifest-Get-free live-set scan.
func TestWalkPayloads(t *testing.T) {
	store := openTestStore(t)
	type want struct {
		payload []byte
		format  string
		version string
	}
	wants := map[string]want{}
	for _, id := range []string{"1", "2", "7"} {
		k := snapshot.SnapshotKey{OwnerKey: "s/" + id}
		payload := []byte("manifest-payload-" + id)
		md, err := store.Put(ctx(), k, snapshot.SnapshotMetadata{Format: "fmt-" + id}, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		wants["s/"+id] = want{payload: payload, format: "fmt-" + id, version: md.Version}
	}

	seen := map[string]bool{}
	err := store.WalkPayloads(ctx(), func(key snapshot.SnapshotKey, version string, meta snapshot.SnapshotMetadata, payload []byte) error {
		w, ok := wants[key.OwnerKey]
		if !ok {
			t.Errorf("unexpected key %q", key.OwnerKey)
			return nil
		}
		seen[key.OwnerKey] = true
		if !bytes.Equal(payload, w.payload) {
			t.Errorf("key %q payload = %q, want %q", key.OwnerKey, payload, w.payload)
		}
		if meta.Format != w.format || version != w.version || meta.Version != w.version {
			t.Errorf("key %q meta mismatch: format=%q version=%q (want %q/%q)",
				key.OwnerKey, meta.Format, version, w.format, w.version)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkPayloads: %v", err)
	}
	if len(seen) != len(wants) {
		t.Errorf("WalkPayloads visited %d manifests, want %d", len(seen), len(wants))
	}
}

// TestGetLatestBatch_MatchesGetLatest cross-checks the batch path against the
// per-key GetLatest for payload + version, so the two never diverge.
func TestGetLatestBatch_MatchesGetLatest(t *testing.T) {
	store := openTestStore(t)
	keys := make([]snapshot.SnapshotKey, 0, 5)
	for _, id := range []string{"10", "11", "12", "2", "3"} { // mixes digit-widths (lexical vs numeric)
		k := snapshot.SnapshotKey{OwnerKey: "s/" + id}
		if _, err := store.Put(ctx(), k, snapshot.SnapshotMetadata{}, bytes.NewReader([]byte("data-"+id))); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	batch, err := store.GetLatestBatch(ctx(), keys)
	if err != nil {
		t.Fatalf("GetLatestBatch: %v", err)
	}
	for _, k := range keys {
		rc, meta, err := store.GetLatest(ctx(), k)
		if err != nil {
			t.Fatalf("GetLatest %v: %v", k, err)
		}
		want, _ := io.ReadAll(rc)
		_ = rc.Close()
		e := batch[k]
		if !bytes.Equal(e.Payload, want) {
			t.Errorf("key %v: batch payload %q != GetLatest %q", k, e.Payload, want)
		}
		if e.Meta.Version != meta.Version {
			t.Errorf("key %v: batch version %q != GetLatest %q", k, e.Meta.Version, meta.Version)
		}
	}
}
