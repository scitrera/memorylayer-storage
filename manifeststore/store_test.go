// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package manifeststore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/manifeststore"
	"github.com/scitrera/memorylayer-storage/manifeststore/internal/testpg"
)

// openTestStore returns a migrated Store backed by a private, throwaway PG
// schema. Skips when MLFS_TEST_DATABASE_URL is unset.
func openTestStore(t *testing.T) *manifeststore.Store {
	t.Helper()
	db := testpg.DB(t)
	if err := manifeststore.Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return manifeststore.New(db, nil)
}

func ctx() context.Context { return context.Background() }

// --- Put / GetLatest round-trip (payload + metadata fidelity) ---------------

func TestPutGetLatest_RoundTrip(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", Workspace: "ws", User: "u", OwnerKey: "s/42"}

	blob := []byte("manifest payload bytes")
	inMeta := snapshot.SnapshotMetadata{
		Runtime: "runc",
		Format:  "dill-pickle-tar",
		Tags:    map[string]string{"k": "v"},
	}

	stored, err := store.Put(ctx(), key, inMeta, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.Version == "" {
		t.Error("Put: Version not set")
	}
	if stored.SizeBytes != int64(len(blob)) {
		t.Errorf("SizeBytes = %d, want %d", stored.SizeBytes, len(blob))
	}
	if stored.Runtime != "runc" {
		t.Errorf("Runtime = %q, want \"runc\"", stored.Runtime)
	}
	if stored.Key.Tenant != key.Tenant {
		t.Errorf("Key.Tenant = %q, want %q", stored.Key.Tenant, key.Tenant)
	}
	if stored.Tags["k"] != "v" {
		t.Errorf("Tags[k] = %q, want \"v\"", stored.Tags["k"])
	}

	rc, gotMeta, err := store.GetLatest(ctx(), key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("payload mismatch: got %q, want %q", got, blob)
	}
	if gotMeta.Version != stored.Version {
		t.Errorf("Version mismatch: got %q, want %q", gotMeta.Version, stored.Version)
	}
	if gotMeta.Runtime != "runc" {
		t.Errorf("Runtime = %q, want \"runc\"", gotMeta.Runtime)
	}
}

// --- SizeBytes is computed from stream (caller value ignored) ---------------

func TestPut_SizeBytesComputed(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	payload := []byte("abcdefghij") // 10 bytes
	meta := snapshot.SnapshotMetadata{SizeBytes: 9999}
	stored, err := store.Put(ctx(), key, meta, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.SizeBytes != 10 {
		t.Errorf("SizeBytes = %d, want 10", stored.SizeBytes)
	}
}

// --- GetLatestMetadata returns metadata without consuming blob ---------------

func TestGetLatestMetadata(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	blob := []byte("checkpoint-blob")
	inMeta := snapshot.SnapshotMetadata{
		Runtime: "runsc",
		Format:  "gvisor-checkpoint",
		Tags:    map[string]string{"sandbox_cidr": "10.0.0.0/24", "tok": "abc"},
	}
	stored, err := store.Put(ctx(), key, inMeta, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	meta, err := store.GetLatestMetadata(ctx(), key)
	if err != nil {
		t.Fatalf("GetLatestMetadata: %v", err)
	}
	if meta.Version != stored.Version {
		t.Errorf("Version = %q, want %q", meta.Version, stored.Version)
	}
	if meta.Runtime != "runsc" {
		t.Errorf("Runtime = %q, want \"runsc\"", meta.Runtime)
	}
	if meta.Tags["sandbox_cidr"] != "10.0.0.0/24" {
		t.Errorf("sandbox_cidr = %q", meta.Tags["sandbox_cidr"])
	}
	if meta.Tags["tok"] != "abc" {
		t.Errorf("tok = %q", meta.Tags["tok"])
	}
}

// --- GetLatestMetadata absent key returns ErrNoSnapshot ---------------------

func TestGetLatestMetadata_NoSnapshot(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "no-such"}

	_, err := store.GetLatestMetadata(ctx(), key)
	if !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Errorf("got %v, want ErrNoSnapshot", err)
	}
}

// --- Multiple versions: GetLatest returns newest, List returns all ----------

func TestMultipleVersions_GetLatestIsNewest(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Put three versions. NewVersion() is time-based so a tiny sleep between
	// them ensures strict ordering. We use fixed CreatedAt metadata but the
	// version string is what drives ordering.
	type versionedPut struct {
		version string
		content string
	}
	var puts []versionedPut

	for i, content := range []string{"first", "second", "third"} {
		// Ensure version strings differ even within same test run: inject
		// a known-sorted CreatedAt (not used for ordering, but kept for parity).
		meta := snapshot.SnapshotMetadata{
			CreatedAt: time.Date(2024, 1, i+1, 0, 0, 0, 0, time.UTC),
		}
		stored, err := store.Put(ctx(), key, meta, strings.NewReader(content))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		puts = append(puts, versionedPut{stored.Version, content})
	}

	// Versions should all be distinct.
	if puts[0].version == puts[1].version || puts[1].version == puts[2].version {
		t.Fatal("Puts produced duplicate versions")
	}

	// GetLatest must return the third (highest-version) blob.
	rc, latestMeta, err := store.GetLatest(ctx(), key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "third" {
		t.Errorf("GetLatest content = %q, want \"third\"", data)
	}
	if latestMeta.Version != puts[2].version {
		t.Errorf("GetLatest version = %q, want %q", latestMeta.Version, puts[2].version)
	}

	// List must return all three, newest first.
	list, err := store.List(ctx(), key)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("List: got %d entries, want 3", len(list))
	}
	// Newest first (descending version order).
	if list[0].Version != puts[2].version {
		t.Errorf("List[0] = %q, want newest %q", list[0].Version, puts[2].version)
	}
	if list[2].Version != puts[0].version {
		t.Errorf("List[2] = %q, want oldest %q", list[2].Version, puts[0].version)
	}
}

// --- Delete removes a version; others survive --------------------------------

func TestDelete_SpecificVersion(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	var versions []string
	for i, content := range []string{"first", "second", "third"} {
		stored, err := store.Put(ctx(), key, snapshot.SnapshotMetadata{}, strings.NewReader(content))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		versions = append(versions, stored.Version)
	}

	// Delete the middle version.
	if err := store.Delete(ctx(), key, versions[1]); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// First and third must still be retrievable via Get.
	for _, v := range []string{versions[0], versions[2]} {
		rc, _, err := store.Get(ctx(), key, v)
		if err != nil {
			t.Errorf("Get version %q after delete: %v", v, err)
			continue
		}
		rc.Close()
	}

	// Middle must be gone.
	_, _, err := store.Get(ctx(), key, versions[1])
	if !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Errorf("Get deleted version: got %v, want ErrNoSnapshot", err)
	}
}

// --- Delete is idempotent (absent version → nil) ----------------------------

func TestDelete_Idempotent(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	if err := store.Delete(ctx(), key, "does-not-exist"); err != nil {
		t.Errorf("Delete nonexistent: got %v, want nil", err)
	}
}

// --- GetLatest on absent key returns ErrNoSnapshot --------------------------

func TestGetLatest_AbsentKey_ErrNoSnapshot(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "no-such"}

	_, _, err := store.GetLatest(ctx(), key)
	if !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Errorf("GetLatest absent: got %v, want ErrNoSnapshot", err)
	}
}

// --- Get absent version returns ErrNoSnapshot --------------------------------

func TestGet_AbsentVersion_ErrNoSnapshot(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	_, _, err := store.Get(ctx(), key, "no-such-version")
	if !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Errorf("Get absent version: got %v, want ErrNoSnapshot", err)
	}
}

// --- Walk visits all stored manifests across keys ---------------------------

func TestWalk_VisitsAllManifests(t *testing.T) {
	store := openTestStore(t)

	keys := []snapshot.SnapshotKey{
		{Tenant: "t1", OwnerKey: "s/1"},
		{Tenant: "t1", OwnerKey: "s/2"},
		{Tenant: "t2", OwnerKey: "s/3"},
	}
	wantVersions := make(map[string]bool)
	for _, k := range keys {
		stored, err := store.Put(ctx(), k, snapshot.SnapshotMetadata{}, strings.NewReader("data"))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		wantVersions[stored.Version] = false // false = not yet seen by Walk
	}

	err := store.Walk(ctx(), func(key snapshot.SnapshotKey, version string, meta snapshot.SnapshotMetadata) error {
		if _, ok := wantVersions[version]; ok {
			wantVersions[version] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	for v, seen := range wantVersions {
		if !seen {
			t.Errorf("Walk: version %q not visited", v)
		}
	}
}

// --- Walk abort on yield error ----------------------------------------------

func TestWalk_YieldError_Propagates(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "o"}

	if _, err := store.Put(ctx(), key, snapshot.SnapshotMetadata{}, strings.NewReader("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sentinel := errors.New("stop walk")
	err := store.Walk(ctx(), func(_ snapshot.SnapshotKey, _ string, _ snapshot.SnapshotMetadata) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Walk: got %v, want sentinel error", err)
	}
}

// --- Empty workspace/user keys round-trip cleanly ---------------------------

func TestEmptyWorkspaceUser_RoundTrip(t *testing.T) {
	store := openTestStore(t)
	key := snapshot.SnapshotKey{Tenant: "t", OwnerKey: "s/99"} // Workspace and User empty

	stored, err := store.Put(ctx(), key, snapshot.SnapshotMetadata{}, strings.NewReader("slice data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, meta, err := store.Get(ctx(), key, stored.Version)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	rc.Close()
	if meta.Version != stored.Version {
		t.Errorf("Version mismatch: got %q, want %q", meta.Version, stored.Version)
	}
}
