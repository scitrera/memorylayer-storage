// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLocalStore_PutGetDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()

	key := SnapshotKey{
		Tenant:    "tenantA",
		Workspace: "ws1",
		User:      "userX",
		OwnerKey:  "ownerABC",
	}
	blob := []byte("hello snapshot world")
	inMeta := SnapshotMetadata{
		Runtime: "runc",
		Format:  "dill-pickle-tar",
	}

	stored, err := store.Put(ctx, key, inMeta, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.Version == "" {
		t.Error("Put: Version not set")
	}
	if stored.SizeBytes != int64(len(blob)) {
		t.Errorf("Put: SizeBytes = %d, want %d", stored.SizeBytes, len(blob))
	}
	if stored.Runtime != "runc" {
		t.Errorf("Put: Runtime = %q, want \"runc\"", stored.Runtime)
	}
	if stored.Key.Tenant != key.Tenant {
		t.Errorf("Put: Key.Tenant = %q, want %q", stored.Key.Tenant, key.Tenant)
	}

	// Get the stored version back.
	rc, gotMeta, err := store.Get(ctx, key, stored.Version)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("Get: content mismatch: got %q, want %q", got, blob)
	}
	if gotMeta.Version != stored.Version {
		t.Errorf("Get: Version mismatch: got %q, want %q", gotMeta.Version, stored.Version)
	}

	// Delete and confirm gone.
	if err := store.Delete(ctx, key, stored.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, _, err = store.Get(ctx, key, stored.Version)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("Get after Delete: got %v, want ErrNoSnapshot", err)
	}
}

func TestLocalStore_MissingKeyReturnsErrNoSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()

	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	_, _, err = store.Get(ctx, key, "nosuchversion")
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("Get missing: got %v, want ErrNoSnapshot", err)
	}
	_, _, err = store.GetLatest(ctx, key)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("GetLatest missing: got %v, want ErrNoSnapshot", err)
	}
}

func TestLocalStore_TwoPuts_DistinctVersions_GetLatestIsNewer(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Freeze time for the first put so the second is strictly newer.
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	timeNow = func() time.Time { return t1 }
	m1, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("v1 data"))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	timeNow = func() time.Time { return t2 }
	m2, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("v2 data"))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	t.Cleanup(func() { timeNow = time.Now })

	if m1.Version == m2.Version {
		t.Fatal("two Puts produced the same version")
	}

	// GetLatest should return the v2 blob.
	rc, latestMeta, err := store.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "v2 data" {
		t.Errorf("GetLatest content = %q, want \"v2 data\"", data)
	}
	if latestMeta.Version != m2.Version {
		t.Errorf("GetLatest version = %q, want %q", latestMeta.Version, m2.Version)
	}

	// List should return both, newest first.
	list, err := store.List(ctx, key)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List: got %d entries, want 2", len(list))
	}
	if list[0].Version != m2.Version {
		t.Errorf("List[0] = %q, want newer version %q", list[0].Version, m2.Version)
	}
	if list[1].Version != m1.Version {
		t.Errorf("List[1] = %q, want older version %q", list[1].Version, m1.Version)
	}
}

func TestLocalStore_DeleteMiddleVersionDoesNotAffectOthers(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	var versions []string
	for i, content := range []string{"first", "second", "third"} {
		ts := time.Date(2024, 1, i+1, 0, 0, 0, 0, time.UTC)
		timeNow = func() time.Time { return ts }
		m, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader(content))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		versions = append(versions, m.Version)
	}
	t.Cleanup(func() { timeNow = time.Now })

	// Delete the middle version.
	if err := store.Delete(ctx, key, versions[1]); err != nil {
		t.Fatalf("Delete middle: %v", err)
	}

	// First and third must still be retrievable.
	for _, v := range []string{versions[0], versions[2]} {
		rc, _, err := store.Get(ctx, key, v)
		if err != nil {
			t.Errorf("Get version %q after middle delete: %v", v, err)
			continue
		}
		rc.Close()
	}

	// Middle must be gone.
	_, _, err = store.Get(ctx, key, versions[1])
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("Get deleted version: got %v, want ErrNoSnapshot", err)
	}
}

func TestLocalStore_SizeBytesComputed(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	payload := []byte("abcdefghij")           // 10 bytes
	meta := SnapshotMetadata{SizeBytes: 9999} // caller's value must be ignored
	stored, err := store.Put(ctx, key, meta, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.SizeBytes != 10 {
		t.Errorf("SizeBytes = %d, want 10", stored.SizeBytes)
	}
}

func TestLocalStore_EmptyWorkspaceUser(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	ctx := context.Background()

	// Keys with empty workspace and user must round-trip cleanly.
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
	m, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, got, err := store.Get(ctx, key, m.Version)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	rc.Close()
	if got.Version != m.Version {
		t.Errorf("Version mismatch: got %q, want %q", got.Version, m.Version)
	}
}

func TestLocalStore_DeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Delete a version that never existed — must return nil.
	if err := store.Delete(ctx, key, "does-not-exist"); err != nil {
		t.Errorf("Delete nonexistent: got %v, want nil", err)
	}
}

func TestLocalStore_GetLatestMetadata_DoesNotOpenBlob(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "o"}

	// Put a snapshot with a known tag.
	blob := []byte("checkpoint-blob-data")
	inMeta := SnapshotMetadata{
		Runtime: "runsc",
		Format:  "gvisor-checkpoint",
		Tags:    map[string]string{"sandbox_cidr": "192.168.254.0/30", "access_token": "tok-abc"},
	}
	stored, err := store.Put(ctx, key, inMeta, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// GetLatestMetadata must return matching metadata without error.
	meta, err := store.GetLatestMetadata(ctx, key)
	if err != nil {
		t.Fatalf("GetLatestMetadata: %v", err)
	}
	if meta.Version != stored.Version {
		t.Errorf("Version = %q, want %q", meta.Version, stored.Version)
	}
	if meta.Tags["sandbox_cidr"] != "192.168.254.0/30" {
		t.Errorf("sandbox_cidr = %q, want 192.168.254.0/30", meta.Tags["sandbox_cidr"])
	}
	if meta.Tags["access_token"] != "tok-abc" {
		t.Errorf("access_token = %q, want tok-abc", meta.Tags["access_token"])
	}
	// The blob file must still exist (GetLatestMetadata must not open/consume it).
	versionDir := store.keyPath(key, stored.Version)
	if _, statErr := os.Stat(blobPath(versionDir)); statErr != nil {
		t.Errorf("blob file missing after GetLatestMetadata: %v", statErr)
	}
}

func TestLocalStore_GetLatestMetadata_NoSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "no-such"}

	_, err := store.GetLatestMetadata(ctx, key)
	if !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("GetLatestMetadata missing: got %v, want ErrNoSnapshot", err)
	}
}
