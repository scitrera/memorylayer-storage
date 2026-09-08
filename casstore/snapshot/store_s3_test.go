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
)

// TestS3Store_Integration runs against a real RustFS instance when
// RUSTFS_TEST_ENDPOINT is set. Otherwise the test is skipped with a clear
// message explaining how to run it locally.
//
// To run locally:
//
//	docker run -d -p 9000:9000 -p 9001:9001 \
//	  -e RUSTFS_ROOT_USER=rustfsadmin -e RUSTFS_ROOT_PASSWORD=rustfsadmin \
//	  rustfs/rustfs server /data --console-address ":9001"
//
//	RUSTFS_TEST_ENDPOINT=http://localhost:9000 \
//	RUSTFS_TEST_BUCKET=test-snapshots \
//	  go test ./internal/snapshot/ -run TestS3Store_Integration -v
//
// The test creates and tears down objects in the bucket; it does NOT create
// or delete the bucket itself — create it with the aws-cli first:
//
//	aws --endpoint-url http://localhost:9000 s3 mb s3://test-snapshots
func TestS3Store_Integration(t *testing.T) {
	endpoint := os.Getenv("RUSTFS_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("RUSTFS_TEST_ENDPOINT not set — skipping S3 integration tests. " +
			"See docs/snapshot.md for how to run these tests locally with RustFS.")
	}
	bucket := os.Getenv("RUSTFS_TEST_BUCKET")
	if bucket == "" {
		bucket = "test-snapshots"
	}

	ctx := context.Background()
	cfg := S3Config{
		Bucket:         bucket,
		Prefix:         "integration-test",
		Region:         "us-east-1",
		Endpoint:       endpoint,
		AccessKey:      getEnvOr("RUSTFS_TEST_ACCESS_KEY", "rustfsadmin"),
		SecretKey:      getEnvOr("RUSTFS_TEST_SECRET_KEY", "rustfsadmin"),
		ForcePathStyle: true,
	}

	store, err := NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}

	t.Run("PutGetDelete", func(t *testing.T) {
		key := SnapshotKey{
			Tenant:    "s3-test-tenant",
			Workspace: "ws1",
			User:      "user1",
			OwnerKey:  "owner1",
		}
		blob := []byte("s3 snapshot blob")
		meta := SnapshotMetadata{Runtime: "runsc", Format: "gvisor-checkpoint"}

		stored, err := store.Put(ctx, key, meta, bytes.NewReader(blob))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if stored.Version == "" {
			t.Error("Put: Version not set")
		}
		if stored.SizeBytes != int64(len(blob)) {
			t.Errorf("Put: SizeBytes = %d, want %d", stored.SizeBytes, len(blob))
		}

		// Get.
		rc, gotMeta, err := store.Get(ctx, key, stored.Version)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if !bytes.Equal(data, blob) {
			t.Errorf("Get content mismatch: got %q, want %q", data, blob)
		}
		if gotMeta.Runtime != "runsc" {
			t.Errorf("Get Runtime = %q, want \"runsc\"", gotMeta.Runtime)
		}

		// Delete.
		if err := store.Delete(ctx, key, stored.Version); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, _, err = store.Get(ctx, key, stored.Version)
		if !errors.Is(err, ErrNoSnapshot) {
			t.Errorf("Get after Delete: got %v, want ErrNoSnapshot", err)
		}
	})

	t.Run("GetLatestAndList", func(t *testing.T) {
		key := SnapshotKey{
			Tenant:   "s3-test-tenant",
			OwnerKey: "owner-latest",
		}

		m1, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("first"))
		if err != nil {
			t.Fatalf("Put first: %v", err)
		}
		m2, err := store.Put(ctx, key, SnapshotMetadata{}, strings.NewReader("second"))
		if err != nil {
			t.Fatalf("Put second: %v", err)
		}
		t.Cleanup(func() {
			_ = store.Delete(ctx, key, m1.Version)
			_ = store.Delete(ctx, key, m2.Version)
		})

		// GetLatest should return the second blob.
		rc, latestMeta, err := store.GetLatest(ctx, key)
		if err != nil {
			t.Fatalf("GetLatest: %v", err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != "second" {
			t.Errorf("GetLatest content = %q, want \"second\"", data)
		}
		if latestMeta.Version != m2.Version {
			t.Errorf("GetLatest version = %q, want %q", latestMeta.Version, m2.Version)
		}

		// List should return 2 entries, newest first.
		list, err := store.List(ctx, key)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("List: got %d entries, want 2", len(list))
		}
		if list[0].Version != m2.Version {
			t.Errorf("List[0].Version = %q, want %q", list[0].Version, m2.Version)
		}
	})

	t.Run("MissingKeyReturnsErrNoSnapshot", func(t *testing.T) {
		key := SnapshotKey{Tenant: "s3-test-tenant", OwnerKey: "does-not-exist"}
		_, _, err := store.Get(ctx, key, "nosuchversion")
		if !errors.Is(err, ErrNoSnapshot) {
			t.Errorf("Get missing: got %v, want ErrNoSnapshot", err)
		}
		_, _, err = store.GetLatest(ctx, key)
		if !errors.Is(err, ErrNoSnapshot) {
			t.Errorf("GetLatest missing: got %v, want ErrNoSnapshot", err)
		}
	})
}

func getEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
