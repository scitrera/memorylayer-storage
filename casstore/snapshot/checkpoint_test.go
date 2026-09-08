// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// --- stubs ---

type stubCheckpointer struct {
	checkpointErr error
	metadata      map[string]string
	blob          []byte
}

func (s *stubCheckpointer) Checkpoint(_ context.Context, w io.Writer) error {
	if s.checkpointErr != nil {
		return s.checkpointErr
	}
	blob := s.blob
	if blob == nil {
		blob = []byte("checkpoint-data")
	}
	_, err := w.Write(blob)
	return err
}

func (s *stubCheckpointer) SnapshotMetadata() map[string]string {
	return s.metadata
}

type stubStore struct {
	putErr error
	puts   []stubPut
}

type stubPut struct {
	key  SnapshotKey
	meta SnapshotMetadata
	data []byte
}

func (s *stubStore) Put(_ context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error) {
	if s.putErr != nil {
		return SnapshotMetadata{}, s.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	s.puts = append(s.puts, stubPut{key: key, meta: meta, data: data})
	return SnapshotMetadata{Key: key, Version: "v-test", SizeBytes: int64(len(data))}, nil
}

func (s *stubStore) Get(_ context.Context, _ SnapshotKey, _ string) (io.ReadCloser, SnapshotMetadata, error) {
	return nil, SnapshotMetadata{}, ErrNoSnapshot
}
func (s *stubStore) GetLatest(_ context.Context, _ SnapshotKey) (io.ReadCloser, SnapshotMetadata, error) {
	return nil, SnapshotMetadata{}, ErrNoSnapshot
}
func (s *stubStore) GetLatestMetadata(_ context.Context, _ SnapshotKey) (SnapshotMetadata, error) {
	return SnapshotMetadata{}, ErrNoSnapshot
}
func (s *stubStore) List(_ context.Context, _ SnapshotKey) ([]SnapshotMetadata, error) {
	return nil, nil
}
func (s *stubStore) Delete(_ context.Context, _ SnapshotKey, _ string) error { return nil }
func (s *stubStore) Walk(_ context.Context, _ func(SnapshotKey, string, SnapshotMetadata) error) error {
	return nil
}

// --- tests ---

// TestCheckpointAndStore_TagMerge verifies that baseMeta tags win over
// sandbox-supplied tags on conflict, and that sandbox-only tags are included.
func TestCheckpointAndStore_TagMerge(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok1"}
	baseMeta := SnapshotMetadata{
		Key:     key,
		Runtime: "runsc",
		Format:  "gvisor-checkpoint",
		Tags:    map[string]string{"format_tag": "x"},
	}
	cp := &stubCheckpointer{
		metadata: map[string]string{
			"access_token": "T1",
			"format_tag":   "y", // base should win
		},
	}
	store := &stubStore{}

	stored, err := CheckpointAndStore(context.Background(), cp, store, key, baseMeta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored.Version != "v-test" {
		t.Errorf("Version = %q, want %q", stored.Version, "v-test")
	}
	if stored.SizeBytes == 0 {
		t.Error("SizeBytes should be > 0")
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected 1 Put, got %d", len(store.puts))
	}
	tags := store.puts[0].meta.Tags
	if tags["format_tag"] != "x" {
		t.Errorf("format_tag = %q, want %q (base should win)", tags["format_tag"], "x")
	}
	if tags["access_token"] != "T1" {
		t.Errorf("access_token = %q, want %q (sandbox key should be present)", tags["access_token"], "T1")
	}
}

// TestCheckpointAndStore_EmptySandboxMetadata verifies baseMeta tags are
// unchanged when the sandbox returns nil metadata.
func TestCheckpointAndStore_EmptySandboxMetadata(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok2"}
	baseMeta := SnapshotMetadata{
		Key:    key,
		Tags:   map[string]string{"k": "v"},
		Format: "gvisor-checkpoint",
	}
	cp := &stubCheckpointer{metadata: nil}
	store := &stubStore{}

	_, err := CheckpointAndStore(context.Background(), cp, store, key, baseMeta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected 1 Put, got %d", len(store.puts))
	}
	tags := store.puts[0].meta.Tags
	if tags["k"] != "v" {
		t.Errorf("base tag k = %q, want %q", tags["k"], "v")
	}
}

// TestCheckpointAndStore_CheckpointError verifies that a checkpoint failure
// is returned and that the helper does not panic.
func TestCheckpointAndStore_CheckpointError(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok3"}
	checkErr := errors.New("disk full")
	cp := &stubCheckpointer{checkpointErr: checkErr}
	store := &stubStore{}

	_, err := CheckpointAndStore(context.Background(), cp, store, key, SnapshotMetadata{Key: key})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, checkErr) {
		t.Errorf("error = %v, should wrap %v", err, checkErr)
	}
}

// TestCheckpointAndStore_ErrSnapshotUnsupported verifies that
// ErrSnapshotUnsupported is returned unwrapped so callers can errors.Is on it.
func TestCheckpointAndStore_ErrSnapshotUnsupported(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok4"}
	cp := &stubCheckpointer{checkpointErr: ErrSnapshotUnsupported}
	store := &stubStore{}

	_, err := CheckpointAndStore(context.Background(), cp, store, key, SnapshotMetadata{Key: key})
	if !errors.Is(err, ErrSnapshotUnsupported) {
		t.Errorf("expected ErrSnapshotUnsupported, got %v", err)
	}
}

// TestCheckpointAndStore_StoreError verifies that a store Put error is returned.
func TestCheckpointAndStore_StoreError(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok5"}
	storeErr := errors.New("s3 unavailable")
	cp := &stubCheckpointer{}
	store := &stubStore{putErr: storeErr}

	_, err := CheckpointAndStore(context.Background(), cp, store, key, SnapshotMetadata{Key: key})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, storeErr) {
		t.Errorf("error = %v, should wrap %v", err, storeErr)
	}
}

// TestCheckpointAndStore_Success verifies the happy path: non-empty Version
// and SizeBytes > 0 in returned metadata.
func TestCheckpointAndStore_Success(t *testing.T) {
	key := SnapshotKey{Tenant: "t1", OwnerKey: "ok6"}
	cp := &stubCheckpointer{blob: []byte("some checkpoint blob")}
	store := &stubStore{}

	stored, err := CheckpointAndStore(context.Background(), cp, store, key, SnapshotMetadata{
		Key:     key,
		Runtime: "runsc",
		Format:  "gvisor-checkpoint",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored.Version == "" {
		t.Error("expected non-empty Version")
	}
	if stored.SizeBytes == 0 {
		t.Error("expected SizeBytes > 0")
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected 1 Put, got %d", len(store.puts))
	}
	if !bytes.Equal(store.puts[0].data, []byte("some checkpoint blob")) {
		t.Errorf("blob mismatch: got %q", store.puts[0].data)
	}
}
