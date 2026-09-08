// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"
)

// roundTrip writes a payload through a CompressingStore over a LocalStore
// backend and asserts it reads back byte-identical with the expected
// compression tag.
func roundTrip(t *testing.T, algo CompressionAlgo, payload []byte) {
	t.Helper()

	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	store, err := NewCompressingStore(base, algo)
	if err != nil {
		t.Fatalf("NewCompressingStore: %v", err)
	}

	ctx := context.Background()
	key := SnapshotKey{Tenant: "t1", Workspace: "w1", User: "u1", OwnerKey: "owner-1"}
	meta := SnapshotMetadata{Runtime: "runc", Format: "test-blob"}

	stored, err := store.Put(ctx, key, meta, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantTag := string(algo)
	if algo == CompressNone {
		wantTag = ""
	}
	if got := stored.Tags[tagCompression]; got != wantTag {
		t.Fatalf("Tags[compression] = %q, want %q", got, wantTag)
	}

	rc, getMeta, err := store.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	if getMeta.Version != stored.Version {
		t.Fatalf("version mismatch: got %q want %q", getMeta.Version, stored.Version)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload round-trip mismatch (algo=%s): got %d bytes, want %d bytes", algo, len(got), len(payload))
	}
}

func TestCompressingStoreRoundTripZstd(t *testing.T) {
	roundTrip(t, CompressZstd, []byte(strings.Repeat("hello-zstd\n", 4096)))
}

func TestCompressingStoreRoundTripGzip(t *testing.T) {
	roundTrip(t, CompressGzip, []byte(strings.Repeat("hello-gzip\n", 4096)))
}

func TestCompressingStoreRoundTripNonePassThrough(t *testing.T) {
	// With CompressNone the decorator should not stamp the tag and should
	// behave indistinguishably from the underlying store.
	payload := []byte("uncompressed payload")
	roundTrip(t, CompressNone, payload)
}

func TestCompressingStoreActuallyShrinks(t *testing.T) {
	// Highly compressible payload — both gzip and zstd should yield a stored
	// blob smaller than the original.
	payload := make([]byte, 256*1024)
	// Half zeros, half a repeating pattern. Real-world compressibility.
	for i := range payload[len(payload)/2:] {
		payload[len(payload)/2+i] = byte(i % 17)
	}

	for _, algo := range []CompressionAlgo{CompressGzip, CompressZstd} {
		t.Run(string(algo), func(t *testing.T) {
			base, err := NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			store, err := NewCompressingStore(base, algo)
			if err != nil {
				t.Fatalf("NewCompressingStore: %v", err)
			}
			ctx := context.Background()
			key := SnapshotKey{Tenant: "t", OwnerKey: "o"}
			stored, err := store.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if stored.SizeBytes >= int64(len(payload)) {
				t.Fatalf("compressed size %d not smaller than original %d", stored.SizeBytes, len(payload))
			}
		})
	}
}

func TestCompressingStoreBackwardCompatibleReadOfUntaggedBlob(t *testing.T) {
	// Snapshots written before this decorator existed lack the compression
	// tag. The decorator must return them unmodified.
	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "legacy"}
	payload := []byte("legacy uncompressed snapshot")
	if _, err := base.Put(ctx, key, SnapshotMetadata{Runtime: "runc"}, bytes.NewReader(payload)); err != nil {
		t.Fatalf("seed base.Put: %v", err)
	}

	// Now wrap with zstd compression and ensure reads of the legacy blob
	// still succeed (the tag is absent → algo == none on read).
	store, err := NewCompressingStore(base, CompressZstd)
	if err != nil {
		t.Fatalf("NewCompressingStore: %v", err)
	}
	rc, _, err := store.GetLatest(ctx, key)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("legacy blob mismatch: got %q want %q", got, payload)
	}
}

func TestCompressingStoreDoesNotMutateCallerTags(t *testing.T) {
	// The decorator must stamp the compression tag on a copy of the caller's
	// map, never the original.
	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	store, err := NewCompressingStore(base, CompressZstd)
	if err != nil {
		t.Fatalf("NewCompressingStore: %v", err)
	}

	callerTags := map[string]string{"access_token": "secret"}
	meta := SnapshotMetadata{Runtime: "runc", Tags: callerTags}

	_, err = store.Put(context.Background(),
		SnapshotKey{Tenant: "t", OwnerKey: "o"},
		meta,
		bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, leaked := callerTags[tagCompression]; leaked {
		t.Fatalf("caller's tags map was mutated; compression tag leaked into %v", callerTags)
	}
}

func TestCompressingStoreLargeRandomPayload(t *testing.T) {
	// Stream a non-trivially-sized random payload to exercise the io.Pipe
	// goroutine + compression edge cases. Random data won't shrink much, so
	// we only assert byte-identity round-trip here.
	payload := make([]byte, 2*1024*1024) // 2 MiB
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	roundTrip(t, CompressZstd, payload)
}

func TestParseCompressionAlgo(t *testing.T) {
	cases := []struct {
		in   string
		want CompressionAlgo
		err  bool
	}{
		{"", CompressZstd, false},
		{"none", CompressNone, false},
		{"off", CompressNone, false},
		{"disabled", CompressNone, false},
		{"gzip", CompressGzip, false},
		{"zstd", CompressZstd, false},
		{"GZIP", "", true}, // case-sensitive on purpose
		{"snappy", "", true},
	}
	for _, c := range cases {
		got, err := ParseCompressionAlgo(c.in)
		if c.err {
			if err == nil {
				t.Errorf("ParseCompressionAlgo(%q) = (%q, nil), want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCompressionAlgo(%q) returned unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseCompressionAlgo(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCompressingStoreRejectsInvalidAlgo(t *testing.T) {
	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	_, err = NewCompressingStore(base, CompressionAlgo("snappy"))
	if err == nil {
		t.Fatal("expected error for invalid algorithm, got nil")
	}
}

func TestCompressingStoreDecompressFailureOnGarbageBlob(t *testing.T) {
	// If the metadata claims zstd but the blob is garbage, Get must return
	// an error rather than silently returning corrupt bytes.
	base, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx := context.Background()
	key := SnapshotKey{Tenant: "t", OwnerKey: "broken"}
	meta := SnapshotMetadata{
		Runtime: "runc",
		Tags:    map[string]string{tagCompression: string(CompressZstd)},
	}
	if _, err := base.Put(ctx, key, meta, bytes.NewReader([]byte("not zstd"))); err != nil {
		t.Fatalf("seed base.Put: %v", err)
	}
	store, err := NewCompressingStore(base, CompressZstd)
	if err != nil {
		t.Fatalf("NewCompressingStore: %v", err)
	}
	rc, _, err := store.GetLatest(ctx, key)
	if err == nil {
		// Some decoders defer the format error until first Read; tolerate
		// either Get-time or Read-time failure.
		_, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr == nil {
			t.Fatal("expected decompression error, got nil from both Get and Read")
		}
		return
	}
	// Get-time failure path: nothing more to check, error is what we wanted.
	_ = errors.Is(err, ErrNoSnapshot)
}
