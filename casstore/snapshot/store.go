// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"errors"
	"io"
	"time"
)

// SnapshotKey identifies the per-sandbox snapshot stream. Snapshots for a
// given key form a version sequence (latest, previous, etc.).
//
// OwnerKey is the post-Phase-0 successor to thread_id — an opaque
// scope-identifier supplied by the caller (tenant, user, workspace, and the
// per-(tenant,workspace,user) sandbox identity).
type SnapshotKey struct {
	Tenant    string
	Workspace string // may be empty
	User      string // may be empty
	OwnerKey  string // per-(tenant,workspace,user) sandbox identity
}

// SnapshotMetadata is the per-version descriptor stored alongside the blob.
// It is JSON-serialised as metadata.json next to the blob in every backend.
type SnapshotMetadata struct {
	Key       SnapshotKey       `json:"key"`
	Version   string            `json:"version"`    // sortable; generated as RFC3339Nano + 6-char hex suffix
	CreatedAt time.Time         `json:"created_at"` // UTC wall clock at Put time
	SizeBytes int64             `json:"size_bytes"` // computed from the stream; caller's value is ignored
	Runtime   string            `json:"runtime"`    // "runsc" | "runc"
	Format    string            `json:"format"`     // "gvisor-checkpoint" | "dill-pickle-tar" | future
	Tags      map[string]string `json:"tags,omitempty"`
}

// SnapshotStore is the contract every backend implements. Streaming I/O
// throughout so callers do not have to buffer large checkpoint images in memory.
type SnapshotStore interface {
	// Put writes a new snapshot version for the key. The store generates the
	// version string and stamps it on the returned metadata. SizeBytes on the
	// input meta is ignored; the store computes it from the actual stream.
	Put(ctx context.Context, key SnapshotKey, meta SnapshotMetadata, r io.Reader) (SnapshotMetadata, error)

	// Get retrieves a specific version. Caller must Close the reader.
	Get(ctx context.Context, key SnapshotKey, version string) (io.ReadCloser, SnapshotMetadata, error)

	// GetLatest is a convenience that resolves to the highest-version snapshot
	// for the key. Returns ErrNoSnapshot if none exist.
	GetLatest(ctx context.Context, key SnapshotKey) (io.ReadCloser, SnapshotMetadata, error)

	// GetLatestMetadata returns only the metadata for the highest-version
	// snapshot without opening the blob. Use this when only the tags are
	// needed (e.g. IPAM preferred-CIDR pinning on restore) to avoid the
	// cost of fetching a large checkpoint blob just to peek at metadata.
	// Returns ErrNoSnapshot if none exist.
	GetLatestMetadata(ctx context.Context, key SnapshotKey) (SnapshotMetadata, error)

	// List returns metadata for all versions of the key, newest first.
	List(ctx context.Context, key SnapshotKey) ([]SnapshotMetadata, error)

	// Delete removes a specific version. Returns nil if already absent.
	Delete(ctx context.Context, key SnapshotKey, version string) error

	// Walk yields every (key, version) tuple in the store, in unspecified
	// order. The yield callback returns an error to abort enumeration
	// early; the same error is returned from Walk. Used by the chunked-
	// store GC to compute the live set of referenced chunks across all
	// snapshots. Implementations should stream results to bound memory
	// when the store is large.
	Walk(ctx context.Context, yield func(key SnapshotKey, version string, meta SnapshotMetadata) error) error
}

// ErrNoSnapshot is returned by Get / GetLatest when no snapshot exists for
// the given key (or version).
var ErrNoSnapshot = errors.New("snapshot: not found")

// ErrSnapshotUnsupported is returned by Sandbox.Checkpoint and Sandbox.Restore
// on providers that do not support native kernel-level checkpoint/restore
// (docker, k8s). Callers should fall back to their workload-level snapshot
// strategy (dill/uv-freeze) for those runtimes.
var ErrSnapshotUnsupported = errors.New("snapshot: runtime does not support native checkpoint/restore")

// segmentOf returns the path segment to use for a potentially-empty string
// field. Empty values become the literal underscore "_" so the directory
// layout is always consistent (no variable-depth paths).
func segmentOf(s string) string {
	if s == "" {
		return "_"
	}
	return s
}
