// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package blobstore is a thin adapter over github.com/kopia/kopia/repo/blob,
// exposing only the types sandbox-provider needs. The adapter exists so the
// rest of the codebase doesn't import kopia directly — if we ever swap
// implementations, only this package changes.
//
// We chose kopia/repo/blob over building our own BlobStore for these
// reasons (see .slop/blobstore-kopia-spike.md for the full evaluation):
//
//   - The Storage interface is almost exactly what we'd have designed,
//     including PutOptions.DoNotRecreate which is the If-None-Match
//     primitive we need for safe CAS Put.
//   - Production-tested backends for S3, GCS, Azure, B2, Backblaze,
//     filesystem, SFTP, WebDAV, and more — instead of writing our own.
//   - Decorators for retrying, sharding, throttling, metrics, logging
//     already exist.
//   - Apache 2.0 license, widely deployed via the kopia backup tool.
//
// What we explicitly did NOT adopt: kopia's repo/content/ layer (their
// full CAS / encrypted-pack implementation). That layer is shaped around
// a backup tool's requirements (encryption, sessions, BLAKE3 IDs) and
// would force too much of kopia's repository format on us. We build our
// own chunked-dedup logic on top of this adapter — see
// internal/snapshot/store_chunked.go.
package blobstore

import (
	"context"

	kblob "github.com/kopia/kopia/repo/blob"
)

// Storage is the BLOB storage interface every backend implements. See
// kopia/repo/blob.Storage for the full contract: read-after-write,
// atomicity, monotonic timestamps, durable storage.
//
// Aliased here so the rest of our codebase doesn't import kopia directly.
type Storage = kblob.Storage

// ID identifies a single blob. By convention sandbox-provider uses
// "{tenant}/blobs/{hash[:2]}/{hash}" for chunks and
// "{tenant}/manifests/{snapshot_key_path}/{version}" for snapshot manifests
// (the snapshot KEY path encoding is upstream's concern). The ID is opaque
// to kopia's blob layer; we pick the convention.
type ID = kblob.ID

// Metadata describes a single blob. The Timestamp field doubles as our
// "ETag" for mark-and-sweep GC: capture timestamps during the mark phase,
// re-check during sweep before delete (architect-recommended pattern;
// see .slop/blobstore-architect-review.md "What's missing #1").
type Metadata = kblob.Metadata

// PutOptions controls a single PutBlob. The DoNotRecreate field is the
// If-None-Match equivalent — when true the blob is written only if no
// blob currently exists at that ID; otherwise ErrBlobAlreadyExists is
// returned. This is the primitive we use to make chunk Puts idempotent
// over concurrent writers (different snapshots may chunk identical bytes
// and race to put them).
type PutOptions = kblob.PutOptions

// Bytes is kopia's chunked-buffer reader type. Use NewBytes to wrap a
// byte slice or use kopia's gather package for vectorised IO. We re-export
// the type so callers can build values without importing kopia directly.
type Bytes = kblob.Bytes

// OutputBuffer is kopia's writable-buffer type, used as the destination
// for GetBlob (rather than returning an io.ReadCloser). Use the helper
// AsReadCloser to bridge to streaming consumers.
type OutputBuffer = kblob.OutputBuffer

// Sentinel errors. Re-exported so callers can errors.Is against them
// without importing kopia.
var (
	// ErrBlobNotFound is returned by GetBlob/GetMetadata when the blob is
	// absent. errors.Is friendly.
	ErrBlobNotFound = kblob.ErrBlobNotFound

	// ErrBlobAlreadyExists is returned by PutBlob with DoNotRecreate=true
	// when a blob already exists at the ID. Treat as a successful no-op for
	// our content-addressed Put path (CAS == idempotent).
	ErrBlobAlreadyExists = kblob.ErrBlobAlreadyExists
)

// ListAllBlobs is a convenience over Storage.ListBlobs that materialises
// the entire result. Use the streaming ListBlobs callback for large
// listings; this helper is for one-shot operations and tests.
func ListAllBlobs(ctx context.Context, st Storage, prefix ID) ([]Metadata, error) {
	return kblob.ListAllBlobs(ctx, st, prefix)
}
