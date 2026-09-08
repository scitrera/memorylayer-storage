// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package snapshot provides a pluggable storage backend for sandbox checkpoint
// blobs (opaque byte streams) with keying, versioning, and an optional
// local-filesystem write-through cache.
//
// Three backends are provided:
//
//   - LocalStore  — filesystem-backed; suitable for development and single-node
//     deployments. Blobs live under a configurable root directory.
//   - S3Store     — backed by any S3-compatible object store (AWS S3, RustFS,
//     Cloudflare R2, Backblaze B2). Handles multipart upload transparently.
//   - CacheStore  — decorator that wraps any SnapshotStore with a local
//     filesystem LRU cache capped at a configurable byte size.
//
// All I/O is streaming so callers do not need to buffer large checkpoint images
// in memory. The package treats blob contents as opaque; the provider decides
// the format (gVisor checkpoint image, dill/uv-freeze tar, etc.).
//
// This package is a foundation layer. Provider wiring (Checkpoint/Restore
// methods on the Sandbox interface, IdleReaper integration, and environment
// variable parsing) is handled in a separate slice.
package snapshot
