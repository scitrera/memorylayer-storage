// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// Backend identifies which blob store implementation to construct.
type Backend string

const (
	// BackendNone disables the blob store. Open returns (nil, nil) — the
	// caller (typically internal/snapshot/store_chunked.go) treats this
	// as "chunked-dedup feature is disabled".
	BackendNone Backend = "none"

	// BackendLocal stores blobs on the local filesystem.
	BackendLocal Backend = "local"

	// BackendS3 stores blobs in an S3-compatible bucket.
	BackendS3 Backend = "s3"
)

// Env var names. Kept distinct from SANDBOX_SNAPSHOT_* so the chunked
// blob store can be configured independently of the snapshot store.
const (
	envBackend          = "SANDBOX_BLOBSTORE_BACKEND"
	envLocalDir         = "SANDBOX_BLOBSTORE_LOCAL_DIR"
	envS3Bucket         = "SANDBOX_BLOBSTORE_S3_BUCKET"
	envS3Prefix         = "SANDBOX_BLOBSTORE_S3_PREFIX"
	envS3Region         = "SANDBOX_BLOBSTORE_S3_REGION"
	envS3Endpoint       = "SANDBOX_BLOBSTORE_S3_ENDPOINT"
	envS3AccessKey      = "SANDBOX_BLOBSTORE_S3_ACCESS_KEY"
	envS3SecretKey      = "SANDBOX_BLOBSTORE_S3_SECRET_KEY"
	envS3DoNotUseTLS    = "SANDBOX_BLOBSTORE_S3_DO_NOT_USE_TLS"
	envS3DoNotVerifyTLS = "SANDBOX_BLOBSTORE_S3_DO_NOT_VERIFY_TLS"
)

// Open constructs a Storage from environment variables, mirroring the
// SnapshotStore env-driven construction pattern (see
// cmd/sandbox-service/main.go::newSnapshotStore). Returns (nil, nil)
// when SANDBOX_BLOBSTORE_BACKEND=none — callers should treat that as
// "chunked dedup disabled" rather than a configuration error.
func Open(ctx context.Context) (Storage, error) {
	backend := Backend(strings.ToLower(strings.TrimSpace(os.Getenv(envBackend))))
	if backend == "" {
		// No backend configured at all — same as none, for back-compat
		// with deployments that haven't opted into chunked dedup.
		return nil, nil
	}
	switch backend {
	case BackendNone:
		return nil, nil
	case BackendLocal:
		root := os.Getenv(envLocalDir)
		if root == "" {
			root = "/var/lib/sandbox-provider/blobs"
		}
		return NewLocalStorage(ctx, LocalConfig{Root: root})
	case BackendS3:
		return NewS3Storage(ctx, S3Config{
			Bucket:         os.Getenv(envS3Bucket),
			Prefix:         os.Getenv(envS3Prefix),
			Region:         os.Getenv(envS3Region),
			Endpoint:       os.Getenv(envS3Endpoint),
			AccessKey:      os.Getenv(envS3AccessKey),
			SecretKey:      os.Getenv(envS3SecretKey),
			DoNotUseTLS:    envBool(envS3DoNotUseTLS),
			DoNotVerifyTLS: envBool(envS3DoNotVerifyTLS),
		})
	default:
		return nil, fmt.Errorf("blobstore: unknown backend %q (valid: none, local, s3)", backend)
	}
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
