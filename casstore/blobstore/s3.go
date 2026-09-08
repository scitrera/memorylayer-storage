// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo/blob/s3"
)

// S3Config configures the S3-backed blob store. Mirrors the SnapshotStore
// S3 env vars (SANDBOX_BLOBSTORE_S3_*) so operators can configure both
// stores with consistent naming. S3-compatible backends (RustFS,
// Cloudflare R2, Wasabi, Backblaze B2-S3) are configured by setting the
// Endpoint field; kopia's s3 driver auto-detects path-style addressing
// requirements per backend.
type S3Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// Prefix is prepended to every blob ID inside the bucket. Useful for
	// sharing one bucket across multiple services or environments.
	Prefix string

	// Region is the AWS region. Optional for some endpoints (RustFS,
	// generic S3-compatible). Required for AWS S3 itself.
	Region string

	// Endpoint, when non-empty, overrides the AWS default endpoint —
	// e.g. "rustfs.local:9000" for RustFS, "<account>.r2.cloudflarestorage.com"
	// for Cloudflare R2.
	Endpoint string

	// AccessKey + SecretKey are the IAM credentials. When empty, kopia
	// falls back to the AWS default credential chain (env, IMDS, etc.).
	AccessKey string
	SecretKey string

	// SessionToken is the STS session token for temporary credentials (e.g.
	// AssumeRoleWithWebIdentity, ADR §2.9). When empty (the default), behavior
	// is unchanged — static keys or the AWS default credential chain. When set,
	// it is threaded into kopia's s3.Options.SessionToken so temporary
	// credentials can drive the chunk-store S3 client for direct I/O.
	SessionToken string

	// DoNotUseTLS disables HTTPS. Use only for local RustFS dev setups —
	// production endpoints should always be TLS.
	DoNotUseTLS bool

	// DoNotVerifyTLS skips certificate verification. Use only for
	// self-signed local dev environments.
	DoNotVerifyTLS bool
}

// NewS3Storage opens an S3-backed blob store. Validates that Bucket is
// set; everything else is optional and falls back to the AWS default
// credential chain when unspecified.
func NewS3Storage(ctx context.Context, cfg S3Config) (Storage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("blobstore: S3Config.Bucket is required")
	}
	st, err := s3.New(ctx, s3Options(cfg), true /* isCreate */)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open s3 backend: %w", err)
	}
	return st, nil
}

// s3Options maps an S3Config onto kopia's s3.Options. It is split out from
// NewS3Storage so the field mapping (notably SessionToken for STS temporary
// credentials) can be unit-tested without opening a live backend (s3.New makes
// a network probe).
func s3Options(cfg S3Config) *s3.Options {
	return &s3.Options{
		BucketName:      cfg.Bucket,
		Prefix:          cfg.Prefix,
		Region:          cfg.Region,
		Endpoint:        cfg.Endpoint,
		AccessKeyID:     cfg.AccessKey,
		SecretAccessKey: cfg.SecretKey,
		SessionToken:    cfg.SessionToken,
		DoNotUseTLS:     cfg.DoNotUseTLS,
		DoNotVerifyTLS:  cfg.DoNotVerifyTLS,
	}
}
