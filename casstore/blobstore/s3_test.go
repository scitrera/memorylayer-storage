// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import "testing"

// TestS3Options_ThreadsSessionToken verifies the S3Config → kopia s3.Options
// mapping carries the STS session token (ADR §2.9 / §7.8): with a non-empty
// SessionToken set, the constructed s3.Options.SessionToken must match so STS
// temporary credentials can drive the chunk-store S3 client for direct I/O.
func TestS3Options_ThreadsSessionToken(t *testing.T) {
	cfg := S3Config{
		Bucket:       "tenant-bkt",
		Prefix:       "chunks",
		Region:       "us-east-1",
		Endpoint:     "minio.local:9000",
		AccessKey:    "AK",
		SecretKey:    "SK",
		SessionToken: "session-tok",
		DoNotUseTLS:  true,
	}

	opts := s3Options(cfg)

	if opts.SessionToken != "session-tok" {
		t.Errorf("s3.Options.SessionToken = %q, want %q", opts.SessionToken, "session-tok")
	}
	if opts.AccessKeyID != cfg.AccessKey || opts.SecretAccessKey != cfg.SecretKey {
		t.Errorf("creds mismatch: AccessKeyID=%q SecretAccessKey=%q", opts.AccessKeyID, opts.SecretAccessKey)
	}
	if opts.BucketName != cfg.Bucket || opts.Prefix != cfg.Prefix ||
		opts.Region != cfg.Region || opts.Endpoint != cfg.Endpoint {
		t.Errorf("locator mismatch: %+v from %+v", opts, cfg)
	}
}

// TestS3Options_EmptySessionTokenIsBackwardCompatible verifies the additive
// change is backward-compatible: a zero SessionToken leaves s3.Options.SessionToken
// empty (today's static-key / default-credential-chain behavior).
func TestS3Options_EmptySessionTokenIsBackwardCompatible(t *testing.T) {
	cfg := S3Config{Bucket: "b", AccessKey: "AK", SecretKey: "SK"}
	if got := s3Options(cfg).SessionToken; got != "" {
		t.Errorf("s3.Options.SessionToken = %q, want empty for unset token", got)
	}
}
