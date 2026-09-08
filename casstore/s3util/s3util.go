// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package s3util centralizes construction of an aws-sdk-go-v2 *s3.Client for
// the family of S3-backed stores in this monorepo (casstore snapshot, blobgw
// s3stage, blobgw-s3 smoke harness). It speaks AWS S3 and any S3-compatible
// service (RustFS, Cloudflare R2, Backblaze B2) via the optional Endpoint field.
//
// The construction logic here is the consolidation of three previously
// copy-pasted blocks. It uses the current aws-sdk-go-v2 endpoint API
// (s3.Options.BaseEndpoint + UsePathStyle) — the deprecated
// WithEndpointResolverWithOptions/aws.Endpoint resolver was removed here
// (TECH_DEBT #36); centralizing first made that a one-line, one-place change.
package s3util

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config carries the configuration for an S3-backed client. It works with AWS
// S3 and any S3-compatible service via the optional Endpoint field.
type Config struct {
	Bucket         string // required by callers (not validated here)
	Prefix         string // optional bucket-level key prefix (no trailing slash)
	Region         string // required for real AWS; may be "us-east-1" for RustFS
	Endpoint       string // optional; non-empty selects S3-compatible mode
	AccessKey      string // optional; falls back to standard AWS credential chain
	SecretKey      string // optional; falls back to standard AWS credential chain
	SessionToken   string // optional; STS session token for temporary credentials (ADR §2.9)
	ForcePathStyle bool   // true for RustFS; false for AWS
}

// NewClient builds an *s3.Client from cfg. The provided ctx is used only during
// construction to load the AWS config.
//
// Resolution rules (preserved from the original call sites):
//   - Region is applied only when non-empty.
//   - Static credentials are applied only when BOTH AccessKey and SecretKey are
//     set; otherwise the standard AWS credential chain is used. SessionToken is
//     threaded into the static credentials provider (its 3rd arg) when present,
//     so STS temporary credentials drive the client; an empty token preserves
//     today's plain static-key behavior.
//   - A custom endpoint (BaseEndpoint) is set only when Endpoint is non-empty.
//   - Path-style addressing is enabled only when ForcePathStyle is true. For an
//     S3-compatible endpoint (RustFS/R2/B2) this replaces the old resolver's
//     HostnameImmutable flag — UsePathStyle keeps the bucket in the path rather
//     than the host, which is what HostnameImmutable achieved.
func NewClient(ctx context.Context, cfg Config) (*s3.Client, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error

	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3util: load aws config: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		if cfg.ForcePathStyle {
			o.UsePathStyle = true
		}
	}), nil
}
