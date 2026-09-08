// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package s3stage is an S3-backed StagingStore for blobgw's stage-then-finalize
// upload path (data-path A): clients PUT raw bytes straight to S3 via a
// presigned URL (bytes bypass blobgw), and Finalize reads them back through
// GetObject to chunk+dedup into casstore. It speaks AWS S3 and any
// S3-compatible service (RustFS, R2, B2) via the optional Endpoint field,
// mirroring casstore's S3 configuration.
package s3stage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/s3util"
)

// Config configures the S3 staging store. It is an alias for the shared
// s3util.Config (also surfaced as casstore's snapshot.S3Config); the alias
// keeps the s3stage.Config public name stable for callers while the S3 client
// construction lives in one place.
type Config = s3util.Config

// Store is an S3-backed StagingStore.
type Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

// Compile-time proof Store satisfies the gateway's StagingStore seam.
var _ gateway.StagingStore = (*Store)(nil)

// New constructs an S3 staging store. ctx is used only to load AWS config.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3stage: Bucket is required")
	}

	// Normalize a bare-host endpoint (e.g. "s3.us-east-2.amazonaws.com") to a valid
	// URI before it reaches the aws-sdk-go-v2 client: the SDK's endpoint resolver
	// rejects a scheme-less BaseEndpoint ("... was not a valid URI"), which silently
	// broke the staging presign (op=STAGE minted, POST /staged 500'd). The chunk-store
	// path already normalizes via tenantbind/convert; the staging store fed
	// o.s3Endpoint through raw, so mirror it here. EnsureEndpointScheme is idempotent
	// (empty and already-schemed pass through unchanged).
	cfg.Endpoint = tenantbind.EnsureEndpointScheme(cfg.Endpoint)
	client, err := s3util.NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("s3stage: %w", err)
	}
	return &Store{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
		prefix:  strings.TrimSuffix(cfg.Prefix, "/"),
	}, nil
}

func (s *Store) objectKey(stagingKey string) string {
	if s.prefix == "" {
		return stagingKey
	}
	return s.prefix + "/" + stagingKey
}

// PresignPut returns a presigned S3 PUT URL valid for ttl. Note: maxSize is
// advisory at this layer — a presigned PUT cannot cap the upload size without
// forcing an exact Content-Length match, so true size enforcement belongs to
// the L1.3 edge (or a bucket policy). It is accepted for interface parity.
func (s *Store) PresignPut(ctx context.Context, stagingKey string, _ int64, ttl time.Duration) (string, time.Time, error) {
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(stagingKey)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("s3stage: presign put %q: %w", stagingKey, err)
	}
	return req.URL, time.Now().UTC().Add(ttl), nil
}

// Open streams the staged object's bytes for Finalize to chunk+dedup.
func (s *Store) Open(ctx context.Context, stagingKey string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(stagingKey)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3stage: open %q: %w", stagingKey, err)
	}
	return out.Body, nil
}

// Delete removes a staged object after finalize (best-effort cleanup).
func (s *Store) Delete(ctx context.Context, stagingKey string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(stagingKey)),
	})
	if err != nil {
		return fmt.Errorf("s3stage: delete %q: %w", stagingKey, err)
	}
	return nil
}
