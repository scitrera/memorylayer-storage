// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// handlePresign serves blobgw.*.presign: resolve the SUBJECT tenant's binding,
// build a presign client over the binding's endpoint/region/creds, and mint one
// URL per key. All keys in a batch share one expiry (ExpiresAtUnix), which is the
// refresh deadline the node schedules against (ADR §2.9). The effective TTL is
// min(requested, presignMaxTTL).
//
// Tenant isolation (ADR §2.6/§2.9): the credential binding is resolved from the
// subject-validated tenant (TenantFromSubject/ValidateTenant), NEVER from the
// client-supplied req.Domain. req.Domain is a namespace key only; if a client
// supplies a non-empty Domain that does not match its own subject tenant, the
// request is REJECTED (defense-in-depth) so a client can never point presign at
// another tenant's bucket.
// handlePresign returns the effective handler outcome (nil on success, the error
// surfaced to the client otherwise) for the cp.* RED metrics.
func (s *Server) handlePresign(ctx context.Context, msg *nats.Msg) error {
	tenant, ok := s.tenantFromMsg(msg)
	if !ok {
		return nil
	}
	var req ctlproto.PresignBatchRequest
	if err := s.codec.Unmarshal(msg.Data, &req); err != nil {
		s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("decode presign request: %v", err)})
		return err
	}
	if err := validateDomain(req.Domain); err != nil {
		s.reply(msg, ctlproto.PresignBatchResponse{Error: err.Error()})
		return err
	}
	// Defense-in-depth: a non-empty Domain must match the subject tenant. The
	// binding (and thus the bucket) is always resolved from the subject tenant,
	// so a mismatching Domain is a sign of a confused or hostile client trying
	// to mint URLs scoped to a namespace it does not own — reject it rather than
	// silently ignoring the field.
	if req.Domain != "" && req.Domain != tenant {
		s.logger.Warn("controlplane: presign domain mismatch", "tenant", tenant, "domain", req.Domain)
		s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("domain %q does not match subject tenant %q", req.Domain, tenant)})
		return fmt.Errorf("domain %q does not match subject tenant %q", req.Domain, tenant)
	}
	if req.Op != ctlproto.PresignGet && req.Op != ctlproto.PresignPut {
		s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("invalid presign op %q (want GET or PUT)", req.Op)})
		return fmt.Errorf("invalid presign op %q", req.Op)
	}
	if len(req.Keys) > ctlproto.MaxPresignKeysPerBatch {
		s.reply(msg, ctlproto.PresignBatchResponse{Error: oversizeErr("presign keys", len(req.Keys), ctlproto.MaxPresignKeysPerBatch)})
		return errors.New("oversize batch")
	}

	cctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	// Resolve the binding from the SUBJECT tenant, never req.Domain: the minted
	// URLs are scoped to the subject tenant's bucket (binding.Bucket) +
	// binding.Prefix (ADR §2.6/§2.9). A client cannot redirect presign to
	// another tenant's bucket.
	binding, err := s.provider.BindingFor(cctx, tenant)
	if err != nil {
		s.logger.Error("controlplane: presign binding", "tenant", tenant, "err", err)
		s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("resolve binding: %v", err)})
		return err
	}

	presign, err := newPresignClient(cctx, binding)
	if err != nil {
		s.logger.Error("controlplane: presign client", "tenant", tenant, "err", err)
		s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("build presign client: %v", err)})
		return err
	}

	ttl := s.effectiveTTL(req.TTLSeconds)
	expiresAt := time.Now().UTC().Add(ttl)

	urls := make(map[string]string, len(req.Keys))
	for _, key := range req.Keys {
		objKey, err := objectKey(binding.Prefix, key)
		if err != nil {
			s.logger.Error("controlplane: presign key", "tenant", tenant, "key", key, "err", err)
			s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("presign %q: %v", key, err)})
			return err
		}
		url, err := presignOne(cctx, presign, binding.Bucket, objKey, req.Op, ttl)
		if err != nil {
			s.logger.Error("controlplane: presign key", "tenant", tenant, "key", key, "err", err)
			s.reply(msg, ctlproto.PresignBatchResponse{Error: fmt.Sprintf("presign %q: %v", key, err)})
			return err
		}
		urls[key] = url
	}

	s.reply(msg, ctlproto.PresignBatchResponse{
		URLs:          urls,
		ExpiresAtUnix: expiresAt.Unix(),
	})
	return nil
}

// effectiveTTL clamps the requested TTL to (0, presignMaxTTL]. A non-positive
// request defaults to the cap.
func (s *Server) effectiveTTL(requestedSeconds int32) time.Duration {
	if requestedSeconds <= 0 {
		return s.presignMaxTTL
	}
	requested := time.Duration(requestedSeconds) * time.Second
	if requested > s.presignMaxTTL {
		return s.presignMaxTTL
	}
	return requested
}

// newPresignClient builds an aws-sdk-go-v2 S3 presign client from a tenant
// binding. It is constructed directly (rather than via casstore's s3util) so the
// binding's SessionToken is threaded into the credentials: presigning with
// static keys works today, and the same path already signs correctly when the
// future STS impl supplies a session token (ADR §2.9). No network call is made
// — presigning is signing-only.
func newPresignClient(_ context.Context, b tenantbind.Binding) (*s3.PresignClient, error) {
	if b.Bucket == "" {
		return nil, fmt.Errorf("binding has empty bucket")
	}
	creds := credentials.NewStaticCredentialsProvider(
		b.Creds.AccessKeyID, b.Creds.SecretAccessKey, b.Creds.SessionToken,
	)
	opts := s3.Options{
		Region:      b.Region,
		Credentials: aws.NewCredentialsCache(creds),
	}
	if b.Endpoint != "" {
		// Normalize to a scheme'd URI (bare host "s3.us-east-2.amazonaws.com" →
		// "https://s3.us-east-2.amazonaws.com"); the aws-sdk endpoint resolver
		// rejects a scheme-less BaseEndpoint ("was not a valid URI"). The kopia
		// chunk store wants the bare host, so the binding stores bare and each
		// client normalizes — this is the SDK presign path.
		opts.BaseEndpoint = aws.String(tenantbind.EnsureEndpointScheme(b.Endpoint))
		// Per-tenant S3-compatible endpoints (RustFS/MinIO/R2) need path-style
		// addressing; real AWS ignores it for virtual-host buckets.
		opts.UsePathStyle = true
	}
	client := s3.New(opts)
	return s3.NewPresignClient(client), nil
}

// presignOne mints a single presigned URL for op against bucket/key.
func presignOne(ctx context.Context, presign *s3.PresignClient, bucket, key string, op ctlproto.PresignOp, ttl time.Duration) (string, error) {
	switch op {
	case ctlproto.PresignGet:
		req, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(ttl))
		if err != nil {
			return "", err
		}
		return req.URL, nil
	case ctlproto.PresignPut:
		req, err := presign.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(ttl))
		if err != nil {
			return "", err
		}
		return req.URL, nil
	default:
		return "", fmt.Errorf("unsupported presign op %q", op)
	}
}

// objectKey joins the binding prefix with the request key (no leading/trailing
// slash duplication), mirroring s3stage.objectKey. A request key containing a
// ".." path segment is rejected (prefix-escape hardening): otherwise a key like
// "../other-prefix/x" could mint a URL that resolves outside the tenant's
// binding.Prefix.
func objectKey(prefix, key string) (string, error) {
	if hasDotDotSegment(key) {
		return "", fmt.Errorf("key %q contains a %q path segment", key, "..")
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return key, nil
	}
	return prefix + "/" + key, nil
}

// hasDotDotSegment reports whether key contains a ".." path segment (bounded by
// "/" separators or the string ends). A literal "foo..bar" is fine; only a
// standalone ".." segment can escape the prefix.
func hasDotDotSegment(key string) bool {
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}
