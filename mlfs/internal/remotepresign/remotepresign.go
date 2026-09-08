// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package remotepresign implements a node-side client that requests presigned
// S3 URLs from blobgw's control plane over NATS request-reply using the
// ctlproto wire contract (ADR-001 §2.2, §2.5, §2.9).
//
// An mlfs node creates one Client per NATS connection and calls PresignGet /
// PresignPut when it needs short-lived URLs to read or write pack blobs
// directly against the tenant S3 bucket.  blobgw is the sole holder of
// per-tenant bucket credentials; nodes remain credential-light (ADR-001 §2.2).
//
// # Transport reuse
//
// Client imports remoteindex.Requester and remoteindex.NatsRequester — it does
// NOT duplicate the NATS transport logic.  The same *NatsRequester instance can
// be shared between a RemoteDedupStore and a Client if desired.
//
// # Tenant validation
//
// The Domain field of every PresignBatchRequest is set to the tenant argument.
// blobgw validates that Domain matches the subject tenant (ADR-001 §7: subject-
// tenant validation is a necessary guard; per-account NATS auth is the deferred
// completion).  Callers MUST therefore pass the same tenant value they used to
// construct the NATS subject — the two are kept in sync here by deriving the
// subject from ctlproto.PresignSubject(tenant) and setting Domain to tenant.
//
// # Batch splitting
//
// PresignGet and PresignPut automatically split input key slices into sub-batches
// of at most ctlproto.MaxPresignKeysPerBatch to stay within the NATS 1 MB
// max-payload limit.
//
// # Expiry tracking
//
// blobgw may cap the presigned URL lifetime based on the credential impl (e.g.
// the AWS STS session cliff — see ADR-001 §2.9).  When a request is split into
// multiple sub-batches, each response carries its own ExpiresAtUnix.  The
// client returns the EARLIEST expiry across all batches so callers schedule
// refresh before the first URL in the set becomes invalid.
package remotepresign

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/memorylayer-storage/ctlproto"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
)

// defaultTimeout is applied when the caller's context has no deadline and no
// WithTimeout option was supplied.
const defaultTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// options holds optional configuration for Client.
type options struct {
	codec   ctlproto.Codec
	timeout time.Duration
}

// Option is a functional option for NewClient.
type Option func(*options)

// WithCodec overrides the codec used to marshal/unmarshal control-plane
// messages.  Defaults to ctlproto.DefaultCodec (currently JSONCodec).
func WithCodec(c ctlproto.Codec) Option {
	return func(o *options) { o.codec = c }
}

// WithTimeout sets the total per-call timeout applied when the caller's context
// has no deadline.  The timeout covers ALL sub-batches within a single
// PresignGet or PresignPut call — it is not applied per sub-batch.
// Defaults to 5 s.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client requests presigned S3 URLs from blobgw over NATS req-reply.
// It is safe for concurrent use.
type Client struct {
	r       remoteindex.Requester
	codec   ctlproto.Codec
	timeout time.Duration
}

// NewClient constructs a Client.  r must not be nil.  Optional Option values
// tune the codec and per-call timeout.
func NewClient(r remoteindex.Requester, opts ...Option) *Client {
	o := &options{
		codec:   ctlproto.DefaultCodec,
		timeout: defaultTimeout,
	}
	for _, fn := range opts {
		fn(o)
	}
	return &Client{r: r, codec: o.codec, timeout: o.timeout}
}

// withTimeout wraps ctx with the client's timeout when ctx has no deadline.
// The wrapping covers the TOTAL budget for all sub-batches within one call —
// do not call this inside the sub-batch loop.
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

// PresignGet requests presigned GET URLs for the given S3 object keys within
// the tenant bucket.  It returns a map from key to presigned URL and the
// earliest expiry time across all sub-batches (schedule a refresh before this
// time).
//
// Keys is split into sub-batches of at most ctlproto.MaxPresignKeysPerBatch.
// An empty keys slice returns an empty map and a zero expiresAt without sending
// any NATS request.
//
// The Domain field in each request is set to tenant; blobgw validates that
// Domain matches the subject tenant.
func (c *Client) PresignGet(ctx context.Context, tenant string, keys []string, ttl time.Duration) (urls map[string]string, expiresAt time.Time, err error) {
	return c.presign(ctx, tenant, ctlproto.PresignGet, keys, ttl)
}

// PresignPut requests presigned PUT URLs for the given S3 object keys within
// the tenant bucket.  Behaviour is identical to PresignGet except Op is PUT.
//
// The Domain field in each request is set to tenant; blobgw validates that
// Domain matches the subject tenant.
func (c *Client) PresignPut(ctx context.Context, tenant string, keys []string, ttl time.Duration) (urls map[string]string, expiresAt time.Time, err error) {
	return c.presign(ctx, tenant, ctlproto.PresignPut, keys, ttl)
}

// presign is the shared implementation for PresignGet and PresignPut.
func (c *Client) presign(ctx context.Context, tenant string, op ctlproto.PresignOp, keys []string, ttl time.Duration) (map[string]string, time.Time, error) {
	if len(keys) == 0 {
		return map[string]string{}, time.Time{}, nil
	}

	// withTimeout wraps the TOTAL budget for all sub-batches, not each one
	// individually — do not move this call inside the loop below.
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	subject := ctlproto.PresignSubject(tenant)
	ttlSec := int32(ttl.Seconds())

	result := make(map[string]string, len(keys))
	var (
		earliestExpiry int64
		haveExpiry     bool
	)

	for len(keys) > 0 {
		batch := keys
		if len(batch) > ctlproto.MaxPresignKeysPerBatch {
			batch = keys[:ctlproto.MaxPresignKeysPerBatch]
		}
		keys = keys[len(batch):]

		req := ctlproto.PresignBatchRequest{
			Domain:     tenant,
			Op:         op,
			Keys:       batch,
			TTLSeconds: ttlSec,
		}
		data, err := c.codec.Marshal(req)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("remotepresign: marshal PresignBatchRequest: %w", err)
		}

		raw, err := c.r.Request(ctx, subject, data)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("remotepresign: presign transport: %w", err)
		}

		var resp ctlproto.PresignBatchResponse
		if err := c.codec.Unmarshal(raw, &resp); err != nil {
			return nil, time.Time{}, fmt.Errorf("remotepresign: unmarshal PresignBatchResponse: %w", err)
		}
		if resp.Error != "" {
			return nil, time.Time{}, fmt.Errorf("remotepresign: server error: %s", resp.Error)
		}

		for k, u := range resp.URLs {
			result[k] = u
		}

		// Track the earliest expiry across batches: the node must refresh
		// before the first URL in the whole set becomes invalid.
		// ExpiresAtUnix==0 means the server signalled no expiry — skip it so
		// that a zero sentinel never wins as "earliest" and produces a 1970
		// timestamp that schedulers would treat as already-expired.
		if resp.ExpiresAtUnix > 0 && (!haveExpiry || resp.ExpiresAtUnix < earliestExpiry) {
			earliestExpiry = resp.ExpiresAtUnix
			haveExpiry = true
		}
	}

	if !haveExpiry {
		// No batch reported a positive expiry; return zero time ("no expiry").
		return result, time.Time{}, nil
	}
	return result, time.Unix(earliestExpiry, 0), nil
}
