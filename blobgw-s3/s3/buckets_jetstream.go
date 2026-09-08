// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultBucketKVBucket is the JetStream KV bucket name the durable bucket
// registry uses when the caller does not override it.
const DefaultBucketKVBucket = "blobgw_s3_buckets"

// jetStreamBucketRegistry is a BucketRegistry backed by a JetStream KV bucket.
// It closes ADR-001 #37 for the bucket index: the bucket→(domain, created-at)
// records survive a blobgw-s3 restart because they live in NATS JetStream
// rather than process memory. Each registered bucket is one KV entry keyed by
// the (encoded) bucket name with a JSON-serialized bucketRecord value.
//
// The interface methods carry no context (they sit on the HTTP request path and
// match the in-memory impl's signatures), so KV calls use a background context;
// transport errors are logged and surfaced as a miss / no-op, preserving the
// in-memory store's fail-soft behavior.
type jetStreamBucketRegistry struct {
	kv jetstream.KeyValue
}

// NewJetStreamBucketRegistry opens (creating if absent) a JetStream KV bucket
// and returns a durable BucketRegistry over it. Pass DefaultBucketKVBucket for
// kvBucket to use the conventional name.
func NewJetStreamBucketRegistry(ctx context.Context, js jetstream.JetStream, kvBucket string) (BucketRegistry, error) {
	if kvBucket == "" {
		kvBucket = DefaultBucketKVBucket
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      kvBucket,
		Description: "blobgw-s3 bucket registry (ADR-001 #37)",
		Storage:     jetstream.FileStorage,
		History:     1,
	})
	if err != nil {
		return nil, err
	}
	return &jetStreamBucketRegistry{kv: kv}, nil
}

func (r *jetStreamBucketRegistry) create(bucket, domain string, createdAt time.Time) bool {
	if bucket == "" || domain == "" {
		return false
	}
	key := encodeKVKey(bucket)
	rec := bucketRecord{Domain: domain, CreatedAt: createdAt.UTC()}
	val, err := json.Marshal(rec)
	if err != nil {
		slog.Error("blobgw-s3: marshal bucket record", "bucket", bucket, "err", err)
		return false
	}
	// Create (not Put) so a concurrent/duplicate CreateBucket is rejected,
	// yielding BucketAlreadyOwnedByYou rather than silently overwriting.
	if _, err := r.kv.Create(context.Background(), key, val); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return false
		}
		slog.Error("blobgw-s3: create bucket record", "bucket", bucket, "err", err)
		return false
	}
	return true
}

func (r *jetStreamBucketRegistry) get(bucket string) (bucketRecord, bool) {
	if bucket == "" {
		return bucketRecord{}, false
	}
	entry, err := r.kv.Get(context.Background(), encodeKVKey(bucket))
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Error("blobgw-s3: get bucket record", "bucket", bucket, "err", err)
		}
		return bucketRecord{}, false
	}
	var rec bucketRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		slog.Error("blobgw-s3: unmarshal bucket record", "bucket", bucket, "err", err)
		return bucketRecord{}, false
	}
	return rec, true
}

func (r *jetStreamBucketRegistry) del(bucket string) {
	if bucket == "" {
		return
	}
	if err := r.kv.Delete(context.Background(), encodeKVKey(bucket)); err != nil &&
		!errors.Is(err, jetstream.ErrKeyNotFound) {
		slog.Error("blobgw-s3: delete bucket record", "bucket", bucket, "err", err)
	}
}

func (r *jetStreamBucketRegistry) list() []listedBucket {
	ctx := context.Background()
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Error("blobgw-s3: list bucket records", "err", err)
		}
		return nil
	}
	out := make([]listedBucket, 0, len(keys))
	for _, k := range keys {
		entry, err := r.kv.Get(ctx, k)
		if err != nil {
			continue // deleted between Keys and Get
		}
		var rec bucketRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			slog.Error("blobgw-s3: unmarshal bucket record", "key", k, "err", err)
			continue
		}
		name, err := decodeKVKey(k)
		if err != nil {
			slog.Error("blobgw-s3: decode bucket key", "key", k, "err", err)
			continue
		}
		out = append(out, listedBucket{Name: name, CreatedAt: rec.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// encodeKVKey maps an arbitrary identifier (bucket name, access key id, upload
// id) to a valid JetStream KV key. KV keys are restricted to a token charset
// (alnum plus -/_=.) and may not contain spaces or arbitrary bytes, so we
// URL-safe-base64 the raw identifier (no padding, "=" is dropped). The mapping
// is reversible via decodeKVKey for list operations.
func encodeKVKey(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeKVKey(key string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
