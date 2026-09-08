// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"sort"
	"sync"
	"time"
)

// bucketRecord is one registered bucket: the dedup domain its objects route
// into plus its creation time (reported by HeadBucket/ListBuckets). The domain
// is fixed at CreateBucket time so the bucket→domain mapping is stable even if
// the creating credential later changes which domains it can reach.
type bucketRecord struct {
	Domain    string
	CreatedAt time.Time
}

// BucketRegistry is the bucket → (domain, creation time) index backing the
// bucket lifecycle operations (CreateBucket / HeadBucket / DeleteBucket /
// ListBuckets). It is split behind an interface so a later stage can persist it
// (Postgres) the same way the ref store and meta store are. Implementations
// must be safe for concurrent use.
//
// The registry is optional: a handler built without one keeps Stage-1's
// behavior (every bucket routes to the authenticated credential's domain and no
// existence check is enforced). Supplying a registry switches on existence
// checks — an object request for an unregistered bucket then yields NoSuchBucket
// — while the credential-domain resolver still provides the domain for the
// registered record.
type BucketRegistry interface {
	// create registers bucket with the given domain and creation time. It
	// reports ok=false (without overwriting) if the bucket already exists, so
	// the caller can answer BucketAlreadyOwnedByYou.
	create(bucket, domain string, createdAt time.Time) (ok bool)
	// get returns the record for bucket, ok=false if unregistered.
	get(bucket string) (bucketRecord, bool)
	// del removes bucket. Removing an absent bucket is a no-op.
	del(bucket string)
	// list returns every registered bucket, sorted by name (S3 ListBuckets
	// orders lexicographically).
	list() []listedBucket
}

// listedBucket is one row of ListBuckets: the bucket name and its creation time.
type listedBucket struct {
	Name      string
	CreatedAt time.Time
}

// memoryBucketRegistry is an in-process BucketRegistry for dev/tests/single-node,
// matching the in-memory ref/meta stores the rest of the stack uses.
type memoryBucketRegistry struct {
	mu sync.RWMutex
	m  map[string]bucketRecord
}

// NewMemoryBucketRegistry constructs an empty in-memory bucket registry.
func NewMemoryBucketRegistry() BucketRegistry {
	return &memoryBucketRegistry{m: make(map[string]bucketRecord)}
}

func (r *memoryBucketRegistry) create(bucket, domain string, createdAt time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[bucket]; exists {
		return false
	}
	r.m[bucket] = bucketRecord{Domain: domain, CreatedAt: createdAt}
	return true
}

func (r *memoryBucketRegistry) get(bucket string) (bucketRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.m[bucket]
	return rec, ok
}

func (r *memoryBucketRegistry) del(bucket string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, bucket)
}

func (r *memoryBucketRegistry) list() []listedBucket {
	r.mu.RLock()
	out := make([]listedBucket, 0, len(r.m))
	for name, rec := range r.m {
		out = append(out, listedBucket{Name: name, CreatedAt: rec.CreatedAt})
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// registryResolver resolves a bucket to its registered domain, falling back to
// the credential's own domain for buckets the registry doesn't know about. This
// keeps Stage-1's default-domain semantics intact (a never-created bucket still
// resolves, so existing single-tenant flows work) while letting CreateBucket
// pin a bucket to a specific domain. It is only installed when the handler is
// given a registry AND no explicit resolver; supplying a custom BucketResolver
// overrides it.
type registryResolver struct {
	reg      BucketRegistry
	fallback BucketResolver
}

func (rr registryResolver) Resolve(cred Credential, bucket string) (string, bool) {
	if rec, ok := rr.reg.get(bucket); ok {
		return rec.Domain, true
	}
	return rr.fallback.Resolve(cred, bucket)
}
