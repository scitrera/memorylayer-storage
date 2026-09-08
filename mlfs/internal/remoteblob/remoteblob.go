// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package remoteblob implements the casstore blobstore.Storage interface for
// an mlfs node's direct-S3 data plane (ADR-001 §2.2, §5).
//
// A node never holds tenant S3 credentials. Instead it asks blobgw for a
// short-lived presigned URL (via remotepresign.Client over NATS) and then
// transfers bytes directly against S3 over plain HTTP (via s3http.Client).
// BlobStore wires those two primitives behind the kopia blob.Storage contract
// so the existing casstore chunk path can write/read packs without change.
//
// # Scope: what this adapter does NOT do
//
// The node data plane only ever calls PutBlob (whole pack) and GetBlob (whole
// pack today; ranged reads are wired through for #18). Listing, metadata and
// deletion are garbage-collection concerns, and GC runs on blobgw — it owns the
// per-tenant credentials and the authoritative index. Therefore GetMetadata,
// ListBlobs and DeleteBlob return ErrNotSupported rather than guessing at an
// implementation the node has no business performing.
//
// # Blob-ID → S3-object-key mapping
//
// casstore blob IDs use the convention "pack-<domain>-<hash>" /
// "chunk-<domain>-<hash>" (see casstore/blobstore.ID). Those strings are
// already valid, stable, printable-ASCII S3 object keys with no leading slash,
// so the default keyFn maps the ID verbatim: key == string(id). Both the node
// and blobgw must agree on the object key for a given blob; mapping the ID
// verbatim keeps that agreement trivial and avoids re-deriving structure on two
// sides. Callers that need a different layout (e.g. a "blobs/" prefix) supply
// WithKeyFunc.
//
// # Idempotent PutBlob
//
// Blob IDs are content hashes, so an unconditional presigned PUT is byte-
// idempotent: re-putting the same ID re-uploads identical bytes. We therefore
// IGNORE PutOptions.DoNotRecreate and never return ErrBlobAlreadyExists — the
// CAS contract is satisfied by the content-addressing itself.
package remoteblob

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/remotepresign"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"

	kblob "github.com/kopia/kopia/repo/blob"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// ErrNotSupported is returned by GetMetadata, ListBlobs and DeleteBlob. Those
// operations are owned by blobgw (it holds the credentials and the index and
// runs GC); the node's direct-S3 data plane intentionally does not implement
// them. errors.Is friendly.
var ErrNotSupported = errors.New("remoteblob: not supported on node: GC and listing are blobgw-owned")

// defaultPresignTTL is the requested validity of presigned URLs when the caller
// does not override it. blobgw may cap this lower (e.g. AWS STS session limit).
const defaultPresignTTL = 15 * time.Minute

// presignRefreshMargin is how long before a presigned GET URL's expiry the cache
// stops serving it and re-presigns, so a URL is never handed out with too little
// life left to finish an in-flight transfer.
const presignRefreshMargin = 60 * time.Second

// KeyFunc maps a blob ID to an S3 object key within the tenant bucket.
type KeyFunc func(blobstore.ID) string

// defaultKeyFunc maps the blob ID verbatim to the S3 object key. See the
// package doc "Blob-ID → S3-object-key mapping" for the rationale.
func defaultKeyFunc(id blobstore.ID) string { return string(id) }

// options holds optional configuration for BlobStore.
type options struct {
	keyFn      KeyFunc
	presignTTL time.Duration
}

// Option is a functional option for NewBlobStore.
type Option func(*options)

// WithKeyFunc overrides the blob-ID → S3-object-key mapping. The default maps
// the ID verbatim (key == string(id)).
func WithKeyFunc(fn KeyFunc) Option {
	return func(o *options) {
		if fn != nil {
			o.keyFn = fn
		}
	}
}

// WithPresignTTL overrides the requested presigned-URL validity. The default is
// 15 minutes. blobgw may cap the effective TTL lower.
func WithPresignTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.presignTTL = d
		}
	}
}

// BlobStore implements casstore blobstore.Storage over presigned-URL direct-S3
// transfers. It embeds kblob.DefaultProviderImplementation to inherit the no-op
// Close/FlushCaches/GetCapacity/ExtendBlobRetention/IsReadOnly methods. It is
// safe for concurrent use (its dependencies are).
type BlobStore struct {
	kblob.DefaultProviderImplementation

	presign *remotepresign.Client
	http    *s3http.Client
	tenant  string
	keyFn   KeyFunc
	ttl     time.Duration

	// Presigned GET-URL cache, keyed by S3 object key (== pack/chunk blob). A
	// presigned GET URL is range-INDEPENDENT (the byte range rides an HTTP Range
	// header, not the signature, see s3http.GetRange), so one URL serves every
	// ranged read of a pack for its whole validity. Reads re-touch the same packs
	// constantly — a slice's chunk-run, the framing-header probe, and every other
	// slice that shares the pack — so without this each of those paid a fresh
	// presign NATS round trip. Caching collapses them to one per pack per TTL,
	// removing a round trip from the per-slice cold-read cost (the readahead
	// bottleneck). singleflight collapses a concurrent first-touch herd (the
	// prefetcher fires many same-pack reads at once) into a single presign. PUTs
	// are not cached: a pack is written once, and reusing a PUT URL has no benefit.
	getMu    sync.Mutex
	getURLs  map[string]cachedURL
	getGroup singleflight.Group
}

// cachedURL is a presigned GET URL with the instant past which it must be
// re-presigned (its expiry less presignRefreshMargin).
type cachedURL struct {
	url       string
	refreshAt time.Time
}

// Compile-time assertion that BlobStore satisfies the full Storage contract.
var _ blobstore.Storage = (*BlobStore)(nil)

// NewBlobStore constructs a BlobStore. presign and http must not be nil; tenant
// is the domain whose bucket the node transfers against (it is forwarded to
// remotepresign for subject-tenant validation).
func NewBlobStore(presign *remotepresign.Client, http *s3http.Client, tenant string, opts ...Option) *BlobStore {
	o := &options{
		keyFn:      defaultKeyFunc,
		presignTTL: defaultPresignTTL,
	}
	for _, fn := range opts {
		fn(o)
	}
	return &BlobStore{
		presign: presign,
		http:    http,
		tenant:  tenant,
		keyFn:   o.keyFn,
		ttl:     o.presignTTL,
		getURLs: make(map[string]cachedURL),
	}
}

// cachedGetURL returns a presigned GET URL for key, reusing a cached one while it
// has more than presignRefreshMargin of life left and otherwise presigning a
// fresh one. Concurrent misses for the same key collapse into a single presign
// via singleflight (the presign uses a cancellation-decoupled context so one
// caller's cancellation can't poison the shared result for the others).
func (b *BlobStore) cachedGetURL(ctx context.Context, key string) (string, error) {
	now := time.Now()
	b.getMu.Lock()
	if e, ok := b.getURLs[key]; ok && now.Before(e.refreshAt) {
		url := e.url
		b.getMu.Unlock()
		return url, nil
	}
	b.getMu.Unlock()

	v, err, _ := b.getGroup.Do(key, func() (any, error) {
		// Re-check: a racing caller may have just filled the cache.
		now := time.Now()
		b.getMu.Lock()
		if e, ok := b.getURLs[key]; ok && now.Before(e.refreshAt) {
			b.getMu.Unlock()
			return e.url, nil
		}
		b.getMu.Unlock()

		urls, expiresAt, perr := b.presign.PresignGet(context.WithoutCancel(ctx), b.tenant, []string{key}, b.ttl)
		if perr != nil {
			return "", perr
		}
		url, ok := urls[key]
		if !ok || url == "" {
			return "", fmt.Errorf("no URL returned for key")
		}
		// blobgw reports the real expiry (capped per STS); expiresAt.IsZero() means
		// "no expiry", in which case fall back to the requested TTL as a local bound.
		refreshAt := expiresAt.Add(-presignRefreshMargin)
		if expiresAt.IsZero() {
			refreshAt = now.Add(b.ttl - presignRefreshMargin)
		}
		b.getMu.Lock()
		b.getURLs[key] = cachedURL{url: url, refreshAt: refreshAt}
		b.getMu.Unlock()
		return url, nil
	})
	if err != nil {
		return "", fmt.Errorf("remoteblob: presign GET %q: %w", key, err)
	}
	return v.(string), nil
}

// invalidateGetURL drops any cached GET URL for key, forcing the next read to
// re-presign. Called when a transfer fails so an expired/revoked URL (despite the
// refresh margin) is not served again.
func (b *BlobStore) invalidateGetURL(key string) {
	b.getMu.Lock()
	delete(b.getURLs, key)
	b.getMu.Unlock()
}

// presignURL requests a single presigned URL for key under op and returns it.
// A response that omits the requested key is treated as a descriptive error
// rather than an empty URL.
func (b *BlobStore) presignURL(
	ctx context.Context,
	key string,
	getURLs func(context.Context, string, []string, time.Duration) (map[string]string, time.Time, error),
	op string,
) (string, error) {
	urls, _, err := getURLs(ctx, b.tenant, []string{key}, b.ttl)
	if err != nil {
		return "", fmt.Errorf("remoteblob: presign %s %q: %w", op, key, err)
	}
	url, ok := urls[key]
	if !ok || url == "" {
		return "", fmt.Errorf("remoteblob: presign %s %q: no URL returned for key", op, key)
	}
	return url, nil
}

// PutBlob uploads data to the blob's S3 object via a presigned PUT.
//
// PutOptions.DoNotRecreate is ignored: blob IDs are content hashes so the PUT
// is byte-idempotent (see package doc). All other PutOptions are unused by the
// direct-S3 data plane.
func (b *BlobStore) PutBlob(ctx context.Context, id blobstore.ID, data blobstore.Bytes, _ blobstore.PutOptions) error {
	key := b.keyFn(id)

	url, err := b.presignURL(ctx, key, b.presign.PresignPut, "PUT")
	if err != nil {
		return err
	}

	if err := b.http.Put(ctx, url, data.Reader(), int64(data.Length())); err != nil {
		return fmt.Errorf("remoteblob: put %q: %w", key, err)
	}
	return nil
}

// GetBlob fetches full or partial blob contents into output.
//
// Per the kopia Reader.GetBlob contract (repo/blob/storage.go): length<0
// fetches the whole blob (the casstore case), length>0 fetches the range
// [offset, offset+length). A negative offset is invalid and returns
// blobstore.ErrInvalidRange. length==0 is also invalid (a zero-byte ranged
// read is undefined by the contract and would produce a degenerate HTTP Range
// header); it returns ErrInvalidRange without issuing any S3 request.
// The whole-blob path uses s3http.Get; the ranged path uses s3http.GetRange
// (wired through for #18 readahead).
func (b *BlobStore) GetBlob(ctx context.Context, id blobstore.ID, offset, length int64, output blobstore.OutputBuffer) error {
	if offset < 0 {
		return fmt.Errorf("remoteblob: GetBlob %q: negative offset %d: %w", id, offset, kblob.ErrInvalidRange)
	}
	if length == 0 {
		return fmt.Errorf("remoteblob: GetBlob %q: zero length is invalid: %w", id, kblob.ErrInvalidRange)
	}

	key := b.keyFn(id)

	url, err := b.cachedGetURL(ctx, key)
	if err != nil {
		return err
	}

	var bytesOut []byte
	if length < 0 {
		// Whole blob (the casstore chunk path always lands here today).
		bytesOut, err = b.http.Get(ctx, url)
	} else {
		// Ranged read — wired through for #18 readahead / parallel ranged GETs.
		bytesOut, err = b.http.GetRange(ctx, url, offset, length)
	}
	if err != nil {
		// A cached URL may have expired or been revoked despite the refresh margin;
		// drop it so the next read re-presigns rather than reusing a dead URL.
		b.invalidateGetURL(key)
		return fmt.Errorf("remoteblob: get %q: %w", key, err)
	}

	output.Reset()
	if _, err := output.Write(bytesOut); err != nil {
		return fmt.Errorf("remoteblob: get %q: write output: %w", key, err)
	}
	return nil
}

// GetMetadata is not supported on the node. See ErrNotSupported and the package
// doc: metadata/listing/deletion are blobgw-owned GC concerns.
func (b *BlobStore) GetMetadata(_ context.Context, _ blobstore.ID) (blobstore.Metadata, error) {
	return blobstore.Metadata{}, ErrNotSupported
}

// ListBlobs is not supported on the node. See ErrNotSupported and the package
// doc: metadata/listing/deletion are blobgw-owned GC concerns.
func (b *BlobStore) ListBlobs(_ context.Context, _ blobstore.ID, _ func(blobstore.Metadata) error) error {
	return ErrNotSupported
}

// DeleteBlob is not supported on the node. See ErrNotSupported and the package
// doc: metadata/listing/deletion are blobgw-owned GC concerns.
func (b *BlobStore) DeleteBlob(_ context.Context, _ blobstore.ID) error {
	return ErrNotSupported
}

// ConnectionInfo returns a minimal description of this storage. The node data
// plane has no serializable credential config to expose (credentials live on
// blobgw), so only the Type is set.
func (b *BlobStore) ConnectionInfo() kblob.ConnectionInfo {
	return kblob.ConnectionInfo{Type: "presigned-s3"}
}

// DisplayName returns a human-readable identifier for this storage.
func (b *BlobStore) DisplayName() string {
	return fmt.Sprintf("presigned-s3:%s", b.tenant)
}
