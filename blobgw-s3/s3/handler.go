// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package s3 is a transparent S3-compatible front end over the blobgw object
// gateway. Off-the-shelf S3 clients (aws-sdk-go-v2, aws-cli) get casstore's
// content-addressed dedup and compression for free: the handler authenticates
// SigV4 requests, maps bucket→dedup-domain and object-key→gateway-ref, and
// translates the S3 object operations into Gateway.Put/Get/Head/Delete.
//
// Stage 1 implements path-style PutObject, GetObject, HeadObject, and
// DeleteObject with header, single-chunk, and streaming SigV4 payload modes.
// Stage 2 adds ListObjectsV2 (prefix / delimiter / continuation-token /
// start-after / max-keys) plus a minimal marker-based ListObjects v1, the bucket
// lifecycle operations (CreateBucket / HeadBucket / DeleteBucket / ListBuckets)
// behind an optional BucketRegistry, and Range GET (206 / 416). Multipart upload
// is a later stage; the bucket/key→ref mapping and the metadata sidecar are
// structured so it slots in without reworking the auth or routing paths.
package s3

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// Router resolves a dedup domain to its Gateway. *gateway.TenantRouter satisfies
// it directly; tests can supply a single-gateway adapter.
type Router interface {
	For(domain string) (*gateway.Gateway, error)
}

// Config carries the handler's collaborators. Region defaults to "us-east-1"
// (the SigV4 region clients sign with against a generic endpoint) but is not
// itself enforced — the credential scope's region is taken from the request.
type Config struct {
	Router   Router
	Creds    CredentialStore
	Resolver BucketResolver // nil → route every bucket to the credential's domain
	// Buckets, when non-nil, enables the bucket lifecycle operations
	// (CreateBucket / HeadBucket / DeleteBucket / ListBuckets) and switches on
	// bucket-existence enforcement for object operations (an unregistered bucket
	// yields NoSuchBucket). When nil, Stage-1 behavior is preserved: buckets are
	// implicit and every bucket routes to the credential's domain. Supply
	// NewMemoryBucketRegistry() (or a durable impl) to opt in.
	Buckets BucketRegistry
	// MPU holds the in-flight multipart-upload state (uploadId → upload record +
	// parts). nil → an in-memory store, matching Meta/Buckets. A later stage can
	// supply a durable (Postgres) implementation.
	MPU       mpuStore
	Now       func() time.Time
	MaxObject int64 // reject PutObject bodies larger than this (0 = unlimited)
}

type handler struct {
	router   Router
	creds    CredentialStore
	resolver BucketResolver
	buckets  BucketRegistry
	mpu      mpuStore
	now      func() time.Time
	maxObj   int64
}

// New returns an http.Handler speaking the Stage-1 S3 REST API over cfg.Router.
func New(cfg Config) http.Handler {
	h := &handler{
		router:   cfg.Router,
		creds:    cfg.Creds,
		resolver: cfg.Resolver,
		buckets:  cfg.Buckets,
		mpu:      cfg.MPU,
		now:      cfg.Now,
		maxObj:   cfg.MaxObject,
	}
	if h.resolver == nil {
		// With a registry, resolve registered buckets to their pinned domain and
		// fall back to the credential's domain for not-yet-created buckets (keeps
		// Stage-1 single-tenant flows working). Without a registry, every bucket
		// routes to the credential's domain.
		if h.buckets != nil {
			h.resolver = registryResolver{reg: h.buckets, fallback: credentialDomainResolver{}}
		} else {
			h.resolver = credentialDomainResolver{}
		}
	}
	if h.mpu == nil {
		h.mpu = newMemoryMPUStore()
	}
	if h.now == nil {
		h.now = time.Now
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitPath(r.URL.Path)

	// Service-level request: GET / → ListBuckets. The bucket/key are both empty.
	if bucket == "" {
		if r.Method == http.MethodGet {
			cred, err := h.authenticateService(r)
			if err != nil {
				writeError(w, r.URL.Path, err)
				return
			}
			h.listBuckets(w, r, cred)
			return
		}
		writeError(w, r.URL.Path, errInvalidRequest)
		return
	}

	cred, domain, err := h.authenticate(r, bucket)
	if err != nil {
		writeError(w, r.URL.Path, err)
		return
	}

	gw, err := h.router.For(domain)
	if err != nil {
		writeError(w, r.URL.Path, errNoSuchBucket)
		return
	}

	// Bucket-level request (no object key): CreateBucket / HeadBucket /
	// DeleteBucket / ListObjects(V2).
	if key == "" {
		h.serveBucket(w, r, gw, domain, bucket, cred)
		return
	}

	// Object-level request. Enforce bucket existence when a registry is wired.
	if h.buckets != nil {
		if _, ok := h.buckets.get(bucket); !ok {
			writeError(w, r.URL.Path, errNoSuchBucket)
			return
		}
	}

	q := r.URL.Query()
	switch r.Method {
	case http.MethodPost:
		// Multipart: ?uploads initiates; ?uploadId=U completes.
		if _, ok := q["uploads"]; ok {
			h.createMultipartUpload(w, r, domain, bucket, key)
			return
		}
		if uploadID := q.Get("uploadId"); uploadID != "" {
			h.completeMultipartUpload(w, r, gw, domain, key, uploadID)
			return
		}
		writeError(w, r.URL.Path, errInvalidRequest)
	case http.MethodPut:
		// Multipart UploadPart carries ?partNumber=N&uploadId=U; otherwise a plain
		// PutObject.
		if uploadID := q.Get("uploadId"); uploadID != "" {
			n, err := strconv.Atoi(q.Get("partNumber"))
			if err != nil {
				writeError(w, r.URL.Path, errInvalidArgument)
				return
			}
			h.uploadPart(w, r, gw, uploadID, n, cred)
			return
		}
		h.putObject(w, r, gw, domain, key, cred)
	case http.MethodGet:
		if uploadID := q.Get("uploadId"); uploadID != "" {
			h.listParts(w, r, bucket, key, uploadID)
			return
		}
		h.getObject(w, r, gw, domain, key, true)
	case http.MethodHead:
		h.getObject(w, r, gw, domain, key, false)
	case http.MethodDelete:
		if uploadID := q.Get("uploadId"); uploadID != "" {
			h.abortMultipartUpload(w, r, gw, uploadID)
			return
		}
		h.deleteObject(w, r, gw, domain, key)
	default:
		writeError(w, r.URL.Path, errMethodNotAllowed)
	}
}

// serveBucket dispatches bucket-level operations (no object key in the path):
// GET lists objects, PUT creates the bucket, HEAD probes existence, DELETE
// removes an empty bucket.
func (h *handler) serveBucket(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, domain, bucket string, cred Credential) {
	switch r.Method {
	case http.MethodGet:
		if _, ok := r.URL.Query()["uploads"]; ok {
			h.listMultipartUploads(w, r, bucket)
			return
		}
		h.listObjects(w, r, gw, domain, bucket)
	case http.MethodPut:
		h.createBucket(w, r, domain, bucket)
	case http.MethodHead:
		h.headBucket(w, r, bucket)
	case http.MethodDelete:
		h.deleteBucket(w, r, gw, bucket)
	default:
		writeError(w, r.URL.Path, errMethodNotAllowed)
	}
}

// splitPath splits a path-style URL path "/{bucket}/{key...}" into bucket and
// key. The key keeps any embedded slashes. A bare "/{bucket}" yields an empty
// key.
func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", ""
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

// authenticate verifies the request's SigV4 signature and resolves the bucket to
// a dedup domain. It returns the matched credential and the resolved domain.
func (h *handler) authenticate(r *http.Request, bucket string) (Credential, string, error) {
	scope, err := parseAuthorization(r.Header.Get(authorizationHeader))
	if err != nil {
		return Credential{}, "", err
	}
	cred, ok := h.creds.Lookup(scope.accessKeyID)
	if !ok {
		return Credential{}, "", errInvalidAccessKeyID
	}
	if err := checkRequestTime(r.Header.Get(amzDateHeader), h.now()); err != nil {
		return Credential{}, "", err
	}

	payloadHash := r.Header.Get(amzContentSHA)
	if payloadHash == "" {
		// Clients always send x-amz-content-sha256 under SigV4; its absence means
		// the request was not signed the way we verify.
		return Credential{}, "", errAuthorizationHeaderForm
	}
	if _, err := verifySeedSignature(r, scope, cred.SecretKey, payloadHash); err != nil {
		return Credential{}, "", err
	}

	domain, ok := h.resolver.Resolve(cred, bucket)
	if !ok {
		return Credential{}, "", errAccessDenied
	}
	return cred, domain, nil
}

// authenticateService verifies a service-scoped request's SigV4 signature (no
// bucket to resolve) and returns the matched credential. Used by ListBuckets,
// whose URL addresses no bucket.
func (h *handler) authenticateService(r *http.Request) (Credential, error) {
	scope, err := parseAuthorization(r.Header.Get(authorizationHeader))
	if err != nil {
		return Credential{}, err
	}
	cred, ok := h.creds.Lookup(scope.accessKeyID)
	if !ok {
		return Credential{}, errInvalidAccessKeyID
	}
	if err := checkRequestTime(r.Header.Get(amzDateHeader), h.now()); err != nil {
		return Credential{}, err
	}
	payloadHash := r.Header.Get(amzContentSHA)
	if payloadHash == "" {
		return Credential{}, errAuthorizationHeaderForm
	}
	if _, err := verifySeedSignature(r, scope, cred.SecretKey, payloadHash); err != nil {
		return Credential{}, err
	}
	return cred, nil
}

// putObject stores the request body through the gateway (dedup + compression
// happen there), computing the MD5 ETag in-stream, and persists the S3
// presentation metadata (ETag + x-amz-meta-*) DURABLY in the gateway object's
// user metadata — no in-memory sidecar — so a fresh handler over the same
// backend recovers it after a restart. The body is decoded on the fly for
// streaming-signed uploads so large objects are never fully buffered.
func (h *handler) putObject(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, _, key string, cred Credential) {
	body, err := h.objectBody(r, cred)
	if err != nil {
		writeError(w, r.URL.Path, err)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// The ETag must reflect exactly the stored bytes, but the gateway computes the
	// content hash in-stream — so we tee the body through MD5 as the gateway
	// consumes it, then bind the resulting S3 presentation metadata (ETag +
	// x-amz-meta-*) to the just-written object. SetUserMeta rewrites only the
	// manifest (the bytes already dedup to the same chunks), so the durable bind
	// is cheap and the ETag survives a restart from the manifest itself.
	md5h := md5.New()
	if _, err := gw.Put(r.Context(), key, contentType, io.TeeReader(body, md5h)); err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}
	etag := hex.EncodeToString(md5h.Sum(nil))
	if err := gw.SetUserMeta(r.Context(), key, buildObjectMeta(etag, userMetadata(r.Header))); err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// objectBody returns the decoded object body for a PutObject, transparently
// unwrapping a streaming aws-chunked payload (verifying each chunk's signature)
// or applying the optional size ceiling to a plain body.
func (h *handler) objectBody(r *http.Request, cred Credential) (io.Reader, error) {
	scope, err := parseAuthorization(r.Header.Get(authorizationHeader))
	if err != nil {
		return nil, err
	}
	body := io.Reader(r.Body)
	contentSHA := r.Header.Get(amzContentSHA)
	verifyHash := ""
	switch contentSHA {
	case streamingPayload:
		// Re-derive the seed signature + signing key the chunk chain hangs off of.
		seed, err := verifySeedSignature(r, scope, cred.SecretKey, streamingPayload)
		if err != nil {
			return nil, err
		}
		key := signingKey(cred.SecretKey, scope.date, scope.region, scope.service)
		body = newChunkReader(r.Body, key, r.Header.Get(amzDateHeader), scope.credentialScope(), seed)
	case unsignedPayload, "":
		// Body is intentionally excluded from the signature; nothing to verify.
	default:
		// A real hex digest: the client committed to the body's SHA-256 in the
		// signed header, so verify the consumed bytes hash to it. Defer the wrap
		// until after the size ceiling so the verifier sees the same EOF the
		// gateway does.
		verifyHash = strings.ToLower(contentSHA)
	}
	if h.maxObj > 0 {
		body = io.LimitReader(body, h.maxObj)
	}
	if verifyHash != "" {
		body = newSHA256VerifyReader(body, verifyHash)
	}
	return body, nil
}

// sha256VerifyReader wraps an object body whose x-amz-content-sha256 header is a
// real hex digest. It streams the body through SHA-256 and, at EOF, fails with
// XAmzContentSHA256Mismatch (BadDigest for a malformed expected digest) if the
// computed hash differs from the header — closing the gap where a signed
// single-chunk PutObject could carry a body that does not match its declared
// content hash.
type sha256VerifyReader struct {
	r    io.Reader
	h    hash.Hash
	want string // lower-case hex SHA-256 the body must hash to
}

func newSHA256VerifyReader(r io.Reader, want string) *sha256VerifyReader {
	return &sha256VerifyReader{r: r, h: sha256.New(), want: want}
}

func (v *sha256VerifyReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		_, _ = v.h.Write(p[:n])
	}
	if err == io.EOF {
		got := hex.EncodeToString(v.h.Sum(nil))
		if len(v.want) != len(got) {
			// A non-SHA-256-shaped expected digest is a client error in the header.
			return n, errBadDigest
		}
		if got != v.want {
			return n, errContentSHA256Mismatch
		}
	}
	return n, err
}

// getObject serves GetObject (withBody=true) and HeadObject (withBody=false).
// Both advertise Accept-Ranges and set the S3 response headers (ETag,
// Content-Type, Content-Length, Last-Modified) plus the surviving x-amz-meta-*
// user metadata. A GET carrying a Range header yields 206 Partial Content (or
// 416 for an unsatisfiable range); HEAD always reports the full size.
func (h *handler) getObject(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, _, key string, withBody bool) {
	if !withBody {
		info, err := gw.Head(r.Context(), key)
		if err != nil {
			writeError(w, r.URL.Path, mapGatewayErr(err))
			return
		}
		h.writeObjectHeaders(w, info)
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	rc, info, err := gw.Get(r.Context(), key)
	if err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}
	defer rc.Close()

	rng, ranged, satisfiable := parseRange(r.Header.Get("Range"), info.Size)
	if ranged && !satisfiable {
		// 416: surface the actual size so the client can re-request. We must not
		// have written any body bytes yet.
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(info.Size, 10))
		writeError(w, r.URL.Path, errInvalidRange)
		return
	}

	h.writeObjectHeaders(w, info)
	if !ranged {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)
		return
	}

	// 206 Partial Content. Whole-fetch-then-slice: discard up to the range start,
	// then stream exactly the range length. Ranged pack reads are deferred per
	// tech-debt #18, so the gateway gives us the full stream regardless.
	w.Header().Set("Content-Length", strconv.FormatInt(rng.length(), 10))
	w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(rng.start, 10)+"-"+
		strconv.FormatInt(rng.end, 10)+"/"+strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusPartialContent)
	if rng.start > 0 {
		if _, err := io.CopyN(io.Discard, rc, rng.start); err != nil {
			return // body truncated; nothing more we can do mid-stream
		}
	}
	_, _ = io.CopyN(w, rc, rng.length())
}

// writeObjectHeaders emits the S3 response headers for an object. The ETag and
// x-amz-meta-* user metadata are read from the gateway object's DURABLE user
// metadata (ObjectInfo.UserMeta, persisted in the casstore manifest), so they
// are correct even for a handler that never saw the original PutObject.
func (h *handler) writeObjectHeaders(w http.ResponseWriter, info gateway.ObjectInfo) {
	ct := info.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", info.CreatedAt.UTC().Format(http.TimeFormat))
	if etag := etagFromMeta(info.UserMeta); etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	for k, v := range userMetaFromMeta(info.UserMeta) {
		w.Header().Set("x-amz-meta-"+k, v)
	}
}

func (h *handler) deleteObject(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, _, key string) {
	if err := gw.Delete(r.Context(), key); err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// userMetadata extracts x-amz-meta-* request headers into a map keyed by the
// lower-cased suffix (S3 normalizes user-metadata keys to lower case).
func userMetadata(hdr http.Header) map[string]string {
	const prefix = "x-amz-meta-"
	var out map[string]string
	for k, vs := range hdr {
		lk := strings.ToLower(k)
		if suffix, ok := strings.CutPrefix(lk, prefix); ok && len(vs) > 0 {
			if out == nil {
				out = make(map[string]string)
			}
			out[suffix] = vs[0]
		}
	}
	return out
}

// mapGatewayErr translates gateway sentinels into S3 error codes. A NotFound
// becomes NoSuchKey (the bucket existed — we resolved its domain — so the miss
// is on the key); anything else is an internal error.
func mapGatewayErr(err error) error {
	// A body reader (streaming chunk decoder, hash verifier, size ceiling) can
	// surface a typed S3 error through Gateway.Put while it consumes the request
	// body; preserve that code rather than masking it as InternalError.
	var ae apiError
	if errors.As(err, &ae) {
		return ae
	}
	// The sentinel→category decision is centralized in gateway.Classify; the S3
	// layer keeps its OWN wire mapping to XML error codes. Only NotFound/InvalidRef
	// get specific codes; every other classification (including the gateway's
	// NotStaged/Integrity sentinels, which the S3 object path never produces) stays
	// InternalError.
	switch gateway.Classify(err) {
	case gateway.KindNotFound:
		return errNoSuchKey
	case gateway.KindInvalidRef:
		return errInvalidArgument
	default:
		return errInternal
	}
}
