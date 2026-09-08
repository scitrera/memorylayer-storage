// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package gateway is the L1 object gateway over the casstore substrate. It
// turns casstore's content-addressed, deduplicated chunk store into a logical
// object API — PUT/GET/HEAD/DELETE/LIST over caller-named refs — plus a
// stage-then-finalize flow that reconciles presigned uploads with content
// addressing.
//
// An object is a logical blob identified by a ref (a stable caller key, e.g.
// enterprise's vfs_ref). Its bytes are stored through a casstore ChunkedStore
// configured with a dedup domain (typically the whole enterprise instance), so
// identical content across distinct refs is stored once. A RefStore holds the
// logical ref → content metadata index (the blob_ref table); the physical
// chunk→pack dedup index lives in casstore's DedupStore. The two never touch:
// authorization is on the ref, dedup is on the chunk.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// Sentinel errors. Callers (and the HTTP layer) map these to status codes.
var (
	// ErrNotFound is returned when no object exists for a ref.
	ErrNotFound = errors.New("blobgw: object not found")
	// ErrInvalidRef is returned for an empty or malformed ref.
	ErrInvalidRef = errors.New("blobgw: invalid ref")
	// ErrNotStaged is returned by Finalize when the ref has no pending staged
	// upload (e.g. it was never minted, or already finalized and re-finalized).
	ErrNotStaged = errors.New("blobgw: ref has no pending staged upload")
	// ErrIntegrity is returned by Finalize when the staged bytes don't match the
	// expectation captured at mint time (content hash and/or size). The ref is
	// NOT bound to the mismatched content; the staged object is deleted
	// best-effort. The HTTP layer maps this to 409 Conflict.
	ErrIntegrity = errors.New("blobgw: staged content failed integrity check")
)

// ObjectInfo is the per-ref metadata the gateway tracks — one row of the
// blob_ref table. ContentHash is the sha256 of the whole object (distinct from
// casstore's per-chunk hashes); it supports client-side idempotency and dedup
// negotiation. Pending/StagingKey are set only between MintRef and Finalize.
type ObjectInfo struct {
	Ref         string    `json:"ref"`
	Domain      string    `json:"domain"`
	ContentHash string    `json:"content_hash,omitempty"` // sha256 hex of full object bytes
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	Version     string    `json:"version,omitempty"` // casstore snapshot version backing this ref

	// UserMeta is the caller-supplied metadata attached on write (the optional
	// map passed to PutWithMeta / MintRef). It is stored DURABLY in the backing
	// casstore manifest's SnapshotMetadata.Tags — not in any in-memory sidecar —
	// so Get/Head recover it after a restart from the manifest itself. Callers
	// (e.g. the S3 layer) use it to round-trip presentation metadata such as the
	// S3 ETag and x-amz-meta-* headers. Empty/absent on objects written without
	// metadata. casstore's own internal tag keys are not surfaced here.
	UserMeta map[string]string `json:"user_meta,omitempty"`

	// Pending is true for a minted-but-not-finalized ref; StagingKey is where
	// its bytes are being uploaded. Both are cleared on Finalize.
	Pending    bool   `json:"pending,omitempty"`
	StagingKey string `json:"staging_key,omitempty"`

	// ExpectedHash/ExpectedSize are the OPTIONAL finalize integrity gate captured
	// at mint time (MintRefWithMeta). On Finalize, the staged bytes' computed
	// sha256/size must match any expectation set here or Finalize returns
	// ErrIntegrity without binding the ref. Zero values ("" / 0) mean "no
	// expectation, accept whatever is staged". They live on the pending row only;
	// finalized rows carry the same values for auditability but no longer gate.
	ExpectedHash string `json:"expected_hash,omitempty"` // expected sha256 hex of the staged bytes
	ExpectedSize int64  `json:"expected_size,omitempty"` // expected total size in bytes
}

// RefStore is the logical object index: ref → ObjectInfo, scoped by domain.
// blobgw (production) backs it with the same Postgres as the dedup index; the
// in-memory implementation is for tests and single-process use. Implementations
// must be safe for concurrent use.
type RefStore interface {
	// Put inserts or replaces the metadata for info.Ref within info.Domain.
	Put(ctx context.Context, info ObjectInfo) error
	// Get returns the object metadata for (domain, ref), ok=false if absent.
	Get(ctx context.Context, domain, ref string) (ObjectInfo, bool, error)
	// Delete removes the metadata row for (domain, ref). Absent is not an error.
	Delete(ctx context.Context, domain, ref string) error
	// List returns objects in domain whose ref has the given prefix, newest
	// first, capped at limit (<=0 means no cap). Pending entries are included.
	List(ctx context.Context, domain, prefix string, limit int) ([]ObjectInfo, error)
	// ListPage is the keyset-paginated form of List: it returns objects in
	// domain whose ref has the given prefix, in the same (created_at DESC, ref
	// ASC) order, starting strictly after the opaque cursor `after` ("" =
	// from the start). It returns at most limit objects (limit<=0 lets the
	// implementation pick a sane internal page size) and the cursor of the last
	// returned row when more rows remain (next=="" means the page is exhausted).
	// Cursor tokens are opaque to callers and produced only by this method.
	// Pending entries are included.
	ListPage(ctx context.Context, domain, prefix, after string, limit int) (objs []ObjectInfo, next string, err error)
	// ListStalePending returns pending (minted-but-not-finalized) objects in
	// domain whose CreatedAt is at or before olderThan, capped at limit (<=0
	// means no cap). It is the input to the staging sweeper: a pending row this
	// old has no in-flight Finalize (Finalize replaces the row, clearing
	// Pending), so its staging slot is safe to reclaim.
	ListStalePending(ctx context.Context, domain string, olderThan time.Time, limit int) ([]ObjectInfo, error)
}

// StagingStore backs the stage-then-finalize flow: it mints a short-lived
// upload target for raw bytes (an S3 presigned PUT in production) and lets
// Finalize read those bytes back to chunk+dedup them into casstore. The
// in-memory implementation serves tests; an S3 implementation provides the
// real presigned-to-bucket path.
type StagingStore interface {
	// PresignPut returns an upload URL the client PUTs raw bytes to, valid for
	// ttl and bounded by maxSize (0 = unbounded).
	PresignPut(ctx context.Context, stagingKey string, maxSize int64, ttl time.Duration) (url string, expiresAt time.Time, err error)
	// Open streams the bytes previously uploaded to stagingKey.
	Open(ctx context.Context, stagingKey string) (io.ReadCloser, error)
	// Delete removes the staged bytes (best-effort cleanup after finalize).
	Delete(ctx context.Context, stagingKey string) error
}

// Gateway serves the object API over a casstore ChunkedStore. The store must
// already be configured with the gateway's dedup domain and (for cross-object
// dedup) a GlobalIndex; the gateway does not own GC — wire a ChunkedGC
// separately. Staging may be nil if the deployment doesn't use presigned
// uploads.
type Gateway struct {
	store  *snapshot.ChunkedStore
	refs   RefStore
	stage  StagingStore
	domain string
	now    func() time.Time
	// validator, when set (WithChunkValidator), lets RegisterRef prove every chunk
	// it is asked to reference already exists in this domain's pack index before
	// binding a ref. nil = validation skipped (the caller vouches for the chunks).
	validator ChunkValidator
}

// Option configures a Gateway at construction. Options are applied in order after
// the required fields are set.
type Option func(*Gateway)

// New constructs a Gateway. domain is the logical ref namespace recorded on
// every ObjectInfo (typically the same string as the store's dedup domain).
// Optional Options wire cross-cutting concerns such as WithChunkValidator.
func New(store *snapshot.ChunkedStore, refs RefStore, stage StagingStore, domain string, opts ...Option) *Gateway {
	g := &Gateway{store: store, refs: refs, stage: stage, domain: domain, now: time.Now}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Domain reports the gateway's configured ref namespace.
func (g *Gateway) Domain() string { return g.domain }

func (g *Gateway) key(ref string) snapshot.SnapshotKey {
	return snapshot.SnapshotKey{Tenant: g.domain, OwnerKey: ref}
}

// Put stores r under ref, chunking+deduping through casstore, and records the
// object metadata. Re-putting a ref creates a new version; the ref resolves to
// the latest. The whole-object content hash is computed in-stream.
//
// Put attaches no user metadata; it is the back-compatible shorthand for
// PutWithMeta(ctx, ref, contentType, nil, r), preserved so existing callers
// (blobgw's own /objects server, the edge) compile unchanged.
func (g *Gateway) Put(ctx context.Context, ref, contentType string, r io.Reader) (ObjectInfo, error) {
	return g.PutWithMeta(ctx, ref, contentType, nil, r)
}

// PutWithMeta stores r under ref like Put, additionally attaching userMeta as
// DURABLE metadata: it is written into the backing casstore manifest's
// SnapshotMetadata.Tags, so Get/Head recover it from the manifest after a
// restart (no in-memory sidecar). userMeta keys must not collide with casstore's
// reserved internal tag namespace (see metaTagPrefix); the gateway namespaces
// them so they never clash with casstore's own tags (e.g. chunked_format).
// A nil/empty userMeta is equivalent to Put.
func (g *Gateway) PutWithMeta(ctx context.Context, ref, contentType string, userMeta map[string]string, r io.Reader) (ObjectInfo, error) {
	if ref == "" {
		return ObjectInfo{}, ErrInvalidRef
	}
	hc := newHashingCounter(r)
	// Carry the caller's Content-Type into SnapshotMetadata.Format so casstore's
	// compression policy can decide per object whether to compress the packs
	// (e.g. compress text/JSON, store already-compressed/GPU-loadable types
	// uncompressed). It does not affect chunking, dedup, or the content hash.
	sm, err := g.store.Put(ctx, g.key(ref), snapshot.SnapshotMetadata{
		Format: contentType,
		Tags:   encodeUserTags(userMeta),
	}, hc)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw put %q: store: %w", ref, err)
	}
	info := ObjectInfo{
		Ref:         ref,
		Domain:      g.domain,
		ContentHash: hc.hexSum(),
		Size:        hc.n,
		ContentType: contentType,
		CreatedAt:   g.now().UTC(),
		Version:     sm.Version,
		UserMeta:    cloneMeta(userMeta),
	}
	if err := g.refs.Put(ctx, info); err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw put %q: ref store: %w", ref, err)
	}
	return info, nil
}

// SetUserMeta replaces the durable user metadata on an existing object without
// changing its bytes. It is the post-write companion to PutWithMeta for callers
// that only learn the metadata after streaming the body (e.g. the S3 layer,
// which computes the MD5 ETag in-stream).
//
// It rewrites ONLY the backing casstore manifest in place (via
// ChunkedStore.UpdateLatestMeta), reusing the existing chunkRefs verbatim — no
// chunk is re-read, re-chunked, or re-uploaded. This fixes the 2x read
// amplification of the prior implementation, which re-streamed the whole object
// through casstore just to rewrite the manifest with the new Tags.
//
// Replace semantics (unchanged): the whole user-metadata namespace is replaced,
// not merged. Keys present before but absent from userMeta are dropped;
// casstore's own internal tags (e.g. chunked_format) are preserved. The new tag
// set is as durable as the object itself. Returns ErrNotFound if the ref is
// unknown or still pending.
func (g *Gateway) SetUserMeta(ctx context.Context, ref string, userMeta map[string]string) error {
	info, ok, err := g.refs.Get(ctx, g.domain, ref)
	if err != nil {
		return err
	}
	if !ok || info.Pending {
		return ErrNotFound
	}
	// Read the current durable manifest tags so we can clear any prior user
	// keys that the new set omits (replace, not merge) while leaving casstore's
	// internal tags untouched.
	sm, err := g.store.GetLatestMetadata(ctx, g.key(ref))
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return ErrNotFound
		}
		return fmt.Errorf("blobgw set-meta %q: read manifest meta: %w", ref, err)
	}
	// Build the overlay: new user tags set their values; previously-present user
	// tags not in the new set are deleted (empty value = delete in
	// UpdateLatestMeta). Both sides live in the metaTagPrefix namespace, so
	// casstore-internal tags are never affected.
	overlay := encodeUserTags(userMeta)
	if overlay == nil {
		overlay = make(map[string]string)
	}
	for k := range sm.Tags {
		if !strings.HasPrefix(k, metaTagPrefix) {
			continue
		}
		if _, keep := overlay[k]; !keep {
			overlay[k] = "" // delete the stale user key
		}
	}
	newSM, err := g.store.UpdateLatestMeta(ctx, g.key(ref), overlay)
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return ErrNotFound
		}
		return fmt.Errorf("blobgw set-meta %q: update manifest meta: %w", ref, err)
	}
	// The in-place update writes a new manifest version that supersedes the
	// prior latest; keep the ref index pointing at it (and mirror the user meta
	// for callers reading ObjectInfo directly). Get/Head source UserMeta from
	// the manifest regardless, so this is purely to avoid a stale Version row.
	info.Version = newSM.Version
	info.UserMeta = cloneMeta(userMeta)
	if err := g.refs.Put(ctx, info); err != nil {
		return fmt.Errorf("blobgw set-meta %q: ref store: %w", ref, err)
	}
	return nil
}

// Get returns a streaming reader over the object's reassembled bytes plus its
// metadata. The caller must Close the reader. Returns ErrNotFound if the ref
// is unknown or still pending.
func (g *Gateway) Get(ctx context.Context, ref string) (io.ReadCloser, ObjectInfo, error) {
	info, ok, err := g.refs.Get(ctx, g.domain, ref)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if !ok || info.Pending {
		return nil, ObjectInfo{}, ErrNotFound
	}
	rc, sm, err := g.store.GetLatest(ctx, g.key(ref))
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return nil, ObjectInfo{}, ErrNotFound
		}
		return nil, ObjectInfo{}, fmt.Errorf("blobgw get %q: %w", ref, err)
	}
	// User metadata is sourced from the durable casstore manifest (the single
	// source of truth), not the ref index, so it survives a restart.
	info.UserMeta = decodeUserTags(sm.Tags)
	return rc, info, nil
}

// Head returns object metadata without the body. Returns ErrNotFound if the
// ref is unknown or still pending.
func (g *Gateway) Head(ctx context.Context, ref string) (ObjectInfo, error) {
	info, ok, err := g.refs.Get(ctx, g.domain, ref)
	if err != nil {
		return ObjectInfo{}, err
	}
	if !ok || info.Pending {
		return ObjectInfo{}, ErrNotFound
	}
	// User metadata lives in the durable casstore manifest, not the ref index.
	// Read it back without fetching the object body (GetLatestMetadata peeks the
	// manifest's metadata only). A missing manifest for a non-pending ref means
	// the object is gone underneath us → ErrNotFound.
	sm, err := g.store.GetLatestMetadata(ctx, g.key(ref))
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, fmt.Errorf("blobgw head %q: %w", ref, err)
	}
	info.UserMeta = decodeUserTags(sm.Tags)
	return info, nil
}

// Delete removes the object's metadata and every backing manifest version.
// Chunks/packs are not deleted here — GC reclaims any that become unreferenced
// (and purges their dedup-index entries). Deleting an absent ref is a no-op.
func (g *Gateway) Delete(ctx context.Context, ref string) error {
	key := g.key(ref)
	versions, err := g.store.List(ctx, key)
	if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
		return fmt.Errorf("blobgw delete %q: list versions: %w", ref, err)
	}
	for _, v := range versions {
		if err := g.store.Delete(ctx, key, v.Version); err != nil {
			return fmt.Errorf("blobgw delete %q: delete version %s: %w", ref, v.Version, err)
		}
	}
	if err := g.refs.Delete(ctx, g.domain, ref); err != nil {
		return fmt.Errorf("blobgw delete %q: ref store: %w", ref, err)
	}
	return nil
}

// List returns object metadata for refs with the given prefix, newest first.
func (g *Gateway) List(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error) {
	return g.refs.List(ctx, g.domain, prefix, limit)
}

// ListPage is the keyset-paginated form of List. It returns at most limit
// objects for refs with the given prefix (newest first) starting strictly after
// the opaque cursor `after` ("" = from the start), plus the next cursor when
// more rows remain (next=="" means exhausted). The cursor is opaque to callers;
// pass a returned next value back as `after` to fetch the following page.
func (g *Gateway) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	return g.refs.ListPage(ctx, g.domain, prefix, after, limit)
}

// DeletePrefix removes every object whose ref starts with prefix and returns the
// number deleted. Each ref is removed with the SAME logic as Delete (every
// backing manifest version AND the ref row), so no manifests are orphaned. It is
// best-effort: a per-ref failure is recorded but does not abort the sweep; the
// accumulated count is returned alongside a wrapped error if any ref failed.
//
// An empty prefix is rejected with ErrInvalidRef to avoid a "delete the whole
// domain" footgun; callers that genuinely want that must iterate explicitly.
func (g *Gateway) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	if prefix == "" {
		return 0, ErrInvalidRef
	}
	deleted := 0
	var failures []error
	after := ""
	for {
		page, next, err := g.refs.ListPage(ctx, g.domain, prefix, after, 0)
		if err != nil {
			return deleted, fmt.Errorf("blobgw delete-prefix %q: list page: %w", prefix, err)
		}
		for _, info := range page {
			if err := ctx.Err(); err != nil {
				return deleted, err
			}
			if err := g.Delete(ctx, info.Ref); err != nil {
				failures = append(failures, fmt.Errorf("ref %q: %w", info.Ref, err))
				continue
			}
			deleted++
		}
		if next == "" {
			break
		}
		after = next
	}
	if len(failures) > 0 {
		return deleted, fmt.Errorf("blobgw delete-prefix %q: %d of %d failed: %w",
			prefix, len(failures), deleted+len(failures), errors.Join(failures...))
	}
	return deleted, nil
}

// StagedUpload is returned by MintRef: where and how the client should upload.
type StagedUpload struct {
	Ref        string    `json:"ref"`
	UploadURL  string    `json:"upload_url"`
	StagingKey string    `json:"staging_key"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// MintRef begins a stage-then-finalize upload. It mints a ref (if the caller
// passes ""), records a pending object, and returns a short-lived upload URL.
// The client uploads raw bytes to UploadURL, then calls Finalize(ref) to
// chunk+dedup the staged bytes into casstore and bind the ref to its content.
//
// MintRef attaches no user metadata; it is the back-compatible shorthand for
// MintRefWithMeta(..., nil, ...).
func (g *Gateway) MintRef(ctx context.Context, ref, contentType string, maxSize int64, ttl time.Duration) (StagedUpload, error) {
	return g.MintRefWithMeta(ctx, ref, contentType, nil, maxSize, ttl)
}

// MintOptions carries the optional inputs to MintRefWithOptions. All fields are
// optional; the zero value reproduces MintRef's behavior. ExpectedHash/
// ExpectedSize, when set, arm the finalize integrity gate (see ObjectInfo and
// Finalize): a staged upload whose computed sha256/size disagrees is rejected
// with ErrIntegrity instead of finalizing corrupt content.
type MintOptions struct {
	ContentType  string            // object Content-Type carried into the manifest
	UserMeta     map[string]string // durable user metadata applied at Finalize
	MaxSize      int64             // presign upper bound (0 = unbounded)
	TTL          time.Duration     // upload-URL validity
	ExpectedHash string            // expected sha256 hex of the staged bytes (""=unset)
	ExpectedSize int64             // expected total size in bytes (0=unset)
}

// MintRefWithMeta is MintRef plus user metadata captured at mint time and
// applied to the durable casstore manifest when the staged upload is finalized
// (so Get/Head recover it after a restart, identical to PutWithMeta). The
// metadata is held on the pending ref row until Finalize.
func (g *Gateway) MintRefWithMeta(ctx context.Context, ref, contentType string, userMeta map[string]string, maxSize int64, ttl time.Duration) (StagedUpload, error) {
	return g.MintRefWithOptions(ctx, ref, MintOptions{
		ContentType: contentType,
		UserMeta:    userMeta,
		MaxSize:     maxSize,
		TTL:         ttl,
	})
}

// MintRefWithOptions is the full mint entry point: it is MintRefWithMeta plus
// the optional finalize integrity gate (opts.ExpectedHash/ExpectedSize). All of
// the pending-row state — user metadata and the integrity expectations — is
// persisted on the pending ref row so it survives a restart between mint and
// Finalize (the durable pgindex columns; see ObjectInfo).
func (g *Gateway) MintRefWithOptions(ctx context.Context, ref string, opts MintOptions) (StagedUpload, error) {
	if g.stage == nil {
		return StagedUpload{}, errors.New("blobgw: staging not configured")
	}
	if ref == "" {
		ref = newID("ref")
	}
	stagingKey := g.domain + "/staging/" + ref + "/" + newID("up")
	url, exp, err := g.stage.PresignPut(ctx, stagingKey, opts.MaxSize, opts.TTL)
	if err != nil {
		return StagedUpload{}, fmt.Errorf("blobgw mint %q: presign: %w", ref, err)
	}
	pending := ObjectInfo{
		Ref:          ref,
		Domain:       g.domain,
		ContentType:  opts.ContentType,
		CreatedAt:    g.now().UTC(),
		Pending:      true,
		StagingKey:   stagingKey,
		UserMeta:     cloneMeta(opts.UserMeta),
		ExpectedHash: opts.ExpectedHash,
		ExpectedSize: opts.ExpectedSize,
	}
	if err := g.refs.Put(ctx, pending); err != nil {
		return StagedUpload{}, fmt.Errorf("blobgw mint %q: ref store: %w", ref, err)
	}
	return StagedUpload{Ref: ref, UploadURL: url, StagingKey: stagingKey, ExpiresAt: exp}, nil
}

// Finalize reads the staged bytes for ref, chunks+dedups them into casstore,
// and binds the ref to its content. It is idempotent: finalizing an already
// finalized ref returns its current metadata. Returns ErrNotFound for an
// unknown ref and ErrNotStaged if the ref was never minted for staging.
func (g *Gateway) Finalize(ctx context.Context, ref string) (ObjectInfo, error) {
	if g.stage == nil {
		return ObjectInfo{}, errors.New("blobgw: staging not configured")
	}
	info, ok, err := g.refs.Get(ctx, g.domain, ref)
	if err != nil {
		return ObjectInfo{}, err
	}
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	if !info.Pending {
		return info, nil // already finalized — idempotent
	}
	if info.StagingKey == "" {
		return ObjectInfo{}, ErrNotStaged
	}

	rc, err := g.stage.Open(ctx, info.StagingKey)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw finalize %q: open staging: %w", ref, err)
	}
	defer rc.Close()

	hc := newHashingCounter(rc)
	// Carry the staged object's Content-Type into SnapshotMetadata.Format so the
	// compression policy can decide per object (same rationale as Put), and the
	// user metadata captured at mint time into the durable manifest Tags.
	sm, err := g.store.Put(ctx, g.key(ref), snapshot.SnapshotMetadata{
		Format: info.ContentType,
		Tags:   encodeUserTags(info.UserMeta),
	}, hc)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw finalize %q: store: %w", ref, err)
	}
	gotHash, gotSize := hc.hexSum(), hc.n
	// Integrity gate: a presigned PUT is uploaded out-of-band, so a truncated,
	// empty, or wrong upload could otherwise finalize to corrupt content. If the
	// caller armed an expectation at mint time, the staged bytes must match it.
	// On mismatch we DO NOT bind the ref (the pending row stays, so a corrected
	// re-upload + re-finalize can still succeed) and best-effort delete the
	// staged bytes so the bad upload can't be re-finalized as-is. An armed
	// ExpectedSize also rejects an empty (0-byte) staged object outright.
	if mismatch := checkIntegrity(info.ExpectedHash, info.ExpectedSize, gotSize, gotHash); mismatch != "" {
		// Roll back the manifest version we just wrote for the mismatched bytes so
		// it can't shadow a later good finalize; best-effort, like the staging
		// cleanup below.
		_ = g.store.Delete(ctx, g.key(ref), sm.Version)
		_ = g.stage.Delete(ctx, info.StagingKey)
		return ObjectInfo{}, fmt.Errorf("blobgw finalize %q: %s: %w", ref, mismatch, ErrIntegrity)
	}
	final := ObjectInfo{
		Ref:         ref,
		Domain:      g.domain,
		ContentHash: gotHash,
		Size:        gotSize,
		ContentType: info.ContentType,
		CreatedAt:   g.now().UTC(),
		Version:     sm.Version,
		UserMeta:    cloneMeta(info.UserMeta),
		// Preserve the (now-satisfied) expectations on the finalized row for
		// auditability; they no longer gate once Pending is false.
		ExpectedHash: info.ExpectedHash,
		ExpectedSize: info.ExpectedSize,
	}
	if err := g.refs.Put(ctx, final); err != nil {
		return ObjectInfo{}, fmt.Errorf("blobgw finalize %q: ref store: %w", ref, err)
	}
	// Best-effort: reclaim the staging slot. A failure here only leaves a
	// stray staged blob for the staging store's own GC to sweep.
	_ = g.stage.Delete(ctx, info.StagingKey)
	return final, nil
}

// StagingGC reclaims the slots left behind by minted-but-never-finalized
// uploads. MintRef writes a PENDING blob_ref row plus a staging object; if the
// client never calls Finalize, both leak forever. StagingGC sweeps pending
// rows older than TTL — deleting their staging bytes (best-effort) and the
// pending row — while preserving any pending ref younger than TTL (whose
// upload may still be in flight). It mirrors casstore's ChunkedGC: a Loop for
// a daemon and RunOnce for an admin/maintenance invocation, both leader-only.
type StagingGC struct {
	refs   RefStore
	stage  StagingStore
	domain string
	ttl    time.Duration
	logger *slog.Logger
	now    func() time.Time
}

// NewStagingGC constructs a staging sweeper for domain. ttl is the minimum age
// a pending ref must reach before it is reclaimed (the MintRef→Finalize grace
// window); logger may be nil (falls back to slog.Default).
func NewStagingGC(refs RefStore, stage StagingStore, domain string, ttl time.Duration, logger *slog.Logger) *StagingGC {
	if logger == nil {
		logger = slog.Default()
	}
	return &StagingGC{refs: refs, stage: stage, domain: domain, ttl: ttl, logger: logger, now: time.Now}
}

// SetClock overrides the clock used to compute the TTL cutoff (tests).
func (g *StagingGC) SetClock(now func() time.Time) { g.now = now }

// StagingGCResult summarises a single staging sweep.
type StagingGCResult struct {
	// PendingScanned is the count of stale pending refs examined this pass.
	PendingScanned int
	// SlotsReclaimed is the count of pending refs whose row was removed.
	SlotsReclaimed int
	// StagingDeleteErrors is the count of staging-object deletes that failed
	// (the pending row is still removed; the orphaned staged bytes are left for
	// the staging store's own lifecycle policy).
	StagingDeleteErrors int
}

// RunOnce sweeps once: it finds pending refs older than TTL, deletes their
// staging bytes (best-effort) and pending rows, and returns a summary. Safe to
// invoke from an admin/maintenance entrypoint. If staging is not configured it
// is a no-op.
func (g *StagingGC) RunOnce(ctx context.Context) (StagingGCResult, error) {
	var res StagingGCResult
	if g.stage == nil {
		return res, nil
	}
	cutoff := g.now().UTC().Add(-g.ttl)
	stale, err := g.refs.ListStalePending(ctx, g.domain, cutoff, 0)
	if err != nil {
		return res, fmt.Errorf("blobgw: staging gc: list stale pending: %w", err)
	}
	res.PendingScanned = len(stale)
	for _, info := range stale {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		// Delete the staging bytes first: if the row is removed but the bytes
		// linger, nothing references them anymore; the reverse would re-expose
		// a half-reclaimed slot.
		if info.StagingKey != "" {
			if err := g.stage.Delete(ctx, info.StagingKey); err != nil {
				res.StagingDeleteErrors++
				g.logger.WarnContext(ctx, "blobgw: staging gc: delete staging object",
					"domain", g.domain, "ref", info.Ref, "staging_key", info.StagingKey, "err", err)
			}
		}
		if err := g.refs.Delete(ctx, g.domain, info.Ref); err != nil {
			return res, fmt.Errorf("blobgw: staging gc: delete pending ref %q: %w", info.Ref, err)
		}
		res.SlotsReclaimed++
	}
	return res, nil
}

// Loop runs RunOnce immediately and then every interval until ctx is done.
// interval <= 0 disables the loop. Mirrors ChunkedGC.Loop so a daemon wires
// both GCs the same way.
func (g *StagingGC) Loop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		g.logger.InfoContext(ctx, "blobgw: staging gc: disabled (interval <= 0)")
		return
	}
	g.logger.InfoContext(ctx, "blobgw: staging gc: loop started", "interval", interval, "ttl", g.ttl)
	g.runOnceLogged(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.runOnceLogged(ctx)
		}
	}
}

func (g *StagingGC) runOnceLogged(ctx context.Context) {
	res, err := g.RunOnce(ctx)
	if err != nil {
		g.logger.ErrorContext(ctx, "blobgw: staging gc: pass failed", "domain", g.domain, "err", err)
		return
	}
	if res.SlotsReclaimed > 0 || res.StagingDeleteErrors > 0 {
		g.logger.InfoContext(ctx, "blobgw: staging gc: pass complete", "domain", g.domain,
			"pending_scanned", res.PendingScanned, "slots_reclaimed", res.SlotsReclaimed,
			"staging_delete_errors", res.StagingDeleteErrors)
	}
}

// metaTagPrefix namespaces caller-supplied user-metadata keys within the casstore
// manifest's SnapshotMetadata.Tags. casstore reserves bare keys for its own
// internal use (e.g. "chunked_format"); prefixing every user key keeps the two
// disjoint so neither can clobber the other, and lets the read path recover
// exactly the user set without leaking casstore's internals into ObjectInfo.
const metaTagPrefix = "u."

// encodeUserTags maps caller user metadata into the namespaced tag set written
// to the casstore manifest. nil/empty in → nil out (no tags written).
func encodeUserTags(userMeta map[string]string) map[string]string {
	if len(userMeta) == 0 {
		return nil
	}
	tags := make(map[string]string, len(userMeta))
	for k, v := range userMeta {
		tags[metaTagPrefix+k] = v
	}
	return tags
}

// decodeUserTags is the inverse of encodeUserTags: it extracts and un-prefixes
// the user keys from a manifest's tag set, dropping casstore's own internal
// tags. Returns nil when no user tags are present.
func decodeUserTags(tags map[string]string) map[string]string {
	var out map[string]string
	for k, v := range tags {
		if suffix, ok := strings.CutPrefix(k, metaTagPrefix); ok {
			if out == nil {
				out = make(map[string]string)
			}
			out[suffix] = v
		}
	}
	return out
}

// checkIntegrity compares the staged bytes' computed hash/size against the
// expectations armed at mint time. It returns "" when everything the caller
// asked to verify matches (including the no-expectation case), or a short
// human-readable reason on the first mismatch (wrapped into ErrIntegrity by the
// caller). An armed ExpectedSize also rejects a 0-byte staged object, catching
// a presigned PUT that uploaded nothing.
func checkIntegrity(expectedHash string, expectedSize, gotSize int64, gotHash string) string {
	if expectedSize > 0 {
		if gotSize == 0 {
			return "staged object is empty"
		}
		if gotSize != expectedSize {
			return fmt.Sprintf("size mismatch: expected %d, got %d", expectedSize, gotSize)
		}
	}
	if expectedHash != "" && !strings.EqualFold(expectedHash, gotHash) {
		return fmt.Sprintf("hash mismatch: expected %s, got %s", expectedHash, gotHash)
	}
	return ""
}

// cloneMeta returns an independent copy of m so a stored ObjectInfo never aliases
// the caller's map. nil/empty in → nil out.
func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// defaultPageSize bounds an unbounded (limit<=0) cursored page so a single
// ListPage call can't load the whole prefix into memory at once; callers that
// want everything follow the returned cursor across pages.
const defaultPageSize = 1000

// EncodeListCursor produces the opaque keyset cursor for a List page boundary:
// base64url(no padding) of "created_at(RFC3339Nano)|ref". RefStore
// implementations emit it via ListPage and never interpret it; it is paired with
// DecodeListCursor below. Exported so every RefStore implementation (pgindex,
// in-memory) shares one token format.
func EncodeListCursor(createdAt time.Time, ref string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + ref
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeListCursor parses a cursor produced by EncodeListCursor back into the
// (created_at, ref) boundary. An empty token means "from the start" (zero
// time, ""). A malformed token is an error so a corrupt cursor fails loudly
// rather than silently restarting the scan.
func DecodeListCursor(token string) (createdAt time.Time, ref string, err error) {
	if token == "" {
		return time.Time{}, "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("blobgw: invalid list cursor: %w", err)
	}
	ts, r, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", fmt.Errorf("blobgw: invalid list cursor: missing separator")
	}
	createdAt, err = time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("blobgw: invalid list cursor timestamp: %w", err)
	}
	return createdAt.UTC(), r, nil
}

// hashingCounter wraps a reader, computing the sha256 of and counting all bytes
// read through it. Used to capture an object's whole-content hash and size as
// casstore consumes the stream.
type hashingCounter struct {
	r io.Reader
	h hash.Hash
	n int64
}

func newHashingCounter(r io.Reader) *hashingCounter {
	return &hashingCounter{r: r, h: sha256.New()}
}

func (hc *hashingCounter) Read(p []byte) (int, error) {
	n, err := hc.r.Read(p)
	if n > 0 {
		_, _ = hc.h.Write(p[:n])
		hc.n += int64(n)
	}
	return n, err
}

func (hc *hashingCounter) hexSum() string { return hex.EncodeToString(hc.h.Sum(nil)) }

// newID returns a short unique id of the form "<prefix>-<RFC3339Nano>-<hex>",
// reusing casstore's monotonic-version convention so ids sort by creation time.
func newID(prefix string) string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	return prefix + "-" + time.Now().UTC().Format(time.RFC3339Nano) + "-" + hex.EncodeToString(buf)
}
