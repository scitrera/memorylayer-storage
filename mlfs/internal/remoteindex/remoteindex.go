// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package remoteindex implements snapshot.DedupStore over the blobgw control
// plane using NATS request-reply and the ctlproto wire contract (ADR-001 §2.2,
// §2.5). An mlfs node creates one RemoteDedupStore per NATS connection and
// passes it as the DedupStore when constructing a casstore GlobalIndex.
//
// Transport is abstracted behind Requester so tests can substitute an in-memory
// fake without a real NATS server. The production path uses NatsRequester,
// which wraps *nats.Conn.RequestWithContext and maps nats.ErrNoResponders to
// ErrNoResponders so callers can distinguish "nobody home" from other errors.
//
// Batching: LookupBatch and Record automatically split input slices into
// sub-batches of at most ctlproto.MaxChunkHashesPerBatch to respect the NATS
// 1 MB max-payload limit. PurgePacks does the same for pack hashes.
package remoteindex

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// ErrNoResponders is returned by NatsRequester when NATS reports that no
// subscriber is listening on the requested subject. This is the sentinel that
// lets callers distinguish "blobgw is down / not yet subscribed" from a
// generic network or decode error.
var ErrNoResponders = errors.New("remoteindex: no responders on subject")

// defaultTimeout is applied when the caller does not supply a WithTimeout
// option.
const defaultTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// Requester — transport abstraction
// ---------------------------------------------------------------------------

// Requester is the single-method transport interface RemoteDedupStore relies
// on. It matches the semantics of nats.Conn.RequestWithContext: send data on
// subject, wait for exactly one reply, return its payload. Implementations
// must respect ctx cancellation and deadline.
type Requester interface {
	Request(ctx context.Context, subject string, data []byte) ([]byte, error)
}

// NatsRequester adapts a *nats.Conn to Requester. It wraps RequestWithContext
// and converts nats.ErrNoResponders to the package-local ErrNoResponders so
// callers can type-switch on it without importing the NATS package.
type NatsRequester struct {
	conn    *nats.Conn
	timeout time.Duration // per-request deadline, applied when ctx has none
}

// NewNatsRequester constructs a NatsRequester. timeout is the per-request
// deadline applied when the supplied context carries no deadline of its own.
// Pass 0 to use the package default (5 s).
func NewNatsRequester(conn *nats.Conn, timeout time.Duration) *NatsRequester {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &NatsRequester{conn: conn, timeout: timeout}
}

// Request sends data to subject and returns the first reply payload.
func (r *NatsRequester) Request(ctx context.Context, subject string, data []byte) ([]byte, error) {
	// If the caller's context already has a deadline, honour it exactly;
	// otherwise impose our own timeout so we never block indefinitely.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	msg, err := r.conn.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: subject,
		Data:    data,
	})
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrNoResponders, subject)
		}
		return nil, fmt.Errorf("remoteindex: nats request to %q: %w", subject, err)
	}
	return msg.Data, nil
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// options holds optional configuration for RemoteDedupStore.
type options struct {
	codec   ctlproto.Codec
	timeout time.Duration
}

// Option is a functional option for NewRemoteDedupStore.
type Option func(*options)

// WithCodec overrides the codec used to marshal/unmarshal control-plane
// messages. Defaults to ctlproto.DefaultCodec (currently JSONCodec).
func WithCodec(c ctlproto.Codec) Option {
	return func(o *options) { o.codec = c }
}

// WithTimeout sets the per-request timeout applied when the caller's context
// has no deadline. Defaults to 5 s. Has no effect when using a NatsRequester
// constructed with an explicit timeout — this option controls a timeout that
// RemoteDedupStore itself can apply via context wrapping when the per-request
// context is background/TODO.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// ---------------------------------------------------------------------------
// RemoteDedupStore
// ---------------------------------------------------------------------------

// RemoteDedupStore implements snapshot.DedupStore by forwarding every
// operation to blobgw's control plane over NATS request-reply using the
// ctlproto wire contract. It is safe for concurrent use.
//
// Compile-time interface assertion:
var _ snapshot.DedupStore = (*RemoteDedupStore)(nil)

// RemoteDedupStore holds the transport and codec needed to speak the
// ctlproto control-plane protocol.
type RemoteDedupStore struct {
	r       Requester
	codec   ctlproto.Codec
	timeout time.Duration
}

// NewRemoteDedupStore constructs a RemoteDedupStore. r must not be nil.
// Optional Option values tune the codec and per-request timeout.
func NewRemoteDedupStore(r Requester, opts ...Option) *RemoteDedupStore {
	o := &options{
		codec:   ctlproto.DefaultCodec,
		timeout: defaultTimeout,
	}
	for _, fn := range opts {
		fn(o)
	}
	return &RemoteDedupStore{r: r, codec: o.codec, timeout: o.timeout}
}

// withTimeout wraps ctx with the store's timeout when ctx has no deadline.
func (s *RemoteDedupStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.timeout)
}

// Lookup returns the stored location of chunkHash in domain, or ok=false if
// absent. It delegates to LookupBatch with a single-element slice; LookupBatch
// owns the timeout wrapping so Lookup must not add its own.
func (s *RemoteDedupStore) Lookup(ctx context.Context, domain, chunkHash string) (snapshot.PackRef, bool, error) {
	found, err := s.LookupBatch(ctx, domain, []string{chunkHash})
	if err != nil {
		return snapshot.PackRef{}, false, err
	}
	ref, ok := found[chunkHash]
	return ref, ok, nil
}

// LookupBatch resolves many chunk hashes in one or more NATS round-trips. The
// returned map contains only the hashes that were found; missing hashes are
// absent. Input slices larger than ctlproto.MaxChunkHashesPerBatch are split
// into sub-batches automatically.
func (s *RemoteDedupStore) LookupBatch(ctx context.Context, domain string, chunkHashes []string) (map[string]snapshot.PackRef, error) {
	// withTimeout wraps the TOTAL budget for all sub-batches, not each one
	// individually — do not move this call inside the loop below.
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	subject := ctlproto.IndexLookupSubject(domain)
	result := make(map[string]snapshot.PackRef, len(chunkHashes))

	for len(chunkHashes) > 0 {
		batch := chunkHashes
		if len(batch) > ctlproto.MaxChunkHashesPerBatch {
			batch = chunkHashes[:ctlproto.MaxChunkHashesPerBatch]
		}
		chunkHashes = chunkHashes[len(batch):]

		req := ctlproto.LookupBatchRequest{
			Domain:      domain,
			ChunkHashes: batch,
		}
		data, err := s.codec.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("remoteindex: marshal LookupBatchRequest: %w", err)
		}

		raw, err := s.r.Request(ctx, subject, data)
		if err != nil {
			return nil, fmt.Errorf("remoteindex: LookupBatch transport: %w", err)
		}

		var resp ctlproto.LookupBatchResponse
		if err := s.codec.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("remoteindex: unmarshal LookupBatchResponse: %w", err)
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("remoteindex: LookupBatch server error: %s", resp.Error)
		}

		for hash, ref := range resp.Locations {
			result[hash] = snapshot.PackRef{
				PackHash: ref.PackHash,
				Offset:   int(ref.Offset),
				Size:     int(ref.Size),
			}
		}
	}
	return result, nil
}

// Record idempotently persists chunk locations in domain. Input slices larger
// than ctlproto.MaxChunkHashesPerBatch are split into sub-batches. Errors
// from the server or transport are returned; the casstore GlobalIndex treats
// DedupStore errors as best-effort (pack normally), but we do not swallow them.
func (s *RemoteDedupStore) Record(ctx context.Context, domain string, locs []snapshot.ChunkLocation) error {
	// withTimeout wraps the TOTAL budget for all sub-batches, not each one
	// individually — do not move this call inside the loop below.
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	subject := ctlproto.IndexRecordSubject(domain)

	for len(locs) > 0 {
		batch := locs
		if len(batch) > ctlproto.MaxChunkHashesPerBatch {
			batch = locs[:ctlproto.MaxChunkHashesPerBatch]
		}
		locs = locs[len(batch):]

		wire := make([]ctlproto.ChunkLocation, len(batch))
		for i, loc := range batch {
			wire[i] = ctlproto.ChunkLocation{
				ChunkHash: loc.ChunkHash,
				PackRef: ctlproto.PackRef{
					PackHash: loc.PackRef.PackHash,
					Offset:   int64(loc.PackRef.Offset),
					Size:     int64(loc.PackRef.Size),
				},
			}
		}

		req := ctlproto.RecordRequest{
			Domain:    domain,
			Locations: wire,
		}
		data, err := s.codec.Marshal(req)
		if err != nil {
			return fmt.Errorf("remoteindex: marshal RecordRequest: %w", err)
		}

		raw, err := s.r.Request(ctx, subject, data)
		if err != nil {
			return fmt.Errorf("remoteindex: Record transport: %w", err)
		}

		var resp ctlproto.RecordResponse
		if err := s.codec.Unmarshal(raw, &resp); err != nil {
			return fmt.Errorf("remoteindex: unmarshal RecordResponse: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("remoteindex: Record server error: %s", resp.Error)
		}
	}
	return nil
}

// PurgePacks removes every index entry in domain that points to any of the
// given pack hashes. Input is split into sub-batches of
// ctlproto.MaxPackHashesPerBatch. Idempotent; purging an absent pack is a no-op.
func (s *RemoteDedupStore) PurgePacks(ctx context.Context, domain string, packHashes []string) error {
	// withTimeout wraps the TOTAL budget for all sub-batches, not each one
	// individually — do not move this call inside the loop below.
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	subject := ctlproto.GCPurgeSubject(domain)

	for len(packHashes) > 0 {
		batch := packHashes
		if len(batch) > ctlproto.MaxPackHashesPerBatch {
			batch = packHashes[:ctlproto.MaxPackHashesPerBatch]
		}
		packHashes = packHashes[len(batch):]

		req := ctlproto.PurgePacksRequest{
			Domain:     domain,
			PackHashes: batch,
		}
		data, err := s.codec.Marshal(req)
		if err != nil {
			return fmt.Errorf("remoteindex: marshal PurgePacksRequest: %w", err)
		}

		raw, err := s.r.Request(ctx, subject, data)
		if err != nil {
			return fmt.Errorf("remoteindex: PurgePacks transport: %w", err)
		}

		var resp ctlproto.PurgePacksResponse
		if err := s.codec.Unmarshal(raw, &resp); err != nil {
			return fmt.Errorf("remoteindex: unmarshal PurgePacksResponse: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("remoteindex: PurgePacks server error: %s", resp.Error)
		}
	}
	return nil
}

// Associate implements snapshot.PackAssociator over the control plane: it tells
// blobgw which owner context references which packs, so reclamation can later be
// scoped to that owner without walking the store.
//
// Only the association WRITE lives on this client. The rest of the reclamation
// lifecycle (dropping a context, publishing marks, deciding a pack may die) needs
// the durable index and blob-deletion authority together and therefore runs
// blobgw-side — a filesystem node is authoritative for neither.
//
// A server refusal because a pack is being reclaimed is surfaced as
// snapshot.ErrPackObliterating, so the ChunkedStore write path fails the Put
// cleanly (before its manifest exists) rather than referencing bytes on their way
// out. Retrying re-packs the chunks, because the reclaiming pack is by then
// hidden from dedup lookups.
func (s *RemoteDedupStore) Associate(ctx context.Context, domain string, assocs []snapshot.PackAssociation) error {
	if len(assocs) == 0 {
		return nil
	}
	// The total budget covers every sub-batch, matching PurgePacks.
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	subject := ctlproto.AssocRecordSubject(domain)

	for len(assocs) > 0 {
		batch := assocs
		if len(batch) > ctlproto.MaxPackHashesPerBatch {
			batch = assocs[:ctlproto.MaxPackHashesPerBatch]
		}
		assocs = assocs[len(batch):]

		wire := make([]ctlproto.PackAssociation, 0, len(batch))
		for _, a := range batch {
			wire = append(wire, ctlproto.PackAssociation{PackHash: a.PackHash, Context: a.Context})
		}
		data, err := s.codec.Marshal(ctlproto.AssociateRequest{Domain: domain, Associations: wire})
		if err != nil {
			return fmt.Errorf("remoteindex: marshal AssociateRequest: %w", err)
		}
		raw, err := s.r.Request(ctx, subject, data)
		if err != nil {
			return fmt.Errorf("remoteindex: Associate transport: %w", err)
		}
		var resp ctlproto.AssociateResponse
		if err := s.codec.Unmarshal(raw, &resp); err != nil {
			return fmt.Errorf("remoteindex: unmarshal AssociateResponse: %w", err)
		}
		if resp.Obliterating {
			return fmt.Errorf("remoteindex: Associate refused (%s): %w", resp.Error, snapshot.ErrPackObliterating)
		}
		if resp.Error != "" {
			return fmt.Errorf("remoteindex: Associate server error: %s", resp.Error)
		}
	}
	return nil
}
