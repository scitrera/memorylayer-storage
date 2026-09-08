// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

// Pack associations and mark-based reclamation.
//
// # Why
//
// The original reclamation model derives liveness by walking EVERY manifest in
// the store on every GC pass (see ChunkedGC.run) and protects in-flight writes
// with a time-based SafetyWindow. That costs O(total store) per pass and leaves a
// documented residual race: the dedup index can make a pack reachable with no
// manifest referencing it, so a pack orphaned long ago can be deduped INTO at the
// same moment GC decides to reclaim it (TECH_DEBT #1 residual, #51). A time window
// cannot close that; only a mark can.
//
// # The model
//
// Two ideas, taken from Lore's storage subsystem (see docs/DESIGN_lore_evaluation.md
// §4.2, §4.3) and adapted to the fact that our PAYLOAD unit (a pack) is coarser
// than our ADDRESSED unit (a chunk):
//
//   - A *context* is an opaque, caller-chosen owner tag recorded alongside every
//     pack an object references. It is NOT an access boundary (the dedup domain is)
//     and it does not change what is stored — identical bytes still dedup to one
//     pack. It exists so reclamation can be scoped to an owner without walking the
//     store, and so a pack's liveness is a QUERY rather than a traversal.
//
//   - A pack carries a reclamation *lifecycle state*. Reclaiming is a sequence
//     (see PackReclaimer.ObliterateContext) whose middle step publishes an
//     `obliterating` mark, so any writer that would otherwise reuse the pack is
//     turned away rather than racing the delete.
//
// # The invariant that makes this safe
//
// A pack is deleted only when NO association remains for it, re-checked after the
// mark is published and a drain interval has elapsed. Two guards keep a concurrent
// writer from referencing a pack that is on its way out:
//
//  1. DedupStore.Lookup MUST NOT return a location in a pack that is obliterating
//     or obliterated, so a Put never dedups into a doomed pack in the first place.
//  2. Associate REFUSES a pack that is not in the stored state, returning
//     ErrPackObliterating. ChunkedStore associates BEFORE it writes the manifest,
//     so a refusal fails the Put before anything references the doomed pack. The
//     caller retries; the retry's Lookup (guard 1) no longer sees the pack and the
//     chunks are packed afresh. This is the narrow TOCTOU between guard 1 and
//     commit — rare, and it fails safe rather than corrupting.
//
// # Backfill
//
// Associations only exist for objects written after this landed. An
// association-derived live set would therefore see all pre-existing data as
// garbage. BackfillAssociations populates associations from existing manifests
// (one walk, once per domain) and records a marker; the association-derived live
// set refuses to run for a domain that has not been backfilled. See
// ChunkedGC.UseAssociations.

// PackState is a pack's reclamation lifecycle state. A pack with no recorded
// state is treated as PackStored: state rows are created lazily, so a store that
// predates this feature behaves exactly as before.
type PackState string

const (
	// PackStored is the normal state: the pack's bytes are present and it may be
	// read, deduped into, and associated with new contexts.
	PackStored PackState = "stored"
	// PackObliterating means reclamation has published its mark and is deciding
	// whether the pack may be deleted. Lookup must not hand it out and Associate
	// must refuse it. The state is reversible: if the drain re-check finds a
	// surviving association, the mark is released back to PackStored.
	PackObliterating PackState = "obliterating"
	// PackObliterated means the pack's bytes have been deleted. Terminal.
	PackObliterated PackState = "obliterated"
)

// ErrPackObliterating reports that an operation targeted a pack that is being, or
// has been, reclaimed. Associate returns it so a Put fails before writing a
// manifest that would reference bytes on their way out; the operation is safe to
// retry, and the retry will pack the chunks afresh.
var ErrPackObliterating = errors.New("snapshot: pack is being reclaimed")

// PackAssociation binds one pack to one owning context within a dedup domain.
// It is the row a caller records to say "this owner references these bytes".
type PackAssociation struct {
	PackHash string
	Context  string
}

// PackAssocStore is an OPTIONAL capability a DedupStore MAY implement to support
// context-scoped, mark-based reclamation. It follows the same opt-in shape as
// PackSizeRecorder: stores that do not implement it keep the manifest-walk GC and
// lose nothing else.
//
// Every operation is scoped by domain, the same trust boundary DedupStore uses.
//
// Implementations MUST be safe for concurrent use, and MUST honour two contracts
// that the rest of the model depends on:
//
//   - Lookup/LookupBatch (on the embedding DedupStore) must not return locations
//     in packs that are obliterating or obliterated.
//   - Associate must refuse (ErrPackObliterating) any pack not in PackStored.
type PackAssocStore interface {
	PackAssociator

	// DropContext removes every association held by packContext in domain and
	// returns the distinct pack hashes that were referenced, which are the
	// candidates for reclamation. Idempotent: dropping an unknown context removes
	// nothing and returns no candidates.
	DropContext(ctx context.Context, domain, packContext string) ([]string, error)

	// HasAssociations reports, per pack hash, whether ANY association remains.
	// This is the liveness question reclamation asks after its drain interval.
	HasAssociations(ctx context.Context, domain string, packHashes []string) (map[string]bool, error)

	// WalkLivePacks yields every pack hash in domain that holds at least one
	// association — the association-derived live set, replacing the manifest walk.
	WalkLivePacks(ctx context.Context, domain string, yield func(packHash string) error) error

	// MarkObliterating atomically moves a pack from stored to obliterating and
	// reports whether it won the transition. false (with a nil error) means the
	// pack was already obliterating or obliterated, so another reclaimer owns it.
	MarkObliterating(ctx context.Context, domain, packHash string) (bool, error)

	// ReleaseMark moves a pack from obliterating back to stored. Called when the
	// drain re-check finds the pack is still referenced.
	ReleaseMark(ctx context.Context, domain, packHash string) error

	// CommitObliterated atomically moves a pack from obliterating to obliterated,
	// but ONLY if it still holds no associations. It reports whether it committed;
	// false (with a nil error) means an association appeared during the drain and
	// the caller must release the mark instead of deleting. Doing the re-check and
	// the state transition in one atomic step is what closes the gap between
	// "checked" and "deleted".
	CommitObliterated(ctx context.Context, domain, packHash string) (bool, error)

	// MarkBackfilled records that domain's associations have been populated from
	// existing manifests, so the association-derived live set may be trusted.
	MarkBackfilled(ctx context.Context, domain string) error

	// IsBackfilled reports whether MarkBackfilled has been called for domain.
	IsBackfilled(ctx context.Context, domain string) (bool, error)
}

// PackAssociator is the WRITE half of association support: recording that an
// owner references a pack. It is deliberately separate from the full
// PackAssocStore because the two halves live in different places.
//
// A writer only ever needs to associate. Reclamation — dropping a context,
// publishing marks, deciding a pack may die — needs the durable index AND the
// blob store, so it runs where both are authoritative (blobgw over Postgres),
// never on a filesystem node. mlfs's converged path therefore implements just
// this one method over its control-plane RPC, and asks blobgw to reclaim.
type PackAssociator interface {
	// Associate idempotently records that each context references each pack.
	// Recording an association that already exists is a no-op. It returns
	// ErrPackObliterating if ANY named pack is not in the stored state, and in
	// that case records nothing — the caller must treat the whole batch as failed.
	Associate(ctx context.Context, domain string, assocs []PackAssociation) error
}

// packAssociatorProvider is implemented by a DedupIndex (GlobalIndex) whose
// durable store may support associations, mirroring packSizeRecorderProvider.
type packAssociatorProvider interface {
	packAssociator() PackAssociator
}

// packAssociator exposes the durable store's association-write capability, or nil.
func (g *GlobalIndex) packAssociator() PackAssociator {
	if s, ok := g.store.(PackAssociator); ok {
		return s
	}
	return nil
}

var _ packAssociatorProvider = (*GlobalIndex)(nil)

// packAssociator returns the configured index's PackAssociator, or nil when the
// index or its durable store does not support associations (the PriorManifestIndex
// default, or a DedupStore predating the capability).
func (c *ChunkedStore) packAssociator() PackAssociator {
	if p, ok := c.index.(packAssociatorProvider); ok {
		return p.packAssociator()
	}
	return nil
}

// packAssocStore returns the configured index's FULL association store — the
// reclamation-side capability — or nil. Only callers that manage lifecycle
// (backfill, reclaimer wiring) need it.
func (c *ChunkedStore) packAssocStore() PackAssocStore {
	if p, ok := c.index.(packAssociatorProvider); ok {
		if full, ok := p.packAssociator().(PackAssocStore); ok {
			return full
		}
	}
	return nil
}

// TagOwnerContext is a SnapshotMetadata.Tags key a caller sets to choose the
// reclamation context for an object explicitly. When absent, ChunkedStore uses
// the SnapshotKey's OwnerKey — which is already the object's identity on every
// current call site (mlfs writes "s/<slice_id>", blobgw writes its ref), so the
// default needs no call-site changes.
const TagOwnerContext = "owner_context"

// contextFor resolves the reclamation context for one Put.
func contextFor(key SnapshotKey, meta SnapshotMetadata) string {
	if c := meta.Tags[TagOwnerContext]; c != "" {
		return c
	}
	return key.OwnerKey
}

// associatePacks records this object's ownership of every pack its manifest
// references — both packs written by this Put and packs it deduped INTO, since
// both are bytes the object now depends on. It runs after the dedup session
// commits and BEFORE the manifest is written, so a refusal (ErrPackObliterating)
// fails the Put before anything can reference a pack on its way out.
//
// A nil PackAssocStore (no association support wired) makes this a no-op, which
// is what keeps the feature opt-in and backward compatible.
func (c *ChunkedStore) associatePacks(ctx context.Context, domain, packContext string, chunks []chunkRef) error {
	store := c.packAssociator()
	if store == nil || packContext == "" {
		return nil
	}
	seen := make(map[string]struct{}, len(chunks))
	assocs := make([]PackAssociation, 0, 8)
	for _, ref := range chunks {
		if ref.PackHash == "" {
			continue // v1 standalone chunk blob: no pack to associate
		}
		if _, dup := seen[ref.PackHash]; dup {
			continue
		}
		seen[ref.PackHash] = struct{}{}
		assocs = append(assocs, PackAssociation{PackHash: ref.PackHash, Context: packContext})
	}
	if len(assocs) == 0 {
		return nil
	}
	return store.Associate(ctx, domain, assocs)
}

// ---------------------------------------------------------------------------
// PackReclaimer — context-scoped, mark-based reclamation
// ---------------------------------------------------------------------------

// ReclaimResult summarises one ObliterateContext call.
type ReclaimResult struct {
	// PacksConsidered is the number of distinct packs the context referenced.
	PacksConsidered int
	// PacksReclaimed is the number of packs whose bytes were deleted.
	PacksReclaimed int
	// PacksStillReferenced is the number of packs left alone because another
	// context still references them (the shared-bytes case).
	PacksStillReferenced int
	// PacksHeldByOther is the number of packs another reclaimer already owned
	// (they were not in the stored state when the mark was attempted).
	PacksHeldByOther int
	// PacksSkippedError is the number of packs left for a later pass because an
	// operation failed. Reclamation never aborts the whole call for one pack.
	PacksSkippedError int
	// BytesReclaimed is the total on-disk size of the deleted pack blobs.
	BytesReclaimed int64
	// Duration is the wall-clock time of the call.
	Duration time.Duration
}

// PackReclaimer performs context-scoped reclamation using the association index
// and the pack lifecycle mark. It is the precise counterpart to ChunkedGC's
// sweep: where GC asks "what is unreferenced anywhere?", this asks "this owner is
// gone — what can go with it?", and answers without walking the store.
type PackReclaimer struct {
	assoc  PackAssocStore
	chunks blobstore.Storage
	index  DedupGC
	logger *slog.Logger

	// DrainInterval is how long to wait between publishing the obliterating mark
	// and re-checking for surviving associations. It exists to let writes that
	// were already in flight when the mark went up land their associations, so the
	// re-check sees them. It is NOT the SafetyWindow: correctness comes from the
	// mark plus the atomic CommitObliterated re-check, not from the delay. A few
	// seconds is ample; the default is 5s.
	DrainInterval time.Duration

	sleep func(time.Duration)
}

// NewPackReclaimer constructs a reclaimer. assoc and chunks are required; index
// may be nil (no dedup entries to purge); logger may be nil.
func NewPackReclaimer(assoc PackAssocStore, chunks blobstore.Storage, index DedupGC, logger *slog.Logger) *PackReclaimer {
	if logger == nil {
		logger = slog.Default()
	}
	return &PackReclaimer{
		assoc:         assoc,
		chunks:        chunks,
		index:         index,
		logger:        logger,
		DrainInterval: 5 * time.Second,
		sleep:         time.Sleep,
	}
}

// SetSleep overrides the drain wait (tests).
func (r *PackReclaimer) SetSleep(f func(time.Duration)) { r.sleep = f }

// ObliterateContext reclaims the bytes an owner context held, and only those.
//
// The sequence mirrors Lore's obliterate (docs/DESIGN_lore_evaluation.md §4.3),
// with a pack standing in for Lore's fragment payload:
//
//  1. Drop the context's associations. Once this returns, the owner no longer
//     names the content — that is the obligation. Everything after is reclamation.
//  2. Publish the obliterating mark on each candidate pack. From here a concurrent
//     writer cannot dedup into it (Lookup filters) or associate it (Associate
//     refuses).
//  3. Drop the context's associations AGAIN, to catch a writer that raced step 1
//     and re-associated while the pack still looked live.
//  4. Wait the drain interval, so in-flight writes land their associations.
//  5. For each marked pack, atomically commit obliterated-if-still-unassociated.
//     A pack that gained an association has its mark released and is left alone.
//  6. Purge dedup-index entries, then delete the bytes.
//
// Per-pack failures are logged, counted, and skipped; a later call retries them.
func (r *PackReclaimer) ObliterateContext(ctx context.Context, domain, packContext string) (ReclaimResult, error) {
	start := time.Now()
	res := ReclaimResult{}

	candidates, err := r.assoc.DropContext(ctx, domain, packContext)
	if err != nil {
		return res, fmt.Errorf("reclaim: drop context %q: %w", packContext, err)
	}
	res.PacksConsidered = len(candidates)
	if len(candidates) == 0 {
		res.Duration = time.Since(start)
		return res, nil
	}

	marked := make([]string, 0, len(candidates))
	for _, packHash := range candidates {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		won, merr := r.assoc.MarkObliterating(ctx, domain, packHash)
		if merr != nil {
			r.logger.WarnContext(ctx, "reclaim: mark failed; deferring pack",
				"domain", domain, "pack", packHash, "err", merr)
			res.PacksSkippedError++
			continue
		}
		if !won {
			// Another reclaimer owns this pack, or it is already gone.
			res.PacksHeldByOther++
			continue
		}
		marked = append(marked, packHash)
	}

	// Catch a writer that re-associated this context between step 1 and the mark.
	if _, err := r.assoc.DropContext(ctx, domain, packContext); err != nil {
		r.logger.WarnContext(ctx, "reclaim: post-mark context re-drop failed",
			"domain", domain, "context", packContext, "err", err)
	}

	if len(marked) > 0 && r.DrainInterval > 0 {
		r.sleep(r.DrainInterval)
	}

	for _, packHash := range marked {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		r.finish(ctx, domain, packHash, &res)
	}
	res.Duration = time.Since(start)
	return res, nil
}

// finish decides one marked pack's fate and, if it may go, deletes it.
func (r *PackReclaimer) finish(ctx context.Context, domain, packHash string, res *ReclaimResult) {
	// Atomic re-check + transition: an association that appeared during the drain
	// makes this return false, and the pack is spared. Doing both in one step is
	// what removes the gap between deciding and deleting.
	committed, err := r.assoc.CommitObliterated(ctx, domain, packHash)
	if err != nil {
		r.logger.WarnContext(ctx, "reclaim: commit obliterated failed; deferring pack",
			"domain", domain, "pack", packHash, "err", err)
		res.PacksSkippedError++
		return
	}
	if !committed {
		if rerr := r.assoc.ReleaseMark(ctx, domain, packHash); rerr != nil {
			r.logger.WarnContext(ctx, "reclaim: release mark failed",
				"domain", domain, "pack", packHash, "err", rerr)
			res.PacksSkippedError++
			return
		}
		res.PacksStillReferenced++
		return
	}

	id := packBlobID(domain, packHash)
	// Size for accounting, before the delete. A missing blob is not an error: the
	// state row is authoritative and the bytes may already be gone.
	var size int64
	if md, merr := r.chunks.GetMetadata(ctx, id); merr == nil {
		size = md.Length
	}

	// Purge dedup-index entries BEFORE deleting, so the invariant "every index
	// entry points at an existing pack" holds even if the delete fails.
	if r.index != nil {
		if perr := r.index.PurgePacks(ctx, domain, []string{packHash}); perr != nil {
			r.logger.WarnContext(ctx, "reclaim: dedup-index purge failed; deferring delete",
				"domain", domain, "pack", packHash, "err", perr)
			res.PacksSkippedError++
			return
		}
	}
	if derr := r.chunks.DeleteBlob(ctx, id); derr != nil && !errors.Is(derr, blobstore.ErrBlobNotFound) {
		r.logger.WarnContext(ctx, "reclaim: delete failed",
			"domain", domain, "pack", packHash, "err", derr)
		res.PacksSkippedError++
		return
	}
	res.PacksReclaimed++
	res.BytesReclaimed += size
}

// ---------------------------------------------------------------------------
// MemoryDedupStore association support
// ---------------------------------------------------------------------------

// memAssoc is MemoryDedupStore's association + lifecycle state for one domain.
type memAssoc struct {
	// byPack maps pack hash → set of contexts referencing it.
	byPack map[string]map[string]struct{}
	// byContext maps context → set of pack hashes it references (the reverse
	// index DropContext needs to avoid scanning every pack).
	byContext map[string]map[string]struct{}
	// state maps pack hash → lifecycle state; absent means PackStored.
	state map[string]PackState
	// backfilled records whether associations were populated from manifests.
	backfilled bool
}

// assocFor returns (creating if needed) the association state for a domain.
// Callers must hold m.assocMu.
func (m *MemoryDedupStore) assocFor(domain string) *memAssoc {
	if m.assoc == nil {
		m.assoc = make(map[string]*memAssoc)
	}
	a := m.assoc[domain]
	if a == nil {
		a = &memAssoc{
			byPack:    make(map[string]map[string]struct{}),
			byContext: make(map[string]map[string]struct{}),
			state:     make(map[string]PackState),
		}
		m.assoc[domain] = a
	}
	return a
}

// stateOf reports a pack's lifecycle state; an unrecorded pack is stored.
// Callers must hold m.assocMu.
func (a *memAssoc) stateOf(packHash string) PackState {
	if s, ok := a.state[packHash]; ok {
		return s
	}
	return PackStored
}

var _ PackAssocStore = (*MemoryDedupStore)(nil)

func (m *MemoryDedupStore) Associate(_ context.Context, domain string, assocs []PackAssociation) error {
	if len(assocs) == 0 {
		return nil
	}
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	// Validate the WHOLE batch first: a refusal must record nothing, so the caller
	// can treat the Put as cleanly failed.
	for _, as := range assocs {
		if a.stateOf(as.PackHash) != PackStored {
			return fmt.Errorf("associate pack %s in domain %s: %w", as.PackHash, domain, ErrPackObliterating)
		}
	}
	for _, as := range assocs {
		if a.byPack[as.PackHash] == nil {
			a.byPack[as.PackHash] = make(map[string]struct{})
		}
		a.byPack[as.PackHash][as.Context] = struct{}{}
		if a.byContext[as.Context] == nil {
			a.byContext[as.Context] = make(map[string]struct{})
		}
		a.byContext[as.Context][as.PackHash] = struct{}{}
	}
	return nil
}

func (m *MemoryDedupStore) DropContext(_ context.Context, domain, packContext string) ([]string, error) {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	packs := a.byContext[packContext]
	if len(packs) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(packs))
	for packHash := range packs {
		out = append(out, packHash)
		if ctxs := a.byPack[packHash]; ctxs != nil {
			delete(ctxs, packContext)
			if len(ctxs) == 0 {
				delete(a.byPack, packHash)
			}
		}
	}
	delete(a.byContext, packContext)
	return out, nil
}

func (m *MemoryDedupStore) HasAssociations(_ context.Context, domain string, packHashes []string) (map[string]bool, error) {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	out := make(map[string]bool, len(packHashes))
	for _, packHash := range packHashes {
		out[packHash] = len(a.byPack[packHash]) > 0
	}
	return out, nil
}

func (m *MemoryDedupStore) WalkLivePacks(_ context.Context, domain string, yield func(string) error) error {
	m.assocMu.Lock()
	live := make([]string, 0, len(m.assocFor(domain).byPack))
	for packHash, ctxs := range m.assocFor(domain).byPack {
		if len(ctxs) > 0 {
			live = append(live, packHash)
		}
	}
	m.assocMu.Unlock()
	for _, packHash := range live {
		if err := yield(packHash); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryDedupStore) MarkObliterating(_ context.Context, domain, packHash string) (bool, error) {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	if a.stateOf(packHash) != PackStored {
		return false, nil
	}
	a.state[packHash] = PackObliterating
	return true, nil
}

func (m *MemoryDedupStore) ReleaseMark(_ context.Context, domain, packHash string) error {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	if a.stateOf(packHash) == PackObliterating {
		a.state[packHash] = PackStored
	}
	return nil
}

func (m *MemoryDedupStore) CommitObliterated(_ context.Context, domain, packHash string) (bool, error) {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	a := m.assocFor(domain)
	if a.stateOf(packHash) != PackObliterating {
		return false, nil
	}
	if len(a.byPack[packHash]) > 0 {
		// An association appeared during the drain: spare the pack.
		return false, nil
	}
	a.state[packHash] = PackObliterated
	return true, nil
}

func (m *MemoryDedupStore) MarkBackfilled(_ context.Context, domain string) error {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	m.assocFor(domain).backfilled = true
	return nil
}

func (m *MemoryDedupStore) IsBackfilled(_ context.Context, domain string) (bool, error) {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	return m.assocFor(domain).backfilled, nil
}

// packUnavailable reports whether a pack must be hidden from dedup lookups
// because it is being or has been reclaimed. This is guard 1 of the safety model:
// a Put must never dedup into a pack on its way out. Callers must NOT hold
// assocMu.
func (m *MemoryDedupStore) packUnavailable(domain, packHash string) bool {
	m.assocMu.Lock()
	defer m.assocMu.Unlock()
	if m.assoc == nil {
		return false
	}
	a := m.assoc[domain]
	if a == nil {
		return false
	}
	return a.stateOf(packHash) != PackStored
}
