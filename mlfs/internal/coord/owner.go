// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// liveBucket holds one entry per owned scope; its native TTL is the lease
// lifetime, so an owner that stops refreshing has its key auto-expire and a
// contender (or Watch) sees the scope go free without any SQL poll.
const liveBucket = "mlfs_live"

// OwnershipStore is the authoritative (SQL) side the NATS owner defers to for
// the fence generation. *meta.Engine satisfies it.
type OwnershipStore interface {
	AcquireOrResume(ctx context.Context, scopeKey, node string, ttlMs int64) (meta.Lease, bool, error)
	Refresh(ctx context.Context, scopeKey, node string, generation, ttlMs int64) (bool, error)
	ReleaseOwnership(ctx context.Context, scopeKey, node string, generation int64) (bool, error)
}

// liveVal is the JSON payload stored in mlfs_live[scope].
type liveVal struct {
	Node string `json:"n"`
	Gen  int64  `json:"g"`
}

// NATSOwner implements meta.OwnerView. It keeps lease LIVENESS in a JetStream KV
// bucket (fast refresh + Watch-based discovery) and takes the authoritative
// fence GENERATION from the SQL ownership row via OwnershipStore. This is the
// low-latency path the plan calls for: the per-refresh and per-discovery traffic
// is NATS, not PostgreSQL; SQL is consulted only to mint/bump a generation on a
// real acquire or steal, and as the periodic durability backstop.
type NATSOwner struct {
	sql     OwnershipStore
	kv      jetstream.KeyValue
	nc      *nats.Conn // for the ownership-handoff request/reply (L2.8.4)
	nodeID  string
	ttl     time.Duration
	refresh time.Duration
	now     func() time.Time

	// Handoff tuning (L2.8.4). minHold is the hysteresis floor: a scope is not
	// handed off until held at least this long, preventing ping-pong thrash on a
	// hot shared scope. releaseHook flushes any node-local dirty state (e.g. the
	// write-back cache) so the new owner can read it from the shared backing
	// store; nil disables the flush.
	handoffTimeout time.Duration
	minHold        time.Duration
	releaseHook    func(ctx context.Context, scope string) error

	mu          sync.Mutex
	held        map[string]ownEntry
	sqlBackTick int // SQL backstop runs every Nth refresh

	// metrics, when set, counts lease acquire/lose transitions (multi-node
	// canaries). An interface keeps coord free of a hard internal/metrics
	// dependency; *metrics.Registry satisfies it. Nil = no recording.
	metrics LeaseMetrics
}

// LeaseMetrics is the owner's optional metrics hook (the daemon's
// *metrics.Registry satisfies it). LeaseAcquired fires when this node takes a
// scope's lease; LeaseLost fires when it drops one to a peer steal.
type LeaseMetrics interface {
	LeaseAcquired()
	LeaseLost()
}

// SetMetrics installs the lease-transition counter hook (nil clears it). Call
// before Run.
func (o *NATSOwner) SetMetrics(m LeaseMetrics) { o.metrics = m }

type ownEntry struct {
	gen         int64
	localExpiry time.Time
	acquiredAt  time.Time // for handoff hysteresis (minHold)
}

// NewNATSOwner provisions the liveness bucket and returns an owner for nodeID.
// ttl is the lease lifetime (bucket TTL); refresh is the NATS renew interval
// (<= ttl/2). replicas sets KV replication for clustered deployments (1 for
// single-node).
func NewNATSOwner(ctx context.Context, n *NATS, sql OwnershipStore, nodeID string, ttl, refresh time.Duration, replicas int) (*NATSOwner, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if refresh <= 0 || refresh >= ttl {
		refresh = ttl / 3
	}
	if replicas <= 0 {
		replicas = 1
	}
	kv, err := n.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   liveBucket,
		TTL:      ttl, // native expiry == lease liveness
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
		History:  1,
	})
	if err != nil {
		return nil, err
	}
	return &NATSOwner{
		sql:            sql,
		kv:             kv,
		nc:             n.Conn(),
		nodeID:         nodeID,
		ttl:            ttl,
		refresh:        refresh,
		now:            time.Now,
		handoffTimeout: 2 * time.Second,
		minHold:        2 * time.Second,
		held:           make(map[string]ownEntry),
	}, nil
}

// SetReleaseHook installs a callback invoked before this node releases a scope
// during a handoff — used to flush node-local dirty state (e.g. the write-back
// cache) so the incoming owner can read it from the shared backing store.
func (o *NATSOwner) SetReleaseHook(fn func(ctx context.Context, scope string) error) {
	o.releaseHook = fn
}

// Enabled reports that multi-node fencing is active.
func (o *NATSOwner) Enabled() bool { return true }

// Ensure acquires-or-confirms this node's lease on scope. Order of checks,
// cheapest first: in-memory cache → NATS liveness (is it free / ours / a live
// peer's?) → SQL AcquireOrResume (authoritative generation) only when free.
func (o *NATSOwner) Ensure(ctx context.Context, scope string) (int64, bool, error) {
	o.mu.Lock()
	if e, ok := o.held[scope]; ok && o.now().Before(e.localExpiry) {
		gen := e.gen
		o.mu.Unlock()
		return gen, true, nil
	}
	o.mu.Unlock()

	// NATS liveness: a present entry owned by a live PEER means we cannot just
	// steal. Request a handoff; if the owner releases (or is gone) we fall
	// through and acquire, otherwise report not-held (caller gets ESTALE).
	if entry, err := o.kv.Get(ctx, natsKey(scope)); err == nil {
		var lv liveVal
		if json.Unmarshal(entry.Value(), &lv) == nil && lv.Node != "" && lv.Node != o.nodeID {
			if !o.requestHandoff(ctx, scope) {
				return 0, false, nil // owner kept it (hysteresis) or no response
			}
			// fall through: the prior owner released; acquire below.
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false, err
	}

	// Free (or already ours): take the authoritative generation from SQL.
	lease, got, err := o.sql.AcquireOrResume(ctx, scope, o.nodeID, o.ttl.Milliseconds())
	if err != nil {
		return 0, false, err
	}
	if !got {
		return 0, false, nil
	}
	if err := o.putLive(ctx, scope, lease.Generation); err != nil {
		return 0, false, err
	}
	o.mu.Lock()
	_, wasHeld := o.held[scope]
	o.held[scope] = ownEntry{gen: lease.Generation, localExpiry: o.now().Add(o.ttl), acquiredAt: o.now()}
	o.mu.Unlock()
	// Count a genuine acquire (this node did not already hold the scope) — a
	// renew/resume of an already-held scope is not a transition.
	if !wasHeld && o.metrics != nil {
		o.metrics.LeaseAcquired()
	}
	return lease.Generation, true, nil
}

// dropHeld removes scope from the held set (a steal/fence detected during
// refresh) and counts one lease loss if this node actually held it. Idempotent:
// a second drop of an already-gone scope counts nothing.
func (o *NATSOwner) dropHeld(scope string) {
	o.mu.Lock()
	_, wasHeld := o.held[scope]
	delete(o.held, scope)
	o.mu.Unlock()
	if wasHeld && o.metrics != nil {
		o.metrics.LeaseLost()
	}
}

func (o *NATSOwner) putLive(ctx context.Context, scope string, gen int64) error {
	v, _ := json.Marshal(liveVal{Node: o.nodeID, Gen: gen})
	_, err := o.kv.Put(ctx, natsKey(scope), v)
	return err
}

// Run renews held leases until ctx is cancelled. Every tick refreshes NATS
// liveness (fast); every Nth tick also bumps the SQL lease_expires backstop so
// the authoritative row never looks falsely dead if NATS state is lost. A scope
// observed owned by a peer (we were stolen) is dropped.
func (o *NATSOwner) Run(ctx context.Context) {
	t := time.NewTicker(o.refresh)
	defer t.Stop()
	// SQL backstop cadence: at least every ttl/2.
	backEvery := int(o.ttl / 2 / o.refresh)
	if backEvery < 1 {
		backEvery = 1
	}
	for {
		select {
		case <-ctx.Done():
			o.releaseAll(ctx)
			return
		case <-t.C:
			o.mu.Lock()
			o.sqlBackTick++
			doSQL := o.sqlBackTick%backEvery == 0
			snap := make(map[string]int64, len(o.held))
			for s, e := range o.held {
				snap[s] = e.gen
			}
			o.mu.Unlock()
			o.refreshScopes(ctx, snap, doSQL)
		}
	}
}

func (o *NATSOwner) refreshScopes(ctx context.Context, snap map[string]int64, doSQL bool) {
	for scope, gen := range snap {
		// Detect a steal: if NATS shows a different owner, drop the lease.
		if entry, err := o.kv.Get(ctx, natsKey(scope)); err == nil {
			var lv liveVal
			if json.Unmarshal(entry.Value(), &lv) == nil && lv.Node != o.nodeID {
				o.dropHeld(scope)
				continue
			}
		}
		if err := o.putLive(ctx, scope, gen); err != nil {
			continue
		}
		if doSQL {
			if ok, err := o.sql.Refresh(ctx, scope, o.nodeID, gen, o.ttl.Milliseconds()); err == nil && !ok {
				// SQL says we were fenced: drop.
				o.dropHeld(scope)
				continue
			}
		}
		o.mu.Lock()
		if e, ok := o.held[scope]; ok {
			e.localExpiry = o.now().Add(o.ttl)
			o.held[scope] = e
		}
		o.mu.Unlock()
	}
}

// releaseAll deletes this node's liveness keys on shutdown so a successor can
// take over immediately (rather than waiting out the TTL). The SQL generation
// is left intact to fence any late writes from this node.
func (o *NATSOwner) releaseAll(ctx context.Context) {
	o.mu.Lock()
	scopes := make([]string, 0, len(o.held))
	for s := range o.held {
		scopes = append(scopes, s)
	}
	o.held = make(map[string]ownEntry)
	o.mu.Unlock()
	for _, s := range scopes {
		_ = o.kv.Delete(ctx, natsKey(s))
	}
}

// natsKey maps a scope_key to a NATS-KV-safe key. scope keys are "root" or
// "ino:<n>"; the colon is not a legal KV key char, so encode it.
func natsKey(scope string) string {
	out := make([]byte, 0, len(scope))
	for i := 0; i < len(scope); i++ {
		c := scope[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '_')
			out = strconv.AppendUint(out, uint64(c), 16)
		}
	}
	return string(out)
}
