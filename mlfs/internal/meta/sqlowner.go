// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"sync"
	"time"
)

// SQLOwner is a pure-PostgreSQL OwnerView: it drives the ownership-lease
// primitives (AcquireOrResume / Refresh) already on the Engine. It makes the
// engine multi-node-SAFE without any external coordinator — the fence is fully
// authoritative in SQL. It is also the reference an accelerator (the NATS-backed
// coordinator in internal/coord) is measured against: that swap replaces the
// liveness/refresh traffic with KV+Watch but keeps this exact fence contract.
//
// Lifecycle: construct with NewSQLOwner, register via Engine.SetCoordinator,
// then run Run(ctx) in a goroutine to keep held leases alive. Ensure is called
// on the write path (cheap in-memory fast path when the lease is already held);
// the authoritative re-check happens in the write tx via checkFence.
type SQLOwner struct {
	e       *Engine
	nodeID  string
	ttl     time.Duration
	refresh time.Duration
	now     func() time.Time

	mu   sync.Mutex
	held map[string]heldLease
}

type heldLease struct {
	gen         int64
	localExpiry time.Time // when we stop trusting the in-memory fast path
}

// NewSQLOwner builds a SQL-backed owner for nodeID. ttl is the lease lifetime
// (a foreign owner is considered dead after it); refresh is how often Run renews
// held leases (should be <= ttl/2). Zero values fall back to 30s / 10s.
func NewSQLOwner(e *Engine, nodeID string, ttl, refresh time.Duration) *SQLOwner {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if refresh <= 0 || refresh >= ttl {
		refresh = ttl / 3
	}
	return &SQLOwner{
		e:       e,
		nodeID:  nodeID,
		ttl:     ttl,
		refresh: refresh,
		now:     time.Now,
		held:    make(map[string]heldLease),
	}
}

// Enabled reports that multi-node fencing is active.
func (o *SQLOwner) Enabled() bool { return true }

// Ensure acquires-or-confirms this node's lease on scope. The fast path returns
// a still-valid cached generation with no SQL round-trip; otherwise it calls
// AcquireOrResume. held=false means a live foreign owner holds the scope.
//
// A stale cache (we believe we hold it but were stolen) is harmless: Ensure
// returns the cached generation, and checkFence in the write tx rejects it
// against the authoritative SQL row, yielding ESTALE. Run also drops the lease
// on the next refresh.
func (o *SQLOwner) Ensure(ctx context.Context, scope string) (int64, bool, error) {
	o.mu.Lock()
	if l, ok := o.held[scope]; ok && o.now().Before(l.localExpiry) {
		gen := l.gen
		o.mu.Unlock()
		return gen, true, nil
	}
	o.mu.Unlock()

	lease, got, err := o.e.AcquireOrResume(ctx, scope, o.nodeID, o.ttl.Milliseconds())
	if err != nil {
		return 0, false, err
	}
	if !got {
		return 0, false, nil
	}
	o.mu.Lock()
	o.held[scope] = heldLease{gen: lease.Generation, localExpiry: o.now().Add(o.ttl)}
	o.mu.Unlock()
	return lease.Generation, true, nil
}

// Run renews every held lease on a ticker until ctx is cancelled. A lease whose
// Refresh fails (the scope was stolen) is dropped, so the next write re-acquires
// or reports ESTALE. Idle eviction is intentionally omitted from this first cut
// (see docs/L2.8_IMPLEMENTATION.md §4); a held scope stays held until lost.
func (o *SQLOwner) Run(ctx context.Context) {
	t := time.NewTicker(o.refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			o.releaseAll(ctx)
			return
		case <-t.C:
			o.refreshAll(ctx)
		}
	}
}

func (o *SQLOwner) refreshAll(ctx context.Context) {
	// Snapshot under lock, refresh outside, then apply results.
	o.mu.Lock()
	scopes := make(map[string]int64, len(o.held))
	for s, l := range o.held {
		scopes[s] = l.gen
	}
	o.mu.Unlock()

	for scope, gen := range scopes {
		ok, err := o.e.Refresh(ctx, scope, o.nodeID, gen, o.ttl.Milliseconds())
		o.mu.Lock()
		if err != nil {
			// Transient error: keep the lease but let the local fast path lapse so
			// the next Ensure re-validates against SQL.
			if l, present := o.held[scope]; present {
				l.localExpiry = o.now()
				o.held[scope] = l
			}
		} else if !ok {
			delete(o.held, scope) // stolen/fenced out
		} else if l, present := o.held[scope]; present {
			l.localExpiry = o.now().Add(o.ttl)
			o.held[scope] = l
		}
		o.mu.Unlock()
	}
}

// releaseAll lets every held lease lapse locally on shutdown. It does not delete
// the SQL ownership rows: the generation must persist so a successor that steals
// the (now-unrefreshed, soon-expired) scope bumps it and fences any late writes
// from this node. The rows expire naturally via lease_expires_unix_ms.
func (o *SQLOwner) releaseAll(ctx context.Context) {
	o.mu.Lock()
	o.held = make(map[string]heldLease)
	o.mu.Unlock()
}
