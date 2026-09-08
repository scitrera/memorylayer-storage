// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// Ownership handoff (L2.8.4). When a node needs to write a scope a live peer
// owns, it asks the owner to release rather than failing outright. The owner,
// subject to a min-hold hysteresis floor, flushes node-local dirty state,
// releases the SQL lease + NATS liveness key, and replies; the requester then
// acquires (bumping the generation) and proceeds. Contention thus serializes
// gracefully instead of returning ESTALE-and-retry.
//
// Protocol: request/reply on a single subject; the payload is the scope key.
// Every node subscribes, but only the current owner replies — "ok" once it has
// released, or "no" if it declines under hysteresis. Non-owners stay silent, so
// the requester's reply is always from the authoritative owner (or a timeout if
// the owner has since died, in which case the scope is free to steal anyway).
const handoffSubject = "mlfs.own.req"

const (
	handoffOK      = "ok"
	handoffDecline = "no"
)

// StartHandoffResponder subscribes this node as a handoff responder until ctx is
// cancelled. Safe to call once after construction.
func (o *NATSOwner) StartHandoffResponder(ctx context.Context) error {
	sub, err := o.nc.Subscribe(handoffSubject, func(msg *nats.Msg) {
		o.handleHandoff(ctx, msg)
	})
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()
	return nil
}

// handleHandoff replies only if THIS node owns the requested scope.
func (o *NATSOwner) handleHandoff(ctx context.Context, msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	scope := string(msg.Data)

	o.mu.Lock()
	e, owned := o.held[scope]
	heldFor := o.now().Sub(e.acquiredAt)
	o.mu.Unlock()
	if !owned {
		return // not ours: stay silent so the real owner answers
	}
	if heldFor < o.minHold {
		_ = msg.Respond([]byte(handoffDecline)) // hysteresis: keep it for now
		return
	}

	// Release: flush node-local dirty state so the new owner can read it, then
	// give up the SQL lease and the NATS liveness key.
	rctx, cancel := context.WithTimeout(ctx, o.handoffTimeout)
	defer cancel()
	if o.releaseHook != nil {
		if err := o.releaseHook(rctx, scope); err != nil {
			_ = msg.Respond([]byte(handoffDecline)) // could not flush: hold on
			return
		}
	}
	if _, err := o.sql.ReleaseOwnership(rctx, scope, o.nodeID, e.gen); err != nil {
		_ = msg.Respond([]byte(handoffDecline))
		return
	}
	_ = o.kv.Delete(rctx, natsKey(scope))
	o.mu.Lock()
	delete(o.held, scope)
	o.mu.Unlock()
	_ = msg.Respond([]byte(handoffOK))
}

// requestHandoff asks the current owner of scope to release it. Returns true if
// the owner released (or, on timeout, there is no live owner to object — the
// caller re-checks via AcquireOrResume either way).
func (o *NATSOwner) requestHandoff(ctx context.Context, scope string) bool {
	if o.nc == nil {
		return false
	}
	to := o.handoffTimeout
	if to <= 0 {
		to = 2 * time.Second
	}
	reply, err := o.nc.Request(handoffSubject, []byte(scope), to)
	if err != nil {
		// No responder before the timeout: the owner is likely gone. Let the
		// caller try to acquire — if a live owner still holds the SQL lease,
		// AcquireOrResume returns not-held and the caller gets ESTALE.
		return true
	}
	return string(reply.Data) == handoffOK
}
