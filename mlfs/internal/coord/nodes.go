// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const nodesBucket = "mlfs_nodes"

// Registry tracks live mounts via a TTL'd JetStream KV bucket: each node
// heartbeats its lock-session id (sid), and the entry auto-expires if the node
// dies. The liveness reaper diffs the live sids against the lock tables to clear
// a dead peer's stale flock/plock rows — the multi-node replacement for the
// single-node wholesale ClearAllLocks.
type Registry struct {
	kv     jetstream.KeyValue
	nodeID string
	sid    uint64
	ttl    time.Duration
	beat   time.Duration
}

// NewRegistry provisions the node bucket and returns a registry for nodeID/sid.
func NewRegistry(ctx context.Context, n *NATS, nodeID string, sid uint64, ttl, beat time.Duration, replicas int) (*Registry, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if beat <= 0 || beat >= ttl {
		beat = ttl / 3
	}
	if replicas <= 0 {
		replicas = 1
	}
	kv, err := n.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   nodesBucket,
		TTL:      ttl,
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
		History:  1,
	})
	if err != nil {
		return nil, err
	}
	return &Registry{kv: kv, nodeID: nodeID, sid: sid, ttl: ttl, beat: beat}, nil
}

// Heartbeat publishes this node's liveness once.
func (r *Registry) Heartbeat(ctx context.Context) error {
	_, err := r.kv.Put(ctx, natsKey(r.nodeID), []byte(strconv.FormatUint(r.sid, 10)))
	return err
}

// Run heartbeats on a ticker until ctx is cancelled, then removes the entry.
func (r *Registry) Run(ctx context.Context) {
	_ = r.Heartbeat(ctx)
	t := time.NewTicker(r.beat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = r.kv.Delete(context.WithoutCancel(ctx), natsKey(r.nodeID))
			return
		case <-t.C:
			_ = r.Heartbeat(ctx)
		}
	}
}

// LiveSIDs returns the lock-session ids of all currently-live nodes (entries
// that have not expired).
func (r *Registry) LiveSIDs(ctx context.Context) (map[uint64]struct{}, error) {
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return map[uint64]struct{}{}, nil
		}
		return nil, err
	}
	live := make(map[uint64]struct{}, len(keys))
	for _, k := range keys {
		entry, err := r.kv.Get(ctx, k)
		if err != nil {
			continue // expired between Keys and Get
		}
		if sid, perr := strconv.ParseUint(string(entry.Value()), 10, 64); perr == nil {
			live[sid] = struct{}{}
		}
	}
	return live, nil
}

// LockReaper is the meta side the reaper needs: list lock-holding sids and clear
// one sid's rows. *meta.Engine satisfies it.
type LockReaper interface {
	ListLockSIDs(ctx context.Context) ([]uint64, error)
	ClearLocksForSID(ctx context.Context, sid uint64) error
}

// ReapDeadLocks clears flock/plock rows whose owning sid is not in the live set.
// Run it on the GC leader only. Returns the number of dead sessions reaped.
func (r *Registry) ReapDeadLocks(ctx context.Context, m LockReaper) (int, error) {
	live, err := r.LiveSIDs(ctx)
	if err != nil {
		return 0, err
	}
	// A node with no locks may still be alive but absent from the lock tables;
	// that's fine — we only ever DELETE rows for sids confirmed not-live.
	sids, err := m.ListLockSIDs(ctx)
	if err != nil {
		return 0, err
	}
	reaped := 0
	for _, sid := range sids {
		if _, ok := live[sid]; ok {
			continue
		}
		if err := m.ClearLocksForSID(ctx, sid); err != nil {
			return reaped, err
		}
		reaped++
	}
	return reaped, nil
}
