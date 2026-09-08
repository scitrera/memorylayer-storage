// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const leaderBucket = "mlfs_leader"

// Leader is a single-winner election over a TTL'd JetStream KV key. Exactly one
// live node holds the key at a time; the holder renews it (resetting the TTL),
// and if it dies the key expires and a peer claims it. Used to gate cluster-wide
// singletons — GC, changelog trim, and the lock reaper — so they run on one node
// only (closing the plan's GC-leader-lock residual #29).
type Leader struct {
	kv       jetstream.KeyValue
	key      string
	nodeID   string
	ttl      time.Duration
	renew    time.Duration
	isLeader atomic.Bool

	// metrics, when set, counts leadership transitions (became/lost leader). An
	// interface keeps coord free of a hard internal/metrics dependency;
	// *metrics.Registry satisfies it. Nil = no recording.
	metrics LeaderMetrics
}

// LeaderMetrics is the election's optional metrics hook (the daemon's
// *metrics.Registry satisfies it). LeaderTransition fires only on an actual role
// change, with leader=true when this node just became leader.
type LeaderMetrics interface {
	LeaderTransition(leader bool)
}

// SetMetrics installs the leadership-transition counter hook (nil clears it).
// Call before Run.
func (l *Leader) SetMetrics(m LeaderMetrics) { l.metrics = m }

// setLeader updates the leadership flag and, on an actual change, counts the
// transition. Centralizes the metric so every campaign branch records it.
func (l *Leader) setLeader(v bool) {
	if l.isLeader.Swap(v) != v && l.metrics != nil {
		l.metrics.LeaderTransition(v)
	}
}

// NewLeader provisions the leader bucket and returns an election for key (e.g.
// "gc"). ttl is the leadership lease; renew (<= ttl/2) is how often the holder
// reasserts it.
func NewLeader(ctx context.Context, n *NATS, key, nodeID string, ttl, renew time.Duration, replicas int) (*Leader, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if renew <= 0 || renew >= ttl {
		renew = ttl / 3
	}
	if replicas <= 0 {
		replicas = 1
	}
	kv, err := n.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   leaderBucket,
		TTL:      ttl,
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
		History:  1,
	})
	if err != nil {
		return nil, err
	}
	return &Leader{kv: kv, key: natsKey(key), nodeID: nodeID, ttl: ttl, renew: renew}, nil
}

// IsLeader reports whether this node currently holds leadership.
func (l *Leader) IsLeader() bool { return l.isLeader.Load() }

// Run campaigns for and maintains leadership until ctx is cancelled, releasing
// the key on exit if held.
func (l *Leader) Run(ctx context.Context) {
	t := time.NewTicker(l.renew)
	defer t.Stop()
	l.campaign(ctx)
	for {
		select {
		case <-ctx.Done():
			if l.isLeader.Load() {
				_ = l.kv.Delete(context.WithoutCancel(ctx), l.key)
			}
			return
		case <-t.C:
			l.campaign(ctx)
		}
	}
}

// campaign performs one election step: claim the key if free, renew it if ours,
// or stand down if a peer holds it.
func (l *Leader) campaign(ctx context.Context) {
	entry, err := l.kv.Get(ctx, l.key)
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		// Free: try to claim atomically.
		if _, cerr := l.kv.Create(ctx, l.key, []byte(l.nodeID)); cerr == nil {
			l.setLeader(true)
		} else {
			l.setLeader(false) // lost the race
		}
	case err != nil:
		l.setLeader(false)
	default:
		if string(entry.Value()) == l.nodeID {
			// Ours: renew (reset TTL) via revision CAS.
			if _, uerr := l.kv.Update(ctx, l.key, []byte(l.nodeID), entry.Revision()); uerr == nil {
				l.setLeader(true)
			} else {
				l.setLeader(false)
			}
		} else {
			l.setLeader(false)
		}
	}
}
