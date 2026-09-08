// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// leaderBucket is the JetStream KV bucket holding blobgw's GC leadership lease.
// It is distinct from mlfs's "mlfs_leader" bucket so the two coordination planes
// never collide even when they share a NATS cluster.
const leaderBucket = "blobgw_gc_leader"

// leaseReleaseTimeout bounds the best-effort lease Delete on shutdown. A
// partitioned NATS would otherwise let the Delete block indefinitely and stall
// process exit; when it elapses we rely on the lease's native TTL to free the
// key for a peer instead.
const leaseReleaseTimeout = 3 * time.Second

// Leader is a single-winner election over a TTL'd JetStream KV key, mirroring
// the mlfs coordination plane's Leader (mlfs/internal/coord/leader.go). Exactly
// one live blobgw replica holds the key at a time; the holder renews it
// (resetting the TTL), and if it dies the key expires and a peer claims it.
//
// blobgw cannot import mlfs/internal/coord (it is an internal package of a
// sibling module), so this is a compact, self-contained equivalent that keeps
// the ops surface identical: a JetStream KV lease with native TTL, the same
// claim/renew/stand-down campaign loop, and the same first-writer-wins
// Create-then-CAS-Update semantics. It gates the cluster-wide GC singleton so
// exactly one replica sweeps at a time (ADR-001 §2.4 #29, §4 I3).
type Leader struct {
	kv       jetstream.KeyValue
	key      string
	nodeID   string
	ttl      time.Duration
	renew    time.Duration
	isLeader atomic.Bool
}

// LeaderConfig configures a Leader election.
type LeaderConfig struct {
	// Key is the leadership key within the shared bucket (e.g. "gc"). Different
	// keys elect independent leaders; blobgw GC uses a single "gc" key.
	Key string
	// NodeID uniquely identifies this replica. Two replicas MUST NOT share a
	// NodeID, or both would treat the other's lease as their own.
	NodeID string
	// TTL is the leadership lease lifetime. A holder that fails to renew within
	// TTL loses the lease (the KV key expires) and a peer can claim it. Defaults
	// to 30s when non-positive.
	TTL time.Duration
	// Renew is how often the holder reasserts the lease (resetting its TTL). It
	// MUST be < TTL; defaults to TTL/3 when non-positive or >= TTL.
	Renew time.Duration
	// Replicas is the JetStream KV replication factor for the lease bucket.
	// Defaults to 1; production clustered JetStream (ADR §6.1) should set 3.
	Replicas int
}

// NewLeader provisions the leader KV bucket on conn's JetStream and returns an
// election for cfg.Key. conn must be a connected *nats.Conn with JetStream
// enabled on the server. The bucket is created-or-updated idempotently, so it is
// safe for every replica to call NewLeader concurrently.
func NewLeader(ctx context.Context, conn *nats.Conn, cfg LeaderConfig) (*Leader, error) {
	if conn == nil {
		return nil, errors.New("gc: nil nats connection")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("gc: empty leader node id")
	}
	if cfg.Key == "" {
		cfg.Key = "gc"
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Second
	}
	if cfg.Renew <= 0 || cfg.Renew >= cfg.TTL {
		cfg.Renew = cfg.TTL / 3
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   leaderBucket,
		TTL:      cfg.TTL,
		Storage:  jetstream.FileStorage,
		Replicas: cfg.Replicas,
		History:  1,
	})
	if err != nil {
		return nil, err
	}
	return &Leader{
		kv:     kv,
		key:    cfg.Key,
		nodeID: cfg.NodeID,
		ttl:    cfg.TTL,
		renew:  cfg.Renew,
	}, nil
}

// IsLeader reports whether this node currently holds leadership.
func (l *Leader) IsLeader() bool { return l.isLeader.Load() }

// Run campaigns for and maintains leadership until ctx is cancelled, releasing
// the key on exit if held. It mirrors coord.Leader.Run: an immediate campaign on
// entry, then a renew-cadence ticker. Designed to be launched as a goroutine.
func (l *Leader) Run(ctx context.Context) {
	t := time.NewTicker(l.renew)
	defer t.Stop()
	l.campaign(ctx)
	for {
		select {
		case <-ctx.Done():
			if l.isLeader.Load() {
				// Release the lease so a peer takes over immediately rather than
				// waiting out the TTL. WithoutCancel so a cancelled ctx still
				// lets the delete reach the server, but BOUND it with a short
				// timeout: on a partitioned NATS the Delete would otherwise block
				// indefinitely and stall process exit. On timeout we fall back to
				// TTL expiry (a peer claims the key once the lease lapses).
				relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
				_ = l.kv.Delete(relCtx, l.key)
				cancel()
				l.isLeader.Store(false)
			}
			return
		case <-t.C:
			l.campaign(ctx)
		}
	}
}

// campaign performs one election step: claim the key if free, renew it if ours,
// or stand down if a peer holds it. Mirrors coord.Leader.campaign exactly.
func (l *Leader) campaign(ctx context.Context) {
	entry, err := l.kv.Get(ctx, l.key)
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		// Free: try to claim atomically. Create fails if a peer claimed it
		// first, so only one replica wins the race.
		if _, cerr := l.kv.Create(ctx, l.key, []byte(l.nodeID)); cerr == nil {
			l.isLeader.Store(true)
		} else {
			l.isLeader.Store(false) // lost the race
		}
	case err != nil:
		l.isLeader.Store(false)
	default:
		if string(entry.Value()) == l.nodeID {
			// Ours: renew (reset TTL) via revision CAS. A failed CAS means a peer
			// raced us; stand down and re-evaluate next tick.
			if _, uerr := l.kv.Update(ctx, l.key, []byte(l.nodeID), entry.Revision()); uerr == nil {
				l.isLeader.Store(true)
			} else {
				l.isLeader.Store(false)
			}
		} else {
			l.isLeader.Store(false)
		}
	}
}
