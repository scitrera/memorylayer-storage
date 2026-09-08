// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/coord"
	fusebridge "github.com/scitrera/memorylayer-storage/mlfs/internal/fuse"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

// coordConfig is the multi-node coordination wiring (L2.8). Coordination is OFF
// unless NodeID is set; then the daemon embeds (or connects to) NATS JetStream
// for lease liveness, the node registry, and GC leadership, while PostgreSQL
// stays the authoritative fence.
type coordConfig struct {
	NodeID      string
	NATSURL     string // external NATS; empty = embedded server
	Creds       string // path to a NATS credentials file (per-dedup-domain account) for external NATS
	Cluster     string // embedded JetStream cluster name; empty = standalone in-proc
	ListenHost  string
	ListenPort  int
	ClusterPort int
	Routes      []string
	StoreDir    string
	TTL         time.Duration
	Refresh     time.Duration
	Replicas    int
}

// coordination bundles the running coordination components for shutdown.
type coordination struct {
	nats   *coord.NATS
	owner  *coord.NATSOwner
	reg    *coord.Registry
	leader *coord.Leader
	cf     *coord.Changefeed
}

// setupCoordination starts the coordination plane and registers the engine's
// ownership coordinator. The caller runs (*coordination).run and defers Close.
func setupCoordination(ctx context.Context, engine *meta.Engine, cfg coordConfig, mreg *metrics.Registry) (*coordination, error) {
	var n *coord.NATS
	var err error
	if cfg.NATSURL != "" {
		// Per-dedup-domain isolation on a shared NATS backbone: the credentials
		// select this tenant's NATS account, which gives it an isolated JetStream
		// (its own KV buckets + streams), so identical bucket names / scope keys
		// never collide across domains. See docs/multi-tenant-coordination.md.
		var nopts []nats.Option
		if cfg.Creds != "" {
			nopts = append(nopts, nats.UserCredentials(cfg.Creds))
		}
		n, err = coord.Connect(cfg.NATSURL, nopts...)
	} else {
		n, err = coord.StartEmbedded(coord.EmbeddedConfig{
			NodeName:    cfg.NodeID,
			StoreDir:    cfg.StoreDir,
			ClusterName: cfg.Cluster,
			ListenHost:  cfg.ListenHost,
			ListenPort:  cfg.ListenPort,
			ClusterPort: cfg.ClusterPort,
			Routes:      cfg.Routes,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("coordination nats: %w", err)
	}

	owner, err := coord.NewNATSOwner(ctx, n, engine, cfg.NodeID, cfg.TTL, cfg.Refresh, cfg.Replicas)
	if err != nil {
		n.Close()
		return nil, fmt.Errorf("coordination owner: %w", err)
	}
	reg, err := coord.NewRegistry(ctx, n, cfg.NodeID, engine.LockSID(), cfg.TTL, cfg.Refresh, cfg.Replicas)
	if err != nil {
		n.Close()
		return nil, fmt.Errorf("coordination registry: %w", err)
	}
	leader, err := coord.NewLeader(ctx, n, "gc", cfg.NodeID, cfg.TTL, cfg.Refresh, cfg.Replicas)
	if err != nil {
		n.Close()
		return nil, fmt.Errorf("coordination leader: %w", err)
	}

	// Multi-node event canaries: lease acquire/lose on the owner, leader-election
	// transitions on the election. Only wired when metrics are enabled, so the
	// hooks' nil guards stay meaningful.
	if mreg != nil {
		owner.SetMetrics(mreg)
		leader.SetMetrics(mreg)
	}

	engine.SetCoordinator(cfg.NodeID, owner)
	return &coordination{nats: n, owner: owner, reg: reg, leader: leader, cf: coord.NewChangefeed(n)}, nil
}

// startChangefeed begins fanning out + applying cross-node cache invalidations:
// the GC leader tails the changelog and publishes; this node subscribes and
// invalidates the bridge's kernel caches. Called after the bridge is mounted.
func (c *coordination) startChangefeed(ctx context.Context, scanner coord.ChangelogScanner, bridge *fusebridge.Bridge, interval time.Duration) error {
	go c.cf.RunPublisher(ctx, scanner, c.leader.IsLeader, interval)
	return c.cf.Subscribe(ctx, func(m coord.ChangeMsg) {
		bridge.ApplyChange(m.Ino, m.Op, m.Payload)
	})
}

// run starts the background loops (lease refresh, heartbeat, leader election)
// and the ownership-handoff responder.
func (c *coordination) run(ctx context.Context) error {
	if err := c.owner.StartHandoffResponder(ctx); err != nil {
		return fmt.Errorf("coordination handoff responder: %w", err)
	}
	go c.owner.Run(ctx)
	go c.reg.Run(ctx)
	go c.leader.Run(ctx)
	return nil
}

// Close tears down the NATS connection/server (the background loops stop when
// the shared context is cancelled).
func (c *coordination) Close() {
	if c != nil && c.nats != nil {
		c.nats.Close()
	}
}
