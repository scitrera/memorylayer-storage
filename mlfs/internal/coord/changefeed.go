// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// Change feed (L2.8.5). A LATENCY optimization, not a correctness requirement:
// mlfs reads always hit Postgres live (no in-engine cache), so close-to-open
// consistency already holds across nodes. The feed exists only to invalidate
// kernel/FUSE caches faster than their attr/entry timeouts, so a write on one
// node becomes visible on another in ~poll-interval instead of ~TTL.
//
// Design: the GC leader tails the durable fs_changelog (already written in-tx by
// every mutation — the durable rail) since its last seen LSN and fans each entry
// out over a NATS subject (the fast rail). Every node subscribes and applies the
// change to its FUSE cache. One node polls SQL; N learn over NATS. Best-effort:
// a dropped NATS message costs only a slightly staler cache until the next op or
// timeout — never correctness.
const changesSubject = "mlfs.changes"

// ChangelogScanner is the durable-rail source. *meta.Engine satisfies it.
type ChangelogScanner interface {
	ScanChangelog(ctx context.Context, after int64, fn func(meta.ChangeEntry) error) error
}

// ChangeMsg is one fan-out entry: the inode, op code (meta.Change*), and the
// op's raw fs_changelog payload (parent/name/etc.) for entry invalidation.
type ChangeMsg struct {
	LSN     int64           `json:"l"`
	Ino     uint64          `json:"i"`
	Op      uint8           `json:"o"`
	Payload json.RawMessage `json:"p"`
}

// Changefeed publishes and subscribes change events over NATS.
type Changefeed struct {
	nc *nats.Conn
}

// NewChangefeed builds a change feed over the coordination NATS connection.
func NewChangefeed(n *NATS) *Changefeed { return &Changefeed{nc: n.Conn()} }

// RunPublisher tails the changelog and fans out new entries while this node is
// the GC leader (so exactly one node publishes). It advances its LSN cursor past
// whatever exists at startup so it only forwards changes from here on. Returns
// when ctx is cancelled.
func (c *Changefeed) RunPublisher(ctx context.Context, scanner ChangelogScanner, isLeader func() bool, interval time.Duration) {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	var lastLSN int64
	// Start at the current tail: don't replay history on boot.
	_ = scanner.ScanChangelog(ctx, 0, func(e meta.ChangeEntry) error {
		if e.LSN > lastLSN {
			lastLSN = e.LSN
		}
		return nil
	})
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if isLeader != nil && !isLeader() {
				continue
			}
			_ = scanner.ScanChangelog(ctx, lastLSN, func(e meta.ChangeEntry) error {
				msg := ChangeMsg{LSN: e.LSN, Ino: uint64(e.Ino), Op: e.Op, Payload: e.Payload}
				if b, err := json.Marshal(msg); err == nil {
					_ = c.nc.Publish(changesSubject, b)
				}
				if e.LSN > lastLSN {
					lastLSN = e.LSN
				}
				return nil
			})
		}
	}
}

// Subscribe delivers fan-out change events to handler until ctx is cancelled.
func (c *Changefeed) Subscribe(ctx context.Context, handler func(ChangeMsg)) error {
	sub, err := c.nc.Subscribe(changesSubject, func(m *nats.Msg) {
		var msg ChangeMsg
		if json.Unmarshal(m.Data, &msg) == nil {
			handler(msg)
		}
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
