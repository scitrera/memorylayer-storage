// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc_test

import (
	"context"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gc"
)

// TestLeader_SingleWinner asserts that with two Leader instances campaigning on
// the same NATS/JetStream KV for the same key, exactly ONE becomes leader.
func TestLeader_SingleWinner(t *testing.T) {
	connect := startJetStreamNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Short TTL/renew so the test moves fast.
	cfg := gc.LeaderConfig{Key: "gc", TTL: time.Second, Renew: 100 * time.Millisecond}

	cfgA := cfg
	cfgA.NodeID = "node-a"
	la, err := gc.NewLeader(ctx, connect("a"), cfgA)
	if err != nil {
		t.Fatalf("NewLeader a: %v", err)
	}
	cfgB := cfg
	cfgB.NodeID = "node-b"
	lb, err := gc.NewLeader(ctx, connect("b"), cfgB)
	if err != nil {
		t.Fatalf("NewLeader b: %v", err)
	}

	go la.Run(ctx)
	go lb.Run(ctx)

	// One of them must become leader.
	waitFor(t, 5*time.Second, "a leader to be elected", func() bool {
		return la.IsLeader() || lb.IsLeader()
	})

	// Exactly one — never both — across several observations.
	for i := 0; i < 10; i++ {
		if la.IsLeader() && lb.IsLeader() {
			t.Fatalf("both instances claim leadership simultaneously")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestLeader_Failover asserts that when the current leader is LOST (its Run
// context is cancelled, as on process death/shutdown), the standby instance
// takes over — the core "another takes over" guarantee. Each leader runs under
// its OWN context so the incumbent can be stopped independently.
func TestLeader_Failover(t *testing.T) {
	connect := startJetStreamNATS(t)

	cfg := gc.LeaderConfig{Key: "gc", TTL: time.Second, Renew: 100 * time.Millisecond}

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA() // released at test end; the takeover assertion finishes first
	cfgA := cfg
	cfgA.NodeID = "node-a"
	la, err := gc.NewLeader(ctxA, connect("a"), cfgA)
	if err != nil {
		t.Fatalf("NewLeader a: %v", err)
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	cfgB := cfg
	cfgB.NodeID = "node-b"
	lb, err := gc.NewLeader(ctxB, connect("b"), cfgB)
	if err != nil {
		t.Fatalf("NewLeader b: %v", err)
	}

	doneA := make(chan struct{})
	go func() { defer close(doneA); la.Run(ctxA) }()
	go lb.Run(ctxB)

	waitFor(t, 5*time.Second, "an initial leader", func() bool {
		return la.IsLeader() || lb.IsLeader()
	})

	// Identify the current leader and the standby. Stop the leader's Run loop so
	// it releases its lease and stops re-campaigning; the standby must take over.
	var standby *gc.Leader
	var stopLeader context.CancelFunc
	var leaderDone chan struct{}
	if la.IsLeader() {
		standby, stopLeader, leaderDone = lb, cancelA, doneA
	} else {
		// b is leader: stop it. (doneA tracks a; for b we just cancel and wait
		// on a short settle since a is the surviving standby here.)
		standby, stopLeader = la, cancelB
	}
	stopLeader()
	if leaderDone != nil {
		<-leaderDone // ensure the lease is released before asserting takeover
	}

	waitFor(t, 5*time.Second, "standby to take over after leader loss", func() bool {
		return standby.IsLeader()
	})
}

// TestLeader_CloseReleasesLease asserts that cancelling a leader's Run context
// releases the lease so a peer can claim it (clean shutdown handover, not a
// TTL-expiry wait).
func TestLeader_CloseReleasesLease(t *testing.T) {
	connect := startJetStreamNATS(t)
	// Long TTL: if release on shutdown did NOT happen, the peer would have to
	// wait out this TTL, far longer than the test's 5s budget — so a takeover
	// within budget proves the explicit release path works.
	cfg := gc.LeaderConfig{Key: "gc", TTL: 30 * time.Second, Renew: time.Second}

	ctxA, cancelA := context.WithCancel(context.Background())
	cfgA := cfg
	cfgA.NodeID = "node-a"
	la, err := gc.NewLeader(ctxA, connect("a"), cfgA)
	if err != nil {
		t.Fatalf("NewLeader a: %v", err)
	}
	go la.Run(ctxA)
	waitFor(t, 5*time.Second, "a to become leader", la.IsLeader)

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	cfgB := cfg
	cfgB.NodeID = "node-b"
	lb, err := gc.NewLeader(ctxB, connect("b"), cfgB)
	if err != nil {
		t.Fatalf("NewLeader b: %v", err)
	}
	go lb.Run(ctxB)

	// b is a follower while a holds the lease.
	if lb.IsLeader() {
		t.Fatalf("b should not be leader while a holds the lease")
	}

	// Shut a down: Run releases the lease on ctx cancel.
	cancelA()

	waitFor(t, 5*time.Second, "b to take over after a's shutdown", lb.IsLeader)
}
