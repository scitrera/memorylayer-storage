// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc_test

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startJetStreamNATS boots an in-process NATS server WITH JetStream (required
// for the KV-backed leader lease), connected over the in-process pipe (no TCP
// listener), and returns a connection factory so multiple "replicas" can attach
// to the SAME server. It mirrors the embedded pattern in
// mlfs/internal/coord/nats.go (StartEmbedded standalone mode) but stays local to
// the gc tests. The server + every connection are torn down on test cleanup.
func startJetStreamNATS(t *testing.T) (connect func(name string) *nats.Conn) {
	t.Helper()
	opts := &natsserver.Options{
		ServerName:         "gc-test",
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 << 20,
		JetStreamMaxStore:  1 << 30,
		DontListen:         true, // in-process only, zero network surface
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats not ready")
	}
	t.Cleanup(srv.Shutdown)

	return func(name string) *nats.Conn {
		conn, err := nats.Connect("", nats.InProcessServer(srv), nats.Name(name))
		if err != nil {
			t.Fatalf("connect nats (%s): %v", name, err)
		}
		t.Cleanup(conn.Close)
		return conn
	}
}

// waitFor polls cond until it returns true or the deadline elapses, failing the
// test with msg on timeout. Used to await eventual-consistency outcomes (a peer
// claiming a freed lease, the leader settling) without a brittle fixed sleep.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
}
