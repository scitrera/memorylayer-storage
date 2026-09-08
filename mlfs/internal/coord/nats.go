// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package coord is mlfs's multi-node coordination plane (L2.8). It embeds (or
// connects to) NATS JetStream and uses it as the LOW-LATENCY accelerator for
// ownership coordination: lease liveness lives in a KV bucket with native TTL +
// Watch, so refresh and ownership-discovery never hit PostgreSQL on the hot
// path. PostgreSQL remains the system of record and the authoritative fence —
// the generation a write is fenced on always comes from the SQL ownership row
// (see internal/meta/fence.go). NATS only decides who is alive and notifies
// peers fast; if NATS is wiped or partitioned the worst case is a slower handoff
// or a blocked write, never a double-owner. See docs/L2.8_IMPLEMENTATION.md.
package coord

import (
	"fmt"
	"net/url"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATS holds a JetStream handle plus, when embedded, the in-process server that
// backs it. Close tears down the connection and (if owned) the server.
type NATS struct {
	server *natsserver.Server // nil when connected to an external cluster
	conn   *nats.Conn
	js     jetstream.JetStream
}

// EmbeddedConfig configures an in-process NATS+JetStream node.
type EmbeddedConfig struct {
	NodeName string // server + cluster identity
	StoreDir string // JetStream file storage directory

	// Cluster wiring. Leave ClusterName empty for a standalone single node
	// (in-process, no network listener — ideal for tests and single-mount dev).
	// Set ClusterName + ListenHost/ListenPort + Routes to form a multi-node
	// JetStream cluster across mlfs daemons.
	ClusterName string
	ListenHost  string
	ListenPort  int      // client port; 0 picks a free port
	ClusterPort int      // route port
	Routes      []string // nats-route URLs of peer nodes

	ReadyTimeout time.Duration // default 10s
}

// StartEmbedded boots an in-process NATS server with JetStream and returns a
// handle over an in-process (or local) connection. Standalone mode (no
// ClusterName) uses DontListen + an in-process pipe — zero network surface.
func StartEmbedded(cfg EmbeddedConfig) (*NATS, error) {
	if cfg.ReadyTimeout <= 0 {
		cfg.ReadyTimeout = 10 * time.Second
	}
	opts := &natsserver.Options{
		ServerName: cfg.NodeName,
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
		// Keep JetStream bounded; mlfs coordination state is tiny.
		JetStreamMaxMemory: 64 << 20,
		JetStreamMaxStore:  1 << 30,
	}
	if cfg.ClusterName == "" {
		// Standalone, in-process only: no TCP listener.
		opts.DontListen = true
	} else {
		host := cfg.ListenHost
		if host == "" {
			host = "0.0.0.0"
		}
		opts.Host = host
		opts.Port = cfg.ListenPort
		opts.Cluster = natsserver.ClusterOpts{
			Name: cfg.ClusterName,
			Host: host,
			Port: cfg.ClusterPort,
		}
		for _, r := range cfg.Routes {
			u, err := url.Parse(r)
			if err != nil {
				return nil, fmt.Errorf("coord: bad route %q: %w", r, err)
			}
			opts.Routes = append(opts.Routes, u)
		}
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("coord: new nats server: %w", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(cfg.ReadyTimeout) {
		srv.Shutdown()
		return nil, fmt.Errorf("coord: nats server not ready within %s", cfg.ReadyTimeout)
	}

	var connOpts []nats.Option
	url := srv.ClientURL()
	if cfg.ClusterName == "" {
		connOpts = append(connOpts, nats.InProcessServer(srv))
		url = "" // ignored with InProcessServer
	}
	conn, err := nats.Connect(url, connOpts...)
	if err != nil {
		srv.Shutdown()
		return nil, fmt.Errorf("coord: connect embedded nats: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		return nil, fmt.Errorf("coord: jetstream: %w", err)
	}
	return &NATS{server: srv, conn: conn, js: js}, nil
}

// Connect attaches to an EXTERNAL NATS/JetStream cluster (no embedded server).
func Connect(url string, opts ...nats.Option) (*NATS, error) {
	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("coord: connect nats %q: %w", url, err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("coord: jetstream: %w", err)
	}
	return &NATS{conn: conn, js: js}, nil
}

// JS returns the JetStream context.
func (n *NATS) JS() jetstream.JetStream { return n.js }

// Conn returns the underlying NATS connection.
func (n *NATS) Conn() *nats.Conn { return n.conn }

// Close drains the connection and shuts down the embedded server if owned.
func (n *NATS) Close() {
	if n.conn != nil {
		_ = n.conn.Drain()
		n.conn.Close()
	}
	if n.server != nil {
		n.server.Shutdown()
		n.server.WaitForShutdown()
	}
}
