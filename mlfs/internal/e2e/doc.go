// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package e2e holds the hermetic cross-module integration test that proves the
// converged storage stack works end to end: the REAL blobgw control-plane
// server (controlplane.Server over embedded NATS, with a tenantbind credential
// provider and an in-memory dedup index) wired to the REAL mlfs node data path
// (chunkstore.NewRemote → remoteindex.NatsRequester / s3http / casstore), with
// an httptest server standing in for S3. No external infrastructure is required
// (no real S3, no PostgreSQL, no out-of-process NATS).
//
// # Why this edge is test-only
//
// This is the ONLY place mlfs imports blobgw, and it exists SOLELY for this
// integration test (the file is build-tagged into the test binary only — it is
// a _test.go file in a test-only package). At runtime mlfs and blobgw remain
// fully decoupled: they communicate exclusively via the ctlproto wire contract
// over NATS, never by Go import. blobgw does not import mlfs, so adding the
// mlfs → blobgw test edge keeps the module graph acyclic.
package e2e
