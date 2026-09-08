// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package testpg provides Postgres-backed test isolation for mlfs. Each call to
// DB creates a fresh, uniquely-named schema (pinned via the connection's
// search_path) and drops it on t.Cleanup, so gated integration tests no longer
// share one database behind a TRUNCATE. The upshot: `go test ./...` is
// parallel-safe and the old `-p 1` requirement is gone.
package testpg

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// EnvVar names the pgx DSN that enables the gated tests.
const EnvVar = "MLFS_TEST_DATABASE_URL"

var schemaSeq atomic.Uint64

// DB returns a *sql.DB whose connections are scoped to a fresh, private schema.
// It skips the test when EnvVar is unset. The schema and everything in it is
// dropped on t.Cleanup, giving each caller an isolated namespace with no shared
// TRUNCATE — so packages (and tests) can run concurrently.
func DB(t testing.TB) *sql.DB {
	t.Helper()
	dsn := os.Getenv(EnvVar)
	if dsn == "" {
		t.Skip("set " + EnvVar + " (a pgx DSN) to run gated Postgres tests")
	}
	// A per-process, per-call schema name: the pid keeps concurrent test
	// binaries (one per package) apart; the counter keeps tests within a
	// binary apart.
	schema := fmt.Sprintf("mlfs_test_%d_%d", os.Getpid(), schemaSeq.Add(1))

	ctx := context.Background()
	admin, err := sql.Open("pgx", dsn) // base DSN: used only to create/drop the schema
	if err != nil {
		t.Fatalf("testpg: open admin: %v", err)
	}
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("testpg: cannot reach %s (%v)", EnvVar, err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		_ = admin.Close()
		t.Fatalf("testpg: create schema %q: %v", schema, err)
	}

	scoped, err := withSearchPath(dsn, schema)
	if err != nil {
		_ = admin.Close()
		t.Fatalf("testpg: %v", err)
	}
	db, err := sql.Open("pgx", scoped)
	if err != nil {
		_ = admin.Close()
		t.Fatalf("testpg: open scoped: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		_ = admin.Close()
	})
	return db
}

// withSearchPath returns dsn with the connection search_path pinned to ONLY the
// private schema. pgx forwards the unrecognized search_path key as a startup
// RuntimeParam, so it applies to every pooled connection. We deliberately do NOT
// append "public": if a stale public.node existed, `CREATE TABLE IF NOT EXISTS`
// would see it through the path and skip creating the private copy, silently
// de-isolating onto public. Built-in types live in pg_catalog (always implicitly
// in the path), so the schema alone is sufficient. Supports URL and keyword DSNs.
func withSearchPath(dsn, schema string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse dsn: %w", err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return dsn + " search_path=" + schema, nil
}
