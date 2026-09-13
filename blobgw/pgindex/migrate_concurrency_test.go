// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package pgindex_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/scitrera/memorylayer-storage/blobgw/pgindex"
)

func isolatedMigrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("BLOBGW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BLOBGW_TEST_DATABASE_URL to run migration integration tests")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "migration_" + randHex(t, 8)
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE")
		_ = admin.Close()
	})
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(16)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrateConcurrentFreshSchemaAndExistingRows(t *testing.T) {
	db := isolatedMigrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	migrateTogether := func() {
		start := make(chan struct{})
		errors := make(chan error, 12)
		var workers sync.WaitGroup
		for i := 0; i < 12; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				errors <- pgindex.New(db).Migrate(ctx)
			}()
		}
		close(start)
		workers.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatalf("concurrent migration: %v", err)
			}
		}
	}
	migrateTogether()
	if _, err := db.ExecContext(ctx, "INSERT INTO blob_ref(domain, ref, content_hash) VALUES('fixture', 'kept', 'original')"); err != nil {
		t.Fatal(err)
	}
	migrateTogether()
	var hash string
	if err := db.QueryRowContext(ctx, "SELECT content_hash FROM blob_ref WHERE domain='fixture' AND ref='kept'").Scan(&hash); err != nil || hash != "original" {
		t.Fatalf("migration changed existing state: hash=%q error=%v", hash, err)
	}
}

func TestMigrateLockCancellationAndRetry(t *testing.T) {
	db := isolatedMigrationDB(t)
	holder, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock(hashtext('blobgw.pgindex.migrate'), hashtext(current_schema()))"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := pgindex.New(db).Migrate(ctx); err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("migration did not honor the lock/cancellation: %v", err)
	}
	var tables int
	if err := holder.QueryRow("SELECT count(*) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind='r'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("blocked migration wrote schema: tables=%d error=%v", tables, err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := pgindex.New(db).Migrate(context.Background()); err != nil {
		t.Fatalf("retry after canceled lock failed: %v", err)
	}
}
