// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package testpg

import (
	"strings"
	"testing"
)

// TestIsolation proves DB() actually isolates: a table created on the returned
// connection lands in a private mlfs_test_* schema, never in public.
func TestIsolation(t *testing.T) {
	db := DB(t)
	if _, err := db.Exec("CREATE TABLE probe (x int)"); err != nil {
		t.Fatalf("create probe: %v", err)
	}
	var schema string
	if err := db.QueryRow(
		"SELECT table_schema FROM information_schema.tables WHERE table_name='probe'",
	).Scan(&schema); err != nil {
		t.Fatalf("locate probe: %v", err)
	}
	if schema == "public" {
		t.Fatalf("probe landed in public — search_path isolation is NOT in effect")
	}
	if !strings.HasPrefix(schema, "mlfs_test_") {
		t.Fatalf("probe landed in unexpected schema %q", schema)
	}
}

// TestIsolationDistinct proves two DB() calls get distinct schemas, so tests
// don't see each other's rows.
func TestIsolationDistinct(t *testing.T) {
	a, b := DB(t), DB(t)
	var sa, sb string
	if err := a.QueryRow("SELECT current_schema()").Scan(&sa); err != nil {
		t.Fatalf("schema a: %v", err)
	}
	if err := b.QueryRow("SELECT current_schema()").Scan(&sb); err != nil {
		t.Fatalf("schema b: %v", err)
	}
	if sa == sb {
		t.Fatalf("two DB() calls shared schema %q — not isolated", sa)
	}
}
