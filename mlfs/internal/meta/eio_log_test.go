// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"syscall"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// captureHandler is a minimal slog.Handler that records each record's message
// and string attributes, so a test can assert what the engine logged without a
// live database.
type captureHandler struct {
	records []capturedRecord
}

type capturedRecord struct {
	msg   string
	attrs map[string]string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := capturedRecord{msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	h.records = append(h.records, rec)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// TestEIOLogsAndReturnsEIO forces a metadata read against a closed database
// handle (a deterministic, network-free non-ErrNoRows failure) and verifies the
// engine (1) still returns syscall.EIO to its caller and (2) logged the
// underlying cause through its slog.Logger with the operation label — the
// observability breadcrumb that bare EIO returns previously discarded.
func TestEIOLogsAndReturnsEIO(t *testing.T) {
	// A pgx handle that we immediately close: any query then fails with a real
	// driver error (sql.ErrConnDone), exercising the err != nil → eio path
	// without needing a reachable server.
	db, err := sql.Open("pgx", "postgres://invalid:invalid@127.0.0.1:1/none?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	cap := &captureHandler{}
	e := Open(db, 1, WithLogger(slog.New(cap)))

	_, st := e.GetAttr(context.Background(), RootInode)
	if st != syscall.EIO {
		t.Fatalf("GetAttr on closed db: got errno %v, want EIO", st)
	}

	if len(cap.records) == 0 {
		t.Fatal("expected the forced DB error to be logged, got no records")
	}
	rec := cap.records[len(cap.records)-1]
	if rec.attrs["op"] != "getAttr" {
		t.Errorf("logged op = %q, want %q", rec.attrs["op"], "getAttr")
	}
	if rec.attrs["err"] == "" {
		t.Errorf("expected a non-empty err attr, got none (attrs=%v)", rec.attrs)
	}
	// The breadcrumb must not leak row contents — only the op label + driver
	// error. A closed-handle error is purely connection state.
	if strings.Contains(rec.attrs["err"], "RootInode") {
		t.Errorf("err attr unexpectedly contains row data: %q", rec.attrs["err"])
	}
}

// TestEIONilErrLogsNothing verifies the helper returns EIO but logs nothing when
// handed a nil error (the logic-guard case), so guards that have no underlying
// cause do not emit empty breadcrumbs.
func TestEIONilErrLogsNothing(t *testing.T) {
	cap := &captureHandler{}
	e := &Engine{log: slog.New(cap)}
	if st := e.eio(context.Background(), "guard", nil); st != syscall.EIO {
		t.Fatalf("eio(nil) = %v, want EIO", st)
	}
	if len(cap.records) != 0 {
		t.Errorf("eio(nil) logged %d records, want 0", len(cap.records))
	}
}

// TestOpenDefaultsLogger verifies an engine constructed without WithLogger has a
// non-nil logger (defaulting to slog.Default()), so the eio helper never
// nil-panics for existing callers.
func TestOpenDefaultsLogger(t *testing.T) {
	e := Open(nil, 1)
	if e.log == nil {
		t.Fatal("Open without WithLogger left a nil logger")
	}
}
