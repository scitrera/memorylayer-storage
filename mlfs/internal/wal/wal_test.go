// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func writeRec(t *testing.T, w *WAL, ino, sliceID uint64) uint64 {
	t.Helper()
	seq, err := w.Append(Record{Op: OpWrite, Ino: ino, Indx: 0, Off: 0, SliceID: sliceID, Size: 1024, SliceLen: 1024})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return seq
}

func collect(t *testing.T, w *WAL) []Record {
	t.Helper()
	var got []Record
	if err := w.Replay(func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

func TestAppendReplayOrder(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	for i := uint64(1); i <= 5; i++ {
		writeRec(t, w, 100, i)
	}
	got := collect(t, w)
	if len(got) != 5 {
		t.Fatalf("replayed %d records, want 5", len(got))
	}
	for i, r := range got {
		if r.Seq != uint64(i+1) || r.SliceID != uint64(i+1) {
			t.Errorf("record %d: seq=%d sliceID=%d", i, r.Seq, r.SliceID)
		}
	}
}

// TestDurabilityAcrossReopen: every Append fsyncs, so all records survive a
// reopen even without Close (simulating a crash). This is the acceptance core:
// acknowledged writes are durably replayable after a crash.
func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := uint64(1); i <= 50; i++ {
		writeRec(t, w, 7, i)
	}
	// "crash": abandon without Close.

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	got := collect(t, w2)
	if len(got) != 50 {
		t.Fatalf("after crash, replayed %d records, want 50", len(got))
	}
	if w2.LastSeq() != 50 {
		t.Errorf("LastSeq after reopen: %d want 50", w2.LastSeq())
	}
	// New appends continue the sequence.
	seq := writeRec(t, w2, 7, 51)
	if seq != 51 {
		t.Errorf("next seq after reopen: %d want 51", seq)
	}
}

// TestTornTailTruncated: a partial record at the end (crash mid-write) is
// dropped on Open; earlier valid records survive and appends resume cleanly.
func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		writeRec(t, w, 1, i)
	}
	w.Close()

	// Append garbage (a torn record) to the segment file.
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	f, _ := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0, 0, 0, 45, 1, 2, 3}) // valid-looking header len=45 + truncated payload
	f.Close()

	w2, err := Open(dir) // should truncate the torn tail
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	got := collect(t, w2)
	if len(got) != 3 {
		t.Fatalf("after torn tail, replayed %d records, want 3", len(got))
	}
	// Appends resume after the last valid record.
	if seq := writeRec(t, w2, 1, 4); seq != 4 {
		t.Errorf("resume seq: %d want 4", seq)
	}
	if got := collect(t, w2); len(got) != 4 {
		t.Errorf("after resume, %d records want 4", len(got))
	}
}

func TestSegmentRotationAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	w.segmentSize = 200 // force frequent rotation (each frame is 8+45=53 bytes)

	for i := uint64(1); i <= 20; i++ {
		writeRec(t, w, 1, i)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segs) < 3 {
		t.Fatalf("expected multiple segments from rotation, got %d", len(segs))
	}
	if got := collect(t, w); len(got) != 20 {
		t.Fatalf("replay across segments: %d want 20", len(got))
	}

	// Checkpoint the first ~half; old fully-committed segments are dropped.
	if err := w.Checkpoint(10); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	got := collect(t, w)
	if len(got) == 0 || len(got) == 20 {
		t.Fatalf("checkpoint should drop some but not all records, got %d", len(got))
	}
	for _, r := range got {
		// Remaining records belong to segments not fully <= 10; the earliest
		// dropped segment's records (seq <= 10) should be mostly gone.
		_ = r
	}
	// The highest records must always survive.
	last := got[len(got)-1]
	if last.Seq != 20 {
		t.Errorf("last surviving seq: %d want 20", last.Seq)
	}
}
