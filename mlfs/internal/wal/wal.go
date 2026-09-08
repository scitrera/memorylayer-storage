// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package wal is mlfs's local data write-ahead log.
//
// Each in-flight write appends a durable (fsync'd) record describing the slice
// it produced — (seq, op, ino, indx, off, slice_id, geometry) — BEFORE the
// metadata commit to PostgreSQL. If the daemon crashes after the data is staged
// (internal/cache) but before the slice_ref row commits, startup Replay re-runs
// the metadata write so no acknowledged write is lost. Replay is idempotent via
// the slice_id natural key (meta.Write uses ON CONFLICT DO NOTHING).
//
// Records are framed [u32 len][u32 crc32(payload)][payload]; a torn tail (short
// read or bad CRC, only possible at the end of the last segment after a crash)
// is truncated on Open. Segments roll at a size threshold; Checkpoint drops
// segments whose records have all been committed.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// OpType identifies the logged operation.
type OpType uint8

const (
	OpWrite    OpType = 1 // a slice written over a chunk
	OpTruncate OpType = 2 // a file truncation (length change)
)

// Record is one logged operation. For OpWrite the slice fields describe the
// data; for OpTruncate, Off carries the new length's low 32 bits and Indx the
// high bits (callers encode as needed).
type Record struct {
	Seq      uint64
	Op       OpType
	Ino      uint64
	Indx     uint32
	Off      uint32
	SliceID  uint64
	Size     uint32
	SliceOff uint32
	SliceLen uint32
}

const recordPayloadLen = 8 + 1 + 8 + 4 + 4 + 8 + 4 + 4 + 4 // 45 bytes

func (r *Record) marshal() []byte {
	b := make([]byte, recordPayloadLen)
	binary.BigEndian.PutUint64(b[0:], r.Seq)
	b[8] = byte(r.Op)
	binary.BigEndian.PutUint64(b[9:], r.Ino)
	binary.BigEndian.PutUint32(b[17:], r.Indx)
	binary.BigEndian.PutUint32(b[21:], r.Off)
	binary.BigEndian.PutUint64(b[25:], r.SliceID)
	binary.BigEndian.PutUint32(b[33:], r.Size)
	binary.BigEndian.PutUint32(b[37:], r.SliceOff)
	binary.BigEndian.PutUint32(b[41:], r.SliceLen)
	return b
}

func unmarshalRecord(b []byte) (Record, bool) {
	if len(b) != recordPayloadLen {
		return Record{}, false
	}
	return Record{
		Seq:      binary.BigEndian.Uint64(b[0:]),
		Op:       OpType(b[8]),
		Ino:      binary.BigEndian.Uint64(b[9:]),
		Indx:     binary.BigEndian.Uint32(b[17:]),
		Off:      binary.BigEndian.Uint32(b[21:]),
		SliceID:  binary.BigEndian.Uint64(b[25:]),
		Size:     binary.BigEndian.Uint32(b[33:]),
		SliceOff: binary.BigEndian.Uint32(b[37:]),
		SliceLen: binary.BigEndian.Uint32(b[41:]),
	}, true
}

// DefaultSegmentBytes is the size at which a segment rolls.
const DefaultSegmentBytes = 64 << 20 // 64 MiB

type segment struct {
	path     string
	startSeq uint64
	maxSeq   uint64
}

// WAL is an append-only, segmented, fsync'd log. Safe for concurrent use.
type WAL struct {
	dir         string
	segmentSize int64

	mu       sync.Mutex
	seq      uint64
	segs     []*segment
	cur      *os.File
	curBytes int64
}

// Open opens (or creates) the WAL in dir, truncating a torn tail left by a
// crash and positioning to append after the highest valid record.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	w := &WAL{dir: dir, segmentSize: DefaultSegmentBytes}
	if err := w.load(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) load() error {
	entries, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, p := range entries {
		var start uint64
		if _, err := fmt.Sscanf(filepath.Base(p), "wal-%d.log", &start); err != nil {
			continue
		}
		w.segs = append(w.segs, &segment{path: p, startSeq: start})
	}
	// Scan every segment to recover seq + per-segment maxSeq; truncate a torn
	// tail on the last segment.
	for i, s := range w.segs {
		valid, maxSeq, err := scanSegment(s.path, i == len(w.segs)-1)
		if err != nil {
			return err
		}
		s.maxSeq = maxSeq
		if maxSeq > w.seq {
			w.seq = maxSeq
		}
		_ = valid
	}
	// Open (or create) the current segment for appending.
	if len(w.segs) == 0 {
		return w.rollLocked(1)
	}
	last := w.segs[len(w.segs)-1]
	f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open current segment: %w", err)
	}
	info, _ := f.Stat()
	w.cur = f
	w.curBytes = info.Size()
	return nil
}

// scanSegment reads records, returning the count and the max seq. When
// truncTorn is set (the last segment), it truncates the file at the end of the
// last fully-valid record (dropping a torn tail from a crash).
func scanSegment(path string, truncTorn bool) (int, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var count int
	var maxSeq uint64
	var validEnd int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break // EOF or short header → end of valid records
		}
		plen := binary.BigEndian.Uint32(hdr[0:])
		crc := binary.BigEndian.Uint32(hdr[4:])
		if plen != recordPayloadLen {
			break // garbage/torn
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn tail
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // torn/corrupt
		}
		rec, ok := unmarshalRecord(payload)
		if !ok {
			break
		}
		count++
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		validEnd += int64(8 + plen)
	}
	if truncTorn {
		if info, err := os.Stat(path); err == nil && info.Size() != validEnd {
			if err := os.Truncate(path, validEnd); err != nil {
				return count, maxSeq, fmt.Errorf("wal: truncate torn tail %s: %w", path, err)
			}
		}
	}
	return count, maxSeq, nil
}

// rollLocked closes the current segment and starts a new one beginning at
// startSeq. Caller holds w.mu.
func (w *WAL) rollLocked(startSeq uint64) error {
	if w.cur != nil {
		_ = w.cur.Sync()
		_ = w.cur.Close()
	}
	path := filepath.Join(w.dir, fmt.Sprintf("wal-%020d.log", startSeq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	if err := fsyncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.cur = f
	w.curBytes = 0
	w.segs = append(w.segs, &segment{path: path, startSeq: startSeq})
	return nil
}

// Append assigns a sequence number, writes the record framed + fsync'd, and
// returns the assigned seq. The record is durable when Append returns.
func (w *WAL) Append(rec Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur != nil && w.curBytes >= w.segmentSize {
		if err := w.rollLocked(w.seq + 1); err != nil {
			return 0, err
		}
	}
	w.seq++
	rec.Seq = w.seq
	payload := rec.marshal()
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if _, err := w.cur.Write(frame); err != nil {
		return 0, fmt.Errorf("wal: append: %w", err)
	}
	if err := w.cur.Sync(); err != nil { // durability before ack
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}
	w.curBytes += int64(len(frame))
	if s := w.segs[len(w.segs)-1]; s != nil {
		s.maxSeq = w.seq
	}
	return w.seq, nil
}

// Replay invokes fn for every valid record across all segments, in seq order.
// The handler is responsible for idempotency (mlfs's meta.Write is idempotent
// on slice_id).
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	segs := append([]*segment(nil), w.segs...)
	w.mu.Unlock()
	for _, s := range segs {
		if err := replaySegment(s.path, fn); err != nil {
			return err
		}
	}
	return nil
}

func replaySegment(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil // end of valid records
		}
		plen := binary.BigEndian.Uint32(hdr[0:])
		crc := binary.BigEndian.Uint32(hdr[4:])
		if plen != recordPayloadLen {
			return nil
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return nil
		}
		rec, ok := unmarshalRecord(payload)
		if !ok {
			return nil
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// Checkpoint drops fully-committed segments: any non-current segment whose
// highest seq is <= upToSeq is deleted (its records have all landed in SQL).
func (w *WAL) Checkpoint(upToSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keep := w.segs[:0:0]
	for i, s := range w.segs {
		isCurrent := i == len(w.segs)-1
		if !isCurrent && s.maxSeq != 0 && s.maxSeq <= upToSeq {
			if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal: checkpoint remove %s: %w", s.path, err)
			}
			continue
		}
		keep = append(keep, s)
	}
	w.segs = keep
	return nil
}

// LastSeq returns the highest assigned sequence number.
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// Close fsyncs and closes the current segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return nil
	}
	if err := w.cur.Sync(); err != nil {
		return err
	}
	err := w.cur.Close()
	w.cur = nil
	return err
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
