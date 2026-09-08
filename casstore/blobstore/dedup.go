// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package blobstore

import (
	"context"
	"sync"
)

// InflightDeduper coalesces concurrent PutIfAbsent calls for the SAME blob ID
// within a single process into one underlying upload. Content-addressed Puts
// (blob ID = hash of content) are idempotent, so when several goroutines race
// to store identical bytes the result is the same either way — but without
// coalescing each racer pays a full upload (the "loser wastes an upload"
// window: see PutIfAbsent's fallback HEAD-then-PUT and S3's upload-then-reject
// conditional write). The deduper lets the first caller do the work while the
// others wait and share its result, so N concurrent puts of one ID cost one
// upload instead of N.
//
// Scope is in-process: it does not (and need not) coordinate across processes,
// since cross-process races are still content-addressed-safe — distinct
// processes writing identical content produce identical blobs.
type InflightDeduper struct {
	st    Storage
	mu    sync.Mutex
	calls map[ID]*putCall
}

type putCall struct {
	done chan struct{}
	err  error
}

// NewInflightDeduper wraps st with per-blob-ID PutIfAbsent coalescing.
func NewInflightDeduper(st Storage) *InflightDeduper {
	return &InflightDeduper{st: st, calls: make(map[ID]*putCall)}
}

// Storage returns the wrapped backend (for reads / other ops that don't need
// coalescing).
func (d *InflightDeduper) Storage() Storage { return d.st }

// PutIfAbsent stores data at id if absent, coalescing concurrent calls for the
// same id: only the first caller performs the underlying PutIfAbsent; callers
// that arrive while it is in flight wait and return its result without issuing
// a second upload. Followers therefore share the leader's error — for the
// content-addressed Put path this is acceptable (the whole Put is retried on
// failure), and it never produces a corrupt or partial blob.
func (d *InflightDeduper) PutIfAbsent(ctx context.Context, id ID, data Bytes) error {
	d.mu.Lock()
	if c, ok := d.calls[id]; ok {
		d.mu.Unlock()
		select {
		case <-c.done:
			return c.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c := &putCall{done: make(chan struct{})}
	d.calls[id] = c
	d.mu.Unlock()

	c.err = PutIfAbsent(ctx, d.st, id, data)

	d.mu.Lock()
	delete(d.calls, id)
	d.mu.Unlock()
	close(c.done)
	return c.err
}
