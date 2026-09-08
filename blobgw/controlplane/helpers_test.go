// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane_test

import (
	"context"
	"errors"
	"sync"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// errStore is a snapshot.DedupStore whose mutating ops always fail, so handler
// error-propagation can be exercised. Reads return empty.
type errStore struct{}

var _ snapshot.DedupStore = errStore{}

func (errStore) Lookup(context.Context, string, string) (snapshot.PackRef, bool, error) {
	return snapshot.PackRef{}, false, nil
}

func (errStore) LookupBatch(context.Context, string, []string) (map[string]snapshot.PackRef, error) {
	return nil, errors.New("boom: lookup failed")
}

func (errStore) Record(context.Context, string, []snapshot.ChunkLocation) error {
	return errors.New("boom: record failed")
}

func (errStore) PurgePacks(context.Context, string, []string) error {
	return errors.New("boom: purge failed")
}

// countingStore wraps a MemoryDedupStore and counts Record calls, so the
// queue-group test can assert a request was delivered to exactly one server.
type countingStore struct {
	*snapshot.MemoryDedupStore
	mu          sync.Mutex
	recordCalls int
}

var _ snapshot.DedupStore = (*countingStore)(nil)

func (c *countingStore) Record(ctx context.Context, domain string, locs []snapshot.ChunkLocation) error {
	c.mu.Lock()
	c.recordCalls++
	c.mu.Unlock()
	return c.MemoryDedupStore.Record(ctx, domain, locs)
}

func (c *countingStore) records() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recordCalls
}
