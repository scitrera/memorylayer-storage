// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryRefStore is an in-memory, concurrency-safe RefStore for tests and
// single-process deployments. Production uses the Postgres-backed store.
type MemoryRefStore struct {
	mu   sync.RWMutex
	rows map[string]map[string]ObjectInfo // domain → ref → info
}

// NewMemoryRefStore constructs an empty in-memory ref store.
func NewMemoryRefStore() *MemoryRefStore {
	return &MemoryRefStore{rows: make(map[string]map[string]ObjectInfo)}
}

func (m *MemoryRefStore) Put(_ context.Context, info ObjectInfo) error {
	if info.Ref == "" {
		return ErrInvalidRef
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dom := m.rows[info.Domain]
	if dom == nil {
		dom = make(map[string]ObjectInfo)
		m.rows[info.Domain] = dom
	}
	dom[info.Ref] = info
	return nil
}

func (m *MemoryRefStore) Get(_ context.Context, domain, ref string) (ObjectInfo, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.rows[domain][ref]
	return info, ok, nil
}

func (m *MemoryRefStore) Delete(_ context.Context, domain, ref string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows[domain], ref)
	return nil
}

func (m *MemoryRefStore) List(_ context.Context, domain, prefix string, limit int) ([]ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ObjectInfo
	for ref, info := range m.rows[domain] {
		if strings.HasPrefix(ref, prefix) {
			out = append(out, info)
		}
	}
	// Newest first; ties broken by ref for a stable order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Ref < out[j].Ref
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemoryRefStore) ListPage(_ context.Context, domain, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	afterTime, afterRef, err := DecodeListCursor(after)
	if err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	all := make([]ObjectInfo, 0, len(m.rows[domain]))
	for ref, info := range m.rows[domain] {
		if strings.HasPrefix(ref, prefix) {
			all = append(all, info)
		}
	}
	m.mu.RUnlock()
	// Same total order as List: newest first, ties broken by ref ascending.
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].Ref < all[j].Ref
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})
	// Keyset skip: keep rows strictly after the (created_at DESC, ref ASC)
	// cursor boundary — older created_at, or equal created_at with a larger ref.
	if after != "" {
		filtered := all[:0]
		for _, info := range all {
			ct := info.CreatedAt.UTC()
			if ct.Before(afterTime) || (ct.Equal(afterTime) && info.Ref > afterRef) {
				filtered = append(filtered, info)
			}
		}
		all = filtered
	}
	pageSize := limit
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	next := ""
	if len(all) > pageSize {
		last := all[pageSize-1]
		next = EncodeListCursor(last.CreatedAt, last.Ref)
		all = all[:pageSize]
	}
	return all, next, nil
}

func (m *MemoryRefStore) ListStalePending(_ context.Context, domain string, olderThan time.Time, limit int) ([]ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ObjectInfo
	for _, info := range m.rows[domain] {
		if info.Pending && !info.CreatedAt.After(olderThan) {
			out = append(out, info)
		}
	}
	// Oldest first; ties broken by ref for a stable order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Ref < out[j].Ref
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// MemoryStagingStore is an in-memory StagingStore for tests. PresignPut returns
// a "mem://" URL that no real client can reach; tests upload by calling
// PutStaged directly to simulate the client's PUT to the presigned target.
type MemoryStagingStore struct {
	mu   sync.Mutex
	blob map[string][]byte
	now  func() time.Time
}

// NewMemoryStagingStore constructs an empty in-memory staging store.
func NewMemoryStagingStore() *MemoryStagingStore {
	return &MemoryStagingStore{blob: make(map[string][]byte), now: time.Now}
}

func (s *MemoryStagingStore) PresignPut(_ context.Context, stagingKey string, _ int64, ttl time.Duration) (string, time.Time, error) {
	return "mem://staging/" + stagingKey, s.now().UTC().Add(ttl), nil
}

// PutStaged simulates a client uploading raw bytes to the presigned target.
// Test-only helper; real clients PUT to the S3 presigned URL directly.
func (s *MemoryStagingStore) PutStaged(stagingKey string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blob[stagingKey] = append([]byte(nil), data...)
}

func (s *MemoryStagingStore) Open(_ context.Context, stagingKey string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.blob[stagingKey]
	if !ok {
		return nil, fmt.Errorf("staging key %q not uploaded", stagingKey)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *MemoryStagingStore) Delete(_ context.Context, stagingKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blob, stagingKey)
	return nil
}
