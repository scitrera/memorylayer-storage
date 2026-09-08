// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"sort"
	"sync"
	"time"
)

// mpuPartPrefix is the reserved ref namespace each in-flight multipart part is
// stored under: "\x00mpu\x00<uploadId>\x00<partNumber>". The leading NUL byte
// keeps these temp refs out of any client's key space — S3 object keys are
// valid UTF-8 and SDKs never emit a NUL-prefixed key — so ListObjectsV2 (and the
// DeleteBucket emptiness probe) can filter the whole namespace with one prefix
// check. Parts are stored as ordinary gateway objects so casstore chunks+dedups
// them; Complete/Abort delete them via Gateway.Delete.
const mpuPartPrefix = "\x00mpu\x00"

// uploadedPart records one stored part: its 1-based number, the raw MD5 of the
// part bytes (used to assemble the S3 multipart ETag), and the gateway ref the
// bytes live under. Size is kept for ListParts.
type uploadedPart struct {
	PartNumber int
	MD5        [16]byte // raw MD5 of the part content
	ETag       string   // hex MD5, the value returned to the client (no quotes)
	Ref        string   // gateway ref the part bytes are stored under
	Size       int64
}

// mpuUpload is the state of one in-flight multipart upload: the target object's
// identity + presentation metadata (captured at CreateMultipartUpload) and the
// set of parts uploaded so far, keyed by part number.
type mpuUpload struct {
	UploadID    string
	Bucket      string
	Domain      string
	Key         string
	ContentType string
	UserMeta    map[string]string
	CreatedAt   time.Time
	parts       map[int]uploadedPart
}

// mpuStore is the uploadId → in-flight upload index backing the multipart
// operations. It is split behind an interface so a later stage can persist it
// (Postgres) alongside the ref/meta/bucket stores; the in-memory implementation
// serves dev/tests/single-node. Implementations must be safe for concurrent use.
type mpuStore interface {
	// create records a new upload and returns it. The caller supplies the
	// already-allocated UploadID.
	create(u mpuUpload)
	// get returns the upload for uploadID, ok=false if unknown.
	get(uploadID string) (mpuUpload, bool)
	// putPart records (or replaces) one part of an upload. It reports ok=false
	// if the upload is unknown.
	putPart(uploadID string, part uploadedPart) (ok bool)
	// del discards an upload's state. Removing an absent upload is a no-op.
	del(uploadID string)
}

// memoryMPUStore is an in-process mpuStore for dev/tests/single-node, matching
// the in-memory ref/meta/bucket stores the rest of the stack uses.
type memoryMPUStore struct {
	mu sync.RWMutex
	m  map[string]*mpuUpload
}

func newMemoryMPUStore() *memoryMPUStore {
	return &memoryMPUStore{m: make(map[string]*mpuUpload)}
}

func (s *memoryMPUStore) create(u mpuUpload) {
	if u.parts == nil {
		u.parts = make(map[int]uploadedPart)
	}
	cp := u
	s.mu.Lock()
	s.m[u.UploadID] = &cp
	s.mu.Unlock()
}

func (s *memoryMPUStore) get(uploadID string) (mpuUpload, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.m[uploadID]
	if !ok {
		return mpuUpload{}, false
	}
	return s.snapshot(u), true
}

func (s *memoryMPUStore) putPart(uploadID string, part uploadedPart) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.m[uploadID]
	if !ok {
		return false
	}
	u.parts[part.PartNumber] = part
	return true
}

func (s *memoryMPUStore) del(uploadID string) {
	s.mu.Lock()
	delete(s.m, uploadID)
	s.mu.Unlock()
}

// snapshot returns a deep-ish copy of u so callers never see concurrent part
// mutations through the returned map. Caller holds at least RLock.
func (s *memoryMPUStore) snapshot(u *mpuUpload) mpuUpload {
	out := *u
	out.parts = make(map[int]uploadedPart, len(u.parts))
	for k, v := range u.parts {
		out.parts[k] = v
	}
	return out
}

// sortedParts returns an upload's parts in ascending part-number order.
func (u mpuUpload) sortedParts() []uploadedPart {
	out := make([]uploadedPart, 0, len(u.parts))
	for _, p := range u.parts {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	return out
}
