// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// maxPutPartRetries is the number of optimistic-concurrency retry attempts
// putPart will make before giving up on a revision conflict. Each attempt
// re-fetches the current entry and retries the update.
//
// With N concurrent goroutines each writing a distinct part to the same
// upload, a goroutine may lose a CAS up to N-1 times before every other
// writer has committed. We set this to 32 to comfortably cover realistic
// parallelism (S3 allows up to 10,000 parts but concurrent putPart calls
// in practice are bounded by client concurrency, typically 8–16).
const maxPutPartRetries = 32

// putPartOnRetry is an optional hook called each time putPart enters the
// CAS-conflict retry path. It is nil in production; tests set it to an atomic
// counter to prove the retry branch actually fires under concurrency.
var putPartOnRetry func()

// DefaultMPUKVBucket is the JetStream KV bucket name the durable multipart-upload
// store uses when the caller does not override it.
const DefaultMPUKVBucket = "blobgw_s3_mpu"

// mpuRecord is the JSON-serializable form of an in-flight multipart upload. The
// in-memory mpuUpload keeps parts in an unexported map; mpuRecord flattens that
// to an exported slice so the whole upload (target identity, presentation
// metadata, and every part's bookkeeping) round-trips through KV. The object
// BYTES are already durable in casstore — this persists only the MPU
// bookkeeping so an in-flight upload survives a restart and can be
// resumed/completed.
type mpuRecord struct {
	UploadID    string            `json:"upload_id"`
	Bucket      string            `json:"bucket"`
	Domain      string            `json:"domain"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type"`
	UserMeta    map[string]string `json:"user_meta,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	Parts       []uploadedPart    `json:"parts,omitempty"`
}

func toMPURecord(u mpuUpload) mpuRecord {
	rec := mpuRecord{
		UploadID:    u.UploadID,
		Bucket:      u.Bucket,
		Domain:      u.Domain,
		Key:         u.Key,
		ContentType: u.ContentType,
		UserMeta:    u.UserMeta,
		CreatedAt:   u.CreatedAt,
	}
	rec.Parts = make([]uploadedPart, 0, len(u.parts))
	for _, p := range u.parts {
		rec.Parts = append(rec.Parts, p)
	}
	return rec
}

func (rec mpuRecord) toUpload() mpuUpload {
	u := mpuUpload{
		UploadID:    rec.UploadID,
		Bucket:      rec.Bucket,
		Domain:      rec.Domain,
		Key:         rec.Key,
		ContentType: rec.ContentType,
		UserMeta:    rec.UserMeta,
		CreatedAt:   rec.CreatedAt,
		parts:       make(map[int]uploadedPart, len(rec.Parts)),
	}
	for _, p := range rec.Parts {
		u.parts[p.PartNumber] = p
	}
	return u
}

// jetStreamMPUStore is an mpuStore backed by a JetStream KV bucket. It closes
// ADR-001 #37 for the multipart-upload registry: in-flight uploads (keyed by
// upload id, with their parts metadata) survive a blobgw-s3 restart because they
// live in NATS JetStream rather than process memory. A fresh store instance over
// the same KV bucket sees prior in-flight uploads and can resume/complete them.
//
// The interface methods carry no context (they sit on the HTTP request path),
// so KV calls use a background context; transport errors are logged and surfaced
// as a miss / no-op, matching the in-memory store's behavior.
type jetStreamMPUStore struct {
	kv jetstream.KeyValue
}

// NewJetStreamMPUStore opens (creating if absent) a JetStream KV bucket and
// returns a durable mpuStore over it. Pass DefaultMPUKVBucket for kvBucket to
// use the conventional name.
func NewJetStreamMPUStore(ctx context.Context, js jetstream.JetStream, kvBucket string) (mpuStore, error) {
	if kvBucket == "" {
		kvBucket = DefaultMPUKVBucket
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      kvBucket,
		Description: "blobgw-s3 in-flight multipart-upload registry (ADR-001 #37)",
		Storage:     jetstream.FileStorage,
		History:     1,
	})
	if err != nil {
		return nil, err
	}
	return &jetStreamMPUStore{kv: kv}, nil
}

func (s *jetStreamMPUStore) create(u mpuUpload) {
	if u.parts == nil {
		u.parts = make(map[int]uploadedPart)
	}
	s.put(u)
}

func (s *jetStreamMPUStore) get(uploadID string) (mpuUpload, bool) {
	if uploadID == "" {
		return mpuUpload{}, false
	}
	entry, err := s.kv.Get(context.Background(), encodeKVKey(uploadID))
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Error("blobgw-s3: get mpu upload", "upload_id", uploadID, "err", err)
		}
		return mpuUpload{}, false
	}
	var rec mpuRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		slog.Error("blobgw-s3: unmarshal mpu upload", "upload_id", uploadID, "err", err)
		return mpuUpload{}, false
	}
	return rec.toUpload(), true
}

func (s *jetStreamMPUStore) putPart(uploadID string, part uploadedPart) bool {
	if uploadID == "" {
		return false
	}
	ctx := context.Background()
	key := encodeKVKey(uploadID)
	for attempt := range maxPutPartRetries {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				slog.Error("blobgw-s3: get mpu upload for putPart", "upload_id", uploadID, "attempt", attempt, "err", err)
			}
			return false
		}
		var rec mpuRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			slog.Error("blobgw-s3: unmarshal mpu upload for putPart", "upload_id", uploadID, "err", err)
			return false
		}
		u := rec.toUpload()
		u.parts[part.PartNumber] = part
		val, err := json.Marshal(toMPURecord(u))
		if err != nil {
			slog.Error("blobgw-s3: marshal mpu upload for putPart", "upload_id", uploadID, "err", err)
			return false
		}
		if _, err := s.kv.Update(ctx, key, val, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				// Revision conflict: another concurrent putPart won the race.
				// Re-fetch and retry so we merge both parts.
				if putPartOnRetry != nil {
					putPartOnRetry()
				}
				continue
			}
			slog.Error("blobgw-s3: update mpu upload for putPart", "upload_id", uploadID, "attempt", attempt, "err", err)
			return false
		}
		return true
	}
	slog.Error("blobgw-s3: putPart exceeded max retries", "upload_id", uploadID, "max_retries", maxPutPartRetries)
	return false
}

func (s *jetStreamMPUStore) del(uploadID string) {
	if uploadID == "" {
		return
	}
	if err := s.kv.Delete(context.Background(), encodeKVKey(uploadID)); err != nil &&
		!errors.Is(err, jetstream.ErrKeyNotFound) {
		slog.Error("blobgw-s3: delete mpu upload", "upload_id", uploadID, "err", err)
	}
}

// put serializes and upserts an upload. It returns false (logging) on a
// transport/encoding failure so putPart can report the failure to the caller.
func (s *jetStreamMPUStore) put(u mpuUpload) bool {
	val, err := json.Marshal(toMPURecord(u))
	if err != nil {
		slog.Error("blobgw-s3: marshal mpu upload", "upload_id", u.UploadID, "err", err)
		return false
	}
	if _, err := s.kv.Put(context.Background(), encodeKVKey(u.UploadID), val); err != nil {
		slog.Error("blobgw-s3: put mpu upload", "upload_id", u.UploadID, "err", err)
		return false
	}
	return true
}
