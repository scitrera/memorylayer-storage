// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultCredentialKVBucket is the JetStream KV bucket name the durable
// credential store uses when the caller does not override it.
const DefaultCredentialKVBucket = "blobgw_s3_credentials"

// jetStreamCredentialStore is a CredentialStore backed by a JetStream KV
// bucket. It closes ADR-001 #37 for the credential registry: access-key →
// {secret, domain} records survive a blobgw-s3 restart because they live in
// NATS JetStream rather than process memory. Each credential is one KV entry
// keyed by the (encoded) access key id with a JSON-serialized Credential value.
type jetStreamCredentialStore struct {
	kv jetstream.KeyValue
}

// DurableCredentialStore is a CredentialStore that can also be seeded with new
// credentials at runtime. The JetStream-backed implementation returns this
// interface so callers can upsert credentials (e.g. on daemon startup from a
// credentials file) without depending on the concrete type.
type DurableCredentialStore interface {
	CredentialStore
	// Put upserts a credential keyed by its access key id. It is the durable
	// equivalent of seeding NewMemoryCredentialStore.
	Put(ctx context.Context, cred Credential) error
}

// NewJetStreamCredentialStore opens (creating if absent) a JetStream KV bucket
// and returns a durable DurableCredentialStore over it. Pass
// DefaultCredentialKVBucket for kvBucket to use the conventional name. Seed it
// with Put — typically the daemon loads the credentials JSON file once at
// startup and upserts each entry, which is then durable across restarts.
func NewJetStreamCredentialStore(ctx context.Context, js jetstream.JetStream, kvBucket string) (DurableCredentialStore, error) {
	if kvBucket == "" {
		kvBucket = DefaultCredentialKVBucket
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      kvBucket,
		Description: "blobgw-s3 credential registry (ADR-001 #37)",
		Storage:     jetstream.FileStorage,
		History:     1,
	})
	if err != nil {
		return nil, err
	}
	return &jetStreamCredentialStore{kv: kv}, nil
}

// Put upserts a credential, keyed by its access key id. It is the durable
// equivalent of seeding NewMemoryCredentialStore.
//
// Security note: the KV entry holds the credential including SecretKey in
// plaintext JSON. The NATS file-store directory (StoreDir) must be
// access-controlled with the same care as the credentials JSON file on disk.
// The secret is never logged by this method.
func (s *jetStreamCredentialStore) Put(ctx context.Context, cred Credential) error {
	if cred.AccessKeyID == "" {
		return errors.New("blobgw-s3: credential access key id must not be empty")
	}
	val, err := json.Marshal(cred)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, encodeKVKey(cred.AccessKeyID), val)
	return err
}

// Lookup implements CredentialStore.
func (s *jetStreamCredentialStore) Lookup(accessKeyID string) (Credential, bool) {
	if accessKeyID == "" {
		return Credential{}, false
	}
	entry, err := s.kv.Get(context.Background(), encodeKVKey(accessKeyID))
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Error("blobgw-s3: get credential", "access_key_id", accessKeyID, "err", err)
		}
		return Credential{}, false
	}
	var cred Credential
	if err := json.Unmarshal(entry.Value(), &cred); err != nil {
		slog.Error("blobgw-s3: unmarshal credential", "access_key_id", accessKeyID, "err", err)
		return Credential{}, false
	}
	return cred, true
}
