// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-s3/s3"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// credentialFile is the on-disk JSON shape of the -credentials file: a list of
// access-key → {secret, domain} entries. It is deliberately minimal — a real
// secrets-manager / Postgres lookup replaces it in production by implementing
// s3.CredentialStore, so the file format never has to grow auth features.
//
//	{
//	  "credentials": [
//	    {"access_key_id": "AKIA...", "secret_key": "...", "domain": "tenant-a"},
//	    {"access_key_id": "AKIB...", "secret_key": "...", "domain": "tenant-b"}
//	  ]
//	}
type credentialFile struct {
	Credentials []credentialEntry `json:"credentials"`
}

type credentialEntry struct {
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_key"`
	Domain      string `json:"domain"`
}

// parseCredentials reads and validates the JSON credentials file, returning the
// parsed credentials. Every entry must carry a non-empty access key, secret, and
// domain (the domain is the casstore dedup namespace the key acts within). It is
// shared by both the in-memory and JetStream-backed credential stores: the
// memory store wraps the slice directly, the JetStream store upserts each entry
// into KV so the credential registry survives a restart.
func parseCredentials(path string) ([]s3.Credential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials file: %w", err)
	}
	var cf credentialFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cf); err != nil {
		return nil, fmt.Errorf("parse credentials JSON: %w", err)
	}
	if len(cf.Credentials) == 0 {
		return nil, fmt.Errorf("credentials file has no entries")
	}
	creds := make([]s3.Credential, 0, len(cf.Credentials))
	seen := make(map[string]struct{}, len(cf.Credentials))
	for i, e := range cf.Credentials {
		if e.AccessKeyID == "" || e.SecretKey == "" || e.Domain == "" {
			return nil, fmt.Errorf("credentials entry %d: access_key_id, secret_key, and domain are all required", i)
		}
		if _, dup := seen[e.AccessKeyID]; dup {
			return nil, fmt.Errorf("credentials entry %d: duplicate access_key_id %q", i, e.AccessKeyID)
		}
		seen[e.AccessKeyID] = struct{}{}
		creds = append(creds, s3.Credential{
			AccessKeyID: e.AccessKeyID,
			SecretKey:   e.SecretKey,
			Domain:      e.Domain,
		})
	}
	return creds, nil
}

// loadCredentials reads the JSON credentials file and returns an in-memory
// store. This is the default (memory) registry backend.
func loadCredentials(path string) (s3.CredentialStore, error) {
	creds, err := parseCredentials(path)
	if err != nil {
		return nil, err
	}
	return s3.NewMemoryCredentialStore(creds...), nil
}

// newLocalRouter wires a TenantRouter over local-filesystem casstore backends
// (manifests under dir/manifests, chunks under dir/chunks) with a content-type
// compression policy using codec. The returned router satisfies s3.Router and
// its GC() method returns a ChunkedGC ready for scheduling.
//
// Previously this file contained a hand-rolled localRouter type that duplicated
// gateway.TenantRouter just to wire CompressionPolicy. Now that TenantRouter
// supports compression natively via gateway.WithCompressionPolicy, we delegate
// directly to gateway.NewLocalTenantRouter + WithCompressionPolicy.
func newLocalRouter(dir string, packTarget int, codec snapshot.CompressionAlgo, framing snapshot.PackCompressionMode) (*localRouterAdapter, error) {
	policy := snapshot.NewContentTypePolicy(codec)
	ltr, err := gateway.NewLocalTenantRouter(dir, packTarget,
		gateway.WithCompressionPolicy(policy),
		gateway.WithPackCompression(framing))
	if err != nil {
		return nil, err
	}
	return &localRouterAdapter{ltr: ltr}, nil
}

// localRouterAdapter wraps a LocalTenantRouter to expose the s3.Router interface
// and the gc() helper the main package uses to schedule GC.
type localRouterAdapter struct {
	ltr *gateway.LocalTenantRouter
}

// For implements s3.Router.
func (a *localRouterAdapter) For(domain string) (*gateway.Gateway, error) {
	return a.ltr.Router.For(domain)
}

// gc returns a ChunkedGC with the given safety window, ready for Loop.
func (a *localRouterAdapter) gc(safetyWindow time.Duration) *snapshot.ChunkedGC {
	gc := a.ltr.Router.GC()
	gc.SafetyWindow = safetyWindow
	return gc
}
