// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3util

import (
	"context"
	"testing"
)

// TestNewClient_ThreadsSessionToken verifies the Config → aws-sdk-go-v2 static
// credentials mapping carries the STS session token (ADR §2.9 / §7.8): the
// client's credentials provider must Retrieve a SessionToken equal to the one
// supplied, so STS temporary credentials drive the snapshot-store S3 client.
func TestNewClient_ThreadsSessionToken(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Bucket:       "tenant-bkt",
		Region:       "us-east-1",
		AccessKey:    "AK",
		SecretKey:    "SK",
		SessionToken: "session-tok",
	}

	client, err := NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	creds, err := client.Options().Credentials.Retrieve(ctx)
	if err != nil {
		t.Fatalf("Credentials.Retrieve: %v", err)
	}
	if creds.SessionToken != "session-tok" {
		t.Errorf("creds.SessionToken = %q, want %q", creds.SessionToken, "session-tok")
	}
	if creds.AccessKeyID != "AK" || creds.SecretAccessKey != "SK" {
		t.Errorf("creds mismatch: AccessKeyID=%q SecretAccessKey=%q", creds.AccessKeyID, creds.SecretAccessKey)
	}
}

// TestNewClient_EmptySessionTokenStaticKeys verifies the additive change is
// backward-compatible: with static keys and no session token, the provider
// Retrieves an empty SessionToken (today's behavior).
func TestNewClient_EmptySessionTokenStaticKeys(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Bucket: "b", Region: "us-east-1", AccessKey: "AK", SecretKey: "SK"}

	client, err := NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	creds, err := client.Options().Credentials.Retrieve(ctx)
	if err != nil {
		t.Fatalf("Credentials.Retrieve: %v", err)
	}
	if creds.SessionToken != "" {
		t.Errorf("creds.SessionToken = %q, want empty for unset token", creds.SessionToken)
	}
	if creds.AccessKeyID != "AK" {
		t.Errorf("creds.AccessKeyID = %q, want AK", creds.AccessKeyID)
	}
}
