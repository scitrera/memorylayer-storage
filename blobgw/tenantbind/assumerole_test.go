// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeAssumer is a deterministic webIdentityAssumer for tests: it records the
// last roleARN/sessionName it was asked to assume and returns canned creds (or a
// canned error), with no AWS dependency.
type fakeAssumer struct {
	gotRoleARN     string
	gotSessionName string
	calls          int

	creds Creds
	err   error
}

func (f *fakeAssumer) Assume(_ context.Context, roleARN, sessionName string) (Creds, error) {
	f.calls++
	f.gotRoleARN = roleARN
	f.gotSessionName = sessionName
	if f.err != nil {
		return Creds{}, f.err
	}
	return f.creds, nil
}

func TestAssumeRoleProvider_MapsDescriptorToBinding(t *testing.T) {
	expiry := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	assumer := &fakeAssumer{creds: Creds{
		AccessKeyID:     "ASIA-temp",
		SecretAccessKey: "temp-secret",
		SessionToken:    "temp-token",
		Expiry:          expiry,
	}}
	resolver := NewMapResolver()
	resolver.Set("acme", Descriptor{
		Endpoint:      "s3.us-east-1.amazonaws.com",
		Region:        "us-east-1",
		Bucket:        "acme-bkt-x9",
		Prefix:        "packs",
		CredentialRef: "arn:aws:iam::123456789012:role/blobgw-acme-7f3a",
	})

	p, err := NewAssumeRoleProvider(resolver, assumer)
	if err != nil {
		t.Fatalf("NewAssumeRoleProvider: %v", err)
	}

	b, err := p.BindingFor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("BindingFor: %v", err)
	}

	// Descriptor.CredentialRef is interpreted as the role ARN.
	if assumer.gotRoleARN != "arn:aws:iam::123456789012:role/blobgw-acme-7f3a" {
		t.Errorf("assumed roleARN = %q, want the descriptor CredentialRef", assumer.gotRoleARN)
	}
	// RoleSessionName is the §2.9 audit-distinguishing "blobgw-<tenant>".
	if assumer.gotSessionName != "blobgw-acme" {
		t.Errorf("sessionName = %q, want %q", assumer.gotSessionName, "blobgw-acme")
	}
	// Locator comes from the Descriptor.
	if b.Endpoint != "s3.us-east-1.amazonaws.com" || b.Region != "us-east-1" ||
		b.Bucket != "acme-bkt-x9" || b.Prefix != "packs" {
		t.Errorf("binding locator mismatch: %+v", b)
	}
	// Creds (incl. session token + expiry) come from the STS response.
	if b.Creds.AccessKeyID != "ASIA-temp" || b.Creds.SecretAccessKey != "temp-secret" ||
		b.Creds.SessionToken != "temp-token" {
		t.Errorf("binding creds mismatch: %+v", b.Creds)
	}
	if !b.Creds.Expiry.Equal(expiry) {
		t.Errorf("binding Expiry = %v, want %v", b.Creds.Expiry, expiry)
	}
}

func TestAssumeRoleProvider_UnknownTenant(t *testing.T) {
	p, err := NewAssumeRoleProvider(NewMapResolver(), &fakeAssumer{})
	if err != nil {
		t.Fatalf("NewAssumeRoleProvider: %v", err)
	}
	if _, err := p.BindingFor(context.Background(), "ghost"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestAssumeRoleProvider_EmptyTenant(t *testing.T) {
	p, _ := NewAssumeRoleProvider(NewMapResolver(), &fakeAssumer{})
	if _, err := p.BindingFor(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty tenant")
	}
}

func TestAssumeRoleProvider_AssumeError(t *testing.T) {
	resolver := NewMapResolver()
	resolver.Set("acme", Descriptor{Region: "us-east-1", Bucket: "b", CredentialRef: "arn:aws:iam::1:role/r"})
	sentinel := errors.New("sts boom")
	p, _ := NewAssumeRoleProvider(resolver, &fakeAssumer{err: sentinel})
	_, err := p.BindingFor(context.Background(), "acme")
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sts error, got %v", err)
	}
}

func TestAssumeRoleProvider_EmptyCredentialRef(t *testing.T) {
	resolver := NewMapResolver()
	resolver.Set("acme", Descriptor{Region: "us-east-1", Bucket: "b"}) // no CredentialRef
	assumer := &fakeAssumer{}
	p, _ := NewAssumeRoleProvider(resolver, assumer)
	if _, err := p.BindingFor(context.Background(), "acme"); err == nil {
		t.Fatal("expected error for empty CredentialRef (role ARN)")
	}
	if assumer.calls != 0 {
		t.Fatalf("expected no STS call when CredentialRef is empty, got %d", assumer.calls)
	}
}

func TestNewAssumeRoleProvider_Validation(t *testing.T) {
	if _, err := NewAssumeRoleProvider(nil, &fakeAssumer{}); err == nil {
		t.Fatal("expected error for nil resolver")
	}
	if _, err := NewAssumeRoleProvider(NewMapResolver(), nil); err == nil {
		t.Fatal("expected error for nil assumer")
	}
}

func TestRoleSessionName(t *testing.T) {
	if got := RoleSessionName("acme"); got != "blobgw-acme" {
		t.Errorf("RoleSessionName = %q, want blobgw-acme", got)
	}
}
