// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package remotepresign

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/ctlproto"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
)

// ---------------------------------------------------------------------------
// Fake Requester
// ---------------------------------------------------------------------------

// fakeRequester is a test double for remoteindex.Requester.  handler is called
// on every Request; it decodes the PresignBatchRequest and returns a
// PresignBatchResponse encoded as JSON.
type fakeRequester struct {
	handler func(subject string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse
	// callCount is incremented atomically for each Request call.
	callCount atomic.Int32
}

func (f *fakeRequester) Request(_ context.Context, subject string, data []byte) ([]byte, error) {
	f.callCount.Add(1)
	var req ctlproto.PresignBatchRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("fakeRequester: unmarshal: %w", err)
	}
	resp := f.handler(subject, req)
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("fakeRequester: marshal: %w", err)
	}
	return b, nil
}

// errRequester always returns the given error from Request.
type errRequester struct{ err error }

func (e *errRequester) Request(_ context.Context, _ string, _ []byte) ([]byte, error) {
	return nil, e.err
}

// ctxRequester blocks until the context is cancelled so we can test timeout
// propagation.
type ctxRequester struct{}

func (c *ctxRequester) Request(ctx context.Context, _ string, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// ---------------------------------------------------------------------------
// Helper — deterministic URL generator
// ---------------------------------------------------------------------------

// makeURL builds a deterministic presigned URL for a key and op, mimicking
// what a real blobgw would return.
func makeURL(key string, op ctlproto.PresignOp) string {
	return fmt.Sprintf("https://s3/bucket/%s?sig=%s", key, string(op))
}

// defaultHandler builds a fakeRequester handler that returns deterministic URLs
// and a fixed ExpiresAtUnix.
func defaultHandler(expiresAt int64) func(string, ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
	return func(_ string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
		urls := make(map[string]string, len(req.Keys))
		for _, k := range req.Keys {
			urls[k] = makeURL(k, req.Op)
		}
		return ctlproto.PresignBatchResponse{
			URLs:          urls,
			ExpiresAtUnix: expiresAt,
		}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPresignGet_HappyPath(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Unix()
	fake := &fakeRequester{handler: defaultHandler(expiry)}
	c := NewClient(fake)

	keys := []string{"pack/a", "pack/b", "pack/c"}
	urls, expiresAt, err := c.PresignGet(context.Background(), "acme", keys, 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: unexpected error: %v", err)
	}
	if got, want := len(urls), len(keys); got != want {
		t.Fatalf("PresignGet: got %d URLs, want %d", got, want)
	}
	for _, k := range keys {
		want := makeURL(k, ctlproto.PresignGet)
		if urls[k] != want {
			t.Errorf("PresignGet: key %q: got URL %q, want %q", k, urls[k], want)
		}
	}
	if expiresAt.Unix() != expiry {
		t.Errorf("PresignGet: expiresAt unix=%d, want %d", expiresAt.Unix(), expiry)
	}
	// Exactly one NATS request for a sub-MaxPresignKeysPerBatch batch.
	if n := fake.callCount.Load(); n != 1 {
		t.Errorf("PresignGet: callCount=%d, want 1", n)
	}
}

func TestPresignPut_HappyPath(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Unix()
	fake := &fakeRequester{handler: defaultHandler(expiry)}
	c := NewClient(fake)

	keys := []string{"pack/x", "pack/y"}
	urls, _, err := c.PresignPut(context.Background(), "tenant1", keys, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: unexpected error: %v", err)
	}
	for _, k := range keys {
		want := makeURL(k, ctlproto.PresignPut)
		if urls[k] != want {
			t.Errorf("PresignPut: key %q: got URL %q, want %q", k, urls[k], want)
		}
	}
}

func TestPresignGet_OpFieldSentToServer(t *testing.T) {
	var capturedOps []ctlproto.PresignOp
	fake := &fakeRequester{
		handler: func(_ string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			capturedOps = append(capturedOps, req.Op)
			return ctlproto.PresignBatchResponse{
				URLs:          map[string]string{req.Keys[0]: makeURL(req.Keys[0], req.Op)},
				ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
			}
		},
	}
	c := NewClient(fake)

	if _, _, err := c.PresignGet(context.Background(), "t", []string{"k1"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.PresignPut(context.Background(), "t", []string{"k2"}, time.Minute); err != nil {
		t.Fatal(err)
	}

	if len(capturedOps) != 2 {
		t.Fatalf("expected 2 captured ops, got %d", len(capturedOps))
	}
	if capturedOps[0] != ctlproto.PresignGet {
		t.Errorf("first op: got %q, want %q", capturedOps[0], ctlproto.PresignGet)
	}
	if capturedOps[1] != ctlproto.PresignPut {
		t.Errorf("second op: got %q, want %q", capturedOps[1], ctlproto.PresignPut)
	}
}

func TestPresignGet_BatchSplitting(t *testing.T) {
	// Build a key set that exceeds MaxPresignKeysPerBatch to force splitting.
	total := ctlproto.MaxPresignKeysPerBatch + 5
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("pack/%04d", i)
	}

	expiry := time.Now().Add(time.Hour).Unix()
	fake := &fakeRequester{handler: defaultHandler(expiry)}
	c := NewClient(fake)

	urls, _, err := c.PresignGet(context.Background(), "acme", keys, time.Hour)
	if err != nil {
		t.Fatalf("PresignGet batch split: unexpected error: %v", err)
	}

	// Two NATS requests: one full batch + one remainder.
	if n := fake.callCount.Load(); n != 2 {
		t.Errorf("batch split: callCount=%d, want 2", n)
	}

	// All keys must be present in the merged result.
	if got, want := len(urls), total; got != want {
		t.Fatalf("batch split: got %d URLs, want %d", got, want)
	}
	for _, k := range keys {
		if _, ok := urls[k]; !ok {
			t.Errorf("batch split: missing key %q in result", k)
		}
	}
}

func TestPresignGet_EarliestExpiryAcrossBatches(t *testing.T) {
	// First batch expires later, second batch expires earlier.
	// Client must return the earlier expiry.
	total := ctlproto.MaxPresignKeysPerBatch + 1
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	call := 0
	laterExpiry := time.Now().Add(2 * time.Hour).Unix()
	earlierExpiry := time.Now().Add(30 * time.Minute).Unix()

	fake := &fakeRequester{
		handler: func(_ string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			call++
			exp := laterExpiry
			if call == 2 {
				exp = earlierExpiry
			}
			urls := make(map[string]string, len(req.Keys))
			for _, k := range req.Keys {
				urls[k] = makeURL(k, req.Op)
			}
			return ctlproto.PresignBatchResponse{URLs: urls, ExpiresAtUnix: exp}
		},
	}
	c := NewClient(fake)

	_, expiresAt, err := c.PresignGet(context.Background(), "t", keys, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expiresAt.Unix() != earlierExpiry {
		t.Errorf("expiresAt unix=%d, want earlierExpiry=%d", expiresAt.Unix(), earlierExpiry)
	}
}

func TestPresignGet_ServerErrorPropagation(t *testing.T) {
	fake := &fakeRequester{
		handler: func(_ string, _ ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			return ctlproto.PresignBatchResponse{Error: "bucket not found"}
		},
	}
	c := NewClient(fake)

	_, _, err := c.PresignGet(context.Background(), "acme", []string{"k"}, time.Minute)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bucket not found") {
		t.Errorf("error %q does not contain server error text", err)
	}
}

func TestPresignGet_TransportErrorPropagation(t *testing.T) {
	transportErr := errors.New("nats: connection closed")
	c := NewClient(&errRequester{err: transportErr})

	_, _, err := c.PresignGet(context.Background(), "acme", []string{"k"}, time.Minute)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, transportErr) {
		t.Errorf("expected errors.Is chain to transportErr; got: %v", err)
	}
}

func TestPresignGet_NoRespondersError(t *testing.T) {
	// Verify that remoteindex.ErrNoResponders is threaded through correctly when
	// the NatsRequester (real transport) reports no subscribers.  Here we simulate
	// it via errRequester wrapping the sentinel.
	wrapped := fmt.Errorf("%w: blobgw.acme.presign", remoteindex.ErrNoResponders)
	c := NewClient(&errRequester{err: wrapped})

	_, _, err := c.PresignGet(context.Background(), "acme", []string{"k"}, time.Minute)
	if !errors.Is(err, remoteindex.ErrNoResponders) {
		t.Errorf("expected errors.Is(err, ErrNoResponders); got: %v", err)
	}
}

func TestPresignGet_CtxTimeout(t *testing.T) {
	c := NewClient(&ctxRequester{}, WithTimeout(50*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := c.PresignGet(ctx, "acme", []string{"k"}, time.Minute)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded; got: %v", err)
	}
}

func TestPresignGet_EmptyKeys(t *testing.T) {
	// No request should be issued for an empty key set.
	called := false
	fake := &fakeRequester{
		handler: func(_ string, _ ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			called = true
			return ctlproto.PresignBatchResponse{}
		},
	}
	c := NewClient(fake)

	urls, expiresAt, err := c.PresignGet(context.Background(), "acme", []string{}, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("Request should not have been called for empty keys")
	}
	if len(urls) != 0 {
		t.Errorf("expected empty map, got %v", urls)
	}
	if !expiresAt.IsZero() {
		t.Errorf("expected zero expiresAt, got %v", expiresAt)
	}
}

func TestPresignGet_NilKeys(t *testing.T) {
	// nil slice is also empty — no request issued.
	called := false
	fake := &fakeRequester{
		handler: func(_ string, _ ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			called = true
			return ctlproto.PresignBatchResponse{}
		},
	}
	c := NewClient(fake)

	urls, expiresAt, err := c.PresignGet(context.Background(), "acme", nil, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("Request should not have been called for nil keys")
	}
	if len(urls) != 0 {
		t.Errorf("expected empty map, got %v", urls)
	}
	if !expiresAt.IsZero() {
		t.Errorf("expected zero expiresAt, got %v", expiresAt)
	}
}

func TestPresignGet_SubjectContainsTenant(t *testing.T) {
	// The NATS subject must be ctlproto.PresignSubject(tenant).
	tenant := "mytenant"
	wantSubject := ctlproto.PresignSubject(tenant)

	var gotSubject string
	fake := &fakeRequester{
		handler: func(subject string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			gotSubject = subject
			return ctlproto.PresignBatchResponse{
				URLs:          map[string]string{req.Keys[0]: makeURL(req.Keys[0], req.Op)},
				ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
			}
		},
	}
	c := NewClient(fake)

	if _, _, err := c.PresignGet(context.Background(), tenant, []string{"k"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if gotSubject != wantSubject {
		t.Errorf("subject: got %q, want %q", gotSubject, wantSubject)
	}
}

// ---------------------------------------------------------------------------
// Expiry sentinel / zero-ExpiresAtUnix tests (fix: haveExpiry bool)
// ---------------------------------------------------------------------------

func TestPresignGet_ZeroExpiresAtUnix_ReturnsZeroTime(t *testing.T) {
	// Server returns ExpiresAtUnix=0 (unset). The client must return the zero
	// time.Time{} — NOT time.Unix(0,0) (1970) — so schedulers don't treat a
	// fresh set of URLs as already expired.
	fake := &fakeRequester{handler: defaultHandler(0)}
	c := NewClient(fake)

	_, expiresAt, err := c.PresignGet(context.Background(), "acme", []string{"k"}, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !expiresAt.IsZero() {
		t.Errorf("expiresAt = %v, want zero time (server returned ExpiresAtUnix=0)", expiresAt)
	}
}

func TestPresignGet_MixedExpiry_ZeroIgnored(t *testing.T) {
	// Two batches: first returns ExpiresAtUnix=0 (unset), second returns a
	// positive value. The zero must be ignored and the positive value returned.
	total := ctlproto.MaxPresignKeysPerBatch + 1
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	positiveExpiry := time.Now().Add(time.Hour).Unix()
	call := 0
	fake := &fakeRequester{
		handler: func(_ string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			call++
			exp := int64(0)
			if call == 2 {
				exp = positiveExpiry
			}
			urls := make(map[string]string, len(req.Keys))
			for _, k := range req.Keys {
				urls[k] = makeURL(k, req.Op)
			}
			return ctlproto.PresignBatchResponse{URLs: urls, ExpiresAtUnix: exp}
		},
	}
	c := NewClient(fake)

	_, expiresAt, err := c.PresignGet(context.Background(), "t", keys, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expiresAt.IsZero() {
		t.Error("expiresAt is zero time; want positive expiry (zero batch should be ignored)")
	}
	if expiresAt.Unix() != positiveExpiry {
		t.Errorf("expiresAt unix=%d, want positiveExpiry=%d", expiresAt.Unix(), positiveExpiry)
	}
}

func TestPresignGet_DomainMatchesTenant(t *testing.T) {
	// The Domain field in the request must equal the tenant argument so blobgw
	// can validate Domain == subject tenant (ADR-001 §7).
	tenant := "mytenant"
	var gotDomain string
	fake := &fakeRequester{
		handler: func(_ string, req ctlproto.PresignBatchRequest) ctlproto.PresignBatchResponse {
			gotDomain = req.Domain
			return ctlproto.PresignBatchResponse{
				URLs:          map[string]string{req.Keys[0]: makeURL(req.Keys[0], req.Op)},
				ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
			}
		},
	}
	c := NewClient(fake)

	if _, _, err := c.PresignGet(context.Background(), tenant, []string{"k"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if gotDomain != tenant {
		t.Errorf("Domain: got %q, want %q", gotDomain, tenant)
	}
}
