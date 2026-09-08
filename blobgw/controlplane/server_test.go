// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

const (
	testTenant = "acme"
	testDomain = "acme"
	testBucket = "acme-bucket"

	// A second tenant, used to prove presign credential/bucket resolution is
	// scoped to the SUBJECT tenant and a cross-tenant Domain is rejected.
	otherTenant = "globex"
	otherBucket = "globex-bucket"
)

// startNATS boots an in-process NATS server (no TCP listener) and returns a
// connected client conn. Both are cleaned up via t.Cleanup.
func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{DontListen: true}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats not ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	conn, err := nats.Connect("", nats.InProcessServer(srv))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

// newProvider builds a static tenantbind provider pointing acme at a fake
// S3-compatible endpoint. Presigning is signing-only, so no live S3 is needed.
func newProvider(t *testing.T) tenantbind.Provider {
	t.Helper()
	res := tenantbind.NewMapResolver()
	res.Set(testTenant, tenantbind.Descriptor{
		Endpoint:      "http://fake-s3.local:9000",
		Region:        "us-east-1",
		Bucket:        testBucket,
		Prefix:        "packs",
		CredentialRef: "acme-secret",
	})
	res.Set(otherTenant, tenantbind.Descriptor{
		Endpoint:      "http://fake-s3.local:9000",
		Region:        "us-east-1",
		Bucket:        otherBucket,
		Prefix:        "packs",
		CredentialRef: "globex-secret",
	})
	secrets := tenantbind.NewMapSecretStore()
	secrets.Set("acme-secret", "AKIDEXAMPLE", "secretkeyexample")
	secrets.Set("globex-secret", "AKIDGLOBEX", "secretkeyglobex")
	return tenantbind.NewStaticKeysProvider(res, secrets)
}

// quietLogger discards log output so handler-error tests don't spam.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newServer wires a Server over the given index + a static provider, starts it,
// and returns it. Cleanup closes it.
func newServer(t *testing.T, conn *nats.Conn, index snapshot.DedupStore, opts ...controlplane.Option) *controlplane.Server {
	t.Helper()
	opts = append([]controlplane.Option{controlplane.WithLogger(quietLogger())}, opts...)
	srv, err := controlplane.NewServer(conn, index, newProvider(t), opts...)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// request marshals req, sends it on subject, and unmarshals the reply into resp.
func request(t *testing.T, conn *nats.Conn, subject string, req, resp any) {
	t.Helper()
	codec := ctlproto.DefaultCodec
	data, err := codec.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	msg, err := conn.Request(subject, data, 3*time.Second)
	if err != nil {
		t.Fatalf("request %q: %v", subject, err)
	}
	if err := codec.Unmarshal(msg.Data, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
}

func TestRecordThenLookup(t *testing.T) {
	conn := startNATS(t)
	index := snapshot.NewMemoryDedupStore()
	newServer(t, conn, index)

	// Record two chunk locations.
	var recResp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain: testDomain,
		Locations: []ctlproto.ChunkLocation{
			{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 0, Size: 10}},
			{ChunkHash: "h2", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 10, Size: 20}},
		},
	}, &recResp)
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	// Lookup: one hit, one miss.
	var lookResp ctlproto.LookupBatchResponse
	request(t, conn, ctlproto.IndexLookupSubject(testTenant), ctlproto.LookupBatchRequest{
		Domain:      testDomain,
		ChunkHashes: []string{"h1", "missing"},
	}, &lookResp)
	if lookResp.Error != "" {
		t.Fatalf("lookup error: %s", lookResp.Error)
	}
	if len(lookResp.Locations) != 1 {
		t.Fatalf("want 1 location, got %d (%v)", len(lookResp.Locations), lookResp.Locations)
	}
	got, ok := lookResp.Locations["h1"]
	if !ok {
		t.Fatalf("h1 not found in %v", lookResp.Locations)
	}
	if got.PackHash != "p1" || got.Offset != 0 || got.Size != 10 {
		t.Fatalf("h1 = %+v, want {p1 0 10}", got)
	}
	if _, ok := lookResp.Locations["missing"]; ok {
		t.Fatalf("missing hash should be absent")
	}
}

func TestPurge(t *testing.T) {
	conn := startNATS(t)
	index := snapshot.NewMemoryDedupStore()
	newServer(t, conn, index)

	var recResp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain: testDomain,
		Locations: []ctlproto.ChunkLocation{
			{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Size: 10}},
		},
	}, &recResp)
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	var purgeResp ctlproto.PurgePacksResponse
	request(t, conn, ctlproto.GCPurgeSubject(testTenant), ctlproto.PurgePacksRequest{
		Domain:     testDomain,
		PackHashes: []string{"p1"},
	}, &purgeResp)
	if purgeResp.Error != "" {
		t.Fatalf("purge error: %s", purgeResp.Error)
	}

	// After purge, h1 must be gone.
	var lookResp ctlproto.LookupBatchResponse
	request(t, conn, ctlproto.IndexLookupSubject(testTenant), ctlproto.LookupBatchRequest{
		Domain:      testDomain,
		ChunkHashes: []string{"h1"},
	}, &lookResp)
	if lookResp.Error != "" {
		t.Fatalf("lookup error: %s", lookResp.Error)
	}
	if len(lookResp.Locations) != 0 {
		t.Fatalf("want 0 locations after purge, got %v", lookResp.Locations)
	}
}

func TestPresign(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	for _, op := range []ctlproto.PresignOp{ctlproto.PresignGet, ctlproto.PresignPut} {
		t.Run(string(op), func(t *testing.T) {
			var resp ctlproto.PresignBatchResponse
			request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
				Domain:     testDomain,
				Op:         op,
				Keys:       []string{"obj-a", "obj-b"},
				TTLSeconds: 300,
			}, &resp)
			if resp.Error != "" {
				t.Fatalf("presign error: %s", resp.Error)
			}
			if len(resp.URLs) != 2 {
				t.Fatalf("want 2 urls, got %d (%v)", len(resp.URLs), resp.URLs)
			}
			for _, key := range []string{"obj-a", "obj-b"} {
				url := resp.URLs[key]
				if url == "" {
					t.Fatalf("empty url for %q", key)
				}
				if !strings.Contains(url, testBucket) {
					t.Fatalf("url for %q does not contain bucket %q: %s", key, testBucket, url)
				}
				// Prefix is applied: keys live under packs/.
				if !strings.Contains(url, "packs/"+key) {
					t.Fatalf("url for %q missing prefixed key: %s", key, url)
				}
			}
			if resp.ExpiresAtUnix <= time.Now().Unix() {
				t.Fatalf("ExpiresAtUnix %d not in the future", resp.ExpiresAtUnix)
			}
		})
	}
}

// TestPresignDomainMismatchRejected is the tenant-isolation regression guard
// (ADR §2.6/§2.9): a presign request whose subject tenant is "acme" but whose
// payload Domain points at a DIFFERENT tenant ("globex") must be REJECTED, and
// must never mint a URL for the other tenant's bucket. Credential/bucket
// resolution uses the subject tenant, never the client-supplied Domain.
func TestPresignDomainMismatchRejected(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	var resp ctlproto.PresignBatchResponse
	// Subject tenant = acme (PresignSubject(testTenant)); Domain = globex.
	request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
		Domain:     otherTenant,
		Op:         ctlproto.PresignGet,
		Keys:       []string{"obj"},
		TTLSeconds: 300,
	}, &resp)
	if resp.Error == "" {
		t.Fatalf("expected cross-tenant domain rejection, got success with %d urls", len(resp.URLs))
	}
	if !strings.Contains(resp.Error, "does not match subject tenant") {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	// Defense in depth: no URL was minted at all, so it cannot reference globex's
	// bucket.
	for _, url := range resp.URLs {
		if strings.Contains(url, otherBucket) {
			t.Fatalf("minted a URL for the OTHER tenant's bucket %q: %s", otherBucket, url)
		}
	}
	if len(resp.URLs) != 0 {
		t.Fatalf("expected no URLs on a rejected request, got %d", len(resp.URLs))
	}
}

// TestPresignScopedToSubjectTenant proves the minted URL is scoped to the
// SUBJECT tenant's bucket (ADR §2.6/§2.9), not the client-supplied Domain. The
// subject is globex with a Domain that matches it, so the URL must contain
// globex's bucket and never acme's. (An empty Domain is a separate contract —
// rejected by validateDomain — exercised by TestEmptyDomainRejected.)
func TestPresignScopedToSubjectTenant(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	var resp ctlproto.PresignBatchResponse
	request(t, conn, ctlproto.PresignSubject(otherTenant), ctlproto.PresignBatchRequest{
		Domain:     otherTenant,
		Op:         ctlproto.PresignGet,
		Keys:       []string{"obj"},
		TTLSeconds: 300,
	}, &resp)
	if resp.Error != "" {
		t.Fatalf("presign error: %s", resp.Error)
	}
	url := resp.URLs["obj"]
	if url == "" {
		t.Fatalf("no url minted: %v", resp.URLs)
	}
	if !strings.Contains(url, otherBucket) {
		t.Fatalf("url not scoped to subject tenant's bucket %q: %s", otherBucket, url)
	}
	if strings.Contains(url, testBucket) {
		t.Fatalf("url leaked the other tenant's bucket %q: %s", testBucket, url)
	}
}

// TestPresignKeyDotDotRejected hardens against prefix escape (ADR §2.6): a
// request key containing a ".." path segment must be rejected, never minted.
func TestPresignKeyDotDotRejected(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	for _, key := range []string{"..", "../escape", "a/../../b", "nested/../x"} {
		var resp ctlproto.PresignBatchResponse
		request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
			Domain:     testDomain,
			Op:         ctlproto.PresignGet,
			Keys:       []string{key},
			TTLSeconds: 300,
		}, &resp)
		if resp.Error == "" {
			t.Fatalf("expected rejection for key %q, got success with %d urls", key, len(resp.URLs))
		}
		if !strings.Contains(resp.Error, "..") {
			t.Fatalf("key %q: unexpected error: %s", key, resp.Error)
		}
		if len(resp.URLs) != 0 {
			t.Fatalf("key %q: expected no URLs on rejection, got %d", key, len(resp.URLs))
		}
	}

	// A literal ".." inside a segment (not a standalone segment) is allowed.
	var ok ctlproto.PresignBatchResponse
	request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
		Domain:     testDomain,
		Op:         ctlproto.PresignGet,
		Keys:       []string{"foo..bar"},
		TTLSeconds: 300,
	}, &ok)
	if ok.Error != "" {
		t.Fatalf("legitimate key foo..bar rejected: %s", ok.Error)
	}
	if ok.URLs["foo..bar"] == "" {
		t.Fatalf("legitimate key foo..bar not minted: %v", ok.URLs)
	}
}

func TestPresignTTLCapped(t *testing.T) {
	conn := startNATS(t)
	// Cap at 1m; request 1h. Expiry must reflect the cap, not the request.
	newServer(t, conn, snapshot.NewMemoryDedupStore(), controlplane.WithPresignMaxTTL(time.Minute))

	before := time.Now()
	var resp ctlproto.PresignBatchResponse
	request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
		Domain:     testDomain,
		Op:         ctlproto.PresignGet,
		Keys:       []string{"obj"},
		TTLSeconds: 3600,
	}, &resp)
	if resp.Error != "" {
		t.Fatalf("presign error: %s", resp.Error)
	}
	// Expiry must reflect the 1m cap, not the requested 1h. Tighten the window to
	// cap + a few seconds of scheduling slack: a 2x off-by-one in effectiveTTL
	// (e.g. 2m) would land outside this window and fail the test, where the old
	// 2m window would have masked it.
	const cap = time.Minute
	upper := before.Add(cap + 5*time.Second).Unix()
	if resp.ExpiresAtUnix > upper {
		t.Fatalf("ExpiresAtUnix %d exceeds capped window (<= %d, cap=%s)", resp.ExpiresAtUnix, upper, cap)
	}
	// Lower bound: it must be at least roughly the cap out (not, say, the default
	// or zero), so the window cannot pass by being too short either.
	lower := before.Add(cap - 5*time.Second).Unix()
	if resp.ExpiresAtUnix < lower {
		t.Fatalf("ExpiresAtUnix %d is below the capped window (>= %d, cap=%s)", resp.ExpiresAtUnix, lower, cap)
	}
}

func TestInvalidTenantSubjectDropped(t *testing.T) {
	conn := startNATS(t)
	// Server subscribes to wildcards; a malformed subject under blobgw.* with a
	// wildcard-looking tenant cannot reach a handler (and we never reply). We
	// instead assert a well-formed but rejected scenario: a request with no
	// reply subject and an unparsable tenant is silently dropped. Since wildcard
	// subscriptions only match blobgw.<token>.<op>, the cleanest assertion is
	// that a subject with an empty tenant token (blobgw..presign) does not crash
	// and yields no reply within a short window.
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	data, _ := ctlproto.DefaultCodec.Marshal(ctlproto.PresignBatchRequest{Domain: testDomain, Op: ctlproto.PresignGet, Keys: []string{"k"}})
	// "blobgw..presign" → tenant token is "" → TenantFromSubject rejects it.
	_, err := conn.Request("blobgw..presign", data, 500*time.Millisecond)
	if err == nil {
		t.Fatalf("expected no reply (timeout) for invalid-tenant subject, got a response")
	}
	if err != nats.ErrTimeout && err != nats.ErrNoResponders {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOversizedBatchRejected(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	keys := make([]string, ctlproto.MaxPresignKeysPerBatch+1)
	for i := range keys {
		keys[i] = "k"
	}
	var resp ctlproto.PresignBatchResponse
	request(t, conn, ctlproto.PresignSubject(testTenant), ctlproto.PresignBatchRequest{
		Domain: testDomain,
		Op:     ctlproto.PresignGet,
		Keys:   keys,
	}, &resp)
	if resp.Error == "" {
		t.Fatalf("expected oversize rejection, got success with %d urls", len(resp.URLs))
	}
	if !strings.Contains(resp.Error, "too large") {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestHandlerErrorPopulatesResponseError(t *testing.T) {
	conn := startNATS(t)
	// errStore fails every Record so the handler must surface the error in the
	// response rather than dropping the reply.
	newServer(t, conn, errStore{})

	var resp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1"}}},
	}, &resp)
	if resp.Error == "" {
		t.Fatalf("expected handler error in response, got empty Error")
	}
	if !strings.Contains(resp.Error, "boom") {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestEmptyDomainRejected(t *testing.T) {
	conn := startNATS(t)
	newServer(t, conn, snapshot.NewMemoryDedupStore())

	var resp ctlproto.LookupBatchResponse
	request(t, conn, ctlproto.IndexLookupSubject(testTenant), ctlproto.LookupBatchRequest{
		Domain:      "",
		ChunkHashes: []string{"h1"},
	}, &resp)
	if resp.Error == "" {
		t.Fatalf("expected empty-domain rejection")
	}
	if !strings.Contains(resp.Error, "domain") {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestQueueGroupDeliversOnce(t *testing.T) {
	conn := startNATS(t)
	// Two servers sharing the queue group: a single client request must be
	// handled by exactly one of them. We assert via the dedup index: a Record
	// inserts h1 once regardless of which server handled it, and a follow-up
	// Lookup confirms a single, consistent entry — i.e. no double-delivery
	// corruption. To directly observe single-delivery we count handler hits with
	// a shared counting index.
	counter := &countingStore{MemoryDedupStore: snapshot.NewMemoryDedupStore()}
	newServer(t, conn, counter)
	newServer(t, conn, counter)

	var resp ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1"}}},
	}, &resp)
	if resp.Error != "" {
		t.Fatalf("record error: %s", resp.Error)
	}
	if got := counter.records(); got != 1 {
		t.Fatalf("queue group should deliver to exactly one server: Record called %d times", got)
	}
}
