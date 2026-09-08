// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package remoteblob_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/ctlproto"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteblob"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remotepresign"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"

	kblob "github.com/kopia/kopia/repo/blob"
)

const testTenant = "tenant-a"

// fakeS3 is an in-memory object store served over httptest. It records every
// object key it sees so tests can assert key stability.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	putKeys []string
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: make(map[string][]byte)} }

// objectKey extracts the object key from the request path. Presigned URLs in
// this harness use the path "/<key>" (query string carries the fake signature).
func objectKey(r *http.Request) string {
	return strings.TrimPrefix(r.URL.Path, "/")
}

func (s *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := objectKey(r)
	switch r.Method {
	case http.MethodPut:
		body := new(bytes.Buffer)
		if _, err := body.ReadFrom(r.Body); err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		s.mu.Lock()
		s.objects[key] = body.Bytes()
		s.putKeys = append(s.putKeys, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		s.mu.Lock()
		data, ok := s.objects[key]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// http.ServeContent handles the Range header (206 partial content),
		// exercising s3http.GetRange's 206 path.
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// fakeRequester implements remoteindex.Requester. It decodes the presign
// request via ctlproto and returns presigned URLs that point at the httptest
// server, exercising the REAL remotepresign.Client + s3http.Client + adapter.
type fakeRequester struct {
	baseURL string
	codec   ctlproto.Codec

	failErr error // when non-nil, every Request returns this error

	reqCount atomic.Int64  // number of presign requests served (cache-hit assertions)
	entered  chan struct{} // if non-nil, signalled on each Request entry (barrier tests)
	release  chan struct{} // if non-nil, Request blocks until this is closed
}

func (f *fakeRequester) Request(_ context.Context, _ string, data []byte) ([]byte, error) {
	f.reqCount.Add(1)
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.release != nil {
		<-f.release
	}
	if f.failErr != nil {
		return nil, f.failErr
	}

	var req ctlproto.PresignBatchRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("fakeRequester: unmarshal request: %w", err)
	}

	urls := make(map[string]string, len(req.Keys))
	for _, k := range req.Keys {
		// Path-escape the key so multi-segment keys ("blobs/x") map cleanly to
		// a URL path the fake S3 server can decode back to the same key.
		urls[k] = f.baseURL + "/" + (&url.URL{Path: k}).EscapedPath() + "?sig=fake-" + string(req.Op)
	}

	resp := ctlproto.PresignBatchResponse{
		URLs:          urls,
		ExpiresAtUnix: 1 << 40, // far future
	}
	return f.codec.Marshal(resp)
}

// newStore wires the real remotepresign + s3http clients against the fake S3
// server and fake requester, returning the adapter under test.
func newStore(t *testing.T, srv *httptest.Server, fr *fakeRequester, opts ...remoteblob.Option) *remoteblob.BlobStore {
	t.Helper()
	presign := remotepresign.NewClient(fr)
	httpClient := s3http.NewClient(s3http.WithHTTPClient(srv.Client()))
	return remoteblob.NewBlobStore(presign, httpClient, testTenant, opts...)
}

func TestPutGetWholeBlobRoundTrip(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("pack-tenant-a-deadbeef")
	payload := []byte("hello presigned world")

	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	out := blobstore.NewOutputBuffer()
	if err := st.GetBlob(ctx, id, 0, -1, out); err != nil {
		t.Fatalf("GetBlob whole: %v", err)
	}
	if got := out.Bytes(); !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, payload)
	}
}

func TestGetBlobRanged(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("pack-tenant-a-cafef00d")
	payload := []byte("0123456789abcdef")

	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	out := blobstore.NewOutputBuffer()
	// bytes [4, 4+6) == "456789"
	if err := st.GetBlob(ctx, id, 4, 6, out); err != nil {
		t.Fatalf("GetBlob ranged: %v", err)
	}
	if got, want := out.Bytes(), payload[4:10]; !bytes.Equal(got, want) {
		t.Fatalf("ranged mismatch: got %q want %q", got, want)
	}
}

func TestGetBlobNegativeOffset(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	out := blobstore.NewOutputBuffer()
	err := st.GetBlob(context.Background(), "pack-tenant-a-x", -1, 4, out)
	if !errors.Is(err, kblob.ErrInvalidRange) {
		t.Fatalf("negative offset: got %v, want ErrInvalidRange", err)
	}
}

func TestGetBlobZeroLength(t *testing.T) {
	// length==0 is an invalid range per the kopia contract: the whole-blob path
	// requires length<0 and the ranged path requires length>0. A zero-length
	// ranged GET would produce a degenerate HTTP Range header (bytes=offset-offset)
	// which may return HTTP 416. The guard must reject it before issuing any
	// presign or S3 request.
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	// Use a fakeRequester that fails loudly so we can detect if any presign
	// call leaks through the guard.
	fr := &fakeRequester{
		baseURL: srv.URL,
		codec:   ctlproto.DefaultCodec,
		failErr: errors.New("presign should not be called for zero-length GetBlob"),
	}
	st := newStore(t, srv, fr)

	out := blobstore.NewOutputBuffer()
	err := st.GetBlob(context.Background(), "pack-tenant-a-x", 0, 0, out)
	if !errors.Is(err, kblob.ErrInvalidRange) {
		t.Fatalf("zero length: got %v, want ErrInvalidRange", err)
	}
	// If the presign layer were reached, the error message would contain
	// "presign should not be called". Ensure it does not.
	if err != nil && strings.Contains(err.Error(), "presign should not be called") {
		t.Fatalf("zero length: guard did not fire before presign/S3 call: %v", err)
	}
}

func TestPutBlobDoNotRecreateIgnored(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("chunk-tenant-a-aaaa")
	payload := []byte("idempotent")

	// First put.
	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{DoNotRecreate: true}); err != nil {
		t.Fatalf("PutBlob #1: %v", err)
	}
	// Re-put same id with DoNotRecreate=true must succeed (no ErrBlobAlreadyExists).
	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{DoNotRecreate: true}); err != nil {
		t.Fatalf("PutBlob #2 (DoNotRecreate ignored): %v", err)
	}
	if errors.Is(nil, blobstore.ErrBlobAlreadyExists) {
		t.Fatal("unreachable")
	}

	// Bytes still readable and correct.
	out := blobstore.NewOutputBuffer()
	if err := st.GetBlob(ctx, id, 0, -1, out); err != nil {
		t.Fatalf("GetBlob after re-put: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("after re-put: got %q want %q", out.Bytes(), payload)
	}
}

func TestUnsupportedOperations(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)
	ctx := context.Background()

	if _, err := st.GetMetadata(ctx, "pack-tenant-a-x"); !errors.Is(err, remoteblob.ErrNotSupported) {
		t.Errorf("GetMetadata: got %v, want ErrNotSupported", err)
	}
	if err := st.ListBlobs(ctx, "", func(blobstore.Metadata) error { return nil }); !errors.Is(err, remoteblob.ErrNotSupported) {
		t.Errorf("ListBlobs: got %v, want ErrNotSupported", err)
	}
	if err := st.DeleteBlob(ctx, "pack-tenant-a-x"); !errors.Is(err, remoteblob.ErrNotSupported) {
		t.Errorf("DeleteBlob: got %v, want ErrNotSupported", err)
	}
}

func TestPresignLayerErrorPropagates(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{
		baseURL: srv.URL,
		codec:   ctlproto.DefaultCodec,
		failErr: errors.New("nats: no responders"),
	}
	st := newStore(t, srv, fr)
	ctx := context.Background()

	err := st.PutBlob(ctx, "pack-tenant-a-x", blobstore.BytesFromSlice([]byte("x")), blobstore.PutOptions{})
	if err == nil || !strings.Contains(err.Error(), "no responders") {
		t.Fatalf("PutBlob presign error: got %v, want propagated transport error", err)
	}

	out := blobstore.NewOutputBuffer()
	err = st.GetBlob(ctx, "pack-tenant-a-x", 0, -1, out)
	if err == nil || !strings.Contains(err.Error(), "no responders") {
		t.Fatalf("GetBlob presign error: got %v, want propagated transport error", err)
	}
}

func TestS3NonSuccessPropagates(t *testing.T) {
	// Empty fake S3: GET of an absent key returns 404, which s3http surfaces as
	// a *StatusError that GetBlob must propagate.
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	out := blobstore.NewOutputBuffer()
	err := st.GetBlob(context.Background(), "pack-tenant-a-missing", 0, -1, out)
	if err == nil {
		t.Fatal("GetBlob of absent object: want error, got nil")
	}
	var se *s3http.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound {
		t.Fatalf("GetBlob 404: got %v, want StatusError 404", err)
	}
}

func TestKeyMappingStableDefault(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)
	ctx := context.Background()

	id := blobstore.ID("pack-tenant-a-stablekey")
	for i := 0; i < 3; i++ {
		if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice([]byte("v")), blobstore.PutOptions{}); err != nil {
			t.Fatalf("PutBlob #%d: %v", i, err)
		}
	}

	s3.mu.Lock()
	keys := append([]string(nil), s3.putKeys...)
	s3.mu.Unlock()

	if len(keys) != 3 {
		t.Fatalf("expected 3 PUTs, got %d", len(keys))
	}
	for _, k := range keys {
		// Default keyFn maps the ID verbatim to the object key.
		if k != string(id) {
			t.Fatalf("unstable/unexpected key: got %q want %q", k, string(id))
		}
	}
}

func TestWithKeyFuncCustomMapping(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr, remoteblob.WithKeyFunc(func(id blobstore.ID) string {
		return "blobs/" + string(id)
	}))
	ctx := context.Background()

	id := blobstore.ID("pack-tenant-a-prefixed")
	payload := []byte("prefixed-bytes")
	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	s3.mu.Lock()
	_, ok := s3.objects["blobs/"+string(id)]
	s3.mu.Unlock()
	if !ok {
		t.Fatalf("custom keyFn not applied; objects=%v", s3.objects)
	}

	// And it round-trips through the same mapping.
	out := blobstore.NewOutputBuffer()
	if err := st.GetBlob(ctx, id, 0, -1, out); err != nil {
		t.Fatalf("GetBlob custom key: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("custom key round-trip: got %q want %q", out.Bytes(), payload)
	}
}

func TestDisplayNameAndConnectionInfo(t *testing.T) {
	st := remoteblob.NewBlobStore(remotepresign.NewClient(&fakeRequester{codec: ctlproto.DefaultCodec}), s3http.NewClient(), testTenant)
	if got, want := st.DisplayName(), "presigned-s3:"+testTenant; got != want {
		t.Errorf("DisplayName: got %q want %q", got, want)
	}
	if got := st.ConnectionInfo(); got.Type != "presigned-s3" {
		t.Errorf("ConnectionInfo.Type: got %q want %q", got.Type, "presigned-s3")
	}
}

// TestGetBlobPresignURLCached verifies the per-pack presigned-URL cache: many
// reads of the same blob (whole + ranged at different offsets) presign exactly
// once. This is the per-slice round-trip removed on the cold-read path — a pack
// holds many slices' chunks, and each used to re-presign the same blob.
func TestGetBlobPresignURLCached(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("pack-tenant-a-cacheme")
	payload := []byte("0123456789abcdefghij")
	if err := st.PutBlob(ctx, id, blobstore.BytesFromSlice(payload), blobstore.PutOptions{}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	putPresigns := fr.reqCount.Load() // the PUT presign (not cached)

	out := blobstore.NewOutputBuffer()
	for i := 0; i < 5; i++ {
		if err := st.GetBlob(ctx, id, int64(i), 3, out); err != nil {
			t.Fatalf("GetBlob ranged #%d: %v", i, err)
		}
		if got, want := out.Bytes(), payload[i:i+3]; !bytes.Equal(got, want) {
			t.Fatalf("ranged #%d mismatch: got %q want %q", i, got, want)
		}
	}
	if err := st.GetBlob(ctx, id, 0, -1, out); err != nil { // whole-blob too
		t.Fatalf("GetBlob whole: %v", err)
	}

	if gets := fr.reqCount.Load() - putPresigns; gets != 1 {
		t.Fatalf("6 reads of one blob presigned %d times, want 1 (cache miss only on the first)", gets)
	}
}

// TestPresignCacheInvalidatedOnTransferError verifies that a failed transfer
// drops the cached URL so the next read re-presigns rather than reusing a dead
// (expired/revoked) URL. Two GETs of a missing object (S3 404) → two presigns.
func TestPresignCacheInvalidatedOnTransferError(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	fr := &fakeRequester{baseURL: srv.URL, codec: ctlproto.DefaultCodec}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("pack-tenant-a-missing") // never PUT → S3 returns 404
	out := blobstore.NewOutputBuffer()
	for i := 0; i < 2; i++ {
		if err := st.GetBlob(ctx, id, 0, -1, out); err == nil {
			t.Fatalf("GetBlob #%d of a missing object should fail", i)
		}
	}
	if n := fr.reqCount.Load(); n != 2 {
		t.Fatalf("two failed reads presigned %d times, want 2 (each invalidated the cache)", n)
	}
}

// TestPresignSingleflightCollapsesHerd verifies that concurrent first-touch reads
// of the same blob (the prefetcher fires many same-pack reads at once) collapse
// into ONE presign via singleflight, not one per goroutine. A barrier holds the
// leader's presign until all callers have joined the in-flight call.
func TestPresignSingleflightCollapsesHerd(t *testing.T) {
	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	defer srv.Close()

	// Barrier is configured ONCE and never mutated after the goroutines launch.
	fr := &fakeRequester{
		baseURL: srv.URL, codec: ctlproto.DefaultCodec,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	st := newStore(t, srv, fr)

	ctx := context.Background()
	id := blobstore.ID("pack-tenant-a-herd")
	// Seed the object directly so GETs succeed without a PUT presign (the default
	// keyFn maps the id verbatim to the S3 object key / URL path).
	s3.objects[string(id)] = []byte("herd-collapse-payload")

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := blobstore.NewOutputBuffer()
			errs[i] = st.GetBlob(ctx, id, 0, -1, out)
		}(i)
	}
	<-fr.entered                      // one presign leader has entered
	time.Sleep(50 * time.Millisecond) // give the other 15 time to join the singleflight call
	beforeRelease := fr.reqCount.Load()
	close(fr.release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent GetBlob #%d: %v", i, err)
		}
	}
	// Exactly one presign for the whole herd (singleflight collapsed it).
	if beforeRelease != 1 {
		t.Fatalf("herd presigned %d times before release, want 1 (collapsed)", beforeRelease)
	}
	if total := fr.reqCount.Load(); total != 1 {
		t.Fatalf("total presigns = %d, want 1 (singleflight collapsed the GET herd)", total)
	}
}
