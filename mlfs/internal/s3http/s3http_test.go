// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3http_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"
)

// newClient returns a Client wired to srv's URL. WithHTTPClient overrides the
// default transport so requests go to the test server.
func newClient(srv *httptest.Server, opts ...s3http.Option) (*s3http.Client, string) {
	opts = append([]s3http.Option{s3http.WithHTTPClient(srv.Client())}, opts...)
	return s3http.NewClient(opts...), srv.URL
}

// ----- Get -----

func TestGet_200(t *testing.T) {
	want := []byte("hello, s3")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(want) //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv)
	got, err := c.Get(context.Background(), base+"/object")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get: got %q, want %q", got, want)
	}
}

func TestGet_NonSuccess_StatusError(t *testing.T) {
	for _, code := range []int{403, 404, 400} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				io.WriteString(w, "error body") //nolint:errcheck
			}))
			defer srv.Close()

			c, base := newClient(srv, s3http.WithMaxRetries(0))
			_, err := c.Get(context.Background(), base+"/object")
			if err == nil {
				t.Fatal("Get: expected error, got nil")
			}
			var se *s3http.StatusError
			if !errors.As(err, &se) {
				t.Fatalf("Get: error is %T (%v), want *s3http.StatusError", err, err)
			}
			if se.Code != code {
				t.Fatalf("StatusError.Code = %d, want %d", se.Code, code)
			}
		})
	}
}

// ----- GetRange -----

func TestGetRange_206(t *testing.T) {
	full := []byte("0123456789abcdef")
	offset, length := int64(4), int64(6)
	want := full[offset : offset+length]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		wantHdr := "bytes=4-9"
		if rangeHdr != wantHdr {
			http.Error(w, "bad Range: "+rangeHdr, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		w.Write(full[offset : offset+length]) //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv)
	got, err := c.GetRange(context.Background(), base+"/object", offset, length)
	if err != nil {
		t.Fatalf("GetRange: unexpected error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("GetRange: got %q, want %q", got, want)
	}
}

func TestGetRange_200Fallback(t *testing.T) {
	// Server ignores the Range header and returns 200 with the full body.
	// Client must slice to [offset, offset+length).
	full := []byte("0123456789abcdef")
	offset, length := int64(3), int64(5)
	want := full[offset : offset+length]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Deliberately ignore Range; return full body.
		w.WriteHeader(http.StatusOK)
		w.Write(full) //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv)
	got, err := c.GetRange(context.Background(), base+"/object", offset, length)
	if err != nil {
		t.Fatalf("GetRange 200 fallback: unexpected error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("GetRange 200 fallback: got %q, want %q", got, want)
	}
}

func TestGetRange_RangeHeaderFormat(t *testing.T) {
	// Verifies the exact Range header sent for various offset/length pairs.
	cases := []struct {
		offset, length int64
		wantHeader     string
	}{
		{0, 1, "bytes=0-0"},
		{0, 10, "bytes=0-9"},
		{5, 1, "bytes=5-5"},
		{100, 50, "bytes=100-149"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.wantHeader, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got := r.Header.Get("Range")
				if got != tc.wantHeader {
					http.Error(w, "wrong Range: "+got, http.StatusBadRequest)
					return
				}
				body := bytes.Repeat([]byte("x"), int(tc.length))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(body) //nolint:errcheck
			}))
			defer srv.Close()
			c, base := newClient(srv)
			_, err := c.GetRange(context.Background(), base+"/o", tc.offset, tc.length)
			if err != nil {
				t.Fatalf("GetRange %v: %v", tc.wantHeader, err)
			}
		})
	}
}

// ----- Put -----

func TestPut_200(t *testing.T) {
	wantBody := []byte("pack-object-bytes")
	var gotBody []byte
	var gotContentLength int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "not PUT", http.StatusMethodNotAllowed)
			return
		}
		gotContentLength = r.ContentLength
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, base := newClient(srv)
	err := c.Put(context.Background(), base+"/object", bytes.NewReader(wantBody), int64(len(wantBody)))
	if err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}
	if !bytes.Equal(gotBody, wantBody) {
		t.Fatalf("Put body: got %q, want %q", gotBody, wantBody)
	}
	if gotContentLength != int64(len(wantBody)) {
		t.Fatalf("Put Content-Length: got %d, want %d", gotContentLength, len(wantBody))
	}
}

func TestPut_NonSuccess_StatusError(t *testing.T) {
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code></Error>`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, xmlBody) //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv)
	err := c.Put(context.Background(), base+"/object", strings.NewReader("data"), 4)
	if err == nil {
		t.Fatal("Put: expected error, got nil")
	}
	var se *s3http.StatusError
	if !errors.As(err, &se) {
		t.Fatalf("Put error is %T (%v), want *s3http.StatusError", err, err)
	}
	if se.Code != http.StatusForbidden {
		t.Fatalf("StatusError.Code = %d, want 403", se.Code)
	}
	if !strings.Contains(se.Snippet, "AccessDenied") {
		t.Fatalf("StatusError.Snippet %q does not contain 'AccessDenied'", se.Snippet)
	}
}

func TestPut_NotAutoRetried(t *testing.T) {
	// A PUT that returns 500 must NOT be retried; the server must see exactly
	// one request (unlike GET which retries up to maxRetries times).
	var callCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, base := newClient(srv, s3http.WithMaxRetries(3))
	_ = c.Put(context.Background(), base+"/object", strings.NewReader("data"), 4)

	if n := atomic.LoadInt32(&callCount); n != 1 {
		t.Fatalf("Put retried: server saw %d calls, want exactly 1", n)
	}
}

// ----- ctx cancellation -----

func TestGet_CtxCancel(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		// Hold the response until the client cancels.
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c, base := newClient(srv, s3http.WithMaxRetries(0))

	done := make(chan error, 1)
	go func() {
		_, err := c.Get(ctx, base+"/slow")
		done <- err
	}()

	<-started
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Get with cancelled ctx: expected error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Get did not respect ctx cancellation within 3s")
	}
}

func TestPut_CtxCancel(t *testing.T) {
	// The server receives the request and hangs before sending a response.
	// Cancelling the context must cause Put to return an error promptly.
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body so the client's upload completes and it begins
		// waiting for the response — that is the operation the ctx must abort.
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		started <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c, base := newClient(srv)

	body := bytes.Repeat([]byte("x"), 64)
	done := make(chan error, 1)
	go func() {
		done <- c.Put(ctx, base+"/slow", bytes.NewReader(body), int64(len(body)))
	}()

	// Wait until the server has drained the body and is blocking on response.
	<-started
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Put with cancelled ctx: expected error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put did not respect ctx cancellation within 3s")
	}
}

// ----- Retry -----

func TestGet_Retry_SucceedsAfterTransient(t *testing.T) {
	// First call returns 500; second returns 200. With MaxRetries=3 the client
	// must succeed on the second attempt.
	var callCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok") //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv, s3http.WithMaxRetries(3))
	got, err := c.Get(context.Background(), base+"/object")
	if err != nil {
		t.Fatalf("Get retry: unexpected error: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("Get retry: got %q, want %q", got, "ok")
	}
	if n := atomic.LoadInt32(&callCount); n != 2 {
		t.Fatalf("Get retry: expected 2 server calls, got %d", n)
	}
}

func TestGet_Retry_ExhaustedReturnsError(t *testing.T) {
	// Server always 500; retry exhaustion must return an error.
	var callCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	maxRetries := 2
	c, base := newClient(srv, s3http.WithMaxRetries(maxRetries))
	_, err := c.Get(context.Background(), base+"/object")
	if err == nil {
		t.Fatal("Get exhausted retries: expected error, got nil")
	}
	wantCalls := int32(maxRetries + 1)
	if n := atomic.LoadInt32(&callCount); n != wantCalls {
		t.Fatalf("Get retries: expected %d server calls, got %d", wantCalls, n)
	}
}

// countCloser wraps an io.ReadCloser and counts how many times Close is called.
type countCloser struct {
	io.ReadCloser
	count atomic.Int32
}

func (c *countCloser) Close() error {
	c.count.Add(1)
	return c.ReadCloser.Close()
}

// wrapTransport wraps a base RoundTripper, calling wrap on each response.
type wrapTransport struct {
	base http.RoundTripper
	wrap func(*http.Response) *http.Response
}

func (rt *wrapTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return rt.wrap(resp), nil
}

func TestGet_Retry_BodyClosedExactlyOnce(t *testing.T) {
	// On the transient-retry path the response body must be closed exactly once
	// per response — never double-closed. We use countCloser to verify.
	var callCount int32
	// closers holds the countCloser for each HTTP response, indexed by call order.
	closers := make([]*countCloser, 2)
	var closersMu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok") //nolint:errcheck
	}))
	defer srv.Close()

	respIdx := int32(-1)
	transport := &wrapTransport{
		base: srv.Client().Transport,
		wrap: func(resp *http.Response) *http.Response {
			idx := int(atomic.AddInt32(&respIdx, 1))
			cc := &countCloser{ReadCloser: resp.Body}
			resp.Body = cc
			closersMu.Lock()
			if idx < len(closers) {
				closers[idx] = cc
			}
			closersMu.Unlock()
			return resp
		},
	}
	hc := &http.Client{Transport: transport}
	c := s3http.NewClient(s3http.WithHTTPClient(hc), s3http.WithMaxRetries(3))

	got, err := c.Get(context.Background(), srv.URL+"/object")
	if err != nil {
		t.Fatalf("Get retry: unexpected error: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("Get retry: got %q, want %q", got, "ok")
	}

	// Body 0 (the 500 response) must be closed exactly once — no double-close.
	closersMu.Lock()
	cc0 := closers[0]
	closersMu.Unlock()
	if cc0 == nil {
		t.Fatal("countCloser for first response was not captured")
	}
	if n := cc0.count.Load(); n != 1 {
		t.Errorf("transient body Close called %d times, want exactly 1", n)
	}
}

func TestGetRange_200Fallback_ShortBody_Error(t *testing.T) {
	// Server ignores Range and returns 200 with a body shorter than
	// offset+length. The client must return an error, not a silent short slice.
	body := []byte("short") // only 5 bytes
	offset, length := int64(0), int64(10)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(body) //nolint:errcheck
	}))
	defer srv.Close()

	c, base := newClient(srv)
	got, err := c.GetRange(context.Background(), base+"/object", offset, length)
	if err == nil {
		t.Fatalf("GetRange 200 short body: expected error, got slice of len %d", len(got))
	}
	if got != nil {
		t.Errorf("GetRange 200 short body: expected nil data on error, got %q", got)
	}
}

// ----- Head -----

func TestHead_ReturnsContentLength(t *testing.T) {
	const wantSize = int64(12345)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			http.Error(w, "not HEAD", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Length", "12345")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, base := newClient(srv)
	got, err := c.Head(context.Background(), base+"/object")
	if err != nil {
		t.Fatalf("Head: unexpected error: %v", err)
	}
	if got != wantSize {
		t.Fatalf("Head: got size %d, want %d", got, wantSize)
	}
}

// ----- StatusError -----

func TestStatusError_Is(t *testing.T) {
	// errors.As must unwrap through a wrapping error.
	inner := &s3http.StatusError{Code: 403, URLPath: "/obj", Snippet: "Forbidden"}
	wrapped := &wrappedErr{inner: inner}

	var se *s3http.StatusError
	if !errors.As(wrapped, &se) {
		t.Fatal("errors.As: failed to unwrap StatusError through wrapping error")
	}
	if se.Code != 403 {
		t.Fatalf("unwrapped StatusError.Code = %d, want 403", se.Code)
	}
}

type wrappedErr struct{ inner error }

func (e *wrappedErr) Error() string { return "wrapped: " + e.inner.Error() }
func (e *wrappedErr) Unwrap() error { return e.inner }

// ----- URL redaction -----

func TestURLRedaction_NoQueryInError(t *testing.T) {
	// A presigned URL with a query string must not appear in error messages.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	c, base := newClient(srv, s3http.WithMaxRetries(0))
	_, err := c.Get(context.Background(), base+"/object?X-Amz-Signature=supersecret")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("error message leaks query string (signature): %v", err)
	}
	if strings.Contains(err.Error(), "X-Amz-Signature") {
		t.Fatalf("error message leaks query param name: %v", err)
	}
}
