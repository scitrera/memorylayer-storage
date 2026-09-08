// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package s3http is the node-side HTTP transfer primitive for the direct-S3
// data plane described in ADR-001 §2.2 and §5.
//
// Presigned URLs carry their own authentication (SigV4 query parameters), so
// no AWS SDK or S3 credentials are needed on the node. This package wraps
// net/http only.
//
// # Ranged reads (#18)
//
// GetRange implements the ranged-read primitive for ADR-001 §5 ("#18 readahead
// / ranged reads — node-side parallel presigned ranged GETs replacing whole-
// slice sequential reads (chunkstore.go:94-99)"). Callers should issue ranged
// GETs in parallel goroutines over a shared Client to saturate S3 throughput.
//
// # URL confidentiality
//
// Presigned URLs embed a signature in the query string. The Client never logs
// full URLs at any level; if debug logging is added in the future, redact the
// query string (url.URL.Path only) before writing to any log sink.
//
// # Retries
//
// GET and HEAD are idempotent and are retried on transient 5xx or network
// errors up to Client.maxRetries times (default 3). PUT is NOT auto-retried
// (non-idempotent: an S3 PUT that returns a network error after the bytes were
// received would duplicate the object). Callers that know their PUT is safe to
// retry (e.g. content-addressed packs whose key is deterministic) may wrap Put
// in their own retry loop.
package s3http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// defaultTransport is a *http.Transport tuned for parallel fanout to S3:
// multiple concurrent ranged GETs per host benefit from a higher per-host idle
// connection pool (S3 endpoints are per-bucket, so MaxIdleConnsPerHost is the
// operative knob). ResponseHeaderTimeout bounds a hung connection independently
// of the overall context deadline.
var defaultTransport = &http.Transport{
	MaxIdleConns:        128,
	MaxIdleConnsPerHost: 64,
	IdleConnTimeout:     90 * time.Second,
	// Bound time waiting for first response byte; the overall duration is
	// governed by the caller's context.
	ResponseHeaderTimeout: 30 * time.Second,
	// Enable HTTP/2 for compatible servers; S3 uses HTTP/1.1 in practice but
	// this is harmless for compliant transports.
	ForceAttemptHTTP2: true,
}

// defaultHTTPClient is the Client's default http.Client.
var defaultHTTPClient = &http.Client{
	Transport: defaultTransport,
	// No global Timeout here: per-request timeouts are driven by the caller's
	// context.Context, which is more composable than a fixed client-level timeout.
}

// StatusError is returned when the server responds with a non-success HTTP
// status. It carries the status code, the URL path (query string redacted),
// and a snippet of the response body for diagnostics.
//
// Use errors.As to unwrap from a wrapping error:
//
//	var se *s3http.StatusError
//	if errors.As(err, &se) && se.Code == 403 { /* expired presign */ }
type StatusError struct {
	// Code is the HTTP status code.
	Code int
	// URLPath is the request path with the query string redacted to avoid
	// logging presigned URL signatures.
	URLPath string
	// Snippet is up to snippetMax bytes of the response body, trimmed of
	// leading/trailing whitespace. S3 error bodies are XML; the snippet is
	// for human diagnostics only.
	Snippet string
}

func (e *StatusError) Error() string {
	if e.Snippet != "" {
		return fmt.Sprintf("s3http: %s %s: %s", http.StatusText(e.Code), e.URLPath, e.Snippet)
	}
	return fmt.Sprintf("s3http: %s %s", http.StatusText(e.Code), e.URLPath)
}

// snippetMax is the maximum body bytes read for error diagnostics.
const snippetMax = 512

// redactURL returns only the path component of a URL string, discarding the
// query string that carries presign signature parameters.
func redactURL(rawURL string) string {
	// Fast path: find "?" without a full url.Parse allocation.
	if i := strings.IndexByte(rawURL, '?'); i >= 0 {
		rawURL = rawURL[:i]
	}
	return rawURL
}

// readSnippet reads up to snippetMax bytes from r and returns them as a
// trimmed string. It never fails the caller: errors and EOF are both treated
// as empty.
func readSnippet(r io.Reader) string {
	buf := make([]byte, snippetMax)
	n, _ := io.ReadFull(r, buf)
	return strings.TrimSpace(string(buf[:n]))
}

// statusError constructs a *StatusError from a response. It drains and closes
// the response body.
func statusError(resp *http.Response) *StatusError {
	snippet := readSnippet(resp.Body)
	resp.Body.Close()
	return &StatusError{
		Code:    resp.StatusCode,
		URLPath: redactURL(resp.Request.URL.String()),
		Snippet: snippet,
	}
}

// is2xx reports whether code is in the [200, 299] range.
func is2xx(code int) bool { return code >= 200 && code < 300 }

// Client performs presigned-URL S3 HTTP transfers. It is safe for concurrent
// use by multiple goroutines. The zero value is not usable; create one with
// NewClient.
type Client struct {
	hc         *http.Client
	maxRetries int
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the default http.Client. Use this to inject a
// custom Transport (e.g. for testing with httptest or custom TLS config).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// WithMaxRetries sets the number of additional attempts for idempotent
// requests (GET, HEAD) on transient errors. 0 means no retries (one attempt
// total). Default is 3.
func WithMaxRetries(n int) Option {
	return func(c *Client) { c.maxRetries = n }
}

// NewClient constructs a Client with the given options. Without any options,
// the Client uses a transport tuned for parallel S3 fanout and retries GET/HEAD
// up to 3 times on transient errors.
func NewClient(opts ...Option) *Client {
	c := &Client{
		hc:         defaultHTTPClient,
		maxRetries: 3,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// isTransient reports whether a status code warrants a retry. Only 5xx server
// errors are retried; 4xx (including 403 expired presign, 404 not found) are
// not — they require caller action (e.g. refresh the presigned URL).
func isTransient(code int) bool { return code >= 500 }

// doGet executes a GET (or other idempotent method) with retry logic. On
// success it returns the *http.Response with the body open; the caller is
// responsible for closing it.
func (c *Client) doGet(ctx context.Context, req *http.Request) (*http.Response, error) {
	var lastErr error
	attempts := c.maxRetries + 1
	for i := range attempts {
		if i > 0 {
			// Brief back-off between retries: check context first.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		resp, err := c.hc.Do(req.Clone(ctx))
		if err != nil {
			// Network/transport error — retry.
			lastErr = err
			continue
		}
		if is2xx(resp.StatusCode) {
			return resp, nil
		}
		if isTransient(resp.StatusCode) {
			// Capture status before draining; drain and close exactly once so
			// the connection can be reused, then retry.
			code := resp.StatusCode
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
			lastErr = &StatusError{
				Code:    code,
				URLPath: redactURL(req.URL.String()),
			}
			continue
		}
		// Non-transient non-2xx: return immediately.
		return nil, statusError(resp)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("s3http: doGet: no attempts made")
}

// Get fetches the full object at the presigned URL and returns its bytes.
// Non-2xx responses are returned as *StatusError.
func (c *Client) Get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("s3http: Get: build request: %w", err)
	}
	resp, err := c.doGet(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("s3http: Get %s: %w", redactURL(url), err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("s3http: Get %s: read body: %w", redactURL(url), err)
	}
	return data, nil
}

// GetRange fetches bytes [offset, offset+length) of the object at the presigned
// URL using the HTTP Range header.
//
// Servers that honour the Range header return 206 Partial Content with exactly
// the requested bytes. Servers that ignore the Range header return 200 OK with
// the full object; GetRange handles this transparently by slicing the full body
// to [offset, offset+length) as a fallback (the full body is still downloaded,
// so callers should prefer servers that support ranged responses for large
// objects).
//
// This is the #18 ranged-read primitive (ADR-001 §5).
func (c *Client) GetRange(ctx context.Context, url string, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("s3http: GetRange: negative offset %d", offset)
	}
	if length <= 0 {
		return nil, fmt.Errorf("s3http: GetRange: non-positive length %d", length)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("s3http: GetRange: build request: %w", err)
	}
	// RFC 9110 §14.1.2: "bytes=<first>-<last>" (inclusive range).
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := c.doGet(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("s3http: GetRange %s [%d+%d]: %w", redactURL(url), offset, length, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent: // 206 — server honoured Range
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("s3http: GetRange %s: read 206 body: %w", redactURL(url), err)
		}
		if int64(len(data)) != length {
			return nil, fmt.Errorf("s3http: GetRange %s: got %d bytes, want %d", redactURL(url), len(data), length)
		}
		return data, nil

	case http.StatusOK: // 200 — server ignored Range; slice locally
		// Note: we download the entire object. For large objects this is wasteful.
		// This fallback exists for compatibility with servers that do not support
		// ranged responses (e.g. some MinIO configurations, local stubs).
		full, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("s3http: GetRange %s: read 200 fallback body: %w", redactURL(url), err)
		}
		if offset >= int64(len(full)) {
			return nil, fmt.Errorf("s3http: GetRange %s: offset %d beyond object length %d", redactURL(url), offset, len(full))
		}
		end := offset + length
		if end > int64(len(full)) {
			// Body is shorter than the requested range; mirror the 206 length-
			// mismatch error rather than silently returning fewer bytes.
			return nil, fmt.Errorf("s3http: GetRange %s: 200 fallback body length %d too short for offset %d + length %d", redactURL(url), len(full), offset, length)
		}
		return full[offset:end], nil

	default:
		// doGet only returns non-2xx non-transient errors, but after retry
		// exhaustion doGet can also return a transient status as a StatusError.
		// This branch handles the (unexpected) case where doGet returns a
		// success-range code that is neither 200 nor 206.
		// Drain before returning so the connection can be returned to the pool.
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil, &StatusError{
			Code:    resp.StatusCode,
			URLPath: redactURL(url),
		}
	}
}

// Put uploads body to the presigned URL using HTTP PUT. size must be the
// exact byte count of body; S3 presigned PUTs require Content-Length.
//
// PUT is NOT auto-retried. If the caller's body is content-addressed and
// therefore safe to re-send, wrap Put in an external retry loop. See
// package-level doc for rationale.
//
// Non-2xx responses are returned as *StatusError with a snippet of the S3
// XML error body for diagnostics.
func (c *Client) Put(ctx context.Context, url string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return fmt.Errorf("s3http: Put: build request: %w", err)
	}
	req.ContentLength = size

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("s3http: Put %s: %w", redactURL(url), err)
	}
	if is2xx(resp.StatusCode) {
		// Drain and close to return the connection to the pool.
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		return nil
	}
	return fmt.Errorf("s3http: Put %s: %w", redactURL(url), statusError(resp))
}

// Head fetches the Content-Length of the object at the presigned URL.
// Non-2xx responses are returned as *StatusError.
func (c *Client) Head(ctx context.Context, url string) (size int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, fmt.Errorf("s3http: Head: build request: %w", err)
	}
	resp, err := c.doGet(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("s3http: Head %s: %w", redactURL(url), err)
	}
	resp.Body.Close()
	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("s3http: Head %s: server did not return Content-Length", redactURL(url))
	}
	return resp.ContentLength, nil
}
