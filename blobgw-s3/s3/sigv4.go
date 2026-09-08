// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// SigV4 (AWS Signature Version 4) verification for the header-based
// Authorization form used by aws-sdk-go-v2 and the AWS CLI.
//
// The flow mirrors the AWS spec:
//  1. rebuild the canonical request from the wire request,
//  2. derive the string-to-sign from its hash plus the scope,
//  3. derive the signing key from the secret + scope,
//  4. HMAC-SHA256 the string-to-sign with that key and compare.
//
// Three payload modes are recognized via the x-amz-content-sha256 header:
//   - UNSIGNED-PAYLOAD: body excluded from the seed signature.
//   - a hex digest: single-chunk signed payload (body hashed whole).
//   - STREAMING-AWS4-HMAC-SHA256-PAYLOAD: aws-chunked body whose seed signature
//     covers the headers only; each chunk carries its own chained signature
//     (verified while decoding — see chunkReader).
//
// Only signature verification lives here; the handler owns body reading so the
// streaming decoder can wrap the request body in-line.

const (
	algorithm           = "AWS4-HMAC-SHA256"
	unsignedPayload     = "UNSIGNED-PAYLOAD"
	streamingPayload    = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingChunkAlgo  = "AWS4-HMAC-SHA256-PAYLOAD"
	emptyStringSHA256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	amzDateHeader       = "X-Amz-Date"
	amzContentSHA       = "X-Amz-Content-Sha256"
	authorizationHeader = "Authorization"
	amzDateFormat       = "20060102T150405Z" // X-Amz-Date wire format (ISO 8601 basic)
)

// maxClockSkew bounds how far a signed request's X-Amz-Date may diverge from the
// server clock before it is rejected as RequestTimeTooSkewed. This is the SigV4
// replay defense: a captured request can only be replayed within this window.
// 15 minutes matches AWS S3's own tolerance.
const maxClockSkew = 15 * time.Minute

// checkRequestTime enforces SigV4 freshness: the X-Amz-Date header (which is part
// of the signed string-to-sign, so it cannot be altered without invalidating the
// signature) must be within maxClockSkew of now. A missing or unparseable date —
// or one outside the window — is rejected so a captured signature cannot be
// replayed indefinitely. now is the server clock (injectable for tests).
func checkRequestTime(amzDate string, now time.Time) error {
	if amzDate == "" {
		return errMissingSecurityHeader
	}
	t, err := time.Parse(amzDateFormat, amzDate)
	if err != nil {
		return errAuthorizationHeaderForm
	}
	skew := now.Sub(t)
	if skew < 0 {
		skew = -skew
	}
	if skew > maxClockSkew {
		return errRequestTimeTooSkewed
	}
	return nil
}

// authScope is the parsed Authorization header: the credential components and
// the signed-header list plus the client-provided signature.
type authScope struct {
	accessKeyID   string
	date          string // yyyymmdd
	region        string
	service       string // "s3"
	signedHeaders []string
	signature     string // hex, as sent by the client
}

// credentialScope returns the "date/region/service/aws4_request" string used in
// both the string-to-sign and the signing key derivation.
func (s authScope) credentialScope() string {
	return strings.Join([]string{s.date, s.region, s.service, "aws4_request"}, "/")
}

// parseAuthorization parses an "AWS4-HMAC-SHA256 Credential=.../...,
// SignedHeaders=...;..., Signature=..." header. A malformed header is reported
// so the handler can answer AuthorizationHeaderMalformed (or
// MissingSecurityHeader for an absent one).
func parseAuthorization(header string) (authScope, error) {
	var s authScope
	if header == "" {
		return s, errMissingSecurityHeader
	}
	rest, ok := strings.CutPrefix(header, algorithm+" ")
	if !ok {
		return s, errAuthorizationHeaderForm
	}
	var cred, signed, sig string
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			cred = strings.TrimPrefix(part, "Credential=")
		case strings.HasPrefix(part, "SignedHeaders="):
			signed = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			sig = strings.TrimPrefix(part, "Signature=")
		}
	}
	if cred == "" || signed == "" || sig == "" {
		return s, errAuthorizationHeaderForm
	}
	// Credential = accessKeyID/date/region/service/aws4_request
	cp := strings.Split(cred, "/")
	if len(cp) != 5 || cp[4] != "aws4_request" {
		return s, errAuthorizationHeaderForm
	}
	s.accessKeyID = cp[0]
	s.date = cp[1]
	s.region = cp[2]
	s.service = cp[3]
	s.signedHeaders = strings.Split(signed, ";")
	s.signature = sig
	return s, nil
}

// verifySeedSignature recomputes the request's seed signature from secretKey and
// compares it (constant-time) to the client's. payloadHash is the value used in
// the canonical request: a content hash, UNSIGNED-PAYLOAD, or the streaming
// sentinel. It returns the seed signature (needed to chain streaming chunk
// signatures) on success.
func verifySeedSignature(r *http.Request, scope authScope, secretKey, payloadHash string) (string, error) {
	canonReq := canonicalRequest(r, scope.signedHeaders, payloadHash)
	sts := stringToSign(r.Header.Get(amzDateHeader), scope.credentialScope(), canonReq)
	key := signingKey(secretKey, scope.date, scope.region, scope.service)
	want := hmacHex(key, sts)
	if !hmac.Equal([]byte(want), []byte(scope.signature)) {
		return "", errSignatureDoesNotMatch
	}
	return want, nil
}

// canonicalRequest builds the SigV4 canonical request string. Path-style
// addressing means the canonical URI is the (encoded) request path; aws-sdk-go-v2
// sends the already-encoded path, so we re-encode each segment the same way S3
// does to match the client's canonicalization.
func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r.URL))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL))
	b.WriteByte('\n')
	b.WriteString(canonicalHeaders(r, signedHeaders))
	b.WriteByte('\n')
	b.WriteString(strings.Join(signedHeaders, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

// canonicalURI returns the URI-encoded path. S3 (unlike most services) does not
// double-encode the path, so each already-decoded segment is re-encoded once
// with the AWS rule set (RFC 3986, with "/" preserved between segments).
func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	// Decode then re-encode per-segment so our encoding matches the SDK's,
	// regardless of how the path arrived on the wire.
	segs := strings.Split(u.Path, "/")
	for i, s := range segs {
		segs[i] = awsURIEncode(s, false)
	}
	return strings.Join(segs, "/")
}

// canonicalQuery returns the canonical query string: params sorted by key, each
// key and value AWS-URI-encoded, joined by "&".
func canonicalQuery(u *url.URL) string {
	q := u.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := q[k]
		sort.Strings(vals)
		ek := awsURIEncode(k, true)
		for _, v := range vals {
			parts = append(parts, ek+"="+awsURIEncode(v, true))
		}
	}
	return strings.Join(parts, "&")
}

// canonicalHeaders returns the canonical headers block: each signed header
// lower-cased, value trimmed and inner whitespace collapsed, sorted by name,
// one "name:value\n" per line.
func canonicalHeaders(r *http.Request, signedHeaders []string) string {
	var b strings.Builder
	for _, h := range signedHeaders {
		lower := strings.ToLower(h)
		var v string
		if lower == "host" {
			v = r.Host
		} else {
			v = strings.Join(r.Header.Values(http.CanonicalHeaderKey(h)), ",")
		}
		b.WriteString(lower)
		b.WriteByte(':')
		b.WriteString(collapseWhitespace(v))
		b.WriteByte('\n')
	}
	return b.String()
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// stringToSign assembles the SigV4 string-to-sign from the request's amz date,
// the credential scope, and the hash of the canonical request.
func stringToSign(amzDate, credScope, canonReq string) string {
	return strings.Join([]string{
		algorithm,
		amzDate,
		credScope,
		hexSHA256([]byte(canonReq)),
	}, "\n")
}

// signingKey derives the SigV4 signing key by chaining HMACs over the scope.
func signingKey(secret, date, region, service string) []byte {
	kDate := hmacRaw([]byte("AWS4"+secret), date)
	kRegion := hmacRaw(kDate, region)
	kService := hmacRaw(kRegion, service)
	return hmacRaw(kService, "aws4_request")
}

func hmacRaw(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hmacHex(key []byte, data string) string {
	return hex.EncodeToString(hmacRaw(key, data))
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// awsURIEncode encodes s per the AWS canonicalization rules: unreserved chars
// (A-Z a-z 0-9 - _ . ~) pass through; everything else becomes %XX uppercase.
// When encodeSlash is false, "/" is preserved (used for path segments joined by
// the caller); when true (query components) "/" is escaped.
func awsURIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return b.String()
}
