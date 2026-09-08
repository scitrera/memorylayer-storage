// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"encoding/base64"
	"net/url"
)

// encodeKey renders an object key for an XML list response. With encoding-type=url
// the key is percent-encoded (the mechanism S3 offers so keys containing XML-
// hostile bytes — control chars, etc. — survive a round-trip); otherwise it is
// emitted verbatim and Go's xml package escapes the XML metacharacters.
func encodeKey(key string, encodeURL bool) string {
	if !encodeURL {
		return key
	}
	// S3's url encoding escapes the key but preserves "/" between path segments,
	// matching url.QueryEscape except that "/" and " " differ. Use the path-style
	// escaping and then re-encode "+" which QueryEscape would have produced for
	// spaces. PathEscape keeps "/" unescaped which is what S3 does for keys.
	return url.PathEscape(key)
}

// encodeContinuationToken turns the last-key of a truncated page into the opaque
// NextContinuationToken a client echoes back. S3 tokens are opaque base64; we
// base64 the exclusive cursor so decode is a pure inverse with no server state.
func encodeContinuationToken(lastKey string) string {
	return base64.StdEncoding.EncodeToString([]byte(lastKey))
}

// decodeContinuationToken recovers the exclusive cursor from a token previously
// produced by encodeContinuationToken. A malformed token is an InvalidArgument.
func decodeContinuationToken(tok string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
