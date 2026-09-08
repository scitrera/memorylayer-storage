// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package tenantbind

import (
	"strings"

	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// The two S3 clients a Binding feeds want the endpoint in DIFFERENT shapes from
// the SAME single Binding.Endpoint value (which operators set once, e.g. the
// bare host "s3.us-east-2.amazonaws.com"):
//
//   - The casstore blobstore (kopia s3 driver, ToBlobstoreS3Config) wants a BARE
//     HOST with NO scheme (e.g. "s3.us-east-2.amazonaws.com"). A scheme-prefixed
//     value ("https://s3.us-east-2.amazonaws.com") fails kopia's minio-go host
//     validation ("does not follow ip address or domain name standards").
//   - The aws-sdk-go-v2 client (snapshot manifest store + presign,
//     ToSnapshotS3Config) wants a value WITH a scheme or empty; a bare host fails
//     the SDK's endpoint URI parse ("Custom endpoint `s3...` was not a valid URI").
//
// So convert.go normalizes the single binding value PER CLIENT: stripScheme for
// the kopia path, ensureScheme for the aws-sdk path. A single bare-host (or URI)
// binding therefore works for both, and the chart can keep the bare host the
// kopia path needs without breaking the presign/manifest path.

// stripScheme removes a leading "https://" or "http://" from ep, returning the
// bare host and whether the scheme was plaintext http:// (so the caller can turn
// TLS off for an http endpoint). An already-bare host returns unchanged with
// http=false (TLS on, the default), and empty returns empty.
func stripScheme(ep string) (host string, httpScheme bool) {
	if rest, ok := strings.CutPrefix(ep, "https://"); ok {
		return rest, false
	}
	if rest, ok := strings.CutPrefix(ep, "http://"); ok {
		return rest, true
	}
	return ep, false
}

// ensureScheme prepends "https://" to a non-empty, scheme-less ep so the
// aws-sdk-go-v2 client gets a valid endpoint URI. A value that already carries a
// scheme is returned unchanged, and empty returns empty (the AWS SDK then derives
// the endpoint from the region).
func ensureScheme(ep string) string {
	if ep == "" {
		return ""
	}
	if strings.HasPrefix(ep, "https://") || strings.HasPrefix(ep, "http://") {
		return ep
	}
	return "https://" + ep
}

// EnsureEndpointScheme exposes the aws-sdk-go-v2 endpoint normalization (prepend
// https:// to a non-empty scheme-less host; empty/already-schemed pass through)
// for callers that build their own SDK client straight from Binding.Endpoint —
// e.g. the control-plane presign client — instead of going through
// ToSnapshotS3Config. Without it a bare-host binding ("s3.us-east-2.amazonaws.com")
// makes the SDK endpoint resolver fail "Custom endpoint ... was not a valid URI".
func EnsureEndpointScheme(ep string) string { return ensureScheme(ep) }

// prefixWithTrailingSlash gives a non-empty prefix a single trailing "/" so the
// kopia blobstore's raw "prefix+blobID" object key matches the presign objectKey
// convention ("<prefix>/<key>"). Empty stays empty; an existing trailing slash is
// preserved (no doubling).
func prefixWithTrailingSlash(p string) string {
	if p == "" || strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// ToBlobstoreS3Config maps the Binding onto casstore's blobstore.S3Config, the
// config for the chunk (pack object) store (ADR §2.6). The endpoint is normalized
// to the BARE HOST kopia's s3 driver requires (stripScheme): an https:// prefix is
// dropped (TLS stays on), an http:// prefix is dropped AND sets DoNotUseTLS so a
// plaintext-http binding (dev/MinIO) stays plaintext, and an already-bare host
// passes through with TLS on. DoNotVerifyTLS is left at its zero value (verify on);
// it is a dev-only self-signed concern, not part of the per-tenant binding.
//
// Creds.SessionToken IS mapped now that casstore's blobstore.S3Config carries a
// SessionToken field (ADR §7.8): the AWS AssumeRoleWithWebIdentity impl
// (ADR §2.9) supplies STS temporary credentials with a session token, and they
// must reach the chunk-store S3 client for direct I/O — the same token the
// presign path already uses. A zero token (static keys) maps through as empty,
// preserving the pre-existing behavior.
func (b Binding) ToBlobstoreS3Config() blobstore.S3Config {
	host, httpScheme := stripScheme(b.Endpoint)
	return blobstore.S3Config{
		Bucket: b.Bucket,
		// TRAILING SLASH is REQUIRED: kopia's s3 driver prepends Prefix to the
		// blob ID RAW (no separator), so Prefix "blobgw" makes it look at
		// "blobgwpack-..." — but objects are written (via the presign objectKey,
		// which does prefix + "/" + key) at "blobgw/pack-...". Without the slash
		// the GC/compaction pack LIST returns 0 (chunks_scanned=0, no repack).
		Prefix:       prefixWithTrailingSlash(b.Prefix),
		Region:       b.Region,
		Endpoint:     host,
		AccessKey:    b.Creds.AccessKeyID,
		SecretKey:    b.Creds.SecretAccessKey,
		SessionToken: b.Creds.SessionToken,
		DoNotUseTLS:  httpScheme,
	}
}

// ToSnapshotS3Config maps the Binding onto casstore's snapshot.S3Config (an
// alias for s3util.Config), the config for the manifest store (ADR §2.6). The
// endpoint is normalized to a valid URI the aws-sdk-go-v2 client accepts
// (ensureScheme): a non-empty scheme-less host gets an https:// prefix, a value
// that already carries a scheme passes through, and an empty endpoint stays empty
// so the SDK derives it from the region. ForcePathStyle is left at its zero value
// (false = AWS virtual-host style); it is an endpoint-shape concern (RustFS), not
// part of the credential binding.
//
// Creds.SessionToken IS mapped now that casstore's snapshot.S3Config
// (s3util.Config) carries a SessionToken field (ADR §7.8): it threads the token
// into the aws-sdk-go-v2 static credentials provider so the AWS STS impl's
// temporary credentials (ADR §2.9) drive the manifest-store S3 client for direct
// I/O — see the same note on ToBlobstoreS3Config. A zero token (static keys)
// maps through as empty, preserving the pre-existing behavior.
func (b Binding) ToSnapshotS3Config() snapshot.S3Config {
	return snapshot.S3Config{
		Bucket:       b.Bucket,
		Prefix:       b.Prefix,
		Region:       b.Region,
		Endpoint:     ensureScheme(b.Endpoint),
		AccessKey:    b.Creds.AccessKeyID,
		SecretKey:    b.Creds.SecretAccessKey,
		SessionToken: b.Creds.SessionToken,
	}
}
