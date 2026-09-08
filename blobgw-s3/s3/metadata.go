// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import "strings"

// S3 presentation metadata that the casstore gateway does not derive on its own
// is stored DURABLY in the gateway's per-object user metadata (which lands in the
// casstore manifest's SnapshotMetadata.Tags, not an in-memory sidecar). This is
// what makes the S3 ETag and x-amz-meta-* headers survive a restart: a fresh
// handler over the same backend reads them straight back from the manifest via
// gateway.ObjectInfo.UserMeta.
//
// Two reserved key namespaces partition that map:
//   - etagMetaKey holds the S3 ETag (the MD5 of a single-part object, or the
//     "<md5-of-md5s>-<N>" composite ETag of a completed multipart upload). The
//     gateway already tracks a sha256 content hash, but S3 clients expect MD5,
//     so we persist the S3 ETag explicitly.
//   - userMetaPrefix prefixes each x-amz-meta-* user-metadata key (already
//     lower-cased to its header suffix) so user keys can never collide with the
//     reserved etag key.
//
// Content-Type, size, and last-modified are NOT stored here: the gateway already
// round-trips Content-Type (ObjectInfo.ContentType) and size, and last-modified
// is ObjectInfo.CreatedAt.
const (
	etagMetaKey    = "s3.etag"
	userMetaPrefix = "s3.meta."
)

// buildObjectMeta assembles the gateway user-metadata map for a stored object
// from its S3 ETag and the x-amz-meta-* user headers. The result is handed to
// gateway PutWithMeta / Finalize, which persists it durably in the manifest.
func buildObjectMeta(etag string, userMeta map[string]string) map[string]string {
	out := make(map[string]string, len(userMeta)+1)
	if etag != "" {
		out[etagMetaKey] = etag
	}
	for k, v := range userMeta {
		out[userMetaPrefix+k] = v
	}
	return out
}

// etagFromMeta extracts the stored S3 ETag (no quotes) from a gateway object's
// user metadata, "" if absent.
func etagFromMeta(meta map[string]string) string { return meta[etagMetaKey] }

// userMetaFromMeta extracts the x-amz-meta-* user metadata (suffix-keyed,
// lower-cased) from a gateway object's user metadata, nil if none.
func userMetaFromMeta(meta map[string]string) map[string]string {
	var out map[string]string
	for k, v := range meta {
		if suffix, ok := strings.CutPrefix(k, userMetaPrefix); ok {
			if out == nil {
				out = make(map[string]string)
			}
			out[suffix] = v
		}
	}
	return out
}
