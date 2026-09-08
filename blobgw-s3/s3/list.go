// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// maxKeysCap is the hard ceiling S3 applies to a single ListObjects response:
// a client asking for more (or omitting max-keys) still gets at most 1000 keys
// per page and must paginate via the continuation token / marker.
const maxKeysCap = 1000

// listBucketResult is the ListObjectsV2 (and, with the legacy fields, v1) XML
// response document. The element/field layout matches the S3 wire format
// aws-sdk-go-v2 unmarshals, including the ListBucketResult namespace clients
// expect on the root element.
type listBucketResult struct {
	XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	Delimiter   string   `xml:"Delimiter,omitempty"`
	MaxKeys     int      `xml:"MaxKeys"`
	EncodingTyp string   `xml:"EncodingType,omitempty"`
	KeyCount    int      `xml:"KeyCount"`
	IsTruncated bool     `xml:"IsTruncated"`

	// v2 pagination tokens.
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	StartAfter            string `xml:"StartAfter,omitempty"`

	// v1 pagination markers (only emitted for the legacy list path).
	Marker     string `xml:"Marker,omitempty"`
	NextMarker string `xml:"NextMarker,omitempty"`

	Contents       []listEntry           `xml:"Contents"`
	CommonPrefixes []listCommonPrefixXML `xml:"CommonPrefixes"`
}

// listEntry is one object row in a ListBucketResult.
type listEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"` // ISO8601 / RFC3339, the S3 list format
	ETag         string `xml:"ETag"`         // quoted MD5
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// listCommonPrefixXML is one rolled-up "folder" prefix when a delimiter is set.
type listCommonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

// listParams is the parsed, normalized set of list query parameters shared by
// v1 and v2. cursor is the effective starting point (exclusive): for v2 it is
// max(continuation-token, start-after); for v1 it is the marker.
type listParams struct {
	prefix    string
	delimiter string
	maxKeys   int
	cursor    string
	encodeURL bool // encoding-type=url requested
	// v2 echoes back the raw tokens it received.
	continuationToken string
	startAfter        string
	isV2              bool
}

// listObjects handles both ListObjectsV2 (?list-type=2) and the legacy
// ListObjects v1 (no list-type). It translates the request into a gateway
// prefix List, applies delimiter rollup + pagination over the (sorted) key
// space, joins each object's size (from the gateway ObjectInfo) with its ETag /
// last-modified (read from the object's DURABLE gateway user metadata), and
// renders the ListBucketResult XML.
func (h *handler) listObjects(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, _, bucket string) {
	p, err := parseListParams(r)
	if err != nil {
		writeError(w, r.URL.Path, err)
		return
	}

	// Pull every object under the prefix. The gateway returns newest-first; the
	// S3 list contract is lexicographic by key, so we re-sort below. A domain
	// can hold more than one bucket's worth of refs in principle, but in this
	// deployment a bucket maps 1:1 to a domain, so the domain's refs ARE the
	// bucket's keys.
	infos, err := gw.List(r.Context(), p.prefix, 0)
	if err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Ref < infos[j].Ref })

	res := h.buildListResult(r, gw, bucket, p, infos)
	writeXML(w, http.StatusOK, res)
}

// buildListResult walks the lexicographically-sorted objects applying the
// cursor (start-after / continuation / marker), delimiter rollup, and max-keys
// page cap, producing the response document. infos must be sorted by ref. Each
// emitted entry's ETag is read from the object's durable gateway user metadata
// via a per-key Head (the durable single source of truth, no sidecar).
func (h *handler) buildListResult(r *http.Request, gw *gateway.Gateway, bucket string, p listParams, infos []gateway.ObjectInfo) listBucketResult {
	res := listBucketResult{
		Name:      bucket,
		Prefix:    encodeKey(p.prefix, p.encodeURL),
		Delimiter: encodeKey(p.delimiter, p.encodeURL),
		MaxKeys:   p.maxKeys,
	}
	if p.encodeURL {
		res.EncodingTyp = "url"
	}
	if p.isV2 {
		res.ContinuationToken = p.continuationToken
		res.StartAfter = encodeKey(p.startAfter, p.encodeURL)
	} else {
		res.Marker = encodeKey(p.cursor, p.encodeURL)
	}

	// seenPrefix dedups CommonPrefixes; count tracks page budget across both
	// Contents and CommonPrefixes (each rolled-up prefix counts as one key, per
	// S3).
	seenPrefix := make(map[string]struct{})
	count := 0
	var lastKey string

	for _, info := range infos {
		key := info.Ref
		// Hide reserved multipart part objects (stored under mpuPartPrefix) from
		// the user-visible key space.
		if strings.HasPrefix(key, mpuPartPrefix) {
			continue
		}
		// Cursor is exclusive: skip everything at or before it.
		if p.cursor != "" && key <= p.cursor {
			continue
		}

		// Delimiter rollup: if the key has the delimiter after the prefix, fold
		// it into a CommonPrefix instead of listing the object.
		if p.delimiter != "" {
			rest := strings.TrimPrefix(key, p.prefix)
			if idx := strings.Index(rest, p.delimiter); idx >= 0 {
				cp := p.prefix + rest[:idx+len(p.delimiter)]
				if _, dup := seenPrefix[cp]; dup {
					// Already rolled up; this key doesn't consume budget or
					// advance pagination on its own.
					lastKey = cp
					continue
				}
				if count >= p.maxKeys {
					res.IsTruncated = true
					break
				}
				seenPrefix[cp] = struct{}{}
				res.CommonPrefixes = append(res.CommonPrefixes, listCommonPrefixXML{Prefix: encodeKey(cp, p.encodeURL)})
				count++
				lastKey = cp
				continue
			}
		}

		if count >= p.maxKeys {
			res.IsTruncated = true
			break
		}
		etag := ""
		// The ETag lives in the object's durable gateway user metadata; Head reads
		// it back from the casstore manifest. A Head miss (object raced away after
		// List) just omits the ETag for that row.
		if head, err := gw.Head(r.Context(), key); err == nil {
			if e := etagFromMeta(head.UserMeta); e != "" {
				etag = `"` + e + `"`
			}
		}
		res.Contents = append(res.Contents, listEntry{
			Key:          encodeKey(key, p.encodeURL),
			LastModified: info.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         etag,
			Size:         info.Size,
			StorageClass: "STANDARD",
		})
		count++
		lastKey = key
	}

	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
	if res.IsTruncated {
		if p.isV2 {
			res.NextContinuationToken = encodeContinuationToken(lastKey)
		} else {
			res.NextMarker = encodeKey(lastKey, p.encodeURL)
		}
	}
	return res
}

// parseListParams reads and normalizes the list query parameters, distinguishing
// v2 (list-type=2) from the legacy v1 path. An out-of-range max-keys is clamped
// rather than rejected (matching S3, which silently caps at 1000).
func parseListParams(r *http.Request) (listParams, error) {
	q := r.URL.Query()
	p := listParams{
		prefix:    q.Get("prefix"),
		delimiter: q.Get("delimiter"),
		isV2:      q.Get("list-type") == "2",
	}
	p.encodeURL = strings.EqualFold(q.Get("encoding-type"), "url")

	p.maxKeys = maxKeysCap
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return listParams{}, errInvalidArgument
		}
		if n < p.maxKeys {
			p.maxKeys = n
		}
	}

	if p.isV2 {
		p.continuationToken = q.Get("continuation-token")
		p.startAfter = q.Get("start-after")
		// The continuation token, when present, supersedes start-after (S3: a
		// continued page ignores start-after). Otherwise start-after seeds the
		// exclusive cursor.
		if p.continuationToken != "" {
			tok, err := decodeContinuationToken(p.continuationToken)
			if err != nil {
				return listParams{}, errInvalidArgument
			}
			p.cursor = tok
		} else {
			p.cursor = p.startAfter
		}
	} else {
		p.cursor = q.Get("marker")
	}
	return p, nil
}

// writeXML marshals v as an S3 XML response with the standard header.
func writeXML(w http.ResponseWriter, status int, v any) {
	out, err := xml.Marshal(v)
	if err != nil {
		writeError(w, "", errInternal)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}
