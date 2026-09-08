// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// listAllMyBucketsResult is the ListBuckets (GET /) XML response document.
type listAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   bucketOwner   `xml:"Owner"`
	Buckets bucketListXML `xml:"Buckets"`
}

type bucketOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketListXML struct {
	Bucket []bucketXML `xml:"Bucket"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// createBucket handles PUT /{bucket}. With a registry configured it registers
// the bucket against the credential's resolved domain; re-creating a bucket the
// caller already owns is BucketAlreadyOwnedByYou. Without a registry it is a
// no-op success (Stage-1 single-tenant deployments treat buckets as implicit).
func (h *handler) createBucket(w http.ResponseWriter, r *http.Request, domain, bucket string) {
	if h.buckets == nil {
		// No registry: buckets are implicit. Acknowledge so clients that create
		// the bucket before first use don't fail.
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return
	}
	if ok := h.buckets.create(bucket, domain, h.now().UTC()); !ok {
		writeError(w, r.URL.Path, errBucketAlreadyOwned)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// headBucket handles HEAD /{bucket}: 200 if the bucket exists, 404 NoSuchBucket
// otherwise. Without a registry every authenticated bucket is treated as
// existing (Stage-1 behavior).
func (h *handler) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if h.buckets == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, ok := h.buckets.get(bucket); !ok {
		// HEAD carries no body; S3 signals a missing bucket with a bare 404.
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// deleteBucket handles DELETE /{bucket}. S3 rejects deleting a non-empty bucket
// with BucketNotEmpty; we enforce the same by checking the gateway for any
// remaining object under the bucket's domain. Without a registry the operation
// is rejected as NotImplemented (there is no bucket to remove).
func (h *handler) deleteBucket(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, bucket string) {
	if h.buckets == nil {
		writeError(w, r.URL.Path, errNotImplemented)
		return
	}
	if _, ok := h.buckets.get(bucket); !ok {
		writeError(w, r.URL.Path, errNoSuchBucket)
		return
	}
	// Emptiness check: a single user-visible object under the domain blocks
	// deletion. Reserved multipart part objects (mpuPartPrefix) are not user
	// objects, so they don't count — list unbounded and skip them.
	infos, err := gw.List(r.Context(), "", 0)
	if err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}
	for _, info := range infos {
		if strings.HasPrefix(info.Ref, mpuPartPrefix) {
			continue
		}
		writeError(w, r.URL.Path, errBucketNotEmpty)
		return
	}
	h.buckets.del(bucket)
	w.WriteHeader(http.StatusNoContent)
}

// listBuckets handles GET /: it enumerates the registry. Without a registry it
// returns an empty owner-scoped list (Stage-1 has no bucket inventory to report).
func (h *handler) listBuckets(w http.ResponseWriter, r *http.Request, cred Credential) {
	res := listAllMyBucketsResult{
		Owner: bucketOwner{ID: cred.Domain, DisplayName: cred.Domain},
	}
	if h.buckets != nil {
		for _, b := range h.buckets.list() {
			res.Buckets.Bucket = append(res.Buckets.Bucket, bucketXML{
				Name:         b.Name,
				CreationDate: b.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			})
		}
	}
	writeXML(w, http.StatusOK, res)
}
