// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// Multipart upload (S3) over the blobgw gateway. The four core operations —
// CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
// AbortMultipartUpload — let large objects upload in parts and assemble into a
// single deduped gateway object; ListParts and ListMultipartUploads round out
// the surface for SDK introspection.
//
// Parts are stored as ordinary gateway objects under the reserved mpuPartPrefix
// ref namespace (so casstore chunks+dedups them and they survive a crash mid-
// upload); CompleteMultipartUpload streams them in part-number order through a
// single Gateway.Put so the *assembled* object also dedups, then deletes the
// temp part objects. AbortMultipartUpload deletes the parts without assembling.
//
// The S3 part-size rule (every part except the last must be >= 5 MiB) is NOT
// enforced yet — we accept smaller parts so the SDK's default behavior is never
// blocked. See tech-debt note in the Stage-3 report.

const maxPartNumber = 10000

// initiateMultipartUploadResult is the CreateMultipartUpload XML response.
type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// completeMultipartUpload is the CompleteMultipartUpload request body: the
// client's ordered list of parts (number + ETag) to assemble.
type completeMultipartUpload struct {
	XMLName xml.Name             `xml:"CompleteMultipartUpload"`
	Parts   []completeUploadPart `xml:"Part"`
}

type completeUploadPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completeMultipartUploadResult is the CompleteMultipartUpload XML response.
type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// listPartsResult is the ListParts XML response.
type listPartsResult struct {
	XMLName     xml.Name      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`
	Bucket      string        `xml:"Bucket"`
	Key         string        `xml:"Key"`
	UploadID    string        `xml:"UploadId"`
	MaxParts    int           `xml:"MaxParts"`
	IsTruncated bool          `xml:"IsTruncated"`
	Parts       []listPartXML `xml:"Part"`
}

type listPartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
	Size       int64  `xml:"Size"`
}

// listMultipartUploadsResult is the ListMultipartUploads XML response. We report
// an empty in-progress set (uploads are not enumerated cross-key in this stage);
// the document is well-formed so SDKs that probe it succeed.
type listMultipartUploadsResult struct {
	XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult"`
	Bucket      string   `xml:"Bucket"`
	KeyMarker   string   `xml:"KeyMarker"`
	MaxUploads  int      `xml:"MaxUploads"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// mpuPartRef builds the reserved gateway ref one part's bytes are stored under.
// Part numbers are zero-padded to 5 digits so the refs sort in part-number order
// (purely cosmetic; assembly sorts numerically regardless).
func mpuPartRef(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s%s\x00%05d", mpuPartPrefix, uploadID, partNumber)
}

// createMultipartUpload handles POST /{bucket}/{key}?uploads. It allocates an
// UploadId and records the target object's identity + presentation metadata for
// the eventual CompleteMultipartUpload.
func (h *handler) createMultipartUpload(w http.ResponseWriter, r *http.Request, domain, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	uploadID := newUploadID()
	h.mpu.create(mpuUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Domain:      domain,
		Key:         key,
		ContentType: contentType,
		UserMeta:    userMetadata(r.Header),
		CreatedAt:   h.now().UTC(),
	})
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	})
}

// uploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=U. It stores the
// part bytes as a reserved gateway object (deduped through casstore), records
// the part's MD5, and returns the part ETag header. Parts arrive via the same
// streaming-SigV4 body path as PutObject (reusing objectBody / chunkReader).
func (h *handler) uploadPart(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, uploadID string, partNumber int, cred Credential) {
	if partNumber < 1 || partNumber > maxPartNumber {
		writeError(w, r.URL.Path, errInvalidArgument)
		return
	}
	if _, ok := h.mpu.get(uploadID); !ok {
		writeError(w, r.URL.Path, errNoSuchUpload)
		return
	}

	body, err := h.objectBody(r, cred)
	if err != nil {
		writeError(w, r.URL.Path, err)
		return
	}

	ref := mpuPartRef(uploadID, partNumber)
	md5h := md5.New()
	info, err := gw.Put(r.Context(), ref, "application/octet-stream", io.TeeReader(body, md5h))
	if err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}

	var raw [16]byte
	md5h.Sum(raw[:0])
	etag := hex.EncodeToString(raw[:])
	if !h.mpu.putPart(uploadID, uploadedPart{
		PartNumber: partNumber,
		MD5:        raw,
		ETag:       etag,
		Ref:        ref,
		Size:       info.Size,
	}) {
		// Upload was aborted concurrently between the get above and here; reclaim
		// the part we just stored so it doesn't leak.
		_ = gw.Delete(r.Context(), ref)
		writeError(w, r.URL.Path, errNoSuchUpload)
		return
	}

	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// completeMultipartUpload handles POST /{bucket}/{key}?uploadId=U. It validates
// the client's part list against the stored parts (InvalidPart on a missing part
// or ETag mismatch), assembles them IN PART-NUMBER ORDER into the final object
// via a single streaming Gateway.Put (so the assembled object dedups), computes
// the S3 multipart ETag, persists the presentation metadata, and deletes the
// temp part objects.
func (h *handler) completeMultipartUpload(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, _, key, uploadID string) {
	up, ok := h.mpu.get(uploadID)
	if !ok {
		writeError(w, r.URL.Path, errNoSuchUpload)
		return
	}

	var req completeMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r.URL.Path, errMalformedXML)
		return
	}
	if len(req.Parts) == 0 {
		writeError(w, r.URL.Path, errInvalidPart)
		return
	}

	// Resolve and validate each requested part against the stored set, in the
	// client-listed order. S3 requires the listed parts be in ascending part-
	// number order; we re-sort defensively so assembly is always ordered.
	parts := make([]uploadedPart, 0, len(req.Parts))
	var totalSize int64
	for _, want := range req.Parts {
		stored, ok := up.parts[want.PartNumber]
		if !ok || stored.ETag != trimETag(want.ETag) {
			writeError(w, r.URL.Path, errInvalidPart)
			return
		}
		totalSize += stored.Size
		parts = append(parts, stored)
	}
	// Per-part bodies are bounded by h.maxObj via objectBody, but the assembled
	// object is the sum of up to maxPartNumber parts — enforce the same ceiling on
	// the aggregate so Complete can't materialize an object larger than allowed.
	if h.maxObj > 0 && totalSize > h.maxObj {
		writeError(w, r.URL.Path, errEntityTooLarge)
		return
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	// Assemble: open each part's reader and concatenate via io.MultiReader so the
	// gateway streams the whole object through casstore (chunk+dedup) without
	// buffering it. Readers are closed after the Put completes.
	readers := make([]io.Reader, 0, len(parts))
	closers := make([]io.Closer, 0, len(parts))
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()
	for _, p := range parts {
		rc, _, err := gw.Get(r.Context(), p.Ref)
		if err != nil {
			writeError(w, r.URL.Path, mapGatewayErr(err))
			return
		}
		readers = append(readers, rc)
		closers = append(closers, rc)
	}

	// Assemble first, then bind the durable S3 metadata. The composite multipart
	// ETag ("-<N>") is known up front (it derives only from the part MD5s), so we
	// can persist it in the same write via PutWithMeta — landing the ETag +
	// x-amz-meta-* in the assembled object's casstore manifest so it survives a
	// restart.
	etag := multipartETag(parts)
	if _, err := gw.PutWithMeta(r.Context(), key, up.ContentType,
		buildObjectMeta(etag, up.UserMeta), io.MultiReader(readers...)); err != nil {
		writeError(w, r.URL.Path, mapGatewayErr(err))
		return
	}

	// Clean up: delete the temp part objects and discard the upload state. A part
	// delete failure leaves a reserved-prefix object for GC to reclaim; it never
	// surfaces to clients (filtered from listings), so we don't fail Complete.
	for _, p := range parts {
		_ = gw.Delete(r.Context(), p.Ref)
	}
	h.mpu.del(uploadID)

	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Location: "/" + up.Bucket + "/" + key,
		Bucket:   up.Bucket,
		Key:      key,
		ETag:     `"` + etag + `"`,
	})
}

// abortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=U. It deletes
// every stored part and discards the upload state, returning 204. Aborting an
// unknown upload is NoSuchUpload.
func (h *handler) abortMultipartUpload(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, uploadID string) {
	up, ok := h.mpu.get(uploadID)
	if !ok {
		writeError(w, r.URL.Path, errNoSuchUpload)
		return
	}
	for _, p := range up.sortedParts() {
		_ = gw.Delete(r.Context(), p.Ref)
	}
	h.mpu.del(uploadID)
	w.WriteHeader(http.StatusNoContent)
}

// listParts handles GET /{bucket}/{key}?uploadId=U: it lists the parts stored so
// far for an in-flight upload, ascending by part number.
func (h *handler) listParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	up, ok := h.mpu.get(uploadID)
	if !ok {
		writeError(w, r.URL.Path, errNoSuchUpload)
		return
	}
	res := listPartsResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		MaxParts: maxPartNumber,
	}
	for _, p := range up.sortedParts() {
		res.Parts = append(res.Parts, listPartXML{
			PartNumber: p.PartNumber,
			ETag:       `"` + p.ETag + `"`,
			Size:       p.Size,
		})
	}
	writeXML(w, http.StatusOK, res)
}

// listMultipartUploads handles GET /{bucket}?uploads. This stage does not
// enumerate in-progress uploads cross-key; it returns a well-formed empty,
// non-truncated result so SDKs that probe it succeed.
func (h *handler) listMultipartUploads(w http.ResponseWriter, _ *http.Request, bucket string) {
	writeXML(w, http.StatusOK, listMultipartUploadsResult{
		Bucket:     bucket,
		MaxUploads: maxPartNumber,
	})
}

// multipartETag computes the S3 multipart ETag: the MD5 of the concatenated raw
// MD5 bytes of each part (in part-number order), hex-encoded, suffixed with
// "-<numParts>". parts must already be sorted by part number.
func multipartETag(parts []uploadedPart) string {
	h := md5.New()
	for _, p := range parts {
		_, _ = h.Write(p.MD5[:])
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}

// trimETag strips the surrounding quotes S3 clients wrap an ETag in before
// comparing it to the stored hex digest.
func trimETag(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}

// newUploadID mints an opaque, collision-resistant upload id. It mirrors the
// gateway's id convention (prefix + RFC3339Nano timestamp + random hex) so ids
// sort by creation time the same way refs/versions do elsewhere in the stack.
func newUploadID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return "mpu-" + time.Now().UTC().Format(time.RFC3339Nano) + "-" + hex.EncodeToString(buf)
}
