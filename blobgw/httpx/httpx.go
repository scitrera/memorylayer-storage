// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package httpx holds the small HTTP response helpers shared by blobgw's own
// JSON server and the blobgw-edge data path: a JSON writer, a JSON error writer
// (the {"error": "..."} body shape), and the object-response header writer
// (Content-Type / Content-Length / ETag from a gateway.ObjectInfo). It exists
// to remove the byte-identical copies those two servers carried; the output is
// intentionally identical to what they emitted inline, so it is a drop-in.
//
// The S3 front end layers its own extra headers (Accept-Ranges, Last-Modified,
// x-amz-meta-*) and so does not use SetObjectHeaders here.
package httpx

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// errBody is the JSON error envelope: {"error": "<msg>"}. It matches the shape
// blobgw/server and blobgw-edge each previously defined locally.
type errBody struct {
	Error string `json:"error"`
}

// WriteJSON writes v as JSON with the given status, setting
// Content-Type: application/json. It is the shared form of the writeJSON
// helpers previously duplicated in server.go and edge.go (same header, same
// json.Encoder.Encode trailing newline).
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes the {"error": msg} body with the given status via WriteJSON,
// matching the prior server.go errBody{...} and edge.go writeErr output exactly.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, errBody{Error: msg})
}

// SetObjectHeaders writes the object response headers shared by blobgw/server
// and blobgw-edge: Content-Type (only when non-empty), Content-Length (always),
// and ETag (only when a content hash is present, quoted). It reproduces the
// exact header set both servers wrote inline for GET/HEAD object responses.
//
// It does NOT default a missing Content-Type to application/octet-stream — both
// original call sites only set Content-Type when info.ContentType was non-empty,
// and the blobgw PUT path already defaults the stored Content-Type at write
// time. (The S3 layer, which DOES default the header and adds Accept-Ranges /
// Last-Modified / x-amz-meta-*, keeps its own writer.)
func SetObjectHeaders(w http.ResponseWriter, info gateway.ObjectInfo) {
	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	if info.ContentHash != "" {
		w.Header().Set("ETag", `"`+info.ContentHash+`"`)
	}
}
