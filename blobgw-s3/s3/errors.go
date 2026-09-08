// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"encoding/xml"
	"net/http"
)

// apiError is an S3 error code paired with its canonical HTTP status. The S3
// wire format reports failures as an <Error> XML document plus the matching
// status line; clients (including aws-sdk-go-v2) parse the Code element to
// surface typed errors, so the strings must match the S3 spec exactly.
type apiError struct {
	code   string // S3 error code, e.g. "NoSuchKey"
	status int    // HTTP status to send with it
}

func (e apiError) Error() string { return e.code }

// Stage-1 error set. These cover the object operations and the auth path; later
// stages (list, bucket, multipart) add their own codes as needed.
var (
	errNoSuchKey               = apiError{"NoSuchKey", http.StatusNotFound}
	errNoSuchBucket            = apiError{"NoSuchBucket", http.StatusNotFound}
	errBucketNotEmpty          = apiError{"BucketNotEmpty", http.StatusConflict}
	errBucketAlreadyOwned      = apiError{"BucketAlreadyOwnedByYou", http.StatusConflict}
	errInvalidRange            = apiError{"InvalidRange", http.StatusRequestedRangeNotSatisfiable}
	errNoSuchUpload            = apiError{"NoSuchUpload", http.StatusNotFound}
	errInvalidPart             = apiError{"InvalidPart", http.StatusBadRequest}
	errMalformedXML            = apiError{"MalformedXML", http.StatusBadRequest}
	errAccessDenied            = apiError{"AccessDenied", http.StatusForbidden}
	errSignatureDoesNotMatch   = apiError{"SignatureDoesNotMatch", http.StatusForbidden}
	errInvalidAccessKeyID      = apiError{"InvalidAccessKeyId", http.StatusForbidden}
	errMissingSecurityHeader   = apiError{"MissingSecurityHeader", http.StatusBadRequest}
	errAuthorizationHeaderForm = apiError{"AuthorizationHeaderMalformed", http.StatusBadRequest}
	errInvalidRequest          = apiError{"InvalidRequest", http.StatusBadRequest}
	errInvalidArgument         = apiError{"InvalidArgument", http.StatusBadRequest}
	errEntityTooLarge          = apiError{"EntityTooLarge", http.StatusBadRequest}
	errBadDigest               = apiError{"BadDigest", http.StatusBadRequest}
	errContentSHA256Mismatch   = apiError{"XAmzContentSHA256Mismatch", http.StatusBadRequest}
	errRequestTimeTooSkewed    = apiError{"RequestTimeTooSkewed", http.StatusForbidden}
	errMethodNotAllowed        = apiError{"MethodNotAllowed", http.StatusMethodNotAllowed}
	errNotImplemented          = apiError{"NotImplemented", http.StatusNotImplemented}
	errInternal                = apiError{"InternalError", http.StatusInternalServerError}
)

// errorResponse is the S3 <Error> document. Resource and RequestId are optional
// in the spec but clients log them, so we populate Resource with the request
// path.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// writeError renders err as an S3 XML error response. A non-apiError is mapped
// to InternalError so the gateway never leaks Go error text as an S3 code.
func writeError(w http.ResponseWriter, resource string, err error) {
	ae, ok := err.(apiError)
	if !ok {
		ae = errInternal
	}
	body := errorResponse{
		Code:     ae.code,
		Message:  errMessage(ae.code, err),
		Resource: resource,
	}
	out, mErr := xml.Marshal(body)
	if mErr != nil {
		out = []byte("<Error><Code>InternalError</Code></Error>")
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(ae.status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}

// errMessage returns a human-readable message for a code. For the generic
// codes that wrap an underlying error we surface that error's text to aid
// debugging without changing the machine-readable Code.
func errMessage(code string, err error) string {
	switch code {
	case "NoSuchKey":
		return "The specified key does not exist."
	case "NoSuchBucket":
		return "The specified bucket does not exist."
	case "BucketNotEmpty":
		return "The bucket you tried to delete is not empty."
	case "BucketAlreadyOwnedByYou":
		return "Your previous request to create the named bucket succeeded and you already own it."
	case "InvalidRange":
		return "The requested range is not satisfiable."
	case "NoSuchUpload":
		return "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	case "InvalidPart":
		return "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified ETag might not have matched the part's ETag."
	case "MalformedXML":
		return "The XML you provided was not well-formed or did not validate against our published schema."
	case "AccessDenied":
		return "Access Denied"
	case "SignatureDoesNotMatch":
		return "The request signature we calculated does not match the signature you provided. Check your key and signing method."
	case "InvalidAccessKeyId":
		return "The AWS Access Key Id you provided does not exist in our records."
	case "EntityTooLarge":
		return "Your proposed upload exceeds the maximum allowed size."
	case "BadDigest":
		return "The Content-MD5 or checksum value you specified did not match what the server received."
	case "XAmzContentSHA256Mismatch":
		return "The provided 'x-amz-content-sha256' header does not match what was computed."
	case "RequestTimeTooSkewed":
		return "The difference between the request time and the current time is too large."
	case "MissingSecurityHeader":
		return "Your request was missing a required header."
	case "AuthorizationHeaderMalformed":
		return "The authorization header you provided is invalid."
	case "InternalError":
		return "We encountered an internal error. Please try again."
	default:
		if err != nil {
			return err.Error()
		}
		return code
	}
}
