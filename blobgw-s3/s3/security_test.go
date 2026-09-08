// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// secEnv stands up the S3 handler with a caller-controlled Config (so tests can
// pin MaxObject for the aggregate-size ceiling or Now for the clock-skew check)
// over a local casstore gateway, plus an httptest.Server and an aws-sdk-go-v2
// client. It mirrors newTestEnv/newRegistryEnv but exposes the raw server URL so
// hand-signed requests can be issued for the SigV4 freshness / payload-hash
// paths the SDK does not exercise by default.
type secEnv struct {
	client *awss3.Client
	gw     *gateway.Gateway
	url    string
}

func newSecEnv(t *testing.T, cfg Config) *secEnv {
	t.Helper()
	local, err := gateway.NewLocalTenantRouter(t.TempDir(), 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalTenantRouter: %v", err)
	}
	gw, err := local.Router.For(testDomain)
	if err != nil {
		t.Fatalf("router.For: %v", err)
	}
	cfg.Router = singleRouter{gw}
	cfg.Creds = NewMemoryCredentialStore(Credential{
		AccessKeyID: testAccessKey,
		SecretKey:   testSecretKey,
		Domain:      testDomain,
	})
	srv := httptest.NewServer(New(cfg))
	t.Cleanup(srv.Close)

	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  awscreds.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	})
	return &secEnv{client: client, gw: gw, url: srv.URL}
}

// signRequest signs r with SigV4 (header form) using the package's own canonical
// helpers — the same primitives the server verifies with — so tests can mint a
// valid signature while controlling X-Amz-Date and x-amz-content-sha256
// independently of the SDK. amzDate is the X-Amz-Date wire value; payloadHash is
// the x-amz-content-sha256 the signature commits to (and that the server reads).
func signRequest(t *testing.T, r *http.Request, amzDate, payloadHash string) {
	t.Helper()
	date := amzDate[:8] // yyyymmdd
	region, service := "us-east-1", "s3"
	r.Header.Set(amzDateHeader, amzDate)
	r.Header.Set(amzContentSHA, payloadHash)
	if r.Host == "" {
		r.Host = r.URL.Host
	}
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonReq := canonicalRequest(r, signed, payloadHash)
	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	sts := stringToSign(amzDate, scope, canonReq)
	key := signingKey(testSecretKey, date, region, service)
	sig := hmacHex(key, sts)
	r.Header.Set(authorizationHeader, algorithm+" "+
		"Credential="+testAccessKey+"/"+scope+", "+
		"SignedHeaders="+strings.Join(signed, ";")+", "+
		"Signature="+sig)
}

// TestCompleteMultipartRejectsOversizeAggregate (S2) uploads two parts whose
// per-part sizes are each within MaxObject but whose SUM exceeds it, then asserts
// CompleteMultipartUpload is rejected with EntityTooLarge — the assembled object
// must honor the same ceiling objectBody enforces per part.
func TestCompleteMultipartRejectsOversizeAggregate(t *testing.T) {
	const partSize = 5 * 1024 * 1024
	env := newSecEnv(t, Config{
		Buckets:   NewMemoryBucketRegistry(),
		MaxObject: 6 * 1024 * 1024, // each 5 MiB part fits; two parts (10 MiB) do not
	})
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	create, err := env.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(testBucket), Key: aws.String("big/agg.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := aws.ToString(create.UploadId)

	var completed []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		up, err := env.client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket: aws.String(testBucket), Key: aws.String("big/agg.bin"),
			UploadId: aws.String(uploadID), PartNumber: aws.Int32(n),
			Body: bytes.NewReader(randomBytes(t, partSize)),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		completed = append(completed, types.CompletedPart{
			PartNumber: aws.Int32(n),
			ETag:       up.ETag,
		})
	}

	_, err = env.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(testBucket), Key: aws.String("big/agg.bin"),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err == nil {
		t.Fatal("CompleteMultipartUpload over aggregate ceiling: expected EntityTooLarge, got nil")
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "EntityTooLarge" {
		t.Fatalf("expected EntityTooLarge, got %v", err)
	}

	// The object must not have been materialized.
	if _, err := env.gw.Head(ctx, "big/agg.bin"); err == nil {
		t.Fatal("oversize-aggregate object was materialized despite rejection")
	}
}

// TestStaleRequestRejectedAsSkewed (S3) drives a normal SDK request against a
// server whose clock is pinned far in the future, so the SDK's fresh X-Amz-Date
// looks stale: the server must answer RequestTimeTooSkewed. A control request
// against a server using the real clock succeeds, proving the check does not
// reject fresh requests.
func TestStaleRequestRejectedAsSkewed(t *testing.T) {
	// Server clock pinned 1 hour ahead of real now → the SDK's X-Amz-Date is ~1h
	// in the past relative to the server, exceeding the 15-minute window.
	future := func() time.Time { return time.Now().Add(1 * time.Hour) }
	stale := newSecEnv(t, Config{Buckets: NewMemoryBucketRegistry(), Now: future})
	ctx := context.Background()
	if _, err := stale.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err == nil {
		t.Fatal("stale request: expected RequestTimeTooSkewed, got nil")
	} else {
		var ae smithy.APIError
		if !errors.As(err, &ae) || ae.ErrorCode() != "RequestTimeTooSkewed" {
			t.Fatalf("stale request: expected RequestTimeTooSkewed, got %v", err)
		}
	}

	// Control: default (real) clock → the same fresh request is accepted.
	fresh := newSecEnv(t, Config{Buckets: NewMemoryBucketRegistry()})
	if _, err := fresh.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("fresh request rejected: %v", err)
	}
}

// TestPutObjectContentSHA256Mismatch (S4) issues a hand-signed PutObject whose
// signed x-amz-content-sha256 commits to a digest that does NOT match the body
// bytes; the server must consume the body, recompute SHA-256, and reject with
// XAmzContentSHA256Mismatch. A control request whose declared digest matches the
// body succeeds.
func TestPutObjectContentSHA256Mismatch(t *testing.T) {
	env := newSecEnv(t, Config{})
	amzDate := time.Now().UTC().Format(amzDateFormat)
	body := []byte("the payload bytes that will actually be sent")

	// Sign with a hash that the server will NOT recompute from the body: a valid
	// signature over a wrong content hash. The server verifies the seed signature
	// (passes — we signed this exact wrong hash) then rejects on the body mismatch.
	wrongHash := hexSHA256([]byte("a different payload entirely"))
	req, err := http.NewRequest(http.MethodPut, env.url+"/"+testBucket+"/hash/mismatch.bin", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(t, req, amzDate, wrongHash)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("mismatch PutObject: got status %d want 400", resp.StatusCode)
	}
	if code := readErrorCode(t, resp); code != "XAmzContentSHA256Mismatch" {
		t.Fatalf("mismatch PutObject: got code %q want XAmzContentSHA256Mismatch", code)
	}

	// Control: a correct declared digest is accepted.
	goodHash := hexSHA256(body)
	req2, err := http.NewRequest(http.MethodPut, env.url+"/"+testBucket+"/hash/ok.bin", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(t, req2, amzDate, goodHash)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("Do (control): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("matching PutObject: got status %d want 200", resp2.StatusCode)
	}
}

// readErrorCode parses the S3 <Error> XML body and returns its Code element, for
// hand-issued requests where there is no SDK to surface a typed error.
func readErrorCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read error body: %v", err)
	}
	var er errorResponse
	if err := xml.Unmarshal(body, &er); err != nil {
		t.Fatalf("unmarshal error body %q: %v", body, err)
	}
	return er.Code
}
