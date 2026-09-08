// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
)

const (
	testAccessKey = "AKIATESTKEY00000DEV0"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testBucket    = "ml-objects"
	testDomain    = "tenant-a"
)

// singleRouter routes every domain to one gateway (the credential resolver
// already pins the domain), satisfying the handler's Router with a fixed
// gateway so the test owns the underlying stack for blob-count inspection.
type singleRouter struct{ gw *gateway.Gateway }

func (r singleRouter) For(string) (*gateway.Gateway, error) { return r.gw, nil }

// testEnv stands up the S3 handler on an httptest.Server over a local casstore
// gateway, plus an aws-sdk-go-v2 client pointed at it (path-style, static test
// creds). It returns the client, the chunk store (for dedup assertions), and a
// cleanup.
type testEnv struct {
	client *awss3.Client
	chunks blobstore.Storage
	gw     *gateway.Gateway // the single backing gateway (for reserved-ref inspection)
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	local, err := gateway.NewLocalTenantRouter(t.TempDir(), 1*1024*1024)
	if err != nil {
		t.Fatalf("NewLocalTenantRouter: %v", err)
	}
	gw, err := local.Router.For(testDomain)
	if err != nil {
		t.Fatalf("router.For: %v", err)
	}

	creds := NewMemoryCredentialStore(Credential{
		AccessKeyID: testAccessKey,
		SecretKey:   testSecretKey,
		Domain:      testDomain,
	})
	h := New(Config{Router: singleRouter{gw}, Creds: creds})

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  awscreds.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	})
	return &testEnv{client: client, chunks: local.Chunks, gw: gw}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func md5hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// countDomainBlobs reports the physical chunk/pack blobs casstore has stored for
// the test domain. Mirrors the blob-count helper pattern in blobgw/mlfs tests.
func countDomainBlobs(t *testing.T, chunks blobstore.Storage) int {
	t.Helper()
	n := 0
	for _, prefix := range []blobstore.ID{
		blobstore.ID("chunk-" + testDomain + "-"),
		blobstore.ID("pack-" + testDomain + "-"),
	} {
		if err := chunks.ListBlobs(context.Background(), prefix, func(blobstore.Metadata) error {
			n++
			return nil
		}); err != nil {
			t.Fatalf("ListBlobs %q: %v", prefix, err)
		}
	}
	return n
}

// TestPutGetRoundTrip drives PutObject → GetObject through aws-sdk-go-v2 (which
// uses streaming SigV4 by default) and asserts byte-identity, the MD5 ETag, and
// that Content-Type + a user-metadata header survive.
func TestPutGetRoundTrip(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	payload := randomBytes(t, 1024*1024)

	_, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(testBucket),
		Key:         aws.String("docs/report.bin"),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/x-custom"),
		Metadata:    map[string]string{"origin": "unit-test"},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	out, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("docs/report.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = out.Body.Close()

	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %d bytes want %d", len(got), len(payload))
	}
	wantETag := `"` + md5hex(payload) + `"`
	if aws.ToString(out.ETag) != wantETag {
		t.Errorf("ETag: got %q want %q", aws.ToString(out.ETag), wantETag)
	}
	if aws.ToString(out.ContentType) != "application/x-custom" {
		t.Errorf("ContentType: got %q", aws.ToString(out.ContentType))
	}
	if out.ContentLength == nil || *out.ContentLength != int64(len(payload)) {
		t.Errorf("ContentLength: got %v want %d", out.ContentLength, len(payload))
	}
	if out.Metadata["origin"] != "unit-test" {
		t.Errorf("user metadata origin: got %q want %q", out.Metadata["origin"], "unit-test")
	}
}

// TestHeadObject asserts HeadObject returns size/ETag/content-type and no body.
func TestHeadObject(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	payload := randomBytes(t, 4096)

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(testBucket),
		Key:         aws.String("a/b/c.dat"),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("text/plain"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	head, err := env.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("a/b/c.dat"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(payload)) {
		t.Errorf("ContentLength: got %v want %d", head.ContentLength, len(payload))
	}
	if aws.ToString(head.ETag) != `"`+md5hex(payload)+`"` {
		t.Errorf("ETag: got %q", aws.ToString(head.ETag))
	}
	if aws.ToString(head.ContentType) != "text/plain" {
		t.Errorf("ContentType: got %q", aws.ToString(head.ContentType))
	}
}

// TestDeleteThenGetNoSuchKey deletes an object and asserts a subsequent
// GetObject surfaces NoSuchKey (404).
func TestDeleteThenGetNoSuchKey(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("ephemeral"),
		Body:   bytes.NewReader(randomBytes(t, 2048)),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := env.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("ephemeral"),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	_, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("ephemeral"),
	})
	if err == nil {
		t.Fatal("GetObject after delete: expected error, got nil")
	}
	var nsk *types.NoSuchKey
	if !errors.As(err, &nsk) {
		t.Errorf("expected NoSuchKey, got %v", err)
	}
}

// TestDedupThroughS3 is the money shot: PutObject the SAME large content under
// two distinct keys through the real S3 client and assert casstore stored ~one
// physical copy — proving transparent dedup through the S3 API.
func TestDedupThroughS3(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	payload := randomBytes(t, 8*1024*1024) // 8 MiB identical content

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("copies/one"),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject one: %v", err)
	}
	blobsAfterFirst := countDomainBlobs(t, env.chunks)
	if blobsAfterFirst == 0 {
		t.Fatal("expected physical blobs after first object")
	}

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("copies/two"),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject two: %v", err)
	}
	blobsAfterSecond := countDomainBlobs(t, env.chunks)

	if blobsAfterSecond != blobsAfterFirst {
		t.Errorf("dedup failed through S3: identical 8 MiB content under 2 keys grew blob count %d → %d",
			blobsAfterFirst, blobsAfterSecond)
	}

	// Both keys are independently retrievable and byte-exact.
	for _, key := range []string{"copies/one", "copies/two"} {
		out, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		got, _ := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if !bytes.Equal(got, payload) {
			t.Errorf("key %s round-trip mismatch", key)
		}
	}
}

// TestBadSignature drives a request whose credentials carry the wrong secret and
// asserts a 403 SignatureDoesNotMatch.
func TestBadSignature(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// A client signing with the right access key id but a WRONG secret produces a
	// signature the server cannot reproduce.
	badClient := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: env.client.Options().BaseEndpoint,
		UsePathStyle: true,
		Credentials:  awscreds.NewStaticCredentialsProvider(testAccessKey, "totally-wrong-secret", ""),
	})

	_, err := badClient.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("denied"),
		Body:   bytes.NewReader([]byte("data")),
	})
	if err == nil {
		t.Fatal("expected SignatureDoesNotMatch, got nil")
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("expected smithy.APIError, got %v", err)
	}
	if ae.ErrorCode() != "SignatureDoesNotMatch" {
		t.Errorf("error code: got %q want SignatureDoesNotMatch", ae.ErrorCode())
	}
}
