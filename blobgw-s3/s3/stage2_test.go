// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// newRegistryEnv stands up the S3 handler with a bucket registry enabled (the
// Stage-2 posture) plus an aws-sdk-go-v2 client. The bucket must be created
// before objects can be written, exercising the bucket-existence enforcement.
func newRegistryEnv(t *testing.T) *testEnv {
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
	h := New(Config{
		Router:  singleRouter{gw},
		Creds:   creds,
		Buckets: NewMemoryBucketRegistry(),
	})
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

// TestListObjectsV2Paginated creates ~250 objects across two prefixes and walks
// them with the SDK paginator using a small page size, asserting every key is
// returned exactly once and in lexicographic order — proving continuation-token
// pagination through aws-sdk-go-v2's ListObjectsV2 paginator.
func TestListObjectsV2Paginated(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()

	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	var want []string
	for i := 0; i < 150; i++ {
		want = append(want, fmt.Sprintf("logs/2026/%04d.txt", i))
	}
	for i := 0; i < 100; i++ {
		want = append(want, fmt.Sprintf("data/part-%04d.bin", i))
	}
	for _, k := range want {
		if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(k),
			Body:   bytes.NewReader([]byte(k)), // distinct content per key
		}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}
	sort.Strings(want)

	pager := awss3.NewListObjectsV2Paginator(env.client, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(testBucket),
		MaxKeys: aws.Int32(37), // small → forces ~7 pages
	})
	var got []string
	pages := 0
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("paginator page %d: %v", pages, err)
		}
		pages++
		for _, o := range page.Contents {
			got = append(got, aws.ToString(o.Key))
			if o.Size == nil || *o.Size != int64(len(aws.ToString(o.Key))) {
				t.Errorf("key %q size: got %v want %d", aws.ToString(o.Key), o.Size, len(aws.ToString(o.Key)))
			}
			if aws.ToString(o.ETag) == "" {
				t.Errorf("key %q: empty ETag", aws.ToString(o.Key))
			}
		}
	}
	if pages < 2 {
		t.Errorf("expected multiple pages, got %d", pages)
	}
	if len(got) != len(want) {
		t.Fatalf("key count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key %d: got %q want %q", i, got[i], want[i])
		}
	}
}

// TestListObjectsV2Delimiter asserts delimiter rollup: listing with delimiter
// "/" returns the top-level "folders" as CommonPrefixes and only the keys with
// no further delimiter as Contents.
func TestListObjectsV2Delimiter(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	keys := []string{
		"root.txt",
		"a/1.txt", "a/2.txt",
		"b/c/deep.txt", "b/d.txt",
	}
	for _, k := range keys {
		if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(k),
			Body:   bytes.NewReader([]byte("x")),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	out, err := env.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    aws.String(testBucket),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}

	var contents []string
	for _, o := range out.Contents {
		contents = append(contents, aws.ToString(o.Key))
	}
	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(cp.Prefix))
	}
	sort.Strings(contents)
	sort.Strings(prefixes)

	wantContents := []string{"root.txt"}
	wantPrefixes := []string{"a/", "b/"}
	if fmt.Sprint(contents) != fmt.Sprint(wantContents) {
		t.Errorf("Contents: got %v want %v", contents, wantContents)
	}
	if fmt.Sprint(prefixes) != fmt.Sprint(wantPrefixes) {
		t.Errorf("CommonPrefixes: got %v want %v", prefixes, wantPrefixes)
	}
}

// TestListObjectsV2PrefixAndStartAfter asserts prefix scoping plus the
// start-after exclusive cursor on a single (non-continued) request.
func TestListObjectsV2PrefixAndStartAfter(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for _, k := range []string{"p/a", "p/b", "p/c", "p/d", "q/z"} {
		if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(testBucket), Key: aws.String(k), Body: bytes.NewReader([]byte("x")),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	out, err := env.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:     aws.String(testBucket),
		Prefix:     aws.String("p/"),
		StartAfter: aws.String("p/b"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	var got []string
	for _, o := range out.Contents {
		got = append(got, aws.ToString(o.Key))
	}
	sort.Strings(got)
	want := []string{"p/c", "p/d"} // prefix p/, exclusive of p/b, excludes q/z
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("keys: got %v want %v", got, want)
	}
}

// TestBucketLifecycle drives the full bucket lifecycle through the SDK:
// CreateBucket → HeadBucket 200 → ListBuckets shows it → PutObject → DeleteBucket
// on non-empty → BucketNotEmpty → delete object → DeleteBucket → HeadBucket 404.
func TestBucketLifecycle(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()

	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := env.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("HeadBucket after create: %v", err)
	}

	lb, err := env.client.ListBuckets(ctx, &awss3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	found := false
	for _, b := range lb.Buckets {
		if aws.ToString(b.Name) == testBucket {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListBuckets did not include %q", testBucket)
	}

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("keep"), Body: bytes.NewReader([]byte("data")),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// DeleteBucket on a non-empty bucket must fail with BucketNotEmpty.
	_, err = env.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(testBucket)})
	if err == nil {
		t.Fatal("DeleteBucket on non-empty: expected error")
	}
	var ae smithy.APIError
	if !smithyAs(err, &ae) || ae.ErrorCode() != "BucketNotEmpty" {
		t.Fatalf("expected BucketNotEmpty, got %v", err)
	}

	if _, err := env.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("keep"),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := env.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("DeleteBucket on empty: %v", err)
	}

	// HeadBucket now 404s.
	_, err = env.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(testBucket)})
	if err == nil {
		t.Fatal("HeadBucket after delete: expected error")
	}
}

// TestObjectIntoUnknownBucket asserts that, with a registry, writing to a bucket
// that was never created yields NoSuchBucket.
func TestObjectIntoUnknownBucket(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	_, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("never-made"), Key: aws.String("k"), Body: bytes.NewReader([]byte("x")),
	})
	if err == nil {
		t.Fatal("PutObject into unknown bucket: expected error")
	}
	var ae smithy.APIError
	if !smithyAs(err, &ae) || ae.ErrorCode() != "NoSuchBucket" {
		t.Fatalf("expected NoSuchBucket, got %v", err)
	}
}

// TestRangeGet puts a known multi-KiB object and asserts a mid-range GET returns
// 206 with the exact bytes and Content-Range, and an unsatisfiable range 416.
func TestRangeGet(t *testing.T) {
	env := newTestEnv(t) // Stage-1 env (no registry) — Range works without bucket ops
	ctx := context.Background()
	payload := randomBytes(t, 8192)

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("blob"), Body: bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	out, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("blob"),
		Range:  aws.String("bytes=1000-1999"),
	})
	if err != nil {
		t.Fatalf("GetObject ranged: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if !bytes.Equal(got, payload[1000:2000]) {
		t.Errorf("range body mismatch: got %d bytes", len(got))
	}
	if out.ContentLength == nil || *out.ContentLength != 1000 {
		t.Errorf("Content-Length: got %v want 1000", out.ContentLength)
	}
	if aws.ToString(out.ContentRange) != "bytes 1000-1999/8192" {
		t.Errorf("Content-Range: got %q want %q", aws.ToString(out.ContentRange), "bytes 1000-1999/8192")
	}

	// Suffix range: last 500 bytes.
	sfx, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("blob"), Range: aws.String("bytes=-500"),
	})
	if err != nil {
		t.Fatalf("GetObject suffix range: %v", err)
	}
	sgot, _ := io.ReadAll(sfx.Body)
	_ = sfx.Body.Close()
	if !bytes.Equal(sgot, payload[len(payload)-500:]) {
		t.Errorf("suffix range mismatch")
	}

	// HEAD reports the full size and advertises ranges.
	head, err := env.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("blob"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(payload)) {
		t.Errorf("HEAD Content-Length: got %v want %d", head.ContentLength, len(payload))
	}
	if aws.ToString(head.AcceptRanges) != "bytes" {
		t.Errorf("HEAD Accept-Ranges: got %q", aws.ToString(head.AcceptRanges))
	}

	// Unsatisfiable range → 416 InvalidRange.
	_, err = env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("blob"), Range: aws.String("bytes=99999-100000"),
	})
	if err == nil {
		t.Fatal("GetObject unsatisfiable range: expected error")
	}
	var ae smithy.APIError
	if !smithyAs(err, &ae) || ae.ErrorCode() != "InvalidRange" {
		t.Fatalf("expected InvalidRange, got %v", err)
	}
}

// TestListObjectsV1Marker exercises the legacy v1 list path (no list-type) with
// a marker, confirming we still serve it minimally.
func TestListObjectsV1Marker(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(testBucket), Key: aws.String(k), Body: bytes.NewReader([]byte("x")),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}
	out, err := env.client.ListObjects(ctx, &awss3.ListObjectsInput{
		Bucket: aws.String(testBucket),
		Marker: aws.String("k2"),
	})
	if err != nil {
		t.Fatalf("ListObjects v1: %v", err)
	}
	var got []string
	for _, o := range out.Contents {
		got = append(got, aws.ToString(o.Key))
	}
	sort.Strings(got)
	want := []string{"k3", "k4"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("v1 marker keys: got %v want %v", got, want)
	}
}

// smithyAs is errors.As specialized for smithy.APIError, kept local so each
// assertion site reads as a single boolean check.
func smithyAs(err error, target *smithy.APIError) bool {
	return errors.As(err, target)
}
