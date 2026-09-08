// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// multipartETagRe matches the S3 multipart ETag form: a 32-hex-char digest, a
// dash, then the part count.
var multipartETagRe = regexp.MustCompile(`^"[0-9a-f]{32}-([0-9]+)"$`)

// TestMultipartRoundTrip drives the aws-sdk-go-v2 manager.Uploader with a small
// PartSize so it actually performs CreateMultipartUpload / UploadPart×N /
// CompleteMultipartUpload, then GetObject and asserts byte-identity plus the
// "-N" multipart ETag suffix with the right N.
func TestMultipartRoundTrip(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	const partSize = 5 * 1024 * 1024        // 5 MiB
	payload := randomBytes(t, 17*1024*1024) // 17 MiB → 4 parts (5+5+5+2)
	wantParts := (len(payload) + partSize - 1) / partSize

	uploader := transfermanager.New(env.client, func(o *transfermanager.Options) {
		o.PartSizeBytes = partSize
		o.Concurrency = 3
		// Pin the multipart threshold to the part size: transfermanager switches
		// to multipart on MultipartUploadThreshold (default 16 MiB), whereas the
		// old manager.Uploader switched on PartSize. This preserves the test's
		// "multipart whenever body > part size" expectation.
		o.MultipartUploadThreshold = partSize
	})
	out, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:      aws.String(testBucket),
		Key:         aws.String("big/object.bin"),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/x-multipart"),
		Metadata:    map[string]string{"origin": "mpu-test"},
	})
	if err != nil {
		t.Fatalf("Upload (multipart): %v", err)
	}

	// The returned ETag must carry the multipart "-N" suffix with N == part count.
	m := multipartETagRe.FindStringSubmatch(aws.ToString(out.ETag))
	if m == nil {
		t.Fatalf("multipart ETag: got %q want /-N/ suffixed digest", aws.ToString(out.ETag))
	}
	if n, _ := strconv.Atoi(m[1]); n != wantParts {
		t.Errorf("multipart ETag part count: got %d want %d (etag %q)", n, wantParts, aws.ToString(out.ETag))
	}

	got, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("big/object.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = got.Body.Close()
	if !bytes.Equal(body, payload) {
		t.Errorf("multipart round-trip mismatch: got %d bytes want %d", len(body), len(payload))
	}
	if aws.ToString(got.ContentType) != "application/x-multipart" {
		t.Errorf("ContentType: got %q want application/x-multipart", aws.ToString(got.ContentType))
	}
	if got.Metadata["origin"] != "mpu-test" {
		t.Errorf("user metadata origin: got %q want mpu-test", got.Metadata["origin"])
	}
	if !strings.HasSuffix(aws.ToString(got.ETag), "-"+strconv.Itoa(wantParts)+`"`) {
		t.Errorf("GET ETag missing multipart suffix: %q", aws.ToString(got.ETag))
	}
}

// TestMultipartAbortNoLeak starts a multipart upload, uploads one part, aborts,
// and asserts the temp part objects are gone (no leak) and the object does not
// exist.
func TestMultipartAbortNoLeak(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	create, err := env.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("aborted/object.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := aws.ToString(create.UploadId)

	part := randomBytes(t, 6*1024*1024)
	if _, err := env.client.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String(testBucket),
		Key:        aws.String("aborted/object.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part),
	}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// A part is now stored: blob count must be non-zero (the part bytes landed).
	if countDomainBlobs(t, env.chunks) == 0 {
		t.Fatal("expected physical blobs after UploadPart")
	}

	if _, err := env.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String(testBucket),
		Key:      aws.String("aborted/object.bin"),
		UploadId: aws.String(uploadID),
	}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// The part ref is gone: no reserved-prefix ref remains in the domain.
	assertNoReservedRefs(t, env)

	// The object does not exist.
	_, err = env.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("aborted/object.bin"),
	})
	if err == nil {
		t.Fatal("HeadObject after abort: expected error, got nil")
	}

	// ListParts on the aborted upload now reports NoSuchUpload.
	_, err = env.client.ListParts(ctx, &awss3.ListPartsInput{
		Bucket:   aws.String(testBucket),
		Key:      aws.String("aborted/object.bin"),
		UploadId: aws.String(uploadID),
	})
	if err == nil {
		t.Fatal("ListParts after abort: expected NoSuchUpload, got nil")
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchUpload" {
		t.Fatalf("expected NoSuchUpload, got %v", err)
	}
}

// TestDedupAcrossMultipart uploads the SAME large content twice as two separate
// multipart uploads under distinct keys and asserts casstore stored ~one
// physical copy — proving the assembled objects dedup through the S3 multipart
// path (and that the temp parts were cleaned up, not left doubling the count).
func TestDedupAcrossMultipart(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	const partSize = 5 * 1024 * 1024
	payload := randomBytes(t, 13*1024*1024) // 13 MiB identical content → 3 parts

	upload := func(key string) {
		uploader := transfermanager.New(env.client, func(o *transfermanager.Options) {
			o.PartSizeBytes = partSize
			o.Concurrency = 1
			// Force multipart for the 13 MiB body: transfermanager's default
			// threshold is 16 MiB, so pin it to the part size (matching the old
			// manager.Uploader's PartSize-driven multipart switch).
			o.MultipartUploadThreshold = partSize
		})
		if _, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(payload),
		}); err != nil {
			t.Fatalf("Upload %s: %v", key, err)
		}
	}

	upload("dedup/one.bin")
	blobsAfterFirst := countDomainBlobs(t, env.chunks)
	if blobsAfterFirst == 0 {
		t.Fatal("expected physical blobs after first multipart upload")
	}
	// Parts were cleaned up on Complete: no reserved refs survive.
	assertNoReservedRefs(t, env)

	upload("dedup/two.bin")
	blobsAfterSecond := countDomainBlobs(t, env.chunks)

	if blobsAfterSecond != blobsAfterFirst {
		t.Errorf("dedup across multipart failed: identical 13 MiB content under 2 keys grew blob count %d → %d",
			blobsAfterFirst, blobsAfterSecond)
	}
	assertNoReservedRefs(t, env)

	// Both objects are independently retrievable and byte-exact.
	for _, key := range []string{"dedup/one.bin", "dedup/two.bin"} {
		out, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(testBucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		body, _ := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if !bytes.Equal(body, payload) {
			t.Errorf("key %s round-trip mismatch", key)
		}
	}
}

// TestMultipartPartsHiddenFromList completes a multipart upload and asserts the
// reserved part/temp refs do NOT appear in ListObjectsV2 — only the assembled
// object key is listed.
func TestMultipartPartsHiddenFromList(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	const partSize = 5 * 1024 * 1024
	uploader := transfermanager.New(env.client, func(o *transfermanager.Options) {
		o.PartSizeBytes = partSize
		// Pin the multipart threshold to the part size so the 12 MiB body is
		// uploaded as multipart (transfermanager's default threshold is 16 MiB,
		// unlike the old manager.Uploader which switched on PartSize).
		o.MultipartUploadThreshold = partSize
	})
	if _, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("visible/object.bin"),
		Body:   bytes.NewReader(randomBytes(t, 12*1024*1024)),
	}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	out, err := env.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(testBucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	if fmt.Sprint(keys) != fmt.Sprint([]string{"visible/object.bin"}) {
		t.Fatalf("ListObjectsV2 keys: got %v want [visible/object.bin]", keys)
	}
	for _, k := range keys {
		if strings.HasPrefix(k, mpuPartPrefix) {
			t.Errorf("reserved multipart part leaked into listing: %q", k)
		}
	}

	// Also confirm at the gateway layer that ONLY the assembled key (no reserved
	// refs) survives after Complete.
	assertNoReservedRefs(t, env)
}

// TestMultipartCompleteInvalidPart asserts CompleteMultipartUpload rejects a part
// whose ETag does not match the stored part with InvalidPart.
func TestMultipartCompleteInvalidPart(t *testing.T) {
	env := newRegistryEnv(t)
	ctx := context.Background()
	if _, err := env.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	create, err := env.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(testBucket), Key: aws.String("inv/object.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := aws.ToString(create.UploadId)

	if _, err := env.client.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket: aws.String(testBucket), Key: aws.String("inv/object.bin"),
		UploadId: aws.String(uploadID), PartNumber: aws.Int32(1),
		Body: bytes.NewReader(randomBytes(t, 5*1024*1024)),
	}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	_, err = env.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(testBucket), Key: aws.String("inv/object.bin"),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{
				PartNumber: aws.Int32(1),
				ETag:       aws.String(`"deadbeefdeadbeefdeadbeefdeadbeef"`), // wrong ETag
			}},
		},
	})
	if err == nil {
		t.Fatal("CompleteMultipartUpload with bad ETag: expected InvalidPart, got nil")
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "InvalidPart" {
		t.Fatalf("expected InvalidPart, got %v", err)
	}
}

// assertNoReservedRefs fails the test if any reserved multipart part ref remains
// in the test domain's gateway — the leak check shared across the Stage-3 tests.
func assertNoReservedRefs(t *testing.T, env *testEnv) {
	t.Helper()
	infos, err := env.gw.List(context.Background(), mpuPartPrefix, 0)
	if err != nil {
		t.Fatalf("gateway List: %v", err)
	}
	for _, info := range infos {
		if strings.HasPrefix(info.Ref, mpuPartPrefix) {
			t.Errorf("leaked reserved multipart ref: %q", info.Ref)
		}
	}
}
