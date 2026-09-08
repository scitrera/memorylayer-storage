// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// restart returns a NEW S3 client backed by a freshly constructed handler over
// the SAME gateway/backend as env. It models a process restart: the new handler
// has no in-memory state of its own (the object-metadata sidecar is gone), so
// any S3 presentation metadata it serves must come from the durable casstore
// manifest via the gateway. The multipart store is fresh too (in-flight uploads
// don't survive a restart, which is fine — we only assert COMPLETED objects).
func (env *testEnv) restart(t *testing.T) *awss3.Client {
	t.Helper()
	creds := NewMemoryCredentialStore(Credential{
		AccessKeyID: testAccessKey,
		SecretKey:   testSecretKey,
		Domain:      testDomain,
	})
	h := New(Config{Router: singleRouter{env.gw}, Creds: creds})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  awscreds.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	})
}

// TestDurability_PutObjectSurvivesRestart is the tech-debt #38 acceptance test:
// PutObject (with a Content-Type and an x-amz-meta-* header) through one handler,
// then a FRESH handler over the same backend (sidecar gone) must still report the
// correct ETag, Content-Type, and user metadata on Head and Get.
func TestDurability_PutObjectSurvivesRestart(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	payload := randomBytes(t, 512*1024)

	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(testBucket),
		Key:         aws.String("durable/report.bin"),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/x-custom"),
		Metadata:    map[string]string{"origin": "ingest-svc", "page": "7"},
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	wantETag := `"` + md5hex(payload) + `"`

	// Restart: brand-new handler instance, no sidecar.
	client := env.restart(t)

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("durable/report.bin"),
	})
	if err != nil {
		t.Fatalf("HeadObject after restart: %v", err)
	}
	if aws.ToString(head.ETag) != wantETag {
		t.Errorf("ETag lost across restart: got %q want %q", aws.ToString(head.ETag), wantETag)
	}
	if aws.ToString(head.ContentType) != "application/x-custom" {
		t.Errorf("ContentType lost across restart: got %q", aws.ToString(head.ContentType))
	}
	if head.Metadata["origin"] != "ingest-svc" || head.Metadata["page"] != "7" {
		t.Errorf("user metadata lost across restart: %+v", head.Metadata)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("durable/report.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject after restart: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("body mismatch after restart: got %d bytes want %d", len(got), len(payload))
	}
	if aws.ToString(out.ETag) != wantETag {
		t.Errorf("GET ETag across restart: got %q want %q", aws.ToString(out.ETag), wantETag)
	}
	if out.Metadata["origin"] != "ingest-svc" {
		t.Errorf("GET user metadata across restart: %+v", out.Metadata)
	}

	// ListObjectsV2 must also surface the durable ETag after a restart.
	list, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(testBucket),
		Prefix: aws.String("durable/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 after restart: %v", err)
	}
	var found bool
	for _, o := range list.Contents {
		if aws.ToString(o.Key) == "durable/report.bin" {
			found = true
			if aws.ToString(o.ETag) != wantETag {
				t.Errorf("List ETag across restart: got %q want %q", aws.ToString(o.ETag), wantETag)
			}
		}
	}
	if !found {
		t.Error("ListObjectsV2 did not return the durable object")
	}
}

// TestDurability_MultipartETagSurvivesRestart proves a completed multipart
// upload's composite "-N" ETag (and its user metadata) are stored durably and
// recovered by a fresh handler over the same backend.
func TestDurability_MultipartETagSurvivesRestart(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	key := "durable/big.bin"

	create, err := env.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String(testBucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/x-mpu"),
		Metadata:    map[string]string{"origin": "mpu-svc"},
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := aws.ToString(create.UploadId)

	const nParts = 3
	part := randomBytes(t, 256*1024)
	completed := make([]types.CompletedPart, 0, nParts)
	for i := 1; i <= nParts; i++ {
		up, err := env.client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:     aws.String(testBucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(i)),
			Body:       bytes.NewReader(part),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
		completed = append(completed, types.CompletedPart{
			ETag:       up.ETag,
			PartNumber: aws.Int32(int32(i)),
		})
	}

	comp, err := env.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String(testBucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	wantETag := aws.ToString(comp.ETag)
	if wantETag == "" {
		t.Fatal("complete returned empty ETag")
	}

	// Restart: fresh handler, no sidecar.
	client := env.restart(t)

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject after restart: %v", err)
	}
	if aws.ToString(head.ETag) != wantETag {
		t.Errorf("multipart ETag lost across restart: got %q want %q", aws.ToString(head.ETag), wantETag)
	}
	if aws.ToString(head.ContentType) != "application/x-mpu" {
		t.Errorf("ContentType lost across restart: got %q", aws.ToString(head.ContentType))
	}
	if head.Metadata["origin"] != "mpu-svc" {
		t.Errorf("user metadata lost across restart: %+v", head.Metadata)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject after restart: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if len(got) != nParts*len(part) {
		t.Errorf("assembled size after restart: got %d want %d", len(got), nParts*len(part))
	}
	if aws.ToString(out.ETag) != wantETag {
		t.Errorf("GET multipart ETag across restart: got %q want %q", aws.ToString(out.ETag), wantETag)
	}
}
