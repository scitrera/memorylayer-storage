// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package s3stage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/scitrera/memorylayer-storage/blobgw/s3stage"
	"github.com/scitrera/memorylayer-storage/casstore/s3util"
)

// testConfig reads the RustFS/S3 endpoint from the environment, skipping the
// test when unset (mirrors casstore's RUSTFS_TEST_* convention).
//
//	BLOBGW_TEST_S3_ENDPOINT   e.g. http://127.0.0.1:59000
//	BLOBGW_TEST_S3_BUCKET     e.g. blobgw-test
//	BLOBGW_TEST_S3_ACCESS_KEY / BLOBGW_TEST_S3_SECRET_KEY
func testConfig(t *testing.T) s3stage.Config {
	t.Helper()
	endpoint := os.Getenv("BLOBGW_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOBGW_TEST_S3_ENDPOINT (+ _BUCKET/_ACCESS_KEY/_SECRET_KEY) to run s3stage tests")
	}
	bucket := os.Getenv("BLOBGW_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "blobgw-test"
	}
	return s3stage.Config{
		Bucket:         bucket,
		Region:         "us-east-1",
		Endpoint:       endpoint,
		AccessKey:      os.Getenv("BLOBGW_TEST_S3_ACCESS_KEY"),
		SecretKey:      os.Getenv("BLOBGW_TEST_S3_SECRET_KEY"),
		ForcePathStyle: true,
	}
}

// ensureBucket creates the test bucket if it doesn't already exist.
func ensureBucket(t *testing.T, cfg s3stage.Config) {
	t.Helper()
	ctx := context.Background()
	// s3stage.Config is an alias for s3util.Config, so the test client shares the
	// production client-construction path (BaseEndpoint + UsePathStyle) instead of
	// the deprecated global endpoint resolver.
	cli, err := s3util.NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		// A pre-existing bucket (BucketAlreadyExists/OwnedByYou) is fine; only
		// a genuine connectivity/permission failure should surface, and the
		// subsequent PUT will fail loudly if the bucket truly isn't usable.
		t.Logf("CreateBucket (ok if bucket already exists): %v", err)
	}
}

func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}

func TestS3Stage_PresignOpenDelete(t *testing.T) {
	cfg := testConfig(t)
	ensureBucket(t, cfg)
	ctx := context.Background()

	store, err := s3stage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	key := "s3stage-test/" + randHex(t, 8)
	payload := make([]byte, 256*1024)
	_, _ = rand.Read(payload)

	// Mint presigned PUT and upload to it as a plain client would.
	url, exp, err := store.PresignPut(ctx, key, 0, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	if !exp.After(time.Now()) {
		t.Errorf("expiry %v not in the future", exp)
	}
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT to presigned URL: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("presigned PUT status %d", resp.StatusCode)
	}

	// Open reads the staged bytes back exactly.
	rc, err := store.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("staged round-trip mismatch: %d vs %d bytes", len(got), len(payload))
	}

	// Delete removes it; a subsequent Open fails.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Open(ctx, key); err == nil {
		t.Error("Open after Delete should fail")
	}
}
