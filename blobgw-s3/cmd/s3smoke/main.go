// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command s3smoke drives a RUNNING blobgw-s3 daemon over real HTTP with
// aws-sdk-go-v2 (static creds + path-style) to prove the live S3 surface end to
// end. It is invoked by .slop/blobgw-s3-smoke.sh; it is not part of the shipping
// product, just the integration harness's client side.
//
// Flow: CreateBucket → PutObject (small) → GetObject (full + Range, byte check)
// → HeadObject → PutObject (>5 MiB via the multipart manager) → GetObject (full
// + Range, byte check) → ListObjectsV2 → a second >5 MiB PutObject of the SAME
// bytes under a different key (the dedup proof the bash side measures on disk).
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/scitrera/memorylayer-storage/casstore/s3util"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8095", "blobgw-s3 daemon endpoint")
	region := flag.String("region", "us-east-1", "SigV4 region")
	accessKey := flag.String("access-key", "", "access key id")
	secretKey := flag.String("secret-key", "", "secret key")
	bucket := flag.String("bucket", "smoke", "bucket name")
	flag.Parse()

	if *accessKey == "" || *secretKey == "" {
		fail("-access-key and -secret-key are required")
	}

	ctx := context.Background()
	client, err := s3util.NewClient(ctx, s3util.Config{
		Region:         *region,
		Endpoint:       *endpoint,
		AccessKey:      *accessKey,
		SecretKey:      *secretKey,
		ForcePathStyle: true,
	})
	if err != nil {
		fail("s3 client: %v", err)
	}

	// 1. CreateBucket.
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: bucket}); err != nil {
		fail("CreateBucket: %v", err)
	}
	fmt.Println("OK CreateBucket", *bucket)

	// 2. PutObject (small) + GetObject full + Range + HeadObject.
	small := randomBytes(4096)
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      bucket,
		Key:         aws.String("small.bin"),
		Body:        bytes.NewReader(small),
		ContentType: aws.String("application/octet-stream"),
		Metadata:    map[string]string{"origin": "s3smoke"},
	}); err != nil {
		fail("PutObject small: %v", err)
	}
	fmt.Println("OK PutObject small", len(small), "bytes")

	getFull(ctx, client, *bucket, "small.bin", small)
	getRange(ctx, client, *bucket, "small.bin", small, 100, 199)

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: bucket, Key: aws.String("small.bin")})
	if err != nil {
		fail("HeadObject: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(small)) {
		fail("HeadObject size: got %v want %d", head.ContentLength, len(small))
	}
	if head.Metadata["origin"] != "s3smoke" {
		fail("HeadObject metadata: %+v", head.Metadata)
	}
	fmt.Println("OK HeadObject size + metadata")

	// 3. >5 MiB multipart PutObject via the manager Uploader (forces real
	// CreateMultipartUpload/UploadPart/Complete against the live daemon).
	large := randomBytes(8 * 1024 * 1024)
	putMultipart(ctx, client, *bucket, "large-a.bin", large)
	fmt.Println("OK PutObject multipart (8 MiB) large-a.bin")

	getFull(ctx, client, *bucket, "large-a.bin", large)
	getRange(ctx, client, *bucket, "large-a.bin", large, 5_000_000, 5_000_999)
	fmt.Println("OK GetObject multipart full + Range byte-identical")

	// 4. ListObjectsV2.
	list, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: bucket})
	if err != nil {
		fail("ListObjectsV2: %v", err)
	}
	keys := map[string]bool{}
	for _, o := range list.Contents {
		keys[aws.ToString(o.Key)] = true
	}
	if !keys["small.bin"] || !keys["large-a.bin"] {
		fail("ListObjectsV2 missing keys: %v", keys)
	}
	fmt.Println("OK ListObjectsV2", len(list.Contents), "objects")

	// 5. Dedup proof: put the SAME large bytes under a different key. casstore
	// must dedup the chunks, so the on-disk chunk dir stays ~flat (the bash
	// harness measures the bytes before/after this step).
	putMultipart(ctx, client, *bucket, "large-b.bin", large)
	fmt.Println("OK PutObject multipart (8 MiB, identical bytes) large-b.bin")
	getFull(ctx, client, *bucket, "large-b.bin", large)
	fmt.Println("OK GetObject large-b.bin byte-identical")

	fmt.Println("S3SMOKE OK")
}

func putMultipart(ctx context.Context, client *awss3.Client, bucket, key string, data []byte) {
	up := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = 5 * 1024 * 1024 // force >1 part for an 8 MiB body
		o.Concurrency = 2
		// transfermanager decides multipart vs single by MultipartUploadThreshold
		// (default 16 MiB), unlike the old manager.Uploader which switched on
		// PartSize. Pin the threshold to the part size to preserve the old
		// "multipart whenever body > part size" behavior for the 8 MiB body.
		o.MultipartUploadThreshold = 5 * 1024 * 1024
	})
	if _, err := up.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	}); err != nil {
		fail("multipart upload %s: %v", key, err)
	}
}

func getFull(ctx context.Context, client *awss3.Client, bucket, key string, want []byte) {
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		fail("GetObject %s: %v", key, err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if !bytes.Equal(got, want) {
		fail("GetObject %s body mismatch: got %d bytes want %d", key, len(got), len(want))
	}
}

func getRange(ctx context.Context, client *awss3.Client, bucket, key string, full []byte, start, end int) {
	rng := fmt.Sprintf("bytes=%d-%d", start, end)
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(rng),
	})
	if err != nil {
		fail("GetObject %s range %s: %v", key, rng, err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if !bytes.Equal(got, full[start:end+1]) {
		fail("GetObject %s range %s mismatch: got %d bytes want %d", key, rng, len(got), end-start+1)
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		fail("rand: %v", err)
	}
	return b
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "s3smoke FAIL: "+format+"\n", args...)
	os.Exit(1)
}
