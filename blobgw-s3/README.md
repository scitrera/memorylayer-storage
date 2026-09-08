# blobgw-s3 — S3-compatible gateway

Maps S3 bucket/key operations to object references with SigV4 authentication, ranged reads, multipart uploads, and optional JetStream persistence.

Go module: `github.com/scitrera/memorylayer-storage/blobgw-s3`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Run `go run ./cmd/blobgw-s3 -help`. This implements a subset of the S3 API; validate the operations required by your client. Durable bucket, credential, and multipart state requires the configured persistent backend.

From the repository root:

```sh
cd blobgw-s3
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `blobgw-s3/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
