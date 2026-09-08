# casstore — Content-addressed storage

Content-defined chunking, deduplication within a domain, compression, ranged reads, integrity verification, snapshots, and garbage collection over local disk or S3-compatible storage.

Go module: `github.com/scitrera/memorylayer-storage/casstore`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

`blobstore` provides backing storage, `snapshot` provides object and snapshot operations, and `s3util` provides S3 configuration helpers.

From the repository root:

```sh
cd casstore
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `casstore/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
