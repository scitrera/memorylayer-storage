# blobgw-s3 — S3-compatible gateway

Maps S3 bucket/key operations to object references with SigV4 authentication, ranged reads, multipart uploads, and optional JetStream persistence.

Go module: `github.com/scitrera/memorylayer-storage/blobgw-s3`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Run `go run ./cmd/blobgw-s3 -help`. This implements a subset of the S3 API; validate the operations required by your client.

## Limitations of the shipped command

The `blobgw-s3` binary wires a single local-filesystem profile: manifests and
packs under `-data-dir`, with **in-memory** dedup and object-reference indexes.
It has no flag for a persistent (Postgres) index. Treat it as a development and
evaluation gateway.

- **Objects do not survive a restart.** The object-reference index maps each
  S3 key to its stored object, and it lives only in process memory. After a
  restart, `GetObject`/`HeadObject` return `NoSuchKey` and `ListObjects` is
  empty, even though the manifests and packs are still on disk. Those stranded
  objects cannot be deleted through the S3 API, and garbage collection keeps
  their packs because their manifests still exist. Multipart parts are stored
  the same way, so in-flight uploads do not survive a restart either.
- **`-registry-backend=jetstream` does not change this.** It makes the bucket,
  credential, and multipart-upload registries durable in NATS JetStream KV. It
  does not persist the object-reference or dedup indexes.
- **Dedup restarts cold.** The dedup index is rebuilt from nothing on each
  start, so content written before a restart is not deduplicated against.

## Buckets share a key namespace per domain

Each bucket is pinned, at `CreateBucket`, to the dedup domain of the credential
that created it. Object keys are stored per domain and are **not** prefixed by
bucket name. Every bucket in the same domain therefore sees the same set of
objects:

- `PUT a/x` followed by `GET b/x` returns the object written through bucket `a`.
- `ListObjects` on any bucket in the domain lists every object in the domain.
- `DeleteBucket` fails with `BucketNotEmpty` if any bucket in the domain holds
  objects.

For independent key spaces, create each bucket with a credential mapped to its
own domain. Content is then not deduplicated across those buckets, because the
domain is also the dedup namespace.

From the repository root:

```sh
cd blobgw-s3
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `blobgw-s3/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
