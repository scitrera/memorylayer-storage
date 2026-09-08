# blobgw — Object gateway and control plane

HTTP object operations, staged uploads, metadata and pagination, a PostgreSQL dedup/ref index, tenant backend bindings, NATS control-plane requests, and garbage collection.

Go module: `github.com/scitrera/memorylayer-storage/blobgw`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Run `go run ./cmd/blobgw -help` for configuration. The local quickstart is in the root README. Production deployments select persistent indexes, tenant bindings, and credentials.

From the repository root:

```sh
cd blobgw
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `blobgw/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
