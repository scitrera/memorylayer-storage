# manifeststore — PostgreSQL manifest storage

Stores snapshot manifests in PostgreSQL, including version metadata, batched latest-manifest reads, and payload walking for garbage collection.

Go module: `github.com/scitrera/memorylayer-storage/manifeststore`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Pass a database connection to the store. The manifest database used by garbage collection must be the same source of live manifests used by writers.

From the repository root:

```sh
cd manifeststore
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `manifeststore/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
