# mlfs — POSIX filesystem

A FUSE filesystem with PostgreSQL metadata, content-addressed file slices, durable write-back cache, copy-on-write, ownership leases, invalidation, POSIX ACLs, and local or remote storage paths.

Go module: `github.com/scitrera/memorylayer-storage/mlfs`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

The `mlfs` daemon mounts the filesystem. `mlfs-admin` provides administrative operations and `mlfs-bench` provides workload/metrics tooling. Run each command with `-help`. Linux and FUSE are required for mount tests.

From the repository root:

```sh
cd mlfs
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `mlfs/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
