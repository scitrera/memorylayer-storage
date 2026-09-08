# ctlproto — Control-plane protocol

Wire types and codecs for the storage control plane, including tenant validation, bounded batch requests, dedup-index operations, pack associations, and presigned access.

Go module: `github.com/scitrera/memorylayer-storage/ctlproto`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

The `.proto` file describes the contract; the Go implementation provides the currently supported codecs. Coordinate wire changes with both clients and servers.

From the repository root:

```sh
cd ctlproto
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `ctlproto/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
