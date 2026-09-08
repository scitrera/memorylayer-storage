# blobgw-edge — Capability gateway

Authorizes external object access with Ed25519 capability tokens and applies tenant routing, quotas, revocation, and policy checks.

Go module: `github.com/scitrera/memorylayer-storage/blobgw-edge`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Run `go run ./cmd/blobgw-edge -help`. Supply signing/public-key configuration and persistent policy storage for deployments that require durable revocation and quotas.

From the repository root:

```sh
cd blobgw-edge
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `blobgw-edge/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
