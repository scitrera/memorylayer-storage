# blobgw-edge — Capability gateway

Authorizes external object access with Ed25519 capability tokens and applies tenant routing, quotas, revocation, and policy checks.

Go module: `github.com/scitrera/memorylayer-storage/blobgw-edge`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Run `go run ./cmd/blobgw-edge -help`. Use `-policy=postgres` (with
`-database-url`) for deployments that require durable revocation and quotas.

## Signing key

The edge both mints and verifies capability tokens with a single Ed25519 key
pair. There is no separate public-key configuration: the verifier trusts only
the public half of the edge's own signing key.

- `-key-seed-hex` takes a 32-byte Ed25519 seed as 64 hex characters. The key id
  (`kid`) stamped into each token is derived from the public key, so a given
  seed always produces the same key and `kid`.
- If `-key-seed-hex` is empty (the default), the edge generates an ephemeral
  key at startup. Every restart then invalidates all outstanding capabilities,
  and replicas cannot verify each other's tokens.

Production deployments must set a stable seed, sourced from a secret, and every
replica behind the same `-audience` must use the same seed. Only one key is
active at a time, so changing the seed invalidates all capabilities minted
under the old key.

From the repository root:

```sh
cd blobgw-edge
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `blobgw-edge/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
