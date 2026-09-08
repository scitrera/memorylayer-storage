# mlfs-csi — Kubernetes CSI driver

Provisions and mounts domain-scoped filesystem volumes. Pod mode manages a separate mount pod for each domain; the container image bundles the CSI driver and the mlfs daemon.

Go module: `github.com/scitrera/memorylayer-storage/mlfs-csi`.
Initial version: **0.7.1**, licensed under [AGPL-3.0-only](LICENSE).

Start from `deploy/mlfs-csi.yaml` and provide your own metadata database, NATS accounts, backend bindings, and credential Secrets. The bundled mlfs version is selected in the image build from the workspace source; bump the CSI image version whenever that bundle changes.

From the repository root:

```sh
cd mlfs-csi
go test ./...
go build ./...
```

The root `go.work` resolves sibling modules for development. Published module
versions use `mlfs-csi/vX.Y.Z` tags. See [development and releases](../docs/releases.md)
and [operations](../docs/operations.md).
