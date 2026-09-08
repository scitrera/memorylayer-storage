# Operations

## Local development

`make dev-up` starts PostgreSQL and blobgw with persistent Docker volumes.
The object API is at `http://127.0.0.1:8080`; health endpoints are `/healthz`
and `/readyz`. The Compose configuration binds published ports to localhost.
It uses development credentials, in-memory staging, and disables scheduled GC.
`make dev-down` stops the services while retaining stored data. Set
`POSTGRES_PORT` and `BLOBGW_PORT` to override the default localhost ports.

Run each daemon with `-help` to see its configuration flags. The `docker/`
directory contains a build definition for each daemon; every build uses the
repository root as its context so all workspace modules are available.

## Remote filesystem deployments

A remote deployment needs S3-compatible object storage, PostgreSQL metadata and
manifest databases, a blobgw control plane, and NATS connectivity with credentials
for each domain. Configure domain/backend bindings before mounting clients.

Use [control-plane accounts](blobgw-control-plane-accounts.md) and
[multi-tenant coordination](multi-tenant-coordination.md) to configure account
boundaries. The CSI [reference manifests](../mlfs-csi/deploy/mlfs-csi.yaml) are
examples to adapt; database and credential provisioning belongs to the operator.

Back up filesystem metadata, manifests, object refs, and their corresponding
backing packs together. A bucket backup alone does not preserve the filesystem
namespace or object reference index. Exercise restore with the same component
versions before upgrading an existing deployment.

## Garbage collection and compression

Garbage collection must read the manifest store used by writers. In particular,
remote mlfs manifests in PostgreSQL must be included in blobgw's GC live set;
configure `-manifest-dsn-file` for those domains. Pointing GC at an empty S3
manifest namespace cannot establish liveness for PostgreSQL-backed manifests.

Choose GC retention/safety windows that exceed the supported write-back and
client-offline intervals. Use maintenance windows for administrative operations
that require quiescent writers. Reclamation and compaction affect persistent
storage: validate the exact configuration on disposable data first.

Compression and pack-size choices affect ranged-read cost. Validate cold reads,
sequential throughput, and mmap-based workloads on representative files before
changing the compression policy of a model-serving filesystem.

See [observability](OBSERVABILITY.md) for instrument names and
[data model](data-model.md) for persistent schemas and formats.
