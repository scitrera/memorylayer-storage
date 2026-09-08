# MemoryLayer Storage

MemoryLayer Storage provides content-addressed object storage and a distributed
POSIX filesystem. Applications can use its Go libraries, HTTP object API,
S3-compatible gateway, Python client, or Kubernetes CSI volumes independently.

Initial release: **0.7.1**. Components have independent versions in
[`versions.yaml`](versions.yaml).

| Component | Purpose |
|---|---|
| [casstore](casstore) | Chunking, deduplication, compression, snapshots, and garbage collection |
| [ctlproto](ctlproto) | Storage control-plane wire contract |
| [manifeststore](manifeststore) | PostgreSQL snapshot manifests |
| [blobgw](blobgw) | HTTP object gateway and NATS storage control plane |
| [blobgw-edge](blobgw-edge) | Capability authentication, tenant routing, quotas, and revocation |
| [blobgw-s3](blobgw-s3) | S3-compatible object access |
| [mlfs](mlfs) | FUSE filesystem with PostgreSQL metadata and durable write-back cache |
| [mlfs-csi](mlfs-csi) | Kubernetes CSI driver and domain mount management |
| [blobgw-client](clients/python) | Python HTTP client and storage-service adapter |

## Try the object gateway

Requires Go 1.26.8 or newer, plus Python 3.10+ and uv for the client example.
From a checkout:

```sh
cd blobgw
go run ./cmd/blobgw -addr 127.0.0.1:8080 -domain demo \
  -backend local -data-dir /tmp/memorylayer-demo \
  -index memory -staging memory -gc-interval 0
```

In another terminal, from the repository root:

```sh
uv venv
. .venv/bin/activate
uv pip install ./clients/python
python - <<'PYTHON'
from blobgw_client import BlobGWClient
client = BlobGWClient("http://127.0.0.1:8080")
client.put("demo/hello.txt", b"Hello, MemoryLayer!", "text/plain")
assert client.get("demo/hello.txt") == b"Hello, MemoryLayer!"
print(client.head("demo/hello.txt"))
PYTHON
```

This quickstart uses in-memory indexes and staging for a disposable local demo.
For persistent local development, `make dev-up` starts PostgreSQL and a gateway
with a persistent local backing directory. See [operations](docs/operations.md)
for the remote storage and multi-tenant deployment model.

## Develop

```sh
make build             # all eight Go modules
make test              # Go tests, with the race detector
make test-python       # Python client against a real gateway
make check             # formatting, vet, build, Go and Python tests, CI/version drift
```

PostgreSQL tests run when `MLFS_TEST_DATABASE_URL` and
`BLOBGW_TEST_DATABASE_URL` point to a disposable database. Real FUSE/CSI tests
also need Linux, `/dev/fuse`, and mount privileges; see [testing](docs/testing.md).

## Releases and licenses

Go imports use `github.com/scitrera/memorylayer-storage/<component>`.
Each Go module publishes a `<component>/vX.Y.Z` tag; the Python client uses
`clients/python/vX.Y.Z`. A component fix does not require releasing its siblings.
The Python client is distributed from Git tags for now. Once its tag is published,
install it into a virtual environment with:

```sh
uv pip install "blobgw-client @ git+https://github.com/scitrera/memorylayer-storage.git@clients/python/v0.7.1#subdirectory=clients/python"
```

See [release management](docs/releases.md) and [architecture](docs/architecture.md).

The storage stack is licensed under [AGPL-3.0-only](LICENSE).
The Python client in `clients/python/` is licensed under
[Apache-2.0](clients/python/LICENSE). Dependencies retain their own licenses;
see [third-party notices](THIRD_PARTY_NOTICES.md).
