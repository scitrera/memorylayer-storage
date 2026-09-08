# Testing

Install Go 1.26.8+, Python 3.10+, uv, and the client test extra:

```sh
uv venv
. .venv/bin/activate
uv pip install -e './clients/python[test]'
make build test test-python
```

The Go suite includes embedded NATS servers, HTTP tests, filesystem metadata
logic, storage integrity, and CSI tests with simulated mounts. The Python suite
builds and talks to a real gateway; missing Go, build failures, and failed gateway
startup are errors.

Database tests are enabled with disposable PostgreSQL DSNs:

```sh
export MLFS_TEST_DATABASE_URL='postgres://storage:storage@localhost:5432/storage?sslmode=disable'
export BLOBGW_TEST_DATABASE_URL="$MLFS_TEST_DATABASE_URL"
make test
```

`make dev-up` provides that local database. CI supplies a PostgreSQL service to
each Go test job and runs the race detector. When the DSN is unset, database-gated
tests skip; a configured unreachable mlfs/manifest database fails the test.

Real kernel/FUSE and CSI mount lifecycle tests require Linux, `/dev/fuse`, and
mount privileges. Run `mlfs-csi/hack/integration-test.sh` on an appropriate Docker
host. `mlfs/test/fstest/` and `mlfs/test/fio/` contain additional acceptance
harnesses. Passing ordinary CI does not establish that these privileged tests,
a Kubernetes PVC soak, or workload-specific throughput acceptance have run.
