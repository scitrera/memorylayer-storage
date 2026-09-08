#!/usr/bin/env bash
# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only

# Run the mlfs-csi integration tests (build tag `integration`) inside a
# `--privileged` Docker container, so they can create real kernel mounts (tmpfs
# + FUSE) and spawn real child processes. They are gated behind the tag + a root
# check, so `go test ./...` on a dev box / CI never runs them.
#
# Coverage:
#   - TestIntegrationMultiDomainLifecycleNoLeak / ...RestartReadoptNoLeak:
#       real MountManager + exec launcher + linux mounter against a tmpfs-mounting
#       `fakemlfs` helper — ref-count, teardown, restart re-adoption, leak checks.
#   - TestIntegrationRealMlfsTwoDomainsNoLeak:
#       the REAL mlfs daemon (local single-node mode) under the manager against an
#       ephemeral Postgres — two domains, write/read through FUSE, isolation,
#       clean unmount, leak checks.
#
# Usage:  ./hack/integration-test.sh
# Env:    GO_IMAGE (default golang:1.26.8), PG_IMAGE (default postgres:16-alpine)
set -euo pipefail

GO_IMAGE="${GO_IMAGE:-golang:1.26.8}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
csi_dir="$(cd "$here/.." && pwd)"
storage_dir="$(cd "$csi_dir/.." && pwd)"   # build context (go workspace root)
modcache="$(go env GOMODCACHE 2>/dev/null || echo "$HOME/go/pkg/mod")"

net="mlfs-itest-net-$$"
pg="mlfs-itest-pg-$$"
cleanup() {
  docker rm -fv "$pg" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$net" >/dev/null
docker run -d --name "$pg" --network "$net" \
  -e POSTGRES_HOST_AUTH_METHOD=trust "$PG_IMAGE" >/dev/null

echo "waiting for postgres..."
for _ in $(seq 1 30); do
  docker exec "$pg" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$pg" psql -U postgres -c 'CREATE DATABASE acme;' >/dev/null
docker exec "$pg" psql -U postgres -c 'CREATE DATABASE beta;' >/dev/null

docker run --rm --privileged --network "$net" \
  -v "$storage_dir:/src" \
  -v "$modcache:/go/pkg/mod:ro" \
  -w /src/mlfs-csi \
  -e GOTOOLCHAIN=local -e 'GOFLAGS=-mod=readonly -buildvcs=false' \
  -e GOWORK=/src/go.work -e GOPROXY=off \
  -e MLFS_TEST_DSN_A="postgres://postgres@${pg}:5432/acme?sslmode=disable" \
  -e MLFS_TEST_DSN_B="postgres://postgres@${pg}:5432/beta?sslmode=disable" \
  "$GO_IMAGE" \
  bash -c '
    set -e
    apt-get update >/dev/null 2>&1 && apt-get install -y fuse3 >/dev/null 2>&1
    (cd /src/mlfs && go build -o /tmp/mlfs ./cmd/mlfs)
    export MLFS_TEST_BIN=/tmp/mlfs
    go test -tags integration -run Integration -v ./internal/driver/
  '
