#!/usr/bin/env bash
# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only

# Shared helper for the mlfs test harnesses (test/fstest, test/fio).
#
# Source this file, then call `mlfs_up` to build the daemon and bring up a real
# mount, and `mlfs_down` to tear it down. It mounts via the actual cmd/mlfs
# binary so the harnesses exercise the same stack operators run.
#
# Inputs (env):
#   MLFS_META_DSN   pgx DSN for the metadata engine. If unset, a throwaway
#                   Postgres container is started via docker (and removed on
#                   teardown).
#   MLFS_MNT        mountpoint. Default: a fresh temp dir.
#   GO              Go executable. Default: `go` on PATH.
#
# Exports after mlfs_up: MLFS_MNT (mountpoint), MLFS_PID (daemon pid).
set -euo pipefail

_MLFS_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)" # the mlfs module root
: "${GO:=go}"
command -v "$GO" >/dev/null 2>&1 || { echo "Go toolchain not found: $GO" >&2; exit 1; }

_MLFS_PG=""        # docker container name if we started one
_MLFS_TMP=""       # temp scratch dir we own
_MLFS_STARTED_MNT="" # set once mounted, for teardown

mlfs_up() {
	command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
		echo "SKIP: fusermount not found — FUSE not available here" >&2; exit 0; }
	[ -e /dev/fuse ] || { echo "SKIP: /dev/fuse missing — FUSE not available here" >&2; exit 0; }

	_MLFS_TMP="$(mktemp -d)"
	local bin="$_MLFS_TMP/mlfs"
	( cd "$_MLFS_REPO" && "$GO" build -o "$bin" ./cmd/mlfs )

	if [ -z "${MLFS_META_DSN:-}" ]; then
		command -v docker >/dev/null 2>&1 || { echo "SKIP: no MLFS_META_DSN and docker unavailable" >&2; exit 0; }
		_MLFS_PG="mlfs-harness-$$"
		local port=55470
		docker rm -f "$_MLFS_PG" >/dev/null 2>&1 || true
		docker run -d --name "$_MLFS_PG" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=mlfs -p "$port:5432" postgres:16-alpine >/dev/null
		local i; for i in $(seq 1 30); do docker exec "$_MLFS_PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
		MLFS_META_DSN="postgres://postgres:test@127.0.0.1:$port/mlfs?sslmode=disable"
	fi

	MLFS_MNT="${MLFS_MNT:-$_MLFS_TMP/mnt}"
	mkdir -p "$MLFS_MNT"
	# MLFS_EXTRA_ARGS appends daemon flags (word-split intentionally), e.g.
	# MLFS_EXTRA_ARGS="-allow-other" for multi-uid (pjdfstest) runs.
	"$bin" -mount "$MLFS_MNT" -meta-dsn "$MLFS_META_DSN" \
		-data-dir "$_MLFS_TMP/data" -cache-dir "$_MLFS_TMP/cache" -domain harness \
		${MLFS_EXTRA_ARGS:-} &
	MLFS_PID=$!
	local i; for i in $(seq 1 100); do mountpoint -q "$MLFS_MNT" && break; sleep 0.1; done
	mountpoint -q "$MLFS_MNT" || { echo "FAIL: mlfs did not mount at $MLFS_MNT" >&2; mlfs_down; exit 1; }
	_MLFS_STARTED_MNT="$MLFS_MNT"
	export MLFS_MNT MLFS_PID
	echo "mlfs mounted at $MLFS_MNT (pid $MLFS_PID)" >&2
}

mlfs_down() {
	[ -n "${MLFS_PID:-}" ] && kill -TERM "$MLFS_PID" 2>/dev/null || true
	[ -n "${MLFS_PID:-}" ] && wait "$MLFS_PID" 2>/dev/null || true
	# Belt-and-suspenders: force-unmount if the daemon didn't.
	[ -n "$_MLFS_STARTED_MNT" ] && mountpoint -q "$_MLFS_STARTED_MNT" 2>/dev/null && \
		( fusermount3 -u "$_MLFS_STARTED_MNT" 2>/dev/null || fusermount -u "$_MLFS_STARTED_MNT" 2>/dev/null || true )
	[ -n "$_MLFS_PG" ] && docker rm -f "$_MLFS_PG" >/dev/null 2>&1 || true
	[ -n "$_MLFS_TMP" ] && rm -rf "$_MLFS_TMP" || true
}
