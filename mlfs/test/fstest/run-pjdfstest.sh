#!/usr/bin/env bash
# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only

# POSIX conformance gating for mlfs via pjdfstest (https://github.com/pjd/pjdfstest).
#
# Brings up a real mlfs mount (test/lib/mount.sh), then runs the pjdfstest suite
# under `prove` inside the mount and reports the pass rate. Acceptance target:
# >= 95% (deliberate deviations documented in docs/operations.md).
#
#   ./run-pjdfstest.sh                 # auto: throwaway PG + temp mount
#   MLFS_META_DSN=... ./run-pjdfstest.sh
#   PJDFSTEST_DIR=/path/to/pjdfstest ./run-pjdfstest.sh   # use a prebuilt checkout
#
# NOTE: pjdfstest must run as root for the full matrix — chown/chmod/sticky/
# permission tests require switching uid/gid. Run under sudo for a true ≥95%
# number; as non-root those cases are skipped (and the rate is not comparable).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../lib/mount.sh"

command -v prove >/dev/null 2>&1 || { echo "SKIP: 'prove' (perl Test::Harness) not installed" >&2; exit 0; }

# Obtain pjdfstest (prebuilt dir, else clone+build).
PJDFSTEST_DIR="${PJDFSTEST_DIR:-}"
_built_tmp=""
if [ -z "$PJDFSTEST_DIR" ]; then
	command -v git >/dev/null 2>&1 || { echo "SKIP: git not installed and PJDFSTEST_DIR unset" >&2; exit 0; }
	command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 || { echo "SKIP: no C compiler to build pjdfstest" >&2; exit 0; }
	_built_tmp="$(mktemp -d)"
	echo "cloning pjdfstest into $_built_tmp ..." >&2
	git clone --depth 1 https://github.com/pjd/pjdfstest "$_built_tmp/pjdfstest" >/dev/null 2>&1
	( cd "$_built_tmp/pjdfstest" && autoreconf -ifs >/dev/null 2>&1 && ./configure >/dev/null 2>&1 && make pjdfstest >/dev/null 2>&1 ) || {
		echo "SKIP: pjdfstest build failed (need autoconf + make + cc)" >&2; rm -rf "$_built_tmp"; exit 0; }
	PJDFSTEST_DIR="$_built_tmp/pjdfstest"
fi

cleanup() { mlfs_down; [ -n "$_built_tmp" ] && rm -rf "$_built_tmp" || true; }
trap cleanup EXIT

mlfs_up
[ "$(id -u)" -eq 0 ] || echo "WARN: not root — chown/permission cases will be skipped; pass rate not comparable to the ≥95% target" >&2

# pjdfstest is driven by prove over its tests/ tree, run with CWD inside the FS.
workdir="$MLFS_MNT/pjdfstest-run"
mkdir -p "$workdir"
echo "running pjdfstest under $workdir ..." >&2
set +e
( cd "$workdir" && prove -rf "$PJDFSTEST_DIR/tests" :: "$PJDFSTEST_DIR/pjdfstest" ) | tee "${PJDFSTEST_LOG:-/dev/stderr}"
rc=${PIPESTATUS[0]}
set -e
echo "pjdfstest prove exit: $rc" >&2
exit "$rc"
