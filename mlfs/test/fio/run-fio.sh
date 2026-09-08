#!/usr/bin/env bash
# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only

# Performance gating for mlfs via fio. Brings up a real mlfs mount and runs the
# job files in this directory against it. Acceptance targets live in
# docs/operations.md: 1M seqread >= 200 MB/s warm. The 4K randwrite fsync=1 gate
# is fsync-latency-bound (~device_fsync_rate/4 for mlfs); measure against the backing device —
# target a fraction of the device's measured fsync rate, not an absolute 20K.
#
#   ./run-fio.sh                       # auto: throwaway PG + temp mount, all jobs
#   ./run-fio.sh randwrite-4k-qd32.fio # one job
#   MLFS_META_DSN=... ./run-fio.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../lib/mount.sh"

command -v fio >/dev/null 2>&1 || { echo "SKIP: fio not installed (apt-get install fio)" >&2; exit 0; }

trap mlfs_down EXIT
mlfs_up

jobs=("$@")
[ "${#jobs[@]}" -eq 0 ] && jobs=("$HERE"/*.fio)

for job in "${jobs[@]}"; do
	[ -f "$job" ] || job="$HERE/$job"
	echo "=== fio: $(basename "$job") (DIRECTORY=$MLFS_MNT) ===" >&2
	fio --directory="$MLFS_MNT" "$job"
done
