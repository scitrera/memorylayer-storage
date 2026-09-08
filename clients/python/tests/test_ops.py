# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Live smoke tests for the operational endpoints (A6).

These exercise the unversioned ops surface (/healthz, /readyz, /metrics) against a
real running blobgw binary — the readiness probe, the Prometheus exposition, and
the request-metrics middleware — paths that unit tests alone don't drive end-to-end.
"""

from __future__ import annotations

import urllib.request

from blobgw_client import BlobGWClient


def _get(base: str, path: str) -> tuple[int, str, dict[str, str]]:
    req = urllib.request.Request(base + path, method="GET")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, resp.read().decode(), dict(resp.headers)


def test_healthz_is_liveness_200(blobgw_server):
    status, _, _ = _get(blobgw_server, "/healthz")
    assert status == 200


def test_readyz_reports_ready(blobgw_server):
    # The fixture runs a local backend + memory index, so dependencies are usable.
    status, _, _ = _get(blobgw_server, "/readyz")
    assert status == 200


def test_metrics_exposition_and_increment(blobgw_server):
    # Prometheus text exposition with the documented blobgw_* series.
    status, body, headers = _get(blobgw_server, "/metrics")
    assert status == 200
    assert "text/plain" in headers.get("Content-Type", "")
    assert "# TYPE blobgw_requests_total counter" in body
    assert "blobgw_requests_total" in body

    # A data-plane request must move the request counter.
    client = BlobGWClient(blobgw_server)
    ref = "ops/metrics/probe.bin"
    client.put(ref, b"hello-metrics", "application/octet-stream")
    assert client.get(ref) == b"hello-metrics"
    client.delete(ref)

    _, body2, _ = _get(blobgw_server, "/metrics")
    # bytes_put counter is cumulative and should now be non-zero.
    assert "blobgw_bytes_put_total" in body2
