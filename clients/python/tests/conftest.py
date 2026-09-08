# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Pytest fixtures: build and run a real blobgw binary for client tests.

The client is exercised against the actual gateway (local backend, memory
staging) rather than a mock, so the tests verify real HTTP interop. If the Go
toolchain or the blobgw module can't be found, the fixture skips.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

import pytest

# blobgw module lives two levels up from clients/python.
_REPO_STORAGE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_BLOBGW_DIR = os.path.join(_REPO_STORAGE, "blobgw")
def _find_go() -> str | None:
    return shutil.which(os.environ.get("GO", "go"))


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="session")
def blobgw_server():
    """Build + run blobgw (local backend) and yield its base URL."""
    go = _find_go()
    if go is None:
        pytest.fail("go toolchain not found; cannot build blobgw for client tests")
    if not os.path.isdir(_BLOBGW_DIR):
        pytest.fail(f"blobgw module not found at {_BLOBGW_DIR}")

    tmp = tempfile.mkdtemp(prefix="blobgw-client-test-")
    binpath = os.path.join(tmp, "blobgw")
    env = dict(os.environ)
    env["PATH"] = os.path.dirname(go) + os.pathsep + env.get("PATH", "")

    build = subprocess.run(
        [go, "build", "-o", binpath, "./cmd/blobgw"],
        cwd=_BLOBGW_DIR, env=env, capture_output=True, text=True,
    )
    if build.returncode != 0:
        pytest.fail(f"blobgw build failed:\n{build.stderr}")

    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [binpath, "-addr", f"127.0.0.1:{port}", "-domain", "test",
         "-backend", "local", "-data-dir", os.path.join(tmp, "data"),
         "-index", "memory", "-staging", "memory", "-gc-interval", "0"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    deadline = time.time() + 30
    ready = False
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            pytest.fail(f"blobgw exited early:\n{out}")
        try:
            with urllib.request.urlopen(f"{base}/healthz", timeout=1) as r:
                if r.status == 200:
                    ready = True
                    break
        except Exception:
            time.sleep(0.2)
    if not ready:
        proc.terminate()
        pytest.fail("blobgw did not become healthy in time")

    try:
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)
