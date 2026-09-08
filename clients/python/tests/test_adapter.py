# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for BlobStorageServiceAdapter against a real blobgw binary.

Exercises the enterprise ``BlobStorageService`` seam surface (store_file /
retrieve_file / retrieve_stream / exists / delete / delete_tree / list_tree)
through a freshly built gateway, mirroring ``test_client.py``'s real-interop
approach (no mocks).
"""

from __future__ import annotations

import os

import pytest

from blobgw_client import (
    BlobStorageServiceAdapter,
    BlobGWClient,
    NotFound,
    ObjectInfo,
)


@pytest.fixture
def adapter(blobgw_server):
    return BlobStorageServiceAdapter(BlobGWClient(blobgw_server))


def test_ref_normalization_is_stable():
    # Pure mapping contract: no gateway needed.
    assert BlobStorageServiceAdapter._ref("/ws1/a/b") == "ws1/a/b"
    assert BlobStorageServiceAdapter._ref("ws1//a///b") == "ws1/a/b"
    # The same deterministic path always lands on the same ref.
    assert BlobStorageServiceAdapter._ref("/ws1//a/b") == BlobStorageServiceAdapter._ref("ws1/a/b")


def test_store_exists_retrieve_delete_roundtrip(adapter):
    path = "ws-adapter/documents/doc1/pages/page_0001.png"
    payload = os.urandom(256 * 1024)

    info = adapter.store_file(path, payload, "image/png")
    assert isinstance(info, ObjectInfo)
    assert info.size == len(payload)
    assert info.content_type == "image/png"

    assert adapter.exists(path) is True
    assert adapter.retrieve_file(path) == payload

    adapter.delete(path)
    assert adapter.exists(path) is False
    with pytest.raises(NotFound):
        adapter.retrieve_file(path)


def test_leading_slash_path_maps_to_same_object(adapter):
    # An absolute-looking path and its ref-form address the same object.
    payload = b"identity-mapping"
    adapter.store_file("/ws-adapter/slash/obj", payload)
    assert adapter.retrieve_file("ws-adapter/slash/obj") == payload


def test_retrieve_stream(adapter):
    path = "ws-adapter/stream/big.bin"
    payload = os.urandom(512 * 1024)
    adapter.store_file(path, payload)

    chunks = bytearray()
    with adapter.retrieve_stream(path) as body:
        while True:
            chunk = body.read(65536)
            if not chunk:
                break
            chunks += chunk
    assert bytes(chunks) == payload


def test_delete_tree_removes_all_under_prefix(adapter):
    prefix = "ws-adapter/tree/doc7/pages"
    n = 5
    refs = [f"{prefix}/page_{i:04d}.png" for i in range(n)]
    for ref in refs:
        adapter.store_file(ref, f"page-{ref}".encode())

    listed = adapter.list_tree(prefix + "/")
    assert {o.ref for o in listed} == {BlobStorageServiceAdapter._ref(r) for r in refs}

    deleted = adapter.delete_tree(prefix)
    assert deleted == n

    assert adapter.list_tree(prefix + "/") == []
    for ref in refs:
        assert adapter.exists(ref) is False


def test_delete_tree_includes_exact_prefix_object(adapter):
    # An object stored exactly at the prefix (no trailing segment) is removed
    # alongside the subtree.
    adapter.store_file("ws-adapter/exact/node", b"root")
    adapter.store_file("ws-adapter/exact/node/child", b"child")

    deleted = adapter.delete_tree("ws-adapter/exact/node")
    assert deleted == 2
    assert adapter.exists("ws-adapter/exact/node") is False
    assert adapter.exists("ws-adapter/exact/node/child") is False


def test_delete_tree_empty_prefix_returns_zero(adapter):
    assert adapter.delete_tree("ws-adapter/nothing/here") == 0


def test_delete_tree_uses_server_side_bulk_endpoint(adapter):
    # A8: delete_tree now removes a multi-object subtree via the server-side
    # bulk endpoint (one round-trip) and returns the count N.
    prefix = "ws-adapter/bulk/doc9/pages"
    n = 8
    refs = [f"{prefix}/page_{i:04d}.png" for i in range(n)]
    for ref in refs:
        adapter.store_file(ref, f"page-{ref}".encode())

    deleted = adapter.delete_tree(prefix)
    assert deleted == n
    assert adapter.list_tree(prefix + "/") == []
    for ref in refs:
        assert adapter.exists(ref) is False


def test_delete_tree_excludes_sibling_prefix(adapter):
    # The directory boundary contract: deleting "node" removes "node" and
    # "node/..." but NEVER a sibling like "node2" that merely shares the bare
    # string prefix.
    adapter.store_file("ws-adapter/sib/node", b"root")
    adapter.store_file("ws-adapter/sib/node/child", b"child")
    adapter.store_file("ws-adapter/sib/node2", b"sibling")

    deleted = adapter.delete_tree("ws-adapter/sib/node")
    assert deleted == 2
    assert adapter.exists("ws-adapter/sib/node") is False
    assert adapter.exists("ws-adapter/sib/node/child") is False
    # The sibling survives.
    assert adapter.exists("ws-adapter/sib/node2") is True


def test_retrieve_missing_raises_not_found(adapter):
    with pytest.raises(NotFound):
        adapter.retrieve_file("ws-adapter/missing/path")
