# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""End-to-end tests for BlobGWClient against a real blobgw binary."""

from __future__ import annotations

import os

import pytest

from blobgw_client import (
    BlobGWClient,
    IntegrityError,
    NotFound,
    ObjectInfo,
    StagedUpload,
)


@pytest.fixture
def client(blobgw_server):
    return BlobGWClient(blobgw_server)


def test_put_get_head_roundtrip(client):
    ref = "ws1/documents/doc1/pages/page_0001.png"
    payload = os.urandom(512 * 1024)

    info = client.put(ref, payload, "image/png")
    assert isinstance(info, ObjectInfo)
    assert info.ref == ref
    assert info.size == len(payload)
    assert info.content_type == "image/png"
    assert info.content_hash

    assert client.get(ref) == payload

    head = client.head(ref)
    assert head.size == len(payload)
    assert head.content_type == "image/png"
    assert head.content_hash == info.content_hash

    assert client.exists(ref) is True


def test_head_returns_fully_populated_object_info(client):
    # A4: head() reconstructs domain/version/created_at/pending from the
    # X-Blobgw-* response headers, not just Content-Length/Type/ETag.
    ref = "ws-head/meta/obj"
    payload = os.urandom(4096)
    info = client.put(ref, payload, "application/pdf")

    head = client.head(ref)
    assert head.ref == ref
    assert head.size == len(payload)
    assert head.content_type == "application/pdf"
    assert head.content_hash == info.content_hash
    # Populated from X-Blobgw-* headers (divergent from the old fabricated info).
    assert head.domain == "test"  # the conftest server runs with -domain test
    assert head.version == info.version and head.version != ""
    assert head.created_at != ""
    assert head.pending is False
    # No user metadata was attached over the HTTP PUT path → empty map (not None).
    assert head.user_meta == {}


def test_head_parses_user_meta_header():
    # The HTTP PUT path carries no user metadata, so exercise the
    # X-Blobgw-User-Meta JSON parsing directly with a stub HEAD response.
    import email.message

    from blobgw_client.client import BlobGWClient

    c = BlobGWClient("http://stub.invalid")

    class _Resp:
        def __init__(self) -> None:
            self.headers = email.message.Message()
            self.headers["Content-Length"] = "10"
            self.headers["Content-Type"] = "application/pdf"
            self.headers["ETag"] = '"abc123"'
            self.headers["X-Blobgw-Domain"] = "ent"
            self.headers["X-Blobgw-Created-At"] = "2026-06-04T00:00:00Z"
            self.headers["X-Blobgw-Version"] = "v1"
            self.headers["X-Blobgw-Pending"] = "false"
            self.headers["X-Blobgw-User-Meta"] = '{"author":"alice","etag":"deadbeef"}'

        def close(self) -> None:
            pass

    import urllib.request

    orig = urllib.request.urlopen
    urllib.request.urlopen = lambda *a, **k: _Resp()  # type: ignore[assignment]
    try:
        head = c.head("any/ref")
    finally:
        urllib.request.urlopen = orig  # type: ignore[assignment]

    assert head.domain == "ent"
    assert head.version == "v1"
    assert head.content_hash == "abc123"
    assert head.pending is False
    assert head.user_meta == {"author": "alice", "etag": "deadbeef"}


def test_list_over_many_objects_returns_all(client):
    # A8: list() with limit<=0 must follow X-Blobgw-Next-Cursor across pages and
    # return EVERY matching object, never silently truncating at the server's
    # internal page size. We exceed the small per-call limit by paging manually
    # too, to prove both the auto-follow and the low-level list_page primitive.
    n = 120
    for i in range(n):
        client.put(f"ws-page/obj/{i:04d}", bytes([i % 256]))

    everything = client.list("ws-page/obj/")
    refs = {o.ref for o in everything}
    assert len(refs) == n
    assert refs == {f"ws-page/obj/{i:04d}" for i in range(n)}

    # Manual paging via list_page returns the same set with no dups/skips.
    seen: dict[str, int] = {}
    cursor = ""
    pages = 0
    while True:
        objs, cursor = client.list_page("ws-page/obj/", after=cursor, limit=25)
        assert len(objs) <= 25
        for o in objs:
            seen[o.ref] = seen.get(o.ref, 0) + 1
        pages += 1
        if not cursor:
            break
        assert pages <= n  # never loop forever
    assert set(seen) == refs
    assert all(v == 1 for v in seen.values())


def test_delete_prefix_bulk(client):
    # A8: server-side bulk delete removes every matching object in one call and
    # returns the count; an empty prefix is rejected.
    from blobgw_client import BlobGWError

    n = 6
    for i in range(n):
        client.put(f"ws-bulk/tree/{i}", b"x")
    client.put("ws-bulk/keep", b"k")

    deleted = client.delete_prefix("ws-bulk/tree/")
    assert deleted == n
    assert client.list("ws-bulk/tree/") == []
    assert client.exists("ws-bulk/keep") is True

    with pytest.raises(BlobGWError):
        client.delete_prefix("")


def test_get_stream(client):
    ref = "ws1/stream/obj"
    payload = os.urandom(1024 * 1024)
    client.put(ref, payload)
    chunks = bytearray()
    with client.get_stream(ref) as body:
        while True:
            chunk = body.read(65536)
            if not chunk:
                break
            chunks += chunk
    assert bytes(chunks) == payload


def test_dedup_distinct_refs(client):
    payload = os.urandom(1024 * 1024)
    hashes = set()
    for i in range(5):
        info = client.put(f"ws1/tensors/t{i}", payload)
        hashes.add(info.content_hash)
    # Identical content → identical whole-object hash regardless of ref.
    assert len(hashes) == 1


def test_list_prefix(client):
    payload = b"x" * 4096
    for ref in ["a/1", "a/2", "b/1"]:
        client.put(f"ws-list/{ref}", payload)
    objs = client.list("ws-list/a/")
    assert {o.ref for o in objs} == {"ws-list/a/1", "ws-list/a/2"}


def test_delete_and_not_found(client):
    ref = "ws1/delete/me"
    client.put(ref, b"bye")
    assert client.exists(ref) is True
    client.delete(ref)
    assert client.exists(ref) is False
    with pytest.raises(NotFound):
        client.get(ref)
    with pytest.raises(NotFound):
        client.head(ref)
    # Deleting an absent ref is a no-op.
    client.delete(ref)


def test_get_missing_raises_not_found(client):
    with pytest.raises(NotFound):
        client.get("nope/missing")


def test_mint_ref_returns_staged_upload(client):
    staged = client.mint_ref(content_type="application/pdf", ttl_seconds=600)
    assert isinstance(staged, StagedUpload)
    assert staged.ref
    assert staged.upload_url
    assert staged.staging_key
    # Before finalize the ref is pending → not retrievable.
    assert client.exists(staged.ref) is False
    # NOTE: completing the upload + finalize requires a staging backend the
    # client can PUT to (S3); that full path is covered by blobgw's Go
    # integration tests (s3stage + cmd smoke). Here we verify the mint contract.


def test_mint_ref_accepts_integrity_expectations(client):
    # content_hash + size are optional and must round-trip through the /v1/staged
    # request without error (the integrity gate itself is enforced at finalize,
    # covered by the Go server tests). The ref is usable before upload.
    staged = client.mint_ref(
        content_type="application/octet-stream",
        content_hash="00deadbeef",
        size=4096,
        ttl_seconds=600,
    )
    assert isinstance(staged, StagedUpload)
    assert staged.ref
    assert staged.staging_key
    assert client.exists(staged.ref) is False


def test_integrity_error_is_blobgw_error_subclass():
    # IntegrityError is a BlobGWError so existing broad handlers still catch it,
    # while callers that care can catch it specifically.
    from blobgw_client import BlobGWError

    assert issubclass(IntegrityError, BlobGWError)
