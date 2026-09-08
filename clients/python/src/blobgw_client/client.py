# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Dependency-free HTTP client for the blobgw object gateway.

Mirrors the gateway's HTTP API. The DATA plane is versioned under ``/v1``;
operational endpoints (``/healthz``, ``/readyz``, ``/metrics``) stay at the
root and are not exercised by this client.

    PUT    /v1/objects/{ref}        put
    GET    /v1/objects/{ref}        get / get_stream
    HEAD   /v1/objects/{ref}        head / exists
    DELETE /v1/objects/{ref}        delete
    GET    /v1/objects?prefix=      list / list_page
    DELETE /v1/objects?prefix=      delete_prefix (server-side bulk)
    POST   /v1/staged               mint_ref
    POST   /v1/finalize             finalize

Uses only the standard library (urllib) so it can be vendored anywhere.
"""

from __future__ import annotations

import contextlib
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from email.message import Message
from typing import IO, Iterator


class BlobGWError(Exception):
    """Base error for blobgw client failures.

    Attributes:
        status: HTTP status code, or None for transport-level errors.
        message: Server-provided or transport error message.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class NotFound(BlobGWError):
    """Raised when an object ref does not exist (HTTP 404)."""


class IntegrityError(BlobGWError):
    """Raised when :meth:`BlobGWClient.finalize` rejects a staged upload whose
    bytes don't match the ``content_hash``/``size`` armed at mint time (HTTP
    409). The ref is NOT bound to the mismatched content; a corrected re-upload
    + re-finalize can still succeed."""


@dataclass(frozen=True)
class ObjectInfo:
    """Metadata for a stored object (one blob_ref row).

    ``created_at`` is kept as the raw RFC3339 string the server returns to
    avoid lossy nanosecond parsing; callers that need a datetime can parse it.

    ``user_meta`` is the caller-supplied metadata map attached on write; it is
    populated by get()/list() (from the JSON body) and by head() (from the
    ``X-Blobgw-User-Meta`` header). The field uses a per-instance default
    (``default_factory``) so it is never a shared mutable.
    """

    ref: str
    domain: str
    size: int
    content_hash: str = ""
    content_type: str = ""
    created_at: str = ""
    version: str = ""
    pending: bool = False
    staging_key: str = ""
    user_meta: dict[str, str] = field(default_factory=dict)

    @classmethod
    def _from_json(cls, d: dict) -> "ObjectInfo":
        meta = d.get("user_meta") or {}
        return cls(
            ref=d.get("ref", ""),
            domain=d.get("domain", ""),
            size=int(d.get("size", 0)),
            content_hash=d.get("content_hash", ""),
            content_type=d.get("content_type", ""),
            created_at=d.get("created_at", ""),
            version=d.get("version", ""),
            pending=bool(d.get("pending", False)),
            staging_key=d.get("staging_key", ""),
            user_meta={str(k): str(v) for k, v in meta.items()} if isinstance(meta, dict) else {},
        )


@dataclass(frozen=True)
class StagedUpload:
    """Result of mint_ref: where the client should upload raw bytes."""

    ref: str
    upload_url: str
    staging_key: str
    expires_at: str = ""

    @classmethod
    def _from_json(cls, d: dict) -> "StagedUpload":
        return cls(
            ref=d.get("ref", ""),
            upload_url=d.get("upload_url", ""),
            staging_key=d.get("staging_key", ""),
            expires_at=d.get("expires_at", ""),
        )


class BlobGWClient:
    """Synchronous client for a blobgw gateway.

    The client is stateless and threadsafe; async callers (e.g. enterprise's
    ``BlobStorageService``) can wrap calls in ``asyncio.to_thread`` exactly as
    they wrap fsspec today.
    """

    def __init__(self, base_url: str, *, timeout: float = 60.0) -> None:
        """Create a client.

        Args:
            base_url: Gateway base URL, e.g. ``http://blobgw.internal:8080``.
            timeout: Per-request timeout in seconds.
        """
        self._base = base_url.rstrip("/")
        # Data-plane routes are versioned under /v1 (operational endpoints stay
        # at the root, but this client only talks to the data plane).
        self._api = self._base + "/v1"
        self._timeout = timeout

    # --- object ops ---

    def put(self, ref: str, data: bytes, content_type: str = "application/octet-stream") -> ObjectInfo:
        """Store ``data`` under ``ref`` and return its metadata."""
        body = self._request(
            "PUT", self._object_url(ref), data=data,
            headers={"Content-Type": content_type},
        )
        return ObjectInfo._from_json(_loads(body))

    def get(self, ref: str) -> bytes:
        """Return the full content stored under ``ref``.

        Raises NotFound if the ref does not exist.
        """
        return self._request("GET", self._object_url(ref))

    @contextlib.contextmanager
    def get_stream(self, ref: str) -> Iterator[IO[bytes]]:
        """Yield a streaming file-like over the object's bytes.

        Use for large objects (tensors) to avoid buffering the whole body::

            with client.get_stream(ref) as body:
                shutil.copyfileobj(body, dest)
        """
        req = urllib.request.Request(self._object_url(ref), method="GET")
        try:
            resp = urllib.request.urlopen(req, timeout=self._timeout)
        except urllib.error.HTTPError as e:
            raise self._http_error(e) from None
        except urllib.error.URLError as e:
            raise BlobGWError(f"blobgw transport error: {e.reason}") from None
        try:
            yield resp
        finally:
            resp.close()

    def head(self, ref: str) -> ObjectInfo:
        """Return object metadata without the body.

        Reconstructs a fully-populated :class:`ObjectInfo` from the headers
        blobgw sets on HEAD. Beyond Content-Length/Content-Type/ETag, the
        server emits an ``X-Blobgw-*`` family carrying the rest of the metadata
        so HEAD has the same fidelity as get()/list() (A4):

        ============================ =====================================
        Header                       ObjectInfo field
        ============================ =====================================
        ``Content-Length``           ``size``
        ``Content-Type``             ``content_type``
        ``ETag`` (quoted hex)        ``content_hash``
        ``X-Blobgw-Domain``          ``domain``
        ``X-Blobgw-Created-At``      ``created_at`` (RFC3339)
        ``X-Blobgw-Version``         ``version``
        ``X-Blobgw-Pending``         ``pending`` ("true"/"false")
        ``X-Blobgw-User-Meta``       ``user_meta`` (JSON map; absent = none)
        ============================ =====================================

        Missing headers are tolerated (an older server, or an object without
        that field) and default to ``""`` / ``{}`` / ``False``. Raises NotFound
        if the ref does not exist.
        """
        req = urllib.request.Request(self._object_url(ref), method="HEAD")
        try:
            resp = urllib.request.urlopen(req, timeout=self._timeout)
        except urllib.error.HTTPError as e:
            raise self._http_error(e) from None
        except urllib.error.URLError as e:
            raise BlobGWError(f"blobgw transport error: {e.reason}") from None
        with contextlib.closing(resp):
            h = resp.headers
            etag = (h.get("ETag") or "").strip('"')
            length = h.get("Content-Length") or "0"
            user_meta: dict[str, str] = {}
            raw_meta = h.get("X-Blobgw-User-Meta")
            if raw_meta:
                with contextlib.suppress(Exception):
                    parsed = json.loads(raw_meta)
                    if isinstance(parsed, dict):
                        user_meta = {str(k): str(v) for k, v in parsed.items()}
            return ObjectInfo(
                ref=ref,
                domain=h.get("X-Blobgw-Domain") or "",
                size=int(length),
                content_hash=etag,
                content_type=h.get("Content-Type") or "",
                created_at=h.get("X-Blobgw-Created-At") or "",
                version=h.get("X-Blobgw-Version") or "",
                pending=(h.get("X-Blobgw-Pending") or "").lower() == "true",
                user_meta=user_meta,
            )

    def exists(self, ref: str) -> bool:
        """Return True if an object exists for ``ref``."""
        try:
            self.head(ref)
            return True
        except NotFound:
            return False

    def delete(self, ref: str) -> None:
        """Delete the object for ``ref``. Deleting an absent ref is a no-op."""
        self._request("DELETE", self._object_url(ref))

    def list(self, prefix: str = "", limit: int = 0) -> list[ObjectInfo]:
        """List objects whose ref starts with ``prefix``, newest first.

        When ``limit <= 0`` this returns ALL matching objects: it transparently
        follows the server's ``X-Blobgw-Next-Cursor`` across pages so callers
        never silently truncate at the server's internal page size. When
        ``limit > 0`` it returns at most that many objects (a single page).
        For manual paging use :meth:`list_page`.
        """
        if limit > 0:
            objs, _ = self.list_page(prefix, limit=limit)
            return objs
        out: list[ObjectInfo] = []
        cursor = ""
        while True:
            objs, cursor = self.list_page(prefix, after=cursor)
            out.extend(objs)
            if not cursor:
                return out

    def list_page(
        self, prefix: str = "", after: str = "", limit: int = 0
    ) -> tuple[list[ObjectInfo], str]:
        """Fetch one page of objects under ``prefix`` plus the next cursor.

        Returns ``(objects, next_cursor)``. ``next_cursor`` is an opaque token:
        pass it back as ``after`` to fetch the following page, or an empty
        string means the listing is exhausted. ``limit`` caps the page size
        (``0`` lets the server choose its internal default). This is the
        low-level primitive behind :meth:`list` for callers that want to drive
        paging themselves.
        """
        query = {"prefix": prefix}
        if after:
            query["after"] = after
        if limit > 0:
            query["limit"] = str(limit)
        url = f"{self._api}/objects?{urllib.parse.urlencode(query)}"
        body, headers = self._request_with_headers("GET", url)
        objs = [ObjectInfo._from_json(d) for d in _loads(body)]
        return objs, headers.get("X-Blobgw-Next-Cursor") or ""

    def delete_prefix(self, prefix: str) -> int:
        """Server-side bulk delete: remove every object whose ref starts with
        ``prefix`` and return the count deleted.

        One round-trip to ``DELETE /v1/objects?prefix=...``; the server deletes
        each matching ref (dropping all backing manifest versions, so no
        manifests are orphaned). ``prefix`` MUST be non-empty — an empty prefix
        is rejected by the server with HTTP 400 (guarding against a
        "delete the whole domain" footgun) and surfaces as :class:`BlobGWError`.
        """
        if not prefix:
            raise BlobGWError("delete_prefix requires a non-empty prefix")
        url = f"{self._api}/objects?{urllib.parse.urlencode({'prefix': prefix})}"
        body = self._request("DELETE", url)
        payload = _loads(body)
        if isinstance(payload, dict):
            return int(payload.get("deleted", 0))
        return 0

    # --- stage-then-finalize ---

    def mint_ref(
        self,
        ref: str | None = None,
        content_type: str = "",
        max_size: int = 0,
        ttl_seconds: int = 0,
        content_hash: str = "",
        size: int = 0,
    ) -> StagedUpload:
        """Begin a stage-then-finalize upload.

        Returns a presigned upload target. The client uploads raw bytes to
        ``StagedUpload.upload_url`` (e.g. an HTTP PUT to S3), then calls
        :meth:`finalize` with the returned ref. Mirrors enterprise's
        ``mint_upload_url`` pre-registration: the ref is stable and usable
        downstream before the upload completes.

        ``content_hash`` (hex sha256) and ``size`` are OPTIONAL: when given they
        arm the finalize integrity gate — the staged bytes' computed hash/size
        must match or :meth:`finalize` raises :class:`IntegrityError`. Omit
        (or ``""`` / ``0``) to accept whatever is staged.
        """
        payload: dict = {}
        if ref:
            payload["ref"] = ref
        if content_type:
            payload["content_type"] = content_type
        if max_size > 0:
            payload["max_size"] = max_size
        if ttl_seconds > 0:
            payload["ttl_seconds"] = ttl_seconds
        if content_hash:
            payload["content_hash"] = content_hash
        if size > 0:
            payload["size"] = size
        body = self._request(
            "POST", f"{self._api}/staged",
            data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"},
        )
        return StagedUpload._from_json(_loads(body))

    def finalize(self, ref: str) -> ObjectInfo:
        """Finalize a staged upload: chunk+dedup the staged bytes and bind the
        ref to its content. Idempotent.

        Raises :class:`IntegrityError` (HTTP 409) if the staged bytes don't
        match a ``content_hash``/``size`` armed at :meth:`mint_ref` time."""
        body = self._request(
            "POST", f"{self._api}/finalize",
            data=json.dumps({"ref": ref}).encode(), headers={"Content-Type": "application/json"},
        )
        return ObjectInfo._from_json(_loads(body))

    # --- internals ---

    def _object_url(self, ref: str) -> str:
        if not ref:
            raise BlobGWError("ref must be non-empty")
        # Keep path separators; encode everything else so multi-segment refs
        # map onto the gateway's /v1/objects/{ref...} wildcard route.
        return f"{self._api}/objects/{urllib.parse.quote(ref, safe='/')}"

    def _request(self, method: str, url: str, *, data: bytes | None = None,
                 headers: dict[str, str] | None = None) -> bytes:
        req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            raise self._http_error(e) from None
        except urllib.error.URLError as e:
            raise BlobGWError(f"blobgw transport error: {e.reason}") from None

    def _request_with_headers(
        self, method: str, url: str, *, data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes, Message]:
        """Like :meth:`_request` but also returns the response headers, used by
        the cursored list path to read ``X-Blobgw-Next-Cursor``."""
        req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read(), resp.headers
        except urllib.error.HTTPError as e:
            raise self._http_error(e) from None
        except urllib.error.URLError as e:
            raise BlobGWError(f"blobgw transport error: {e.reason}") from None

    def _http_error(self, e: urllib.error.HTTPError) -> BlobGWError:
        detail = ""
        with contextlib.suppress(Exception):
            payload = json.loads(e.read().decode())
            detail = payload.get("error", "")
        msg = detail or f"HTTP {e.code}"
        if e.code == 404:
            return NotFound(msg, status=404)
        if e.code == 409:
            # The gateway maps both ErrNotStaged and the A1 integrity gate to
            # 409; surface a distinct exception so callers can react to a bad
            # staged upload specifically.
            return IntegrityError(msg, status=409)
        return BlobGWError(msg, status=e.code)


def _loads(body: bytes) -> dict | list:
    return json.loads(body.decode() or "null")
