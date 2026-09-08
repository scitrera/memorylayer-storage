# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Enterprise ``BlobStorageService`` seam adapter over :class:`BlobGWClient`.

The storage platform plan promised a client that "drops into enterprise's
existing ``BlobStorageService`` seam" (``store_file`` / ``retrieve_file`` /
``delete_tree`` / ``exists`` → gateway calls, with deterministic storage paths
becoming gateway refs). This module delivers that surface.

:class:`BlobStorageServiceAdapter` is a thin, synchronous, dependency-free
wrapper around a :class:`BlobGWClient`. It maps storage *paths* onto gateway
*refs* through a single :meth:`_ref` normalizer and re-exposes the named seam
methods. It is deliberately framework-free: the enterprise
``BlobStorageService`` is ``async`` and wraps each call in
``asyncio.to_thread`` (exactly as it already wraps fsspec). This adapter is the
synchronous core those coroutines call into — keeping the gateway mapping in
the client package where it is unit-testable against a real gateway binary.

Path↔ref contract
-----------------
Refs are *relative*, multi-segment identifiers; the gateway routes
``/v1/objects/{ref...}`` as a wildcard, so ``/`` separators are preserved.
:meth:`_ref` is the identity mapping with two normalizations so that the *same*
deterministic storage path always lands on the *same* ref:

* leading ``/`` are stripped (refs are never absolute);
* repeated ``//`` separators collapse to a single ``/``.

No other transformation is applied: a deterministic path such as
``ws1/documents/doc1/pages/page_0001.png`` is used verbatim as its ref.

Divergence from the enterprise seam signatures
----------------------------------------------
The canonical enterprise ``BlobStorageService`` (fsspec-backed) is ``async``
and uses filesystem-flavored contracts: ``store_file`` returns the path
(``str``) and raises ``FileNotFoundError``. This adapter intentionally exposes
the gateway-native contract instead — synchronous, returning the gateway's
:class:`ObjectInfo`, and propagating the client's :class:`NotFound`. Callers
that need the exact async/``FileNotFoundError`` seam wrap these methods (see
``blob_storage_blobgw.py`` in enterprise, which does precisely that).
"""

from __future__ import annotations

import contextlib
from typing import IO, Iterator

from .client import BlobGWClient, NotFound, ObjectInfo


class BlobStorageServiceAdapter:
    """Synchronous ``BlobStorageService``-seam adapter over a gateway client.

    Wraps a :class:`BlobGWClient`, mapping deterministic storage paths onto
    gateway refs (see module docstring for the path↔ref contract). The adapter
    is stateless and threadsafe — it holds only the client reference — so async
    enterprise callers can dispatch each method through ``asyncio.to_thread``.
    """

    def __init__(self, client: BlobGWClient) -> None:
        """Create an adapter wrapping ``client``.

        Args:
            client: A configured :class:`BlobGWClient` pointed at a gateway.
        """
        self._client = client

    # === path ↔ ref mapping ===

    @staticmethod
    def _ref(path: str) -> str:
        """Map a storage ``path`` onto a stable gateway ref.

        Identity by default, with two normalizations so the same path always
        maps to the same ref: leading ``/`` are stripped (refs are relative,
        never absolute) and repeated ``//`` separators collapse to one. ``/``
        separators are otherwise preserved — the gateway supports multi-segment
        refs.

        Args:
            path: A deterministic storage path (or bare ref).

        Returns:
            The normalized ref.

        Raises:
            BlobGWError: Indirectly, when the normalized ref is empty and is
                later handed to a client object operation.
        """
        ref = path.lstrip("/")
        while "//" in ref:
            ref = ref.replace("//", "/")
        return ref

    @staticmethod
    def _dir_prefix(prefix: str) -> str:
        """Normalize a directory ``prefix`` to a ref prefix ending in ``/``.

        A bare ``""`` prefix maps to ``""`` (match-all), so callers can list or
        delete the entire ref space.
        """
        ref = BlobStorageServiceAdapter._ref(prefix)
        if not ref:
            return ""
        return ref.rstrip("/") + "/"

    # === seam surface ===

    def store_file(
        self,
        path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> ObjectInfo:
        """Store ``data`` at ``path`` and return its gateway metadata.

        Args:
            path: Storage path; mapped to a ref via :meth:`_ref`.
            data: Object content.
            content_type: MIME type hint stored alongside the object. The
                gateway decides dedup/compression by content class, not by this
                hint, so a coarse default is fine.

        Returns:
            The stored object's :class:`ObjectInfo`.
        """
        return self._client.put(self._ref(path), data, content_type)

    def retrieve_file(self, path: str) -> bytes:
        """Return the full content stored at ``path``.

        Args:
            path: Storage path; mapped to a ref via :meth:`_ref`.

        Returns:
            The object's bytes.

        Raises:
            NotFound: If no object exists at ``path``.
        """
        return self._client.get(self._ref(path))

    @contextlib.contextmanager
    def retrieve_stream(self, path: str) -> Iterator[IO[bytes]]:
        """Yield a streaming file-like over the object stored at ``path``.

        Use for large objects (tensors, page renders) to avoid buffering the
        whole body::

            with adapter.retrieve_stream(path) as body:
                shutil.copyfileobj(body, dest)

        Args:
            path: Storage path; mapped to a ref via :meth:`_ref`.

        Yields:
            A readable binary stream over the object's bytes.

        Raises:
            NotFound: If no object exists at ``path``.
        """
        with self._client.get_stream(self._ref(path)) as body:
            yield body

    def exists(self, path: str) -> bool:
        """Return True if an object exists at ``path``.

        Args:
            path: Storage path; mapped to a ref via :meth:`_ref`.

        Returns:
            True when present, False otherwise.
        """
        return self._client.exists(self._ref(path))

    def delete(self, path: str) -> None:
        """Delete the object at ``path``. Deleting an absent path is a no-op.

        Args:
            path: Storage path; mapped to a ref via :meth:`_ref`.
        """
        self._client.delete(self._ref(path))

    def delete_tree(self, prefix: str) -> int:
        """Delete every object under ``prefix`` and return the count deleted.

        Best-effort: this delegates to the server-side bulk endpoint
        ``DELETE /v1/objects?prefix=`` (one round-trip), where the gateway
        deletes each matching ref — dropping all backing manifest versions, so
        no manifests are orphaned. An empty tree returns ``0`` without error.

        The subtree under ``prefix/`` is removed via a single prefix delete; an
        object stored *exactly* at ``prefix`` (no trailing segment) is removed
        with one extra single-ref delete so that sibling refs sharing the bare
        string prefix (e.g. ``prefix2``) are NOT swept — preserving the
        directory-boundary semantics callers rely on.

        Args:
            prefix: Storage path prefix; mapped to a ref prefix via
                :meth:`_dir_prefix`. Both the subtree under ``prefix/`` and an
                exact object sitting at ``prefix`` itself are removed.

        Returns:
            The number of objects successfully deleted.

        Raises:
            BlobGWError: If the server rejects the bulk delete (e.g. an empty
                prefix) or another transport/server error occurs.
        """
        dir_prefix = self._dir_prefix(prefix)
        exact = self._ref(prefix)

        deleted = 0
        if dir_prefix:
            deleted += self._client.delete_prefix(dir_prefix)
        if exact and self._client.exists(exact):
            # An object stored exactly at the prefix (no trailing segment) is
            # not under "prefix/", so remove it with a single-ref delete. A
            # bulk delete on the bare string would also catch sibling refs like
            # "prefix2", which the directory contract excludes.
            self._client.delete(exact)
            deleted += 1
        return deleted

    def list_tree(self, prefix: str = "", limit: int = 0) -> list[ObjectInfo]:
        """List objects whose ref starts with ``prefix`` (newest first).

        Args:
            prefix: Storage path prefix; mapped to a ref via :meth:`_ref`. The
                empty default lists the whole ref space.
            limit: Maximum objects to return; ``0`` means no limit.

        Returns:
            A list of :class:`ObjectInfo`, one per matching object.
        """
        return self._client.list(self._ref(prefix), limit)
