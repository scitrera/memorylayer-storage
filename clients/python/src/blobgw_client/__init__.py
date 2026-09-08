# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

"""Python client for the blobgw object gateway.

blobgw is Layer 1 of the casstore storage platform: a content-addressed,
deduplicated object store fronted by an HTTP API. This package is a thin,
dependency-free client over that API, shaped to drop into memorylayer
enterprise's existing ``BlobStorageService`` seam.

Typical use::

    from blobgw_client import BlobGWClient

    client = BlobGWClient("http://blobgw.internal:8080")
    info = client.put("ws1/documents/doc1/pages/page_0001.png", png_bytes, "image/png")
    data = client.get("ws1/documents/doc1/pages/page_0001.png")
"""

from .adapter import BlobStorageServiceAdapter
from .client import (
    BlobGWClient,
    BlobGWError,
    IntegrityError,
    NotFound,
    ObjectInfo,
    StagedUpload,
)

__all__ = [
    "BlobGWClient",
    "BlobGWError",
    "BlobStorageServiceAdapter",
    "IntegrityError",
    "NotFound",
    "ObjectInfo",
    "StagedUpload",
]

__version__ = "0.7.1"
