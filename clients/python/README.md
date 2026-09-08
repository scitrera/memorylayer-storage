# blobgw-client

Synchronous Python client for the MemoryLayer Storage HTTP object gateway.
Version **0.7.1**, licensed under [Apache-2.0](LICENSE), with no runtime dependencies.
Requires Python 3.10 or newer.

Install from the versioned Git tag once it is published. In a virtual environment:

```sh
uv pip install "blobgw-client @ git+https://github.com/scitrera/memorylayer-storage.git@clients/python/v0.7.1#subdirectory=clients/python"
```

The client is distributed through Git tags for now; it is not published to PyPI.
The tag selects the client release, and `subdirectory` selects its Python package.

```python
from blobgw_client import BlobGWClient, BlobStorageServiceAdapter

client = BlobGWClient("http://127.0.0.1:8080")
client.put("documents/hello.txt", b"hello", "text/plain")
assert client.get("documents/hello.txt") == b"hello"
info = client.head("documents/hello.txt")
objects = client.list("documents/")
client.delete("documents/hello.txt")

adapter = BlobStorageServiceAdapter(client)
adapter.store_file("documents/hello.txt", b"hello", "text/plain")
```

The client also provides staged uploads (`mint_ref` / `finalize`), metadata,
pagination, and prefix deletion. Async applications can call it through
`asyncio.to_thread`. This client targets the trusted object API; externally
exposed applications must use appropriate gateway authentication and routing.

For development, run `uv pip install -e '.[test]'` and `python -m pytest` from
this directory in an activated virtual environment.
Tests require Go on PATH and build a real gateway from the repository checkout.
Build or startup failures fail the suite.
