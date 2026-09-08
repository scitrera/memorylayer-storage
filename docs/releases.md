# Development and release management

`versions.yaml` is the common configuration surface for version synchronization,
CI generation, component releases, and container images, managed by
[scitrera/repo-tools](https://github.com/scitrera/repo-tools).
This repository requires **scitrera-repo-tools 0.1.29** (independent releases,
workspace synchronization, and database-backed Go test jobs).

```sh
uv pip install scitrera-repo-tools==0.1.29
make versions ci
repo-tools sync-versions --check
repo-tools generate-ci-gha --check
```

Run the installation command in a virtual environment. The generated workflows
pin the same repo-tools version.

## Version boundaries

Each of the eight Go modules has its own version and `<directory>/vX.Y.Z` tag.
The Python package has its own version and `clients/python/vX.Y.Z` tag. The
`memorylayer-storage` catalog version identifies the overall release arrangement;
it does not force component versions to move together.

All components start at **0.7.1**. For a later isolated fix, update only the
component's entry in `versions.yaml`, synchronize, validate, and tag that component.
Sibling `go.mod` dependency pins remain unchanged until their maintainers choose
to adopt the new version. `go.work` deliberately resolves local sibling sources
for development; check independent consumers as well when changing dependencies.

Do not replace an existing component tag or rebuild changed contents under an
existing image version. In particular, the `mlfs-csi` image includes an mlfs daemon:
changes to that bundled daemon require a new CSI image version even if the CSI
protocol code did not change.

## Initial publication order

1. Release repo-tools 0.1.29 and provision the public GitHub repository.
2. Run `make check`, database tests, and the mount acceptance needed for the target
   deployment. Review source/license contents and commit the clean release tree.
3. Create the initial component tags on that commit:
   `casstore/v0.7.1`, `ctlproto/v0.7.1`, `manifeststore/v0.7.1`,
   `blobgw/v0.7.1`, `blobgw-edge/v0.7.1`, `blobgw-s3/v0.7.1`,
   `mlfs/v0.7.1`, `mlfs-csi/v0.7.1`, and `clients/python/v0.7.1`.
4. Push the branch and explicit component tags after the release is approved.
   Push each tag separately: GitHub does not emit tag-push workflow events when
   more than three tags are pushed together.
   Go module tags are immediately visible to consumers; CI runs after the push.
   The generated workflows check tag/version agreement, test the stack, and
   publish the selected Go component's artifacts. The Python client tag provides
   Git-based installation; its tests run on main-branch pushes and pull requests.

For independently published libraries, install for example:

```sh
go get github.com/scitrera/memorylayer-storage/casstore@v0.7.1
```

After the component tags are published, install the filesystem commands with:

```sh
go install github.com/scitrera/memorylayer-storage/mlfs/cmd/mlfs@v0.7.1
go install github.com/scitrera/memorylayer-storage/mlfs/cmd/mlfs-admin@v0.7.1
go install github.com/scitrera/memorylayer-storage/mlfs/cmd/mlfs-bench@v0.7.1
```

## Registry setup

Go modules are served from the public Git tags. Daemon images publish to
`ghcr.io/scitrera/blobgw`, `blobgw-edge`, `blobgw-s3`, `mlfs`, and `mlfs-csi`,
each with its own component version and Linux amd64/arm64 manifests. Configure
GitHub package permissions/visibility for public pulls. Add a Docker Hub registry
in `versions.yaml` if that distribution channel is desired.

The Python package `blobgw-client` is distributed through its Git tags for now;
PyPI publishing is disabled. `ci.skip_workflows: [release-blobgw-client]` keeps
repo-tools from generating its registry release workflow, and no such workflow
is checked in. The Python test workflow remains enabled. Preserve the Apache-2.0
license in its distributions.

After the client tag is published, install it into a virtual environment with:

```sh
uv pip install "blobgw-client @ git+https://github.com/scitrera/memorylayer-storage.git@clients/python/v0.7.1#subdirectory=clients/python"
```

## Migrating an existing consumer

Replace imports `github.com/scitrera/<component>/...` with
`github.com/scitrera/memorylayer-storage/<component>/...`, require the published
module version, and remove old absolute or sibling checkout replacements.
Use a separate `go.work` for local multi-repository development when needed.

For image consumers, select the new registry and component tag explicitly. Keep
existing tenant bindings, NATS account imports, metadata connections, and
credential provisioning in the deployment configuration. The standalone release
does not provide an application-specific tenant provisioning service or Helm chart.
