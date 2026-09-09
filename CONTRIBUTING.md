# Contributing

Before a contribution can be merged, its contributor must accept the
[MemoryLayer Storage Contributor License Agreement](CLA.md) through a contribution
process designated by Scitrera LLC, the sole Project Owner. This requirement
applies to contributions throughout the repository, including the Python client.
Contributors retain ownership of their contributions; the CLA grants Scitrera LLC
rights to use them under open-source, commercial, or proprietary licenses, with
the open-source availability commitment described in the agreement.

The repository does not yet designate a particular electronic CLA acceptance
service. Until one is documented, coordinate acceptance with Scitrera LLC at
[open-source-team@scitrera.com](mailto:open-source-team@scitrera.com). Maintainers
must confirm acceptance before merging a contribution.

When submitting a contribution:

- submit only work you authored or are authorized to contribute;
- identify third-party material and its source and license; and
- preserve applicable copyright, SPDX, attribution, and license notices.

Use the existing component boundaries and keep dependency changes explicit.
Install the Python test extra and repo-tools version described in
[release management](docs/releases.md), then run `make check`.
See [testing](docs/testing.md) for PostgreSQL and privileged mount coverage.

Edit `versions.yaml` and regenerate workflows; generated files are checked for
drift. Bug fixes can release independently under the affected component's tag.
Contributions to storage code use AGPL-3.0-only; contributions confined to the
Python client use Apache-2.0. Retain upstream notices when adding dependencies.
