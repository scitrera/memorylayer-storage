# Contributing

Use the existing component boundaries and keep dependency changes explicit.
Install the Python test extra and repo-tools version described in
[release management](docs/releases.md), then run `make check`.
See [testing](docs/testing.md) for PostgreSQL and privileged mount coverage.

Edit `versions.yaml` and regenerate workflows; generated files are checked for
drift. Bug fixes can release independently under the affected component's tag.
Contributions to storage code use AGPL-3.0-only; contributions confined to the
Python client use Apache-2.0. Retain upstream notices when adding dependencies.
