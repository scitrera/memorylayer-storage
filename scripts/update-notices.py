#!/usr/bin/env python3
# Copyright 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
"""Collect unchanged notices for modules actually imported by code and tests."""
from pathlib import Path
import json
import os
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
GO = os.environ.get("GO", "go")


def main():
    modules_by_path = {}
    decoder = json.JSONDecoder()
    for manifest in ROOT.glob("*/go.mod"):
        raw = subprocess.check_output(
            [GO, "list", "-deps", "-test", "-json=Module", "./..."],
            cwd=manifest.parent, text=True,
        )
        while raw.strip():
            package, end = decoder.raw_decode(raw.lstrip())
            raw = raw.lstrip()[end:]
            module = package.get("Module")
            if module:
                modules_by_path[module["Path"]] = module
    modules = modules_by_path.values()
    planned = []
    for module in modules:
        if module.get("Main"):
            continue
        effective = module.get("Replace", module)
        source = Path(effective["Dir"])
        files = [p for p in source.iterdir() if p.is_file() and
                 re.match(r"(?i)^(licen[cs]e|copying|copyright|notice)(?:$|[.\-_])", p.name)]
        if not files:
            raise RuntimeError(f"No top-level license/notice found for {effective['Path']}")
        planned.append((effective["Path"], effective["Version"], files))
    target_root = ROOT / "LICENSES/third-party"
    if target_root.exists():
        shutil.rmtree(target_root)
    target_root.mkdir(parents=True)
    rows = []
    for name, version, files in sorted(planned):
        target = target_root / (re.sub(r"[^A-Za-z0-9_.-]", "_", name) + "@" + version)
        target.mkdir()
        for source in files:
            shutil.copyfile(source, target / source.name)
        links = ", ".join(f"[{p.name}]({(target / p.name).relative_to(ROOT).as_posix()})" for p in files)
        rows.append(f"| `{name}` | `{version}` | {links} |")
    goroot = Path(subprocess.check_output([GO, "env", "GOROOT"], text=True).strip())
    for name in ("LICENSE", "PATENTS"):
        shutil.copyfile(goroot / name, target_root / f"Go-{name}")
    (ROOT / "THIRD_PARTY_NOTICES.md").write_text(
        "# Third-party notices\n\n"
        "The license and notice files below are copied without modification from\n"
        "dependencies imported by this workspace's code and tests. Each binary uses\n"
        "a subset. Dependencies retain their licenses; module versions and checksums\n"
        "are recorded in `go.mod` and `go.sum`. Refresh with `python3 scripts/update-notices.py`.\n\n"
        "The Go runtime's [license](LICENSES/third-party/Go-LICENSE) and\n"
        "[patent notice](LICENSES/third-party/Go-PATENTS) are included. Container base-image\n"
        "packages retain their notices under the image's system documentation directories.\n\n"
        "| Module | Version | License and notice files |\n|---|---|---|\n"
        + "\n".join(rows) + "\n"
    )
    print(f"Collected notices for {len(planned)} modules and the Go runtime.")


if __name__ == "__main__":
    main()
