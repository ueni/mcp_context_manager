#!/usr/bin/env python3
"""Update release version declarations."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

VERSION_PATTERN = re.compile(
    r"^[0-9]+[.][0-9]+[.][0-9]+(?:[.-][0-9A-Za-z.-]+)?$"
)
PYPROJECT_VERSION_PATTERN = r'(?m)^version = "[^"]+"$'
MONITOR_VERSION_PATTERN = r'(?m)^EXPECTED_SERVER_VERSION = "[^"]+"$'


def _update_one(
    root: Path,
    relative_path: Path,
    pattern: str,
    replacement: str,
    missing_message: str,
) -> bool:
    path = root / relative_path
    content = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, content, count=1)
    if count != 1:
        raise RuntimeError(missing_message)
    if content == updated:
        return False
    path.write_text(updated, encoding="utf-8")
    return True


def update_versions(root: Path, version: str) -> list[Path]:
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError("version must look like 1.2.3, without a leading v")

    updates = [
        (
            Path("pyproject.toml"),
            PYPROJECT_VERSION_PATTERN,
            f'version = "{version}"',
            "expected one version assignment in pyproject.toml",
        ),
        (
            Path("monitor-metrics.py"),
            MONITOR_VERSION_PATTERN,
            f'EXPECTED_SERVER_VERSION = "{version}"',
            "expected one EXPECTED_SERVER_VERSION assignment in monitor-metrics.py",
        ),
    ]
    changed = []
    for relative_path, pattern, replacement, missing_message in updates:
        if _update_one(root, relative_path, pattern, replacement, missing_message):
            changed.append(relative_path)
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="release version without a leading v")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    changed = update_versions(root, args.version)
    for path in changed:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
