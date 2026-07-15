#!/usr/bin/env python3
"""Update release version declarations."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

try:
    from packaging.version import InvalidVersion, Version
except ImportError:  # pragma: no cover - fallback depends on runner packages.
    try:
        from setuptools._vendor.packaging.version import InvalidVersion, Version
    except ImportError:
        from pip._vendor.packaging.version import InvalidVersion, Version

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
    canonical = canonical_version(version)

    updates = [
        (
            Path("pyproject.toml"),
            PYPROJECT_VERSION_PATTERN,
            f'version = "{canonical}"',
            "expected one version assignment in pyproject.toml",
        ),
        (
            Path("monitor-metrics.py"),
            MONITOR_VERSION_PATTERN,
            f'EXPECTED_SERVER_VERSION = "{canonical}"',
            "expected one EXPECTED_SERVER_VERSION assignment in monitor-metrics.py",
        ),
    ]
    changed = []
    for relative_path, pattern, replacement, missing_message in updates:
        if _update_one(root, relative_path, pattern, replacement, missing_message):
            changed.append(relative_path)
    return changed


def canonical_version(version: str) -> str:
    clean = version.strip()
    if clean.lower().startswith("v"):
        raise ValueError("version must be a valid PEP 440 version, without a leading v")
    try:
        return str(Version(clean))
    except InvalidVersion as exc:
        raise ValueError(
            "version must be a valid PEP 440 version, without a leading v"
        ) from exc


def export_github_env(version: str) -> None:
    github_env = os.environ.get("GITHUB_ENV", "").strip()
    if not github_env:
        return
    with Path(github_env).open("a", encoding="utf-8") as handle:
        handle.write(f"VERSION={version}\n")
        handle.write(f"TAG=v{version}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--print-canonical",
        action="store_true",
        help="print the canonical PEP 440 version and exit without editing files",
    )
    parser.add_argument("version", help="release version without a leading v")
    args = parser.parse_args()
    try:
        canonical = canonical_version(args.version)
    except ValueError as exc:
        parser.error(str(exc))
    if args.print_canonical:
        print(canonical)
        return 0
    root = Path(__file__).resolve().parents[1]
    try:
        changed = update_versions(root, canonical)
    except ValueError as exc:
        parser.error(str(exc))
    export_github_env(canonical)
    for path in changed:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
