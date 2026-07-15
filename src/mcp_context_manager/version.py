"""Resolve the package version from its canonical project metadata."""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path

_PYPROJECT_VERSION_RE = re.compile(r'(?m)^version = "([^"]+)"$')


def package_version() -> str:
    source_version = _source_tree_version()
    if source_version:
        return source_version
    try:
        return metadata.version("mcp-context-manager")
    except metadata.PackageNotFoundError:
        return "unknown"


def _source_tree_version() -> str:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if not pyproject.is_file():
        return ""
    match = _PYPROJECT_VERSION_RE.search(pyproject.read_text(encoding="utf-8"))
    return match.group(1) if match else ""


SERVER_VERSION = package_version()
