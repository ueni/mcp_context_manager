from __future__ import annotations

import pytest

from mcp_context_manager import server as server_module


def test_self_update_parse_checksummed_asset_list() -> None:
    checksums = "\n".join(
        [
            "abcdef  mcp-context-manager-1.0.0-linux-x86_64",
            "123456  other-file",
        ]
    )
    parsed = server_module._parse_checksummed_asset_list(checksums)
    assert parsed["mcp-context-manager-1.0.0-linux-x86_64"] == "abcdef"
    assert parsed["other-file"] == "123456"


def test_self_update_release_api_url_latest() -> None:
    assert (
        server_module._self_update_release_api_url("owner/repo")
        == "https://api.github.com/repos/owner/repo/releases/latest"
    )


def test_self_update_release_api_url_version() -> None:
    assert (
        server_module._self_update_release_api_url("owner/repo", "v1.0.0")
        == "https://api.github.com/repos/owner/repo/releases/tags/v1.0.0"
    )


def test_self_update_platform_suffix(monkeypatch) -> None:
    monkeypatch.setattr(server_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(server_module.platform, "machine", lambda: "x86_64")
    assert (
        server_module._self_update_platform_suffix()
        == "linux-x86_64"
    )


def test_self_update_target_path_rejects_python_file(monkeypatch, tmp_path) -> None:
    script = tmp_path / "mcp-context-manager.py"
    script.write_text("#!/usr/bin/env python\n", encoding="utf-8")
    monkeypatch.setattr(server_module.sys, "argv", [str(script)])
    monkeypatch.setattr(server_module.os, "access", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(server_module.sys, "frozen", False)

    with pytest.raises(RuntimeError, match="standalone executable"):
        server_module._self_update_target_path()
