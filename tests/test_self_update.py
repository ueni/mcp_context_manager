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
    monkeypatch.setattr(server_module.sys, "frozen", False, raising=False)

    with pytest.raises(RuntimeError, match="standalone executable"):
        server_module._self_update_target_path()


def test_self_update_restart_args_preserves_runtime_arguments() -> None:
    raw_args = [
        "--update",
        "--update-version=0.1.0",
        "--update-repo",
        "owner/repo",
        "--update-target",
        "/tmp/current",
        "--existing",
        "value",
    ]
    assert server_module._self_update_restart_args(raw_args) == ["--existing", "value"]


def test_main_update_preserves_runtime_arguments_for_restart(monkeypatch, tmp_path) -> None:
    executable = tmp_path / "mcp-context-manager"
    executable.write_text("old", encoding="utf-8")
    executable.chmod(0o755)

    calls: dict[str, object] = {}

    def fake_execute(*_args, **_kwargs):
        calls["executed"] = (_args, _kwargs)
        return executable

    def fake_execv(path: str, argv: list[str]) -> None:
        calls["exec"] = (path, argv)
        raise SystemExit(0)

    monkeypatch.setattr(server_module.sys, "argv", [
        "mcp-context-manager",
        "--update",
        "--update-version",
        "0.1.0",
        "--existing",
        "value",
        "--update-target",
        str(executable),
        "--update-repo",
        "owner/repo",
        "--extra",
        "arg",
    ])
    monkeypatch.setattr(server_module, "_self_update_execute", fake_execute)
    monkeypatch.setattr(server_module.os, "execv", fake_execv)

    with pytest.raises(SystemExit):
        server_module.main()

    assert "exec" in calls
    assert calls["exec"] == (str(executable), [str(executable), "--existing", "value", "--extra", "arg"])


def test_main_update_target_error_is_reported(monkeypatch, tmp_path, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_execute(*_args, **_kwargs):
        calls["called"] = True
        raise RuntimeError("update target is not a file")

    monkeypatch.setattr(
        server_module.sys,
        "argv",
        ["mcp-context-manager", "--update", "--update-target", str(tmp_path / "missing-target")],
    )
    monkeypatch.setattr(server_module, "_self_update_execute", fake_execute)
    monkeypatch.setattr(
        server_module.os,
        "execv",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("execv should not be called")
        ),
    )

    with pytest.raises(SystemExit):
        server_module.main()

    assert calls["called"]
    captured = capsys.readouterr()
    assert "update failed: update target is not a file" in captured.err
    assert "Traceback" not in captured.err
