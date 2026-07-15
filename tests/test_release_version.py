from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from mcp_context_manager import version as version_module
from mcp_context_manager.version import SERVER_VERSION

ROOT = Path(__file__).resolve().parents[1]


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_repository_version_declarations_are_synchronized() -> None:
    monitor = load_module(ROOT / "monitor-metrics.py", "monitor_metrics_version_test")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    monitor_text = (ROOT / "monitor-metrics.py").read_text(encoding="utf-8")
    package_match = re.search(r'(?m)^version = "([^"]+)"$', pyproject)

    assert package_match is not None
    assert package_match.group(1) == SERVER_VERSION
    assert monitor.EXPECTED_SERVER_VERSION == SERVER_VERSION
    assert f'SERVER_VERSION = "{SERVER_VERSION}"' not in (
        ROOT / "src/mcp_context_manager/version.py"
    ).read_text(encoding="utf-8")
    assert f'EXPECTED_SERVER_VERSION = "{SERVER_VERSION}"' in monitor_text
    assert "from importlib import metadata" not in monitor_text
    assert "from pathlib import Path" not in monitor_text
    assert "_PYPROJECT_VERSION_RE" not in monitor_text
    assert "def expected_server_version" not in monitor_text
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert 'VER="RELEASE_VERSION"' in readme
    assert 'MCP_EXPECTED_SERVER_VERSION="$VER"' not in readme


def test_monitor_version_can_be_set_for_streamed_script(monkeypatch) -> None:
    monkeypatch.setenv("MCP_EXPECTED_SERVER_VERSION", "streamed-version")
    monitor = load_module(
        ROOT / "monitor-metrics.py", "monitor_metrics_streamed_version_test"
    )
    assert monitor.EXPECTED_SERVER_VERSION == "streamed-version"


def test_packaged_server_version_comes_from_distribution_metadata(monkeypatch) -> None:
    monkeypatch.setattr(version_module, "_source_tree_version", lambda: "")
    monkeypatch.setattr(
        version_module.metadata,
        "version",
        lambda distribution: "packaged-version"
        if distribution == "mcp-context-manager"
        else "",
    )

    assert version_module.package_version() == "packaged-version"


def test_release_version_updater_changes_canonical_declaration(tmp_path: Path) -> None:
    updater = load_module(
        ROOT / "scripts/set_release_version.py", "set_release_version_test"
    )
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nversion = "0.0.1"\n', encoding="utf-8"
    )
    (tmp_path / "monitor-metrics.py").write_text(
        'EXPECTED_SERVER_VERSION = "0.0.1"\n', encoding="utf-8"
    )

    changed = updater.update_versions(tmp_path, "1.2.0-rc.1")

    assert changed == [Path("pyproject.toml"), Path("monitor-metrics.py")]
    assert 'version = "1.2.0rc1"' in (tmp_path / "pyproject.toml").read_text()
    assert 'EXPECTED_SERVER_VERSION = "1.2.0rc1"' in (
        tmp_path / "monitor-metrics.py"
    ).read_text()
    assert updater.update_versions(tmp_path, "1.2.0-rc.1") == []
    assert updater.canonical_version(" 1.2.0-rc.1 ") == "1.2.0rc1"


@pytest.mark.parametrize("version", ["1.2.3.foo", "1.2.3--", "v1.2.0"])
def test_release_version_updater_rejects_invalid_versions(
    tmp_path: Path, version: str
) -> None:
    updater = load_module(
        ROOT / "scripts/set_release_version.py", "set_release_version_invalid_test"
    )
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nversion = "0.0.1"\n', encoding="utf-8"
    )
    (tmp_path / "monitor-metrics.py").write_text(
        'EXPECTED_SERVER_VERSION = "0.0.1"\n', encoding="utf-8"
    )

    with pytest.raises(ValueError, match="valid PEP 440 version"):
        updater.update_versions(tmp_path, version)


def test_release_version_updater_exports_canonical_github_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    updater = load_module(
        ROOT / "scripts/set_release_version.py", "set_release_version_env_test"
    )
    github_env = tmp_path / "github-env"
    monkeypatch.setenv("GITHUB_ENV", str(github_env))

    updater.export_github_env("1.2.0rc1")

    assert github_env.read_text(encoding="utf-8") == (
        "VERSION=1.2.0rc1\n"
        "TAG=v1.2.0rc1\n"
    )


def test_release_version_updater_prints_canonical_without_editing(
    tmp_path: Path,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/set_release_version.py"),
            "--print-canonical",
            "1.2.0-rc.1",
        ],
        cwd=tmp_path,
        check=True,
        text=True,
        capture_output=True,
    )

    assert result.stdout == "1.2.0rc1\n"
    assert not (tmp_path / "pyproject.toml").exists()


def test_release_workflow_commits_and_tags_version_before_release() -> None:
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")

    canonicalize = (
        'version="$(python3 scripts/set_release_version.py --print-canonical '
        '"${{ inputs.version }}")"'
    )
    assert canonicalize in workflow
    assert workflow.index("Set up Python") < workflow.index("Validate version")
    assert workflow.index(canonicalize) < workflow.index("git ls-remote")
    assert 'python3 scripts/set_release_version.py "$VERSION"' in workflow
    assert "git add pyproject.toml monitor-metrics.py" in workflow
    assert 'git commit -m "Release ${TAG}"' in workflow
    assert 'git tag -a "$TAG"' in workflow
    assert 'git push --atomic origin "HEAD:${GITHUB_REF_NAME}" "$TAG"' in workflow
    assert workflow.index("Commit release version and create tag") < workflow.index(
        "Build glibc and musl executables"
    )
    assert workflow.index("Push release commit and tag") < workflow.index(
        "Create GitHub release"
    )
    standalone_build = (ROOT / "cmake/BuildStandalone.cmake").read_text(
        encoding="utf-8"
    )
    assert standalone_build.count("--copy-metadata mcp-context-manager") == 2
