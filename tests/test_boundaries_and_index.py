from __future__ import annotations

from pathlib import Path

import pytest

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService


def test_repo_path_boundary_rejects_escape(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    service = ContextService(
        ContextConfig(repo_path=repo.resolve(), state_dir=(repo / ".mcp-context-manager").resolve())
    )

    with pytest.raises(ValueError):
        service.context_lookup(mode="snippet", path="../outside.txt")

    with pytest.raises(ValueError):
        service.config.resolve_repo_path(outside)


def test_index_refresh_search_symbols_and_snippet(service: ContextService) -> None:
    refresh = service.context_admin(mode="index_refresh")
    assert refresh["schema"] == "context_index.refresh.v1"
    assert refresh["file_count"] >= 4
    assert refresh["symbol_count"] >= 3

    search = service.context_lookup(mode="search", query="issue token auth")
    assert search["schema"] == "context_search.v1"
    assert search["count"] > 0
    assert any(row["path"] == "src/auth.py" for row in search["results"])

    symbols = service.context_lookup(mode="symbols", query="AuthService")
    assert symbols["schema"] == "context_symbols.v1"
    assert any(row["name"] == "AuthService" for row in symbols["symbols"])

    snippet = service.context_lookup(mode="snippet", path="src/auth.py", start_line=1, end_line=4)
    assert snippet["schema"] == "context_snippet.v1"
    assert snippet["path"] == "src/auth.py"
    assert "class AuthService" in snippet["content"]


def test_path_scoped_fts_search_applies_path_before_limit(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    for idx in range(20):
        (repo / f"root_match_{idx:02d}.py").write_text(
            "needle = 'root'\n", encoding="utf-8"
        )
    target = repo / "target"
    target.mkdir()
    (target / "hit.py").write_text("needle = 'target'\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    search = service.context_lookup(
        mode="search", query="needle", path="target", max_results=1
    )

    assert search["count"] == 1
    assert search["results"][0]["path"] == "target/hit.py"


def test_tree_omits_generated_state(service: ContextService) -> None:
    service.context_admin(mode="index_refresh")
    tree = service.context_lookup(mode="tree", path=".", max_depth=3)
    paths = {row["path"] for row in tree["entries"]}

    assert "src/auth.py" in paths
    assert not any(path.startswith(".mcp-context-manager") for path in paths)


def test_runtime_skip_rules_hide_bind_mount_secrets(
    sample_repo: Path, tmp_path: Path
) -> None:
    (sample_repo / ".env").write_text(
        "WORKSPACESECRET=do-not-index\n", encoding="utf-8"
    )
    (sample_repo / "credentials").write_text(
        "credentialleak should not be indexed\n", encoding="utf-8"
    )
    stale_state = sample_repo / ".mcp-context-manager" / "references"
    stale_state.mkdir(parents=True)
    (stale_state / "ctxref-secret.json").write_text(
        '{"stateleak": true}\n', encoding="utf-8"
    )
    (sample_repo / "Dockerfile").write_text(
        "FROM scratch\n# dockerallowed\n", encoding="utf-8"
    )
    (sample_repo / "Jenkinsfile").write_text(
        "pipeline { stages { stage('jenkinsallowed') { steps { sh 'true' } } } }\n",
        encoding="utf-8",
    )
    (sample_repo / "BUILD").write_text(
        "# buildallowed target\n", encoding="utf-8"
    )
    (sample_repo / "Gemfile").write_text(
        "source 'https://rubygems.org'\n# gemfileallowed\n", encoding="utf-8"
    )
    script = sample_repo / "deploy"
    script.write_text("#!/bin/sh\n# scriptallowed\n", encoding="utf-8")
    script.chmod(0o755)
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(tmp_path / "external-state").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    paths = {
        row["path"]
        for row in service.context_lookup(mode="tree", path=".", max_depth=4)["entries"]
    }

    assert "Dockerfile" in paths
    assert "Jenkinsfile" in paths
    assert "BUILD" in paths
    assert "Gemfile" in paths
    assert "deploy" in paths
    assert ".env" not in paths
    assert "credentials" not in paths
    assert not any(path.startswith(".mcp-context-manager") for path in paths)
    assert service.context_lookup(mode="search", query="dockerallowed")["results"]
    assert service.context_lookup(mode="search", query="jenkinsallowed")["results"]
    assert service.context_lookup(mode="search", query="buildallowed")["results"]
    assert service.context_lookup(mode="search", query="gemfileallowed")["results"]
    assert service.context_lookup(mode="search", query="scriptallowed")["results"]
    assert not service.context_lookup(mode="search", query="workspacesecret")["results"]
    assert not service.context_lookup(mode="search", query="credentialleak")["results"]
    assert not service.context_lookup(mode="search", query="stateleak")["results"]
    assert "pipeline" in service.context_lookup(
        mode="snippet", path="Jenkinsfile"
    )["content"]
    assert "scriptallowed" in service.context_lookup(
        mode="snippet", path="deploy", start_line=1, end_line=2
    )["content"]

    with pytest.raises(ValueError):
        service.context_lookup(mode="snippet", path=".env")
    with pytest.raises(ValueError):
        service.context_lookup(mode="snippet", path="credentials")
    with pytest.raises(ValueError):
        service.context_lookup(
            mode="snippet",
            path=".mcp-context-manager/references/ctxref-secret.json",
        )
