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


def test_tree_omits_generated_state(service: ContextService) -> None:
    service.context_admin(mode="index_refresh")
    tree = service.context_lookup(mode="tree", path=".", max_depth=3)
    paths = {row["path"] for row in tree["entries"]}

    assert "src/auth.py" in paths
    assert not any(path.startswith(".mcp-context-manager") for path in paths)
