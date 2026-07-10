from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService


def git(repo: Path, *args: str) -> None:
    if shutil.which("git") is None:
        pytest.skip("git executable is not available")
    subprocess.run(["git", *args], cwd=repo, check=True)


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
    assert refresh["fts_enabled"] is True
    assert refresh["search_mode"] == "tantivy"

    search = service.context_lookup(mode="search", query="issue token auth")
    assert search["schema"] == "context_search.v1"
    assert search["count"] > 0
    assert any(row["path"] == "src/auth.py" for row in search["results"])
    assert {row["source"] for row in search["results"]} == {"tantivy"}
    assert search["index"]["fts_enabled"] is True
    assert search["index"]["search_mode"] == "tantivy"

    symbols = service.context_lookup(mode="symbols", query="AuthService")
    assert symbols["schema"] == "context_symbols.v1"
    assert any(row["name"] == "AuthService" for row in symbols["symbols"])

    snippet = service.context_lookup(mode="snippet", path="src/auth.py", start_line=1, end_line=4)
    assert snippet["schema"] == "context_snippet.v1"
    assert snippet["path"] == "src/auth.py"
    assert "class AuthService" in snippet["content"]


def test_delete_file_rows_derives_legacy_terms_from_content(
    service: ContextService,
) -> None:
    rel = "legacy.py"
    existing = {"path": rel, "content": "def legacy_token():\n    return 1\n"}
    service.index.store.put_json(f"index:file:{rel}", existing)
    service.index.store.put_json(
        f"index:term:legacy_token:{rel}",
        {"path": rel, "term": "legacy_token"},
    )

    with service.index.store.write_txn() as txn:
        service.index._delete_file_rows(rel, existing, txn)

    assert service.index.store.get_json(f"index:file:{rel}") is None
    assert service.index.store.get_json(f"index:term:legacy_token:{rel}") is None


def test_delete_file_rows_does_not_scan_all_terms_for_legacy_rows_without_content(
    service: ContextService, monkeypatch: pytest.MonkeyPatch
) -> None:
    rel = "legacy.py"
    existing = {"path": rel}
    original_iter_json = service.index.store.iter_json

    def fail_term_scan(prefix: str, *args, **kwargs):
        if prefix == "index:term:":
            raise AssertionError("must not scan the full term index")
        return original_iter_json(prefix, *args, **kwargs)

    monkeypatch.setattr(service.index.store, "iter_json", fail_term_scan)

    with service.index.store.write_txn() as txn:
        service.index._delete_file_rows(rel, existing, txn)


def test_scoped_index_refresh_reads_only_matching_file_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "target"
    target.mkdir()
    other = repo / "other"
    other.mkdir()
    (target / "hit.py").write_text("needle = 'target'\n", encoding="utf-8")
    (other / "skip.py").write_text("needle = 'other'\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )
    service.context_admin(mode="index_refresh")
    original_iter_json = service.index.store.iter_json

    def fail_full_file_scan(prefix: str, *args, **kwargs):
        if prefix == "index:file:":
            raise AssertionError("scoped refresh must not scan all indexed files")
        return original_iter_json(prefix, *args, **kwargs)

    monkeypatch.setattr(service.index.store, "iter_json", fail_full_file_scan)
    (target / "hit.py").write_text("needle = 'target changed'\n", encoding="utf-8")

    refresh = service.context_admin(mode="index_refresh", path="target")

    assert refresh["updated_count"] == 1
    assert refresh["removed_count"] == 0


def test_index_refresh_falls_back_when_git_metadata_is_unavailable(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    (repo / "app.py").write_text("value = 'tracked by files'\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    status = service.context_admin(mode="index_status")
    warm = service.index.refresh_if_needed()

    assert status["git_head"] == ""
    assert status["refresh_signature_available"] is True
    assert status["refresh_signature"].startswith("files:")
    assert warm["skipped"] is True
    assert warm["reason"] == "signature_unchanged"


def test_git_refresh_signature_tracks_dirty_content_changes(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "config", "user.name", "Test User")
    (repo / "app.py").write_text("value = 'committed'\n", encoding="utf-8")
    git(repo, "add", "app.py")
    git(repo, "commit", "-m", "initial")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    clean = service.index.refresh_signature()
    (repo / "app.py").write_text("value = 'dirty one'\n", encoding="utf-8")
    dirty_one = service.index.refresh_signature()
    (repo / "app.py").write_text("value = 'dirty two'\n", encoding="utf-8")
    dirty_two = service.index.refresh_signature()

    assert clean["source"] == "git"
    assert dirty_one["source"] == "git"
    assert dirty_one["git_status_hash"] == dirty_two["git_status_hash"]
    assert dirty_one["git_changes_hash"] != dirty_two["git_changes_hash"]
    assert clean["signature"] != dirty_one["signature"]
    assert dirty_one["signature"] != dirty_two["signature"]


def test_subdirectory_git_worktree_uses_file_metadata_signature(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent"
    parent.mkdir()
    git(parent, "init")
    git(parent, "config", "user.email", "test@example.invalid")
    git(parent, "config", "user.name", "Test User")
    git(parent, "commit", "--allow-empty", "-m", "initial")
    subproject = parent / "subproject"
    subproject.mkdir()
    tracked = subproject / "app.py"
    tracked.write_text("value = 'one'\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=subproject.resolve(),
            state_dir=(subproject / ".mcp-context-manager").resolve(),
        )
    )

    first = service.index.refresh_signature()
    tracked.write_text("value = 'two with a different size'\n", encoding="utf-8")
    second = service.index.refresh_signature()
    service.context_admin(mode="index_refresh")
    tracked.write_text("value = 'three with another different size'\n", encoding="utf-8")
    refreshed = service.index.refresh_if_needed()

    assert first["source"] == "file_metadata"
    assert first["git_available"] is True
    assert first["git_worktree_matches_path"] is False
    assert first["signature"] != second["signature"]
    assert refreshed["skipped"] is False
    assert refreshed["reason"] == "signature_changed"


def test_path_scoped_fts_search_applies_path_before_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    for idx in range(12):
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
    monkeypatch.setattr(service.index, "_tantivy_candidate_limit", lambda _: 3)
    search = service.context_lookup(
        mode="search", query="needle", path="target", max_results=1
    )

    assert search["count"] == 1
    assert search["results"][0]["path"] == "target/hit.py"
    assert search["results"][0]["source"] == "tantivy"


def test_tantivy_finds_terms_beyond_legacy_term_cap(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    indexed_terms = "\n".join(f"unique_token_{idx}" for idx in range(4100))
    (repo / "large.py").write_text(
        f"{indexed_terms}\n# fallbackonly appears after the old term cap\n",
        encoding="utf-8",
    )
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    search = service.context_lookup(mode="search", query="fallbackonly")

    assert search["count"] == 1
    assert search["results"][0]["path"] == "large.py"
    assert search["results"][0]["source"] == "tantivy"


def test_scoped_refresh_updates_and_removes_tantivy_documents(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "target"
    target.mkdir()
    changed = target / "changed.py"
    deleted = target / "deleted.py"
    changed.write_text("oldneedle = True\n", encoding="utf-8")
    deleted.write_text("deleteme = True\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    changed.write_text("newneedle = True\n", encoding="utf-8")
    deleted.unlink()
    refresh = service.context_admin(mode="index_refresh", path="target")

    assert refresh["updated_count"] == 1
    assert refresh["removed_count"] == 1
    assert service.context_lookup(mode="search", query="newneedle")["results"][0][
        "path"
    ] == "target/changed.py"
    assert service.context_lookup(mode="search", query="oldneedle")["results"] == []
    assert service.context_lookup(mode="search", query="deleteme")["results"] == []


def test_tantivy_search_honors_include_globs(service: ContextService) -> None:
    service.context_admin(mode="index_refresh")

    search = service.context_lookup(
        mode="search",
        query="token",
        include_globs=["tests/**"],
    )

    assert search["results"]
    assert all(row["path"].startswith("tests/") for row in search["results"])


def test_tantivy_include_globs_fetches_past_candidate_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    src = repo / "src"
    src.mkdir()
    for idx in range(8):
        (src / f"root_match_{idx:02d}.py").write_text(
            "needle = 'root'\n", encoding="utf-8"
        )
    tests_dir = repo / "tests"
    tests_dir.mkdir()
    (tests_dir / "hit.py").write_text("needle = 'scoped'\n", encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_admin(mode="index_refresh")
    monkeypatch.setattr(service.index, "_tantivy_candidate_limit", lambda _: 2)
    search = service.context_lookup(
        mode="search",
        query="needle",
        include_globs=["tests/**"],
        max_results=1,
    )

    assert search["count"] == 1
    assert search["results"][0]["path"] == "tests/hit.py"


def test_missing_tantivy_sidecar_rebuilds_from_lmdb(service: ContextService) -> None:
    service.context_admin(mode="index_refresh")
    shutil.rmtree(service.config.tantivy_index_dir)

    search = service.context_lookup(mode="search", query="auth token")
    status = service.context_admin(mode="index_status")

    assert search["results"]
    assert status["search_mode"] == "tantivy"
    assert status["tantivy"]["doc_count"] == status["file_count"]


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
