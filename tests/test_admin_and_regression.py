from __future__ import annotations

import json
from pathlib import Path

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService


def _write_many_python_files(repo: Path, count: int = 12) -> None:
    for idx in range(count):
        (repo / f"file_{idx:03d}.py").write_text(
            "def marker():\n    return 'needle'\n" * 20,
            encoding="utf-8",
        )


def _count_python_read_bytes(repo: Path, monkeypatch) -> list[str]:
    original_read_bytes = Path.read_bytes
    read_paths: list[str] = []

    def counted_read_bytes(path: Path) -> bytes:
        try:
            rel = path.resolve().relative_to(repo.resolve())
        except ValueError:
            pass
        else:
            if rel.suffix == ".py":
                read_paths.append(str(rel))
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)
    return read_paths


def _count_python_read_text(repo: Path, monkeypatch) -> list[str]:
    original_read_text = Path.read_text
    read_paths: list[str] = []

    def counted_read_text(path: Path, *args, **kwargs) -> str:
        try:
            rel = path.resolve().relative_to(repo.resolve())
        except ValueError:
            pass
        else:
            if rel.suffix == ".py":
                read_paths.append(str(rel))
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted_read_text)
    return read_paths


def test_admin_budget_contracts_and_cache(service: ContextService) -> None:
    budget = service.context_admin(
        mode="budget", max_output_chars=4096, default_output_profile="normal"
    )
    assert budget["schema"] == "context_budget.v1"
    assert budget["max_output_chars"] == 4096
    assert budget["default_output_profile"] == "normal"

    contracts = service.context_admin(mode="contracts")
    assert "context_pack" in contracts["contracts"]

    service.context_admin(mode="index_refresh")
    first = service.context_lookup(mode="search", query="auth token")
    second = service.context_lookup(mode="search", query="auth token")
    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is True

    stats = service.context_admin(mode="cache_stats")
    assert stats["entry_count"] >= 1

    pack = service.context_pack("review auth token behavior", max_items=2)
    metrics = service.context_admin(mode="metrics")
    resource = json.loads(service.repo_metrics_resource())

    assert metrics["schema"] == "context_metrics.v1"
    assert metrics["cache"]["hits"] >= 1
    assert metrics["cache"]["misses"] >= 1
    assert metrics["tokens"]["estimated_input_tokens_saved"] >= pack["metrics"]["estimated_input_tokens_saved"]
    assert metrics["benchmarks"]["latency_ms_by_operation"]["context_pack"]["count"] >= 1
    assert metrics["requests"]["by_operation"]["context_lookup.search"]["result_count"] >= first["count"]
    assert resource["schema"] == "context_metrics.v1"
    assert resource["requests"]["total"] == metrics["requests"]["total"]


def test_context_retrieval_regression_smoke(service: ContextService) -> None:
    fixtures = [
        {
            "prompt": "Debug the login failure in src/auth.py",
            "expected_paths": {"src/auth.py"},
        },
        {
            "prompt": "Review token handling and update test coverage",
            "expected_paths": {"src/auth.py", "tests/test_auth.py"},
        },
        {
            "prompt": "Update the README docs for authentication behavior",
            "expected_paths": {"README.md"},
        },
    ]
    hits = 0
    reciprocal_ranks = []
    for fixture in fixtures:
        pack = service.context_pack(fixture["prompt"], max_items=4)
        paths = [item["path"] for item in pack["items"]]
        if fixture["expected_paths"].intersection(paths):
            hits += 1
            first_rank = min(
                paths.index(path) + 1
                for path in fixture["expected_paths"]
                if path in paths
            )
            reciprocal_ranks.append(1.0 / first_rank)
        else:
            reciprocal_ranks.append(0.0)

    recall = hits / len(fixtures)
    mean_efficiency = sum(reciprocal_ranks) / len(reciprocal_ranks)
    assert recall >= 0.8
    assert mean_efficiency >= 0.5


def test_cold_then_warm_context_pack_flow(service: ContextService) -> None:
    cold = service.context_pack("review auth login token behavior", refresh_index=True)
    warm = service.context_pack("review auth login token behavior")

    assert cold["items"]
    assert warm["items"]
    assert cold["cache"]["hit"] is False
    assert warm["cache"]["hit"] is True
    assert warm["metrics"]["elapsed_ms"] >= 0
    assert warm["references"][0]["reference_id"].startswith("ctxref-")


def test_external_state_dir_supports_container_layout(
    sample_repo: Path, tmp_path: Path
) -> None:
    state_dir = tmp_path / "state"
    service = ContextService(
        ContextConfig(repo_path=sample_repo.resolve(), state_dir=state_dir.resolve())
    )

    health = service.context_admin(mode="health")
    index = service.context_admin(mode="index_refresh")
    pack = service.context_pack("review auth token behavior", max_items=2)
    memory = service.context_memory(mode="get")
    references = service.context_lookup(mode="references")
    metrics = service.context_admin(mode="metrics")

    assert health["repo_path"] == "."
    assert health["state_dir"] == "state"
    assert health["index"]["index_path"] == "state/index/context.sqlite3"
    assert index["index_path"] == "state/index/context.sqlite3"
    assert pack["repo"]["path"] == "."
    assert pack["repo"]["state_dir"] == "state"
    assert memory["path"] == "state/memory/context_memory.json"
    assert references["references"][0]["path"].startswith("state/references/")
    assert metrics["path"] == "state/reports/context_metrics.json"

    public_payload = json.dumps(
        [health, index, pack["repo"], memory, references, metrics],
        sort_keys=True,
    )
    assert str(sample_repo.resolve()) not in public_payload
    assert str(state_dir.resolve()) not in public_payload


def test_warm_index_refresh_skips_full_reads_for_unchanged_files(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_many_python_files(repo)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )
    service.context_admin(mode="index_refresh")

    read_paths = _count_python_read_bytes(repo, monkeypatch)

    warm = service.context_admin(mode="index_refresh")

    assert warm["updated_count"] == 0
    assert warm["unchanged_count"] >= 12
    assert read_paths == []


def test_cached_search_does_not_reread_unchanged_files(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_many_python_files(repo)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )
    first = service.context_lookup(mode="search", query="needle")
    assert first["cache"]["hit"] is False

    read_paths = _count_python_read_bytes(repo, monkeypatch)

    second = service.context_lookup(mode="search", query="needle")

    assert second["cache"]["hit"] is True
    assert read_paths == []


def test_indexed_snippet_does_not_reread_source_text(
    sample_repo: Path, monkeypatch
) -> None:
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
        )
    )
    service.context_admin(mode="index_refresh")

    read_paths = _count_python_read_text(sample_repo, monkeypatch)

    snippet = service.context_lookup(mode="snippet", path="src/auth.py")

    assert snippet["source"] == "index"
    assert read_paths == []


def test_warm_context_pack_does_not_probe_every_unchanged_file(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_many_python_files(repo, count=30)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )
    service.context_pack("review needle", max_items=1)

    read_paths = _count_python_read_bytes(repo, monkeypatch)
    text_paths = _count_python_read_text(repo, monkeypatch)

    pack = service.context_pack("review needle", max_items=1)

    assert pack["items"]
    assert pack["cache"]["hit"] is True
    assert len(set(read_paths)) < 30
    assert text_paths == []


def test_codex_guidance_resource_states_pack_first_boundary(
    service: ContextService,
) -> None:
    guidance = json.loads(service.codex_guidance_resource())

    assert guidance["schema"] == "codex_context_pack_first.instructions.v1"
    assert "cannot force the model" in guidance["boundary"]
    assert "call context_pack first" in guidance["instruction"]
    assert "repo://instructions/codex-context-pack-first" in guidance["resource_uris"]


def test_index_refresh_streams_traversal_without_rglob(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_many_python_files(repo, count=3)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    def fail_rglob(_path: Path, _pattern: str):
        raise AssertionError("index refresh should not materialize root.rglob")

    monkeypatch.setattr(Path, "rglob", fail_rglob)

    refresh = service.context_admin(mode="index_refresh")

    assert refresh["updated_count"] == 3
