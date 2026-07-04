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
    compact_contracts = service.context_admin(
        mode="contracts", contract_profile="compact"
    )
    assert compact_contracts["metrics"]["compact_contract_tokens_est"] < contracts[
        "metrics"
    ]["contract_tokens_est"]
    assert "context_pack.v1" in compact_contracts["contracts"]["context_pack"][
        "output_schema_names"
    ]
    compact_pack_contract = service.context_admin(
        mode="contracts", tool_name="context_pack", contract_profile="compact"
    )
    assert compact_pack_contract["schema"] == "tool_output_contract.compact.v1"
    assert compact_pack_contract["output_schema_names"] == ["context_pack.v1"]

    service.context_admin(mode="index_refresh")
    first = service.context_lookup(mode="search", query="auth token")
    second = service.context_lookup(mode="search", query="auth token")
    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is True
    assert second["cache"]["namespace"] == "context_lookup.search"
    assert second["cache"]["reason"] == "hit"

    stats = service.context_admin(mode="cache_stats")
    assert stats["entry_count"] >= 1
    assert "context_lookup.search" in stats["namespaces"]

    pack = service.context_pack("review auth token behavior", max_items=2)
    warm_pack = service.context_pack("review auth token behavior", max_items=2)
    metrics = service.context_admin(mode="metrics")
    resource = json.loads(service.repo_metrics_resource())

    assert metrics["schema"] == "context_metrics.v1"
    assert metrics["cache"]["hits"] >= 1
    assert metrics["cache"]["misses"] >= 1
    assert metrics["cache"]["reasons"]["hit"] >= 1
    assert metrics["cache"]["by_namespace"]["context_pack.retrieval"]["hits"] >= 1
    assert metrics["cache"]["by_namespace"]["context_lookup.search"]["hits"] >= 1
    assert metrics["tokens"]["estimated_input_tokens_saved"] >= pack["metrics"]["estimated_input_tokens_saved"]
    assert metrics["tokens"]["baseline_input_tokens_est"] >= pack["metrics"]["baseline_input_tokens_est"]
    assert metrics["tokens"]["output_tokens_est"] >= pack["metrics"]["output_tokens_est"]
    assert metrics["tooling"]["external_tool_calls_saved_est"] >= pack["metrics"]["external_tool_calls_saved_est"]
    assert metrics["tooling"]["contract_chars"] >= compact_contracts["metrics"][
        "compact_contract_chars"
    ]
    assert metrics["tooling"]["compact_contract_tokens_saved_est"] > 0
    assert metrics["references"]["bytes_deferred_est"] >= pack["metrics"][
        "references_bytes_deferred_est"
    ]
    assert warm_pack["cache"]["reason"] == "hit"
    assert metrics["benchmarks"]["latency_ms_by_operation"]["context_pack"]["count"] >= 1
    assert "stage_latency_ms_by_operation" in metrics["benchmarks"]
    assert "snippet_batch_ms" in metrics["benchmarks"]["stage_latency_ms_by_operation"]["context_pack"]
    assert metrics["requests"]["by_operation"]["context_lookup.search"]["result_count"] >= first["count"]
    assert resource["schema"] == "context_metrics.v1"
    assert resource["requests"]["total"] == metrics["requests"]["total"]

    matrix = service.context_admin(mode="measurement_matrix")
    assert matrix["schema"] == "context_measurement_matrix.v1"
    assert {
        "latency.context_pack.avg_elapsed_ms",
        "latency.context_pack.snippet_batch_avg_ms",
        "cache.context_pack_retrieval_hit_ratio",
        "tokens.context_pack.avg_saved_per_pack",
        "tooling.contract_tokens_saved_est",
        "tooling.external_calls_saved_per_pack",
        "references.bytes_deferred_est",
    }.issubset({check["key"] for check in matrix["checks"]})
    assert {
        check["status"] for check in matrix["checks"]
    }.issubset({"pass", "fail", "insufficient"})


def test_context_pack_benchmark_runs_offline(service: ContextService) -> None:
    benchmark = service.context_admin(mode="benchmark")

    assert benchmark["schema"] == "context_benchmark.v1"
    assert benchmark["run_count"] == 4
    assert [run["name"] for run in benchmark["runs"]] == [
        "cold_refresh",
        "warm_cache",
        "repeated_prompt",
        "compact_focus",
    ]
    assert benchmark["runs"][1]["cache_hit"] is True
    assert benchmark["runs"][2]["cache_hit"] is True
    assert benchmark["compact_contract_sample"]["schema"] == "tool_output_contracts.compact.v1"
    assert benchmark["compact_contract_sample"]["contract_tokens_saved_est"] > 0
    assert benchmark["measurement_matrix"]["schema"] == "context_measurement_matrix.v1"


def test_context_pack_benchmark_honors_max_files(
    service: ContextService, monkeypatch
) -> None:
    original_refresh = service.index.refresh
    seen_max_files: list[int] = []

    def counted_refresh(path: str = ".", max_files: int = 5000):
        seen_max_files.append(max_files)
        return original_refresh(path=path, max_files=max_files)

    monkeypatch.setattr(service.index, "refresh", counted_refresh)

    service.context_admin(mode="benchmark", max_files=2)

    assert 2 in seen_max_files
    assert 5000 not in seen_max_files


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


def test_warm_context_pack_reuses_retrieval_for_response_assembly(
    service: ContextService, monkeypatch
) -> None:
    first = service.context_pack(
        "review auth login token behavior", max_items=2, max_output_chars=5000
    )

    def fail_retrieval(*_args, **_kwargs):
        raise AssertionError("warm pack should reuse cached retrieval")

    monkeypatch.setattr(service.index, "search", fail_retrieval)
    monkeypatch.setattr(service.index, "symbols", fail_retrieval)
    monkeypatch.setattr(service.index, "snippet_batch", fail_retrieval)

    warm = service.context_pack(
        "review auth login token behavior", max_items=2, max_output_chars=3200
    )

    assert first["items"]
    assert warm["items"]
    assert warm["cache"]["hit"] is True
    assert warm["cache"]["reason"] == "hit"
    assert warm["metrics"]["stage_timings_ms"]["candidate_retrieval_ms"] == 0.0
    assert warm["metrics"]["stage_timings_ms"]["snippet_batch_ms"] == 0.0
    assert warm["references"][0]["reference_id"] != first["references"][0]["reference_id"]


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
    assert health["index"]["index_path"] == "state/store/context.lmdb"
    assert health["index"]["storage_backend"] == "lmdb"
    assert index["index_path"] == "state/store/context.lmdb"
    assert index["storage_backend"] == "lmdb"
    assert pack["repo"]["path"] == "."
    assert pack["repo"]["state_dir"] == "state"
    assert memory["path"] == "state/store/context.lmdb"
    assert references["references"][0]["path"].startswith("state/store/context.lmdb")
    assert metrics["path"] == "state/store/context.lmdb"

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


def test_compact_context_pack_skips_symbols_after_enough_queued_search_hits(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_many_python_files(repo, count=20)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    def fail_symbols(*_args, **_kwargs):
        raise AssertionError("compact retrieval should skip symbols")

    monkeypatch.setattr(service.index, "symbols", fail_symbols)

    pack = service.context_pack("review needle", max_items=1, output_profile="compact")

    assert pack["items"]
    assert pack["metrics"]["retrieval_plan"]["symbol_lookup_skipped"] is True


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
