from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import DEFAULT_WARMUP_MAX_FILES, ContextService


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


def _search_fragment_cache_row(
    service: ContextService, term: str = "auth"
) -> tuple[str, dict]:
    for key, row in service.store.iter_json("cache:retrieval.search_term:"):
        if isinstance(row, dict) and row.get("metadata", {}).get("term") == term:
            return key, row
    raise AssertionError(f"missing search fragment cache row for {term}")


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

    varied_query = service.context_lookup(
        mode="search",
        query=" Auth   Token ",
        path="./",
        include_globs=["tests/**", "src/**", "src/**"],
    )
    normalized_query = service.context_lookup(
        mode="search",
        query="auth token",
        path=".",
        include_globs=["src/**", "tests/**"],
    )
    assert varied_query["cache"]["hit"] is False
    assert normalized_query["cache"]["hit"] is True
    assert normalized_query["query"] == "auth token"
    assert normalized_query["terms"] == ["auth", "token"]

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
    assert metrics["tokens"]["tokens_spared_by_mcp_est"] >= pack["metrics"]["tokens_spared_by_mcp_est"]
    assert (
        metrics["tokens"]["tokens_spared_by_mcp_est"]
        == metrics["tokens"]["estimated_input_tokens_saved"]
    )
    assert metrics["tokens"]["avg_tokens_spared_by_mcp_est_per_pack"] >= 0
    assert "context_pack" in metrics["tokens"]["tokens_spared_by_mcp_reason"]
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
    assert "search_ranking_ms" in metrics["benchmarks"]["stage_latency_ms_by_operation"]["context_pack"]
    assert metrics["tokens"]["token_counting"]["token_count_source"] == "estimate"
    assert metrics["requests"]["by_operation"]["context_lookup.search"]["result_count"] >= first["count"]
    assert resource["schema"] == "context_metrics.v1"
    assert resource["requests"]["total"] == metrics["requests"]["total"]

    matrix = service.context_admin(mode="measurement_matrix")
    assert matrix["schema"] == "context_measurement_matrix.v1"
    assert {
        "latency.context_pack.avg_elapsed_ms",
        "latency.context_pack.snippet_batch_avg_ms",
        "cache.context_pack_retrieval_hit_ratio",
        "cache.context_pack_fragment_hit_ratio",
        "tokens.context_pack.avg_saved_per_pack",
        "tokens.context_pack.avg_tokens_spared_by_mcp_per_pack",
        "tooling.contract_tokens_saved_est",
        "tooling.external_calls_saved_per_pack",
        "references.bytes_deferred_est",
    }.issubset({check["key"] for check in matrix["checks"]})
    assert {
        check["status"] for check in matrix["checks"]
    }.issubset({"pass", "fail", "insufficient"})


def test_context_admin_warmup_preinitializes_index_and_search_cache(
    service: ContextService,
    monkeypatch,
) -> None:
    original_search = service.index.search
    fallback_flags: list[bool] = []

    def counted_search(*args, **kwargs):
        fallback_flags.append(bool(kwargs.get("allow_fallback", True)))
        return original_search(*args, **kwargs)

    monkeypatch.setattr(service.index, "search", counted_search)

    warmup = service.context_admin(mode="warmup", path="src", max_entries=3)

    assert warmup["schema"] == "context_cache.warmup.v1"
    assert warmup["state"]["store_exists"] is True
    assert warmup["index"]["max_files"] == DEFAULT_WARMUP_MAX_FILES
    assert warmup["index"]["default_limited"] is True
    assert warmup["index"]["file_count"] >= 1
    assert warmup["workspace"]["file_count"] >= 1
    assert warmup["search_cache"]["namespace"] == "context_lookup.search"
    assert warmup["search_cache"]["path"] == "src"
    assert warmup["search_cache"]["query_count"] == 3
    assert all(row["path"] == "src" for row in warmup["search_cache"]["queries"])
    assert fallback_flags == [False, False, False]
    assert warmup["cache"]["entry_count_after"] >= warmup["search_cache"]["query_count"]
    assert "retrieval.search_term" in warmup["cache"]["namespaces_after"]
    assert "context_pack.retrieval" not in warmup["cache"]["namespaces_after"]

    lookup = service.context_lookup(mode="search", query="test", path="src")

    assert lookup["cache"]["hit"] is True
    assert lookup["cache"]["namespace"] == "context_lookup.search"

    second = service.context_admin(mode="warmup", path="src", max_entries=3)

    assert second["search_cache"]["path"] == "src"
    assert all(row["cache_hit"] is True for row in second["search_cache"]["queries"])

    explicit = service.context_admin(
        mode="warmup", path="src", max_files=7, max_entries=0
    )

    assert explicit["index"]["max_files"] == 7
    assert explicit["index"]["default_limited"] is False


def test_cache_stats_tolerates_legacy_cache_rows_without_namespace(
    service: ContextService,
) -> None:
    service.store.put_json(
        "cache:legacy-row",
        {
            "status": "active",
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "value": {"schema": "legacy.v1"},
        },
    )

    stats = service.context_admin(mode="cache_stats")

    assert stats["schema"] == "context_cache.stats.v1"
    assert stats["namespaces"]["unknown"]["active_count"] == 1
    assert "legacy-row" in stats["namespaces"]["unknown"]["sample_keys"]


def test_context_pack_benchmark_runs_offline(service: ContextService) -> None:
    benchmark = service.context_admin(mode="benchmark")

    assert benchmark["schema"] == "context_benchmark.v1"
    assert benchmark["run_count"] == 5
    assert [run["name"] for run in benchmark["runs"]] == [
        "cold_refresh",
        "warm_cache",
        "repeated_prompt",
        "prompt_variation_reuse",
        "compact_focus",
    ]
    assert benchmark["runs"][1]["cache_hit"] is True
    assert benchmark["runs"][2]["cache_hit"] is True
    assert benchmark["runs"][3]["cache_hit"] is False
    assert benchmark["runs"][3]["fragment_hit_ratio"] >= 0.2
    assert "search_ranking_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "reference_write_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "response_assembly_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert benchmark["runs"][0]["token_counting"]["token_count_source"] == "estimate"
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


def test_expired_search_cache_is_recomputed_and_pruned(
    service: ContextService,
) -> None:
    first = service.context_lookup(mode="search", query="auth token")
    cache_key, row = _search_fragment_cache_row(service, term="auth")
    row["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    service.store.put_json(cache_key, row)

    second = service.context_lookup(mode="search", query="auth token")

    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is False
    assert second["cache"]["status"] == "expired"
    assert second["cache"]["reason"] == "expired"
    assert second["results"]

    row = service.store.get_json(cache_key)
    row["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    service.store.put_json(cache_key, row)
    pruned = service.context_admin(mode="cache_prune", max_age_minutes=999999)

    assert pruned["expired_removed"] >= 1


def test_cache_default_ttl_and_prune_age_are_14_days(
    service: ContextService,
) -> None:
    service.context_lookup(mode="search", query="auth token")
    cache_key, row = _search_fragment_cache_row(service, term="auth")
    updated_at = datetime.fromisoformat(row["updated_at"])
    expires_at = datetime.fromisoformat(row["expires_at"])

    assert row["ttl_seconds"] == 14 * 24 * 60 * 60
    assert timedelta(days=13, hours=23) <= expires_at - updated_at <= timedelta(
        days=14, minutes=1
    )

    row["updated_at"] = (datetime.now(timezone.utc) - timedelta(days=13)).isoformat()
    row["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    service.store.put_json(cache_key, row)

    pruned = service.context_admin(mode="cache_prune")

    assert pruned["removed_entries"] == 0
    assert service.store.get_json(cache_key) is not None


def test_invalidated_context_pack_cache_reports_stale(
    service: ContextService,
) -> None:
    first = service.context_pack("review auth token behavior", max_items=2)
    cache_key = first["cache"]["key"]
    row = service.store.get_json(f"cache:{cache_key}")
    row["status"] = "invalidated"
    row["invalidated_at"] = datetime.now(timezone.utc).isoformat()
    service.store.put_json(f"cache:{cache_key}", row)

    second = service.context_pack("review auth token behavior", max_items=2)

    assert second["cache"]["hit"] is False
    assert second["cache"]["status"] == "stale"
    assert second["cache"]["reason"] == "invalidated"
    assert second["cache"]["warnings"][0]["code"] == "cache_stale"


def test_target_tokenizer_unavailable_falls_back_to_estimate(
    sample_repo: Path,
) -> None:
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
            token_counter_mode="target",
            target_tokenizer="definitely-not-a-real-tokenizer",
        )
    )

    pack = service.context_pack("review auth token behavior", max_items=2)

    token_counting = pack["metrics"]["token_counting"]
    assert token_counting["tokenizer"] == "definitely-not-a-real-tokenizer"
    assert token_counting["tokenizer_available"] is False
    assert token_counting["token_count_source"] == "estimate"
    assert token_counting["warnings"][0]["code"] == "target_tokenizer_unavailable"


def test_target_tokenizer_encode_failure_falls_back_to_estimate(
    sample_repo: Path,
) -> None:
    special_file = sample_repo / "src" / "special_token.py"
    special_file.write_text('SPECIAL = "<|endoftext|>"\n', encoding="utf-8")
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
            token_counter_mode="target",
            target_tokenizer="fake-target",
        )
    )

    class RejectingEncoding:
        def encode(self, text: str) -> list[int]:
            if "<|endoftext|>" in text:
                raise ValueError("special token disallowed")
            return [1] * max(1, len(text) // 4)

    service.token_counter._target_encoding = RejectingEncoding()
    service.token_counter._target_warning = None

    pack = service.context_pack(
        "review src/special_token.py special token handling",
        focus_paths=["src/special_token.py"],
        max_items=1,
    )

    token_counting = pack["metrics"]["token_counting"]
    assert pack["items"][0]["path"] == "src/special_token.py"
    assert token_counting["tokenizer"] == "fake-target"
    assert token_counting["tokenizer_available"] is True
    assert token_counting["token_count_source"] == "estimate"
    assert token_counting["warnings"][0]["code"] == "target_tokenizer_encode_failed"


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


def test_context_pack_prompt_variation_reuses_fragments(
    service: ContextService,
) -> None:
    first = service.context_pack(
        "review auth token behavior", max_items=1, output_profile="compact"
    )
    varied = service.context_pack(
        "inspect auth token flow", max_items=4, output_profile="normal"
    )

    assert first["items"]
    assert varied["items"]
    assert varied["cache"]["hit"] is False
    assert varied["cache"]["fragment_hits"] > 0
    assert varied["cache"]["fragment_hit_ratio"] >= 0.2

    metrics = service.context_admin(mode="metrics")
    assert metrics["cache"]["context_pack_fragment_hits"] >= varied["cache"][
        "fragment_hits"
    ]
    assert metrics["cache"]["context_pack_fragment_hit_ratio"] >= 0.2


def test_context_pack_cache_miss_uses_specific_reason(
    service: ContextService,
) -> None:
    service.context_pack("review auth token behavior", max_items=2)

    changed_terms = service.context_pack("review config settings behavior", max_items=2)

    assert changed_terms["cache"]["hit"] is False
    assert changed_terms["cache"]["reason"] == "terms_changed"


def test_context_pack_changed_signature_invalidates_fragments(
    service: ContextService, sample_repo: Path
) -> None:
    service.context_pack("review auth token behavior", max_items=2)
    auth_file = sample_repo / "src" / "auth.py"
    auth_file.write_text(
        auth_file.read_text(encoding="utf-8") + "\ndef token_refresh():\n    return True\n",
        encoding="utf-8",
    )

    changed = service.context_pack("inspect auth token flow", max_items=2)

    assert changed["cache"]["fragment_misses"] > 0
    assert any(
        detail["reason"] == "index_changed"
        for detail in changed["cache"]["miss_details"]
    )


def test_context_pack_missing_refresh_signature_skips_fragment_writes(
    service: ContextService, monkeypatch
) -> None:
    monkeypatch.setattr(service, "_current_refresh_signature", lambda: ("", False))

    pack = service.context_pack("review auth token behavior", max_items=2)
    stats = service.context_admin(mode="cache_stats")

    assert pack["cache"]["reason"] == "signature_unavailable"
    assert pack["cache"]["fragment_hits"] == 0
    assert all(
        detail["reason"] == "signature_unavailable"
        for detail in pack["cache"]["miss_details"]
    )
    assert "retrieval.search_term" not in stats["namespaces"]
    assert "retrieval.file_summary" not in stats["namespaces"]


def test_legacy_retrieval_cache_rows_are_stale_and_pruned(
    service: ContextService,
) -> None:
    service.store.put_json(
        "cache:legacy-search-fragment",
        {
            "status": "active",
            "namespace": "retrieval.search_term",
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "metadata": {"term": "auth"},
            "value": {"schema": "retrieval.search_term.v0"},
        },
    )

    stats = service.context_admin(mode="cache_stats")

    assert stats["namespaces"]["retrieval.search_term"]["stale_count"] == 1
    assert stats["namespaces"]["retrieval.search_term"]["legacy_count"] == 1

    pruned = service.context_admin(mode="cache_prune")

    assert pruned["stale_removed"] >= 1
    assert service.store.get_json("cache:legacy-search-fragment") is None


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


def test_context_pack_retrieval_cache_normalizes_paths_and_item_floor(
    service: ContextService, monkeypatch
) -> None:
    first = service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py", "tests/test_auth.py"],
        focus_paths=["src/auth.py"],
        max_items=2,
    )

    def fail_retrieval(*_args, **_kwargs):
        raise AssertionError("warm pack should reuse normalized retrieval cache")

    monkeypatch.setattr(service.index, "search", fail_retrieval)
    monkeypatch.setattr(service.index, "symbols", fail_retrieval)
    monkeypatch.setattr(service.index, "snippet_batch", fail_retrieval)

    warm = service.context_pack(
        "review auth token behavior",
        changed_files=["./tests/test_auth.py", "src/auth.py"],
        focus_paths=["tests/test_auth.py", "./src/auth.py"],
        max_items=4,
    )

    assert first["items"]
    assert warm["items"]
    assert warm["cache"]["hit"] is True
    assert warm["cache"]["reason"] == "hit"
    assert warm["metrics"]["stage_timings_ms"]["candidate_retrieval_ms"] == 0.0


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
    assert health["index"]["index_available"] is True
    assert health["index"]["search_mode"] == "term_index"
    assert index["index_available"] is True
    assert index["search_mode"] == "term_index"
    assert pack["repo"]["path"] == "."
    assert pack["repo"]["state_dir"] == "state"
    assert memory["repo_boundary_enforced"] is True
    assert references["references"][0]["repo_boundary_enforced"] is True
    assert references["references"][0]["uri"].startswith("repo://context/")
    assert metrics["tokens"]["token_counting"]["token_count_source"] == "estimate"

    public_payload = json.dumps(
        [health, index, pack["repo"], memory, references, metrics],
        sort_keys=True,
    )
    assert "context.lmdb" not in public_payload
    assert "lmdb" not in public_payload.lower()
    assert "storage_backend" not in public_payload
    assert "index_path" not in public_payload
    assert '"path": "state/store' not in public_payload
    assert str(sample_repo.resolve()) not in public_payload
    assert str(state_dir.resolve()) not in public_payload


def test_state_browser_lists_and_inspects_generated_state(
    service: ContextService, sample_repo: Path
) -> None:
    service.store.put_json(
        "debug:sample",
        {
            "schema": "debug.sample.v1",
            "path": str(sample_repo.resolve() / "src" / "auth.py"),
            "value": {"nested": True},
        },
    )

    listing = service.context_admin(
        mode="state_browser",
        state_prefix="debug:",
        max_entries=10,
        max_output_chars=500,
    )

    assert listing["schema"] == "context_state_browser.v1"
    assert listing["mode"] == "list"
    assert listing["repo_boundary_enforced"] is True
    assert listing["generated_state_only"] is True
    assert listing["rows"][0]["key"] == "debug:sample"
    assert listing["rows"][0]["schema"] == "debug.sample.v1"
    assert str(sample_repo.resolve()) not in json.dumps(listing, sort_keys=True)

    entry = service.context_admin(
        mode="state_browser",
        state_key="debug:sample",
        max_output_chars=1000,
    )

    assert entry["mode"] == "entry"
    assert entry["entry"]["key"] == "debug:sample"
    assert entry["entry"]["schema"] == "debug.sample.v1"
    assert "[REDACTED_HOST_PATH]" in entry["entry"]["preview"]
    assert str(sample_repo.resolve()) not in json.dumps(entry, sort_keys=True)


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
    assert "required = true" in guidance["codex_config_example"]["toml"]
    assert "enabled_tools" in guidance["codex_config_example"]["toml"]
    assert any(
        "AGENTS.md mandatory workflow" in layer
        for layer in guidance["enforcement_layers"]
    )
    assert "mandatory" in guidance["instruction"]
    assert "call context_pack first" in guidance["instruction"]
    assert "Must use context_lookup" in guidance["instruction"]
    assert "Must use context_admin" in guidance["instruction"]
    assert "Must use context_memory only" in guidance["instruction"]
    assert guidance["preferred_tool_order"] == [
        "context_pack",
        "context_lookup",
        "result_reference_resolve",
        "context_admin",
        "context_memory",
    ]
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
