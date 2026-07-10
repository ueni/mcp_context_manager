from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Event, Lock
from typing import Any

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import (
    CACHE_LAST_PRUNED_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_WARMUP_MAX_FILES,
    GENERIC_RETRIEVAL_TERMS,
    WARMUP_AUTO_JOB_KIND,
    WARMUP_AUTO_LEARN_KEY,
    WARMUP_MANIFEST_KEY,
    ContextService,
)
from mcp_context_manager.tantivy_index import TantivySearchIndex


def _write_many_python_files(repo: Path, count: int = 12) -> None:
    for idx in range(count):
        (repo / f"file_{idx:03d}.py").write_text(
            "def marker():\n    return 'needle'\n" * 20,
            encoding="utf-8",
        )


def _write_fallback_only_file(repo: Path, term: str = "fallbackonly") -> None:
    indexed_terms = "\n".join(f"unique_token_{idx}" for idx in range(4100))
    (repo / "large.py").write_text(
        f"{indexed_terms}\n# {term} appears after the index term cap\n",
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


def _wait_for_auto_warmup(service: ContextService, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = service.store.get_json(WARMUP_AUTO_LEARN_KEY, {})
        background = service._background_status()[WARMUP_AUTO_JOB_KIND]
        if (
            isinstance(state, dict)
            and state.get("last_auto_status") == "complete"
            and not background.get("pending")
        ):
            return
        Event().wait(0.01)
    raise AssertionError("auto warmup did not complete")


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
    assert "context_pack.minimal.v1" in compact_pack_contract["output_schema_names"]

    service.context_admin(mode="index_refresh")
    first = service.context_lookup(mode="search", query="auth token")
    second = service.context_lookup(mode="search", query="auth token")
    _cache_key, fragment_row = _search_fragment_cache_row(service, term="auth")
    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is True
    assert second["cache"]["namespace"] == "context_lookup.search"
    assert second["cache"]["reason"] == "hit"
    assert fragment_row["metadata"]["backend_version"].startswith("tantivy:")
    assert fragment_row["value"]["backend_version"].startswith("tantivy:")

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
    assert metrics["cache"]["by_namespace"]["context_lookup.search"]["hits"] >= 1
    assert "context_pack.retrieval" not in metrics["cache"]["by_namespace"]
    assert metrics["cache"]["context_pack_fragment_hits"] >= warm_pack["cache"][
        "fragment_hits"
    ]
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
    assert warm_pack["cache"]["hit"] is False
    assert warm_pack["cache"]["namespace"] == "context_pack.fragments"
    assert warm_pack["cache"]["fragment_hits"] > 0
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
        "latency.context_admin.warmup.avg_elapsed_ms",
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

    matrix_bundle = service.context_admin(mode="metrics_and_matrix")
    assert matrix_bundle["schema"] == "context_metrics_and_matrix.v1"
    assert matrix_bundle["metrics"]["schema"] == "context_metrics.v1"
    assert matrix_bundle["matrix"]["schema"] == "context_measurement_matrix.v1"


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
    assert warmup["trigger"] == "manual"
    assert warmup["state"]["store_exists"] is True
    assert warmup["index"]["max_files"] == DEFAULT_WARMUP_MAX_FILES
    assert warmup["index"]["default_limited"] is True
    assert warmup["index"]["file_count"] >= 1
    assert warmup["workspace"]["file_count"] >= 1
    assert warmup["search_cache"]["namespace"] == "context_pack.search"
    assert warmup["search_cache"]["path"] == "src"
    assert warmup["stage_timings_ms"]["search_ms"] >= 0
    assert warmup["stage_timings_ms"]["file_summary_ms"] >= 0
    assert warmup["search_cache"]["query_count"] == 3
    assert {
        row["term"] for row in warmup["search_cache"]["queries"]
    }.isdisjoint(GENERIC_RETRIEVAL_TERMS)
    assert all(row["source"] in {"default_seed", "route_seed"} for row in warmup["search_cache"]["queries"])
    assert warmup["file_summary_cache"]["summary_count"] > 0
    assert warmup["file_summary_cache"]["summary_count"] <= (min(3 * 4, 80) + min(3 * 2, 40))
    assert warmup["file_summary_cache"]["hits"] + warmup["file_summary_cache"]["misses"] == warmup["file_summary_cache"]["summary_count"]
    assert (
        warmup["file_summary_cache"]["summary_count"]
        == int(sum(warmup["file_summary_cache"]["source_counts"].values()))
    )
    assert all(row["path"] == "src" for row in warmup["search_cache"]["queries"])
    assert fallback_flags == []
    assert warmup["cache"]["entry_count_after"] >= warmup["search_cache"]["query_count"]
    assert "retrieval.search_term" in warmup["cache"]["namespaces_after"]
    assert "context_lookup.search" not in warmup["cache"]["namespaces_after"]
    assert "context_pack.retrieval" not in warmup["cache"]["namespaces_after"]
    assert warmup["term_stats"]["namespace"] == "warmup.term_stats"
    assert warmup["term_stats"]["updated_count"] >= warmup["search_cache"]["query_count"]
    assert warmup["route_seeds"]["schema"] == "warmup.route_seeds.v1"
    assert warmup["hot_chunks"]["namespace"] == "warmup.hot_chunks"
    assert warmup["test_owner_targets"]["namespace"] == "warmup.test_owner_targets"
    assert warmup["manifest"]["schema"] == "warmup.manifest.v1"
    assert warmup["manifest"]["trigger"] == "manual"
    assert warmup["manifest"]["path_scope"] == "src"
    assert warmup["manifest"]["fallback_policy"] == {
        "context_pack_search_allow_fallback": False,
        "context_lookup_search_warmed": False,
    }
    assert service.store.get_json(WARMUP_MANIFEST_KEY)["refresh_signature"] == warmup[
        "manifest"
    ]["refresh_signature"]
    state = service.context_admin(
        mode="state_browser",
        state_prefix="warmup:",
        max_entries=20,
    )
    assert state["entry_count"] >= 3
    assert {
        row["schema"]
        for row in state["rows"]
        if row["schema"].startswith("warmup.")
    }.issuperset(
        {
            "warmup.manifest.v1",
            "warmup.negative_terms.v1",
            "warmup.route_seeds.v1",
        }
    )
    metrics = service.context_admin(mode="metrics")
    assert metrics["warmup"]["schema"] == "context_warmup.metrics.v1"
    assert metrics["warmup"]["count"] == 1
    assert metrics["warmup"]["manual_count"] == 1
    assert metrics["warmup"]["auto_count"] == 0
    assert metrics["warmup"]["query_count"] == warmup["search_cache"]["query_count"]
    assert metrics["warmup"]["last_query_count"] == warmup["search_cache"]["query_count"]
    assert metrics["warmup"]["last_elapsed_ms"] >= 0
    matrix = service.context_admin(mode="measurement_matrix")
    warmup_check = next(
        check
        for check in matrix["checks"]
        if check["key"] == "latency.context_admin.warmup.avg_elapsed_ms"
    )
    assert warmup_check["samples"] == 1
    assert warmup_check["current"] == metrics["warmup"]["avg_elapsed_ms"]

    fallback_flags.clear()
    lookup = service.context_lookup(mode="search", query="test", path="src")

    assert any(fallback_flags)
    assert lookup["cache"]["fragment_misses"] >= 1
    assert lookup["cache"]["namespace"] == "context_lookup.search"

    second = service.context_admin(mode="warmup", path="src", max_entries=3)

    assert second["search_cache"]["path"] == "src"
    assert all(row["cache_hit"] is True for row in second["search_cache"]["queries"])

    explicit = service.context_admin(
        mode="warmup", path="src", max_files=7, max_entries=0
    )

    assert explicit["index"]["max_files"] == 7
    assert explicit["index"]["default_limited"] is False
    assert explicit["search_cache"]["query_count"] == 0
    assert explicit["file_summary_cache"]["summary_count"] == 0
    assert explicit["stage_timings_ms"]["search_ms"] >= 0
    assert explicit["stage_timings_ms"]["file_summary_ms"] >= 0


def test_warmup_prefers_learned_route_terms_and_records_hot_chunks(
    service: ContextService,
) -> None:
    service.context_pack(
        "review auth token behavior",
        max_items=2,
        output_profile="compact",
    )

    warmup = service.context_admin(mode="warmup", max_entries=2)
    warmed_terms = [row["term"] for row in warmup["search_cache"]["queries"]]

    assert set(warmed_terms) == {"auth", "token"}
    assert all(row["source"] == "route_seed" for row in warmup["search_cache"]["queries"])
    assert set(warmup["route_seeds"]["route_seeds"]["review"][:2]) == {
        "auth",
        "token",
    }
    assert warmup["hot_chunks"]["target_count"] >= 1
    assert warmup["file_summary_cache"]["source_counts"]["hot_chunk"] >= 1
    assert {
        row["term"] for row in warmup["manifest"]["warmed_terms"]
    }.issuperset({"auth", "token"})
    assert warmup["manifest"]["warmed_summaries"]

    hot_rows = [
        row
        for _key, row in service.store.iter_json("warmup:hot_chunks:")
        if isinstance(row, dict)
    ]
    assert hot_rows
    assert all(row["file_fingerprint"]["cache_token"] for row in hot_rows)


def test_auto_learn_records_usage_without_enqueuing_before_threshold(
    sample_repo: Path,
) -> None:
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
            max_output_chars=6000,
            auto_learn_min_packs=3,
            auto_learn_min_interval_seconds=60,
            auto_learn_max_entries=4,
        )
    )

    service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )

    state = service.store.get_json(WARMUP_AUTO_LEARN_KEY)
    background = service._background_status()[WARMUP_AUTO_JOB_KIND]
    stats = service.context_admin(mode="cache_stats")

    assert state["schema"] == "warmup.auto_learn.v1"
    assert state["enabled"] is True
    assert state["observed_context_pack_count"] == 1
    assert state["last_reason_code"] == "below_min_packs"
    assert state["learned_target_counts"]["route_seeds"] >= 1
    assert state["learned_target_counts"]["hot_chunks"] >= 1
    assert background["status"] == "idle"
    assert background["pending"] is False
    assert stats["auto_learn"]["observed_context_pack_count"] == 1


def test_cache_stats_and_metrics_summary_do_not_iterate_warmup_target_prefixes(
    service: ContextService, monkeypatch
) -> None:
    original_iter_json = service.store.iter_json
    called_prefixes: list[str] = []

    def tracked_iter_json(prefix: str, *args, **kwargs):
        called_prefixes.append(prefix)
        return original_iter_json(prefix, *args, **kwargs)

    monkeypatch.setattr(service.store, "iter_json", tracked_iter_json)

    service.context_admin(mode="cache_stats")
    service.context_admin(mode="metrics")

    assert not any(
        str(prefix).startswith("warmup:hot_chunks:")
        or str(prefix).startswith("warmup:test_owner_targets:")
        for prefix in called_prefixes
    )


def test_auto_warmup_runs_and_warms_learned_fragment_layers(
    sample_repo: Path,
) -> None:
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
            max_output_chars=6000,
            auto_learn_min_packs=1,
            auto_learn_min_interval_seconds=0,
            auto_learn_max_entries=4,
        )
    )

    service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )
    _wait_for_auto_warmup(service)

    manifest = service.store.get_json(WARMUP_MANIFEST_KEY)
    stats = service.context_admin(mode="cache_stats")
    metrics = service.context_admin(mode="metrics")
    state = service.store.get_json(WARMUP_AUTO_LEARN_KEY)
    summary_sources = {
        row.get("source") for row in manifest["warmed_summaries"] if isinstance(row, dict)
    }

    assert manifest["trigger"] == "auto"
    assert {row["source"] for row in manifest["warmed_terms"]}.issubset(
        {"route_seed", "default_seed"}
    )
    assert {"hot_chunk", "test_owner"}.issubset(summary_sources)
    assert "retrieval.search_term" in stats["namespaces"]
    assert "retrieval.file_summary" in stats["namespaces"]
    assert "context_lookup.search" not in stats["namespaces"]
    assert "context_pack.retrieval" not in stats["namespaces"]
    assert state["last_auto_status"] == "complete"
    assert state["last_run_at"]
    assert metrics["warmup"]["auto_count"] >= 1
    assert metrics["warmup"]["manual_count"] == 0
    assert metrics["warmup"]["last_auto_status"] == "complete"


def test_auto_warmup_deduplicates_pending_jobs_and_respects_min_interval(
    sample_repo: Path,
    monkeypatch,
) -> None:
    service = ContextService(
        ContextConfig(
            repo_path=sample_repo.resolve(),
            state_dir=(sample_repo / ".mcp-context-manager").resolve(),
            max_output_chars=6000,
            auto_learn_min_packs=1,
            auto_learn_min_interval_seconds=60,
            auto_learn_max_entries=4,
        )
    )
    started = Event()
    release = Event()

    def slow_warmup(
        path: str = ".",
        max_files: int | None = None,
        max_entries: int = 100,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        started.set()
        release.wait(timeout=2)
        return {
            "schema": "context_cache.warmup.v1",
            "trigger": trigger,
            "search_cache": {"query_count": max_entries},
            "file_summary_cache": {"summary_count": 0, "hits": 0, "misses": 0},
        }

    monkeypatch.setattr(service, "_cache_warmup", slow_warmup)

    service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )
    assert started.wait(timeout=1)

    service.context_pack(
        "inspect auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )
    state = service.store.get_json(WARMUP_AUTO_LEARN_KEY)
    background = service._background_status()[WARMUP_AUTO_JOB_KIND]

    assert state["last_reason_code"] == "enqueue_deduplicated"
    assert background["deduplicated_count"] >= 1

    release.set()
    _wait_for_auto_warmup(service)

    service.context_pack(
        "debug auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )
    state = service.store.get_json(WARMUP_AUTO_LEARN_KEY)

    assert state["last_reason_code"] == "enqueue_throttled"


def test_auto_learn_cache_can_be_disabled_with_env(
    sample_repo: Path,
    monkeypatch,
) -> None:
    state_dir = sample_repo / ".mcp-context-manager-disabled"
    monkeypatch.setenv("REPO_PATH", str(sample_repo))
    monkeypatch.setenv("MCP_CONTEXT_STATE_DIR", str(state_dir))
    monkeypatch.setenv("MCP_CONTEXT_AUTO_LEARN_CACHE", "0")
    monkeypatch.setenv("MCP_CONTEXT_AUTO_LEARN_MIN_PACKS", "1")
    service = ContextService.from_env()

    service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py"],
        max_items=1,
        output_profile="compact",
    )

    state = service.store.get_json(WARMUP_AUTO_LEARN_KEY)
    background = service._background_status()[WARMUP_AUTO_JOB_KIND]

    assert service.config.auto_learn_cache is False
    assert state["enabled"] is False
    assert state["observed_context_pack_count"] == 1
    assert state["last_reason_code"] == "disabled"
    assert background["status"] == "idle"


def test_auto_learn_state_and_jobs_are_project_isolated(
    sample_repo: Path,
    tmp_path: Path,
    monkeypatch,
) -> None:
    services = [
        ContextService(
            ContextConfig(
                repo_path=sample_repo.resolve(),
                state_dir=(tmp_path / project_id).resolve(),
                project_id=project_id,
                max_output_chars=6000,
                auto_learn_min_packs=1,
                auto_learn_min_interval_seconds=60,
                auto_learn_max_entries=2,
            )
        )
        for project_id in ("project-a", "project-b")
    ]

    def no_op_warmup(
        path: str = ".",
        max_files: int | None = None,
        max_entries: int = 100,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        return {
            "schema": "context_cache.warmup.v1",
            "project_id": "",
            "trigger": trigger,
            "search_cache": {"query_count": max_entries},
        }

    for svc in services:
        monkeypatch.setattr(svc, "_cache_warmup", no_op_warmup)
        svc.context_pack(
            "review auth token behavior",
            changed_files=["src/auth.py"],
            max_items=1,
            output_profile="compact",
        )
        _wait_for_auto_warmup(svc)

    states = [svc.store.get_json(WARMUP_AUTO_LEARN_KEY) for svc in services]
    backgrounds = [svc._background_status()[WARMUP_AUTO_JOB_KIND] for svc in services]

    assert [state["project_id"] for state in states] == ["project-a", "project-b"]
    assert [state["observed_context_pack_count"] for state in states] == [1, 1]
    assert [row["project_id"] for row in backgrounds] == ["project-a", "project-b"]


def test_warmup_route_seeds_prefer_matching_path_scope(
    service: ContextService,
) -> None:
    service.store.put_json(
        "warmup:term_stats:global-auth",
        {
            "schema": "warmup.term_stats.v1",
            "term": "auth",
            "route": "review",
            "path_scope": ".",
            "result_count": 10,
            "result_count_total": 10,
            "selected_count": 5,
            "hit_total": 0,
            "miss_total": 1,
            "zero_result_count": 0,
            "never_selected_count": 0,
            "last_used_at": "2026-07-09T00:00:00+00:00",
        },
    )
    service.store.put_json(
        "warmup:term_stats:scoped-token",
        {
            "schema": "warmup.term_stats.v1",
            "term": "token",
            "route": "review",
            "path_scope": "src",
            "result_count": 4,
            "result_count_total": 4,
            "selected_count": 1,
            "hit_total": 0,
            "miss_total": 1,
            "zero_result_count": 0,
            "never_selected_count": 0,
            "last_used_at": "2026-07-09T01:00:00+00:00",
        },
    )

    warmup = service.context_admin(mode="warmup", path="src", max_entries=1)

    assert warmup["search_cache"]["queries"][0]["term"] == "token"
    assert warmup["route_seeds"]["path_scope"] == "src"
    assert warmup["route_seeds"]["route_seed_details"]["review"][0] == {
        "term": "token",
        "path_scope": "src",
        "selected_count": 1,
        "result_count_total": 4,
        "last_used_at": "2026-07-09T01:00:00+00:00",
    }


def test_warmup_raises_mixed_route_fragment_hit_ratio(
    service: ContextService,
) -> None:
    warmup = service.context_admin(mode="warmup", max_entries=20)
    assert warmup["search_cache"]["query_count"] >= 10
    assert warmup["file_summary_cache"]["summary_count"] > 0

    prompts = [
        ("Implement safer token handling in src/auth.py", ["src/auth.py"]),
        ("Review auth token behavior and related tests", []),
        ("Debug missing user login error in auth service", []),
        ("Update pytest coverage for issue_token auth behavior", []),
        ("Update README docs for authentication token behavior", []),
    ]
    fragment_hits = 0
    fragment_misses = 0
    for prompt, focus_paths in prompts:
        pack = service.context_pack(
            prompt,
            focus_paths=focus_paths,
            max_items=4,
            output_profile="compact",
        )
        assert pack["cache"]["namespace"] == "context_pack.fragments"
        fragment_hits += int(pack["cache"]["fragment_hits"])
        fragment_misses += int(pack["cache"]["fragment_misses"])

    assert fragment_hits + fragment_misses > 0
    assert fragment_hits / (fragment_hits + fragment_misses) >= 0.70


def test_warmup_skips_negative_and_generic_terms(
    service: ContextService,
    monkeypatch,
) -> None:
    service.store.put_json(
        "warmup:term_stats:generic-review",
        {
            "schema": "warmup.term_stats.v1",
            "term": "review",
            "route": "review",
            "path_scope": ".",
            "result_count": 5,
            "result_count_total": 5,
            "selected_count": 3,
            "hit_total": 0,
            "miss_total": 1,
            "zero_result_count": 0,
            "never_selected_count": 0,
            "last_used_at": "2026-07-09T00:00:00+00:00",
        },
    )
    service._warmup_update_term_stats(
        term="auth",
        route="coding",
        path_scope=".",
        result_count=0,
        cache_hit=False,
        selection_observed=False,
    )
    service._warmup_update_term_stats(
        term="auth",
        route="coding",
        path_scope=".",
        result_count=0,
        cache_hit=False,
        selection_observed=False,
    )

    original_search_fragment = service.index.search_fragment
    observed_terms: list[str] = []

    def tracked_search_fragment(*args, **kwargs):
        observed_terms.append(str(kwargs.get("term", args[0] if args else "")))
        return original_search_fragment(*args, **kwargs)

    monkeypatch.setattr(service.index, "search_fragment", tracked_search_fragment)

    warmup = service.context_admin(mode="warmup", max_entries=3)
    warmed_terms = [row["term"] for row in warmup["search_cache"]["queries"]]

    assert "auth" not in warmed_terms
    assert "review" not in warmed_terms
    assert set(warmed_terms).isdisjoint(GENERIC_RETRIEVAL_TERMS)
    assert observed_terms == warmed_terms
    assert any(
        row["term"] == "auth" and row["reason"] == "repeated_zero_results"
        for row in warmup["term_stats"]["negative_terms"]["terms"]
    )
    assert "auth" in warmup["manifest"]["negative_terms"]["terms"][0]["term"]


def test_warmup_term_stats_clear_zero_results_after_success(
    service: ContextService,
) -> None:
    for _ in range(2):
        service._warmup_update_term_stats(
            term="auth",
            route="coding",
            path_scope=".",
            result_count=0,
            cache_hit=False,
            selection_observed=False,
        )

    assert any(row["term"] == "auth" for row in service._warmup_negative_terms())

    service._warmup_update_term_stats(
        term="auth",
        route="coding",
        path_scope=".",
        result_count=3,
        selected_count=1,
        cache_hit=False,
        selection_observed=True,
    )

    row = next(
        row
        for _key, row in service.store.iter_json("warmup:term_stats:")
        if isinstance(row, dict) and row.get("term") == "auth"
    )
    assert row["zero_result_count"] == 0
    assert row["never_selected_count"] == 0
    assert not any(row["term"] == "auth" for row in service._warmup_negative_terms())


def test_warmup_negative_terms_are_route_and_scope_specific(
    service: ContextService,
) -> None:
    for _ in range(2):
        service._warmup_update_term_stats(
            term="auth",
            route="docs",
            path_scope="docs",
            result_count=0,
            cache_hit=False,
            selection_observed=False,
        )
    service.store.put_json(
        "warmup:term_stats:src-auth",
        {
            "schema": "warmup.term_stats.v1",
            "term": "auth",
            "route": "review",
            "path_scope": "src",
            "result_count": 4,
            "result_count_total": 4,
            "selected_count": 2,
            "hit_total": 0,
            "miss_total": 1,
            "zero_result_count": 0,
            "never_selected_count": 0,
            "last_used_at": "2026-07-09T01:00:00+00:00",
        },
    )

    warmup = service.context_admin(mode="warmup", path="src", max_entries=1)

    assert warmup["search_cache"]["queries"][0]["term"] == "auth"
    assert warmup["search_cache"]["queries"][0]["route"] == "review"
    assert warmup["term_stats"]["negative_terms"]["terms"][0]["route"] == "docs"


def test_warmup_negative_terms_are_capped_in_public_response(
    service: ContextService,
) -> None:
    for index in range(4):
        term = f"term{index}"
        for _ in range(2):
            service._warmup_update_term_stats(
                term=term,
                route="coding",
                path_scope=".",
                result_count=0,
                cache_hit=False,
                selection_observed=False,
            )

    warmup = service.context_admin(mode="warmup", max_entries=2)

    negative_terms = warmup["term_stats"]["negative_terms"]
    assert negative_terms["count"] >= 4
    assert len(negative_terms["terms"]) == 2
    assert negative_terms["omitted_count"] >= 2


def test_warmup_includes_test_owner_summary_targets(
    service: ContextService,
    monkeypatch,
) -> None:
    service.context_pack(
        "review src/auth.py auth behavior",
        focus_paths=["src/auth.py"],
        max_items=2,
        output_profile="compact",
    )

    original_cached_file_summary = service._cached_file_summary
    warmed_paths: list[str] = []

    def tracked_file_summary(
        path: str,
        line_anchor: int,
        refresh_signature: str,
        refresh_signature_available: bool,
    ) -> tuple[dict[str, Any], bool, dict[str, Any]]:
        warmed_paths.append(path)
        return original_cached_file_summary(
            path=path,
            line_anchor=line_anchor,
            refresh_signature=refresh_signature,
            refresh_signature_available=refresh_signature_available,
        )

    monkeypatch.setattr(service, "_cached_file_summary", tracked_file_summary)

    warmup = service.context_admin(mode="warmup", path="src", max_entries=1)

    assert "tests/test_auth.py" in warmed_paths
    assert warmup["test_owner_targets"]["target_count"] >= 1
    assert warmup["file_summary_cache"]["source_counts"]["test_owner"] >= 1


def test_context_lookup_search_keeps_public_search_path(
    service: ContextService,
    monkeypatch,
) -> None:
    original_search = service.index.search
    fallback_flags: list[bool] = []

    def counted_search(*args, **kwargs):
        fallback_flags.append(bool(kwargs.get("allow_fallback", True)))
        return original_search(*args, **kwargs)

    monkeypatch.setattr(service.index, "search", counted_search)

    result = service.context_lookup(mode="search", query="auth publicfallback")

    assert result["results"]
    assert {row["source"] for row in result["results"]} == {"tantivy"}
    assert fallback_flags
    assert all(fallback_flags)


def test_context_admin_warmup_scopes_symbol_summary_targets(service: ContextService, monkeypatch) -> None:
    scoped_summary_paths: list[str] = []

    original_cached_file_summary = service._cached_file_summary

    def tracked_file_summary(
        path: str,
        line_anchor: int,
        refresh_signature: str,
        refresh_signature_available: bool,
    ) -> tuple[dict[str, Any], bool, dict[str, Any]]:
        scoped_summary_paths.append(path)
        return original_cached_file_summary(
            path=path,
            line_anchor=line_anchor,
            refresh_signature=refresh_signature,
            refresh_signature_available=refresh_signature_available,
        )

    def staged_symbols(*, query: str = "", limit: int = 50) -> dict[str, Any]:
        symbols = [
            {"path": "src/test_helpers.py", "line_start": 1},
            {"path": "scripts/tools.py", "line_start": 1},
            {"path": "tests/test_auth.py", "line_start": 1},
        ]
        return {
            "schema": "context_symbols.v1",
            "count": len(symbols),
            "symbols": symbols[:limit],
        }

    monkeypatch.setattr(service, "_cached_file_summary", tracked_file_summary)
    monkeypatch.setattr(service.index, "symbols", staged_symbols)
    monkeypatch.setattr(service, "_warm_search_caches", lambda *args, **kwargs: ([], []))

    warmup = service.context_admin(mode="warmup", path="tests", max_entries=1)

    assert warmup["file_summary_cache"]["source_counts"]["symbol"] == 1
    assert warmup["file_summary_cache"]["summary_count"] == 1
    assert all(path == "tests" or path.startswith("tests/") for path in scoped_summary_paths)


def test_context_admin_warmup_serializes_concurrent_project_writes(
    sample_repo: Path,
) -> None:
    config = ContextConfig(
        repo_path=sample_repo.resolve(),
        state_dir=(sample_repo / ".mcp-context-manager").resolve(),
        max_output_chars=6000,
    )
    services = [ContextService(config), ContextService(config)]
    start = Barrier(2)

    def warmup(service: ContextService) -> dict:
        start.wait(timeout=2)
        return service.context_admin(mode="warmup", path="src", max_entries=3)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(warmup, service) for service in services]
        results = [future.result(timeout=5) for future in futures]

    assert [result["schema"] for result in results] == [
        "context_cache.warmup.v1",
        "context_cache.warmup.v1",
    ]
    assert all(result["search_cache"]["query_count"] == 3 for result in results)


def test_tantivy_sidecar_rebuild_serializes_shared_index_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    index_dir = tmp_path / "state" / "tantivy-index"
    records = [
        {
            "path": "src/a.py",
            "content": "needle = 'a'\n",
            "language": "python",
            "sha256": "a",
            "mtime_ns": 1,
        },
        {
            "path": "src/b.py",
            "content": "needle = 'b'\n",
            "language": "python",
            "sha256": "b",
            "mtime_ns": 2,
        },
    ]
    indexes = [TantivySearchIndex(index_dir), TantivySearchIndex(index_dir)]
    start = Barrier(2)
    guard = Lock()
    active_writes = 0
    overlap_detected = False
    original_write_full = TantivySearchIndex._write_full

    def slow_write_full(self: TantivySearchIndex, rows: list[dict[str, Any]]) -> None:
        nonlocal active_writes, overlap_detected
        with guard:
            overlap_detected = overlap_detected or active_writes > 0
            active_writes += 1
        try:
            time.sleep(0.05)
            original_write_full(self, rows)
        finally:
            with guard:
                active_writes -= 1

    monkeypatch.setattr(TantivySearchIndex, "_write_full", slow_write_full)

    def rebuild(index: TantivySearchIndex) -> int:
        start.wait(timeout=2)
        return index.rebuild(records)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(rebuild, index) for index in indexes]
        results = [future.result(timeout=5) for future in futures]

    assert results == [2, 2]
    assert overlap_detected is False
    assert indexes[0].search(["needle"], limit=10)


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
    assert benchmark["runs"][1]["cache_hit"] is False
    assert benchmark["runs"][1]["fragment_hits"] > 0
    assert benchmark["runs"][2]["cache_hit"] is False
    assert benchmark["runs"][2]["fragment_hits"] > 0
    assert benchmark["runs"][3]["cache_hit"] is False
    assert benchmark["runs"][3]["fragment_hit_ratio"] >= 0.2
    assert "search_ranking_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "search_fragment_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "search_merge_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "search_summary_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "symbol_lookup_ms" in benchmark["runs"][0]["stage_timings_ms"]
    assert "test_owner_summary_ms" in benchmark["runs"][0]["stage_timings_ms"]
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
    deadline = datetime.now(timezone.utc) + timedelta(seconds=2)
    while datetime.now(timezone.utc) < deadline:
        if not service._background_status()["cache_prune"]["pending"]:
            break
        Event().wait(0.01)
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


def test_cache_default_ttl_and_prune_age_are_30_days(
    service: ContextService,
) -> None:
    service.context_lookup(mode="search", query="auth token")
    cache_key, row = _search_fragment_cache_row(service, term="auth")
    updated_at = datetime.fromisoformat(row["updated_at"])
    expires_at = datetime.fromisoformat(row["expires_at"])

    assert row["ttl_seconds"] == 30 * 24 * 60 * 60
    assert row["ttl_seconds"] == DEFAULT_CACHE_TTL_SECONDS
    assert timedelta(days=29, hours=23) <= expires_at - updated_at <= timedelta(
        days=30, minutes=1
    )

    row["updated_at"] = (datetime.now(timezone.utc) - timedelta(days=29)).isoformat()
    row["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    service.store.put_json(cache_key, row)

    pruned = service.context_admin(mode="cache_prune")

    assert pruned["removed_entries"] == 0
    assert service.store.get_json(cache_key) is not None


def test_cache_prune_runs_opportunistically_when_due(
    service: ContextService,
) -> None:
    expired_key = "cache:manual-expired"
    service.store.put_json(
        expired_key,
        {
            "schema": "context_cache.entry.v2",
            "schema_version": 2,
            "created_at": (datetime.now(timezone.utc) - timedelta(days=31)).isoformat(),
            "updated_at": (datetime.now(timezone.utc) - timedelta(days=31)).isoformat(),
            "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
            "ttl_seconds": DEFAULT_CACHE_TTL_SECONDS,
            "status": "active",
            "namespace": "test",
            "key": "manual-expired",
            "metadata": {"schema_version": 2},
            "value": {"schema": "test.v1"},
        },
    )
    service.store.put_json(
        CACHE_LAST_PRUNED_KEY,
        {
            "schema": "context_cache.last_pruned.v1",
            "timestamp": 0.0,
            "updated_at": "1970-01-01T00:00:00+00:00",
        },
    )

    service.context_lookup(mode="search", query="auth token")

    deadline = datetime.now(timezone.utc) + timedelta(seconds=2)
    while datetime.now(timezone.utc) < deadline:
        if service.store.get_json(expired_key) is None:
            break
        Event().wait(0.01)
    assert service.store.get_json(expired_key) is None
    last_pruned = service.store.get_json(CACHE_LAST_PRUNED_KEY)
    assert isinstance(last_pruned, dict)
    assert last_pruned["result"]["expired_removed"] >= 1


def test_context_pack_does_not_write_whole_retrieval_cache(
    service: ContextService,
) -> None:
    pack = service.context_pack("review auth token behavior", max_items=2)

    whole_pack_rows = [
        row
        for _key, row in service.store.iter_json("cache:")
        if isinstance(row, dict) and row.get("namespace") == "context_pack.retrieval"
    ]

    assert pack["cache"]["hit"] is False
    assert pack["cache"]["namespace"] == "context_pack.fragments"
    assert pack["cache"]["key"].startswith("context_pack.fragments:")
    assert whole_pack_rows == []


def test_unsupported_context_pack_retrieval_rows_are_stale_and_pruned(
    service: ContextService,
) -> None:
    service.store.put_json(
        "cache:context_pack.retrieval:legacy",
        {
            "schema": "context_cache.entry.v2",
            "schema_version": 2,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "ttl_seconds": DEFAULT_CACHE_TTL_SECONDS,
            "status": "active",
            "namespace": "context_pack.retrieval",
            "key": "context_pack.retrieval:legacy",
            "metadata": {"schema_version": 2},
            "value": {"schema": "context_pack.retrieval.v1"},
        },
    )

    stats = service.context_admin(mode="cache_stats")

    assert stats["namespaces"]["context_pack.retrieval"]["stale_count"] == 1
    assert stats["namespaces"]["context_pack.retrieval"]["legacy_count"] == 0

    pruned = service.context_admin(mode="cache_prune")
    pack = service.context_pack("review auth token behavior", max_items=2)
    whole_pack_rows = [
        row
        for _key, row in service.store.iter_json("cache:")
        if isinstance(row, dict) and row.get("namespace") == "context_pack.retrieval"
    ]

    assert pruned["stale_removed"] >= 1
    assert service.store.get_json("cache:context_pack.retrieval:legacy") is None
    assert pack["cache"]["namespace"] == "context_pack.fragments"
    assert whole_pack_rows == []


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
    assert warm["cache"]["hit"] is False
    assert warm["cache"]["namespace"] == "context_pack.fragments"
    assert warm["cache"]["fragment_hits"] > 0
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
    assert changed_terms["cache"]["reason"] in {"fragment_miss", "terms_changed"}
    assert any(
        detail["reason"] == "terms_changed"
        for detail in changed_terms["cache"]["miss_details"]
    )


def test_context_pack_search_cache_policy_does_not_suppress_public_lookup(
    service: ContextService, monkeypatch
) -> None:
    service.context_pack("auth", max_items=2)

    original_search = service.index.search
    observed_fallback_flags: list[bool] = []

    def tracked_search(*args, **kwargs):
        observed_fallback_flags.append(bool(kwargs.get("allow_fallback", False)))
        return original_search(*args, **kwargs)

    monkeypatch.setattr(service.index, "search", tracked_search)

    lookup = service.context_lookup(mode="search", query="auth")

    assert any(observed_fallback_flags)
    assert lookup["cache"]["fragment_misses"] >= 1


def test_public_search_cache_with_fallback_does_not_reuse_index_only_context_pack_fragments(
    service: ContextService, monkeypatch
) -> None:
    service.context_lookup(mode="search", query="auth")

    original_search_fragment = service.index.search_fragment
    observed = []

    def tracked_search_fragment(*args, **kwargs):
        observed.append((args, kwargs))
        return original_search_fragment(*args, **kwargs)

    monkeypatch.setattr(service.index, "search_fragment", tracked_search_fragment)

    pack = service.context_pack("auth", max_items=1)

    assert observed
    assert pack["cache"]["fragment_misses"] >= 1
    assert any(
        detail.get("reason") == "fallback_policy_changed"
        for detail in pack["cache"]["miss_details"]
    )


def test_tantivy_fragment_beyond_legacy_term_cap_does_not_suppress_public_lookup(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_fallback_only_file(repo)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    service.context_pack("fallbackonly", max_items=1)

    original_search = service.index.search
    fallback_flags: list[bool] = []

    def tracked_search(*args, **kwargs):
        fallback_flags.append(bool(kwargs.get("allow_fallback", False)))
        return original_search(*args, **kwargs)

    monkeypatch.setattr(service.index, "search", tracked_search)

    lookup = service.context_lookup(mode="search", query="fallbackonly")

    assert any(fallback_flags)
    assert lookup["cache"]["fragment_misses"] >= 1
    assert lookup["results"]
    assert lookup["results"][0]["source"] == "tantivy"


def test_public_search_fragment_does_not_feed_index_only_context_pack_cache(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_fallback_only_file(repo)
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    lookup = service.context_lookup(mode="search", query="fallbackonly")
    assert lookup["results"]
    assert lookup["results"][0]["source"] == "tantivy"

    original_search_fragment = service.index.search_fragment
    observed = []

    def tracked_search_fragment(*args, **kwargs):
        observed.append((args, kwargs))
        return original_search_fragment(*args, **kwargs)

    monkeypatch.setattr(service.index, "search_fragment", tracked_search_fragment)

    pack = service.context_pack("fallbackonly", max_items=1)

    assert observed
    assert pack["cache"]["fragment_misses"] >= 1
    assert pack["items"]


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
        detail["reason"] in {"changed_chunk_digest", "index_changed"}
        for detail in changed["cache"]["miss_details"]
    )


def test_unrelated_edit_reuses_unchanged_chunk_summaries(
    service: ContextService, sample_repo: Path
) -> None:
    first = service.context_pack("review auth token behavior", max_items=2)
    unrelated = sample_repo / "config" / "other.toml"
    unrelated.write_text("other = true\n", encoding="utf-8")

    second = service.context_pack("review auth token behavior", max_items=2)

    assert first["items"]
    assert second["items"]
    assert second["cache"]["hit"] is False
    assert second["cache"]["fragment_hits"] > 0
    assert second["cache"]["index_freshness"]["state"] == "last_good"


def test_context_lookup_impact_chunk_and_cache_modes(service: ContextService) -> None:
    impact = service.context_lookup(mode="impact", path="src/auth.py")
    owners = service.context_lookup(mode="test_owners", path="src/auth.py")
    symbols = service.context_lookup(mode="related_symbols", path="src/auth.py")
    chunk = service.context_lookup(mode="chunk", path="src/auth.py", start_line=1)

    assert impact["schema"] == "context_lookup.impact.v1"
    assert any(row["path"] == "tests/test_auth.py" for row in impact["related"])
    assert owners["related"][0]["path"] == "tests/test_auth.py"
    assert symbols["symbols"]
    assert chunk["chunk"]["chunk_id"].startswith("chk_")

    service.context_pack("review auth token behavior", max_items=2)
    cache = service.context_lookup(mode="explain_cache", path="src/auth.py")
    assert cache["schema"] == "context_lookup.explain_cache.v1"
    assert "count" in cache


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


def test_quality_eval_and_tool_only_admin_modes(
    service: ContextService, sample_repo: Path
) -> None:
    fixture_dir = sample_repo / "benchmarks" / "gold_anchors"
    fixture_dir.mkdir(parents=True)
    (fixture_dir / "sample.json").write_text(
        json.dumps(
            {
                "task": "Review token handling and update tests",
                "changed_files": ["src/auth.py"],
                "expected_anchors": [
                    {"path": "src/auth.py", "required": True},
                    {"path": "tests/test_auth.py", "required": True},
                ],
            }
        ),
        encoding="utf-8",
    )

    quality = service.context_admin(mode="quality_eval")
    instructions = service.context_admin(mode="instructions")
    proxied = service.context_admin(mode="resource_proxy", path="repo://summary")
    profile = service.context_admin(mode="profile_calibrate")
    plan = service.context_admin(mode="cache_plan")

    assert quality["schema"] == "context_quality_eval.v1"
    assert quality["fixtures"] == 1
    assert quality["metrics"]["anchor_recall_at_5"] == 1.0
    assert instructions["schema"] == "codex_context_pack_first.instructions.v1"
    assert proxied["schema"] == "context_resource_proxy.v1"
    assert profile["profiles"]["codex"]["output_profile"] == "minimal"
    assert plan["schema"] == "context_budget_plan.v1"


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


def test_search_fragment_cache_backend_version_invalidates_old_rows(
    service: ContextService,
) -> None:
    service.context_lookup(mode="search", query="auth")
    cache_key, row = _search_fragment_cache_row(service, term="auth")
    row["metadata"]["backend_version"] = "term_index:legacy"
    row["value"]["backend_version"] = "term_index:legacy"
    service.store.put_json(cache_key, row)
    stale_lookup = service._cache_lookup(cache_key.removeprefix("cache:"))

    second = service.context_lookup(mode="search", query="auth")
    _new_cache_key, new_row = _search_fragment_cache_row(service, term="auth")

    assert stale_lookup["hit"] is False
    assert stale_lookup["reason"] == "backend_changed"
    assert second["cache"]["hit"] is False
    assert new_row["metadata"]["backend_version"] == service.index.search_backend_version()
    assert second["results"]


def test_warm_context_pack_reuses_search_fragments_for_response_assembly(
    service: ContextService, monkeypatch
) -> None:
    first = service.context_pack(
        "review auth login token behavior",
        max_items=2,
        max_output_chars=5000,
        output_profile="compact",
    )

    def fail_search(*_args, **_kwargs):
        raise AssertionError("warm pack should reuse cached search fragments")

    monkeypatch.setattr(service.index, "search", fail_search)

    warm = service.context_pack(
        "review auth login token behavior",
        max_items=2,
        max_output_chars=3200,
        output_profile="compact",
    )

    assert first["items"]
    assert warm["items"]
    assert warm["cache"]["hit"] is False
    assert warm["cache"]["namespace"] == "context_pack.fragments"
    assert warm["cache"]["fragment_hits"] > 0
    assert warm["metrics"]["stage_timings_ms"]["candidate_retrieval_ms"] >= 0.0
    assert warm["references"][0]["reference_id"] != first["references"][0]["reference_id"]


def test_cold_context_pack_uses_indexed_search_fragments_without_public_search(
    service: ContextService,
    monkeypatch,
) -> None:
    def fail_search(*_args, **_kwargs):
        raise AssertionError("context_pack should use indexed search fragments")

    monkeypatch.setattr(service.index, "search", fail_search)

    pack = service.context_pack(
        "review auth login fastpath",
        max_items=2,
        output_profile="compact",
        diagnostics="full",
    )

    timings = pack["metrics"]["stage_timings_ms"]
    assert pack["items"]
    assert pack["metrics"]["retrieval_plan"]["search_result_count"] >= 1
    assert timings["search_fragment_ms"] >= 0.0
    assert timings["search_merge_ms"] >= 0.0
    assert timings["search_summary_ms"] >= 0.0


def test_compact_context_pack_limits_search_summary_extraction(
    tmp_path: Path,
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

    pack = service.context_pack(
        "review needle",
        max_items=1,
        output_profile="compact",
        diagnostics="full",
    )
    plan = pack["metrics"]["retrieval_plan"]

    assert pack["items"]
    assert plan["search_result_count"] > plan["search_summary_limit"]
    assert plan["search_summary_limit"] == 8
    assert plan["search_summary_count"] == 8
    assert any(
        row.get("reason_code") == "search_summary_limit"
        for row in pack["omitted"]
    )


def test_stable_context_pack_uses_last_good_index_and_queues_refresh(
    service: ContextService,
    monkeypatch,
) -> None:
    service.context_pack("review auth token behavior", max_items=2)

    def fail_synchronous_signature(*_args, **_kwargs):
        raise AssertionError("stable pack should not scan repo freshness synchronously")

    monkeypatch.setattr(service.index, "refresh_signature", fail_synchronous_signature)

    pack = service.context_pack("review auth token behavior", max_items=2)
    metrics = service.context_admin(mode="metrics")

    assert pack["cache"]["hit"] is False
    assert pack["cache"]["namespace"] == "context_pack.fragments"
    assert pack["cache"]["fragment_hits"] > 0
    assert pack["cache"]["index_freshness"]["state"] == "last_good"
    assert pack["metrics"]["stage_timings_ms"]["index_refresh_ms"] < 50
    assert metrics["background"]["index_refresh"]["status"] in {
        "running",
        "failed",
        "throttled",
        "complete",
    }


def test_explicit_paths_refresh_before_context_pack_returns(
    service: ContextService,
    sample_repo: Path,
) -> None:
    service.context_pack("review auth token behavior", max_items=2)
    auth_file = sample_repo / "src" / "auth.py"
    auth_file.write_text(
        auth_file.read_text(encoding="utf-8") + "\ndef explicit_refresh_marker():\n    return True\n",
        encoding="utf-8",
    )

    pack = service.context_pack(
        "review src/auth.py explicit_refresh_marker",
        changed_files=["src/auth.py"],
        max_items=2,
        output_profile="normal",
    )

    assert pack["indexing"]["explicit_path_refresh"]["refreshed_count"] == 1
    assert pack["items"][0]["path"] == "src/auth.py"
    indexed = service.store.get_json("index:file:src/auth.py")
    assert "explicit_refresh_marker" in indexed["content"]


def test_health_status_reuses_cached_counts_and_refreshes_in_background(
    service: ContextService,
    monkeypatch,
) -> None:
    service.context_admin(mode="health")
    original_count = service.index.store.count
    cached = service.context_admin(mode="health")
    assert "index" in cached

    started = Event()
    release = Event()
    original = original_count

    def slow_count(_prefix: str) -> int:
        started.set()
        release.wait(1.0)
        return original(_prefix)

    service.index._status_cache_at = 0.0
    service.index._status_refresh_future = None
    monkeypatch.setattr(service.index.store, "count", slow_count)

    started_at = time.perf_counter()
    health = service.context_admin(mode="health")
    elapsed = time.perf_counter() - started_at

    assert elapsed < 0.5
    assert started.wait(0.5)
    assert health["index"]["file_count"] == cached["index"]["file_count"]

    release.set()
    if service.index._status_refresh_future is not None:
        service.index._status_refresh_future.result(timeout=2.0)


def test_background_index_refresh_deduplicates_concurrent_stable_packs(
    service: ContextService,
    monkeypatch,
) -> None:
    service.context_pack("review auth token behavior", max_items=2)
    started = Event()
    release = Event()
    original_refresh_if_needed = service.index.refresh_if_needed

    def slow_refresh_if_needed(*args, **kwargs):
        started.set()
        release.wait(timeout=2)
        return original_refresh_if_needed(*args, **kwargs)

    monkeypatch.setattr(service.index, "refresh_if_needed", slow_refresh_if_needed)

    first = service.context_pack("review auth token behavior", max_items=2)
    assert started.wait(timeout=1)
    second = service.context_pack("inspect auth token flow", max_items=2)
    release.set()

    assert first["cache"]["background_refresh_pending"] is True
    assert second["indexing"]["background"]["index_refresh"]["deduplicated_count"] >= 1


def test_context_pack_fragment_cache_normalizes_paths_and_item_floor(
    service: ContextService,
) -> None:
    first = service.context_pack(
        "review auth token behavior",
        changed_files=["src/auth.py", "tests/test_auth.py"],
        focus_paths=["src/auth.py"],
        max_items=2,
        output_profile="compact",
    )

    warm = service.context_pack(
        "review auth token behavior",
        changed_files=["./tests/test_auth.py", "src/auth.py"],
        focus_paths=["tests/test_auth.py", "./src/auth.py"],
        max_items=4,
        output_profile="compact",
    )

    assert first["items"]
    assert warm["items"]
    assert warm["cache"]["hit"] is False
    assert warm["cache"]["namespace"] == "context_pack.fragments"
    assert warm["cache"]["fragment_hits"] > 0
    assert not [
        row
        for _key, row in service.store.iter_json("cache:")
        if isinstance(row, dict) and row.get("namespace") == "context_pack.retrieval"
    ]


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
    assert health["index"]["search_mode"] == "tantivy"
    assert index["index_available"] is True
    assert index["search_mode"] == "tantivy"
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
            "created_at": "2026-07-01T10:00:00+00:00",
            "updated_at": "2026-07-01T10:00:00+00:00",
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
    assert listing["rows"][0]["created_at"] == "2026-07-01T10:00:00+00:00"
    assert listing["rows"][0]["updated_at"] == "2026-07-01T10:00:00+00:00"
    assert str(sample_repo.resolve()) not in json.dumps(listing, sort_keys=True)

    entry = service.context_admin(
        mode="state_browser",
        state_key="debug:sample",
        max_output_chars=1000,
    )

    assert entry["mode"] == "entry"
    assert entry["entry"]["key"] == "debug:sample"
    assert entry["entry"]["schema"] == "debug.sample.v1"
    assert entry["entry"]["created_at"] == "2026-07-01T10:00:00+00:00"
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
    assert pack["cache"]["hit"] is False
    assert pack["cache"]["fragment_hits"] > 0
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
    assert "The MCP caller sets client_profile" in guidance["instruction"]
    assert "Must use context_lookup" in guidance["instruction"]
    assert "Must use context_admin" in guidance["instruction"]
    assert "Must use context_memory only" in guidance["instruction"]
    assert guidance["profile_selection"]["set_by"] == (
        "MCP caller per context_pack request"
    )
    assert guidance["profile_selection"]["auto_detection"] is False
    assert guidance["profile_selection"]["precedence"] == [
        "explicit output_profile",
        "client_profile=codex implies output_profile=minimal when omitted",
        "configured default_output_profile",
    ]
    assert guidance["profile_selection"]["client_profiles"]["claude"] == {
        "model_profile": "anthropic",
        "recommended_output_profile": "compact",
    }
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
