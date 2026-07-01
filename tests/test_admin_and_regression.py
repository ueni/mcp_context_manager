from __future__ import annotations

from pathlib import Path

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService


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
    pack = service.context_pack("review auth token behavior", max_items=2)
    memory = service.context_memory(mode="get")
    references = service.context_lookup(mode="references")

    assert health["state_dir"] == str(state_dir.resolve())
    assert health["index"]["index_path"] == str(
        state_dir.resolve() / "index" / "context.sqlite3"
    )
    assert pack["repo"]["state_dir"] == str(state_dir.resolve())
    assert memory["path"] == str(state_dir.resolve() / "memory" / "context_memory.json")
    assert references["references"][0]["path"].startswith(str(state_dir.resolve()))
