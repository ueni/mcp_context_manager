from __future__ import annotations

import json

from mcp_context_manager.context import ContextService


def test_context_pack_returns_cited_budgeted_items_and_reference(service: ContextService) -> None:
    service.context_memory(
        mode="summary_upsert",
        namespace="route/coding",
        focus="auth",
        summary="Auth changes usually need tests/test_auth.py.",
        ttl_days=7,
        source="test",
    )

    pack = service.context_pack(
        prompt="Implement safer token handling in src/auth.py and update tests.",
        focus_paths=["src/auth.py"],
        changed_files=["tests/test_auth.py"],
        memory_session="ticket-1",
        max_output_chars=3500,
        max_items=4,
    )

    assert pack["schema"] == "context_pack.v1"
    assert pack["summary"]["route"] in {"coding", "security", "test"}
    assert pack["items"]
    assert {item["kind"] for item in pack["items"]} == {"summary"}
    assert any(item["path"] == "src/auth.py" for item in pack["items"])
    assert any(item["path"] == "tests/test_auth.py" for item in pack["items"])
    assert all(item["detail_lookup"]["mode"] == "snippet" for item in pack["items"])
    assert pack["memory"]["summary_count"] >= 1
    assert pack["references"][0]["schema"] == "mcp_result_reference.v1"
    assert pack["references"][0]["uri"].startswith("repo://context/")
    assert "storage" not in pack["references"][0]
    assert "path" not in pack["references"][0]["resolver"]
    assert pack["budget"]["estimated_output_tokens"] > 0
    assert pack["budget"]["token_counting"]["token_count_source"] == "estimate"
    assert pack["safety"]["repository_boundary_enforced"] is True
    assert pack["items"][0]["confidence"] > 0
    assert pack["metrics"]["stage_timings_ms"]["index_refresh_ms"] >= 0
    assert pack["metrics"]["stage_timings_ms"]["search_ranking_ms"] >= 0
    assert pack["metrics"]["stage_timings_ms"]["reference_write_ms"] >= 0
    assert pack["metrics"]["stage_timings_ms"]["response_assembly_ms"] >= 0
    assert pack["metrics"]["stage_timings_ms"]["snippet_batch_ms"] >= 0
    assert pack["metrics"]["baseline_input_tokens_est"] >= pack["metrics"]["output_tokens_est"]
    assert pack["metrics"]["token_counting"]["token_count_source"] == "estimate"
    assert pack["metrics"]["token_savings_formula"] == "max(0, baseline_input_tokens_est - output_tokens_est)"
    assert pack["metrics"]["tokens_spared_by_mcp_formula"] == "max(0, baseline_input_tokens_est - output_tokens_est)"
    assert (
        pack["metrics"]["tokens_spared_by_mcp_est"]
        == pack["metrics"]["estimated_input_tokens_saved"]
    )
    assert "MCP context_pack" in pack["metrics"]["tokens_spared_by_mcp_reason"]
    assert pack["metrics"]["external_tool_calls_saved_est"] >= 1
    assert pack["metrics"]["references_bytes_deferred_est"] > 0
    assert pack["metrics"]["retrieval_plan"]["detail_mode"] == "context_lookup.snippet"
    assert pack["metrics"]["retrieval_plan"]["snippet_request_count"] == 0
    assert pack["cache"]["namespace"] == "context_pack.retrieval"
    assert pack["cache"]["reason"] in {
        "miss",
        "arg_changed",
        "stale_index",
        "disabled_refresh_index",
    }
    assert pack["cache"]["status"] in {"missing", "active", "disabled"}

    resolved = service.result_reference_resolve(reference=pack["references"][0])
    assert resolved["status"] == "resolved"
    assert resolved["content"]["candidate_count"] >= pack["summary"]["item_count"]


def test_context_pack_redacts_secret_like_content(service: ContextService, sample_repo) -> None:
    secret_file = sample_repo / "src" / "secretish.py"
    secret_file.write_text('API_TOKEN = "super-secret-token-value-123456"\n', encoding="utf-8")

    pack = service.context_pack(
        prompt="review src/secretish.py token handling",
        focus_paths=["src/secretish.py"],
        max_items=1,
    )

    assert "[REDACTED_SECRET_" in pack["items"][0]["content"]
    assert pack["items"][0]["redactions"]


def test_context_pack_compact_does_not_build_snippets(
    service: ContextService, monkeypatch
) -> None:
    def fail_snippet_batch(*_args, **_kwargs):
        raise AssertionError("context_pack should return summaries, not snippets")

    monkeypatch.setattr(service.index, "snippet_batch", fail_snippet_batch)

    pack = service.context_pack(
        prompt="review auth token behavior",
        max_items=2,
        output_profile="compact",
    )

    assert pack["items"]
    assert {item["kind"] for item in pack["items"]} == {"summary"}
    assert pack["metrics"]["retrieval_plan"]["snippet_request_count"] == 0
    assert pack["metrics"]["stage_timings_ms"]["snippet_batch_ms"] == 0.0


def test_file_summary_uses_matched_line_excerpt(
    service: ContextService, sample_repo
) -> None:
    deep_file = sample_repo / "src" / "deep.py"
    deep_file.write_text(
        "\n".join(
            [
                "import os",
                "import sys",
                "",
                "def unrelated():",
                "    return 'header'",
                "",
                "",
                "def target_marker():",
                "    return 'needle'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    service.context_admin(mode="index_refresh")

    summary = service.index.file_summary(
        "src/deep.py",
        matched_line=8,
        max_chars=300,
    )

    assert "target_marker" in summary["content"]
    assert "import os" not in summary["content"]
    assert summary["start_line"] < 8 <= summary["end_line"]


def test_context_pack_reads_explicit_text_file_that_is_not_indexed(
    service: ContextService, sample_repo
) -> None:
    lock_file = sample_repo / "poetry.lock"
    lock_file.write_text(
        "[[package]]\nname = \"critical-dependency\"\nversion = \"1.2.3\"\n",
        encoding="utf-8",
    )

    pack = service.context_pack(
        prompt="review dependency lock",
        focus_paths=["poetry.lock"],
        max_items=1,
    )

    assert pack["items"][0]["path"] == "poetry.lock"
    assert "critical-dependency" in pack["items"][0]["content"]
    assert not any(
        row.get("path") == "poetry.lock"
        and row.get("reason_code") == "unreadable_explicit_path"
        for row in pack["omitted"]
    )


def test_context_pack_reference_does_not_persist_raw_prompt(
    service: ContextService,
) -> None:
    raw_prompt = "review auth token behavior never-persist-this-private-task-text"

    pack = service.context_pack(raw_prompt, max_items=2)
    reference_id = pack["references"][0]["reference_id"]
    stored = _stored_reference_body(service, reference_id)
    envelope = json.loads(stored)

    assert raw_prompt not in stored
    assert envelope["payload"]["prompt_sha256"]
    assert "prompt" not in envelope["payload"]


def _stored_reference_body(service: ContextService, reference_id: str) -> str:
    record = service.references.store.get_json(f"reference:{reference_id}", {})
    if record.get("storage") == "file":
        return (
            service.config.references_dir / str(record.get("path", ""))
        ).read_text(encoding="utf-8")
    return str(record.get("body", ""))


def test_context_pack_budget_omits_extra_candidates(service: ContextService) -> None:
    pack = service.context_pack(
        prompt="auth token login issue_token revoke_token README tests settings",
        max_output_chars=1800,
        max_items=1,
    )

    assert len(pack["items"]) == 1
    assert pack["omitted"]
    assert {row["reason_code"] for row in pack["omitted"]}.intersection({"item_limit", "budget_exhausted", "duplicate"})
