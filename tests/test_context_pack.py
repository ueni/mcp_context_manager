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
    assert any(item["path"] == "src/auth.py" for item in pack["items"])
    assert any(item["path"] == "tests/test_auth.py" for item in pack["items"])
    assert pack["memory"]["summary_count"] >= 1
    assert pack["references"][0]["schema"] == "mcp_result_reference.v1"
    assert pack["budget"]["estimated_output_tokens"] > 0
    assert pack["safety"]["repository_boundary_enforced"] is True

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


def test_context_pack_reference_does_not_persist_raw_prompt(
    service: ContextService,
) -> None:
    raw_prompt = "review auth token behavior never-persist-this-private-task-text"

    pack = service.context_pack(raw_prompt, max_items=2)
    reference_id = pack["references"][0]["reference_id"]
    reference_path = service.config.references_dir / f"{reference_id}.json"
    stored = reference_path.read_text(encoding="utf-8")
    envelope = json.loads(stored)

    assert raw_prompt not in stored
    assert envelope["payload"]["prompt_sha256"]
    assert "prompt" not in envelope["payload"]


def test_context_pack_budget_omits_extra_candidates(service: ContextService) -> None:
    pack = service.context_pack(
        prompt="auth token login issue_token revoke_token README tests settings",
        max_output_chars=1800,
        max_items=1,
    )

    assert len(pack["items"]) == 1
    assert pack["omitted"]
    assert {row["reason_code"] for row in pack["omitted"]}.intersection({"item_limit", "budget_exhausted", "duplicate"})
