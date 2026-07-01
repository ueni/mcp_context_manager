from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mcp_context_manager.context import ContextService


def test_memory_ttl_decision_priority_and_compaction(service: ContextService) -> None:
    service.context_memory(
        mode="upsert",
        namespace="session/demo",
        key="expired",
        value={"file_paths": ["missing.py"]},
        ttl_days=-1,
        source="test",
    )
    assert service.context_memory(mode="get", namespace="session/demo")["count"] == 0
    assert service.context_memory(mode="get", namespace="session/demo", include_expired=True)["count"] == 1

    service.context_memory(
        mode="decision_record",
        namespace="workspace",
        topic="test-runner",
        decision={"command": "pytest"},
        decided_by="llm",
        confidence=0.9,
    )
    service.context_memory(
        mode="decision_record",
        namespace="workspace",
        topic="test-runner",
        decision={"command": "python -m pytest"},
        decided_by="human",
        confidence=0.8,
    )
    decisions = service.context_memory(mode="get", namespace="workspace")["effective_decisions"]
    assert decisions[0]["decision"]["command"] == "python -m pytest"

    for idx in range(45):
        service.context_memory(
            mode="upsert",
            namespace="session/busy",
            key=f"k{idx}",
            value={"n": idx},
            source="test",
        )
    compact = service.context_memory(mode="compact", namespace="session/busy")
    assert compact["compacted"] is True
    summaries = service.context_memory(mode="get", namespace="session/busy")["summaries"]
    assert any(row["focus"] == "auto_compact" for row in summaries)


def test_memory_validation_reports_stale_paths(service: ContextService) -> None:
    service.context_memory(
        mode="upsert",
        namespace="workspace",
        key="paths",
        value={"file_paths": ["does-not-exist.py"]},
        source="test",
    )
    validation = service.context_memory(mode="validate")
    assert validation["schema"] == "context_memory.validate.v1"
    assert validation["stale_count"] >= 1


def test_result_reference_resolve_statuses(service: ContextService) -> None:
    ref = service.references.create(
        producer="test",
        payload={"hello": "world"},
        summary={"kind": "unit"},
        ttl_hours=24,
    )

    resolved = service.result_reference_resolve(reference=ref)
    assert resolved["status"] == "resolved"
    assert resolved["content"] == {"hello": "world"}

    mismatch = service.result_reference_resolve(reference_id=ref["reference_id"], expected_hash="bad")
    assert mismatch["status"] == "hash_mismatch"

    expired_ref = dict(ref)
    expired_ref["expires_at"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    expired = service.result_reference_resolve(reference=expired_ref)
    assert expired["status"] == "expired"

    invalid = service.result_reference_resolve(reference_id="../nope")
    assert invalid["status"] == "invalid_reference"
