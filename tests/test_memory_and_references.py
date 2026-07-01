from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

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

    missing = service.result_reference_resolve(reference_id="ctxref-0000000000000000")
    assert missing["status"] == "missing"


def test_memory_upsert_redacts_secret_values_before_persisting(
    service: ContextService,
) -> None:
    result = service.context_memory(
        mode="upsert",
        namespace="workspace",
        key="credentials",
        value={
            "note": "api_key=super-secret-value-123456",
            "path": "/home/user/source/private-repo",
        },
        source="test",
    )

    stored = service.config.memory_path.read_text(encoding="utf-8")
    payload = json.loads(stored)

    assert result["sensitivity"]["redacted"] is True
    assert "super-secret-value-123456" not in stored
    assert "/home/user/source/private-repo" not in stored
    assert payload["entries"][0]["value"]["note"].startswith("[REDACTED_SECRET_")
    assert payload["entries"][0]["value"]["path"] == "[REDACTED_HOST_PATH]"


def test_memory_rejects_sensitive_identifiers(service: ContextService) -> None:
    with pytest.raises(ValueError, match="unsafe memory identifier"):
        service.context_memory(
            mode="upsert",
            namespace="workspace",
            key="Bearer abcdefghijklmnopqrstuvwxyz",
            value={"ok": True},
        )


def test_summary_and_decision_payloads_are_redacted_before_persisting(
    service: ContextService,
) -> None:
    service.context_memory(
        mode="summary_upsert",
        namespace="workspace",
        focus="setup",
        summary="password=super-secret-value-123456 is configured in /tmp/private",
    )
    service.context_memory(
        mode="decision_record",
        namespace="workspace",
        topic="runner",
        decision={"command": "TOKEN=super-secret-value-123456 pytest"},
        rationale="validated from /home/user/project/log.txt",
    )

    stored = service.config.memory_path.read_text(encoding="utf-8")

    assert "super-secret-value-123456" not in stored
    assert "/home/user/project/log.txt" not in stored
    assert "/tmp/private" not in stored


def test_result_reference_create_sanitizes_payload_and_summary(
    service: ContextService,
) -> None:
    ref = service.references.create(
        producer="test",
        payload={
            "secret": "api_key=super-secret-value-123456",
            "host_path": "/home/user/source/private-repo",
        },
        summary={"note": "token=another-secret-value-123456"},
    )

    stored = (service.config.references_dir / f"{ref['reference_id']}.json").read_text(
        encoding="utf-8"
    )
    resolved = service.result_reference_resolve(reference_id=ref["reference_id"])

    assert ref["sensitivity"]["redacted"] is True
    assert "super-secret-value-123456" not in stored
    assert "another-secret-value-123456" not in stored
    assert "/home/user/source/private-repo" not in stored
    assert resolved["content"]["secret"].startswith("[REDACTED_SECRET_")
    assert resolved["content"]["host_path"] == "[REDACTED_HOST_PATH]"


def test_expired_reference_envelope_is_enforced_by_id_only(
    service: ContextService,
) -> None:
    ref = service.references.create(
        producer="test",
        payload={"hello": "world"},
        summary={"kind": "expired"},
        ttl_hours=-1,
    )

    expired = service.result_reference_resolve(reference_id=ref["reference_id"])

    assert expired["status"] == "expired"
