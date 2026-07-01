from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import ContextConfig
from .util import (
    merge_redaction_metadata,
    now_iso,
    parse_iso,
    sanitize_json,
    sha256_text,
)

REFERENCE_ID_RE = re.compile(r"^ctxref-[A-Za-z0-9_-]{1,64}$")


class ResultReferences:
    def __init__(self, config: ContextConfig):
        self.config = config

    def create(self, producer: str, payload: Any, summary: dict[str, Any], ttl_hours: int = 24) -> dict[str, Any]:
        self.config.ensure_state_dirs()
        reference_id = f"ctxref-{uuid.uuid4().hex[:16]}"
        created_at = now_iso()
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).isoformat()
        sanitized_payload, payload_sensitivity = sanitize_json(payload)
        sanitized_summary, summary_sensitivity = sanitize_json(summary)
        sensitivity = merge_redaction_metadata(payload_sensitivity, summary_sensitivity)
        envelope = {
            "schema": "mcp_result_reference.envelope.v1",
            "metadata": {
                "reference_id": reference_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "producer_tool": producer,
                "project_id": self.config.project_id,
            },
            "summary": sanitized_summary,
            "payload": sanitized_payload,
            "sensitivity": {**sensitivity, "payload_embedded": False},
        }
        body = json.dumps(envelope, indent=2, sort_keys=True, ensure_ascii=False)
        digest = sha256_text(body)
        path = self.config.references_dir / f"{reference_id}.json"
        path.write_text(body, encoding="utf-8")
        return {
            "schema": "mcp_result_reference.v1",
            "reference_id": reference_id,
            "producer_tool": producer,
            "project_id": self.config.project_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "summary": sanitized_summary,
            "content": {
                "mime_type": "application/json",
                "encoding": "utf-8",
                "size_bytes": len(body.encode("utf-8")),
                "sha256": digest,
            },
            "retention": {"ttl_hours": ttl_hours, "policy": "local_generated_state"},
            "sensitivity": {**sensitivity, "payload_embedded": False},
            "resolver": {
                "tool": "result_reference_resolve",
                "uri": (
                    f"repo://project/{self.config.project_id}/context/{reference_id}"
                    if self.config.project_id
                    else f"repo://context/{reference_id}"
                ),
                "path": self.config.display_path(path),
                "repo_boundary_enforced": True,
            },
        }

    def resolve(
        self,
        reference_id: str = "",
        reference: dict[str, Any] | None = None,
        expected_hash: str = "",
    ) -> dict[str, Any]:
        if reference:
            reference_id = str(reference.get("reference_id", reference_id))
            expected_hash = str(reference.get("content", {}).get("sha256", expected_hash))
        if not reference_id or not REFERENCE_ID_RE.match(reference_id):
            return {"schema": "mcp_result_reference.resolve.v1", "status": "invalid_reference"}
        path = self.config.references_dir / f"{reference_id}.json"
        try:
            path.relative_to(self.config.state_dir)
        except ValueError:
            return {"schema": "mcp_result_reference.resolve.v1", "status": "boundary_rejected"}
        reference_expires_at = (
            parse_iso(str(reference.get("expires_at", ""))) if reference else None
        )
        if reference_expires_at and reference_expires_at < datetime.now(timezone.utc):
            return {"schema": "mcp_result_reference.resolve.v1", "status": "expired", "reference_id": reference_id}
        if not path.is_file():
            return {"schema": "mcp_result_reference.resolve.v1", "status": "missing", "reference_id": reference_id}
        text = path.read_text(encoding="utf-8")
        digest = sha256_text(text)
        content = json.loads(text)
        metadata: dict[str, Any] = {}
        summary: dict[str, Any] = {}
        sensitivity: dict[str, Any] = {
            "redacted": False,
            "redaction_count": 0,
            "categories": [],
            "payload_embedded": False,
        }
        if isinstance(content, dict) and content.get("schema") == "mcp_result_reference.envelope.v1":
            metadata = content.get("metadata") if isinstance(content.get("metadata"), dict) else {}
            summary = content.get("summary") if isinstance(content.get("summary"), dict) else {}
            envelope_expires_at = parse_iso(str(metadata.get("expires_at", "")))
            if envelope_expires_at and envelope_expires_at < datetime.now(timezone.utc):
                return {"schema": "mcp_result_reference.resolve.v1", "status": "expired", "reference_id": reference_id}
            stored_reference_id = str(metadata.get("reference_id") or "")
            if stored_reference_id and stored_reference_id != reference_id:
                return {
                    "schema": "mcp_result_reference.resolve.v1",
                    "status": "invalid_reference",
                    "reference_id": reference_id,
                }
            payload = content.get("payload")
            if isinstance(content.get("sensitivity"), dict):
                sensitivity = content["sensitivity"]
        else:
            payload = content
        if expected_hash and digest != expected_hash:
            return {
                "schema": "mcp_result_reference.resolve.v1",
                "status": "hash_mismatch",
                "reference_id": reference_id,
                "actual_sha256": digest,
            }
        return {
            "schema": "mcp_result_reference.resolve.v1",
            "status": "resolved",
            "reference_id": reference_id,
            "content": payload,
            "content_sha256": digest,
            "metadata": metadata,
            "summary": summary,
            "sensitivity": sensitivity,
        }
