from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import ContextConfig
from .util import now_iso, parse_iso, sha256_text


class ResultReferences:
    def __init__(self, config: ContextConfig):
        self.config = config

    def create(self, producer: str, payload: Any, summary: dict[str, Any], ttl_hours: int = 24) -> dict[str, Any]:
        self.config.ensure_state_dirs()
        reference_id = f"ctxref-{uuid.uuid4().hex[:16]}"
        body = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
        digest = sha256_text(body)
        path = self.config.references_dir / f"{reference_id}.json"
        path.write_text(body, encoding="utf-8")
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).isoformat()
        return {
            "schema": "mcp_result_reference.v1",
            "reference_id": reference_id,
            "producer_tool": producer,
            "project_id": self.config.project_id,
            "created_at": now_iso(),
            "expires_at": expires_at,
            "summary": summary,
            "content": {
                "mime_type": "application/json",
                "encoding": "utf-8",
                "size_bytes": len(body.encode("utf-8")),
                "sha256": digest,
            },
            "retention": {"ttl_hours": ttl_hours, "policy": "local_generated_state"},
            "sensitivity": {"redacted": True, "payload_embedded": False},
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
        if not reference_id or not reference_id.startswith("ctxref-"):
            return {"schema": "mcp_result_reference.resolve.v1", "status": "invalid_reference"}
        path = self.config.references_dir / f"{reference_id}.json"
        try:
            path.relative_to(self.config.state_dir)
        except ValueError:
            return {"schema": "mcp_result_reference.resolve.v1", "status": "boundary_rejected"}
        if reference and parse_iso(str(reference.get("expires_at", ""))):
            expires_at = parse_iso(str(reference.get("expires_at")))
            if expires_at and expires_at < datetime.now(timezone.utc):
                return {"schema": "mcp_result_reference.resolve.v1", "status": "expired", "reference_id": reference_id}
        if not path.is_file():
            return {"schema": "mcp_result_reference.resolve.v1", "status": "missing", "reference_id": reference_id}
        text = path.read_text(encoding="utf-8")
        digest = sha256_text(text)
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
            "content": json.loads(text),
            "content_sha256": digest,
        }
