from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import ContextConfig
from .store import ContextStore
from .util import (
    merge_redaction_metadata,
    now_iso,
    parse_iso,
    sanitize_json,
    sha256_text,
)

REFERENCE_ID_RE = re.compile(r"^ctxref-[A-Za-z0-9_-]{1,64}$")
INLINE_REFERENCE_MAX_BYTES = 131_072


class ResultReferences:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)

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
        body_size = len(body.encode("utf-8"))
        storage = "inline"
        record: dict[str, Any] = {
            "schema": "mcp_result_reference.store.v1",
            "reference_id": reference_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "storage": storage,
            "size_bytes": body_size,
            "sha256": digest,
            "body": body,
        }
        if body_size > INLINE_REFERENCE_MAX_BYTES:
            storage = "file"
            path = self.config.references_dir / f"{reference_id}.json"
            path.write_text(body, encoding="utf-8")
            record = {
                "schema": "mcp_result_reference.store.v1",
                "reference_id": reference_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "storage": storage,
                "size_bytes": body_size,
                "sha256": digest,
                "path": path.name,
            }
        self.store.put_json(f"reference:{reference_id}", record)
        uri = self._public_uri(reference_id)
        return {
            "schema": "mcp_result_reference.v1",
            "reference_id": reference_id,
            "uri": uri,
            "producer_tool": producer,
            "project_id": self.config.project_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "status": "active",
            "summary": sanitized_summary,
            "content": {
                "mime_type": "application/json",
                "encoding": "utf-8",
                "size_bytes": body_size,
                "sha256": digest,
            },
            "retention": {"ttl_hours": ttl_hours, "policy": "local_generated_state"},
            "sensitivity": {**sensitivity, "payload_embedded": False},
            "repo_boundary_enforced": True,
            "resolver": {
                "tool": "result_reference_resolve",
                "uri": uri,
                "repo_boundary_enforced": True,
            },
        }

    def list(self, limit: int = 20) -> list[dict[str, Any]]:
        refs = []
        for _key, row in self.store.iter_json("reference:"):
            if not isinstance(row, dict):
                continue
            refs.append(self._public_list_row(row))
        refs.sort(key=lambda row: str(row.get("created_at", "")), reverse=True)
        return refs[:limit]

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
        reference_expires_at = (
            parse_iso(str(reference.get("expires_at", ""))) if reference else None
        )
        if reference_expires_at and reference_expires_at < datetime.now(timezone.utc):
            return self._resolve_status(
                "expired",
                reference_id,
                reason="provided_reference_expired",
            )
        record = self.store.get_json(f"reference:{reference_id}")
        if not isinstance(record, dict):
            return {"schema": "mcp_result_reference.resolve.v1", "status": "missing", "reference_id": reference_id}
        record_status = self._record_status(record)
        if record_status["status"] != "active":
            return self._resolve_status(
                str(record_status["status"]),
                reference_id,
                reason=str(record_status["reason"]),
                expires_at=str(record.get("expires_at", "")),
            )
        text = self._body_from_record(record)
        if not text:
            return self._resolve_status(
                "stale",
                reference_id,
                reason="payload_unavailable",
                expires_at=str(record.get("expires_at", "")),
            )
        digest = sha256_text(text)
        if digest != str(record.get("sha256", digest)):
            return {
                **self._resolve_status(
                    "stale",
                    reference_id,
                    reason="stored_hash_mismatch",
                    expires_at=str(record.get("expires_at", "")),
                ),
                "actual_sha256": digest,
            }
        try:
            content = json.loads(text)
        except json.JSONDecodeError:
            return self._resolve_status(
                "stale",
                reference_id,
                reason="invalid_json_payload",
                expires_at=str(record.get("expires_at", "")),
            )
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
                return self._resolve_status(
                    "expired",
                    reference_id,
                    reason="envelope_expired",
                    expires_at=str(metadata.get("expires_at", "")),
                )
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
            "uri": self._public_uri(reference_id),
            "content": payload,
            "content_sha256": digest,
            "metadata": metadata,
            "summary": summary,
            "sensitivity": sensitivity,
            "repo_boundary_enforced": True,
            "warnings": [],
        }

    def _public_list_row(self, row: dict[str, Any]) -> dict[str, Any]:
        reference_id = str(row.get("reference_id", ""))
        status = self._record_status(row)
        return {
            "reference_id": reference_id,
            "uri": self._public_uri(reference_id) if reference_id else "",
            "created_at": str(row.get("created_at", "")),
            "expires_at": str(row.get("expires_at", "")),
            "sha256": str(row.get("sha256", "")),
            "size_bytes": int(row.get("size_bytes", 0) or 0),
            "status": status["status"],
            "warnings": status["warnings"],
            "repo_boundary_enforced": True,
        }

    def _body_from_record(self, row: dict[str, Any]) -> str:
        storage = str(row.get("storage") or "inline")
        if storage in {"inline", "lmdb"}:
            body = row.get("body")
            return body if isinstance(body, str) else ""
        file_name = str(row.get("path") or "")
        path = self.config.references_dir / file_name
        try:
            path.relative_to(self.config.state_dir)
        except ValueError:
            return ""
        if not path.is_file():
            return ""
        return path.read_text(encoding="utf-8")

    def _public_uri(self, reference_id: str) -> str:
        return (
            f"repo://project/{self.config.project_id}/context/{reference_id}"
            if self.config.project_id
            else f"repo://context/{reference_id}"
        )

    def _record_status(self, row: dict[str, Any]) -> dict[str, Any]:
        if str(row.get("status", "")).lower() in {"stale", "invalidated"}:
            return {
                "status": "stale",
                "reason": "invalidated",
                "warnings": [self._warning("reference_stale", "reference invalidated")],
            }
        if row.get("invalidated_at"):
            return {
                "status": "stale",
                "reason": "invalidated",
                "warnings": [self._warning("reference_stale", "reference invalidated")],
            }
        expires_at = parse_iso(str(row.get("expires_at", "")))
        if expires_at and expires_at < datetime.now(timezone.utc):
            return {
                "status": "expired",
                "reason": "expired",
                "warnings": [self._warning("reference_expired", "reference expired")],
            }
        return {"status": "active", "reason": "", "warnings": []}

    def _resolve_status(
        self,
        status: str,
        reference_id: str,
        reason: str,
        expires_at: str = "",
    ) -> dict[str, Any]:
        warning_code = "reference_expired" if status == "expired" else "reference_stale"
        return {
            "schema": "mcp_result_reference.resolve.v1",
            "status": status,
            "reference_id": reference_id,
            "uri": self._public_uri(reference_id),
            "expires_at": expires_at,
            "reason": reason,
            "repo_boundary_enforced": True,
            "warnings": [self._warning(warning_code, reason)],
        }

    def _warning(self, code: str, message: str) -> dict[str, str]:
        return {"code": code, "message": message}
