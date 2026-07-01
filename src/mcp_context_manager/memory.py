from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import ContextConfig
from .util import (
    expiry_iso,
    is_expired,
    load_json_file,
    now_iso,
    parse_iso,
    save_json_file,
)


class ContextMemory:
    def __init__(self, config: ContextConfig):
        self.config = config

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(
            self.config.memory_path,
            {"schema": "context_memory_store.v1", "entries": [], "summaries": [], "decisions": []},
        )
        if not isinstance(payload, dict):
            payload = {}
        return {
            "schema": "context_memory_store.v1",
            "entries": payload.get("entries") if isinstance(payload.get("entries"), list) else [],
            "summaries": payload.get("summaries") if isinstance(payload.get("summaries"), list) else [],
            "decisions": payload.get("decisions") if isinstance(payload.get("decisions"), list) else [],
        }

    def _save(self, payload: dict[str, Any]) -> None:
        self.config.ensure_state_dirs()
        save_json_file(self.config.memory_path, payload)

    def upsert(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl_days: int | None = None,
        confidence: float = 1.0,
        source: str = "agent",
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        if not namespace.strip() or not key.strip():
            raise ValueError("namespace and key are required")
        if confidence < 0 or confidence > 1:
            raise ValueError("confidence must be in range [0, 1]")
        payload = self._load()
        now = now_iso()
        expires_at = expiry_iso(ttl_days)
        updated = False
        for row in payload["entries"]:
            if row.get("namespace") == namespace and row.get("key") == key:
                row.update(
                    {
                        "value": value,
                        "confidence": confidence,
                        "source": source,
                        "tags": tags or [],
                        "updated_at": now,
                        "expires_at": expires_at,
                    }
                )
                updated = True
                break
        if not updated:
            payload["entries"].append(
                {
                    "namespace": namespace,
                    "key": key,
                    "value": value,
                    "confidence": confidence,
                    "source": source,
                    "tags": tags or [],
                    "created_at": now,
                    "updated_at": now,
                    "expires_at": expires_at,
                }
            )
        self._save(payload)
        return {
            "schema": "context_memory.upsert.v1",
            "path": str(self.config.memory_path.relative_to(self.config.repo_path)),
            "namespace": namespace,
            "key": key,
            "updated": True,
            "expires_at": expires_at,
        }

    def summary_upsert(
        self,
        namespace: str,
        focus: str,
        summary: str,
        ttl_days: int | None = None,
        confidence: float = 1.0,
        source: str = "agent",
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        if not namespace.strip() or not focus.strip():
            raise ValueError("namespace and focus are required")
        payload = self._load()
        now = now_iso()
        expires_at = expiry_iso(ttl_days)
        for row in payload["summaries"]:
            if row.get("namespace") == namespace and row.get("focus") == focus:
                row.update(
                    {
                        "summary": summary,
                        "confidence": confidence,
                        "source": source,
                        "tags": tags or [],
                        "updated_at": now,
                        "expires_at": expires_at,
                    }
                )
                self._save(payload)
                return {"schema": "context_memory.summary_upsert.v1", "namespace": namespace, "focus": focus, "updated": True}
        payload["summaries"].append(
            {
                "namespace": namespace,
                "focus": focus,
                "summary": summary,
                "confidence": confidence,
                "source": source,
                "tags": tags or [],
                "created_at": now,
                "updated_at": now,
                "expires_at": expires_at,
            }
        )
        self._save(payload)
        return {"schema": "context_memory.summary_upsert.v1", "namespace": namespace, "focus": focus, "updated": True}

    def decision_record(
        self,
        namespace: str,
        topic: str,
        decision: Any,
        decided_by: str = "llm",
        rationale: str = "",
        ttl_days: int | None = None,
        confidence: float = 1.0,
        source: str = "agent",
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        if decided_by not in {"human", "llm"}:
            raise ValueError("decided_by must be human or llm")
        payload = self._load()
        row = {
            "id": f"decision-{len(payload['decisions']) + 1}",
            "namespace": namespace,
            "topic": topic,
            "decision": decision,
            "decided_by": decided_by,
            "rationale": rationale,
            "confidence": confidence,
            "source": source,
            "tags": tags or [],
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "expires_at": expiry_iso(ttl_days),
        }
        payload["decisions"].append(row)
        self._save(payload)
        return {
            "schema": "context_memory.decision_record.v1",
            "recorded": row,
            "effective_decision": self.effective_decisions(namespace=namespace, topic=topic)[:1],
        }

    def get(
        self,
        namespace: str | None = None,
        include_expired: bool = False,
        max_entries: int = 100,
    ) -> dict[str, Any]:
        payload = self._load()
        entries = [
            {**row, "expired": is_expired(row.get("expires_at"))}
            for row in payload["entries"]
            if (namespace is None or row.get("namespace") == namespace)
            and (include_expired or not is_expired(row.get("expires_at")))
        ][:max_entries]
        summaries = [
            {**row, "expired": is_expired(row.get("expires_at"))}
            for row in payload["summaries"]
            if (namespace is None or row.get("namespace") == namespace)
            and (include_expired or not is_expired(row.get("expires_at")))
        ][:max_entries]
        decisions = self.effective_decisions(namespace=namespace, include_expired=include_expired)[:max_entries]
        return {
            "schema": "context_memory.get.v1",
            "path": str(self.config.memory_path.relative_to(self.config.repo_path)),
            "count": len(entries),
            "entries": entries,
            "summary_count": len(summaries),
            "summaries": summaries,
            "effective_decision_count": len(decisions),
            "effective_decisions": decisions,
        }

    def effective_decisions(
        self,
        namespace: str | None = None,
        topic: str | None = None,
        include_expired: bool = False,
    ) -> list[dict[str, Any]]:
        rows = []
        for row in self._load()["decisions"]:
            if namespace is not None and row.get("namespace") != namespace:
                continue
            if topic is not None and row.get("topic") != topic:
                continue
            if not include_expired and is_expired(row.get("expires_at")):
                continue
            rows.append({**row, "expired": is_expired(row.get("expires_at"))})
        rows.sort(
            key=lambda row: (
                1 if row.get("decided_by") == "human" else 0,
                float(row.get("confidence", 0.0) or 0.0),
                parse_iso(row.get("updated_at") or row.get("created_at") or "") or datetime.fromtimestamp(0, timezone.utc),
            ),
            reverse=True,
        )
        seen: set[tuple[str, str]] = set()
        effective = []
        for row in rows:
            key = (str(row.get("namespace")), str(row.get("topic")))
            if key in seen:
                continue
            seen.add(key)
            effective.append(row)
        return effective

    def validate(self, validate_paths: bool = True) -> dict[str, Any]:
        payload = self._load()
        stale_entries = []
        missing_metadata = []
        for row in payload["entries"]:
            if is_expired(row.get("expires_at")):
                stale_entries.append({"namespace": row.get("namespace"), "key": row.get("key"), "reason": "expired"})
            for required in ["source", "confidence", "created_at", "updated_at"]:
                if required not in row:
                    missing_metadata.append({"kind": "entry", "key": row.get("key"), "field": required})
            if validate_paths and isinstance(row.get("value"), dict):
                for rel in row["value"].get("file_paths", []):
                    try:
                        if not self.config.resolve_repo_path(rel).exists():
                            stale_entries.append({"namespace": row.get("namespace"), "key": row.get("key"), "reason": "stale_path", "path": rel})
                    except ValueError:
                        stale_entries.append({"namespace": row.get("namespace"), "key": row.get("key"), "reason": "unsafe_path"})
        return {
            "schema": "context_memory.validate.v1",
            "entry_count": len(payload["entries"]),
            "summary_count": len(payload["summaries"]),
            "decision_count": len(payload["decisions"]),
            "stale_count": len(stale_entries),
            "stale_entries": stale_entries,
            "missing_metadata": missing_metadata,
        }

    def compact(
        self,
        namespace: str | None = None,
        threshold_entries: int = 40,
        keep_entries: int = 12,
        summary_max_chars: int = 1200,
    ) -> dict[str, Any]:
        payload = self._load()
        rows = [row for row in payload["entries"] if namespace is None or row.get("namespace") == namespace]
        if len(rows) <= threshold_entries:
            return {"schema": "context_memory.compact.v1", "compacted": False, "entry_count": len(rows)}
        rows.sort(key=lambda row: (float(row.get("confidence", 0.0) or 0.0), row.get("updated_at", "")), reverse=True)
        kept = rows[:keep_entries]
        lines = []
        for row in kept:
            lines.append(f"- {row.get('key')}: {str(row.get('value'))[:160]}")
        self.summary_upsert(
            namespace=namespace or "global",
            focus="auto_compact",
            summary=("\n".join(lines))[:summary_max_chars],
            ttl_days=60,
            confidence=0.9,
            source="context_memory.compact",
            tags=["auto", "compact"],
        )
        return {
            "schema": "context_memory.compact.v1",
            "compacted": True,
            "entry_count": len(rows),
            "kept_entries": len(kept),
            "summary_focus": "auto_compact",
        }
