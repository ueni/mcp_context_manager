from __future__ import annotations

import json
import re
import time
from typing import Any

from .config import ContextConfig
from .index import ContextIndex
from .memory import ContextMemory
from .references import ResultReferences
from .schemas import output_contracts
from .util import (
    classify_route,
    estimate_tokens,
    load_json_file,
    normalize_query_terms,
    now_iso,
    prompt_injection_signals,
    save_json_file,
    sha256_text,
)


class ContextService:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.index = ContextIndex(config)
        self.memory = ContextMemory(config)
        self.references = ResultReferences(config)

    @classmethod
    def from_env(cls) -> "ContextService":
        return cls(ContextConfig.from_env())

    def context_lookup(
        self,
        mode: str = "search",
        query: str = "",
        path: str = ".",
        start_line: int = 1,
        end_line: int | None = None,
        max_results: int = 20,
        max_entries: int = 200,
        max_depth: int = 2,
        include_globs: list[str] | None = None,
    ) -> dict[str, Any]:
        allowed = {"search", "snippet", "tree", "symbols", "references"}
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "search":
            self._ensure_index_fresh(path=path)
            cache_key = self._cache_key(
                "search",
                {
                    "query": query,
                    "path": path,
                    "max_results": max_results,
                    "include_globs": include_globs or [],
                    "index": self.index.status().get("generated_at", ""),
                },
            )
            cached = self._cache_get(cache_key)
            if cached:
                return {**cached, "cache": {"hit": True, "key": cache_key}}
            result = self.index.search(
                query=query,
                path=path,
                max_results=max_results,
                include_globs=include_globs,
            )
            self._cache_set(cache_key, result)
            return {**result, "cache": {"hit": False, "key": cache_key}}
        if mode == "snippet":
            self._ensure_index_fresh(path=path)
            return self.index.snippet(path=path, start_line=start_line, end_line=end_line)
        if mode == "tree":
            self._ensure_index_fresh(path=path)
            return self.index.tree(path=path, max_entries=max_entries, max_depth=max_depth)
        if mode == "symbols":
            self._ensure_index_fresh()
            return self.index.symbols(query=query, limit=max_results)
        return {
            "schema": "context_references.list.v1",
            "references": self._reference_list(limit=max_results),
        }

    def context_memory(
        self,
        mode: str = "get",
        namespace: str | None = None,
        key: str | None = None,
        value: Any = None,
        ttl_days: int | None = None,
        confidence: float = 1.0,
        source: str = "agent",
        tags: list[str] | None = None,
        focus: str = "",
        summary: str = "",
        topic: str = "",
        decision: Any = None,
        decided_by: str = "llm",
        rationale: str = "",
        include_expired: bool = False,
        max_entries: int = 100,
    ) -> dict[str, Any]:
        allowed = {"get", "upsert", "summary_upsert", "decision_record", "validate", "compact"}
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "upsert":
            if namespace is None or key is None:
                raise ValueError("namespace and key are required for upsert")
            return self.memory.upsert(namespace, key, value, ttl_days, confidence, source, tags)
        if mode == "summary_upsert":
            if namespace is None:
                raise ValueError("namespace is required for summary_upsert")
            return self.memory.summary_upsert(
                namespace, focus, summary, ttl_days, confidence, source, tags
            )
        if mode == "decision_record":
            if namespace is None:
                raise ValueError("namespace is required for decision_record")
            return self.memory.decision_record(
                namespace,
                topic,
                decision,
                decided_by,
                rationale,
                ttl_days,
                confidence,
                source,
                tags,
            )
        if mode == "validate":
            return self.memory.validate()
        if mode == "compact":
            return self.memory.compact(namespace=namespace)
        return self.memory.get(namespace=namespace, include_expired=include_expired, max_entries=max_entries)

    def context_admin(
        self,
        mode: str = "health",
        path: str = ".",
        max_files: int = 5000,
        max_age_minutes: int = 1440,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
        tool_name: str = "",
    ) -> dict[str, Any]:
        allowed = {
            "health",
            "index_refresh",
            "index_status",
            "cache_stats",
            "cache_prune",
            "budget",
            "contracts",
        }
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "health":
            return {
                "schema": "context_admin.health.v1",
                "ok": True,
                "repo_path": str(self.config.repo_path),
                "project_id": self.config.project_id,
                "state_dir": self.config.display_path(self.config.state_dir),
                "index": self.index.status(),
            }
        if mode == "index_refresh":
            return self.index.refresh(path=path, max_files=max_files)
        if mode == "index_status":
            return self.index.status()
        if mode == "cache_stats":
            return {"schema": "context_cache.stats.v1", **self._cache_stats()}
        if mode == "cache_prune":
            return {"schema": "context_cache.prune.v1", **self._cache_prune(max_age_minutes)}
        if mode == "contracts":
            return output_contracts(tool_name=tool_name)
        return self._budget(max_output_chars, default_output_profile)

    def context_pack(
        self,
        prompt: str,
        changed_files: list[str] | None = None,
        focus_paths: list[str] | None = None,
        memory_session: str = "default",
        max_output_chars: int | None = None,
        output_profile: str | None = None,
        max_items: int = 8,
        refresh_index: bool = False,
    ) -> dict[str, Any]:
        if not prompt.strip():
            raise ValueError("prompt is required")
        started = time.perf_counter()
        self.config.ensure_state_dirs()
        self._ensure_index_fresh(max_files=5000)
        profile = output_profile or self._budget()["default_output_profile"]
        budget = max_output_chars or int(self._budget()["max_output_chars"])
        route = classify_route(prompt)
        terms = normalize_query_terms(prompt, max_terms=12)
        explicit_paths = self._collect_paths(prompt, changed_files or [], focus_paths or [])
        memory_context = self._memory_context(route=route, session=memory_session)
        candidates: list[dict[str, Any]] = []
        omitted: list[dict[str, Any]] = []

        for rel in explicit_paths:
            try:
                snippet = self.index.snippet(
                    rel,
                    start_line=1,
                    end_line=80 if profile != "compact" else 40,
                    max_chars=1800 if profile != "compact" else 900,
                )
                candidates.append(
                    self._candidate_from_snippet(
                        snippet,
                        score=12.0,
                        reason_codes=["explicit_path"],
                        source="explicit_path",
                    )
                )
            except Exception as exc:
                omitted.append({"path": rel, "reason_code": "unreadable_explicit_path", "detail": type(exc).__name__})

        if terms:
            try:
                search = self.index.search(query=" ".join(terms), max_results=max(max_items * 3, 12))
                for row in search["results"]:
                    path = row["path"]
                    line = int(row.get("line") or self._first_matching_line(path, terms) or 1)
                    try:
                        snippet = self.index.snippet(
                            path,
                            start_line=line,
                            end_line=line,
                            context_before=3,
                            context_after=8,
                            max_chars=1000 if profile == "compact" else 1800,
                        )
                    except Exception:
                        continue
                    candidates.append(
                        self._candidate_from_snippet(
                            snippet,
                            score=float(row.get("score", 1.0)) + 4.0,
                            reason_codes=["lexical_match"],
                            source=str(row.get("source", "search")),
                        )
                    )
            except Exception as exc:
                omitted.append({"reason_code": "search_failed", "detail": type(exc).__name__})

        try:
            symbols = self.index.symbols(query=" ".join(terms), limit=max_items * 2)
            for row in symbols["symbols"]:
                try:
                    snippet = self.index.snippet(
                        row["path"],
                        start_line=max(1, int(row["line_start"])),
                        end_line=max(1, int(row["line_end"])),
                        context_before=2,
                        context_after=6,
                        max_chars=1000,
                    )
                except Exception:
                    continue
                candidates.append(
                    self._candidate_from_snippet(
                        snippet,
                        score=8.0,
                        reason_codes=["symbol_match", str(row.get("kind", "symbol"))],
                        source="symbol_index",
                        symbol=row,
                    )
                )
        except Exception:
            pass

        selected, omitted_budget = self._select_candidates(candidates, max_items=max_items, content_budget=max(1000, budget - 2400))
        omitted.extend(omitted_budget)
        full_reference = self.references.create(
            producer="context_pack",
            payload={
                "prompt": prompt,
                "route": route,
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "candidates": candidates,
                "omitted": omitted,
            },
            summary={"route": route, "candidate_count": len(candidates), "selected_count": len(selected)},
            ttl_hours=24,
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        result = {
            "schema": "context_pack.v1",
            "generated_at": now_iso(),
            "repo": {
                "path": str(self.config.repo_path),
                "state_dir": self.config.display_path(self.config.state_dir),
                "project_id": self.config.project_id,
                "root_uri_hash": sha256_text(self.config.root_uri)
                if self.config.root_uri
                else "",
            },
            "request": {
                "prompt": prompt,
                "route": route,
                "terms": terms,
                "changed_files": changed_files or [],
                "focus_paths": focus_paths or [],
                "memory_session": memory_session,
                "output_profile": profile,
            },
            "budget": {
                "max_output_chars": budget,
                "estimated_output_tokens": estimate_tokens(json.dumps(selected, ensure_ascii=False)),
            },
            "summary": {
                "item_count": len(selected),
                "candidate_count": len(candidates),
                "omitted_count": len(omitted),
                "route": route,
            },
            "items": selected,
            "memory": memory_context,
            "omitted": omitted,
            "references": [full_reference],
            "safety": {
                "repository_boundary_enforced": True,
                "generated_state_only": True,
                "untrusted_content_signals": self._aggregate_signals(selected),
            },
            "metrics": {
                "elapsed_ms": elapsed_ms,
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "estimated_input_tokens_saved": max(0, sum(int(item.get("raw_chars", 0)) for item in selected) // 4 - estimate_tokens(json.dumps(selected, ensure_ascii=False))),
            },
            "next_actions": [
                {"action": "resolve_reference", "when": "Need full omitted candidate evidence", "reference_id": full_reference["reference_id"]}
            ],
        }
        return result

    def result_reference_resolve(
        self,
        reference_id: str = "",
        reference: dict[str, Any] | None = None,
        expected_hash: str = "",
    ) -> dict[str, Any]:
        return self.references.resolve(reference_id=reference_id, reference=reference, expected_hash=expected_hash)

    def repo_summary_resource(self) -> str:
        return json.dumps(self.index.workspace_facts(), indent=2, sort_keys=True)

    def repo_file_resource(self, path: str) -> str:
        snippet = self.index.snippet(path=path, start_line=1, end_line=100000, max_chars=self.config.max_output_chars)
        return snippet["content"]

    def repo_tree_resource(self, path: str) -> str:
        return json.dumps(self.index.tree(path=path), indent=2, sort_keys=True)

    def repo_context_resource(self, reference_id: str) -> str:
        return json.dumps(self.references.resolve(reference_id=reference_id), indent=2, sort_keys=True)

    def _ensure_index_fresh(
        self, path: str = ".", max_files: int = 5000
    ) -> dict[str, Any]:
        return self.index.refresh(path=path, max_files=max_files)

    def _collect_paths(self, prompt: str, changed: list[str], focus: list[str]) -> list[str]:
        found: list[str] = []
        for item in [*changed, *focus]:
            if item and item not in found:
                found.append(item)
        for match in re.findall(r"(?<![\w/.-])[\w./-]+\.[A-Za-z0-9]{1,8}(?=\b|:)", prompt):
            if match not in found:
                found.append(match)
        safe: list[str] = []
        for rel in found:
            try:
                self.config.resolve_repo_path(rel)
            except ValueError:
                continue
            safe.append(rel)
        return safe[:12]

    def _first_matching_line(self, path: str, terms: list[str]) -> int:
        file_path = self.config.resolve_repo_path(path)
        try:
            lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return 1
        for idx, line in enumerate(lines, start=1):
            low = line.lower()
            if any(term in low for term in terms):
                return idx
        return 1

    def _candidate_from_snippet(
        self,
        snippet: dict[str, Any],
        score: float,
        reason_codes: list[str],
        source: str,
        symbol: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        content = str(snippet.get("content", ""))
        item = {
            "kind": "snippet",
            "path": snippet["path"],
            "start_line": snippet["start_line"],
            "end_line": snippet["end_line"],
            "score": round(score, 4),
            "reason_codes": reason_codes,
            "source": source,
            "content": content,
            "raw_chars": len(content),
            "redactions": snippet.get("redactions", []),
            "prompt_injection_signals": snippet.get("prompt_injection_signals", prompt_injection_signals(content)),
            "provenance": {
                "tool": "context_lookup",
                "mode": "snippet",
                "repo_relative": True,
                "symbol": symbol or {},
            },
        }
        return item

    def _select_candidates(
        self, candidates: list[dict[str, Any]], max_items: int, content_budget: int
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates.sort(key=lambda item: (-float(item.get("score", 0.0)), item.get("path", ""), item.get("start_line", 0)))
        selected: list[dict[str, Any]] = []
        omitted: list[dict[str, Any]] = []
        seen_paths: set[str] = set()
        used_chars = 0
        for item in candidates:
            key = f"{item.get('path')}:{item.get('start_line')}:{item.get('end_line')}"
            if key in seen_paths:
                omitted.append({"path": item.get("path"), "reason_code": "duplicate", "score": item.get("score")})
                continue
            seen_paths.add(key)
            content_chars = len(str(item.get("content", "")))
            if len(selected) >= max_items:
                omitted.append({"path": item.get("path"), "reason_code": "item_limit", "score": item.get("score")})
                continue
            if used_chars + content_chars > content_budget and selected:
                omitted.append({"path": item.get("path"), "reason_code": "budget_exhausted", "score": item.get("score")})
                continue
            used_chars += content_chars
            selected.append(item)
        return selected, omitted

    def _memory_context(self, route: str, session: str) -> dict[str, Any]:
        namespaces = ["workspace", f"route/{route}", f"session/{session or 'default'}"]
        summaries = []
        decisions = []
        for namespace in namespaces:
            payload = self.memory.get(namespace=namespace, max_entries=4)
            summaries.extend(payload["summaries"][:2])
            decisions.extend(payload["effective_decisions"][:2])
        return {
            "schema": "context_pack.memory.v1",
            "namespaces": namespaces,
            "summary_count": len(summaries),
            "summaries": summaries[:6],
            "decision_count": len(decisions),
            "effective_decisions": decisions[:6],
        }

    def _aggregate_signals(self, selected: list[dict[str, Any]]) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for item in selected:
            signals = item.get("prompt_injection_signals", {})
            for category in signals.get("categories", []):
                counts[category] = counts.get(category, 0) + 1
        return {"schema": "prompt_injection_signals.aggregate.v1", "detected": bool(counts), "counts": counts}

    def _budget(
        self,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
    ) -> dict[str, Any]:
        payload = load_json_file(
            self.config.budget_path,
            {
                "schema": "context_budget.v1",
                "max_output_chars": self.config.max_output_chars,
                "default_output_profile": self.config.default_output_profile,
            },
        )
        if max_output_chars is not None:
            payload["max_output_chars"] = max(256, int(max_output_chars))
        if default_output_profile is not None:
            if default_output_profile not in {"compact", "normal", "verbose"}:
                raise ValueError("default_output_profile must be compact, normal, or verbose")
            payload["default_output_profile"] = default_output_profile
        payload["schema"] = "context_budget.v1"
        if max_output_chars is not None or default_output_profile is not None:
            payload["updated_at"] = now_iso()
            save_json_file(self.config.budget_path, payload)
        return payload

    def _cache_key(self, tool: str, args: dict[str, Any]) -> str:
        return f"{tool}:{sha256_text(json.dumps(args, sort_keys=True, default=str))[:24]}"

    def _cache_load(self) -> dict[str, Any]:
        return load_json_file(self.config.cache_path, {"schema": "context_cache.v1", "entries": {}})

    def _cache_save(self, payload: dict[str, Any]) -> None:
        save_json_file(self.config.cache_path, payload)

    def _cache_get(self, key: str) -> dict[str, Any] | None:
        payload = self._cache_load()
        row = payload.get("entries", {}).get(key)
        if isinstance(row, dict):
            return row.get("value")
        return None

    def _cache_set(self, key: str, value: dict[str, Any]) -> None:
        payload = self._cache_load()
        entries = payload.setdefault("entries", {})
        entries[key] = {"updated_at": now_iso(), "value": value}
        if len(entries) > 200:
            ordered = sorted(entries.items(), key=lambda item: item[1].get("updated_at", ""), reverse=True)
            payload["entries"] = dict(ordered[:200])
        self._cache_save(payload)

    def _cache_stats(self) -> dict[str, Any]:
        payload = self._cache_load()
        entries = payload.get("entries", {})
        return {"entry_count": len(entries), "keys": sorted(entries.keys())[:20]}

    def _cache_prune(self, max_age_minutes: int) -> dict[str, Any]:
        payload = self._cache_load()
        entries = payload.get("entries", {})
        kept = {}
        removed = 0
        cutoff_seconds = max_age_minutes * 60
        now = time.time()
        for key, row in entries.items():
            updated = row.get("updated_at", "")
            try:
                age = now - time.mktime(time.strptime(updated[:19], "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                age = cutoff_seconds + 1
            if age > cutoff_seconds:
                removed += 1
            else:
                kept[key] = row
        payload["entries"] = kept
        self._cache_save(payload)
        return {"removed_entries": removed, "entry_count": len(kept)}

    def _reference_list(self, limit: int = 20) -> list[dict[str, Any]]:
        refs = []
        self.config.ensure_state_dirs()
        for path in sorted(self.config.references_dir.glob("ctxref-*.json"), reverse=True)[:limit]:
            refs.append(
                {
                    "reference_id": path.stem,
                    "path": self.config.display_path(path),
                    "size_bytes": path.stat().st_size,
                }
            )
        return refs
