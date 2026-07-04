from __future__ import annotations

import json
import re
import time
from typing import Any

from .config import ContextConfig
from .index import ContextIndex
from .memory import ContextMemory
from .metrics import ContextMetrics
from .references import ResultReferences
from .schemas import output_contracts
from .store import ContextStore
from .util import (
    classify_route,
    estimate_tokens,
    normalize_query_terms,
    now_iso,
    prompt_injection_signals,
    sanitize_json,
    sha256_text,
)


class ContextService:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)
        self.index = ContextIndex(config)
        self.memory = ContextMemory(config)
        self.metrics = ContextMetrics(config)
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
        started = time.perf_counter()
        allowed = {"search", "snippet", "tree", "symbols", "references"}
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "search":
            self._ensure_index_fresh(path=path)
            cache_key = self._cache_key(
                "context_lookup.search",
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
                self._record_metric(
                    "context_lookup.search",
                    started,
                    cache_hit=True,
                    cache_namespace="context_lookup.search",
                    cache_reason="hit",
                    result_count=int(cached.get("count", 0)),
                )
                return {
                    **cached,
                    "cache": {
                        "hit": True,
                        "key": cache_key,
                        "namespace": "context_lookup.search",
                        "reason": "hit",
                    },
                }
            result = self.index.search(
                query=query,
                path=path,
                max_results=max_results,
                include_globs=include_globs,
            )
            self._cache_set(
                cache_key,
                result,
                namespace="context_lookup.search",
                metadata={
                    "query": query,
                    "path": path,
                    "index_generated_at": self.index.status().get("generated_at", ""),
                },
            )
            self._record_metric(
                "context_lookup.search",
                started,
                cache_hit=False,
                cache_namespace="context_lookup.search",
                cache_reason="miss",
                result_count=int(result.get("count", 0)),
            )
            return {
                **result,
                "cache": {
                    "hit": False,
                    "key": cache_key,
                    "namespace": "context_lookup.search",
                    "reason": "miss",
                },
            }
        if mode == "snippet":
            self._ensure_index_fresh(path=path)
            result = self.index.snippet(path=path, start_line=start_line, end_line=end_line)
            self._record_metric("context_lookup.snippet", started, result_count=1)
            return result
        if mode == "tree":
            self._ensure_index_fresh(path=path)
            result = self.index.tree(path=path, max_entries=max_entries, max_depth=max_depth)
            self._record_metric(
                "context_lookup.tree",
                started,
                result_count=int(result.get("count", 0)),
            )
            return result
        if mode == "symbols":
            self._ensure_index_fresh()
            result = self.index.symbols(query=query, limit=max_results)
            self._record_metric(
                "context_lookup.symbols",
                started,
                result_count=int(result.get("count", 0)),
            )
            return result
        references = self._reference_list(limit=max_results)
        self._record_metric(
            "context_lookup.references", started, result_count=len(references)
        )
        return {
            "schema": "context_references.list.v1",
            "references": references,
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
        contract_profile: str = "",
    ) -> dict[str, Any]:
        allowed = {
            "health",
            "index_refresh",
            "index_status",
            "cache_stats",
            "cache_prune",
            "budget",
            "contracts",
            "metrics",
            "measurement_matrix",
            "benchmark",
        }
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "health":
            return {
                "schema": "context_admin.health.v1",
                "ok": True,
                "repo_path": self.config.display_path(self.config.repo_path),
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
            profile = contract_profile or "verbose"
            return output_contracts(tool_name=tool_name, profile=profile)
        if mode == "metrics":
            return self.metrics.snapshot()
        if mode == "measurement_matrix":
            return self.metrics.measurement_matrix()
        if mode == "benchmark":
            return self._context_pack_benchmark(max_files=max_files)
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
        index_max_files: int = 5000,
    ) -> dict[str, Any]:
        if not prompt.strip():
            raise ValueError("prompt is required")
        started = time.perf_counter()
        stage_timings: dict[str, float] = {}
        self.config.ensure_state_dirs()
        stage_started = time.perf_counter()
        index_refresh = self._ensure_index_fresh(
            max_files=index_max_files, force=refresh_index
        )
        stage_timings["index_refresh_ms"] = self._elapsed_ms(stage_started)
        profile = output_profile or self._budget()["default_output_profile"]
        budget = max_output_chars or int(self._budget()["max_output_chars"])
        route = classify_route(prompt)
        terms = normalize_query_terms(prompt, max_terms=12)
        explicit_paths = self._collect_paths(prompt, changed_files or [], focus_paths or [])
        stage_started = time.perf_counter()
        explicit_refresh = self._refresh_explicit_paths(
            explicit_paths,
            whole_repo_refresh=index_refresh,
            force=refresh_index,
        )
        stage_timings["explicit_path_refresh_ms"] = self._elapsed_ms(stage_started)
        stage_started = time.perf_counter()
        memory_context = self._memory_context(route=route, session=memory_session)
        stage_timings["memory_lookup_ms"] = self._elapsed_ms(stage_started)
        prompt_sha256 = sha256_text(prompt)
        cache_key = self._context_pack_retrieval_cache_key(
            prompt_sha256=prompt_sha256,
            terms=terms,
            changed_files=changed_files or [],
            focus_paths=focus_paths or [],
            explicit_paths=explicit_paths,
            profile=profile,
            max_items=max_items,
        )
        stage_started = time.perf_counter()
        cached = None if refresh_index else self._cache_get(cache_key)
        stage_timings["cache_lookup_ms"] = self._elapsed_ms(stage_started)
        cache_hit = cached is not None
        cache_reason = "disabled_refresh_index" if refresh_index else "miss"
        if cached:
            candidates = list(cached.get("candidates", []))
            retrieval_omitted = list(cached.get("omitted", []))
            retrieval_stats = dict(cached.get("retrieval_stats", {}))
            stage_timings["candidate_retrieval_ms"] = 0.0
            stage_timings["snippet_batch_ms"] = 0.0
            cache_reason = "hit"
        else:
            cache_reason = (
                "disabled_refresh_index"
                if refresh_index
                else self._cache_miss_reason(
                    namespace="context_pack.retrieval",
                    metadata={
                        "prompt_sha256": prompt_sha256,
                        "refresh_signature": self.index.status().get(
                            "refresh_signature", ""
                        ),
                    },
                )
            )
            stage_started = time.perf_counter()
            candidates, retrieval_omitted, retrieval_stats = self._context_pack_candidates(
                terms=terms,
                explicit_paths=explicit_paths,
                profile=profile,
                max_items=max(max_items, 8),
            )
            stage_timings["candidate_retrieval_ms"] = self._elapsed_ms(stage_started)
            stage_timings["snippet_batch_ms"] = round(
                float(retrieval_stats.get("snippet_batch_ms", 0.0)), 3
            )
            self._cache_set(
                cache_key,
                {
                    "schema": "context_pack.retrieval_cache.v1",
                    "prompt_sha256": prompt_sha256,
                    "route": route,
                    "terms": terms,
                    "candidates": candidates,
                    "omitted": retrieval_omitted,
                    "retrieval_stats": retrieval_stats,
                },
                namespace="context_pack.retrieval",
                metadata={
                    "prompt_sha256": prompt_sha256,
                    "route": route,
                    "refresh_signature": self.index.status().get(
                        "refresh_signature", ""
                    ),
                    "profile": profile,
                    "retrieval_item_floor": max(max_items, 8),
                },
            )
        stage_started = time.perf_counter()
        omitted = list(retrieval_omitted)
        selected, omitted_budget = self._select_candidates(
            candidates,
            max_items=max_items,
            content_budget=max(1000, budget - 2400),
            per_path_limit=1 if profile == "compact" else 2,
        )
        stage_timings["selection_ms"] = self._elapsed_ms(stage_started)
        omitted.extend(omitted_budget)
        stage_started = time.perf_counter()
        full_reference = self.references.create(
            producer="context_pack",
            payload={
                "prompt_sha256": sha256_text(prompt),
                "route": route,
                "terms": terms,
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "candidates": candidates,
                "omitted": omitted,
                "retrieval_stats": retrieval_stats,
                "cache": {
                    "hit": cache_hit,
                    "key": cache_key,
                    "namespace": "context_pack.retrieval",
                    "reason": cache_reason,
                },
            },
            summary={"route": route, "candidate_count": len(candidates), "selected_count": len(selected)},
            ttl_hours=24,
        )
        stage_timings["reference_write_ms"] = self._elapsed_ms(stage_started)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        stage_timings["total_ms"] = elapsed_ms
        candidate_chars = sum(int(item.get("raw_chars", 0)) for item in candidates)
        selected_chars = sum(int(item.get("raw_chars", 0)) for item in selected)
        output_tokens = estimate_tokens(json.dumps(selected, ensure_ascii=False))
        baseline_tokens = max(self._baseline_input_tokens(candidates), output_tokens)
        estimated_tokens_saved = max(0, baseline_tokens - output_tokens)
        reference_bytes_deferred = int(
            full_reference.get("content", {}).get("size_bytes", 0) or 0
        )
        external_calls_saved = self._external_tool_calls_saved_estimate(
            selected=selected,
            omitted=omitted,
            memory_context=memory_context,
        )
        result = {
            "schema": "context_pack.v1",
            "generated_at": now_iso(),
            "repo": {
                "path": self.config.display_path(self.config.repo_path),
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
            "indexing": {
                "explicit_path_refresh": explicit_refresh,
            },
            "budget": {
                "max_output_chars": budget,
                "estimated_output_tokens": output_tokens,
            },
            "summary": {
                "item_count": len(selected),
                "candidate_count": len(candidates),
                "omitted_count": len(omitted),
                "route": route,
                "candidates_per_selected": round(len(candidates) / len(selected), 3)
                if selected
                else 0.0,
            },
            "items": selected,
            "memory": memory_context,
            "omitted": omitted,
            "references": [full_reference],
            "cache": {
                "hit": cache_hit,
                "key": cache_key,
                "namespace": "context_pack.retrieval",
                "reason": cache_reason,
                "index_refresh": {
                    "skipped": bool(index_refresh.get("skipped", False)),
                    "reason": index_refresh.get("reason", ""),
                },
            },
            "safety": {
                "repository_boundary_enforced": True,
                "generated_state_only": True,
                "untrusted_content_signals": self._aggregate_signals(selected),
            },
            "metrics": {
                "elapsed_ms": elapsed_ms,
                "stage_timings_ms": stage_timings,
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "candidate_raw_chars": candidate_chars,
                "selected_raw_chars": selected_chars,
                "baseline_input_tokens_est": baseline_tokens,
                "output_tokens_est": output_tokens,
                "estimated_input_tokens_saved": estimated_tokens_saved,
                "compression_ratio": round(output_tokens / baseline_tokens, 4)
                if baseline_tokens
                else 0.0,
                "external_tool_calls_saved_est": external_calls_saved,
                "references_bytes_deferred_est": reference_bytes_deferred,
                "retrieval_plan": retrieval_stats,
                "token_savings_formula": "max(0, baseline_input_tokens_est - output_tokens_est)",
            },
            "next_actions": [
                {"action": "resolve_reference", "when": "Need full omitted candidate evidence", "reference_id": full_reference["reference_id"]}
            ],
        }
        self.metrics.record_event(
            "context_pack",
            elapsed_ms=elapsed_ms,
            cache_hit=cache_hit,
            estimated_input_tokens_saved=estimated_tokens_saved,
            baseline_input_tokens_est=baseline_tokens,
            output_tokens_est=output_tokens,
            raw_candidate_chars=candidate_chars,
            selected_chars=selected_chars,
            external_tool_calls_saved=external_calls_saved,
            references_bytes_deferred_est=reference_bytes_deferred,
            result_count=len(selected),
            candidate_count=len(candidates),
            omitted_count=len(omitted),
            route=route,
            stage_timings_ms=stage_timings,
            cache_namespace="context_pack.retrieval",
            cache_reason=cache_reason,
        )
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

    def repo_metrics_resource(self) -> str:
        return json.dumps(self.metrics.snapshot(), indent=2, sort_keys=True)

    def codex_guidance_resource(self) -> str:
        return json.dumps(
            {
                "schema": "codex_context_pack_first.instructions.v1",
                "purpose": "Speed up Codex-style coding agents with a first-pass context pack.",
                "boundary": (
                    "Repository-side MCP config can strongly steer tool use, but it "
                    "cannot force the model to call a tool on every turn."
                ),
                "instruction": (
                    "For repository coding, review, debug, test, docs, security, or "
                    "general questions, call context_pack first with the user's task. "
                    "Pass changed_files and focus_paths when the user names them. "
                    "Use compact output by default, inspect returned cited snippets, "
                    "and resolve references only when raw evidence is needed. Avoid "
                    "broad rg, tree, or whole-file reads until the pack is insufficient."
                ),
                "preferred_tool_order": [
                    "context_pack",
                    "context_lookup",
                    "result_reference_resolve",
                ],
                "resource_uris": [
                    "repo://instructions/codex-context-pack-first",
                    "repo://project/{project_id}/instructions/codex-context-pack-first",
                ],
            },
            indent=2,
            sort_keys=True,
        )

    def _ensure_index_fresh(
        self, path: str = ".", max_files: int = 5000, force: bool = False
    ) -> dict[str, Any]:
        return self.index.refresh_if_needed(path=path, max_files=max_files, force=force)

    def _record_metric(
        self,
        operation: str,
        started: float,
        cache_hit: bool | None = None,
        cache_namespace: str = "",
        cache_reason: str = "",
        result_count: int = 0,
    ) -> None:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        self.metrics.record_event(
            operation,
            elapsed_ms=elapsed_ms,
            cache_hit=cache_hit,
            cache_namespace=cache_namespace,
            cache_reason=cache_reason,
            result_count=result_count,
        )

    def _elapsed_ms(self, started: float) -> float:
        return round((time.perf_counter() - started) * 1000, 3)

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

    def _refresh_explicit_paths(
        self,
        explicit_paths: list[str],
        whole_repo_refresh: dict[str, Any],
        force: bool,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": "context_pack.explicit_path_refresh.v1",
            "path_count": len(explicit_paths),
            "refreshed_count": 0,
            "skipped_count": 0,
            "skipped_paths": [],
            "refreshed_paths": [],
            "omitted": [],
        }
        whole_repo_current = (
            not force
            and bool(whole_repo_refresh.get("skipped", False))
            and whole_repo_refresh.get("reason") == "signature_unchanged"
        )
        for rel in explicit_paths:
            try:
                if whole_repo_current and self.index.indexed_path_current(rel):
                    result["skipped_count"] = int(result["skipped_count"]) + 1
                    result["skipped_paths"].append(
                        {
                            "path": rel,
                            "reason": "signature_unchanged_path_current",
                        }
                    )
                    continue
                refresh = self._ensure_index_fresh(path=rel, max_files=1, force=force)
                result["refreshed_count"] = int(result["refreshed_count"]) + 1
                result["refreshed_paths"].append(
                    {
                        "path": rel,
                        "reason": refresh.get("reason", "scoped_refresh"),
                    }
                )
            except Exception as exc:
                result["omitted"].append(
                    {
                        "path": rel,
                        "reason_code": "explicit_path_refresh_failed",
                        "detail": type(exc).__name__,
                    }
                )
        return result

    def _first_matching_line(self, path: str, terms: list[str]) -> int:
        return self.index.first_matching_line(path, terms)

    def _context_pack_retrieval_cache_key(
        self,
        prompt_sha256: str,
        terms: list[str],
        changed_files: list[str],
        focus_paths: list[str],
        explicit_paths: list[str],
        profile: str,
        max_items: int,
    ) -> str:
        status = self.index.status()
        return self._cache_key(
            "context_pack.retrieval",
            {
                "prompt_sha256": prompt_sha256,
                "terms": terms,
                "changed_files": changed_files,
                "focus_paths": focus_paths,
                "explicit_paths": explicit_paths,
                "output_profile": profile,
                "retrieval_item_floor": max(max_items, 8),
                "index_generated_at": status.get("generated_at", ""),
                "refresh_signature": status.get("refresh_signature", ""),
                "project_id": self.config.project_id,
            },
        )

    def _context_pack_candidates(
        self,
        terms: list[str],
        explicit_paths: list[str],
        profile: str,
        max_items: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        omitted: list[dict[str, Any]] = []
        snippet_requests: list[dict[str, Any]] = []
        retrieval_stats: dict[str, Any] = {
            "schema": "context_pack.retrieval_plan.v1",
            "profile": profile,
            "explicit_path_count": len(explicit_paths),
            "explicit_snippet_count": 0,
            "search_limit": 0,
            "search_result_count": 0,
            "search_snippet_count": 0,
            "symbol_limit": 0,
            "symbol_result_count": 0,
            "symbol_snippet_count": 0,
            "symbol_lookup_skipped": False,
            "snippet_request_count": 0,
            "snippet_batch_ms": 0.0,
            "source_counts": {},
        }

        def count_source(source: str) -> None:
            counts = retrieval_stats.setdefault("source_counts", {})
            counts[source] = int(counts.get(source, 0)) + 1

        for rel in explicit_paths:
            snippet_requests.append(
                {
                    "kind": "explicit",
                    "path": rel,
                    "start_line": 1,
                    "end_line": 80 if profile != "compact" else 40,
                    "max_chars": 1800 if profile != "compact" else 900,
                    "score": 12.0,
                    "reason_codes": ["explicit_path"],
                    "source": "explicit_path",
                    "error_reason": "unreadable_explicit_path",
                    "indexed_only": False,
                }
            )

        if terms:
            try:
                search_limit = max(max_items * 2, 8) if profile == "compact" else max(max_items * 3, 12)
                retrieval_stats["search_limit"] = search_limit
                search = self.index.search(
                    query=" ".join(terms), max_results=search_limit
                )
                retrieval_stats["search_result_count"] = len(search["results"])
                for row in search["results"]:
                    path = row["path"]
                    line = int(row.get("line") or self._first_matching_line(path, terms) or 1)
                    snippet_requests.append(
                        {
                            "kind": "search",
                            "path": path,
                            "start_line": line,
                            "end_line": line,
                            "context_before": 3,
                            "context_after": 8,
                            "max_chars": 1000 if profile == "compact" else 1800,
                            "score": float(row.get("score", 1.0)) + 4.0,
                            "reason_codes": ["lexical_match"],
                            "source": str(row.get("source", "search")),
                            "error_reason": "search_snippet_unavailable",
                        }
                    )
            except Exception as exc:
                omitted.append({"reason_code": "search_failed", "detail": type(exc).__name__})

        try:
            symbol_limit = max(max_items, 4) if profile == "compact" else max_items * 2
            retrieval_stats["symbol_limit"] = symbol_limit
            queued_retrieval = [
                request
                for request in snippet_requests
                if request.get("kind") in {"explicit", "search"}
            ]
            queued_path_count = len(
                {str(request.get("path", "")) for request in queued_retrieval}
            )
            if (
                profile == "compact"
                and len(queued_retrieval) >= max_items * 2
                and queued_path_count >= max_items
            ):
                retrieval_stats["symbol_lookup_skipped"] = True
                omitted.append(
                    {
                        "reason_code": "early_stop_enough_ranked_candidates",
                        "detail": "symbol lookup skipped after explicit/search retrieval",
                        "candidate_count": len(queued_retrieval),
                    }
                )
            else:
                symbols = self.index.symbols(query=" ".join(terms), limit=symbol_limit)
                retrieval_stats["symbol_result_count"] = len(symbols["symbols"])
                for row in symbols["symbols"]:
                    snippet_requests.append(
                        {
                            "kind": "symbol",
                            "path": row["path"],
                            "start_line": max(1, int(row["line_start"])),
                            "end_line": max(1, int(row["line_end"])),
                            "context_before": 2,
                            "context_after": 6,
                            "max_chars": 1000,
                            "score": 8.0,
                            "reason_codes": [
                                "symbol_match",
                                str(row.get("kind", "symbol")),
                            ],
                            "source": "symbol_index",
                            "symbol": row,
                            "error_reason": "symbol_snippet_unavailable",
                        }
                    )
        except Exception:
            pass

        retrieval_stats["snippet_request_count"] = len(snippet_requests)
        stage_started = time.perf_counter()
        snippets = self.index.snippet_batch(snippet_requests, indexed_only=True)
        retrieval_stats["snippet_batch_ms"] = self._elapsed_ms(stage_started)
        for request, snippet in zip(snippet_requests, snippets):
            if snippet.get("schema") == "context_snippet.error.v1":
                omitted.append(
                    {
                        "path": request.get("path", ""),
                        "reason_code": request.get(
                            "error_reason", "snippet_unavailable"
                        ),
                        "detail": snippet.get("error", "Error"),
                    }
                )
                continue
            candidates.append(
                self._candidate_from_snippet(
                    snippet,
                    score=float(request.get("score", 1.0)),
                    reason_codes=list(request.get("reason_codes", [])),
                    source=str(request.get("source", "snippet_batch")),
                    symbol=request.get("symbol"),
                )
            )
            kind = str(request.get("kind", ""))
            if kind == "explicit":
                retrieval_stats["explicit_snippet_count"] = int(
                    retrieval_stats.get("explicit_snippet_count", 0)
                ) + 1
            elif kind == "search":
                retrieval_stats["search_snippet_count"] = int(
                    retrieval_stats.get("search_snippet_count", 0)
                ) + 1
            elif kind == "symbol":
                retrieval_stats["symbol_snippet_count"] = int(
                    retrieval_stats.get("symbol_snippet_count", 0)
                ) + 1
            count_source(str(request.get("source", "snippet_batch")))

        return candidates, omitted, retrieval_stats

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
            "confidence": round(min(0.99, max(0.1, score / 14.0)), 3),
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
        self,
        candidates: list[dict[str, Any]],
        max_items: int,
        content_budget: int,
        per_path_limit: int = 2,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates.sort(key=lambda item: (-float(item.get("score", 0.0)), item.get("path", ""), item.get("start_line", 0)))
        selected: list[dict[str, Any]] = []
        omitted: list[dict[str, Any]] = []
        seen_ranges: set[str] = set()
        path_counts: dict[str, int] = {}
        used_chars = 0
        for item in candidates:
            path = str(item.get("path", ""))
            key = f"{item.get('path')}:{item.get('start_line')}:{item.get('end_line')}"
            if key in seen_ranges:
                omitted.append({"path": item.get("path"), "reason_code": "duplicate", "score": item.get("score")})
                continue
            seen_ranges.add(key)
            if path_counts.get(path, 0) >= per_path_limit:
                omitted.append({"path": item.get("path"), "reason_code": "diversity_limit", "score": item.get("score")})
                continue
            content_chars = len(str(item.get("content", "")))
            if len(selected) >= max_items:
                omitted.append({"path": item.get("path"), "reason_code": "item_limit", "score": item.get("score")})
                continue
            if used_chars + content_chars > content_budget and selected:
                omitted.append({"path": item.get("path"), "reason_code": "budget_exhausted", "score": item.get("score")})
                continue
            used_chars += content_chars
            path_counts[path] = path_counts.get(path, 0) + 1
            selected.append(item)
        return selected, omitted

    def _baseline_input_tokens(self, candidates: list[dict[str, Any]]) -> int:
        evidence = [
            {
                "path": item.get("path", ""),
                "start_line": item.get("start_line", 0),
                "end_line": item.get("end_line", 0),
                "content": item.get("content", ""),
            }
            for item in candidates
        ]
        return estimate_tokens(evidence)

    def _external_tool_calls_saved_estimate(
        self,
        selected: list[dict[str, Any]],
        omitted: list[dict[str, Any]],
        memory_context: dict[str, Any],
    ) -> int:
        baseline_calls = 1 + len(selected)
        if int(memory_context.get("summary_count", 0)) or int(
            memory_context.get("decision_count", 0)
        ):
            baseline_calls += 1
        if omitted:
            baseline_calls += 1
        return max(0, baseline_calls - 1)

    def _context_pack_benchmark(self, max_files: int = 5000) -> dict[str, Any]:
        started = time.perf_counter()
        if not self.index.status().get("file_count"):
            self.index.refresh(max_files=max_files)
        focus_paths = self._benchmark_focus_paths()
        prompt = self._benchmark_prompt(focus_paths)
        runs = []
        for name, refresh_index, max_items in [
            ("cold_refresh", True, 4),
            ("warm_cache", False, 4),
            ("repeated_prompt", False, 4),
            ("compact_focus", False, 2),
        ]:
            pack = self.context_pack(
                prompt=prompt,
                focus_paths=focus_paths,
                max_items=max_items,
                refresh_index=refresh_index,
                output_profile="compact",
                index_max_files=max_files,
            )
            runs.append(
                {
                    "name": name,
                    "cache_hit": bool(pack["cache"]["hit"]),
                    "elapsed_ms": pack["metrics"]["elapsed_ms"],
                    "stage_timings_ms": pack["metrics"]["stage_timings_ms"],
                    "candidate_count": pack["metrics"]["candidate_count"],
                    "selected_count": pack["metrics"]["selected_count"],
                    "baseline_input_tokens_est": pack["metrics"][
                        "baseline_input_tokens_est"
                    ],
                    "output_tokens_est": pack["metrics"]["output_tokens_est"],
                    "estimated_input_tokens_saved": pack["metrics"][
                        "estimated_input_tokens_saved"
                    ],
                    "external_tool_calls_saved_est": pack["metrics"][
                        "external_tool_calls_saved_est"
                    ],
                    "references_bytes_deferred_est": pack["metrics"][
                        "references_bytes_deferred_est"
                    ],
                }
            )
        compact_contract = self.context_admin(
            mode="contracts", contract_profile="compact"
        )
        return {
            "schema": "context_benchmark.v1",
            "generated_at": now_iso(),
            "project_id": self.config.project_id,
            "prompt": prompt,
            "focus_paths": focus_paths,
            "run_count": len(runs),
            "runs": runs,
            "compact_contract_sample": {
                "schema": compact_contract["schema"],
                "contract_tokens_est": compact_contract["metrics"][
                    "compact_contract_tokens_est"
                ],
                "contract_tokens_saved_est": compact_contract["metrics"][
                    "compact_contract_tokens_saved_est"
                ],
                "tool_count": len(compact_contract.get("contracts", {})),
            },
            "elapsed_ms": self._elapsed_ms(started),
            "measurement_matrix": self.metrics.measurement_matrix(),
        }

    def _benchmark_focus_paths(self) -> list[str]:
        files = self.index.files(limit=80)
        preferred: list[str] = []
        fallback: list[str] = []
        for row in files:
            path = str(row.get("path", ""))
            if not path:
                continue
            fallback.append(path)
            if path.startswith(("src/", "lib/", "tests/", "test/")) or path in {
                "README.md",
                "pyproject.toml",
                "package.json",
            }:
                preferred.append(path)
            if len(preferred) >= 3:
                break
        return (preferred or fallback)[:3]

    def _benchmark_prompt(self, focus_paths: list[str]) -> str:
        if not focus_paths:
            return "review repository context retrieval behavior"
        terms = " ".join(
            path.replace("/", " ").replace(".", " ") for path in focus_paths[:3]
        )
        return f"review {terms} behavior and related tests"

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
        payload = self.store.get_json(
            "budget:default",
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
            self.store.put_json("budget:default", payload)
        return payload

    def _cache_key(self, tool: str, args: dict[str, Any]) -> str:
        return f"{tool}:{sha256_text(json.dumps(args, sort_keys=True, default=str))[:24]}"

    def _cache_get(self, key: str) -> dict[str, Any] | None:
        row = self.store.get_json(f"cache:{key}")
        if isinstance(row, dict):
            return row.get("value")
        return None

    def _cache_set(
        self,
        key: str,
        value: dict[str, Any],
        namespace: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        sanitized_value, sensitivity = sanitize_json(value)
        self.store.put_json(
            f"cache:{key}",
            {
                "updated_at": now_iso(),
                "namespace": namespace,
                "key": key,
                "metadata": metadata or {},
                "value": sanitized_value,
                "sensitivity": sensitivity,
            },
        )
        entries = self.store.iter_json("cache:")
        if len(entries) > 200:
            ordered = sorted(
                entries,
                key=lambda item: item[1].get("updated_at", "")
                if isinstance(item[1], dict)
                else "",
                reverse=True,
            )
            keep = {key for key, _row in ordered[:200]}
            with self.store.write_txn() as txn:
                for cache_key, _row in ordered[200:]:
                    if cache_key not in keep:
                        self.store.delete(cache_key, txn=txn)

    def _cache_stats(self) -> dict[str, Any]:
        entries = self.store.iter_json("cache:")
        keys = [key.removeprefix("cache:") for key, _row in entries]
        namespaces: dict[str, dict[str, Any]] = {}
        metric_namespaces = self.metrics.snapshot().get("cache", {}).get(
            "by_namespace", {}
        )
        for cache_key, row in entries:
            if not isinstance(row, dict):
                continue
            key = cache_key.removeprefix("cache:")
            namespace = str(row["namespace"])
            stats = namespaces.setdefault(
                namespace,
                {
                    "entry_count": 0,
                    "sample_keys": [],
                    "hits": 0,
                    "misses": 0,
                    "hit_ratio": 0.0,
                },
            )
            stats["entry_count"] = int(stats["entry_count"]) + 1
            if len(stats["sample_keys"]) < 5:
                stats["sample_keys"].append(key)
        for namespace, metric_stats in metric_namespaces.items():
            stats = namespaces.setdefault(
                namespace,
                {
                    "entry_count": 0,
                    "sample_keys": [],
                    "hits": 0,
                    "misses": 0,
                    "hit_ratio": 0.0,
                },
            )
            stats["hits"] = int(metric_stats.get("hits", 0) or 0)
            stats["misses"] = int(metric_stats.get("misses", 0) or 0)
            stats["hit_ratio"] = float(metric_stats.get("hit_ratio", 0.0) or 0.0)
        return {
            "entry_count": len(entries),
            "keys": sorted(keys)[:20],
            "namespaces": dict(sorted(namespaces.items())),
            "storage_backend": self.store.backend,
        }

    def _cache_prune(self, max_age_minutes: int) -> dict[str, Any]:
        removed = 0
        cutoff_seconds = max_age_minutes * 60
        now = time.time()
        with self.store.write_txn() as txn:
            entries = self.store.iter_json("cache:", txn=txn)
            for key, row in entries:
                if not isinstance(row, dict):
                    self.store.delete(key, txn=txn)
                    removed += 1
                    continue
                updated = row.get("updated_at", "")
                try:
                    age = now - time.mktime(
                        time.strptime(str(updated)[:19], "%Y-%m-%dT%H:%M:%S")
                    )
                except Exception:
                    age = cutoff_seconds + 1
                if age > cutoff_seconds:
                    removed += 1
                    self.store.delete(key, txn=txn)
        return {"removed_entries": removed, "entry_count": self.store.count("cache:")}

    def _reference_list(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.references.list(limit=limit)

    def _cache_miss_reason(
        self, namespace: str, metadata: dict[str, Any] | None = None
    ) -> str:
        metadata = metadata or {}
        rows = []
        for _cache_key, row in self.store.iter_json("cache:"):
            if isinstance(row, dict) and row.get("namespace") == namespace:
                rows.append(row)
        if not rows:
            return "miss"
        prompt_sha256 = str(metadata.get("prompt_sha256", ""))
        if prompt_sha256:
            prompt_rows = [
                row
                for row in rows
                if str(row.get("metadata", {}).get("prompt_sha256", ""))
                == prompt_sha256
            ]
            if not prompt_rows:
                return "arg_changed"
            expected_signature = str(metadata.get("refresh_signature", ""))
            if expected_signature and any(
                str(row.get("metadata", {}).get("refresh_signature", ""))
                != expected_signature
                for row in prompt_rows
            ):
                return "stale_index"
            return "arg_changed"
        return "arg_changed"
