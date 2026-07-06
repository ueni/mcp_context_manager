from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

from .config import ContextConfig
from .index import ContextIndex
from .memory import ContextMemory
from .metrics import ContextMetrics
from .references import ResultReferences
from .schemas import output_contracts
from .store import ContextStore
from .token_counter import TokenCount, TokenCounter
from .util import (
    classify_route,
    normalize_query_terms,
    now_iso,
    parse_iso,
    prompt_injection_signals,
    sanitize_json,
    sha256_text,
)

DEFAULT_CACHE_TTL_SECONDS = 14 * 24 * 60 * 60
DEFAULT_CACHE_MAX_AGE_MINUTES = DEFAULT_CACHE_TTL_SECONDS // 60
DEFAULT_INDEX_MAX_FILES = 5000
DEFAULT_WARMUP_MAX_FILES = 100
CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR = 8


class ContextService:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)
        self.index = ContextIndex(config)
        self.memory = ContextMemory(config)
        self.metrics = ContextMetrics(config)
        self.references = ResultReferences(config)
        self.token_counter = TokenCounter(
            mode=config.token_counter_mode,
            target_tokenizer=config.target_tokenizer,
        )

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
            index_status = self.index.status()
            cache_key = self._cache_key(
                "context_lookup.search",
                self._context_lookup_search_cache_args(
                    query=query,
                    path=path,
                    max_results=max_results,
                    include_globs=include_globs,
                    index_status=index_status,
                ),
            )
            cache_lookup = self._cache_lookup(cache_key)
            cached = cache_lookup.get("value") if cache_lookup["hit"] else None
            if isinstance(cached, dict):
                cached = self._context_lookup_search_cache_value(
                    cached,
                    query=query,
                )
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
                    "cache": self._cache_public_metadata(
                        cache_key,
                        "context_lookup.search",
                        cache_lookup,
                        reason="hit",
                    ),
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
                    "index_generated_at": index_status.get("generated_at", ""),
                    "refresh_signature": index_status.get("refresh_signature", ""),
                },
            )
            self._record_metric(
                "context_lookup.search",
                started,
                cache_hit=False,
                cache_namespace="context_lookup.search",
                cache_reason=str(cache_lookup.get("reason") or "miss"),
                result_count=int(result.get("count", 0)),
            )
            return {
                **result,
                "cache": self._cache_public_metadata(
                    cache_key,
                    "context_lookup.search",
                    cache_lookup,
                    reason=str(cache_lookup.get("reason") or "miss"),
                ),
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
        max_files: int | None = None,
        max_age_minutes: int = DEFAULT_CACHE_MAX_AGE_MINUTES,
        max_entries: int = 100,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
        tool_name: str = "",
        contract_profile: str = "",
        state_prefix: str = "",
        state_key: str = "",
    ) -> dict[str, Any]:
        allowed = {
            "health",
            "index_refresh",
            "index_status",
            "cache_stats",
            "cache_prune",
            "warmup",
            "budget",
            "contracts",
            "metrics",
            "measurement_matrix",
            "benchmark",
            "state_browser",
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
            return self.index.refresh(
                path=path,
                max_files=max_files
                if max_files is not None
                else DEFAULT_INDEX_MAX_FILES,
            )
        if mode == "index_status":
            return self.index.status()
        if mode == "cache_stats":
            return {"schema": "context_cache.stats.v1", **self._cache_stats()}
        if mode == "cache_prune":
            return {"schema": "context_cache.prune.v1", **self._cache_prune(max_age_minutes)}
        if mode == "warmup":
            return self._cache_warmup(
                path=path,
                max_files=max_files,
                max_entries=max_entries,
            )
        if mode == "contracts":
            profile = contract_profile or "verbose"
            return output_contracts(tool_name=tool_name, profile=profile)
        if mode == "metrics":
            return self.metrics.snapshot()
        if mode == "measurement_matrix":
            return self.metrics.measurement_matrix()
        if mode == "benchmark":
            return self._context_pack_benchmark(
                max_files=max_files
                if max_files is not None
                else DEFAULT_INDEX_MAX_FILES
            )
        if mode == "state_browser":
            return self._state_browser(
                prefix=state_prefix,
                key=state_key,
                max_entries=max_entries,
                max_output_chars=max_output_chars,
            )
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
        terms_key = " ".join(sorted(set(terms)))
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
        token_counting = self.token_counter.metadata()
        retrieval_max_items = max(
            CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR,
            max(1, max_items),
        )
        cache_key = self._context_pack_retrieval_cache_key(
            route=route,
            terms=terms,
            explicit_paths=explicit_paths,
            profile=profile,
            retrieval_max_items=retrieval_max_items,
            token_counting=token_counting,
        )
        stage_started = time.perf_counter()
        if refresh_index:
            cache_lookup = {
                "hit": False,
                "status": "disabled",
                "reason": "disabled_refresh_index",
                "warnings": [],
            }
        else:
            cache_lookup = self._cache_lookup(cache_key)
        cached = cache_lookup.get("value") if cache_lookup["hit"] else None
        stage_timings["cache_lookup_ms"] = self._elapsed_ms(stage_started)
        cache_hit = isinstance(cached, dict)
        cache_reason = str(cache_lookup.get("reason") or "miss")
        if isinstance(cached, dict):
            candidates = list(cached.get("candidates", []))
            retrieval_omitted = list(cached.get("omitted", []))
            retrieval_stats = dict(cached.get("retrieval_stats", {}))
            stage_timings["candidate_retrieval_ms"] = 0.0
            stage_timings["search_ranking_ms"] = 0.0
            stage_timings["snippet_batch_ms"] = 0.0
            cache_reason = "hit"
        else:
            cache_reason = (
                "disabled_refresh_index"
                if refresh_index
                else cache_reason
                if cache_reason in {"expired", "invalidated", "invalid_payload"}
                else self._cache_miss_reason(
                    namespace="context_pack.retrieval",
                    metadata={
                        "prompt_sha256": prompt_sha256,
                        "terms_key": terms_key,
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
                max_items=retrieval_max_items,
            )
            stage_timings["candidate_retrieval_ms"] = self._elapsed_ms(stage_started)
            stage_timings["search_ranking_ms"] = stage_timings[
                "candidate_retrieval_ms"
            ]
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
                    "terms_key": terms_key,
                    "profile": profile,
                    "retrieval_item_floor": CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR,
                    "retrieval_max_items": retrieval_max_items,
                    "token_counting": self.token_counter.cache_key_metadata(),
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
                    "status": str(cache_lookup.get("status", "missing")),
                    "warnings": list(cache_lookup.get("warnings") or []),
                },
            },
            summary={"route": route, "candidate_count": len(candidates), "selected_count": len(selected)},
            ttl_hours=24,
        )
        stage_timings["reference_write_ms"] = self._elapsed_ms(stage_started)
        candidate_chars = sum(int(item.get("raw_chars", 0)) for item in candidates)
        selected_chars = sum(int(item.get("raw_chars", 0)) for item in selected)
        output_token_count = self._token_count(
            json.dumps(selected, ensure_ascii=False)
        )
        output_tokens = output_token_count.count
        baseline_count = self._baseline_input_token_count(candidates)
        baseline_tokens = max(baseline_count.count, output_tokens)
        token_counting = self._merge_token_counting_metadata(
            token_counting,
            output_token_count.metadata(),
            baseline_count.metadata(),
        )
        estimated_tokens_saved = max(0, baseline_tokens - output_tokens)
        tokens_spared_reason = (
            "MCP context_pack returned compact selected summaries and deferred "
            "full evidence behind local references instead of sending all ranked "
            "candidate evidence."
        )
        reference_bytes_deferred = int(
            full_reference.get("content", {}).get("size_bytes", 0) or 0
        )
        external_calls_saved = self._external_tool_calls_saved_estimate(
            selected=selected,
            omitted=omitted,
            memory_context=memory_context,
        )
        stage_started = time.perf_counter()
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
                "token_counting": token_counting,
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
                "status": str(cache_lookup.get("status", "missing")),
                "expires_at": str(cache_lookup.get("expires_at", "")),
                "warnings": list(cache_lookup.get("warnings") or []),
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
                "elapsed_ms": 0.0,
                "stage_timings_ms": {},
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "candidate_raw_chars": candidate_chars,
                "selected_raw_chars": selected_chars,
                "token_counting": token_counting,
                "baseline_input_tokens_est": baseline_tokens,
                "output_tokens_est": output_tokens,
                "estimated_input_tokens_saved": estimated_tokens_saved,
                "tokens_spared_by_mcp_est": estimated_tokens_saved,
                "tokens_spared_by_mcp_reason": tokens_spared_reason,
                "compression_ratio": round(output_tokens / baseline_tokens, 4)
                if baseline_tokens
                else 0.0,
                "external_tool_calls_saved_est": external_calls_saved,
                "references_bytes_deferred_est": reference_bytes_deferred,
                "retrieval_plan": retrieval_stats,
                "token_savings_formula": "max(0, baseline_input_tokens_est - output_tokens_est)",
                "tokens_spared_by_mcp_formula": "max(0, baseline_input_tokens_est - output_tokens_est)",
            },
            "next_actions": [
                {"action": "resolve_reference", "when": "Need full omitted candidate evidence", "reference_id": full_reference["reference_id"]}
            ],
        }
        stage_timings["response_assembly_ms"] = self._elapsed_ms(stage_started)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        stage_timings["total_ms"] = elapsed_ms
        result["metrics"]["elapsed_ms"] = elapsed_ms
        result["metrics"]["stage_timings_ms"] = stage_timings
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
                    "Repository-side MCP config can require this server to "
                    "initialize, but it cannot force the model to call a tool "
                    "on every turn."
                ),
                "codex_config_example": {
                    "config_file": "~/.codex/config.toml or trusted-project .codex/config.toml",
                    "toml": (
                        "[mcp_servers.mcp-context-manager]\n"
                        "url = \"http://localhost:8000/mcp\"\n"
                        "required = true\n"
                        "enabled_tools = [\n"
                        "  \"context_pack\",\n"
                        "  \"context_lookup\",\n"
                        "  \"context_memory\",\n"
                        "  \"context_admin\",\n"
                        "  \"result_reference_resolve\",\n"
                        "]\n"
                        "default_tools_approval_mode = \"auto\"\n"
                    ),
                    "effect": (
                        "required=true fails startup or resume if this enabled "
                        "server cannot initialize; enabled_tools keeps the "
                        "advertised surface focused on this server's public tools."
                    ),
                },
                "enforcement_layers": [
                    "Codex config required=true for server availability",
                    "MCP server instructions for tool-selection guidance",
                    "AGENTS.md mandatory workflow for repository tasks",
                    "Review or CI checks that reject work started with broad local inspection",
                ],
                "instruction": (
                    "Treat MCP-first usage as mandatory. For repository coding, "
                    "review, debug, test, docs, security, or general questions, "
                    "call context_pack first with the user's task. Pass "
                    "changed_files and focus_paths when the user names them. "
                    "Use compact output by default. Must use context_lookup for "
                    "targeted follow-up snippets, search, trees, symbols, or "
                    "references before broad shell inspection. Must use "
                    "result_reference_resolve when raw referenced evidence is "
                    "needed before destructive edits, release claims, or security "
                    "conclusions. Must use context_admin for health, index, "
                    "cache, budget, contracts, metrics, benchmark, warmup, or "
                    "generated-state checks. Must use context_memory only for "
                    "structured, non-secret repository facts, summaries, "
                    "decisions, validation, or compaction. Avoid broad rg, tree, "
                    "or whole-file reads until the MCP lookups are insufficient."
                ),
                "preferred_tool_order": [
                    "context_pack",
                    "context_lookup",
                    "result_reference_resolve",
                    "context_admin",
                    "context_memory",
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
        route: str,
        terms: list[str],
        explicit_paths: list[str],
        profile: str,
        retrieval_max_items: int,
        token_counting: dict[str, Any],
    ) -> str:
        status = self.index.status()
        return self._cache_key(
            "context_pack.retrieval",
            {
                "route": route,
                "terms": sorted(set(terms)),
                "explicit_paths": self._canonical_cache_paths(explicit_paths),
                "output_profile": profile,
                "retrieval_max_items": max(
                    CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR,
                    max(1, retrieval_max_items),
                ),
                "index_generated_at": status.get("generated_at", ""),
                "refresh_signature": status.get("refresh_signature", ""),
                "project_id": self.config.project_id,
                "token_counting": {
                    "tokenizer": token_counting.get("tokenizer", ""),
                    "tokenizer_available": token_counting.get(
                        "tokenizer_available", False
                    ),
                    "token_count_source": token_counting.get(
                        "token_count_source", ""
                    ),
                },
            },
        )

    def _context_lookup_search_cache_args(
        self,
        query: str,
        path: str,
        max_results: int,
        include_globs: list[str] | None,
        index_status: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "terms": sorted(set(normalize_query_terms(query, max_terms=8))),
            "path": self._canonical_cache_path(path),
            "max_results": max_results,
            "include_globs": self._canonical_cache_globs(include_globs),
            "index": index_status.get("generated_at", ""),
            "refresh_signature": index_status.get("refresh_signature", ""),
            "project_id": self.config.project_id,
        }

    def _context_lookup_search_cache_value(
        self,
        cached: dict[str, Any],
        query: str,
    ) -> dict[str, Any]:
        terms = normalize_query_terms(query, max_terms=8)
        results = [
            {**row, "terms": terms}
            for row in cached.get("results", [])
            if isinstance(row, dict)
        ]
        return {**cached, "query": query, "terms": terms, "results": results}

    def _canonical_cache_path(self, path: str) -> str:
        try:
            return self.config.repo_relative(path)
        except ValueError:
            normalized = str(path).strip().replace("\\", "/")
            while normalized.startswith("./"):
                normalized = normalized[2:]
            return normalized or "."

    def _canonical_cache_paths(self, paths: list[str]) -> list[str]:
        return sorted(
            {
                canonical
                for path in paths
                if (canonical := self._canonical_cache_path(path))
            }
        )

    def _canonical_cache_globs(self, include_globs: list[str] | None) -> list[str]:
        return sorted(
            {
                glob.strip().replace("\\", "/")
                for glob in include_globs or []
                if glob.strip()
            }
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
        retrieval_stats: dict[str, Any] = {
            "schema": "context_pack.retrieval_plan.v1",
            "profile": profile,
            "detail_mode": "context_lookup.snippet",
            "explicit_path_count": len(explicit_paths),
            "explicit_summary_count": 0,
            "search_limit": 0,
            "search_result_count": 0,
            "search_summary_count": 0,
            "symbol_limit": 0,
            "symbol_result_count": 0,
            "symbol_summary_count": 0,
            "symbol_lookup_skipped": False,
            "snippet_request_count": 0,
            "snippet_batch_ms": 0.0,
            "source_counts": {},
        }

        def count_source(source: str) -> None:
            counts = retrieval_stats.setdefault("source_counts", {})
            counts[source] = int(counts.get(source, 0)) + 1

        for rel in explicit_paths:
            try:
                summary = self.index.file_summary(
                    rel,
                    max_chars=900 if profile != "compact" else 360,
                )
            except Exception as exc:
                omitted.append(
                    {
                        "path": rel,
                        "reason_code": "unreadable_explicit_path",
                        "detail": type(exc).__name__,
                    }
                )
                continue
            candidates.append(
                self._candidate_from_summary(
                    summary,
                    score=12.0,
                    reason_codes=["explicit_path"],
                    source="explicit_path",
                )
            )
            retrieval_stats["explicit_summary_count"] = int(
                retrieval_stats.get("explicit_summary_count", 0)
            ) + 1
            count_source("explicit_path")

        if terms:
            try:
                search_limit = max(max_items * 4, 8)
                retrieval_stats["search_limit"] = search_limit
                search = self.index.search(
                    query=" ".join(terms), max_results=search_limit
                )
                retrieval_stats["search_result_count"] = len(search["results"])
                for row in search["results"]:
                    path = row["path"]
                    line = int(row.get("line") or self._first_matching_line(path, terms) or 1)
                    summary = self.index.file_summary(
                        path,
                        max_chars=420 if profile != "compact" else 260,
                        matched_line=line,
                    )
                    candidates.append(
                        self._candidate_from_summary(
                            summary,
                            score=float(row.get("score", 1.0)) + 4.0,
                            reason_codes=["lexical_match"],
                            source=str(row.get("source", "search")),
                        )
                    )
                    retrieval_stats["search_summary_count"] = int(
                        retrieval_stats.get("search_summary_count", 0)
                    ) + 1
                    count_source(str(row.get("source", "search")))
            except Exception as exc:
                omitted.append({"reason_code": "search_failed", "detail": type(exc).__name__})

        try:
            symbol_limit = max(max_items, 2)
            retrieval_stats["symbol_limit"] = symbol_limit
            queued_path_count = len({str(item.get("path", "")) for item in candidates})
            if (
                profile == "compact"
                and len(candidates) >= max_items * 2
                and queued_path_count >= max_items
            ):
                retrieval_stats["symbol_lookup_skipped"] = True
                omitted.append(
                    {
                        "reason_code": "early_stop_enough_ranked_candidates",
                        "detail": "symbol lookup skipped after explicit/search retrieval",
                        "candidate_count": len(candidates),
                    }
                )
            else:
                symbols = self.index.symbols(query=" ".join(terms), limit=symbol_limit)
                retrieval_stats["symbol_result_count"] = len(symbols["symbols"])
                for row in symbols["symbols"]:
                    summary = self.index.file_summary(
                        str(row["path"]),
                        max_chars=420 if profile != "compact" else 260,
                        matched_line=max(1, int(row["line_start"])),
                    )
                    candidates.append(
                        self._candidate_from_summary(
                            summary,
                            score=8.0,
                            reason_codes=[
                                "symbol_match",
                                str(row.get("kind", "symbol")),
                            ],
                            source="symbol_index",
                            symbol=row,
                        )
                    )
                    retrieval_stats["symbol_summary_count"] = int(
                        retrieval_stats.get("symbol_summary_count", 0)
                    ) + 1
                    count_source("symbol_index")
        except Exception:
            pass

        return candidates, omitted, retrieval_stats

    def _candidate_from_summary(
        self,
        summary: dict[str, Any],
        score: float,
        reason_codes: list[str],
        source: str,
        symbol: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        content = str(summary.get("content") or summary.get("excerpt") or "")
        source_chars = int(summary.get("source_chars", len(content)) or len(content))
        item = {
            "kind": "summary",
            "path": summary["path"],
            "start_line": summary["start_line"],
            "end_line": summary["end_line"],
            "score": round(score, 4),
            "confidence": round(min(0.99, max(0.1, score / 14.0)), 3),
            "reason_codes": reason_codes,
            "source": source,
            "title_hint": summary.get("title_hint", ""),
            "content": content,
            "raw_chars": len(content),
            "source_chars": source_chars,
            "deferred_chars": max(0, source_chars - len(content)),
            "detail_lookup": {
                "tool": "context_lookup",
                "mode": "snippet",
                "path": summary["path"],
                "start_line": summary["start_line"],
            },
            "redactions": summary.get("redactions", []),
            "prompt_injection_signals": summary.get("prompt_injection_signals", prompt_injection_signals(content)),
            "provenance": {
                "tool": "context_lookup",
                "mode": "summary",
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

    def _baseline_input_token_count(
        self, candidates: list[dict[str, Any]]
    ) -> TokenCount:
        evidence = [
            {
                "path": item.get("path", ""),
                "start_line": item.get("start_line", 0),
                "end_line": item.get("end_line", 0),
                "content": item.get("content", ""),
            }
            for item in candidates
        ]
        deferred_chars = sum(int(item.get("deferred_chars", 0) or 0) for item in candidates)
        token_count = self._token_count(evidence)
        return TokenCount(
            count=token_count.count + max(0, (deferred_chars + 3) // 4),
            tokenizer=token_count.tokenizer,
            tokenizer_available=token_count.tokenizer_available,
            token_count_source=token_count.token_count_source,
            warnings=token_count.warnings,
        )

    def _count_tokens(self, text_or_value: Any) -> int:
        return self._token_count(text_or_value).count

    def _token_count(self, text_or_value: Any) -> TokenCount:
        return self.token_counter.count(text_or_value)

    def _merge_token_counting_metadata(
        self, *metadata_rows: dict[str, Any]
    ) -> dict[str, Any]:
        merged = dict(metadata_rows[0]) if metadata_rows else {}
        warnings: list[dict[str, str]] = []
        for row in metadata_rows:
            if row.get("token_count_source") == "estimate":
                merged["token_count_source"] = "estimate"
            if "tokenizer_available" in row:
                merged["tokenizer_available"] = bool(row["tokenizer_available"])
            if row.get("tokenizer"):
                merged["tokenizer"] = row["tokenizer"]
            for warning in row.get("warnings") or []:
                if isinstance(warning, dict) and warning not in warnings:
                    warnings.append(warning)
        merged["warnings"] = warnings
        return merged

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
                    "token_counting": pack["metrics"]["token_counting"],
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

    def _cache_warmup(
        self,
        path: str = ".",
        max_files: int | None = None,
        max_entries: int = 100,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        self.config.ensure_state_dirs()
        store_existed_before = self.store.exists()
        self.store.get_json("budget:default")
        cache_before = self._cache_stats()
        omitted: list[dict[str, Any]] = []
        effective_max_files = (
            max_files if max_files is not None else DEFAULT_WARMUP_MAX_FILES
        )

        try:
            index_refresh = self._ensure_index_fresh(
                path=path,
                max_files=effective_max_files,
            )
        except Exception as exc:
            index_refresh = {
                "schema": "context_index.refresh.v1",
                "index_available": False,
                "skipped": False,
                "reason": "index_refresh_failed",
            }
            omitted.append(
                {
                    "reason_code": "index_refresh_failed",
                    "detail": type(exc).__name__,
                }
            )

        facts: dict[str, Any] = {}
        try:
            facts = self.index.workspace_facts()
        except Exception as exc:
            omitted.append(
                {
                    "reason_code": "workspace_facts_failed",
                    "detail": type(exc).__name__,
                }
            )

        symbol_count = 0
        try:
            symbol_limit = max(0, min(int(max_entries), 100))
            symbol_count = int(
                self.index.symbols(query="", limit=symbol_limit).get("count", 0)
            )
        except Exception as exc:
            omitted.append(
                {
                    "reason_code": "symbols_warmup_failed",
                    "detail": type(exc).__name__,
                }
            )

        search_rows = self._warm_search_caches(path=path, max_entries=max_entries)
        omitted.extend(
            row for row in search_rows if row.get("reason_code") == "search_warmup_failed"
        )
        warmed_searches = [
            row for row in search_rows if row.get("reason_code") != "search_warmup_failed"
        ]
        cache_after = self._cache_stats()
        elapsed_ms = self._elapsed_ms(started)
        self.metrics.record_event(
            "context_admin.warmup",
            elapsed_ms=elapsed_ms,
            result_count=len(warmed_searches),
        )
        return {
            "schema": "context_cache.warmup.v1",
            "generated_at": now_iso(),
            "project_id": self.config.project_id,
            "elapsed_ms": elapsed_ms,
            "state": {
                "state_dir": self.config.display_path(self.config.state_dir),
                "store_existed_before": store_existed_before,
                "store_exists": self.store.exists(),
            },
            "index": {
                "schema": index_refresh.get("schema", "context_index.refresh.v1"),
                "max_files": effective_max_files,
                "default_limited": max_files is None,
                "skipped": bool(index_refresh.get("skipped", False)),
                "reason": str(index_refresh.get("reason", "")),
                "file_count": int(index_refresh.get("file_count", 0) or 0),
                "symbol_count": int(index_refresh.get("symbol_count", 0) or 0),
                "import_count": int(index_refresh.get("import_count", 0) or 0),
                "updated_count": int(index_refresh.get("updated_count", 0) or 0),
                "unchanged_count": int(index_refresh.get("unchanged_count", 0) or 0),
                "removed_count": int(index_refresh.get("removed_count", 0) or 0),
                "search_mode": str(index_refresh.get("search_mode", "")),
                "refresh_signature_available": bool(
                    self.index.status().get("refresh_signature_available", False)
                ),
            },
            "workspace": {
                "schema": facts.get("schema", "workspace_facts.v1"),
                "file_count": int(facts.get("file_count", 0) or 0),
                "top_extensions": facts.get("top_extensions", []),
                "has_tests_dir": bool(facts.get("has_tests_dir", False)),
                "has_readme": bool(facts.get("has_readme", False)),
                "is_git_repo": bool(facts.get("is_git_repo", False)),
            },
            "symbols": {
                "warmed": symbol_count > 0,
                "count": symbol_count,
                "limit": max(0, min(int(max_entries), 100)),
            },
            "search_cache": {
                "namespace": "context_lookup.search",
                "path": path,
                "query_count": len(warmed_searches),
                "queries": warmed_searches,
            },
            "cache": {
                "entry_count_before": int(cache_before.get("entry_count", 0) or 0),
                "entry_count_after": int(cache_after.get("entry_count", 0) or 0),
                "namespaces_before": sorted(cache_before.get("namespaces", {})),
                "namespaces_after": sorted(cache_after.get("namespaces", {})),
            },
            "omitted": omitted,
            "repo_boundary_enforced": True,
            "generated_state_only": True,
        }

    def _warm_search_caches(
        self, path: str = ".", max_entries: int = 100
    ) -> list[dict[str, Any]]:
        seed_queries = (
            "test",
            "debug",
            "review",
            "config",
            "readme",
            "build",
            "cache",
            "index",
            "context",
            "error",
        )
        query_limit = max(0, min(int(max_entries), len(seed_queries)))
        max_results = 20
        index_status = self.index.status()
        rows: list[dict[str, Any]] = []
        for query in seed_queries[:query_limit]:
            cache_key = self._cache_key(
                "context_lookup.search",
                self._context_lookup_search_cache_args(
                    query=query,
                    path=path,
                    max_results=max_results,
                    include_globs=None,
                    index_status=index_status,
                ),
            )
            cache_lookup = self._cache_lookup(cache_key)
            if cache_lookup["hit"]:
                value = cache_lookup.get("value") if isinstance(cache_lookup, dict) else {}
                rows.append(
                    {
                        "query": query,
                        "path": path,
                        "cache_hit": True,
                        "cache_reason": "hit",
                        "result_count": int(
                            value.get("count", 0) if isinstance(value, dict) else 0
                        ),
                        "key": cache_key,
                    }
                )
                continue
            try:
                result = self.index.search(
                    query=query,
                    path=path,
                    max_results=max_results,
                    include_globs=None,
                    allow_fallback=False,
                )
            except Exception as exc:
                rows.append(
                    {
                        "query": query,
                        "reason_code": "search_warmup_failed",
                        "detail": type(exc).__name__,
                    }
                )
                continue
            self._cache_set(
                cache_key,
                result,
                namespace="context_lookup.search",
                metadata={
                    "query": query,
                    "path": path,
                    "index_generated_at": index_status.get("generated_at", ""),
                    "refresh_signature": index_status.get("refresh_signature", ""),
                    "warmup": True,
                },
            )
            rows.append(
                {
                    "query": query,
                    "path": path,
                    "cache_hit": False,
                    "cache_reason": str(cache_lookup.get("reason") or "miss"),
                    "result_count": int(result.get("count", 0) or 0),
                    "key": cache_key,
                }
            )
        return rows

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

    def _state_browser(
        self,
        prefix: str = "",
        key: str = "",
        max_entries: int = 100,
        max_output_chars: int | None = None,
    ) -> dict[str, Any]:
        self.config.ensure_state_dirs()
        prefix = (prefix or "").strip()
        key = (key or "").strip()
        row_budget = max(200, min(int(max_output_chars or 1200), 8000))
        entry_budget = max(500, min(int(max_output_chars or 12000), 50000))
        if key:
            value = self.store.get_json(key)
            exists = value is not None
            preview = self._state_value_preview(value, entry_budget) if exists else ""
            return {
                "schema": "context_state_browser.v1",
                "mode": "entry",
                "project_id": self.config.project_id,
                "key": key,
                "exists": exists,
                "entry": {
                    "key": key,
                    "value_type": type(value).__name__ if exists else "missing",
                    "size_chars": len(
                        json.dumps(
                            sanitize_json(value),
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        )
                    )
                    if exists
                    else 0,
                    "preview": preview,
                    "truncated": exists and len(preview) >= entry_budget,
                    "schema": value.get("schema", "") if isinstance(value, dict) else "",
                    "expires_at": value.get("expires_at", "")
                    if isinstance(value, dict)
                    else "",
                    "status": value.get("status", "") if isinstance(value, dict) else "",
                },
                "repo_boundary_enforced": True,
                "generated_state_only": True,
            }

        entries = self.store.iter_json(prefix)[: max(0, max_entries)]
        all_keys = [row_key for row_key, _value in self.store.iter_json("")]
        prefix_counts: dict[str, int] = {}
        for row_key in all_keys:
            group = row_key.split(":", 1)[0] + ":" if ":" in row_key else row_key
            prefix_counts[group] = prefix_counts.get(group, 0) + 1
        rows = [
            self._state_browser_row(row_key, value, row_budget)
            for row_key, value in entries
        ]
        return {
            "schema": "context_state_browser.v1",
            "mode": "list",
            "project_id": self.config.project_id,
            "prefix": prefix,
            "entry_count": len(rows),
            "max_entries": max_entries,
            "truncated": len(self.store.iter_json(prefix)) > len(rows),
            "prefix_counts": [
                {"prefix": row_prefix, "count": count}
                for row_prefix, count in sorted(prefix_counts.items())
            ],
            "rows": rows,
            "repo_boundary_enforced": True,
            "generated_state_only": True,
        }

    def _state_browser_row(
        self,
        key: str,
        value: Any,
        max_chars: int,
    ) -> dict[str, Any]:
        return {
            "key": key,
            "value_type": type(value).__name__,
            "size_chars": len(
                json.dumps(
                    sanitize_json(value),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
            ),
            "schema": value.get("schema", "") if isinstance(value, dict) else "",
            "status": value.get("status", "") if isinstance(value, dict) else "",
            "expires_at": value.get("expires_at", "")
            if isinstance(value, dict)
            else "",
            "preview": self._state_value_preview(value, max_chars),
        }

    def _state_value_preview(self, value: Any, max_chars: int) -> str:
        preview = json.dumps(
            sanitize_json(value),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            default=str,
        )
        if len(preview) <= max_chars:
            return preview
        return preview[: max(0, max_chars - 14)] + "\n...[truncated]"

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
        lookup = self._cache_lookup(key)
        value = lookup.get("value")
        return value if isinstance(value, dict) and lookup["hit"] else None

    def _cache_lookup(self, key: str) -> dict[str, Any]:
        row = self.store.get_json(f"cache:{key}")
        if not isinstance(row, dict):
            return {
                "hit": False,
                "status": "missing",
                "reason": "miss",
                "warnings": [],
            }
        status = self._cache_row_status(row)
        if status["status"] != "active":
            return {
                "hit": False,
                "status": status["status"],
                "reason": status["reason"],
                "expires_at": str(row.get("expires_at", "")),
                "warnings": status["warnings"],
            }
        value = row.get("value")
        if not isinstance(value, dict):
            return {
                "hit": False,
                "status": "stale",
                "reason": "invalid_payload",
                "expires_at": str(row.get("expires_at", "")),
                "warnings": [
                    {
                        "code": "cache_stale",
                        "message": "cached payload is not valid",
                    }
                ],
            }
        return {
            "hit": True,
            "status": "active",
            "reason": "hit",
            "value": value,
            "expires_at": str(row.get("expires_at", "")),
            "warnings": [],
        }

    def _cache_set(
        self,
        key: str,
        value: dict[str, Any],
        namespace: str,
        metadata: dict[str, Any] | None = None,
        ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        sanitized_value, sensitivity = sanitize_json(value)
        updated_at = now_iso()
        expires_at = (
            datetime.fromtimestamp(time.time() + max(1, ttl_seconds), timezone.utc)
            .isoformat()
        )
        self.store.put_json(
            f"cache:{key}",
            {
                "created_at": updated_at,
                "updated_at": updated_at,
                "expires_at": expires_at,
                "ttl_seconds": max(1, int(ttl_seconds)),
                "status": "active",
                "namespace": namespace,
                "key": key,
                "metadata": {**(metadata or {}), "project_id": self.config.project_id},
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
            namespace = str(row.get("namespace") or "unknown")
            row_status = self._cache_row_status(row)
            stats = namespaces.setdefault(
                namespace,
                {
                    "entry_count": 0,
                    "active_count": 0,
                    "expired_count": 0,
                    "stale_count": 0,
                    "sample_keys": [],
                    "hits": 0,
                    "misses": 0,
                    "hit_ratio": 0.0,
                },
            )
            stats["entry_count"] = int(stats["entry_count"]) + 1
            status_key = f"{row_status['status']}_count"
            stats[status_key] = int(stats.get(status_key, 0)) + 1
            if len(stats["sample_keys"]) < 5:
                stats["sample_keys"].append(key)
        for namespace, metric_stats in metric_namespaces.items():
            stats = namespaces.setdefault(
                namespace,
                {
                    "entry_count": 0,
                    "active_count": 0,
                    "expired_count": 0,
                    "stale_count": 0,
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
        }

    def _cache_prune(self, max_age_minutes: int) -> dict[str, Any]:
        removed = 0
        expired_removed = 0
        stale_removed = 0
        cutoff_seconds = max_age_minutes * 60
        now = time.time()
        with self.store.write_txn() as txn:
            entries = self.store.iter_json("cache:", txn=txn)
            for key, row in entries:
                if not isinstance(row, dict):
                    self.store.delete(key, txn=txn)
                    removed += 1
                    continue
                status = self._cache_row_status(row)
                if status["status"] == "expired":
                    removed += 1
                    expired_removed += 1
                    self.store.delete(key, txn=txn)
                    continue
                if status["status"] == "stale":
                    removed += 1
                    stale_removed += 1
                    self.store.delete(key, txn=txn)
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
        return {
            "removed_entries": removed,
            "expired_removed": expired_removed,
            "stale_removed": stale_removed,
            "entry_count": self.store.count("cache:"),
        }

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
            terms_key = str(metadata.get("terms_key", ""))
            if not prompt_rows and terms_key:
                prompt_rows = [
                    row
                    for row in rows
                    if str(row.get("metadata", {}).get("terms_key", ""))
                    == terms_key
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

    def _cache_row_status(self, row: dict[str, Any]) -> dict[str, Any]:
        if str(row.get("status", "")).lower() in {"stale", "invalidated"}:
            return {
                "status": "stale",
                "reason": "invalidated",
                "warnings": [
                    {"code": "cache_stale", "message": "cache row invalidated"}
                ],
            }
        if row.get("invalidated_at"):
            return {
                "status": "stale",
                "reason": "invalidated",
                "warnings": [
                    {"code": "cache_stale", "message": "cache row invalidated"}
                ],
            }
        expires_at = parse_iso(str(row.get("expires_at", "")))
        if expires_at and expires_at < datetime.now(timezone.utc):
            return {
                "status": "expired",
                "reason": "expired",
                "warnings": [
                    {"code": "cache_expired", "message": "cache row expired"}
                ],
            }
        return {"status": "active", "reason": "", "warnings": []}

    def _cache_public_metadata(
        self,
        key: str,
        namespace: str,
        lookup: dict[str, Any],
        reason: str = "",
    ) -> dict[str, Any]:
        return {
            "hit": bool(lookup.get("hit")),
            "key": key,
            "namespace": namespace,
            "reason": reason or str(lookup.get("reason") or "miss"),
            "status": str(lookup.get("status") or "missing"),
            "expires_at": str(lookup.get("expires_at", "")),
            "warnings": list(lookup.get("warnings") or []),
        }
