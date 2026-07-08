from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ContextConfig
from .index import ContextIndex
from .memory import ContextMemory
from .metrics import ContextMetrics
from .references import ResultReferences
from .runtime import background_jobs, io_executor
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
    trim_text,
)

DEFAULT_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
DEFAULT_CACHE_MAX_AGE_MINUTES = DEFAULT_CACHE_TTL_SECONDS // 60
DEFAULT_CACHE_PRUNE_INTERVAL_SECONDS = 5 * 60 * 60
CACHE_LAST_PRUNED_KEY = "cache:__meta__:last_pruned_at"
DEFAULT_INDEX_MAX_FILES = 5000
DEFAULT_WARMUP_MAX_FILES = 100
DEFAULT_BACKGROUND_REFRESH_INTERVAL_SECONDS = 5.0
CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR = 6
RETRIEVAL_SEARCH_TERM_SCHEMA = "retrieval.search_term.v1"
RETRIEVAL_FILE_SUMMARY_SCHEMA = "retrieval.file_summary.v1"
RETRIEVAL_SEARCH_TERM_POOL_SIZE = 40
RETRIEVAL_FILE_SUMMARY_MAX_CHARS = 1200
CACHE_ENTRY_SCHEMA_VERSION = 2
CHUNK_LINE_COUNT = 80
CHUNK_EXTRACTOR_VERSION = "chunk-lines-1"
CHUNK_REDACTION_VERSION = "redact-1"
OUTPUT_PROFILES = {"minimal", "compact", "normal", "verbose"}
CLIENT_PROFILES = {"generic", "codex", "claude", "copilot"}
MODEL_PROFILES = {"unknown", "openai", "anthropic", "github"}
DIAGNOSTIC_LEVELS = {"none", "summary", "full"}
CACHE_STRATEGIES = {"stable", "fresh", "cold"}
FRAGMENT_CACHE_NAMESPACES = {
    "context_lookup.search",
    "context_pack.retrieval",
    "retrieval.search_term",
    "retrieval.file_summary",
}
GENERIC_RETRIEVAL_TERMS = {
    "add",
    "and",
    "behavior",
    "bug",
    "build",
    "change",
    "code",
    "debug",
    "diff",
    "fix",
    "for",
    "implement",
    "implementation",
    "issue",
    "merge",
    "plan",
    "pr",
    "real",
    "refactor",
    "regression",
    "related",
    "review",
    "run",
    "test",
    "tests",
    "update",
    "verify",
    "with",
    "work",
    "world",
}


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
        allowed = {
            "search",
            "snippet",
            "tree",
            "symbols",
            "references",
            "impact",
            "related_symbols",
            "test_owners",
            "chunk",
            "explain_cache",
        }
        if mode not in allowed:
            raise ValueError(f"mode must be one of: {', '.join(sorted(allowed))}")
        if mode == "search":
            self._ensure_index_fresh(path=path)
            result, cache_metadata = self._cached_search(
                query=query,
                path=path,
                max_results=max_results,
                include_globs=include_globs,
                public_namespace="context_lookup.search",
            )
            self._record_metric(
                "context_lookup.search",
                started,
                cache_hit=bool(cache_metadata.get("hit")),
                cache_namespace="context_lookup.search",
                cache_reason=str(cache_metadata.get("reason") or "miss"),
                result_count=int(result.get("count", 0)),
            )
            self._cache_prune_if_due_best_effort()
            return {
                **result,
                "cache": cache_metadata,
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
        if mode == "chunk":
            self._ensure_index_fresh(path=path)
            result = self._chunk_lookup(path=path, start_line=start_line, end_line=end_line)
            self._record_metric("context_lookup.chunk", started, result_count=1)
            return result
        if mode == "impact":
            self._ensure_index_fresh(path=path)
            result = self._impact_lookup(path=path, max_results=max_results)
            self._record_metric(
                "context_lookup.impact",
                started,
                result_count=int(result.get("count", 0)),
            )
            return result
        if mode == "related_symbols":
            self._ensure_index_fresh(path=path)
            result = self._related_symbols_lookup(
                path=path, query=query, max_results=max_results
            )
            self._record_metric(
                "context_lookup.related_symbols",
                started,
                result_count=int(result.get("count", 0)),
            )
            return result
        if mode == "test_owners":
            self._ensure_index_fresh(path=path)
            result = self._test_owners_lookup(path=path, max_results=max_results)
            self._record_metric(
                "context_lookup.test_owners",
                started,
                result_count=int(result.get("count", 0)),
            )
            return result
        if mode == "explain_cache":
            result = self._explain_cache(path=path, max_results=max_results)
            self._record_metric(
                "context_lookup.explain_cache",
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
            "metrics_and_matrix",
            "benchmark",
            "state_browser",
            "quality_eval",
            "cache_plan",
            "profile_calibrate",
            "instructions",
            "resource_proxy",
            "schema_minify",
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
            return self._metrics_snapshot()
        if mode == "measurement_matrix":
            return self.metrics.measurement_matrix()
        if mode == "metrics_and_matrix":
            metrics = self._metrics_snapshot()
            return {
                "schema": "context_metrics_and_matrix.v1",
                "metrics": metrics,
                "matrix": self.metrics.measurement_matrix(snapshot=metrics),
            }
        if mode == "benchmark":
            return self._context_pack_benchmark(
                max_files=max_files
                if max_files is not None
                else DEFAULT_INDEX_MAX_FILES
            )
        if mode == "quality_eval":
            return self._quality_eval(max_entries=max_entries)
        if mode == "cache_plan":
            return self._cache_plan(max_output_chars=max_output_chars)
        if mode == "profile_calibrate":
            return self._profile_calibrate()
        if mode == "instructions":
            return json.loads(self.codex_guidance_resource())
        if mode == "resource_proxy":
            return self._resource_proxy(path=path, max_output_chars=max_output_chars)
        if mode == "schema_minify":
            return output_contracts(tool_name=tool_name, profile="compact")
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
        client_profile: str = "generic",
        model_profile: str = "unknown",
        evidence_policy: str = "summary_first",
        diagnostics: str = "summary",
        include_request_prompt: bool = False,
        include_runtime_metadata: bool = False,
        max_source_tokens: int = 1200,
        max_diagnostic_tokens: int = 300,
        cache_strategy: str = "stable",
    ) -> dict[str, Any]:
        if not prompt.strip():
            raise ValueError("prompt is required")
        started = time.perf_counter()
        stage_timings: dict[str, float] = {}
        self.config.ensure_state_dirs()
        client_profile = self._normalize_client_profile(client_profile)
        model_profile = self._normalize_model_profile(model_profile)
        diagnostics = self._normalize_diagnostics(diagnostics)
        cache_strategy = self._normalize_cache_strategy(cache_strategy)
        refresh_index = refresh_index or cache_strategy in {"fresh", "cold"}
        stage_started = time.perf_counter()
        index_refresh = self._ensure_context_pack_index(
            max_files=index_max_files,
            strict=refresh_index,
        )
        stage_timings["index_refresh_ms"] = self._elapsed_ms(stage_started)
        profile = self._effective_output_profile(output_profile, client_profile)
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
        refresh_signature, refresh_signature_available = self._current_refresh_signature()
        retrieval_max_items = max(
            CONTEXT_PACK_RETRIEVAL_ITEM_FLOOR,
            max(1, max_items),
        )
        cache_key = self._context_pack_retrieval_cache_key(
            route=route,
            terms=terms,
            explicit_paths=explicit_paths,
            refresh_signature=refresh_signature,
        )
        stage_started = time.perf_counter()
        if refresh_index or cache_strategy == "cold":
            cache_lookup = {
                "hit": False,
                "status": "disabled",
                "reason": "disabled_cache_strategy"
                if cache_strategy == "cold"
                else "disabled_refresh_index",
                "warnings": [],
            }
        else:
            cache_lookup = self._cache_lookup(cache_key)
        cached = cache_lookup.get("value") if cache_lookup["hit"] else None
        stage_timings["cache_lookup_ms"] = self._elapsed_ms(stage_started)
        cache_hit = isinstance(cached, dict)
        cache_reason = str(cache_lookup.get("reason") or "miss")
        fragment_hits = 0
        fragment_misses = 0
        fragment_miss_details: list[dict[str, Any]] = []
        if isinstance(cached, dict):
            candidates = list(cached.get("candidates", []))
            retrieval_omitted = list(cached.get("omitted", []))
            retrieval_stats = dict(cached.get("retrieval_stats", {}))
            retrieval_stats["fragment_hits"] = 0
            retrieval_stats["fragment_misses"] = 0
            retrieval_stats["fragment_hit_ratio"] = 0.0
            retrieval_stats["fragment_miss_details"] = []
            retrieval_stats["whole_pack_cache_hit"] = True
            stage_timings["candidate_retrieval_ms"] = 0.0
            stage_timings["search_ranking_ms"] = 0.0
            stage_timings["snippet_batch_ms"] = 0.0
            cache_reason = "hit"
        else:
            cache_reason = (
                "disabled_cache_strategy"
                if cache_strategy == "cold"
                else "disabled_refresh_index"
                if refresh_index
                else cache_reason
                if cache_reason in {"expired", "invalidated", "invalid_payload"}
                else self._cache_miss_reason(
                    namespace="context_pack.retrieval",
                    metadata={
                        "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                        "prompt_sha256": prompt_sha256,
                        "terms_key": terms_key,
                        "explicit_paths": self._canonical_cache_paths(explicit_paths),
                        "refresh_signature": refresh_signature,
                    },
                )
            )
            stage_started = time.perf_counter()
            candidates, retrieval_omitted, retrieval_stats = self._context_pack_candidates(
                terms=terms,
                explicit_paths=explicit_paths,
                profile=profile,
                route=route,
                max_items=retrieval_max_items,
                refresh_signature=refresh_signature,
                refresh_signature_available=refresh_signature_available,
            )
            stage_timings["candidate_retrieval_ms"] = self._elapsed_ms(stage_started)
            stage_timings["search_ranking_ms"] = stage_timings[
                "candidate_retrieval_ms"
            ]
            stage_timings["snippet_batch_ms"] = round(
                float(retrieval_stats.get("snippet_batch_ms", 0.0)), 3
            )
            fragment_hits = int(retrieval_stats.get("fragment_hits", 0) or 0)
            fragment_misses = int(retrieval_stats.get("fragment_misses", 0) or 0)
            fragment_miss_details = [
                row
                for row in retrieval_stats.get("fragment_miss_details", [])
                if isinstance(row, dict)
            ]
            if refresh_signature_available:
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
                        "refresh_signature": refresh_signature,
                        "terms_key": terms_key,
                        "explicit_paths": self._canonical_cache_paths(explicit_paths),
                    },
                )
            elif cache_reason == "no_compatible_entry":
                cache_reason = "signature_unavailable"
        stage_started = time.perf_counter()
        omitted = list(retrieval_omitted)
        response_candidates = self._profile_candidates(candidates, profile)
        selected, omitted_budget = self._select_candidates(
            response_candidates,
            max_items=max_items,
            content_budget=self._planned_content_budget(
                budget=budget,
                profile=profile,
                max_source_tokens=max_source_tokens,
            ),
            per_path_limit=1 if profile in {"minimal", "compact"} else 2,
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
                "candidate_count": len(response_candidates),
                "selected_count": len(selected),
                "candidates": response_candidates,
                "omitted": omitted,
                "retrieval_stats": retrieval_stats,
                "cache": {
                    "hit": cache_hit,
                    "key": cache_key,
                    "namespace": "context_pack.retrieval",
                    "reason": cache_reason,
                    "status": str(cache_lookup.get("status", "missing")),
                    "warnings": list(cache_lookup.get("warnings") or []),
                    "fragment_hits": fragment_hits,
                    "fragment_misses": fragment_misses,
                    "fragment_hit_ratio": self._hit_ratio(
                        fragment_hits, fragment_misses
                    ),
                    "chunk_hits": int(retrieval_stats.get("chunk_hits", 0) or 0),
                    "chunk_misses": int(retrieval_stats.get("chunk_misses", 0) or 0),
                    "chunk_hit_ratio": float(
                        retrieval_stats.get("chunk_hit_ratio", 0.0) or 0.0
                    ),
                    "miss_details": fragment_miss_details[:12],
                },
            },
            summary={"route": route, "candidate_count": len(response_candidates), "selected_count": len(selected)},
            ttl_hours=24,
        )
        stage_timings["reference_write_ms"] = self._elapsed_ms(stage_started)
        candidate_chars = sum(int(item.get("raw_chars", 0)) for item in response_candidates)
        selected_chars = sum(int(item.get("raw_chars", 0)) for item in selected)
        output_token_count = self._token_count(
            json.dumps(selected, ensure_ascii=False)
        )
        output_tokens = output_token_count.count
        baseline_count = self._baseline_input_token_count(response_candidates)
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
        request_metadata = {
            "prompt_sha256": prompt_sha256,
            "route": route,
            "terms": terms,
            "changed_files": changed_files or [],
            "focus_paths": focus_paths or [],
            "memory_session": memory_session,
            "output_profile": profile,
            "client_profile": client_profile,
            "model_profile": model_profile,
            "evidence_policy": evidence_policy,
            "diagnostics": diagnostics,
            "include_request_prompt": include_request_prompt,
            "include_runtime_metadata": include_runtime_metadata,
            "cache_strategy": cache_strategy,
        }
        if include_request_prompt:
            request_metadata["prompt"] = prompt
        result = {
            "schema": "context_pack.v1",
            "repo": {
                "path": self.config.display_path(self.config.repo_path),
                "state_dir": self.config.display_path(self.config.state_dir),
                "project_id": self.config.project_id,
                "root_uri_hash": sha256_text(self.config.root_uri)
                if self.config.root_uri
                    else "",
            },
            "request": request_metadata,
            "indexing": {
                "index_refresh": index_refresh,
                "index_freshness": self._index_freshness(index_refresh),
                "background": self._background_status(),
                "explicit_path_refresh": explicit_refresh,
            },
            "budget": {
                "max_output_chars": budget,
                "estimated_output_tokens": output_tokens,
                "token_counting": token_counting,
            },
            "summary": {
                "item_count": len(selected),
                "candidate_count": len(response_candidates),
                "omitted_count": len(omitted),
                "route": route,
                "candidates_per_selected": round(len(response_candidates) / len(selected), 3)
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
                "fragment_hits": fragment_hits,
                "fragment_misses": fragment_misses,
                "fragment_hit_ratio": self._hit_ratio(
                    fragment_hits, fragment_misses
                ),
                "chunk_hits": int(retrieval_stats.get("chunk_hits", 0) or 0),
                "chunk_misses": int(retrieval_stats.get("chunk_misses", 0) or 0),
                "chunk_hit_ratio": float(
                    retrieval_stats.get("chunk_hit_ratio", 0.0) or 0.0
                ),
                "miss_details": fragment_miss_details[:12],
                "index_refresh": {
                    "skipped": bool(index_refresh.get("skipped", False)),
                    "reason": index_refresh.get("reason", ""),
                },
                "index_freshness": self._index_freshness(index_refresh),
                "background_refresh_pending": bool(
                    self._background_status().get("index_refresh", {}).get("pending")
                ),
            },
            "safety": {
                "repository_boundary_enforced": True,
                "generated_state_only": True,
                "untrusted_content_signals": self._aggregate_signals(selected),
            },
            "metrics": {
                "elapsed_ms": 0.0,
                "stage_timings_ms": {},
                "candidate_count": len(response_candidates),
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
        if include_runtime_metadata or profile in {"normal", "verbose"}:
            result["generated_at"] = now_iso()
        stage_timings["response_assembly_ms"] = self._elapsed_ms(stage_started)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        stage_timings["total_ms"] = elapsed_ms
        result["metrics"]["elapsed_ms"] = elapsed_ms
        result["metrics"]["stage_timings_ms"] = stage_timings
        diagnostics_reference = self._diagnostics_reference(
            request=request_metadata,
            repo=result["repo"],
            indexing=result["indexing"],
            cache=result["cache"],
            safety=result["safety"],
            metrics=result["metrics"],
            max_diagnostic_tokens=max_diagnostic_tokens,
        )
        result["diagnostics_ref"] = diagnostics_reference["reference_id"]
        result["omitted_ref"] = full_reference["reference_id"]
        if profile == "minimal":
            result = self._minimal_context_pack(
                route=route,
                selected=selected,
                omitted=omitted,
                full_reference=full_reference,
                diagnostics_reference=diagnostics_reference,
                request=request_metadata,
                memory_context=memory_context,
            )
        elif diagnostics != "full":
            result["cache"] = self._cache_summary(result["cache"])
            result["metrics"] = self._metrics_summary(result["metrics"], diagnostics)
            if diagnostics == "none":
                result.pop("indexing", None)
                result.pop("safety", None)
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
            candidate_count=len(response_candidates),
            omitted_count=len(omitted),
            route=route,
            stage_timings_ms=stage_timings,
            cache_namespace="context_pack.retrieval",
            cache_reason=cache_reason,
            fragment_cache_hits=fragment_hits,
            fragment_cache_misses=fragment_misses,
        )
        self._cache_prune_if_due_best_effort()
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
                    "The MCP caller sets client_profile per context_pack "
                    "request: use codex for Codex, claude with "
                    "model_profile=anthropic for Claude, copilot with "
                    "model_profile=github for GitHub Copilot, and generic "
                    "otherwise. Explicit output_profile wins over "
                    "client_profile; when output_profile is omitted, "
                    "client_profile=codex uses minimal and other clients use "
                    "the configured default. "
                    "context_admin(mode=\"profile_calibrate\") reports "
                    "recommendations only; it does not set active state. Must "
                    "use context_lookup for targeted follow-up snippets, "
                    "search, trees, symbols, or references before broad shell "
                    "inspection. Must use "
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
                "profile_selection": {
                    "set_by": "MCP caller per context_pack request",
                    "auto_detection": False,
                    "precedence": [
                        "explicit output_profile",
                        "client_profile=codex implies output_profile=minimal when omitted",
                        "configured default_output_profile",
                    ],
                    "client_profiles": {
                        "codex": {
                            "model_profile": "openai",
                            "recommended_output_profile": "minimal",
                        },
                        "claude": {
                            "model_profile": "anthropic",
                            "recommended_output_profile": "compact",
                        },
                        "copilot": {
                            "model_profile": "github",
                            "recommended_output_profile": "compact",
                            "tools_only": True,
                        },
                        "generic": {
                            "model_profile": "unknown",
                            "recommended_output_profile": "configured default",
                        },
                    },
                    "calibration": (
                        "context_admin(mode=\"profile_calibrate\") reports "
                        "recommendations but does not mutate session or server state."
                    ),
                },
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

    def _ensure_context_pack_index(
        self,
        max_files: int,
        strict: bool,
    ) -> dict[str, Any]:
        if strict:
            result = self._ensure_index_fresh(max_files=max_files, force=True)
            result["freshness"] = "fresh"
            result["background_refresh_pending"] = False
            return result
        status = self.index.status()
        if not int(status.get("file_count", 0) or 0):
            result = self._ensure_index_fresh(max_files=max_files, force=True)
            result["freshness"] = "fresh"
            result["background_refresh_pending"] = False
            return result
        background = self._enqueue_background_index_refresh(max_files=max_files)
        return {
            "schema": "context_index.refresh.v1",
            "generated_at": status.get("generated_at", ""),
            "index_available": True,
            "file_count": int(status.get("file_count", 0) or 0),
            "symbol_count": int(status.get("symbol_count", 0) or 0),
            "import_count": int(status.get("import_count", 0) or 0),
            "files_considered": 0,
            "updated_count": 0,
            "unchanged_count": 0,
            "removed_count": 0,
            "fts_enabled": bool(status.get("fts_enabled", False)),
            "search_mode": str(status.get("search_mode", "term_index")),
            "skipped": True,
            "reason": "last_good_index",
            "freshness": "last_good",
            "background_refresh_pending": bool(background.get("pending")),
            "background_refresh_status": background.get("status", ""),
        }

    def _enqueue_background_index_refresh(self, max_files: int) -> dict[str, Any]:
        return background_jobs.submit(
            self._background_project_id(),
            "index_refresh",
            lambda: self.index.refresh_if_needed(max_files=max_files, force=False),
            executor=io_executor(),
            min_interval_seconds=DEFAULT_BACKGROUND_REFRESH_INTERVAL_SECONDS,
        )

    def _enqueue_background_cache_prune(self) -> dict[str, Any]:
        return background_jobs.submit(
            self._background_project_id(),
            "cache_prune",
            lambda: self._cache_prune_if_due(),
            executor=io_executor(),
            min_interval_seconds=DEFAULT_CACHE_PRUNE_INTERVAL_SECONDS,
        )

    def _background_status(self) -> dict[str, Any]:
        status = background_jobs.status(self._background_project_id())
        by_kind = {
            str(row.get("kind")): row
            for row in status.get("jobs", [])
            if isinstance(row, dict)
        }
        return {
            **status,
            "index_refresh": by_kind.get(
                "index_refresh",
                {
                    "kind": "index_refresh",
                    "status": "idle",
                    "pending": False,
                    "last_error": "",
                },
            ),
            "cache_prune": by_kind.get(
                "cache_prune",
                {
                    "kind": "cache_prune",
                    "status": "idle",
                    "pending": False,
                    "last_error": "",
                },
            ),
        }

    def _index_freshness(self, refresh: dict[str, Any] | None = None) -> dict[str, Any]:
        refresh = refresh or {}
        status = self.index.status()
        freshness = str(refresh.get("freshness") or "")
        if not freshness:
            freshness = "fresh" if not refresh.get("skipped") else "fresh"
        if str(refresh.get("reason", "")) == "last_good_index":
            freshness = "last_good"
        background = self._background_status().get("index_refresh", {})
        return {
            "schema": "context_index.freshness.v1",
            "state": freshness,
            "generated_at": status.get("generated_at", ""),
            "refresh_reason": refresh.get("reason", ""),
            "background_refresh_pending": bool(background.get("pending")),
            "background_refresh_status": background.get("status", "idle"),
            "background_refresh_last_error": background.get("last_error", ""),
        }

    def _metrics_snapshot(self) -> dict[str, Any]:
        snapshot = self.metrics.snapshot()
        snapshot["index_freshness"] = self._index_freshness()
        snapshot["background"] = self._background_status()
        return snapshot

    def _background_project_id(self) -> str:
        return self.config.project_id or sha256_text(
            str(self.config.repo_path.resolve())
        )[:24]

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
        safe: list[str] = []
        for item in [*changed, *focus]:
            if not item or item in safe:
                continue
            try:
                self.config.resolve_repo_path(item)
            except ValueError:
                continue
            safe.append(item)
        for match in re.findall(r"(?<![\w/.-])[\w./-]+\.[A-Za-z0-9]{1,8}(?=\b|:)", prompt):
            if match in safe:
                continue
            if self._is_prompt_path_candidate_current_or_existing(match):
                safe.append(match)
        return safe[:12]

    def _is_prompt_path_candidate_current_or_existing(self, rel: str) -> bool:
        try:
            repo_path = self.config.resolve_repo_path(rel)
        except ValueError:
            return False
        if repo_path.exists():
            return True
        return self.index.indexed_path_current(rel)

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
        refresh_signature: str,
    ) -> str:
        return self._cache_key(
            "context_pack.retrieval",
            {
                "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                "route": route,
                "terms": sorted(set(terms)),
                "explicit_paths": self._canonical_cache_paths(explicit_paths),
                "refresh_signature": refresh_signature,
                "project_id": self.config.project_id,
            },
        )

    def _current_refresh_signature(self) -> tuple[str, bool]:
        status_signature, status_available = self.index.stored_refresh_signature()
        if status_signature and status_available:
            return status_signature, True
        signature = self.index.refresh_signature(max_files=DEFAULT_INDEX_MAX_FILES)
        if bool(signature.get("available", False)) and signature.get("signature"):
            return str(signature["signature"]), True
        return "", False

    def _search_pool_size(self, requested: int) -> int:
        requested = max(1, int(requested))
        if requested <= RETRIEVAL_SEARCH_TERM_POOL_SIZE:
            return RETRIEVAL_SEARCH_TERM_POOL_SIZE
        return requested

    def _search_term_cache_key(
        self,
        term: str,
        path: str,
        include_globs: list[str] | None,
        pool_size: int,
        refresh_signature: str,
    ) -> str:
        return self._cache_key(
            "retrieval.search_term",
            {
                "schema": RETRIEVAL_SEARCH_TERM_SCHEMA,
                "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                "project_id": self.config.project_id,
                "refresh_signature": refresh_signature,
                "path": self._canonical_cache_path(path),
                "include_globs": self._canonical_cache_globs(include_globs),
                "term": term,
                "pool_size": self._search_pool_size(pool_size),
            },
        )

    def _cached_search(
        self,
        query: str,
        path: str,
        max_results: int,
        include_globs: list[str] | None,
        public_namespace: str,
        reusable_terms: list[str] | None = None,
        allow_fallback: bool = True,
        include_index: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        terms = normalize_query_terms(query, max_terms=8)
        if not terms:
            raise ValueError("query must contain at least one searchable term")
        refresh_signature, refresh_signature_available = self._current_refresh_signature()
        shard_terms = reusable_terms if reusable_terms is not None else terms
        shard_terms = [term for term in shard_terms if term]
        pool_size = self._search_pool_size(max_results)
        fragments: list[dict[str, Any]] = []
        miss_details: list[dict[str, Any]] = []
        fragment_hits = 0
        fragment_misses = 0
        for term in shard_terms:
            fragment = self._cached_search_term(
                term=term,
                path=path,
                include_globs=include_globs,
                pool_size=pool_size,
                refresh_signature=refresh_signature,
                refresh_signature_available=refresh_signature_available,
                allow_fallback=allow_fallback,
            )
            fragments.append(fragment)
            if fragment["cache_hit"]:
                fragment_hits += 1
            else:
                fragment_misses += 1
                miss_details.append(fragment["miss_detail"])
        rows = self._merge_search_fragments(
            terms=terms,
            fragments=fragments,
            max_results=max_results,
        )
        result = {
            "schema": "context_search.v1",
            "query": query,
            "terms": terms,
            "count": len(rows),
            "results": rows,
        }
        if include_index:
            result["index"] = self.index.status()
        public_key = self._cache_key(
            public_namespace,
            {
                "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                "project_id": self.config.project_id,
                "refresh_signature": refresh_signature,
                "path": self._canonical_cache_path(path),
                "include_globs": self._canonical_cache_globs(include_globs),
                "terms": sorted(set(terms)),
                "pool_size": pool_size,
            },
        )
        miss_reasons = [
            str(detail.get("reason", ""))
            for detail in miss_details
            if isinstance(detail, dict) and detail.get("reason")
        ]
        if fragment_misses == 0 and shard_terms:
            reason = "hit"
        elif miss_reasons and len(set(miss_reasons)) == 1:
            reason = miss_reasons[0]
        elif miss_reasons:
            reason = "fragment_miss"
        else:
            reason = "no_compatible_entry"
        status = "active"
        if any(reason == "expired" for reason in miss_reasons):
            status = "expired"
        elif any(reason in {"invalidated", "legacy_schema_version", "legacy_missing_refresh_signature"} for reason in miss_reasons):
            status = "stale"
        if not refresh_signature_available:
            reason = "signature_unavailable"
            status = "disabled"
        return result, {
            "hit": fragment_misses == 0 and bool(shard_terms),
            "key": public_key,
            "namespace": public_namespace,
            "reason": reason,
            "status": status,
            "expires_at": "",
            "warnings": [],
            "fragment_hits": fragment_hits,
            "fragment_misses": fragment_misses,
            "fragment_hit_ratio": self._hit_ratio(fragment_hits, fragment_misses),
            "miss_details": miss_details[:12],
        }

    def _cached_search_term(
        self,
        term: str,
        path: str,
        include_globs: list[str] | None,
        pool_size: int,
        refresh_signature: str,
        refresh_signature_available: bool,
        allow_fallback: bool = True,
    ) -> dict[str, Any]:
        canonical_path = self._canonical_cache_path(path)
        canonical_globs = self._canonical_cache_globs(include_globs)
        key = self._search_term_cache_key(
            term=term,
            path=canonical_path,
            include_globs=canonical_globs,
            pool_size=pool_size,
            refresh_signature=refresh_signature,
        )
        miss_detail = {
            "schema": "cache_miss_detail.v1",
            "namespace": "retrieval.search_term",
            "term": term,
            "path": canonical_path,
            "include_globs": canonical_globs,
            "reason": "signature_unavailable"
            if not refresh_signature_available
            else "no_compatible_entry",
        }
        if refresh_signature_available:
            lookup = self._cache_lookup(key)
            cached = lookup.get("value") if lookup["hit"] else None
            if isinstance(cached, dict) and cached.get("schema") == RETRIEVAL_SEARCH_TERM_SCHEMA:
                return {
                    "term": term,
                    "results": [
                        row for row in cached.get("results", []) if isinstance(row, dict)
                    ],
                    "cache_hit": True,
                    "cache_key": key,
                    "miss_detail": {},
                }
            lookup_reason = str(lookup.get("reason") or "")
            miss_detail["reason"] = (
                lookup_reason
                if lookup_reason not in {"", "miss", "hit"}
                else self._cache_miss_reason(
                    namespace="retrieval.search_term",
                    metadata={
                        "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                        "term": term,
                        "path": canonical_path,
                        "include_globs": canonical_globs,
                        "refresh_signature": refresh_signature,
                        "pool_size": self._search_pool_size(pool_size),
                    },
                )
            )
        result = self.index.search(
            query=term,
            path=canonical_path,
            max_results=self._search_pool_size(pool_size),
            include_globs=canonical_globs or None,
            allow_fallback=allow_fallback,
        )
        rows = [row for row in result.get("results", []) if isinstance(row, dict)]
        if refresh_signature_available:
            self._cache_set(
                key,
                {
                    "schema": RETRIEVAL_SEARCH_TERM_SCHEMA,
                    "term": term,
                    "path": canonical_path,
                    "include_globs": canonical_globs,
                    "refresh_signature": refresh_signature,
                    "pool_size": self._search_pool_size(pool_size),
                    "count": len(rows),
                    "results": rows,
                },
                namespace="retrieval.search_term",
                metadata={
                    "schema": RETRIEVAL_SEARCH_TERM_SCHEMA,
                    "term": term,
                    "path": canonical_path,
                    "include_globs": canonical_globs,
                    "refresh_signature": refresh_signature,
                    "pool_size": self._search_pool_size(pool_size),
                },
            )
        return {
            "term": term,
            "results": rows,
            "cache_hit": False,
            "cache_key": key,
            "miss_detail": miss_detail,
        }

    def _merge_search_fragments(
        self,
        terms: list[str],
        fragments: list[dict[str, Any]],
        max_results: int,
    ) -> list[dict[str, Any]]:
        matched: dict[str, dict[str, Any]] = {}
        for fragment in fragments:
            fragment_term = str(fragment.get("term", ""))
            for row in fragment.get("results", []):
                if not isinstance(row, dict):
                    continue
                path = str(row.get("path", ""))
                if not path:
                    continue
                current = matched.setdefault(
                    path,
                    {
                        "path": path,
                        "line": int(row.get("line", 1) or 1),
                        "excerpt": str(row.get("excerpt", "")),
                        "source": str(row.get("source", "term_index")),
                        "term_hits": 0,
                        "term_count": 0,
                        "_matched_terms": set(),
                    },
                )
                current["term_hits"] = int(current.get("term_hits", 0)) + int(
                    row.get("term_hits", 1) or 1
                )
                current["term_count"] = int(current.get("term_count", 0)) + int(
                    row.get("term_count", 1) or 1
                )
                current["_matched_terms"].add(fragment_term)
                row_line = int(row.get("line", 1) or 1)
                if row_line < int(current.get("line", 1) or 1):
                    current["line"] = row_line
                    current["excerpt"] = str(row.get("excerpt", ""))
                    current["source"] = str(row.get("source", current["source"]))
        rows: list[dict[str, Any]] = []
        for row in matched.values():
            path = str(row["path"])
            excerpt = str(row.get("excerpt", ""))
            score = sum(2.0 for term in terms if term in path.lower())
            score += sum(1.0 for term in terms if term in excerpt.lower())
            score += float(row.get("term_hits", 0)) * 2.5
            score += min(float(row.get("term_count", 0)), 8.0) * 0.25
            public_row = {
                key: value
                for key, value in row.items()
                if key != "_matched_terms"
            }
            public_row["score"] = round(score, 4)
            public_row["terms"] = terms
            rows.append(public_row)
        rows.sort(key=lambda item: (-float(item["score"]), item["path"]))
        return rows[:max_results]

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

    def _reusable_retrieval_terms(self, terms: list[str]) -> list[str]:
        reusable: list[str] = []
        seen: set[str] = set()
        for term in terms:
            if term in seen or term in GENERIC_RETRIEVAL_TERMS:
                continue
            seen.add(term)
            reusable.append(term)
            if len(reusable) >= 8:
                break
        return reusable

    def _file_fingerprint(self, path: str) -> dict[str, Any]:
        file_path = self.config.resolve_repo_path(path)
        stat = file_path.stat()
        rel = self.config.repo_relative(file_path)
        row = self.store.get_json(f"index:file:{rel}", {})
        digest = ""
        if (
            isinstance(row, dict)
            and int(row.get("size", -1)) == int(stat.st_size)
            and int(row.get("mtime_ns", -1)) == int(stat.st_mtime_ns)
        ):
            digest = str(row.get("sha256", ""))
        return {
            "path": rel,
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "sha256": digest,
            "cache_token": digest or f"stat:{int(stat.st_size)}:{int(stat.st_mtime_ns)}",
        }

    def _file_summary_cache_key(
        self,
        fingerprint: dict[str, Any],
        line_anchor: int,
    ) -> str:
        chunk = self._chunk_metadata(fingerprint, line_anchor)
        return self._cache_key(
            "retrieval.file_summary",
            {
                "schema": RETRIEVAL_FILE_SUMMARY_SCHEMA,
                "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                "project_id": self.config.project_id,
                "path": fingerprint.get("path", ""),
                "fingerprint": fingerprint.get("cache_token", ""),
                "line_anchor": max(0, int(line_anchor)),
                "chunk_id": chunk["chunk_id"],
                "content_digest": chunk["content_digest"],
                "extractor_version": CHUNK_EXTRACTOR_VERSION,
                "redaction_version": CHUNK_REDACTION_VERSION,
                "max_chars": RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
            },
        )

    def _chunk_metadata(
        self,
        fingerprint: dict[str, Any],
        line_anchor: int,
        end_line: int | None = None,
    ) -> dict[str, Any]:
        start = max(1, int(line_anchor or 1))
        chunk_start = ((start - 1) // CHUNK_LINE_COUNT) * CHUNK_LINE_COUNT + 1
        chunk_end = max(
            chunk_start,
            int(end_line or chunk_start + CHUNK_LINE_COUNT - 1),
        )
        path = str(fingerprint.get("path", ""))
        token = str(fingerprint.get("cache_token", ""))
        digest = sha256_text(f"{path}:{token}:{chunk_start}:{chunk_end}")
        return {
            "schema": "context_chunk_metadata.v1",
            "path": path,
            "file_digest": f"sha256:{fingerprint.get('sha256', '')}"
            if fingerprint.get("sha256")
            else "",
            "chunk_id": f"chk_{digest[:16]}",
            "start_line": chunk_start,
            "end_line": chunk_end,
            "content_digest": f"sha256:{digest}",
            "extractor_version": CHUNK_EXTRACTOR_VERSION,
            "redaction_version": CHUNK_REDACTION_VERSION,
        }

    def _cached_file_summary(
        self,
        path: str,
        line_anchor: int,
        refresh_signature: str,
        refresh_signature_available: bool,
    ) -> tuple[dict[str, Any], bool, dict[str, Any]]:
        fingerprint = self._file_fingerprint(path)
        anchor = max(0, int(line_anchor))
        chunk = self._chunk_metadata(fingerprint, anchor)
        key = self._file_summary_cache_key(fingerprint, anchor)
        miss_detail = {
            "schema": "cache_miss_detail.v1",
            "namespace": "retrieval.file_summary",
            "path": fingerprint["path"],
            "line_anchor": anchor,
            "chunk_id": chunk["chunk_id"],
            "reason": "signature_unavailable"
            if not refresh_signature_available
            else "no_compatible_entry",
        }
        if not refresh_signature_available:
            summary = self.index.file_summary(
                fingerprint["path"],
                max_chars=RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
                matched_line=anchor or None,
            )
            return summary, False, miss_detail
        lookup = self._cache_lookup(key)
        cached = lookup.get("value") if lookup["hit"] else None
        if isinstance(cached, dict) and cached.get("schema") == RETRIEVAL_FILE_SUMMARY_SCHEMA:
            summary = cached.get("summary")
            if isinstance(summary, dict):
                return summary, True, {}
        lookup_reason = str(lookup.get("reason") or "")
        miss_detail["reason"] = (
            lookup_reason
            if lookup_reason not in {"", "miss", "hit"}
            else self._cache_miss_reason(
                namespace="retrieval.file_summary",
                metadata={
                    "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                    "path": fingerprint["path"],
                    "file_fingerprint": fingerprint,
                    "line_anchor": anchor,
                    "chunk_id": chunk["chunk_id"],
                },
            )
        )
        summary = self.index.file_summary(
            fingerprint["path"],
            max_chars=RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
            matched_line=anchor or None,
        )
        if refresh_signature_available:
            self._cache_set(
                key,
                {
                    "schema": RETRIEVAL_FILE_SUMMARY_SCHEMA,
                    "path": fingerprint["path"],
                    "file_fingerprint": fingerprint,
                    "line_anchor": anchor,
                    "chunk": chunk,
                    "max_chars": RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
                    "summary": summary,
                },
                namespace="retrieval.file_summary",
                metadata={
                    "schema": RETRIEVAL_FILE_SUMMARY_SCHEMA,
                    "path": fingerprint["path"],
                    "file_fingerprint": fingerprint,
                    "line_anchor": anchor,
                    "chunk_id": chunk["chunk_id"],
                    "content_digest": chunk["content_digest"],
                    "extractor_version": CHUNK_EXTRACTOR_VERSION,
                    "redaction_version": CHUNK_REDACTION_VERSION,
                    "refresh_signature": refresh_signature,
                    "max_chars": RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
                },
            )
        return summary, False, miss_detail

    def _effective_output_profile(
        self, output_profile: str | None, client_profile: str
    ) -> str:
        profile = output_profile or (
            "minimal" if client_profile == "codex" else self._budget()["default_output_profile"]
        )
        if profile not in OUTPUT_PROFILES:
            raise ValueError("output_profile must be minimal, compact, normal, or verbose")
        return profile

    def _normalize_client_profile(self, client_profile: str) -> str:
        profile = (client_profile or "generic").strip().lower()
        if profile not in CLIENT_PROFILES:
            raise ValueError("client_profile must be codex, claude, copilot, or generic")
        return profile

    def _normalize_model_profile(self, model_profile: str) -> str:
        profile = (model_profile or "unknown").strip().lower()
        if profile not in MODEL_PROFILES:
            raise ValueError("model_profile must be openai, anthropic, github, or unknown")
        return profile

    def _normalize_diagnostics(self, diagnostics: str) -> str:
        level = (diagnostics or "summary").strip().lower()
        if level not in DIAGNOSTIC_LEVELS:
            raise ValueError("diagnostics must be none, summary, or full")
        return level

    def _normalize_cache_strategy(self, cache_strategy: str) -> str:
        strategy = (cache_strategy or "stable").strip().lower()
        if strategy not in CACHE_STRATEGIES:
            raise ValueError("cache_strategy must be stable, fresh, or cold")
        return strategy

    def _planned_content_budget(
        self, budget: int, profile: str, max_source_tokens: int
    ) -> int:
        if profile == "minimal":
            return max(500, min(budget - 500, max(1, int(max_source_tokens)) * 4))
        return max(1000, budget - 2400)

    def _diagnostics_reference(
        self,
        request: dict[str, Any],
        repo: dict[str, Any],
        indexing: dict[str, Any],
        cache: dict[str, Any],
        safety: dict[str, Any],
        metrics: dict[str, Any],
        max_diagnostic_tokens: int,
    ) -> dict[str, Any]:
        payload = {
            "schema": "context_pack.diagnostics.v1",
            "request": request,
            "repo": repo,
            "indexing": indexing,
            "cache": cache,
            "safety": safety,
            "metrics": metrics,
            "max_diagnostic_tokens": max(0, int(max_diagnostic_tokens)),
        }
        return self.references.create(
            producer="context_pack.diagnostics",
            payload=payload,
            summary={
                "route": request.get("route", ""),
                "cache_hit": bool(cache.get("hit")),
                "diagnostic_tokens_est": self._count_tokens(payload),
            },
            ttl_hours=24,
        )

    def _minimal_context_pack(
        self,
        route: str,
        selected: list[dict[str, Any]],
        omitted: list[dict[str, Any]],
        full_reference: dict[str, Any],
        diagnostics_reference: dict[str, Any],
        request: dict[str, Any],
        memory_context: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema": "context_pack.minimal.v1",
            "route": route,
            "summary": self._minimal_summary(route, selected, omitted, memory_context),
            "items": [self._minimal_item(item) for item in selected],
            "omitted_ref": full_reference["reference_id"],
            "diagnostics_ref": diagnostics_reference["reference_id"],
            "request": {
                "prompt_sha256": request.get("prompt_sha256", ""),
                "terms": request.get("terms", []),
                "changed_files": request.get("changed_files", []),
                "focus_paths": request.get("focus_paths", []),
                "client_profile": request.get("client_profile", "generic"),
                "model_profile": request.get("model_profile", "unknown"),
            },
        }

    def _minimal_summary(
        self,
        route: str,
        selected: list[dict[str, Any]],
        omitted: list[dict[str, Any]],
        memory_context: dict[str, Any],
    ) -> str:
        paths = [str(item.get("path", "")) for item in selected[:3] if item.get("path")]
        if not paths:
            return f"{route} context: no matching repository evidence selected."
        suffix = ""
        if omitted:
            suffix = f" {len(omitted)} lower-ranked items deferred."
        if int(memory_context.get("summary_count", 0) or 0):
            suffix += " Memory summaries available in diagnostics."
        return f"{route} context: top evidence in {', '.join(paths)}.{suffix}"

    def _minimal_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "path": item.get("path", ""),
            "lines": [
                int(item.get("start_line", 1) or 1),
                int(item.get("end_line", item.get("start_line", 1)) or 1),
            ],
            "reason": list(item.get("reason_codes", [])),
            "confidence": self._confidence_bucket(item),
            "content": item.get("content", ""),
            "detail_lookup": item.get("detail_lookup", {}),
        }

    def _confidence_bucket(self, item: dict[str, Any]) -> str:
        score = float(item.get("score", 0.0) or 0.0)
        confidence = float(item.get("confidence", 0.0) or 0.0)
        value = max(score / 14.0, confidence)
        if value >= 0.8:
            return "high"
        if value >= 0.45:
            return "medium"
        return "low"

    def _cache_summary(self, cache: dict[str, Any]) -> dict[str, Any]:
        return {
            "hit": bool(cache.get("hit")),
            "key": cache.get("key", ""),
            "namespace": cache.get("namespace", ""),
            "reason": cache.get("reason", ""),
            "status": cache.get("status", ""),
            "expires_at": cache.get("expires_at", ""),
            "warnings": cache.get("warnings", []),
            "fragment_hits": int(cache.get("fragment_hits", 0) or 0),
            "fragment_misses": int(cache.get("fragment_misses", 0) or 0),
            "fragment_hit_ratio": float(cache.get("fragment_hit_ratio", 0.0) or 0.0),
            "chunk_hits": int(cache.get("chunk_hits", 0) or 0),
            "chunk_misses": int(cache.get("chunk_misses", 0) or 0),
            "chunk_hit_ratio": float(cache.get("chunk_hit_ratio", 0.0) or 0.0),
            "miss_details": cache.get("miss_details", []),
            "index_freshness": cache.get("index_freshness", {}),
            "background_refresh_pending": bool(
                cache.get("background_refresh_pending", False)
            ),
        }

    def _metrics_summary(self, metrics: dict[str, Any], diagnostics: str) -> dict[str, Any]:
        if diagnostics == "full":
            return metrics
        summary = {
            "elapsed_ms": metrics.get("elapsed_ms", 0.0),
            "stage_timings_ms": metrics.get("stage_timings_ms", {}),
            "candidate_count": metrics.get("candidate_count", 0),
            "selected_count": metrics.get("selected_count", 0),
            "baseline_input_tokens_est": metrics.get("baseline_input_tokens_est", 0),
            "output_tokens_est": metrics.get("output_tokens_est", 0),
            "estimated_input_tokens_saved": metrics.get("estimated_input_tokens_saved", 0),
            "tokens_spared_by_mcp_est": metrics.get("tokens_spared_by_mcp_est", 0),
            "compression_ratio": metrics.get("compression_ratio", 0.0),
            "external_tool_calls_saved_est": metrics.get("external_tool_calls_saved_est", 0),
            "references_bytes_deferred_est": metrics.get("references_bytes_deferred_est", 0),
            "token_counting": metrics.get("token_counting", {}),
            "token_savings_formula": metrics.get("token_savings_formula", ""),
            "tokens_spared_by_mcp_formula": metrics.get("tokens_spared_by_mcp_formula", ""),
            "tokens_spared_by_mcp_reason": metrics.get("tokens_spared_by_mcp_reason", ""),
        }
        retrieval_plan = metrics.get("retrieval_plan")
        if isinstance(retrieval_plan, dict):
            summary["retrieval_plan"] = {
                "schema": retrieval_plan.get("schema", "context_pack.retrieval_plan.v1"),
                "profile": retrieval_plan.get("profile", ""),
                "detail_mode": retrieval_plan.get("detail_mode", ""),
                "snippet_request_count": retrieval_plan.get("snippet_request_count", 0),
                "symbol_lookup_skipped": retrieval_plan.get("symbol_lookup_skipped", False),
                "chunk_hits": retrieval_plan.get("chunk_hits", 0),
                "chunk_misses": retrieval_plan.get("chunk_misses", 0),
                "chunk_hit_ratio": retrieval_plan.get("chunk_hit_ratio", 0.0),
            }
        return summary

    def _profile_candidates(
        self, candidates: list[dict[str, Any]], profile: str
    ) -> list[dict[str, Any]]:
        profiled = []
        for item in candidates:
            copied = dict(item)
            source_chars = int(copied.get("source_chars", 0) or 0)
            is_explicit = "explicit_path" in set(copied.get("reason_codes", []))
            if profile == "minimal":
                max_chars = 220 if is_explicit else 180
            elif profile == "compact":
                max_chars = 360 if is_explicit else 260
            elif profile == "verbose":
                max_chars = 900
            else:
                max_chars = 420
            content, truncated = trim_text(str(copied.get("content", "")), max_chars)
            copied["content"] = content
            copied["raw_chars"] = len(content)
            copied["deferred_chars"] = max(0, source_chars - len(content))
            copied["prompt_injection_signals"] = prompt_injection_signals(content)
            if truncated:
                copied["truncated"] = True
            profiled.append(copied)
        return profiled

    def _hit_ratio(self, hits: int, misses: int) -> float:
        total = max(0, int(hits)) + max(0, int(misses))
        return round(max(0, int(hits)) / total, 4) if total else 0.0

    def _context_pack_candidates(
        self,
        terms: list[str],
        explicit_paths: list[str],
        profile: str,
        route: str,
        max_items: int,
        refresh_signature: str,
        refresh_signature_available: bool,
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
            "reusable_terms": self._reusable_retrieval_terms(terms),
            "fragment_hits": 0,
            "fragment_misses": 0,
            "fragment_hit_ratio": 0.0,
            "fragment_miss_details": [],
            "chunk_hits": 0,
            "chunk_misses": 0,
            "chunk_hit_ratio": 0.0,
        }

        def count_source(source: str) -> None:
            counts = retrieval_stats.setdefault("source_counts", {})
            counts[source] = int(counts.get(source, 0)) + 1

        def count_fragment(hit: bool, detail: dict[str, Any]) -> None:
            if hit:
                retrieval_stats["fragment_hits"] = int(
                    retrieval_stats.get("fragment_hits", 0)
                ) + 1
                return
            retrieval_stats["fragment_misses"] = int(
                retrieval_stats.get("fragment_misses", 0)
            ) + 1
            if detail:
                retrieval_stats.setdefault("fragment_miss_details", []).append(detail)

        def count_chunk(hit: bool) -> None:
            key = "chunk_hits" if hit else "chunk_misses"
            retrieval_stats[key] = int(retrieval_stats.get(key, 0) or 0) + 1

        for rel in explicit_paths:
            try:
                summary, summary_hit, miss_detail = self._cached_file_summary(
                    rel,
                    line_anchor=0,
                    refresh_signature=refresh_signature,
                    refresh_signature_available=refresh_signature_available,
                )
                count_fragment(summary_hit, miss_detail)
                count_chunk(summary_hit)
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
                reusable_terms = self._reusable_retrieval_terms(terms)
                retrieval_stats["reusable_terms"] = reusable_terms
                search, search_cache = self._cached_search(
                    query=" ".join(terms),
                    path=".",
                    max_results=search_limit,
                    include_globs=None,
                    public_namespace="context_pack.search",
                    reusable_terms=reusable_terms,
                    allow_fallback=False,
                    include_index=False,
                )
                retrieval_stats["fragment_hits"] = int(
                    retrieval_stats.get("fragment_hits", 0)
                ) + int(search_cache.get("fragment_hits", 0) or 0)
                retrieval_stats["fragment_misses"] = int(
                    retrieval_stats.get("fragment_misses", 0)
                ) + int(search_cache.get("fragment_misses", 0) or 0)
                retrieval_stats.setdefault("fragment_miss_details", []).extend(
                    row
                    for row in search_cache.get("miss_details", [])
                    if isinstance(row, dict)
                )
                retrieval_stats["search_result_count"] = len(search["results"])
                for row in search["results"]:
                    path = row["path"]
                    line = int(row.get("line") or self._first_matching_line(path, terms) or 1)
                    summary, summary_hit, miss_detail = self._cached_file_summary(
                        path,
                        line_anchor=line,
                        refresh_signature=refresh_signature,
                        refresh_signature_available=refresh_signature_available,
                    )
                    count_fragment(summary_hit, miss_detail)
                    count_chunk(summary_hit)
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
                profile in {"minimal", "compact"}
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
                    summary, summary_hit, miss_detail = self._cached_file_summary(
                        str(row["path"]),
                        line_anchor=max(1, int(row["line_start"])),
                        refresh_signature=refresh_signature,
                        refresh_signature_available=refresh_signature_available,
                    )
                    count_fragment(summary_hit, miss_detail)
                    count_chunk(summary_hit)
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

        retrieval_stats["fragment_hit_ratio"] = self._hit_ratio(
            int(retrieval_stats.get("fragment_hits", 0) or 0),
            int(retrieval_stats.get("fragment_misses", 0) or 0),
        )
        retrieval_stats["chunk_hit_ratio"] = self._hit_ratio(
            int(retrieval_stats.get("chunk_hits", 0) or 0),
            int(retrieval_stats.get("chunk_misses", 0) or 0),
        )
        retrieval_stats["fragment_miss_details"] = retrieval_stats.get(
            "fragment_miss_details", []
        )[:20]
        self._add_test_owner_candidates(
            candidates=candidates,
            explicit_paths=explicit_paths,
            refresh_signature=refresh_signature,
            refresh_signature_available=refresh_signature_available,
            count_fragment=count_fragment,
            count_chunk=count_chunk,
            retrieval_stats=retrieval_stats,
        )
        self._apply_route_ranking(
            candidates=candidates,
            route=route,
            explicit_paths=explicit_paths,
            terms=terms,
        )
        retrieval_stats["fragment_miss_details"] = retrieval_stats.get(
            "fragment_miss_details", []
        )[:20]
        retrieval_stats["fragment_hit_ratio"] = self._hit_ratio(
            int(retrieval_stats.get("fragment_hits", 0) or 0),
            int(retrieval_stats.get("fragment_misses", 0) or 0),
        )
        retrieval_stats["chunk_hit_ratio"] = self._hit_ratio(
            int(retrieval_stats.get("chunk_hits", 0) or 0),
            int(retrieval_stats.get("chunk_misses", 0) or 0),
        )
        return candidates, omitted, retrieval_stats

    def _add_test_owner_candidates(
        self,
        candidates: list[dict[str, Any]],
        explicit_paths: list[str],
        refresh_signature: str,
        refresh_signature_available: bool,
        count_fragment: Any,
        count_chunk: Any,
        retrieval_stats: dict[str, Any],
    ) -> None:
        seen = {str(item.get("path", "")) for item in candidates}
        added = 0
        for owner in self._test_owner_paths(explicit_paths, max_results=8):
            path = str(owner.get("path", ""))
            if not path or path in seen:
                continue
            try:
                summary, summary_hit, miss_detail = self._cached_file_summary(
                    path,
                    line_anchor=1,
                    refresh_signature=refresh_signature,
                    refresh_signature_available=refresh_signature_available,
                )
            except Exception:
                continue
            count_fragment(summary_hit, miss_detail)
            count_chunk(summary_hit)
            candidates.append(
                self._candidate_from_summary(
                    summary,
                    score=10.0,
                    reason_codes=["test_owner"],
                    source="test_impact",
                )
            )
            seen.add(path)
            added += 1
        retrieval_stats["test_owner_summary_count"] = added

    def _apply_route_ranking(
        self,
        candidates: list[dict[str, Any]],
        route: str,
        explicit_paths: list[str],
        terms: list[str],
    ) -> None:
        explicit = set(self._canonical_cache_paths(explicit_paths))
        term_set = set(terms)
        for item in candidates:
            path = str(item.get("path", ""))
            reasons = set(item.get("reason_codes", []))
            score = float(item.get("score", 0.0) or 0.0)
            if path in explicit:
                score += 6.0
            if "test_owner" in reasons:
                score += 4.0 if route in {"coding", "review", "debug", "test"} else 1.0
            if path.startswith(("tests/", "test/")):
                score += 2.0 if route in {"coding", "review", "debug", "test"} else 0.0
            if route == "docs" and path.lower().startswith(("readme", "docs/")):
                score += 5.0
            if route == "security" and any(
                marker in path.lower()
                for marker in ("auth", "security", "secret", "token", "config", "path")
            ):
                score += 3.0
            if term_set and any(term in path.lower() for term in term_set):
                score += 1.0
            item["score"] = round(score, 4)
            item["confidence"] = round(min(0.99, max(0.1, score / 18.0)), 3)

    def _chunk_lookup(
        self, path: str, start_line: int = 1, end_line: int | None = None
    ) -> dict[str, Any]:
        fingerprint = self._file_fingerprint(path)
        chunk = self._chunk_metadata(fingerprint, start_line, end_line)
        snippet = self.index.snippet(
            path=fingerprint["path"],
            start_line=chunk["start_line"],
            end_line=chunk["end_line"],
            max_chars=RETRIEVAL_FILE_SUMMARY_MAX_CHARS,
        )
        return {
            "schema": "context_lookup.chunk.v1",
            "chunk": chunk,
            "path": fingerprint["path"],
            "content": snippet["content"],
            "detail_lookup": {
                "tool": "context_lookup",
                "mode": "snippet",
                "path": fingerprint["path"],
                "start_line": chunk["start_line"],
                "end_line": chunk["end_line"],
            },
        }

    def _impact_lookup(self, path: str, max_results: int) -> dict[str, Any]:
        related = []
        related.extend(self._test_owner_rows(path=path, max_results=max_results))
        for symbol in self._related_symbol_rows(path=path, query="", max_results=max_results):
            related.append(symbol)
            if len(related) >= max_results:
                break
        return {
            "schema": "context_lookup.impact.v1",
            "source": self._canonical_cache_path(path),
            "count": len(related[:max_results]),
            "related": related[:max_results],
        }

    def _related_symbols_lookup(
        self, path: str, query: str, max_results: int
    ) -> dict[str, Any]:
        rows = self._related_symbol_rows(path=path, query=query, max_results=max_results)
        return {
            "schema": "context_lookup.related_symbols.v1",
            "source": self._canonical_cache_path(path),
            "count": len(rows),
            "symbols": rows,
        }

    def _test_owners_lookup(self, path: str, max_results: int) -> dict[str, Any]:
        rows = self._test_owner_rows(path=path, max_results=max_results)
        return {
            "schema": "context_lookup.test_owners.v1",
            "source": self._canonical_cache_path(path),
            "count": len(rows),
            "related": rows,
        }

    def _related_symbol_rows(
        self, path: str, query: str, max_results: int
    ) -> list[dict[str, Any]]:
        rel = self._canonical_cache_path(path)
        terms = normalize_query_terms(query or rel.replace("/", " "), max_terms=8)
        symbols = self.index.symbols(query=" ".join(terms), limit=max(max_results * 4, 20))
        rows: list[dict[str, Any]] = []
        for row in symbols.get("symbols", []):
            if not isinstance(row, dict):
                continue
            symbol_path = str(row.get("path", ""))
            if symbol_path != rel and Path(symbol_path).stem != Path(rel).stem:
                continue
            rows.append(
                {
                    "path": symbol_path,
                    "symbol": row.get("name", ""),
                    "kind": row.get("kind", ""),
                    "relationship": "same_file" if symbol_path == rel else "name_match",
                    "confidence": "high" if symbol_path == rel else "medium",
                    "detail_lookup": {
                        "tool": "context_lookup",
                        "mode": "snippet",
                        "path": symbol_path,
                        "start_line": int(row.get("line_start", 1) or 1),
                        "end_line": int(row.get("line_end", row.get("line_start", 1)) or 1),
                    },
                }
            )
            if len(rows) >= max_results:
                break
        return rows

    def _test_owner_rows(self, path: str, max_results: int) -> list[dict[str, Any]]:
        return [
            {
                "path": row["path"],
                "relationship": "test_owner",
                "confidence": row["confidence"],
                "detail_lookup": {
                    "tool": "context_lookup",
                    "mode": "snippet",
                    "path": row["path"],
                    "start_line": 1,
                },
            }
            for row in self._test_owner_paths([path], max_results=max_results)
        ]

    def _test_owner_paths(
        self, paths: list[str], max_results: int = 10
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[str] = set()
        for raw_path in paths:
            rel = self._canonical_cache_path(raw_path)
            if not rel or rel.startswith(("tests/", "test/")):
                continue
            stem = Path(rel).stem
            direct = [
                f"tests/test_{stem}.py",
                f"test/test_{stem}.py",
                f"tests/{stem}_test.py",
                f"test/{stem}_test.py",
            ]
            for candidate in direct:
                try:
                    resolved = self.config.resolve_repo_path(candidate)
                except ValueError:
                    continue
                if resolved.is_file() and candidate not in seen:
                    rows.append({"path": candidate, "confidence": "high"})
                    seen.add(candidate)
            if len(rows) >= max_results:
                break
            try:
                search = self.index.search(
                    query=stem,
                    path=".",
                    max_results=max_results,
                    include_globs=["tests/**", "test/**"],
                )
            except Exception:
                continue
            for result in search.get("results", []):
                path = str(result.get("path", ""))
                if path and path not in seen:
                    rows.append({"path": path, "confidence": "medium"})
                    seen.add(path)
                if len(rows) >= max_results:
                    break
        return rows[:max_results]

    def _explain_cache(self, path: str, max_results: int) -> dict[str, Any]:
        rel = self._canonical_cache_path(path)
        rows = []
        for key, row in self.store.iter_json("cache:"):
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata", {})
            metadata = metadata if isinstance(metadata, dict) else {}
            if rel not in {".", "", str(metadata.get("path", ""))}:
                continue
            status = self._cache_row_status(row)
            rows.append(
                {
                    "key": key.removeprefix("cache:"),
                    "namespace": row.get("namespace", ""),
                    "status": status["status"],
                    "reason": status["reason"],
                    "path": metadata.get("path", ""),
                    "chunk_id": metadata.get("chunk_id", ""),
                    "updated_at": row.get("updated_at", ""),
                }
            )
            if len(rows) >= max_results:
                break
        return {
            "schema": "context_lookup.explain_cache.v1",
            "path": rel,
            "count": len(rows),
            "rows": rows,
        }

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

    def _quality_eval(self, max_entries: int = 100) -> dict[str, Any]:
        fixtures = self._gold_anchor_fixtures(max_entries=max_entries)
        total_required = 0
        hits_at_3 = 0
        hits_at_5 = 0
        first_ranks: list[int] = []
        noise_rows = 0
        item_rows = 0
        required_omitted = 0
        stale_hits = 0
        detail_lookup_count = 0
        regressions = []
        for fixture in fixtures:
            task = str(fixture.get("task") or fixture.get("prompt") or "")
            if not task:
                continue
            pack = self.context_pack(
                prompt=task,
                changed_files=fixture.get("changed_files") or [],
                focus_paths=fixture.get("focus_paths") or [],
                max_items=5,
                output_profile="minimal",
                diagnostics="none",
            )
            items = [item for item in pack.get("items", []) if isinstance(item, dict)]
            paths = [str(item.get("path", "")) for item in items]
            item_rows += len(items)
            anchors = [
                row
                for row in fixture.get("expected_anchors", [])
                if isinstance(row, dict) and row.get("path")
            ]
            expected_paths = {str(row.get("path", "")) for row in anchors}
            for item in items:
                if item.get("detail_lookup"):
                    detail_lookup_count += 1
                if str(item.get("path", "")) not in expected_paths:
                    noise_rows += 1
            for anchor in anchors:
                path = str(anchor.get("path", ""))
                required = bool(anchor.get("required", True))
                if required:
                    total_required += 1
                rank = paths.index(path) + 1 if path in paths else 0
                if rank:
                    first_ranks.append(rank)
                    if required and rank <= 3:
                        hits_at_3 += 1
                    if required and rank <= 5:
                        hits_at_5 += 1
                elif required:
                    required_omitted += 1
                    regressions.append(
                        {
                            "task": task,
                            "path": path,
                            "reason": "required_anchor_omitted",
                        }
                    )
            for stale in fixture.get("must_not_include", []):
                if isinstance(stale, dict) and str(stale.get("path", "")) in paths:
                    stale_hits += 1
                    regressions.append(
                        {
                            "task": task,
                            "path": stale.get("path", ""),
                            "reason": stale.get("reason", "must_not_include"),
                        }
                    )
        recall_base = max(1, total_required)
        detail_base = max(1, item_rows)
        return {
            "schema": "context_quality_eval.v1",
            "fixtures": len(fixtures),
            "metrics": {
                "anchor_recall_at_3": round(hits_at_3 / recall_base, 4),
                "anchor_recall_at_5": round(hits_at_5 / recall_base, 4),
                "first_anchor_rank_avg": round(sum(first_ranks) / len(first_ranks), 4)
                if first_ranks
                else 0.0,
                "noise_ratio": round(noise_rows / detail_base, 4),
                "required_anchor_omitted_count": required_omitted,
                "detail_lookup_resolution_rate": round(detail_lookup_count / detail_base, 4),
                "stale_context_rate": round(stale_hits / max(1, len(fixtures)), 4),
            },
            "regressions": regressions,
        }

    def _gold_anchor_fixtures(self, max_entries: int) -> list[dict[str, Any]]:
        fixture_dir = self.config.repo_path / "benchmarks" / "gold_anchors"
        if not fixture_dir.is_dir():
            return []
        fixtures = []
        for path in sorted(fixture_dir.glob("*.json"))[: max(0, int(max_entries))]:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            rows = payload if isinstance(payload, list) else [payload]
            fixtures.extend(row for row in rows if isinstance(row, dict))
        return fixtures[: max(0, int(max_entries))]

    def _cache_plan(self, max_output_chars: int | None = None) -> dict[str, Any]:
        budget = int(max_output_chars or self._budget()["max_output_chars"])
        return {
            "schema": "context_budget_plan.v1",
            "max_output_tokens": max(1, budget // 4),
            "reserved": {"envelope": 120, "references": 80, "diagnostics": 80},
            "items": {
                "max_count": 6,
                "max_tokens_each": 180,
                "mode": "summary_first",
            },
            "defer": {
                "raw_snippets": True,
                "metrics": True,
                "stage_timings": True,
                "cache_details": True,
            },
        }

    def _profile_calibrate(self) -> dict[str, Any]:
        return {
            "schema": "context_profile_calibration.v1",
            "defaults": {
                "client_profile": "generic",
                "model_profile": "unknown",
                "evidence_policy": "summary_first",
                "diagnostics": "summary",
                "include_request_prompt": False,
                "include_runtime_metadata": False,
                "cache_strategy": "stable",
            },
            "profiles": {
                "codex": {"output_profile": "minimal", "diagnostics": "summary"},
                "claude": {"output_profile": "compact", "diagnostics": "summary"},
                "copilot": {"output_profile": "compact", "tools_only": True},
                "generic": {"output_profile": self._budget()["default_output_profile"]},
            },
        }

    def _resource_proxy(
        self, path: str = ".", max_output_chars: int | None = None
    ) -> dict[str, Any]:
        resource = (path or "repo://summary").strip()
        if resource in {".", "repo://summary"}:
            payload: Any = self.index.workspace_facts()
        elif resource == "repo://metrics":
            payload = self.metrics.snapshot()
        elif resource == "repo://instructions/codex-context-pack-first":
            payload = json.loads(self.codex_guidance_resource())
        elif resource.startswith("repo://tree"):
            tree_path = resource.removeprefix("repo://tree").strip("/") or "."
            payload = self.index.tree(path=tree_path, max_entries=100)
        else:
            payload = self.index.snippet(
                path=resource.removeprefix("repo://file/"),
                start_line=1,
                max_chars=max_output_chars or self.config.max_output_chars,
            )
        return {
            "schema": "context_resource_proxy.v1",
            "resource": resource,
            "payload": payload,
            "tools_only": True,
        }

    def _context_pack_benchmark(self, max_files: int = 5000) -> dict[str, Any]:
        started = time.perf_counter()
        if not self.index.status().get("file_count"):
            self.index.refresh(max_files=max_files)
        focus_paths = self._benchmark_focus_paths()
        prompt = self._benchmark_prompt(focus_paths)
        variation_prompt = self._benchmark_prompt_variation(focus_paths)
        runs = []
        for name, run_prompt, refresh_index, max_items in [
            ("cold_refresh", prompt, True, 4),
            ("warm_cache", prompt, False, 4),
            ("repeated_prompt", prompt, False, 4),
            ("prompt_variation_reuse", variation_prompt, False, 4),
            ("compact_focus", prompt, False, 2),
        ]:
            pack = self.context_pack(
                prompt=run_prompt,
                focus_paths=focus_paths,
                max_items=max_items,
                refresh_index=refresh_index,
                output_profile="compact",
                index_max_files=max_files,
            )
            runs.append(
                {
                    "name": name,
                    "prompt": run_prompt,
                    "cache_hit": bool(pack["cache"]["hit"]),
                    "cache_reason": pack["cache"]["reason"],
                    "fragment_hits": int(pack["cache"].get("fragment_hits", 0)),
                    "fragment_misses": int(pack["cache"].get("fragment_misses", 0)),
                    "fragment_hit_ratio": float(
                        pack["cache"].get("fragment_hit_ratio", 0.0)
                    ),
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
        rows: list[dict[str, Any]] = []
        for query in seed_queries[:query_limit]:
            try:
                result, cache = self._cached_search(
                    query=query,
                    path=path,
                    max_results=max_results,
                    include_globs=None,
                    public_namespace="context_lookup.search",
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
            rows.append(
                {
                    "query": query,
                    "path": path,
                    "cache_hit": bool(cache.get("hit")),
                    "cache_reason": str(cache.get("reason") or "miss"),
                    "fragment_hits": int(cache.get("fragment_hits", 0) or 0),
                    "fragment_misses": int(cache.get("fragment_misses", 0) or 0),
                    "result_count": int(result.get("count", 0) or 0),
                    "key": str(cache.get("key", "")),
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

    def _benchmark_prompt_variation(self, focus_paths: list[str]) -> str:
        if not focus_paths:
            return "inspect repository context retrieval cache behavior"
        terms = " ".join(
            path.replace("/", " ").replace(".", " ") for path in focus_paths[:3]
        )
        return f"inspect {terms} retrieval cache and implementation details"

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
                    "created_at": value.get("created_at", "")
                    if isinstance(value, dict)
                    else "",
                    "updated_at": value.get("updated_at", "")
                    if isinstance(value, dict)
                    else "",
                    "expires_at": value.get("expires_at", "")
                    if isinstance(value, dict)
                    else "",
                    "status": value.get("status", "") if isinstance(value, dict) else "",
                    "namespace": value.get("namespace", "")
                    if isinstance(value, dict)
                    else "",
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
            "created_at": value.get("created_at", "") if isinstance(value, dict) else "",
            "updated_at": value.get("updated_at", "") if isinstance(value, dict) else "",
            "expires_at": value.get("expires_at", "")
            if isinstance(value, dict)
            else "",
            "namespace": value.get("namespace", "") if isinstance(value, dict) else "",
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
            if default_output_profile not in OUTPUT_PROFILES:
                raise ValueError("default_output_profile must be minimal, compact, normal, or verbose")
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
                "schema": "context_cache.entry.v2",
                "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                "created_at": updated_at,
                "updated_at": updated_at,
                "expires_at": expires_at,
                "ttl_seconds": max(1, int(ttl_seconds)),
                "status": "active",
                "namespace": namespace,
                "key": key,
                "metadata": {
                    **(metadata or {}),
                    "project_id": self.config.project_id,
                    "schema_version": CACHE_ENTRY_SCHEMA_VERSION,
                },
                "value": sanitized_value,
                "sensitivity": sensitivity,
            },
        )

    def _cache_prune_if_due(
        self,
        interval_seconds: int = DEFAULT_CACHE_PRUNE_INTERVAL_SECONDS,
    ) -> dict[str, Any]:
        now = time.time()
        row = self.store.get_json(CACHE_LAST_PRUNED_KEY)
        last_pruned = 0.0
        if isinstance(row, dict):
            try:
                last_pruned = float(row.get("timestamp", 0.0) or 0.0)
            except (TypeError, ValueError):
                last_pruned = 0.0
        if last_pruned and now - last_pruned < interval_seconds:
            return {
                "schema": "context_cache.prune_if_due.v1",
                "pruned": False,
                "reason": "not_due",
            }
        result = self._cache_prune(DEFAULT_CACHE_MAX_AGE_MINUTES)
        self.store.put_json(
            CACHE_LAST_PRUNED_KEY,
            {
                "schema": "context_cache.last_pruned.v1",
                "timestamp": now,
                "updated_at": now_iso(),
                "interval_seconds": interval_seconds,
                "result": result,
            },
        )
        return {
            "schema": "context_cache.prune_if_due.v1",
            "pruned": True,
            **result,
        }

    def _cache_prune_if_due_best_effort(self) -> None:
        try:
            self._enqueue_background_cache_prune()
        except Exception:
            return

    def _cache_stats(self) -> dict[str, Any]:
        entries = [
            entry
            for entry in self.store.iter_json("cache:")
            if entry[0] != CACHE_LAST_PRUNED_KEY
        ]
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
                    "legacy_count": 0,
                    "sample_keys": [],
                    "hits": 0,
                    "misses": 0,
                    "hit_ratio": 0.0,
                },
            )
            stats["entry_count"] = int(stats["entry_count"]) + 1
            status_key = f"{row_status['status']}_count"
            stats[status_key] = int(stats.get(status_key, 0)) + 1
            if str(row_status.get("reason", "")).startswith("legacy_"):
                stats["legacy_count"] = int(stats.get("legacy_count", 0)) + 1
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
                    "legacy_count": 0,
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
                if key == CACHE_LAST_PRUNED_KEY:
                    continue
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
            "entry_count": len(
                [
                    key
                    for key, _row in self.store.iter_json("cache:")
                    if key != CACHE_LAST_PRUNED_KEY
                ]
            ),
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
            return "no_compatible_entry"
        row_metadata = [
            row.get("metadata", {}) if isinstance(row.get("metadata", {}), dict) else {}
            for row in rows
        ]
        expected_schema_version = int(metadata.get("schema_version", 0) or 0)
        if expected_schema_version and not any(
            int(row.get("schema_version", 0) or 0) >= expected_schema_version
            for row in row_metadata
        ):
            return "schema_version_changed"
        expected_signature = str(metadata.get("refresh_signature", ""))
        signature_rows = row_metadata
        if expected_signature:
            signature_rows = [
                row
                for row in row_metadata
                if str(row.get("refresh_signature", "")) == expected_signature
            ]
            if not signature_rows:
                return "index_changed"
        expected_paths = metadata.get("explicit_paths")
        if expected_paths is not None:
            canonical_paths = list(expected_paths)
            if not any(row.get("explicit_paths") == canonical_paths for row in signature_rows):
                return "path_changed"
        expected_path = str(metadata.get("path", ""))
        if expected_path and not any(
            str(row.get("path", "")) == expected_path for row in signature_rows
        ):
            return "path_changed"
        expected_fingerprint = metadata.get("file_fingerprint")
        if isinstance(expected_fingerprint, dict):
            expected_token = str(expected_fingerprint.get("cache_token", ""))
            if expected_token and not any(
                str(
                    (row.get("file_fingerprint", {}) or {}).get("cache_token", "")
                )
                == expected_token
                for row in signature_rows
            ):
                return "changed_chunk_digest"
        expected_chunk_id = str(metadata.get("chunk_id", ""))
        if expected_chunk_id and not any(
            str(row.get("chunk_id", "")) == expected_chunk_id for row in signature_rows
        ):
            return "changed_chunk_digest"
        expected_globs = metadata.get("include_globs")
        if expected_globs is not None and not any(
            row.get("include_globs") == expected_globs for row in signature_rows
        ):
            return "path_changed"
        expected_terms_key = str(metadata.get("terms_key", ""))
        if expected_terms_key and not any(
            str(row.get("terms_key", "")) == expected_terms_key
            for row in signature_rows
        ):
            return "terms_changed"
        expected_term = str(metadata.get("term", ""))
        if expected_term and not any(
            str(row.get("term", "")) == expected_term for row in signature_rows
        ):
            return "terms_changed"
        expected_pool_size = metadata.get("pool_size")
        if expected_pool_size is not None and not any(
            int(row.get("pool_size", 0) or 0) == int(expected_pool_size)
            for row in signature_rows
        ):
            return "limit_bucket_changed"
        return "no_compatible_entry"

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
        namespace = str(row.get("namespace", ""))
        metadata = row.get("metadata", {})
        metadata = metadata if isinstance(metadata, dict) else {}
        if namespace in FRAGMENT_CACHE_NAMESPACES:
            schema_version = int(
                row.get("schema_version")
                or metadata.get("schema_version")
                or 0
            )
            if schema_version < CACHE_ENTRY_SCHEMA_VERSION:
                return {
                    "status": "stale",
                    "reason": "legacy_schema_version",
                    "warnings": [
                        {
                            "code": "cache_legacy",
                            "message": "cache row predates current schema version",
                        }
                    ],
                }
            if not str(metadata.get("refresh_signature", "")):
                return {
                    "status": "stale",
                    "reason": "legacy_missing_refresh_signature",
                    "warnings": [
                        {
                            "code": "cache_legacy",
                            "message": "cache row has no refresh signature",
                        }
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
