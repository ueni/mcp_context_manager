from __future__ import annotations

from typing import Any

from .config import ContextConfig
from .schemas import contract_size_metrics
from .store import ContextStore
from .token_counter import TokenCounter
from .util import now_iso

MEASUREMENT_TARGETS: tuple[dict[str, Any], ...] = (
    {
        "key": "latency.context_pack.avg_elapsed_ms",
        "operator": "<=",
        "target": 750.0,
        "unit": "ms",
        "min_samples": 1,
    },
    {
        "key": "latency.context_pack.p95_recent_ms",
        "operator": "<=",
        "target": 1500.0,
        "unit": "ms",
        "min_samples": 3,
    },
    {
        "key": "latency.context_pack.index_refresh_avg_ms",
        "operator": "<=",
        "target": 250.0,
        "unit": "ms",
        "min_samples": 1,
    },
    {
        "key": "latency.context_pack.snippet_batch_avg_ms",
        "operator": "<=",
        "target": 250.0,
        "unit": "ms",
        "min_samples": 1,
    },
    {
        "key": "tokens.context_pack.avg_saved_per_pack",
        "operator": ">=",
        "target": 500.0,
        "unit": "tokens",
        "min_samples": 1,
    },
    {
        "key": "tokens.context_pack.avg_tokens_spared_by_mcp_per_pack",
        "operator": ">=",
        "target": 500.0,
        "unit": "tokens",
        "min_samples": 1,
    },
    {
        "key": "tokens.context_pack.compression_ratio",
        "operator": "<=",
        "target": 0.7,
        "unit": "ratio",
        "min_samples": 1,
    },
    {
        "key": "retrieval.context_pack.candidates_per_selected",
        "operator": "<=",
        "target": 8.0,
        "unit": "ratio",
        "min_samples": 1,
    },
    {
        "key": "cache.hit_ratio",
        "operator": ">=",
        "target": 0.2,
        "unit": "ratio",
        "min_samples": 2,
    },
    {
        "key": "cache.context_pack_retrieval_hit_ratio",
        "operator": ">=",
        "target": 0.2,
        "unit": "ratio",
        "min_samples": 2,
    },
    {
        "key": "cache.context_pack_fragment_hit_ratio",
        "operator": ">=",
        "target": 0.2,
        "unit": "ratio",
        "min_samples": 2,
    },
    {
        "key": "tooling.external_calls_saved_per_pack",
        "operator": ">=",
        "target": 2.0,
        "unit": "calls",
        "min_samples": 1,
    },
    {
        "key": "tooling.contract_tokens_saved_est",
        "operator": ">=",
        "target": 1.0,
        "unit": "tokens",
        "min_samples": 1,
    },
    {
        "key": "references.bytes_deferred_est",
        "operator": ">=",
        "target": 1.0,
        "unit": "bytes",
        "min_samples": 1,
    },
)


class ContextMetrics:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)

    def record_event(
        self,
        operation: str,
        elapsed_ms: float,
        cache_hit: bool | None = None,
        cache_namespace: str = "",
        cache_reason: str = "",
        estimated_input_tokens_saved: int = 0,
        baseline_input_tokens_est: int = 0,
        output_tokens_est: int = 0,
        raw_candidate_chars: int = 0,
        selected_chars: int = 0,
        external_tool_calls_saved: int = 0,
        references_bytes_deferred_est: int = 0,
        result_count: int = 0,
        candidate_count: int = 0,
        omitted_count: int = 0,
        route: str = "",
        stage_timings_ms: dict[str, float] | None = None,
        fragment_cache_hits: int = 0,
        fragment_cache_misses: int = 0,
    ) -> None:
        payload = self._load()
        now = now_iso()
        payload["updated_at"] = now

        totals = payload.setdefault("totals", {})
        totals["request_count"] = int(totals.get("request_count", 0)) + 1
        totals["estimated_input_tokens_saved"] = int(
            totals.get("estimated_input_tokens_saved", 0)
        ) + max(0, int(estimated_input_tokens_saved))
        totals["tokens_spared_by_mcp_est"] = int(
            totals.get("tokens_spared_by_mcp_est", 0)
        ) + max(0, int(estimated_input_tokens_saved))
        totals["baseline_input_tokens_est"] = int(
            totals.get("baseline_input_tokens_est", 0)
        ) + max(0, int(baseline_input_tokens_est))
        totals["output_tokens_est"] = int(totals.get("output_tokens_est", 0)) + max(
            0, int(output_tokens_est)
        )
        totals["raw_candidate_chars"] = int(
            totals.get("raw_candidate_chars", 0)
        ) + max(0, int(raw_candidate_chars))
        totals["selected_chars"] = int(totals.get("selected_chars", 0)) + max(
            0, int(selected_chars)
        )
        totals["external_tool_calls_saved"] = int(
            totals.get("external_tool_calls_saved", 0)
        ) + max(0, int(external_tool_calls_saved))
        totals["references_bytes_deferred_est"] = int(
            totals.get("references_bytes_deferred_est", 0)
        ) + max(0, int(references_bytes_deferred_est))
        totals["result_count"] = int(totals.get("result_count", 0)) + max(
            0, int(result_count)
        )
        totals["candidate_count"] = int(totals.get("candidate_count", 0)) + max(
            0, int(candidate_count)
        )
        totals["omitted_count"] = int(totals.get("omitted_count", 0)) + max(
            0, int(omitted_count)
        )

        if cache_hit is True:
            totals["cache_hits"] = int(totals.get("cache_hits", 0)) + 1
        elif cache_hit is False:
            totals["cache_misses"] = int(totals.get("cache_misses", 0)) + 1
        if cache_hit is not None:
            reason = cache_reason or ("hit" if cache_hit else "miss")
            namespace = cache_namespace or operation
            reason_counts = totals.setdefault("cache_reason_counts", {})
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
            namespace_stats = totals.setdefault("cache_namespaces", {}).setdefault(
                namespace,
                {
                    "hits": 0,
                    "misses": 0,
                    "reasons": {},
                },
            )
            if cache_hit:
                namespace_stats["hits"] = int(namespace_stats.get("hits", 0)) + 1
            else:
                namespace_stats["misses"] = int(namespace_stats.get("misses", 0)) + 1
            namespace_reasons = namespace_stats.setdefault("reasons", {})
            namespace_reasons[reason] = int(namespace_reasons.get(reason, 0)) + 1
        fragment_hits = max(0, int(fragment_cache_hits))
        fragment_misses = max(0, int(fragment_cache_misses))
        if fragment_hits or fragment_misses:
            totals["context_pack_fragment_cache_hits"] = int(
                totals.get("context_pack_fragment_cache_hits", 0)
            ) + fragment_hits
            totals["context_pack_fragment_cache_misses"] = int(
                totals.get("context_pack_fragment_cache_misses", 0)
            ) + fragment_misses

        op_stats = payload.setdefault("operations", {}).setdefault(
            operation,
            {
                "count": 0,
                "total_elapsed_ms": 0.0,
                "min_elapsed_ms": None,
                "max_elapsed_ms": 0.0,
                "result_count": 0,
                "candidate_count": 0,
                "omitted_count": 0,
                "estimated_input_tokens_saved": 0,
                "tokens_spared_by_mcp_est": 0,
                "baseline_input_tokens_est": 0,
                "output_tokens_est": 0,
                "raw_candidate_chars": 0,
                "selected_chars": 0,
                "external_tool_calls_saved": 0,
                "references_bytes_deferred_est": 0,
            },
        )
        self._update_stats(
            op_stats,
            elapsed_ms=elapsed_ms,
            estimated_input_tokens_saved=estimated_input_tokens_saved,
            baseline_input_tokens_est=baseline_input_tokens_est,
            output_tokens_est=output_tokens_est,
            raw_candidate_chars=raw_candidate_chars,
            selected_chars=selected_chars,
            external_tool_calls_saved=external_tool_calls_saved,
            references_bytes_deferred_est=references_bytes_deferred_est,
            result_count=result_count,
            candidate_count=candidate_count,
            omitted_count=omitted_count,
        )
        if stage_timings_ms:
            stage_payload = payload.setdefault("stages", {}).setdefault(operation, {})
            for stage, stage_elapsed_ms in sorted(stage_timings_ms.items()):
                self._update_latency_stats(
                    stage_payload.setdefault(
                        stage,
                        {
                            "count": 0,
                            "total_elapsed_ms": 0.0,
                            "min_elapsed_ms": None,
                            "max_elapsed_ms": 0.0,
                        },
                    ),
                    elapsed_ms=float(stage_elapsed_ms),
                )

        if route:
            route_stats = payload.setdefault("routes", {}).setdefault(
                route,
                {
                    "count": 0,
                    "total_elapsed_ms": 0.0,
                    "min_elapsed_ms": None,
                    "max_elapsed_ms": 0.0,
                    "result_count": 0,
                    "candidate_count": 0,
                    "omitted_count": 0,
                    "estimated_input_tokens_saved": 0,
                    "tokens_spared_by_mcp_est": 0,
                    "baseline_input_tokens_est": 0,
                    "output_tokens_est": 0,
                    "raw_candidate_chars": 0,
                    "selected_chars": 0,
                    "external_tool_calls_saved": 0,
                    "references_bytes_deferred_est": 0,
                },
            )
            self._update_stats(
                route_stats,
                elapsed_ms=elapsed_ms,
                estimated_input_tokens_saved=estimated_input_tokens_saved,
                baseline_input_tokens_est=baseline_input_tokens_est,
                output_tokens_est=output_tokens_est,
                raw_candidate_chars=raw_candidate_chars,
                selected_chars=selected_chars,
                external_tool_calls_saved=external_tool_calls_saved,
                references_bytes_deferred_est=references_bytes_deferred_est,
                result_count=result_count,
                candidate_count=candidate_count,
                omitted_count=omitted_count,
            )

        recent = payload.setdefault("recent", [])
        recent.append(
            {
                "operation": operation,
                "recorded_at": now,
                "elapsed_ms": round(float(elapsed_ms), 3),
                "cache_hit": cache_hit,
                "cache_namespace": cache_namespace,
                "cache_reason": cache_reason,
                "route": route,
                "result_count": max(0, int(result_count)),
                "candidate_count": max(0, int(candidate_count)),
                "omitted_count": max(0, int(omitted_count)),
                "estimated_input_tokens_saved": max(
                    0, int(estimated_input_tokens_saved)
                ),
                "tokens_spared_by_mcp_est": max(
                    0, int(estimated_input_tokens_saved)
                ),
                "baseline_input_tokens_est": max(0, int(baseline_input_tokens_est)),
                "output_tokens_est": max(0, int(output_tokens_est)),
                "external_tool_calls_saved": max(0, int(external_tool_calls_saved)),
                "references_bytes_deferred_est": max(
                    0, int(references_bytes_deferred_est)
                ),
                "fragment_cache_hits": fragment_hits,
                "fragment_cache_misses": fragment_misses,
                "stage_timings_ms": {
                    key: round(float(value), 3)
                    for key, value in sorted((stage_timings_ms or {}).items())
                },
            }
        )
        payload["recent"] = recent[-50:]
        self._save(payload)

    def snapshot(self, recent_limit: int = 12) -> dict[str, Any]:
        payload = self._load()
        totals = payload.get("totals", {})
        cache_hits = int(totals.get("cache_hits", 0))
        cache_misses = int(totals.get("cache_misses", 0))
        cache_total = cache_hits + cache_misses
        fragment_hits = int(totals.get("context_pack_fragment_cache_hits", 0))
        fragment_misses = int(totals.get("context_pack_fragment_cache_misses", 0))
        fragment_total = fragment_hits + fragment_misses
        cache_namespaces = {
            name: self._public_cache_namespace(stats)
            for name, stats in sorted(
                totals.get("cache_namespaces", {}).items()
            )
            if isinstance(stats, dict)
        }
        operations = {
            name: self._public_stats(stats)
            for name, stats in sorted(payload.get("operations", {}).items())
        }
        routes = {
            name: self._public_stats(stats)
            for name, stats in sorted(payload.get("routes", {}).items())
        }
        stages = {
            operation: {
                stage: self._public_latency_stats(stats)
                for stage, stats in sorted(stage_rows.items())
            }
            for operation, stage_rows in sorted(payload.get("stages", {}).items())
            if isinstance(stage_rows, dict)
        }
        pack_count = int(operations.get("context_pack", {}).get("count", 0))
        tokens_saved = int(totals.get("estimated_input_tokens_saved", 0))
        baseline_tokens = int(totals.get("baseline_input_tokens_est", 0))
        output_tokens = int(totals.get("output_tokens_est", 0))
        result_count = int(totals.get("result_count", 0))
        candidate_count = int(totals.get("candidate_count", 0))
        external_calls_saved = int(totals.get("external_tool_calls_saved", 0))
        references_bytes_deferred = int(
            totals.get("references_bytes_deferred_est", 0)
        )
        contract_metrics = contract_size_metrics()
        return {
            "schema": "context_metrics.v1",
            "generated_at": now_iso(),
            "project_id": self.config.project_id,
            "since": payload.get("created_at", ""),
            "updated_at": payload.get("updated_at", ""),
            "requests": {
                "total": int(totals.get("request_count", 0)),
                "by_operation": operations,
                "by_route": routes,
            },
            "cache": {
                "hits": cache_hits,
                "misses": cache_misses,
                "hit_ratio": round(cache_hits / cache_total, 4)
                if cache_total
                else 0.0,
                "context_pack_fragment_hits": fragment_hits,
                "context_pack_fragment_misses": fragment_misses,
                "context_pack_fragment_hit_ratio": round(
                    fragment_hits / fragment_total, 4
                )
                if fragment_total
                else 0.0,
                "reasons": dict(sorted(totals.get("cache_reason_counts", {}).items())),
                "by_namespace": cache_namespaces,
            },
            "retrieval": {
                "result_count": result_count,
                "candidate_count": candidate_count,
                "omitted_count": int(totals.get("omitted_count", 0)),
                "candidates_per_selected": round(candidate_count / result_count, 3)
                if result_count
                else 0.0,
            },
            "tokens": {
                "estimated_input_tokens_saved": tokens_saved,
                "tokens_spared_by_mcp_est": tokens_saved,
                "baseline_input_tokens_est": baseline_tokens,
                "output_tokens_est": output_tokens,
                "token_counting": TokenCounter(
                    mode=self.config.token_counter_mode,
                    target_tokenizer=self.config.target_tokenizer,
                ).metadata(),
                "avg_estimated_input_tokens_saved_per_pack": round(
                    tokens_saved / pack_count, 3
                )
                if pack_count
                else 0.0,
                "avg_tokens_spared_by_mcp_est_per_pack": round(
                    tokens_saved / pack_count, 3
                )
                if pack_count
                else 0.0,
                "tokens_spared_by_mcp_reason": (
                    "context_pack returns compact selected context and defers "
                    "full evidence behind local references instead of sending "
                    "all ranked candidate evidence."
                ),
                "avg_baseline_input_tokens_per_pack": round(
                    baseline_tokens / pack_count, 3
                )
                if pack_count
                else 0.0,
                "avg_output_tokens_per_pack": round(output_tokens / pack_count, 3)
                if pack_count
                else 0.0,
                "compression_ratio": round(output_tokens / baseline_tokens, 4)
                if baseline_tokens
                else 0.0,
            },
            "tooling": {
                "external_tool_calls_saved_est": external_calls_saved,
                "avg_external_tool_calls_saved_per_pack": round(
                    external_calls_saved / pack_count, 3
                )
                if pack_count
                else 0.0,
                "contract_chars": int(contract_metrics["contract_chars"]),
                "contract_tokens_est": int(contract_metrics["contract_tokens_est"]),
                "compact_contract_tokens_saved_est": int(
                    contract_metrics["compact_contract_tokens_saved_est"]
                ),
                "contract_tokens_saved_est": int(
                    contract_metrics["compact_contract_tokens_saved_est"]
                ),
            },
            "references": {
                "bytes_deferred_est": references_bytes_deferred,
                "avg_bytes_deferred_per_pack": round(
                    references_bytes_deferred / pack_count, 3
                )
                if pack_count
                else 0.0,
            },
            "benchmarks": {
                "latency_ms_by_operation": {
                    name: {
                        "count": stats["count"],
                        "avg": stats["avg_elapsed_ms"],
                        "min": stats["min_elapsed_ms"],
                        "max": stats["max_elapsed_ms"],
                    }
                    for name, stats in operations.items()
                },
                "stage_latency_ms_by_operation": stages,
                "recent": payload.get("recent", [])[-recent_limit:],
            },
        }

    def measurement_matrix(
        self, snapshot: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        payload = (
            snapshot if isinstance(snapshot, dict) else self.snapshot(recent_limit=50)
        )
        return {
            "schema": "context_measurement_matrix.v1",
            "generated_at": now_iso(),
            "project_id": self.config.project_id,
            "checks": [
                self._measurement_check(target, payload)
                for target in MEASUREMENT_TARGETS
            ],
            "metric_sources": {
                "runtime_metrics": "context_admin(mode='metrics')",
                "benchmark_runner": "context_admin(mode='benchmark')",
                "contract_metrics": "context_admin(mode='contracts', contract_profile='compact')",
                "token_savings_formula": (
                    "max(0, baseline_input_tokens_est - output_tokens_est)"
                ),
                "tokens_spared_by_mcp_formula": (
                    "max(0, baseline_input_tokens_est - output_tokens_est)"
                ),
            },
        }

    def _load(self) -> dict[str, Any]:
        payload = self.store.get_json(
            "metrics:store",
            {
                "schema": "context_metrics.store.v1",
                "created_at": now_iso(),
                "updated_at": "",
                "totals": {},
                "operations": {},
                "routes": {},
                "stages": {},
                "recent": [],
            },
        )
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("schema", "context_metrics.store.v1")
        payload.setdefault("created_at", now_iso())
        payload.setdefault("updated_at", "")
        payload.setdefault("totals", {})
        payload.setdefault("operations", {})
        payload.setdefault("routes", {})
        payload.setdefault("stages", {})
        payload.setdefault("recent", [])
        return payload

    def _save(self, payload: dict[str, Any]) -> None:
        self.store.put_json("metrics:store", payload)

    def _update_stats(
        self,
        stats: dict[str, Any],
        elapsed_ms: float,
        estimated_input_tokens_saved: int,
        baseline_input_tokens_est: int,
        output_tokens_est: int,
        raw_candidate_chars: int,
        selected_chars: int,
        external_tool_calls_saved: int,
        references_bytes_deferred_est: int,
        result_count: int,
        candidate_count: int,
        omitted_count: int,
    ) -> None:
        self._update_latency_stats(stats, elapsed_ms=elapsed_ms)
        stats["result_count"] = int(stats.get("result_count", 0)) + max(
            0, int(result_count)
        )
        stats["candidate_count"] = int(stats.get("candidate_count", 0)) + max(
            0, int(candidate_count)
        )
        stats["omitted_count"] = int(stats.get("omitted_count", 0)) + max(
            0, int(omitted_count)
        )
        stats["estimated_input_tokens_saved"] = int(
            stats.get("estimated_input_tokens_saved", 0)
        ) + max(0, int(estimated_input_tokens_saved))
        stats["tokens_spared_by_mcp_est"] = int(
            stats.get("tokens_spared_by_mcp_est", 0)
        ) + max(0, int(estimated_input_tokens_saved))
        stats["baseline_input_tokens_est"] = int(
            stats.get("baseline_input_tokens_est", 0)
        ) + max(0, int(baseline_input_tokens_est))
        stats["output_tokens_est"] = int(stats.get("output_tokens_est", 0)) + max(
            0, int(output_tokens_est)
        )
        stats["raw_candidate_chars"] = int(stats.get("raw_candidate_chars", 0)) + max(
            0, int(raw_candidate_chars)
        )
        stats["selected_chars"] = int(stats.get("selected_chars", 0)) + max(
            0, int(selected_chars)
        )
        stats["external_tool_calls_saved"] = int(
            stats.get("external_tool_calls_saved", 0)
        ) + max(0, int(external_tool_calls_saved))
        stats["references_bytes_deferred_est"] = int(
            stats.get("references_bytes_deferred_est", 0)
        ) + max(0, int(references_bytes_deferred_est))

    def _update_latency_stats(self, stats: dict[str, Any], elapsed_ms: float) -> None:
        elapsed = round(float(elapsed_ms), 3)
        count = int(stats.get("count", 0)) + 1
        stats["count"] = count
        stats["total_elapsed_ms"] = round(
            float(stats.get("total_elapsed_ms", 0.0)) + elapsed, 3
        )
        min_elapsed = stats.get("min_elapsed_ms")
        stats["min_elapsed_ms"] = (
            elapsed if min_elapsed is None else min(float(min_elapsed), elapsed)
        )
        stats["max_elapsed_ms"] = max(float(stats.get("max_elapsed_ms", 0.0)), elapsed)

    def _public_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        public = self._public_latency_stats(stats)
        result_count = int(stats.get("result_count", 0))
        candidate_count = int(stats.get("candidate_count", 0))
        baseline_tokens = int(stats.get("baseline_input_tokens_est", 0))
        output_tokens = int(stats.get("output_tokens_est", 0))
        public.update(
            {
                "result_count": result_count,
                "candidate_count": candidate_count,
                "omitted_count": int(stats.get("omitted_count", 0)),
                "estimated_input_tokens_saved": int(
                    stats.get("estimated_input_tokens_saved", 0)
                ),
                "tokens_spared_by_mcp_est": int(
                    stats.get("estimated_input_tokens_saved", 0)
                ),
                "baseline_input_tokens_est": baseline_tokens,
                "output_tokens_est": output_tokens,
                "raw_candidate_chars": int(stats.get("raw_candidate_chars", 0)),
                "selected_chars": int(stats.get("selected_chars", 0)),
                "compression_ratio": round(output_tokens / baseline_tokens, 4)
                if baseline_tokens
                else 0.0,
                "candidates_per_selected": round(candidate_count / result_count, 3)
                if result_count
                else 0.0,
                "external_tool_calls_saved": int(
                    stats.get("external_tool_calls_saved", 0)
                ),
                "references_bytes_deferred_est": int(
                    stats.get("references_bytes_deferred_est", 0)
                ),
            }
        )
        return public

    def _public_cache_namespace(self, stats: dict[str, Any]) -> dict[str, Any]:
        hits = int(stats.get("hits", 0) or 0)
        misses = int(stats.get("misses", 0) or 0)
        total = hits + misses
        return {
            "hits": hits,
            "misses": misses,
            "hit_ratio": round(hits / total, 4) if total else 0.0,
            "reasons": dict(sorted(stats.get("reasons", {}).items())),
        }

    def _public_latency_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        count = int(stats.get("count", 0))
        total_elapsed = float(stats.get("total_elapsed_ms", 0.0))
        return {
            "count": count,
            "avg_elapsed_ms": round(total_elapsed / count, 3) if count else 0.0,
            "min_elapsed_ms": round(float(stats.get("min_elapsed_ms") or 0.0), 3),
            "max_elapsed_ms": round(float(stats.get("max_elapsed_ms", 0.0)), 3),
        }

    def _measurement_check(
        self, target: dict[str, Any], snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        key = str(target["key"])
        current, samples = self._measurement_value(key, snapshot)
        min_samples = int(target.get("min_samples", 1))
        status = "insufficient"
        if samples >= min_samples and current is not None:
            status = (
                "pass"
                if self._passes(
                    current=float(current),
                    operator=str(target["operator"]),
                    target=float(target["target"]),
                )
                else "fail"
            )
        return {
            "key": key,
            "current": current,
            "operator": target["operator"],
            "target": target["target"],
            "unit": target["unit"],
            "samples": samples,
            "min_samples": min_samples,
            "status": status,
        }

    def _measurement_value(
        self, key: str, snapshot: dict[str, Any]
    ) -> tuple[float | None, int]:
        operations = snapshot.get("requests", {}).get("by_operation", {})
        pack = operations.get("context_pack", {})
        pack_count = int(pack.get("count", 0) or 0)
        if key == "latency.context_pack.avg_elapsed_ms":
            return float(pack.get("avg_elapsed_ms", 0.0)), pack_count
        if key == "latency.context_pack.p95_recent_ms":
            values = [
                float(row.get("elapsed_ms", 0.0))
                for row in snapshot.get("benchmarks", {}).get("recent", [])
                if row.get("operation") == "context_pack"
            ]
            return self._percentile(values, 95), len(values)
        if key == "latency.context_pack.index_refresh_avg_ms":
            stage = (
                snapshot.get("benchmarks", {})
                .get("stage_latency_ms_by_operation", {})
                .get("context_pack", {})
                .get("index_refresh_ms", {})
            )
            return float(stage.get("avg_elapsed_ms", 0.0)), int(
                stage.get("count", 0) or 0
            )
        if key == "latency.context_pack.snippet_batch_avg_ms":
            stage = (
                snapshot.get("benchmarks", {})
                .get("stage_latency_ms_by_operation", {})
                .get("context_pack", {})
                .get("snippet_batch_ms", {})
            )
            return float(stage.get("avg_elapsed_ms", 0.0)), int(
                stage.get("count", 0) or 0
            )
        if key == "tokens.context_pack.avg_saved_per_pack":
            return (
                float(
                    snapshot.get("tokens", {}).get(
                        "avg_estimated_input_tokens_saved_per_pack", 0.0
                    )
                ),
                pack_count,
            )
        if key == "tokens.context_pack.avg_tokens_spared_by_mcp_per_pack":
            return (
                float(
                    snapshot.get("tokens", {}).get(
                        "avg_tokens_spared_by_mcp_est_per_pack", 0.0
                    )
                ),
                pack_count,
            )
        if key == "tokens.context_pack.compression_ratio":
            return float(snapshot.get("tokens", {}).get("compression_ratio", 0.0)), pack_count
        if key == "retrieval.context_pack.candidates_per_selected":
            return (
                float(pack.get("candidates_per_selected", 0.0)),
                pack_count,
            )
        if key == "cache.hit_ratio":
            cache = snapshot.get("cache", {})
            samples = int(cache.get("hits", 0) or 0) + int(cache.get("misses", 0) or 0)
            return float(cache.get("hit_ratio", 0.0)), samples
        if key == "cache.context_pack_retrieval_hit_ratio":
            namespace = (
                snapshot.get("cache", {})
                .get("by_namespace", {})
                .get("context_pack.retrieval", {})
            )
            samples = int(namespace.get("hits", 0) or 0) + int(
                namespace.get("misses", 0) or 0
            )
            return float(namespace.get("hit_ratio", 0.0)), samples
        if key == "cache.context_pack_fragment_hit_ratio":
            cache = snapshot.get("cache", {})
            samples = int(cache.get("context_pack_fragment_hits", 0) or 0) + int(
                cache.get("context_pack_fragment_misses", 0) or 0
            )
            return float(cache.get("context_pack_fragment_hit_ratio", 0.0)), samples
        if key == "tooling.external_calls_saved_per_pack":
            return (
                float(
                    snapshot.get("tooling", {}).get(
                        "avg_external_tool_calls_saved_per_pack", 0.0
                    )
                ),
                pack_count,
            )
        if key == "tooling.contract_tokens_saved_est":
            value = float(
                snapshot.get("tooling", {}).get("contract_tokens_saved_est", 0.0)
            )
            return value, 1
        if key == "references.bytes_deferred_est":
            return (
                float(snapshot.get("references", {}).get("bytes_deferred_est", 0.0)),
                pack_count,
            )
        return None, 0

    def _passes(self, current: float, operator: str, target: float) -> bool:
        if operator == "<=":
            return current <= target
        if operator == ">=":
            return current >= target
        raise ValueError(f"unsupported measurement operator: {operator}")

    def _percentile(self, values: list[float], percentile: int) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return round(ordered[0], 3)
        rank = (len(ordered) - 1) * (percentile / 100)
        lower = int(rank)
        upper = min(lower + 1, len(ordered) - 1)
        fraction = rank - lower
        return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)
