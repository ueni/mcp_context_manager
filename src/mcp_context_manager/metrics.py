from __future__ import annotations

from typing import Any

from .config import ContextConfig
from .store import ContextStore
from .util import now_iso


class ContextMetrics:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)

    def record_event(
        self,
        operation: str,
        elapsed_ms: float,
        cache_hit: bool | None = None,
        estimated_input_tokens_saved: int = 0,
        result_count: int = 0,
        candidate_count: int = 0,
        omitted_count: int = 0,
        route: str = "",
    ) -> None:
        payload = self._load()
        now = now_iso()
        payload["updated_at"] = now

        totals = payload.setdefault("totals", {})
        totals["request_count"] = int(totals.get("request_count", 0)) + 1
        totals["estimated_input_tokens_saved"] = int(
            totals.get("estimated_input_tokens_saved", 0)
        ) + max(0, int(estimated_input_tokens_saved))
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
            },
        )
        self._update_stats(
            op_stats,
            elapsed_ms=elapsed_ms,
            estimated_input_tokens_saved=estimated_input_tokens_saved,
            result_count=result_count,
            candidate_count=candidate_count,
            omitted_count=omitted_count,
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
                },
            )
            self._update_stats(
                route_stats,
                elapsed_ms=elapsed_ms,
                estimated_input_tokens_saved=estimated_input_tokens_saved,
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
                "route": route,
                "result_count": max(0, int(result_count)),
                "candidate_count": max(0, int(candidate_count)),
                "omitted_count": max(0, int(omitted_count)),
                "estimated_input_tokens_saved": max(
                    0, int(estimated_input_tokens_saved)
                ),
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
        operations = {
            name: self._public_stats(stats)
            for name, stats in sorted(payload.get("operations", {}).items())
        }
        routes = {
            name: self._public_stats(stats)
            for name, stats in sorted(payload.get("routes", {}).items())
        }
        pack_count = int(operations.get("context_pack", {}).get("count", 0))
        tokens_saved = int(totals.get("estimated_input_tokens_saved", 0))
        return {
            "schema": "context_metrics.v1",
            "generated_at": now_iso(),
            "project_id": self.config.project_id,
            "path": self.config.display_path(self.config.store_path),
            "storage_backend": self.store.backend,
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
            },
            "retrieval": {
                "result_count": int(totals.get("result_count", 0)),
                "candidate_count": int(totals.get("candidate_count", 0)),
                "omitted_count": int(totals.get("omitted_count", 0)),
            },
            "tokens": {
                "estimated_input_tokens_saved": tokens_saved,
                "avg_estimated_input_tokens_saved_per_pack": round(
                    tokens_saved / pack_count, 3
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
                "recent": payload.get("recent", [])[-recent_limit:],
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
        payload.setdefault("recent", [])
        return payload

    def _save(self, payload: dict[str, Any]) -> None:
        self.store.put_json("metrics:store", payload)

    def _update_stats(
        self,
        stats: dict[str, Any],
        elapsed_ms: float,
        estimated_input_tokens_saved: int,
        result_count: int,
        candidate_count: int,
        omitted_count: int,
    ) -> None:
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

    def _public_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        count = int(stats.get("count", 0))
        total_elapsed = float(stats.get("total_elapsed_ms", 0.0))
        return {
            "count": count,
            "avg_elapsed_ms": round(total_elapsed / count, 3) if count else 0.0,
            "min_elapsed_ms": round(float(stats.get("min_elapsed_ms") or 0.0), 3),
            "max_elapsed_ms": round(float(stats.get("max_elapsed_ms", 0.0)), 3),
            "result_count": int(stats.get("result_count", 0)),
            "candidate_count": int(stats.get("candidate_count", 0)),
            "omitted_count": int(stats.get("omitted_count", 0)),
            "estimated_input_tokens_saved": int(
                stats.get("estimated_input_tokens_saved", 0)
            ),
        }
