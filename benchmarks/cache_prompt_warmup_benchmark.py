#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mcp_context_manager.config import ContextConfig  # noqa: E402
from mcp_context_manager.context import (  # noqa: E402
    WARMUP_AUTO_JOB_KIND,
    WARMUP_AUTO_LEARN_KEY,
    ContextService,
)

FRAGMENT_NAMESPACES = (
    "retrieval.search_term",
    "retrieval.file_summary",
    "retrieval.test_owner_paths",
)


def _wait_for_prompt_warmup(service: ContextService, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = service.store.get_json(WARMUP_AUTO_LEARN_KEY, {})
        job = service._background_status()[WARMUP_AUTO_JOB_KIND]
        if (
            isinstance(state, dict)
            and state.get("last_auto_status") in {"complete", "skipped"}
            and not job.get("pending")
        ):
            return
        time.sleep(0.01)
    raise TimeoutError("prompt-aware cache warmup did not complete")


def _selection_signature(pack: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "path": item.get("path"),
            "start_line": item.get("start_line"),
            "end_line": item.get("end_line"),
            "reason_codes": item.get("reason_codes"),
            "confidence": item.get("confidence"),
            "detail_lookup": item.get("detail_lookup"),
            "provenance": item.get("provenance"),
        }
        for item in pack.get("items", [])
        if isinstance(item, dict)
    ]


def _reference_signature(pack: dict[str, Any]) -> list[dict[str, Any]]:
    signatures = []
    for row in pack.get("references", []):
        if not isinstance(row, dict):
            continue
        resolver = row.get("resolver", {})
        resolver = resolver if isinstance(resolver, dict) else {}
        signatures.append(
            {
                "schema": row.get("schema"),
                "producer": row.get("producer"),
                "resolver": {
                    "tool": resolver.get("tool"),
                    "repo_boundary_enforced": resolver.get("repo_boundary_enforced"),
                },
            }
        )
    return signatures


def _namespace_totals(packs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    totals = {
        namespace: {"hits": 0, "misses": 0, "hit_ratio": 0.0}
        for namespace in FRAGMENT_NAMESPACES
    }
    for pack in packs:
        rows = pack.get("cache", {}).get("by_namespace", {})
        rows = rows if isinstance(rows, dict) else {}
        for namespace in FRAGMENT_NAMESPACES:
            row = rows.get(namespace, {})
            row = row if isinstance(row, dict) else {}
            totals[namespace]["hits"] += int(row.get("hits", 0) or 0)
            totals[namespace]["misses"] += int(row.get("misses", 0) or 0)
    for row in totals.values():
        sample_count = int(row["hits"]) + int(row["misses"])
        row["hit_ratio"] = (
            round(int(row["hits"]) / sample_count, 4) if sample_count else 0.0
        )
    return totals


def _load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != "cache_prompt_warmup_fixture.v1"
    ):
        raise ValueError("fixture must use cache_prompt_warmup_fixture.v1")
    return [row for row in payload.get("cases", []) if isinstance(row, dict)]


def run_benchmark(
    repo: Path,
    fixture: Path,
    state_root: Path,
    repeats: int,
    max_files: int,
    fragment_threshold: float,
    latency_improvement_threshold: float,
) -> dict[str, Any]:
    cases = _load_cases(fixture)
    case_results = []
    for case in cases:
        cold_latencies: list[float] = []
        warm_latencies: list[float] = []
        warm_packs: list[dict[str, Any]] = []
        retention: list[bool] = []
        regressions: list[dict[str, Any]] = []
        required_anchor_recall: list[float] = []
        for repeat in range(max(1, repeats)):
            case_id = str(case.get("id", "case"))
            common = {
                "repo_path": repo,
                "max_output_chars": 8000,
                "auto_learn_min_packs": 1,
                "auto_learn_min_interval_seconds": 0,
                "auto_learn_max_entries": 12,
            }
            cold_service = ContextService(
                ContextConfig(
                    state_dir=(state_root / case_id / str(repeat) / "cold").resolve(),
                    project_id=f"benchmark-{case_id}-{repeat}-cold",
                    auto_learn_cache=False,
                    **common,
                )
            )
            warm_service = ContextService(
                ContextConfig(
                    state_dir=(state_root / case_id / str(repeat) / "warm").resolve(),
                    project_id=f"benchmark-{case_id}-{repeat}-warm",
                    **common,
                )
            )
            cold_service.index.refresh(max_files=max_files)
            warm_service.index.refresh(max_files=max_files)
            request = {
                "focus_paths": list(case.get("focus_paths", [])),
                "max_items": 6,
                "output_profile": "compact",
                "index_max_files": max_files,
            }
            cold = cold_service.context_pack(
                prompt=str(case.get("related_prompt", "")),
                **request,
            )
            warm_service.context_pack(
                prompt=str(case.get("cold_prompt", "")),
                **request,
            )
            _wait_for_prompt_warmup(warm_service)
            warm = warm_service.context_pack(
                prompt=str(case.get("related_prompt", "")),
                **request,
            )
            _wait_for_prompt_warmup(warm_service)
            cold_latencies.append(float(cold["metrics"]["elapsed_ms"]))
            warm_latencies.append(float(warm["metrics"]["elapsed_ms"]))
            warm_packs.append(warm)
            cold_selection = _selection_signature(cold)
            warm_selection = _selection_signature(warm)
            cold_references = _reference_signature(cold)
            warm_references = _reference_signature(warm)
            retained = (
                cold_selection == warm_selection and cold_references == warm_references
            )
            retention.append(retained)
            if not retained:
                regressions.append(
                    {
                        "repeat": repeat,
                        "cold_selection": cold_selection,
                        "warm_selection": warm_selection,
                        "cold_references": cold_references,
                        "warm_references": warm_references,
                    }
                )
            required = {str(path) for path in case.get("required_anchors", [])}
            selected = {
                str(item.get("path", ""))
                for item in warm.get("items", [])
                if isinstance(item, dict)
            }
            required_anchor_recall.append(
                len(required & selected) / len(required) if required else 1.0
            )

        cold_median = statistics.median(cold_latencies)
        warm_median = statistics.median(warm_latencies)
        improvement = 1.0 - (warm_median / cold_median) if cold_median else 0.0
        namespace_totals = _namespace_totals(warm_packs)
        fragment_hits = sum(
            int(pack.get("cache", {}).get("fragment_hits", 0) or 0)
            for pack in warm_packs
        )
        fragment_misses = sum(
            int(pack.get("cache", {}).get("fragment_misses", 0) or 0)
            for pack in warm_packs
        )
        fragment_total = fragment_hits + fragment_misses
        fragment_ratio = fragment_hits / fragment_total if fragment_total else 0.0
        quality_retained = all(retention) and all(
            recall >= 1.0 for recall in required_anchor_recall
        )
        passed = (
            fragment_ratio >= fragment_threshold
            and improvement >= latency_improvement_threshold
            and quality_retained
        )
        case_results.append(
            {
                "id": case.get("id", ""),
                "passed": passed,
                "cold_latency_ms": cold_latencies,
                "warm_latency_ms": warm_latencies,
                "cold_median_ms": round(cold_median, 3),
                "warm_median_ms": round(warm_median, 3),
                "latency_improvement_ratio": round(improvement, 4),
                "fragment_hits": fragment_hits,
                "fragment_misses": fragment_misses,
                "fragment_hit_ratio": round(fragment_ratio, 4),
                "fragment_namespaces": namespace_totals,
                "selection_and_reference_retained": all(retention),
                "required_anchor_recall": round(
                    min(required_anchor_recall, default=1.0), 4
                ),
                "regressions": regressions,
            }
        )
    return {
        "schema": "cache_prompt_warmup_benchmark.v1",
        "fixture": fixture.name,
        "repeats": max(1, repeats),
        "requirements": {
            "fragment_hit_ratio_min": fragment_threshold,
            "latency_improvement_ratio_min": latency_improvement_threshold,
            "selection_and_reference_retained": True,
            "required_anchor_recall": 1.0,
        },
        "passed": bool(case_results) and all(row["passed"] for row in case_results),
        "cases": case_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark prompt-aware background fragment warmup."
    )
    parser.add_argument("--repo", default=str(REPO_ROOT))
    parser.add_argument(
        "--fixture", default=str(REPO_ROOT / "benchmarks" / "cache_prompt_warmup.json")
    )
    parser.add_argument("--state-root", default="")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--max-files", type=int, default=5000)
    parser.add_argument("--fragment-hit-ratio", type=float, default=0.60)
    parser.add_argument("--latency-improvement", type=float, default=0.30)
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    fixture = Path(args.fixture).resolve()
    if args.state_root:
        result = run_benchmark(
            repo=repo,
            fixture=fixture,
            state_root=Path(args.state_root).resolve(),
            repeats=args.repeats,
            max_files=args.max_files,
            fragment_threshold=args.fragment_hit_ratio,
            latency_improvement_threshold=args.latency_improvement,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="mcp-prompt-warmup-") as temp_dir:
            result = run_benchmark(
                repo=repo,
                fixture=fixture,
                state_root=Path(temp_dir),
                repeats=args.repeats,
                max_files=args.max_files,
                fragment_threshold=args.fragment_hit_ratio,
                latency_improvement_threshold=args.latency_improvement,
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
