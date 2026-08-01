#!/usr/bin/env python3
"""Run one complete native Milestone 3 cache, quality, and freshness gate."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from smoke_native_mcp import initialize_message, read_stdio, request, tool_json, write_stdio

REPO_ROOT = Path(__file__).resolve().parents[1]


def percentile_95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def pack_call(request_id: int) -> dict[str, Any]:
    return request(
        request_id,
        "tools/call",
        {
            "name": "context_pack",
            "arguments": {
                "prompt": "Profile native context pack retrieval latency and identify the measured hot path",
                "focus_paths": [
                    "src/context-core/src/lib.rs",
                    "src/context-index/src/lib.rs",
                ],
                "client_profile": "codex",
                "evidence_policy": "balanced",
                "cache_strategy": "fast",
            },
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("direct_binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run", type=int, required=True)
    parser.add_argument(
        "--monitor-usage",
        action="store_true",
        help="Enable the privacy-safe usage ledger during the MCP latency run.",
    )
    args = parser.parse_args()

    binary = args.binary.resolve()
    direct_binary = args.direct_binary.resolve()
    environment = os.environ.copy()
    environment["REPO_PATH"] = str(REPO_ROOT)
    nonce = f"{args.run}-{os.getpid()}"
    temporary_root = Path(tempfile.gettempdir())
    environment["MCP_CONTEXT_STATE_DIR"] = str(
        temporary_root / f"mcp-context-native-m3-direct-{nonce}"
    )
    direct_run = subprocess.run(
        [str(direct_binary)],
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if direct_run.returncode != 0:
        print(direct_run.stdout, end="")
        print(direct_run.stderr, end="")
        return direct_run.returncode
    direct = json.loads(direct_run.stdout.strip().splitlines()[-1])

    environment["MCP_CONTEXT_STATE_DIR"] = str(
        temporary_root / f"mcp-context-native-m3-mcp-{nonce}"
    )
    process = subprocess.Popen(
        [str(binary), "--transport", "stdio"],
        cwd=REPO_ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    timings: list[float] = []
    try:
        write_stdio(process, initialize_message())
        initialized = read_stdio(process)
        assert initialized["result"]["serverInfo"]["name"] == "mcp-context-manager"
        write_stdio(process, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        warm_request_id = 3 if args.monitor_usage else 2
        if args.monitor_usage:
            write_stdio(
                process,
                request(
                    2,
                    "tools/call",
                    {
                        "name": "context_admin",
                        "arguments": {
                            "mode": "monitor_usage",
                            "action": "enable",
                        },
                    },
                ),
            )
            usage_status = tool_json(read_stdio(process))
            assert usage_status["enabled"] is True
        write_stdio(process, pack_call(warm_request_id))
        warm = tool_json(read_stdio(process))
        assert warm["v"] == 2 and warm["evidence"]
        for request_id in range(warm_request_id + 1, warm_request_id + 101):
            started = time.perf_counter_ns()
            write_stdio(process, pack_call(request_id))
            response = tool_json(read_stdio(process))
            timings.append((time.perf_counter_ns() - started) / 1_000_000)
            assert response == warm
        write_stdio(
            process,
            request(
                warm_request_id + 101,
                "tools/call",
                {"name": "context_admin", "arguments": {"mode": "metrics"}},
            ),
        )
        metrics = tool_json(read_stdio(process))
        usage_report: dict[str, Any] | None = None
        if args.monitor_usage:
            write_stdio(
                process,
                request(
                    warm_request_id + 102,
                    "tools/call",
                    {
                        "name": "context_admin",
                        "arguments": {
                            "mode": "monitor_usage",
                            "action": "report",
                        },
                    },
                ),
            )
            usage_report = tool_json(read_stdio(process))
    finally:
        process.terminate()
        process.wait(timeout=5)

    local_mcp_p95 = percentile_95(timings)
    gates = dict(direct["gates"])
    gates["local_mcp_l0_p95_lte_10ms"] = local_mcp_p95 <= 10.0
    gates["runtime_l0_hits_recorded"] = metrics["cache"]["l0"]["hits"] >= 100
    if usage_report is not None:
        profiled_requests = sum(
            int(bucket.get("profiled_request_count") or 0)
            for bucket in usage_report.get("buckets", [])
            if isinstance(bucket, dict)
        )
        codex_requests = sum(
            int(profile.get("request_count") or 0)
            for bucket in usage_report.get("buckets", [])
            if isinstance(bucket, dict)
            for profile in bucket.get("client_profiles", [])
            if isinstance(profile, dict)
            and profile.get("client_profile") == "codex"
        )
        gates["monitor_usage_profiled_requests_recorded"] = profiled_requests >= 101
        gates["monitor_usage_codex_bucket_recorded"] = codex_requests >= 101
    output = {
        "schema": "rust_native_milestone_3.complete_run.v1",
        "run": args.run,
        "method": {
            "environment": "devcontainer",
            "transport": "persistent MCP stdio",
            "l0_samples": len(timings),
            "state": "isolated rust-v2 overlay per run",
            "monitor_usage": "enabled" if args.monitor_usage else "disabled",
        },
        "summary": {
            **direct["summary"],
            "local_mcp_l0_p95_ms": local_mcp_p95,
            "local_mcp_l0_median_ms": statistics.median(timings),
        },
        "runtime_metrics": metrics,
        "gates": gates,
    }
    if usage_report is not None:
        output["usage_report"] = usage_report
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps(output["summary"], sort_keys=True))
    failed = [name for name, passed in gates.items() if not passed]
    if failed:
        print(json.dumps({"failed_gates": failed}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
