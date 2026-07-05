#!/usr/bin/env python3
"""
monitor-metrics.py

Monitor MCP context-manager metrics for all discovered project IDs using MCP Resources.

- Discovers project metrics resources via MCP resources/list
  (repo://project/{project_id}/metrics)
- Reads each project's metrics via resources/read on repo://project/{id}/metrics
- Prints a compact summary per project, optionally at a fixed interval

Requirements:
- MCP server running with streamable HTTP endpoint (default http://localhost:8000/mcp)
- No third-party dependencies; uses urllib

Usage:
  python3 monitor-metrics.py
  python3 monitor-metrics.py --url http://127.0.0.1:8000/mcp --interval 10

Environment:
  MCP_URL (default: http://localhost:8000/mcp)
  MCP_SESSION_ID (optional; if unset, a UUID is generated)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_URL = os.environ.get("MCP_URL", "http://localhost:8000/mcp")

# Reuse a caller-provided session when set; otherwise initialize lets the server
# assign the Streamable HTTP session id.
SESSION_ID: Optional[str] = os.environ.get("MCP_SESSION_ID")
INITIALIZED = False

def _with_session_query(url: str, session_id: Optional[str]) -> str:
    if not session_id:
        return url
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    # Only set if not present
    q.setdefault("sessionId", session_id)
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))

def _parse_sse_json(raw: str) -> Optional[Dict[str, Any]]:
    lines = raw.splitlines()
    data_lines: List[str] = []
    for line in lines:
        s = line.strip("\ufeff\ufeff\n\r")
        if not s:
            continue
        # Capture any data: lines (ignore event: or id:)
        if s.startswith("data:"):
            data_lines.append(s[len("data:"):].lstrip())
    if not data_lines:
        return None
    joined = "\n".join(data_lines).strip()
    if not joined:
        return None
    try:
        obj = json.loads(joined)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        return None
    return None

def _rpc_call(
    url: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 15.0,
    session_id: Optional[str] = None,
    expect_result: bool = True,
) -> Dict[str, Any]:
    global SESSION_ID
    # Include sessionId and session object in params for compatibility
    effective_params = dict(params or {})
    if session_id:
        effective_params.setdefault("sessionId", session_id)
        # Some servers may expect a nested session object
        sess = effective_params.get("session")
        if not isinstance(sess, dict):
            effective_params["session"] = {"id": session_id}
        else:
            sess.setdefault("id", session_id)

    payload: Dict[str, Any] = {
        "jsonrpc": "2.0",
        "method": method,
        "params": effective_params,
    }
    if expect_result:
        payload["id"] = f"req-{int(time.time() * 1000)}"
    data = json.dumps(payload).encode("utf-8")

    # Headers must accept JSON and SSE; include multiple session header casings
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if session_id:
        headers["Mcp-Session-Id"] = session_id
        headers["X-Session-Id"] = session_id
        headers["Session-Id"] = session_id
        headers["X-Session-ID"] = session_id
        headers["x-session-id"] = session_id

    # Also include sessionId in the URL query
    req_url = _with_session_query(url, session_id)
    req = urllib.request.Request(req_url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            response_session = (
                resp.headers.get("Mcp-Session-Id")
                or resp.headers.get("mcp-session-id")
            )
            if response_session:
                SESSION_ID = response_session
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} calling {method}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Error calling {method}: {e}") from e

    # Parse plain JSON first; if that fails, try SSE-framed JSON
    if not raw.strip():
        if expect_result:
            raise RuntimeError(f"Missing response body for {method}")
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        sse_obj = _parse_sse_json(raw)
        if sse_obj is None:
            raise RuntimeError(f"Invalid JSON-RPC response for {method}: {e}\nRaw: {raw[:500]}") from e
        obj = sse_obj

    if "error" in obj and obj["error"]:
        raise RuntimeError(f"MCP error calling {method}: {obj['error']}")

    if "result" not in obj:
        if not expect_result:
            return {}
        raise RuntimeError(f"Missing result in response for {method}: {obj}")

    return obj["result"]

def initialize_session(url: str) -> None:
    global INITIALIZED
    if INITIALIZED:
        return
    _rpc_call(
        url,
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "monitor-metrics", "version": "0.1.0"},
        },
        session_id=SESSION_ID,
    )
    _rpc_call(
        url,
        "notifications/initialized",
        {},
        session_id=SESSION_ID,
        expect_result=False,
    )
    INITIALIZED = True

def list_resources(url: str) -> List[Dict[str, Any]]:
    result = _rpc_call(url, "resources/list", {}, session_id=SESSION_ID)
    # Expected: { resources: [ { uri, name, mimeType, description? } ] }
    resources = result.get("resources") or result.get("items") or []
    if not isinstance(resources, list):
        raise RuntimeError(f"Unexpected resources/list result shape: {result}")
    return resources

PROJECT_METRICS_RE = re.compile(r"^repo://project/([^/]+)/metrics$")

def discover_project_ids_from_resources(resources: List[Dict[str, Any]]) -> List[str]:
    project_ids = set()
    for r in resources:
        uri = r.get("uri") or r.get("id") or ""
        if not isinstance(uri, str):
            continue
        m = PROJECT_METRICS_RE.match(uri)
        if m:
            project_ids.add(m.group(1))
    return sorted(project_ids)

def read_resource(url: str, uri: str, timeout: float = 15.0) -> Dict[str, Any]:
    # Try single-URI call
    try:
        result = _rpc_call(url, "resources/read", {"uri": uri}, timeout=timeout, session_id=SESSION_ID)
    except RuntimeError:
        # Fallback to plural
        result = _rpc_call(url, "resources/read", {"uris": [uri]}, timeout=timeout, session_id=SESSION_ID)

    if "resource" in result and isinstance(result["resource"], dict):
        return result["resource"]

    contents = result.get("contents") or result.get("resources") or []
    if isinstance(contents, list) and contents:
        exact = [c for c in contents if (c.get("uri") == uri)]
        return (exact[0] if exact else contents[0])

    raise RuntimeError(f"Unexpected resources/read result shape for {uri}: {result}")

def extract_json_from_resource(res_obj: Dict[str, Any]) -> Any:
    if "json" in res_obj:
        return res_obj["json"]
    text = res_obj.get("text")
    if isinstance(text, str):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    for key in ("value", "data", "body"):
        val = res_obj.get(key)
        if isinstance(val, (dict, list)):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return val
    return res_obj

def format_latency(metrics: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    avg = min_v = max_v = None
    by_op = (metrics.get("requests") or {}).get("by_operation") or {}
    cp = by_op.get("context_pack")
    if isinstance(cp, dict):
        avg = cp.get("avg_elapsed_ms", avg)
        min_v = cp.get("min_elapsed_ms", min_v)
        max_v = cp.get("max_elapsed_ms", max_v)
    if avg is None or min_v is None or max_v is None:
        bench = (metrics.get("benchmarks") or {}).get("latency_ms_by_operation") or {}
        cpb = bench.get("context_pack")
        if isinstance(cpb, dict):
            if avg is None:
                avg = cpb.get("avg", None)
            if min_v is None:
                min_v = cpb.get("min", None)
            if max_v is None:
                max_v = cpb.get("max", None)
    return (avg, min_v, max_v)

def print_project_metrics(project_id: str, metrics: Dict[str, Any]) -> None:
    generated_at = metrics.get("generated_at") or metrics.get("updated_at") or ""
    requests_total = (metrics.get("requests") or {}).get("total", 0)
    cache = metrics.get("cache") or {}
    hits = cache.get("hits", 0)
    misses = cache.get("misses", 0)
    hit_ratio = cache.get("hit_ratio", None)
    avg, min_v, max_v = format_latency(metrics)

    def fmt(v: Optional[float]) -> str:
        return f"{v:.2f}" if isinstance(v, (int, float)) else "-"

    print(f"[{project_id}] at {generated_at} | reqs={requests_total} | cache h/m={hits}/{misses} ({hit_ratio if hit_ratio is not None else '-'}) | context_pack ms avg/min/max={fmt(avg)}/{fmt(min_v)}/{fmt(max_v)}")

def monitor_once(url: str) -> int:
    try:
        initialize_session(url)
        resources = list_resources(url)
    except Exception as e:
        print(f"ERROR: failed to list resources from {url}: {e}", file=sys.stderr)
        return 2

    project_ids = discover_project_ids_from_resources(resources)
    if not project_ids:
        print("No project metrics resources found. Ensure the server has visible projects. See README.md 'MCP Resources'.", file=sys.stderr)
        return 1

    exit_code = 0
    for pid in project_ids:
        uri = f"repo://project/{pid}/metrics"
        try:
            res_obj = read_resource(url, uri)
            payload = extract_json_from_resource(res_obj)
            if isinstance(payload, dict) and payload.get("schema") == "context_metrics.v1":
                print_project_metrics(pid, payload)
            else:
                short = json.dumps(payload)[:500] if not isinstance(payload, str) else payload[:500]
                print(f"[{pid}] metrics (raw): {short}")
        except Exception as e:
            print(f"ERROR: reading metrics for {pid}: {e}", file=sys.stderr)
            exit_code = 3
    return exit_code

def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor MCP context-manager metrics via MCP Resources for all discovered projects.")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"MCP streamable HTTP endpoint (default: {DEFAULT_URL})")
    parser.add_argument("--interval", type=float, default=0.0, help="Polling interval in seconds. 0 means run once and exit.")
    args = parser.parse_args()

    if args.interval <= 0:
        return monitor_once(args.url)

    print(f"Monitoring metrics every {args.interval}s via {args.url}. Press Ctrl+C or stop the process to exit.", flush=True)
    try:
        while True:
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            print(f"\n=== Metrics snapshot @ {ts} ===")
            monitor_once(args.url)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
        return 0

if __name__ == "__main__":
    sys.exit(main())
