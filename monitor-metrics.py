#!/usr/bin/env python3
"""
Monitor mcp-context-manager metrics directly through MCP tool calls.

The tool talks to the Streamable HTTP MCP endpoint and calls:

- context_admin(mode="projects")
- context_admin(mode="metrics", project_id=...)
- context_admin(mode="measurement_matrix", project_id=...)

It renders an interactive terminal dashboard for one project or every known project.
No third-party dependencies are required.

Usage:
  python3 monitor-metrics.py
  python3 monitor-metrics.py --once
  python3 monitor-metrics.py --interval 5
  python3 monitor-metrics.py --project-id my-repo-123abc
  python3 monitor-metrics.py --url http://127.0.0.1:8000/mcp --color always

Environment:
  MCP_URL (default: http://localhost:8000/mcp)
  MCP_SESSION_ID (optional)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import select
import shutil
import sys
import termios
import time
import tty
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_URL = os.environ.get("MCP_URL", "http://localhost:8000/mcp")
DEFAULT_TIMEOUT = 15.0
DEFAULT_INTERVAL = 5.0
MIN_INTERVAL = 0.5
MAX_INTERVAL = 3600.0
INTERVAL_STEP = 1.0
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


class Ansi:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    CYAN = "\033[36m"
    REVERSE = "\033[7m"


@dataclass
class ProjectTarget:
    project_id: str
    name: str = ""
    source: str = ""


@dataclass
class ProjectSnapshot:
    target: ProjectTarget
    metrics: dict[str, Any] | None = None
    matrix: dict[str, Any] | None = None
    error: str = ""


@dataclass
class MonitorState:
    selected_index: int = 0
    view: str = "table"
    refresh_interval: float = DEFAULT_INTERVAL
    last_updated: str = ""
    state_selected_index: int = 0
    state_scroll_offset: int = 0
    state_search: str = ""
    state_search_active: bool = False
    state_payload: dict[str, Any] | None = None
    state_entry: dict[str, Any] | None = None
    state_error: str = ""
    state_target: ProjectTarget | None = None


class McpHttpClient:
    def __init__(
        self,
        url: str,
        timeout: float = DEFAULT_TIMEOUT,
        session_id: str | None = None,
    ):
        self.url = url
        self.timeout = timeout
        self.session_id = session_id or os.environ.get("MCP_SESSION_ID") or ""
        self.initialized = False
        self._request_id = 0

    def initialize(self) -> None:
        if self.initialized:
            return
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "monitor-metrics", "version": "0.2.0"},
            },
        )
        self.rpc("notifications/initialized", {}, expect_result=False)
        self.initialized = True

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        result = self.rpc(
            "tools/call",
            {"name": name, "arguments": arguments},
        )
        payload = extract_tool_payload(result)
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected tool payload for {name}: {payload!r}")
        return payload

    def rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        expect_result: bool = True,
    ) -> dict[str, Any]:
        self._request_id += 1
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
        }
        if expect_result:
            payload["id"] = f"monitor-{self._request_id}"
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        request = urllib.request.Request(
            _with_session_query(self.url, self.session_id),
            data=data,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response_session = (
                    response.headers.get("Mcp-Session-Id")
                    or response.headers.get("mcp-session-id")
                )
                if response_session:
                    self.session_id = response_session
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} calling {method}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"error calling {method}: {exc}") from exc

        if not raw.strip():
            if expect_result:
                raise RuntimeError(f"missing response body for {method}")
            return {}
        message = _parse_rpc_message(raw, method)
        if message.get("error"):
            raise RuntimeError(f"MCP error calling {method}: {message['error']}")
        if not expect_result:
            return {}
        result = message.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"missing result in response for {method}: {message}")
        return result


def _with_session_query(url: str, session_id: str) -> str:
    if not session_id:
        return url
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sessionId", session_id)
    return urlunparse(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parts.params,
            urlencode(query, doseq=True),
            parts.fragment,
        )
    )


def _parse_rpc_message(raw: str, method: str) -> dict[str, Any]:
    try:
        message = json.loads(raw)
    except json.JSONDecodeError as exc:
        message = _parse_sse_json(raw)
        if message is None:
            raise RuntimeError(
                f"invalid JSON-RPC response for {method}: {exc}; raw={raw[:500]}"
            ) from exc
    if not isinstance(message, dict):
        raise RuntimeError(f"unexpected JSON-RPC response for {method}: {message!r}")
    return message


def _parse_sse_json(raw: str) -> dict[str, Any] | None:
    data_lines: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("data:"):
            data_lines.append(stripped[len("data:") :].lstrip())
    if not data_lines:
        return None
    try:
        message = json.loads("\n".join(data_lines))
    except json.JSONDecodeError:
        return None
    return message if isinstance(message, dict) else None


def extract_tool_payload(result: dict[str, Any]) -> Any:
    if result.get("isError"):
        raise RuntimeError(f"MCP tool returned an error: {result}")
    structured = result.get("structuredContent")
    if isinstance(structured, (dict, list)):
        return structured
    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("json"), (dict, list)):
                return item["json"]
            text = item.get("text")
            if isinstance(text, str):
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return text
    return result


def discover_projects(client: Any) -> list[ProjectTarget]:
    payload = client.call_tool("context_admin", {"mode": "projects"})
    projects = payload.get("projects") if isinstance(payload, dict) else None
    if not isinstance(projects, list):
        return []
    targets: list[ProjectTarget] = []
    for row in projects:
        if not isinstance(row, dict):
            continue
        project_id = str(row.get("project_id") or "").strip()
        if not project_id:
            continue
        targets.append(
            ProjectTarget(
                project_id=project_id,
                name=str(row.get("name") or ""),
                source=str(row.get("source") or ""),
            )
        )
    targets.sort(key=lambda item: (item.name.lower(), item.project_id))
    return targets


def collect_snapshots(
    client: Any,
    project_ids: list[str],
    root_uri: str,
    include_matrix: bool,
) -> list[ProjectSnapshot]:
    if root_uri:
        target = ProjectTarget(project_id="", name=root_uri, source="root_uri")
        return [_collect_one(client, target, root_uri=root_uri, include_matrix=include_matrix)]

    known_projects = _safe_discover_projects(client)
    known_by_id = {target.project_id: target for target in known_projects}

    targets = [
        known_by_id.get(pid, ProjectTarget(project_id=pid))
        for pid in project_ids
    ]
    if not targets:
        targets = known_projects
    if not targets:
        targets = [ProjectTarget(project_id="", name="default", source="fallback")]
    return [
        _collect_one(client, target, root_uri="", include_matrix=include_matrix)
        for target in targets
    ]


def fetch_state_browser(
    client: Any,
    target: ProjectTarget,
    max_entries: int = 80,
    state_key: str = "",
) -> dict[str, Any]:
    root_uri = target.name if target.source == "root_uri" else ""
    selector = _selector_args(target, root_uri)
    args: dict[str, Any] = {
        "mode": "state_browser",
        **selector,
        "max_entries": max_entries,
        "max_output_chars": 20000 if state_key else 1200,
    }
    if state_key:
        args["state_key"] = state_key
    return client.call_tool("context_admin", args)


def friendly_mcp_error(exc: Exception) -> str:
    message = str(exc)
    if (
        "context_adminArguments" in message
        and "state_browser" in message
        and "literal_error" in message
    ):
        return (
            "The running MCP server does not support "
            "context_admin(mode='state_browser') yet. Rebuild and restart it with "
            "the current checkout, for example: "
            "MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up -d --build"
        )
    return message


def _safe_discover_projects(client: Any) -> list[ProjectTarget]:
    try:
        return discover_projects(client)
    except Exception:
        return []


def _collect_one(
    client: Any,
    target: ProjectTarget,
    root_uri: str,
    include_matrix: bool,
) -> ProjectSnapshot:
    selector = _selector_args(target, root_uri)
    snapshot = ProjectSnapshot(target=target)
    try:
        snapshot.metrics = client.call_tool(
            "context_admin", {"mode": "metrics", **selector}
        )
        if include_matrix:
            snapshot.matrix = client.call_tool(
                "context_admin", {"mode": "measurement_matrix", **selector}
            )
    except Exception as exc:
        snapshot.error = str(exc)
    return snapshot


def _selector_args(target: ProjectTarget, root_uri: str) -> dict[str, str]:
    if root_uri:
        return {"root_uri": root_uri}
    if target.project_id:
        return {"project_id": target.project_id}
    return {}


def render_dashboard(
    snapshots: list[ProjectSnapshot],
    url: str,
    color: bool,
    width: int | None = None,
    selected_index: int | None = None,
    refresh_interval: float | None = None,
) -> str:
    width = width or shutil.get_terminal_size((120, 30)).columns
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ok_rows = [row for row in snapshots if row.metrics and not row.error]
    totals = _aggregate_totals(ok_rows)
    lines = [
        _style("mcp-context-manager metrics", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        f"updated:  {now}   projects: {len(snapshots)}   ok: {len(ok_rows)}   errors: {len(snapshots) - len(ok_rows)}",
        "",
        *_summary_table(totals, snapshots, ok_rows, color),
        "",
        *_project_table(snapshots, color, width, selected_index=selected_index),
    ]
    lines.extend(["", _legend(color)])
    if refresh_interval is not None:
        lines.append(_controls(refresh_interval, color))
    return "\n".join(lines)


def render_monitor_screen(
    snapshots: list[ProjectSnapshot],
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
) -> str:
    if state.view == "state":
        return render_state_browser(url=url, color=color, width=width, state=state)
    if state.view == "detail" and snapshots:
        selected = _clamped_index(state.selected_index, len(snapshots))
        return render_project_detail(
            snapshots[selected],
            url=url,
            color=color,
            width=width,
            state=state,
        )
    return render_dashboard(
        snapshots,
        url=url,
        color=color,
        width=width,
        selected_index=state.selected_index,
        refresh_interval=state.refresh_interval,
    )


def render_project_detail(
    snapshot: ProjectSnapshot,
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
) -> str:
    width = width or shutil.get_terminal_size((120, 30)).columns
    metrics = snapshot.metrics or {}
    cache_hits = _int_at(metrics, ("cache", "hits"))
    cache_misses = _int_at(metrics, ("cache", "misses"))
    cache_total = cache_hits + cache_misses
    cache_ratio = (cache_hits / cache_total) if cache_total else 0.0
    details = [
        ("project", _project_name(snapshot.target)),
        ("project id", _project_identifier(snapshot.target)),
        ("endpoint", url),
        ("updated", state.last_updated or "-"),
        ("requests", fmt_int(_int_at(metrics, ("requests", "total")))),
        (
            "context_pack",
            fmt_int(
                _int_at(
                    metrics,
                    ("requests", "by_operation", "context_pack", "count"),
                )
            ),
        ),
        (
            "avg ms",
            fmt_ms(
                _float_at(
                    metrics,
                    ("requests", "by_operation", "context_pack", "avg_elapsed_ms"),
                )
            ),
        ),
        ("cache", f"{cache_hits}/{cache_misses} h/m  {cache_ratio * 100:5.1f}%"),
        (
            "tokens saved",
            fmt_int(_int_at(metrics, ("tokens", "estimated_input_tokens_saved"))),
        ),
        (
            "refs deferred",
            fmt_bytes(_int_at(metrics, ("references", "bytes_deferred_est"))),
        ),
    ]
    lines = [
        _style("mcp-context-manager project details", color, Ansi.BOLD + Ansi.CYAN),
        _controls(state.refresh_interval, color),
        "",
        *_render_table(("field", "value"), details, aligns=("left", "left")),
    ]
    if snapshot.error:
        lines.extend(["", _style(f"ERROR: {snapshot.error}", color, Ansi.RED)])
    check_rows = _measurement_check_rows(snapshot.matrix or {}, color)
    if check_rows:
        key_width = max(24, min(max(_visible_len(row[0]) for row in check_rows), width - 44))
        lines.extend(
            [
                "",
                *_render_table(
                    ("check", "status", "current", "target"),
                    check_rows,
                    widths=(key_width, 12, 10, 10),
                    aligns=("left", "right", "right", "right"),
                ),
            ]
        )
    return "\n".join(lines)


def render_state_browser(
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
    height: int | None = None,
) -> str:
    terminal_size = shutil.get_terminal_size((120, 30))
    width = width or terminal_size.columns
    height = height or terminal_size.lines
    payload = state.state_payload or {}
    rows = _filtered_state_rows(state)
    visible_count = _state_visible_count(height, bool(state.state_entry))
    state.state_selected_index = _clamped_index(state.state_selected_index, len(rows))
    state.state_scroll_offset = _adjust_scroll_offset(
        state.state_scroll_offset,
        state.state_selected_index,
        visible_count,
        len(rows),
    )
    visible_rows = rows[
        state.state_scroll_offset : state.state_scroll_offset + visible_count
    ]
    project = _project_name(state.state_target) if state.state_target else "-"
    lines = [
        _style("mcp-context-manager state browser", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        f"project:  {project}",
        _browser_controls(state, color),
        "",
    ]
    if state.state_error:
        lines.append(_style(f"ERROR: {state.state_error}", color, Ansi.RED))
        return "\n".join(lines)
    prefix_counts = payload.get("prefix_counts") if isinstance(payload, dict) else []
    if isinstance(prefix_counts, list) and prefix_counts:
        summary = "  ".join(
            f"{row.get('prefix', '-')}{row.get('count', 0)}"
            for row in prefix_counts[:8]
            if isinstance(row, dict)
        )
        lines.extend([f"groups:   {summary}", ""])
    lines.extend(
        [
            (
                f"rows:     {len(rows)}"
                f" / {len(_state_rows(state))}"
                f"   selected: {state.state_selected_index + 1 if rows else 0}"
                f"   window: {state.state_scroll_offset + 1 if rows else 0}-"
                f"{state.state_scroll_offset + len(visible_rows) if rows else 0}"
            ),
            f"search:   {state.state_search or '-'}"
            + ("  (typing)" if state.state_search_active else ""),
            "",
        ]
    )
    table_rows = [
        _state_row_cells(
            row,
            selected=(
                state.state_scroll_offset + index == state.state_selected_index
            ),
        )
        for index, row in enumerate(visible_rows)
        if isinstance(row, dict)
    ]
    lines.extend(
        _render_table(
            ("", "key", "type", "size", "schema", "status", "expires"),
            table_rows,
            widths=_state_table_widths(width),
            aligns=("left", "left", "left", "right", "left", "left", "left"),
        )
    )
    if not table_rows:
        lines.append("No generated-state rows for this project.")
    if state.state_entry:
        lines.extend(["", *render_state_entry_overlay(color, width, state)])
    return "\n".join(lines)


def render_state_entry_overlay(
    color: bool,
    width: int,
    state: MonitorState,
) -> list[str]:
    payload = state.state_entry or {}
    entry = payload.get("entry") if isinstance(payload, dict) else {}
    entry = entry if isinstance(entry, dict) else {}
    preview = str(entry.get("preview") or "")
    inner_width = max(40, min(100, width - 6))
    lines = _render_table(
        ("field", "value"),
        [
            ("key", entry.get("key", "-")),
            ("type", entry.get("value_type", "-")),
            ("size", fmt_int(entry.get("size_chars", 0))),
            ("schema", entry.get("schema", "-") or "-"),
            ("status", entry.get("status", "-") or "-"),
            ("expires", entry.get("expires_at", "-") or "-"),
        ],
        aligns=("left", "left"),
    )
    preview_lines = _wrap_block(preview, inner_width)
    body = [
        _style("state entry overlay", color, Ansi.BOLD + Ansi.CYAN),
        "Esc closes overlay",
        "",
        *lines,
        "",
        _style("preview", color, Ansi.BOLD),
        *preview_lines,
    ]
    return _box_lines(body, width=inner_width + 4)


def _aggregate_totals(rows: list[ProjectSnapshot]) -> dict[str, Any]:
    totals = {
        "requests": 0,
        "packs": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "tokens_saved": 0,
        "bytes_deferred": 0,
    }
    for row in rows:
        metrics = row.metrics or {}
        totals["requests"] += _int_at(metrics, ("requests", "total"))
        totals["packs"] += _int_at(
            metrics, ("requests", "by_operation", "context_pack", "count")
        )
        totals["cache_hits"] += _int_at(metrics, ("cache", "hits"))
        totals["cache_misses"] += _int_at(metrics, ("cache", "misses"))
        totals["tokens_saved"] += _int_at(
            metrics, ("tokens", "estimated_input_tokens_saved")
        )
        totals["bytes_deferred"] += _int_at(
            metrics, ("references", "bytes_deferred_est")
        )
    return totals


def _summary_table(
    totals: dict[str, Any],
    snapshots: list[ProjectSnapshot],
    ok_rows: list[ProjectSnapshot],
    color: bool,
) -> list[str]:
    cache_total = int(totals["cache_hits"]) + int(totals["cache_misses"])
    ratio = (int(totals["cache_hits"]) / cache_total) if cache_total else 0.0
    rows = [
        ("projects", f"{len(snapshots)} total / {len(ok_rows)} ok"),
        ("requests", fmt_int(totals["requests"])),
        ("context_pack", fmt_int(totals["packs"])),
        ("cache hit", f"{_bar(ratio, 18, color)} {ratio * 100:5.1f}%"),
        ("tokens saved", fmt_int(totals["tokens_saved"])),
        ("refs deferred", fmt_bytes(totals["bytes_deferred"])),
    ]
    return _render_table(("metric", "value"), rows, aligns=("left", "right"))


def _project_table(
    snapshots: list[ProjectSnapshot],
    color: bool,
    width: int,
    selected_index: int | None = None,
) -> list[str]:
    widths = _project_column_widths(width)
    rows = [
        _project_cells(snapshot, color, widths, selected=index == selected_index)
        for index, snapshot in enumerate(snapshots)
    ]
    rendered = _render_table(
        ("", "project", "project id", "req", "pack", "avg ms", "cache", "tokens", "checks"),
        rows,
        widths=(
            widths["selector"],
            widths["project"],
            widths["project_id"],
            widths["req"],
            widths["pack"],
            widths["avg_ms"],
            widths["cache"],
            widths["tokens"],
            widths["checks"],
        ),
        aligns=(
            "left",
            "left",
            "left",
            "right",
            "right",
            "right",
            "right",
            "right",
            "right",
        ),
    )
    if selected_index is not None and color:
        header_rows = 3
        selected_row = header_rows + _clamped_index(selected_index, len(snapshots))
        if 0 <= selected_row < len(rendered) - 1:
            rendered[selected_row] = _style(rendered[selected_row], color, Ansi.REVERSE)
    return rendered


def _project_column_widths(width: int) -> dict[str, int]:
    widths = {
        "selector": 1,
        "project": 28,
        "project_id": 22,
        "req": 5,
        "pack": 5,
        "avg_ms": 7,
        "cache": 19,
        "tokens": 8,
        "checks": 8,
    }
    target_width = max(100, width)
    overhead = len(widths) * 3 + 1
    while overhead + sum(widths.values()) > target_width and widths["project_id"] > 12:
        widths["project_id"] -= 1
    while overhead + sum(widths.values()) > target_width and widths["project"] > 16:
        widths["project"] -= 1
    return widths


def _project_cells(
    snapshot: ProjectSnapshot,
    color: bool,
    widths: dict[str, int],
    selected: bool = False,
) -> tuple[str, ...]:
    project = _trim(_project_name(snapshot.target), widths["project"])
    project_id = _trim(_project_identifier(snapshot.target), widths["project_id"])
    selector = ">" if selected else " "
    if snapshot.error:
        return (
            selector,
            project,
            project_id,
            "-",
            "-",
            "-",
            "-",
            "-",
            _style("error", color, Ansi.RED),
        )
    metrics = snapshot.metrics or {}
    requests = _int_at(metrics, ("requests", "total"))
    packs = _int_at(metrics, ("requests", "by_operation", "context_pack", "count"))
    avg_ms = _float_at(
        metrics, ("requests", "by_operation", "context_pack", "avg_elapsed_ms")
    )
    cache_ratio = _float_at(metrics, ("cache", "hit_ratio"))
    tokens_saved = _int_at(metrics, ("tokens", "estimated_input_tokens_saved"))
    checks = _matrix_status(snapshot.matrix or {}, color)
    cache_bar_width = max(6, widths["cache"] - 9)
    return (
        selector,
        project,
        project_id,
        fmt_int(requests),
        fmt_int(packs),
        fmt_ms(avg_ms),
        f"{_bar(cache_ratio, cache_bar_width, color)} {cache_ratio * 100:5.1f}%",
        fmt_int(tokens_saved),
        checks,
    )


def _project_name(target: ProjectTarget) -> str:
    return target.name or target.project_id or "default"


def _project_identifier(target: ProjectTarget) -> str:
    if target.project_id and target.name:
        return target.project_id
    if target.project_id and not target.name:
        return "-"
    return target.source or "-"


def _controls(refresh_interval: float, color: bool) -> str:
    keys = (
        "keys: Up/Down select  Enter details  b state  Esc table  "
        "+/- refresh  r reload  q quit"
    )
    return (
        f"{keys}   refresh={fmt_seconds(refresh_interval)}"
        if not color
        else f"{_style(keys, color, Ansi.DIM)}   refresh={fmt_seconds(refresh_interval)}"
    )


def _browser_controls(state: MonitorState, color: bool) -> str:
    if state.state_entry:
        keys = "keys: Esc close overlay  q quit"
    elif state.state_search_active:
        keys = "keys: type search  Backspace edit  Enter apply  Esc cancel"
    else:
        keys = (
            "keys: Up/Down scroll  PgUp/PgDn jump  / search  "
            "Enter inspect  Esc table  q quit"
        )
    return _style(keys, color, Ansi.DIM) if color else keys


def _measurement_check_rows(
    matrix: dict[str, Any],
    color: bool,
) -> list[tuple[str, str, str, str]]:
    checks = matrix.get("checks") if isinstance(matrix, dict) else None
    if not isinstance(checks, list):
        return []
    rows: list[tuple[str, str, str, str]] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        status = str(check.get("status") or "-")
        rows.append(
            (
                _trim(str(check.get("key") or "-"), 72),
                _status_text(status, color),
                _metric_value(check.get("current")),
                _metric_value(check.get("target")),
            )
        )
    return rows


def _state_rows(state: MonitorState) -> list[dict[str, Any]]:
    rows = (state.state_payload or {}).get("rows")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _filtered_state_rows(state: MonitorState) -> list[dict[str, Any]]:
    rows = _state_rows(state)
    query = state.state_search.strip().lower()
    if not query:
        return rows
    return [row for row in rows if _state_row_matches(row, query)]


def _state_row_matches(row: dict[str, Any], query: str) -> bool:
    haystack = " ".join(
        str(row.get(key, ""))
        for key in (
            "key",
            "value_type",
            "schema",
            "status",
            "expires_at",
            "preview",
        )
    ).lower()
    return query in haystack


def _state_visible_count(height: int, overlay_open: bool) -> int:
    reserved = 22 if overlay_open else 13
    return max(3, height - reserved)


def _adjust_scroll_offset(
    offset: int,
    selected: int,
    visible_count: int,
    row_count: int,
) -> int:
    if row_count <= 0:
        return 0
    max_offset = max(0, row_count - visible_count)
    offset = max(0, min(offset, max_offset))
    if selected < offset:
        return selected
    if selected >= offset + visible_count:
        return min(max_offset, selected - visible_count + 1)
    return offset


def _state_row_cells(row: dict[str, Any], selected: bool = False) -> tuple[str, ...]:
    return (
        ">" if selected else " ",
        _trim(str(row.get("key") or "-"), 42),
        _trim(str(row.get("value_type") or "-"), 8),
        fmt_int(row.get("size_chars", 0)),
        _trim(str(row.get("schema") or "-"), 22),
        _trim(str(row.get("status") or "-"), 10),
        _trim(str(row.get("expires_at") or "-"), 20),
    )


def _state_table_widths(width: int) -> tuple[int, ...]:
    key_width = max(24, min(42, width - 70))
    return (1, key_width, 8, 8, 22, 10, 20)


def _wrap_block(text: str, width: int) -> list[str]:
    if not text:
        return ["-"]
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line
        if not line:
            lines.append("")
            continue
        while len(line) > width:
            lines.append(line[:width])
            line = line[width:]
        lines.append(line)
    return lines


def _box_lines(lines: list[str], width: int) -> list[str]:
    width = max(10, width)
    border = "+" + "-" * (width - 2) + "+"
    boxed = [border]
    for line in lines:
        visible = _visible_len(line)
        if visible <= width - 4:
            boxed.append("| " + line + " " * (width - 4 - visible) + " |")
            continue
        for wrapped in _wrap_block(line, width - 4):
            boxed.append(
                "| "
                + wrapped
                + " " * max(0, width - 4 - _visible_len(wrapped))
                + " |"
            )
    boxed.append(border)
    return boxed


def _status_text(status: str, color: bool) -> str:
    if status == "pass":
        return _style(status, color, Ansi.GREEN)
    if status == "fail":
        return _style(status, color, Ansi.RED)
    if status == "insufficient":
        return _style(status, color, Ansi.YELLOW)
    return status or "-"


def _metric_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return str(value)
    return str(value)


def _clamped_index(index: int, count: int) -> int:
    if count <= 0:
        return 0
    return max(0, min(index, count - 1))


def _render_table(
    headers: tuple[str, ...],
    rows: list[tuple[Any, ...]],
    aligns: tuple[str, ...],
    widths: tuple[int, ...] | None = None,
) -> list[str]:
    text_rows = [tuple(str(cell) for cell in row) for row in rows]
    if widths is None:
        widths = tuple(
            max(
                _visible_len(headers[index]),
                *((
                    _visible_len(row[index])
                    for row in text_rows
                    if index < len(row)
                ) or [0]),
            )
            for index in range(len(headers))
        )
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    rendered = [border, _render_table_row(headers, widths, aligns), border]
    rendered.extend(_render_table_row(row, widths, aligns) for row in text_rows)
    rendered.append(border)
    return rendered


def _render_table_row(
    cells: tuple[Any, ...],
    widths: tuple[int, ...],
    aligns: tuple[str, ...],
) -> str:
    rendered_cells = []
    for index, width in enumerate(widths):
        cell = str(cells[index]) if index < len(cells) else ""
        align = aligns[index] if index < len(aligns) else "left"
        rendered_cells.append(_align_cell(cell, width, align))
    return "| " + " | ".join(rendered_cells) + " |"


def _align_cell(text: str, width: int, align: str) -> str:
    visible = _visible_len(text)
    padding = max(0, width - visible)
    if align == "right":
        return " " * padding + text
    return text + " " * padding


def _visible_len(text: str) -> int:
    return len(ANSI_RE.sub("", text))


def _matrix_status(matrix: dict[str, Any], color: bool) -> str:
    checks = matrix.get("checks") if isinstance(matrix, dict) else None
    if not isinstance(checks, list) or not checks:
        return "-"
    counts = {"pass": 0, "fail": 0, "insufficient": 0}
    for check in checks:
        if isinstance(check, dict):
            status = str(check.get("status") or "")
            if status in counts:
                counts[status] += 1
    if counts["fail"]:
        return _style(f"{counts['fail']} fail", color, Ansi.RED)
    if counts["insufficient"]:
        return _style(f"{counts['pass']} pass", color, Ansi.YELLOW)
    return _style(f"{counts['pass']} pass", color, Ansi.GREEN)


def _bar(ratio: float, width: int, color: bool) -> str:
    ratio = max(0.0, min(1.0, ratio))
    filled = int(round(ratio * width))
    text = "[" + "#" * filled + "-" * (width - filled) + "]"
    if ratio >= 0.75:
        return _style(text, color, Ansi.GREEN)
    if ratio >= 0.35:
        return _style(text, color, Ansi.YELLOW)
    return _style(text, color, Ansi.RED)


def _legend(color: bool) -> str:
    return (
        f"{_style('checks', color, Ansi.BOLD)} come from "
        "context_admin(mode='measurement_matrix'); cache bars show hit ratio."
    )


def _style(text: str, color: bool, code: str) -> str:
    return f"{code}{text}{Ansi.RESET}" if color else text


def _trim(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[: max(0, width - 1)] + "~"


def _int_at(payload: dict[str, Any], path: tuple[str, ...]) -> int:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return 0
        value = value.get(key)
    return int(value or 0) if isinstance(value, (int, float)) else 0


def _float_at(payload: dict[str, Any], path: tuple[str, ...]) -> float:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return 0.0
        value = value.get(key)
    return float(value or 0.0) if isinstance(value, (int, float)) else 0.0


def fmt_int(value: Any) -> str:
    number = int(value or 0)
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if abs(number) >= 1_000:
        return f"{number / 1_000:.1f}k"
    return str(number)


def fmt_bytes(value: Any) -> str:
    number = float(value or 0)
    for suffix in ("B", "KiB", "MiB", "GiB"):
        if number < 1024 or suffix == "GiB":
            return f"{number:.1f}{suffix}" if suffix != "B" else f"{int(number)}B"
        number /= 1024
    return f"{number:.1f}GiB"


def fmt_ms(value: Any) -> str:
    number = float(value or 0.0)
    return "-" if number <= 0 else f"{number:.1f}"


def fmt_seconds(value: Any) -> str:
    number = float(value or 0.0)
    if number >= 10:
        return f"{number:.0f}s"
    return f"{number:.1f}s"


def should_use_color(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return sys.stdout.isatty()


def clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")


class RawTerminal:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.fd: int | None = None
        self.original_attrs: list[Any] | None = None

    def __enter__(self) -> "RawTerminal":
        if not self.enabled:
            return self
        self.fd = sys.stdin.fileno()
        self.original_attrs = termios.tcgetattr(self.fd)
        tty.setcbreak(self.fd)
        sys.stdout.write("\033[?25l")
        sys.stdout.flush()
        return self

    def __exit__(self, *_exc: object) -> None:
        if self.enabled and self.fd is not None and self.original_attrs is not None:
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.original_attrs)
        if self.enabled:
            sys.stdout.write("\033[?25h")
            sys.stdout.flush()


def decode_key(sequence: str) -> str | None:
    if sequence in {"\x1b[A", "\x1bOA"}:
        return "up"
    if sequence in {"\x1b[B", "\x1bOB"}:
        return "down"
    if sequence == "\x1b[5~":
        return "page_up"
    if sequence == "\x1b[6~":
        return "page_down"
    if sequence in {"\x1b[H", "\x1bOH"}:
        return "home"
    if sequence in {"\x1b[F", "\x1bOF"}:
        return "end"
    if sequence in {"\r", "\n"}:
        return "enter"
    if sequence == "\x1b":
        return "escape"
    if sequence in {"\x7f", "\b"}:
        return "backspace"
    if sequence == "/":
        return "search"
    if sequence in {"+", "="}:
        return "plus"
    if sequence in {"-", "_"}:
        return "minus"
    if sequence in {"r", "R"}:
        return "refresh"
    if sequence in {"b", "B"}:
        return "browser"
    if sequence == "\x03":
        return "interrupt"
    if sequence in {"q", "Q"}:
        return "quit"
    if len(sequence) == 1 and sequence.isprintable():
        return f"text:{sequence}"
    return None


def read_key(timeout: float, fd: int | None = None) -> str | None:
    fd = fd if fd is not None else sys.stdin.fileno()
    ready, _, _ = select.select([fd], [], [], max(0.0, timeout))
    if not ready:
        return None
    data = os.read(fd, 1).decode("utf-8", errors="ignore")
    if data == "\x1b":
        sequence = data
        while True:
            ready, _, _ = select.select([fd], [], [], 0.01)
            if not ready:
                break
            sequence += os.read(fd, 1).decode("utf-8", errors="ignore")
            if sequence in {
                "\x1b[A",
                "\x1b[B",
                "\x1bOA",
                "\x1bOB",
                "\x1b[5~",
                "\x1b[6~",
                "\x1b[H",
                "\x1b[F",
                "\x1bOH",
                "\x1bOF",
            }:
                break
        return decode_key(sequence)
    return decode_key(data)


def _search_text_for_key(key: str) -> str:
    if key.startswith("text:"):
        return key[len("text:") :]
    return {
        "plus": "+",
        "minus": "-",
        "refresh": "r",
        "browser": "b",
        "quit": "q",
        "search": "/",
    }.get(key, "")


def handle_key(
    key: str,
    state: MonitorState,
    row_count: int,
    state_row_count: int = 0,
) -> str:
    if state.view == "state" and state.state_entry:
        if key in {"quit", "interrupt"}:
            return "quit"
        if key == "escape":
            state.state_entry = None
            state.state_error = ""
            return "redraw"
        return "ignore"

    if state.view == "state" and state.state_search_active:
        if key == "interrupt":
            return "quit"
        if key == "escape":
            state.state_search_active = False
            return "redraw"
        if key == "enter":
            state.state_search_active = False
            state.state_selected_index = _clamped_index(
                state.state_selected_index, state_row_count
            )
            state.state_scroll_offset = 0
            return "redraw"
        if key == "backspace":
            state.state_search = state.state_search[:-1]
            state.state_selected_index = 0
            state.state_scroll_offset = 0
            return "redraw"
        text = _search_text_for_key(key)
        if text:
            state.state_search += text
            state.state_selected_index = 0
            state.state_scroll_offset = 0
            return "redraw"
        return "ignore"

    if key in {"quit", "interrupt"}:
        return "quit"
    if key == "refresh" and state.view != "state":
        return "refresh"
    if key == "browser" and row_count:
        state.view = "state"
        state.state_entry = None
        state.state_selected_index = 0
        state.state_scroll_offset = 0
        return "browser"
    if key == "up":
        if state.view == "state":
            if state.state_entry:
                return "ignore"
            state.state_selected_index = _clamped_index(
                state.state_selected_index - 1, state_row_count
            )
        else:
            state.selected_index = _clamped_index(state.selected_index - 1, row_count)
        return "redraw"
    if key == "down":
        if state.view == "state":
            if state.state_entry:
                return "ignore"
            state.state_selected_index = _clamped_index(
                state.state_selected_index + 1, state_row_count
            )
        else:
            state.selected_index = _clamped_index(state.selected_index + 1, row_count)
        return "redraw"
    if key == "page_up" and state.view == "state":
        state.state_selected_index = _clamped_index(
            state.state_selected_index - 10, state_row_count
        )
        return "redraw"
    if key == "page_down" and state.view == "state":
        state.state_selected_index = _clamped_index(
            state.state_selected_index + 10, state_row_count
        )
        return "redraw"
    if key == "home" and state.view == "state":
        state.state_selected_index = 0
        state.state_scroll_offset = 0
        return "redraw"
    if key == "end" and state.view == "state":
        state.state_selected_index = _clamped_index(state_row_count - 1, state_row_count)
        return "redraw"
    if key == "search" and state.view == "state":
        state.state_search = ""
        state.state_search_active = True
        state.state_entry = None
        state.state_selected_index = 0
        state.state_scroll_offset = 0
        return "redraw"
    if key == "enter" and state.view == "state" and state_row_count:
        if state.state_entry:
            return "ignore"
        return "state_entry"
    if key == "enter" and row_count:
        state.view = "detail"
        return "redraw"
    if key == "escape":
        if state.view == "state" and state.state_entry:
            state.state_entry = None
            state.state_error = ""
            return "redraw"
        if state.view == "state" and state.state_search_active:
            state.state_search_active = False
            return "redraw"
        state.view = "table"
        state.state_error = ""
        return "redraw"
    if key == "plus" and state.view != "state":
        state.refresh_interval = min(
            MAX_INTERVAL, state.refresh_interval + INTERVAL_STEP
        )
        return "redraw"
    if key == "minus" and state.view != "state":
        state.refresh_interval = max(
            MIN_INTERVAL, state.refresh_interval - INTERVAL_STEP
        )
        return "redraw"
    return "ignore"


def run_once(client: Any, args: argparse.Namespace, color: bool) -> int:
    snapshots = collect_snapshots(
        client,
        project_ids=args.project_id,
        root_uri=args.root_uri,
        include_matrix=not args.no_matrix,
    )
    print(render_dashboard(snapshots, args.url, color), flush=True)
    return 0 if all(not row.error for row in snapshots) else 3


def run_poll_loop(client: Any, args: argparse.Namespace, color: bool) -> int:
    while True:
        snapshots = collect_snapshots(
            client,
            project_ids=args.project_id,
            root_uri=args.root_uri,
            include_matrix=not args.no_matrix,
        )
        if not args.no_clear:
            clear_screen()
        print(
            render_dashboard(
                snapshots,
                args.url,
                color,
                refresh_interval=args.interval,
            ),
            flush=True,
        )
        time.sleep(args.interval)


def _state_row_count(state: MonitorState) -> int:
    return len(_filtered_state_rows(state))


def _load_state_browser(
    client: Any,
    state: MonitorState,
    snapshots: list[ProjectSnapshot],
) -> None:
    if not snapshots:
        state.state_payload = None
        state.state_error = "no project selected"
        return
    selected = _clamped_index(state.selected_index, len(snapshots))
    target = snapshots[selected].target
    state.state_target = target
    state.state_entry = None
    try:
        state.state_payload = fetch_state_browser(client, target)
        state.state_error = ""
        state.state_entry = None
        state.state_selected_index = _clamped_index(
            state.state_selected_index, _state_row_count(state)
        )
        state.state_scroll_offset = _adjust_scroll_offset(
            state.state_scroll_offset,
            state.state_selected_index,
            _state_visible_count(shutil.get_terminal_size((120, 30)).lines, False),
            _state_row_count(state),
        )
    except Exception as exc:
        state.state_payload = None
        state.state_error = friendly_mcp_error(exc)


def _load_state_entry(client: Any, state: MonitorState) -> None:
    rows = _filtered_state_rows(state)
    if not rows:
        state.state_error = "no state row selected"
        return
    row = rows[_clamped_index(state.state_selected_index, len(rows))]
    if not isinstance(row, dict):
        state.state_error = "invalid state row"
        return
    key = str(row.get("key") or "")
    if not key or state.state_target is None:
        state.state_error = "no state key selected"
        return
    try:
        state.state_entry = fetch_state_browser(client, state.state_target, state_key=key)
        state.state_error = ""
        state.view = "state"
    except Exception as exc:
        state.state_error = friendly_mcp_error(exc)


def run_interactive_monitor(client: Any, args: argparse.Namespace, color: bool) -> int:
    state = MonitorState(refresh_interval=max(MIN_INTERVAL, args.interval))
    snapshots: list[ProjectSnapshot] = []
    force_refresh = True
    next_refresh = 0.0
    dirty = True
    fd = sys.stdin.fileno()

    with RawTerminal(enabled=True):
        while True:
            now = time.monotonic()
            browser_open = state.view == "state"
            if force_refresh or (not browser_open and now >= next_refresh):
                snapshots = collect_snapshots(
                    client,
                    project_ids=args.project_id,
                    root_uri=args.root_uri,
                    include_matrix=not args.no_matrix,
                )
                state.selected_index = _clamped_index(
                    state.selected_index, len(snapshots)
                )
                state.last_updated = datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                )
                next_refresh = time.monotonic() + state.refresh_interval
                force_refresh = False
                dirty = True

            if dirty:
                if not args.no_clear:
                    clear_screen()
                sys.stdout.write(
                    render_monitor_screen(
                        snapshots,
                        args.url,
                        color,
                        shutil.get_terminal_size((120, 30)).columns,
                        state,
                    )
                )
                sys.stdout.write("\n")
                sys.stdout.flush()
                dirty = False

            timeout = (
                0.5
                if state.view == "state"
                else min(0.5, max(0.0, next_refresh - time.monotonic()))
            )
            key = read_key(timeout, fd=fd)
            if key is None:
                continue
            action = handle_key(
                key,
                state,
                len(snapshots),
                state_row_count=_state_row_count(state),
            )
            if action == "quit":
                return 0
            if action == "browser":
                _load_state_browser(client, state, snapshots)
                dirty = True
            if action == "state_entry":
                _load_state_entry(client, state)
                dirty = True
            if action == "refresh":
                force_refresh = True
            if action in {"redraw", "refresh"}:
                next_refresh = time.monotonic() + state.refresh_interval
                dirty = True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Visualize mcp-context-manager metrics directly through MCP."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"MCP streamable HTTP endpoint (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help=(
            "Refresh interval in seconds. Default is 5 in an interactive terminal "
            "and one-shot output otherwise. 0 runs once and exits."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render one metrics snapshot and exit.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Disable key handling and print refreshed snapshots in a loop.",
    )
    parser.add_argument(
        "--project-id",
        action="append",
        default=[],
        help="Project id to monitor. Repeat for multiple projects.",
    )
    parser.add_argument(
        "--root-uri",
        default="",
        help="Monitor one repository by file:// root URI instead of enumerating projects.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--no-matrix",
        action="store_true",
        help="Skip measurement_matrix calls and render metrics only.",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear the terminal between refreshes.",
    )
    parser.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default="auto",
        help="Color mode (default: auto).",
    )
    args = parser.parse_args()

    client = McpHttpClient(args.url, timeout=args.timeout)
    color = should_use_color(args.color)
    terminal_attached = sys.stdin.isatty() and sys.stdout.isatty()
    interval_was_set = args.interval is not None
    args.interval = (
        float(args.interval)
        if args.interval is not None
        else (DEFAULT_INTERVAL if terminal_attached else 0.0)
    )

    try:
        if args.once or args.interval <= 0:
            return run_once(client, args, color)
        if terminal_attached and not args.non_interactive:
            return run_interactive_monitor(client, args, color)
        if not interval_was_set:
            return run_once(client, args, color)
        return run_poll_loop(client, args, color)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
