#!/usr/bin/env python3
"""
Monitor mcp-context-manager metrics directly through MCP tool calls.

The tool talks to the Streamable HTTP MCP endpoint and calls:

- context_admin(mode="cached_projects") when no project is selected
- context_admin(mode="metrics", project_id=...)
- context_admin(mode="measurement_matrix", project_id=...)
- context_admin(mode="measurement_report", project_id=...)

It renders an interactive terminal dashboard for the persisted project
catalogue. Use one or more explicit selectors to restrict it to projects.
No third-party dependencies are required.

Usage:
  python3 monitor.py
  python3 monitor.py --once
  python3 monitor.py --interval 60
  python3 monitor.py --project-id my-repo-123abc
  python3 monitor.py --url http://127.0.0.1:8000/mcp --color always

Environment:
  MCP_URL (default: http://localhost:8000/mcp)
  MCP_SESSION_ID (optional)
  MCP_EXPECTED_SERVER_VERSION (optional override)
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
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_URL = os.environ.get("MCP_URL", "http://localhost:8000/mcp")
DEFAULT_TIMEOUT = 15.0
DEFAULT_WARMUP_TIMEOUT = 60.0
DEFAULT_INTERVAL = 60.0
# Metrics polling is deliberately serialized. This keeps monitor refreshes
# below one CPU core even when an operator explicitly selects many projects.
DEFAULT_FETCH_WORKERS = 1
MAX_MONITOR_PROJECTS = 32
MIN_INTERVAL = 10.0
MAX_INTERVAL = 3600.0
INTERVAL_STEP = 1.0
DEFAULT_TERMINAL_COLUMNS = 160
DEFAULT_TERMINAL_LINES = 30
STATE_ENTRY_PREVIEW_LIMIT = 64 * 1024
MCP_INSPECTION_MAX_RESOURCES = 16
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
CRITICAL_MATRIX_KEYS = {
    "latency.context_pack.avg_elapsed_ms",
    "latency.context_pack.p95_recent_ms",
    "latency.context_pack.index_refresh_avg_ms",
    "latency.context_admin.warmup.avg_elapsed_ms",
}
EXPECTED_SERVER_VERSION = "2.0.0"
EXPECTED_SERVER_VERSION = (
    os.environ.get("MCP_EXPECTED_SERVER_VERSION", "").strip() or EXPECTED_SERVER_VERSION
)


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
    state_entry_scroll_offset: int = 0
    state_search: str = ""
    state_search_active: bool = False
    state_payload: dict[str, Any] | None = None
    state_entry: dict[str, Any] | None = None
    state_entry_preview_source_id: int = 0
    state_entry_preview: str = ""
    state_entry_preview_truncated: bool = False
    state_entry_preview_width: int = 0
    state_entry_preview_lines: list[str] = field(default_factory=list)
    state_error: str = ""
    state_target: ProjectTarget | None = None
    mcp_status: str = ""
    mcp_error: str = ""
    usage_enabled: bool | None = None
    usage_report: dict[str, Any] | None = None
    usage_status_requested: bool = False
    inspection_payload: dict[str, Any] | None = None
    inspection_error: str = ""
    inspection_selected_index: int = 0
    inspection_viewer_index: int | None = None
    inspection_content_scroll_offset: int = 0


@dataclass
class StateBrowserResult:
    target: ProjectTarget | None
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class StateEntryResult:
    target: ProjectTarget | None
    key: str
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class WarmupResult:
    target: ProjectTarget | None
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class PruneResult:
    target: ProjectTarget | None
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class MonitorUsageResult:
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class McpInspectionResult:
    payload: dict[str, Any] | None = None
    error: str = ""


@dataclass
class McpResourceContentResult:
    uri: str
    content: str = ""
    error: str = ""


@dataclass
class PendingMcpOperation:
    kind: str
    future: Future[Any]


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
        self._lock = RLock()

    def initialize(self) -> None:
        with self._lock:
            if self.initialized:
                return
            initialized = self.rpc(
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "monitor-metrics",
                        "version": EXPECTED_SERVER_VERSION or "unknown",
                    },
                },
            )
            server_info = initialized.get("serverInfo")
            server_version = ""
            if isinstance(server_info, dict):
                server_version = str(server_info.get("version") or "").strip()
            if EXPECTED_SERVER_VERSION and server_version != EXPECTED_SERVER_VERSION:
                actual = server_version or "not reported"
                warning = f"WARNING: monitor expects server {EXPECTED_SERVER_VERSION}, connected server is {actual}"
                print(
                    _style(warning, sys.stderr.isatty(), Ansi.YELLOW),
                    file=sys.stderr,
                )
            self.rpc("notifications/initialized", {}, expect_result=False)
            self.initialized = True

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        result = self.rpc(
            "tools/call",
            {"name": name, "arguments": arguments},
            timeout=self._timeout_for_tool(name, arguments),
        )
        payload = extract_tool_payload(result)
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected tool payload for {name}: {payload!r}")
        return payload

    def list_tools(self) -> dict[str, Any]:
        self.initialize()
        return self.rpc("tools/list")

    def list_resources(self) -> dict[str, Any]:
        self.initialize()
        return self.rpc("resources/list")

    def read_resource(self, uri: str) -> dict[str, Any]:
        self.initialize()
        return self.rpc("resources/read", {"uri": uri})

    def _timeout_for_tool(self, name: str, arguments: dict[str, Any]) -> float:
        if name == "context_admin" and arguments.get("mode") == "warmup":
            return max(self.timeout, DEFAULT_WARMUP_TIMEOUT)
        return self.timeout

    def rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        expect_result: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._request_id += 1
            request_id = self._request_id
            session_id = self.session_id
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
        }
        if expect_result:
            payload["id"] = f"monitor-{request_id}"
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        request = urllib.request.Request(
            _with_session_query(self.url, session_id),
            data=data,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout if timeout is None else timeout
            ) as response:
                response_session = response.headers.get(
                    "Mcp-Session-Id"
                ) or response.headers.get("mcp-session-id")
                if response_session:
                    with self._lock:
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


def collect_snapshots(
    client: Any,
    project_ids: list[str],
    root_uri: str,
    include_matrix: bool,
    max_workers: int = DEFAULT_FETCH_WORKERS,
) -> list[ProjectSnapshot]:
    if root_uri:
        target = ProjectTarget(project_id="", name=root_uri, source="root_uri")
        return [
            _collect_one(
                client, target, root_uri=root_uri, include_matrix=include_matrix
            )
        ]

    # Never discover projects from the monitor. The server's cached catalogue
    # is a bounded v2-state directory list, not a recursive workspace walk.
    if project_ids:
        targets = [ProjectTarget(project_id=pid) for pid in project_ids[:MAX_MONITOR_PROJECTS]]
    else:
        targets = _safe_cached_projects(client)
        if not targets:
            targets = [ProjectTarget(project_id="", name="default", source="default")]
    return _collect_targets(
        client,
        targets,
        root_uri="",
        include_matrix=include_matrix,
        max_workers=max_workers,
    )


def _safe_cached_projects(client: Any) -> list[ProjectTarget]:
    try:
        payload = client.call_tool("context_admin", {"mode": "cached_projects"})
    except Exception:
        return []
    projects = payload.get("projects") if isinstance(payload, dict) else None
    if not isinstance(projects, list):
        return []
    targets = [
        ProjectTarget(
            project_id=str(row.get("project_id") or "").strip(),
            name=str(row.get("name") or ""),
            source=str(row.get("source") or ""),
        )
        for row in projects
        if isinstance(row, dict) and str(row.get("project_id") or "").strip()
    ]
    return sorted(targets, key=lambda item: (item.name.lower(), item.project_id))[
        :MAX_MONITOR_PROJECTS
    ]


def _collect_targets(
    client: Any,
    targets: list[ProjectTarget],
    root_uri: str,
    include_matrix: bool,
    max_workers: int,
) -> list[ProjectSnapshot]:
    if len(targets) <= 1:
        return [
            _collect_one(
                client, targets[0], root_uri=root_uri, include_matrix=include_matrix
            )
        ]
    worker_count = min(max(1, max_workers), len(targets))
    if worker_count == 1:
        return [
            _collect_one(client, target, root_uri=root_uri, include_matrix=include_matrix)
            for target in targets
        ]
    with ThreadPoolExecutor(
        max_workers=worker_count, thread_name_prefix="monitor-fetch"
    ) as executor:
        futures = [
            executor.submit(
                _collect_one,
                client,
                target,
                root_uri,
                include_matrix,
            )
            for target in targets
        ]
        return [future.result() for future in futures]


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


def warmup_project(client: Any, target: ProjectTarget) -> dict[str, Any]:
    root_uri = target.name if target.source == "root_uri" else ""
    return client.call_tool(
        "context_admin",
        {"mode": "warmup", **_selector_args(target, root_uri)},
    )


def prune_project(client: Any, target: ProjectTarget) -> dict[str, Any]:
    root_uri = target.name if target.source == "root_uri" else ""
    return client.call_tool(
        "context_admin",
        {"mode": "cache_prune", **_selector_args(target, root_uri)},
    )


def monitor_usage(
    client: Any, action: str, target: ProjectTarget | None = None
) -> dict[str, Any]:
    arguments: dict[str, Any] = {"mode": "monitor_usage", "action": action}
    if target and target.project_id:
        arguments["project_id"] = target.project_id
    return client.call_tool("context_admin", arguments)


def inspect_mcp_surface(client: Any) -> dict[str, Any]:
    """Fetch only MCP catalogue metadata; resource bodies are on-demand."""
    tools_result = client.list_tools()
    resources_result = client.list_resources()
    tools = tools_result.get("tools") if isinstance(tools_result, dict) else []
    resources = resources_result.get("resources") if isinstance(resources_result, dict) else []
    resource_rows = resources if isinstance(resources, list) else []
    inspected_resources: list[dict[str, Any]] = []
    for resource in resource_rows[:MCP_INSPECTION_MAX_RESOURCES]:
        if not isinstance(resource, dict):
            continue
        row = dict(resource)
        if not str(row.get("uri") or ""):
            row["error"] = "resource has no URI"
        inspected_resources.append(row)
    return {
        "tools": tools if isinstance(tools, list) else [],
        "resources": inspected_resources,
        "resource_count": len(resource_rows),
        "resources_truncated": len(resource_rows) > MCP_INSPECTION_MAX_RESOURCES,
    }


def inspect_mcp_resource_content(client: Any, uri: str) -> McpResourceContentResult:
    """Read one selected resource off the UI thread."""
    try:
        result = client.read_resource(uri)
        contents = result.get("contents") if isinstance(result, dict) else []
        if not isinstance(contents, list) or not contents:
            return McpResourceContentResult(uri=uri)
        first = contents[0]
        if isinstance(first, dict):
            content = first.get("text") or first.get("blob") or ""
        else:
            content = first
        return McpResourceContentResult(
            uri=uri,
            # The viewer is scrollable, so retain the entire resource body.
            # Catalogue cells remain bounded independently for terminal layout.
            content=_format_mcp_resource_content(content),
        )
    except Exception as exc:
        return McpResourceContentResult(uri=uri, error=friendly_mcp_error(exc))


def friendly_mcp_error(exc: Exception) -> str:
    message = str(exc)
    if "write transaction is already active" in message:
        return (
            "The running MCP server hit concurrent LMDB writes during warmup. "
            "Rebuild and restart it with the current checkout so store writes are "
            "serialized, for example: "
            "MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up -d --build"
        )
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
    if (
        "context_adminArguments" in message
        and "warmup" in message
        and "literal_error" in message
    ):
        return (
            "The running MCP server does not support "
            "context_admin(mode='warmup') yet. Rebuild and restart it with "
            "the current checkout, for example: "
            "MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up -d --build"
        )
    if (
        "context_adminArguments" in message
        and "cache_prune" in message
        and "literal_error" in message
    ):
        return (
            "The running MCP server does not support "
            "context_admin(mode='cache_prune') yet. Rebuild and restart it with "
            "the current checkout, for example: "
            "MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up -d --build"
        )
    return message


def _collect_one(
    client: Any,
    target: ProjectTarget,
    root_uri: str,
    include_matrix: bool,
) -> ProjectSnapshot:
    selector = _selector_args(target, root_uri)
    snapshot = ProjectSnapshot(target=target)
    try:
        if include_matrix:
            payload = client.call_tool(
                "context_admin", {"mode": "measurement_report", **selector}
            )
            if (
                isinstance(payload, dict)
                and isinstance(payload.get("metrics"), dict)
                and isinstance(payload.get("matrix"), dict)
            ):
                snapshot.metrics = payload["metrics"]
                snapshot.matrix = payload["matrix"]
            else:
                snapshot.metrics = client.call_tool(
                    "context_admin", {"mode": "metrics", **selector}
                )
                snapshot.matrix = client.call_tool(
                    "context_admin", {"mode": "measurement_matrix", **selector}
                )
        else:
            snapshot.metrics = client.call_tool(
                "context_admin", {"mode": "metrics", **selector}
            )
    except Exception as exc:
        snapshot.error = str(exc)
    return snapshot


def _selector_args(target: ProjectTarget, root_uri: str) -> dict[str, str]:
    if target.project_id:
        return {"project_id": target.project_id}
    if root_uri:
        return {"root_uri": root_uri}
    return {}


def render_dashboard(
    snapshots: list[ProjectSnapshot],
    url: str,
    color: bool,
    width: int | None = None,
    selected_index: int | None = None,
    refresh_interval: float | None = None,
    status_line: str = "",
) -> str:
    width = (
        width
        or shutil.get_terminal_size(
            (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
        ).columns
    )
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ok_rows = [row for row in snapshots if row.metrics and not row.error]
    totals = _aggregate_totals(ok_rows)
    lines = [
        _style("mcp-context-manager metrics", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        f"updated:  {now}   projects: {len(snapshots)}   ok: {len(ok_rows)}   errors: {len(snapshots) - len(ok_rows)}",
        "",
    ]
    lines.extend(_summary_table(totals, snapshots, ok_rows, color))
    lines.extend(
        [
            "",
            *_project_table(snapshots, color, width, selected_index=selected_index),
        ]
    )
    lines.extend(["", _legend(color)])
    if refresh_interval is not None:
        lines.append(_controls(refresh_interval, color, status_line=status_line))
    return "\n".join(lines)


def render_monitor_screen(
    snapshots: list[ProjectSnapshot],
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
) -> str:
    if state.view == "inspection":
        return render_mcp_inspection(url=url, color=color, width=width, state=state)
    if state.view == "state":
        return render_state_browser(url=url, color=color, width=width, state=state)
    if state.view == "performance" and snapshots:
        selected = _clamped_index(state.selected_index, len(snapshots))
        return render_performance_view(
            snapshots[selected],
            url=url,
            color=color,
            width=width,
            state=state,
        )
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
        status_line=_mcp_status_line(state, color),
    )


def render_mcp_inspection(
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
    height: int | None = None,
) -> str:
    """Render the MCP catalogue or one selected item in a dedicated viewer."""
    terminal_size = shutil.get_terminal_size(
        (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
    )
    width = width or terminal_size.columns
    height = height or terminal_size.lines
    payload = state.inspection_payload or {}
    rows = _inspection_rows(payload)
    lines = [
        _style("mcp-context-manager MCP inspection", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        "",
    ]
    if state.inspection_error:
        lines.append(_style(f"ERROR: {state.inspection_error}", color, Ansi.RED))
    elif not payload:
        lines.append("Loading MCP tools and resources...")
    elif state.inspection_viewer_index is not None:
        item = _inspection_item_at(state, state.inspection_viewer_index)
        if item is None:
            state.inspection_viewer_index = None
            lines.append("The selected item is no longer available.")
        else:
            lines.extend(_render_mcp_inspection_viewer(item, color, width, height, state))
    else:
        selected_index = _clamped_index(state.inspection_selected_index, len(rows))
        kind_width, name_width, uri_width, description_width = _inspection_table_widths(width)
        table_rows = []
        for index, (kind, item) in enumerate(rows):
            table_rows.append(
                (
                    ">" if index == selected_index else " ",
                    _trim(kind, kind_width),
                    _trim(str(item.get("name") or item.get("uri") or "-"), name_width),
                    _trim(str(item.get("uri") or "-"), uri_width),
                    _trim(
                        _bounded_text(item.get("description"), 1_000)
                        or "(no description supplied)",
                        description_width,
                    ),
                )
            )
        resource_total = _int_at(payload, ("resource_count",)) or sum(
            1 for kind, _item in rows if kind == "resource"
        )
        resource_suffix = "; resources truncated" if payload.get("resources_truncated") else ""
        lines.append(
            _style(
                f"CATALOGUE  {len(rows)} items ({resource_total} resources{resource_suffix})",
                color,
                Ansi.BOLD + Ansi.CYAN,
            )
        )
        lines.extend(
            _render_table(
                ("", "type", "name", "URI", "description"),
                table_rows,
                widths=(1, kind_width, name_width, uri_width, description_width),
                aligns=("left", "left", "left", "left", "left"),
            )
        )
        lines.extend(["", "Enter opens the selected item.  Up/Down and PgUp/PgDn navigate."])
    lines.append(
        _controls(state.refresh_interval, color, status_line=_mcp_status_line(state, color))
    )
    return "\n".join(lines)


def _inspection_table_widths(width: int) -> tuple[int, int, int, int]:
    # Five table columns add sixteen border/separator characters.
    available = max(54, width - 16)
    kind = 8
    name = min(28, max(14, width // 5))
    uri = min(38, max(16, width // 3))
    return kind, name, uri, max(16, available - 1 - kind - name - uri)


def _inspection_rows(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    tools = payload.get("tools") if isinstance(payload.get("tools"), list) else []
    resources = payload.get("resources") if isinstance(payload.get("resources"), list) else []
    rows: list[tuple[str, dict[str, Any]]] = []
    rows.extend(("tool", tool) for tool in tools if isinstance(tool, dict))
    rows.extend(("resource", resource) for resource in resources if isinstance(resource, dict))
    return rows


def _inspection_item_at(
    state: MonitorState, index: int | None = None
) -> tuple[str, dict[str, Any]] | None:
    rows = _inspection_rows(state.inspection_payload or {})
    if not rows:
        return None
    selected = _clamped_index(
        state.inspection_selected_index if index is None else index, len(rows)
    )
    if index is None:
        state.inspection_selected_index = selected
    return rows[selected]


def _render_mcp_inspection_viewer(
    item: tuple[str, dict[str, Any]],
    color: bool,
    width: int,
    height: int,
    state: MonitorState,
) -> list[str]:
    kind, value = item
    name = str(value.get("name") or value.get("uri") or kind)
    lines = [_style(f"{kind.upper()}  {name}", color, Ansi.BOLD + Ansi.CYAN)]
    if kind == "tool":
        body = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    elif value.get("error"):
        body = f"ERROR: {value['error']}"
    elif "content" in value:
        body = str(value.get("content") or "(empty content)")
    elif value.get("error") == "resource has no URI":
        body = "ERROR: resource has no URI"
    else:
        body = "Loading resource content..."

    if kind == "resource":
        metadata = {
            key: value[key]
            for key in ("uri", "name", "description", "mimeType")
            if value.get(key) is not None
        }
        metadata_text = json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True)
        body = f"{metadata_text}\n\nCONTENT\n{body}"
    wrapped = _wrap_block(body, max(40, width - 2))
    visible_count = max(3, height - len(lines) - 7)
    state.inspection_content_scroll_offset = _clamp_content_scroll_offset(
        state.inspection_content_scroll_offset, visible_count, len(wrapped)
    )
    start = state.inspection_content_scroll_offset
    visible = wrapped[start : start + visible_count]
    lines.extend(
        [
            "",
            _style(
                f"lines {start + 1}-{start + len(visible)} / {len(wrapped)}",
                color,
                Ansi.BOLD,
            ),
            *visible,
            "",
            "Up/Down scroll  PgUp/PgDn page  Home/End jump  Esc catalogue",
        ]
    )
    return lines


def _format_mcp_resource_content(content: Any) -> str:
    """Pretty-print JSON resource bodies while preserving plain-text resources."""
    if isinstance(content, str):
        text = content
    else:
        return json.dumps(content, ensure_ascii=False, indent=2)
    try:
        return json.dumps(json.loads(text), ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        return text


def _selected_inspection_resource(state: MonitorState) -> dict[str, Any] | None:
    selected = _inspection_item_at(state)
    if selected is None or selected[0] != "resource":
        return None
    return selected[1]


def _inspection_resource_for_uri(state: MonitorState, uri: str) -> dict[str, Any] | None:
    for kind, item in _inspection_rows(state.inspection_payload or {}):
        if kind == "resource" and item.get("uri") == uri:
            return item
    return None


def _bounded_text(value: Any, limit: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    text = value.replace("\x00", "")
    if len(text) <= limit:
        return text
    # Keep the preview bounded without adding a visible truncation suffix to
    # field names or resource content.
    return text[:limit]


def render_project_detail(
    snapshot: ProjectSnapshot,
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
) -> str:
    width = (
        width
        or shutil.get_terminal_size(
            (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
        ).columns
    )
    metrics = snapshot.metrics or {}
    if _is_native_metrics(metrics):
        details = _native_project_details(metrics, snapshot, url, state)
    else:
        details = _legacy_project_details(metrics, snapshot, url, state)
    lines = [
        _style("mcp-context-manager project details", color, Ansi.BOLD + Ansi.CYAN),
        "",
    ]
    lines.extend(_render_table(("field", "value"), details, aligns=("left", "left")))
    if snapshot.error:
        lines.extend(["", _style(f"ERROR: {snapshot.error}", color, Ansi.RED)])
    check_rows = _measurement_check_rows(snapshot.matrix or {}, color)
    if check_rows:
        key_width = max(
            24, min(max(_visible_len(row[0]) for row in check_rows), width - 64)
        )
        description_width = max(14, max(_visible_len(row[4]) for row in check_rows))
        lines.extend(
            [
                "",
                *_render_table(
                    ("check", "status", "current", "target", "description"),
                    check_rows,
                    widths=(key_width, 12, 10, 10, description_width),
                    aligns=("left", "right", "right", "right", "left"),
                ),
            ]
        )
    lines.append(
        _controls(
            state.refresh_interval,
            color,
            status_line=_mcp_status_line(state, color),
        )
    )
    return "\n".join(lines)


def _legacy_project_details(
    metrics: dict[str, Any],
    snapshot: ProjectSnapshot,
    url: str,
    state: MonitorState,
) -> list[tuple[str, str]]:
    """Keep the historical Python monitor details readable during rollout."""

    fragment_hits = _int_at(metrics, ("cache", "context_pack_fragment_hits"))
    fragment_misses = _int_at(metrics, ("cache", "context_pack_fragment_misses"))
    fragment_ratio = _fragment_cache_ratio(metrics)
    return [
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
        (
            "fragment cache",
            f"{fragment_hits}/{fragment_misses} h/m  {fragment_ratio * 100:5.1f}%",
        ),
        (
            "candidate compact.",
            fmt_int(_tokens_spared_by_mcp(metrics)),
        ),
        (
            "refs deferred",
            fmt_bytes(_int_at(metrics, ("references", "bytes_deferred_est"))),
        ),
    ]


def _native_project_details(
    metrics: dict[str, Any],
    snapshot: ProjectSnapshot,
    url: str,
    state: MonitorState,
) -> list[tuple[str, str]]:
    """Render native v2 counters rather than obsolete fragment-cache fields."""

    l0 = _cache_l0(metrics)
    l1 = _cache_l1(metrics)
    retrieval = _mapping_at(metrics, ("retrieval",))
    freshness = _mapping_at(metrics, ("index_freshness",))
    references = _mapping_at(metrics, ("references",))
    pack_tokens = _mapping_at(metrics, ("tokens", "context_pack"))
    return [
        ("project", _project_name(snapshot.target)),
        ("project id", _project_identifier(snapshot.target)),
        ("endpoint", url),
        ("updated", state.last_updated or "-"),
        ("engine", _native_engine_status(metrics)),
        ("requests", fmt_int(_int_at(metrics, ("requests", "total")))),
        ("context_pack", fmt_int(_context_pack_count(metrics))),
        ("avg pack ms", fmt_ms(_context_pack_avg_ms(metrics))),
        ("L0 pack cache", _cache_hit_summary(l0)),
        ("L0 miss causes", _l0_miss_reason_summary(l0)),
        (
            "L0 storage",
            f"{fmt_int(l0.get('entries', 0))} entries, {fmt_bytes(l0.get('weighted_bytes', 0))}",
        ),
        (
            "L1 frontier",
            f"{fmt_int(l1.get('exact_hits', 0))} exact / "
            f"{fmt_int(l1.get('approximate_hits', 0))} approximate hits",
        ),
        ("retrieval misses", fmt_int(_native_retrieval_misses(metrics))),
        (
            "index",
            f"{retrieval.get('backend', '-')}, {fmt_int(retrieval.get('doc_count', 0))} chunks",
        ),
        (
            "freshness",
            f"gen {fmt_int(freshness.get('generation', 0))}, "
            f"{'dirty' if freshness.get('dirty') else 'clean'}, "
            f"{fmt_int(freshness.get('refreshes', 0))} refreshes",
        ),
        (
            "references",
            f"{fmt_int(references.get('active_count', 0))} active / "
            f"{fmt_int(references.get('total_count', 0))} total",
        ),
        ("compression", _compression_factor_summary(pack_tokens)),
        (
            "tokens saved est.",
            f"{fmt_int(pack_tokens.get('saved_tokens_est', 0))} source-to-wire / "
            f"{fmt_int(pack_tokens.get('delta_tokens_saved_est', 0))} delta",
        ),
        (
            "token estimates",
            f"{fmt_int(pack_tokens.get('selected_source_tokens_est', 0))} source / "
            f"{fmt_int(pack_tokens.get('evidence_card_tokens_est', 0))} cards / "
            f"{fmt_int(pack_tokens.get('returned_evidence_tokens_est', 0))} returned / "
            f"{fmt_int(pack_tokens.get('wire_tokens_est', 0))} wire "
            f"({fmt_bytes(pack_tokens.get('wire_bytes', 0))})",
        ),
    ]


def render_performance_view(
    snapshot: ProjectSnapshot,
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
) -> str:
    width = (
        width
        or shutil.get_terminal_size(
            (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
        ).columns
    )
    metrics = snapshot.metrics or {}
    freshness = metrics.get("index_freshness", {})
    freshness = freshness if isinstance(freshness, dict) else {}
    background = metrics.get("background", {})
    background = background if isinstance(background, dict) else {}
    native_metrics = _is_native_metrics(metrics)
    stage_column_overhead = (6 if native_metrics else 4) * 10 + (7 if native_metrics else 5) * 3 + 1
    stage_column_width = max(
        24,
        min(48, width - stage_column_overhead),
    )
    stage_rows = _performance_stage_rows(metrics)
    cache_rows = _performance_cache_rows(metrics, background, freshness)
    if native_metrics:
        stage_table = _render_table(
            ("operation", "avg ms", "p50 ms", "p95 ms", "min ms", "max ms", "last ms"),
            stage_rows,
            widths=(stage_column_width, 9, 9, 9, 9, 9, 9),
            aligns=("left", "right", "right", "right", "right", "right", "right"),
        )
    else:
        stage_table = _render_table(
            ("stage", "avg ms", "min ms", "max ms", "last ms"),
            stage_rows,
            widths=(stage_column_width, 10, 10, 10, 10),
            aligns=("left", "right", "right", "right", "right"),
        )
    lines = [
        _style("mcp-context-manager performance", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        f"project:  {_project_name(snapshot.target)}",
        "",
        *stage_table,
        "",
        *_render_table(
            ("signal", "value"),
            cache_rows,
            widths=(20, max(20, width - 27)),
            aligns=("left", "left"),
        ),
    ]
    report = state.usage_report if isinstance(state.usage_report, dict) else {}
    buckets = report.get("buckets") if isinstance(report.get("buckets"), list) else []
    request_count = sum(_int_at(row, ("request_count",)) for row in buckets if isinstance(row, dict))
    elapsed_micros = sum(_int_at(row, ("elapsed_micros_total",)) for row in buckets if isinstance(row, dict))
    input_tokens = sum(_int_at(row, ("input_tokens_est",)) for row in buckets if isinstance(row, dict))
    wire_tokens = sum(_int_at(row, ("wire_tokens_est",)) for row in buckets if isinstance(row, dict))
    l0_hits = sum(_int_at(row, ("cache_outcomes", "l0_hit")) for row in buckets if isinstance(row, dict))
    l0_misses = sum(_int_at(row, ("cache_outcomes", "l0_miss")) for row in buckets if isinstance(row, dict))
    frontier_hits = sum(_int_at(row, ("frontier_outcomes", "exact_hit")) for row in buckets if isinstance(row, dict))
    frontier_admitted = sum(_int_at(row, ("frontier_outcomes", "admitted")) for row in buckets if isinstance(row, dict))
    capacity_fallbacks = sum(_int_at(row, ("frontier_outcomes", "capacity_fallback")) for row in buckets if isinstance(row, dict))
    source_fallbacks = sum(_int_at(row, ("frontier_outcomes", "source_fallback")) for row in buckets if isinstance(row, dict))
    base_pack_requests = sum(_int_at(row, ("delta", "base_pack_requests")) for row in buckets if isinstance(row, dict))
    delta_saved = sum(_int_at(row, ("delta_tokens_saved_est",)) for row in buckets if isinstance(row, dict))
    refresh_updates = sum(_int_at(row, ("index", "refresh_updated")) for row in buckets if isinstance(row, dict))
    average_ms = elapsed_micros / request_count / 1000.0 if request_count else 0.0
    enabled_label = "enabled" if state.usage_enabled else "disabled"
    if state.usage_enabled is None:
        enabled_label = "unknown (press l)"
    lines.extend(
        [
            "",
            _style("monitor-only detailed usage", color, Ansi.BOLD),
            *_render_table(
                ("signal", "value"),
                (
                    ("global collection", enabled_label),
                    ("selected project", f"{fmt_int(request_count)} requests / {average_ms:.2f} avg ms"),
                    ("30-day tokens", f"{fmt_int(input_tokens)} input / {fmt_int(wire_tokens)} wire"),
                    ("L0 opportunity", f"{fmt_int(l0_hits)} hits / {fmt_int(l0_misses)} misses"),
                    ("frontier opportunity", f"{fmt_int(frontier_hits)} hits / {fmt_int(frontier_admitted)} admitted / {fmt_int(capacity_fallbacks + source_fallbacks)} fallbacks"),
                    ("delta adoption", f"{fmt_int(base_pack_requests)} requests / {fmt_int(delta_saved)} tokens saved"),
                    ("index invalidation", f"{fmt_int(refresh_updates)} refresh updates"),
                ),
                widths=(20, max(20, width - 27)),
                aligns=("left", "left"),
            ),
        ]
    )
    if snapshot.error:
        lines.extend(["", _style(f"ERROR: {snapshot.error}", color, Ansi.RED)])
    lines.append(
        _controls(
            state.refresh_interval,
            color,
            status_line=_mcp_status_line(state, color),
        )
    )
    return "\n".join(lines)


def render_state_browser(
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
    height: int | None = None,
) -> str:
    if state.state_entry:
        return render_state_entry_view(
            url=url, color=color, width=width, state=state, height=height
        )
    terminal_size = shutil.get_terminal_size(
        (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
    )
    width = width or terminal_size.columns
    height = height or terminal_size.lines
    payload = state.state_payload or {}
    rows = _filtered_state_rows(state)
    visible_count = _state_visible_count(height, False)
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
        "",
    ]
    if state.state_error:
        lines.append(_style(f"ERROR: {state.state_error}", color, Ansi.RED))
        lines.append(
            _browser_controls(state, color, status_line=_mcp_status_line(state, color))
        )
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
            selected=(state.state_scroll_offset + index == state.state_selected_index),
        )
        for index, row in enumerate(visible_rows)
        if isinstance(row, dict)
    ]
    lines.extend(
        _render_table(
            ("", "key", "class", "type", "size", "created", "schema", "status"),
            table_rows,
            widths=_state_table_widths(width),
            aligns=("left", "left", "left", "left", "right", "left", "left", "left"),
        )
    )
    if not table_rows:
        lines.append("No generated-state rows for this project.")
    lines.append(
        _browser_controls(state, color, status_line=_mcp_status_line(state, color))
    )
    return "\n".join(lines)


def render_state_entry_view(
    url: str,
    color: bool,
    width: int | None,
    state: MonitorState,
    height: int | None = None,
) -> str:
    terminal_size = shutil.get_terminal_size(
        (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
    )
    width = width or terminal_size.columns
    height = height or terminal_size.lines
    payload = state.state_entry or {}
    wrapped_entry = payload.get("entry") if isinstance(payload, dict) else None
    entry = wrapped_entry
    # state_browser entry responses expose the entry fields at the top level;
    # keep accepting the older wrapped shape for compatibility.
    if not isinstance(entry, dict):
        entry = payload if isinstance(payload, dict) else {}
    value = entry.get("value")
    if state.state_entry_preview_source_id != id(payload):
        preview, truncated = _serialize_state_entry_preview(entry)
        state.state_entry_preview_source_id = id(payload)
        state.state_entry_preview = preview
        state.state_entry_preview_truncated = truncated
        state.state_entry_preview_width = 0
        state.state_entry_preview_lines = []
    preview = state.state_entry_preview
    browser_envelope = (
        wrapped_entry is None
        and entry.get("schema") == "context_state_browser.v1"
        and entry.get("mode") == "entry"
    )

    def metadata_value(name: str) -> Any:
        sources = (value, entry) if browser_envelope else (entry, value)
        for source in sources:
            if isinstance(source, dict) and source.get(name) not in (None, ""):
                return source[name]
        return None

    value_type = entry.get("value_type")
    if not value_type and "value" in entry:
        value_type = (
            "dict" if isinstance(value, dict)
            else "list" if isinstance(value, list)
            else "scalar"
        )
    size_chars = entry.get("size_chars")
    if size_chars is None and "value" in entry:
        size_chars = len(preview)
    status = metadata_value("status")
    if not status and "value" in entry:
        status = "present" if entry.get("found", True) else "missing"
    expires_at = metadata_value("expires_at")
    if expires_at is None and "value" in entry:
        expires_at = "not set"
    lines = _render_table(
        ("field", "value"),
        [
            ("key", entry.get("key", "-")),
            ("type", value_type or "-"),
            ("size", fmt_int(size_chars or 0)),
            ("schema", metadata_value("schema") or "-"),
            ("status", status or "-"),
            ("expires", expires_at or "-"),
        ],
        aligns=("left", "left"),
    )
    preview_width = max(40, width - 2)
    if state.state_entry_preview_width != preview_width:
        state.state_entry_preview_lines = _wrap_block(preview, preview_width)
        state.state_entry_preview_width = preview_width
    preview_lines = state.state_entry_preview_lines
    visible_count = _state_entry_visible_count(height)
    state.state_entry_scroll_offset = _clamp_content_scroll_offset(
        state.state_entry_scroll_offset,
        visible_count,
        len(preview_lines),
    )
    visible_preview = preview_lines[
        state.state_entry_scroll_offset : state.state_entry_scroll_offset
        + visible_count
    ]
    first_line = state.state_entry_scroll_offset + 1 if preview_lines else 0
    last_line = state.state_entry_scroll_offset + len(visible_preview)
    project = _project_name(state.state_target) if state.state_target else "-"
    body = [
        _style("mcp-context-manager state entry", color, Ansi.BOLD + Ansi.CYAN),
        f"endpoint: {url}",
        f"project:  {project}",
        "",
        *lines,
        "",
        _style(
            (
                f"preview lines {first_line}-{last_line} / {len(preview_lines)}"
                + (" (truncated)" if state.state_entry_preview_truncated else "")
            ),
            color,
            Ansi.BOLD,
        ),
        *visible_preview,
    ]
    if state.state_error:
        body.extend(["", _style(f"ERROR: {state.state_error}", color, Ansi.RED)])
    body.append(
        _browser_controls(state, color, status_line=_mcp_status_line(state, color))
    )
    return "\n".join(body)


def _serialize_state_entry_preview(entry: dict[str, Any]) -> tuple[str, bool]:
    if "preview" in entry:
        preview = str(entry.get("preview") or "")
        try:
            preview = json.dumps(
                json.loads(preview), ensure_ascii=False, indent=2, sort_keys=True
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        return preview[:STATE_ENTRY_PREVIEW_LIMIT], len(preview) > STATE_ENTRY_PREVIEW_LIMIT
    if "value" not in entry:
        return "", False

    value = entry.get("value")
    chunks: list[str] = []
    size = 0
    try:
        encoder = json.JSONEncoder(ensure_ascii=False, indent=2, sort_keys=True)
        for chunk in encoder.iterencode(value):
            remaining = STATE_ENTRY_PREVIEW_LIMIT - size
            if len(chunk) > remaining:
                chunks.append(chunk[:remaining])
                return "".join(chunks), True
            chunks.append(chunk)
            size += len(chunk)
    except (TypeError, ValueError):
        preview = str(value)
        return preview[:STATE_ENTRY_PREVIEW_LIMIT], len(preview) > STATE_ENTRY_PREVIEW_LIMIT
    return "".join(chunks), False


def _aggregate_totals(rows: list[ProjectSnapshot]) -> dict[str, Any]:
    totals = {
        "requests": 0,
        "packs": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "l1_exact_hits": 0,
        "l1_approximate_hits": 0,
        "retrieval_misses": 0,
        "active_references": 0,
        "total_references": 0,
        "pack_elapsed_ms": 0.0,
        "source_tokens_est": 0,
        "wire_tokens_est": 0,
        "saved_tokens_est": 0,
        "fragment_hits": 0,
        "fragment_misses": 0,
        "tokens_spared_by_mcp": 0,
        "bytes_deferred": 0,
        "native_rows": 0,
    }
    for row in rows:
        metrics = row.metrics or {}
        if _is_native_metrics(metrics):
            totals["native_rows"] += 1
        totals["requests"] += _int_at(metrics, ("requests", "total"))
        totals["packs"] += _int_at(
            metrics, ("requests", "by_operation", "context_pack", "count")
        )
        totals["cache_hits"] += _int_at(metrics, ("cache", "hits"))
        totals["cache_misses"] += _int_at(metrics, ("cache", "misses"))
        totals["l1_exact_hits"] += _int_at(
            metrics, ("cache", "l1", "exact_hits")
        )
        totals["l1_approximate_hits"] += _int_at(
            metrics, ("cache", "l1", "approximate_hits")
        )
        totals["retrieval_misses"] += _native_retrieval_misses(metrics)
        totals["active_references"] += _int_at(metrics, ("references", "active_count"))
        totals["total_references"] += _int_at(metrics, ("references", "total_count"))
        totals["pack_elapsed_ms"] += _context_pack_count(metrics) * _context_pack_avg_ms(metrics)
        pack_tokens = _mapping_at(metrics, ("tokens", "context_pack"))
        totals["source_tokens_est"] += _int_at(
            pack_tokens, ("selected_source_tokens_est",)
        )
        totals["wire_tokens_est"] += _int_at(pack_tokens, ("wire_tokens_est",))
        totals["saved_tokens_est"] += _int_at(pack_tokens, ("saved_tokens_est",))
        totals["fragment_hits"] += _int_at(
            metrics, ("cache", "context_pack_fragment_hits")
        )
        totals["fragment_misses"] += _int_at(
            metrics, ("cache", "context_pack_fragment_misses")
        )
        totals["tokens_spared_by_mcp"] += _tokens_spared_by_mcp(metrics)
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
    if not totals["native_rows"]:
        return _legacy_summary_table(totals, snapshots, ok_rows, color)
    cache_total = int(totals["cache_hits"]) + int(totals["cache_misses"])
    cache_ratio = (
        int(totals["cache_hits"]) / cache_total if cache_total else 0.0
    )
    packs = int(totals["packs"])
    average_pack_ms = float(totals["pack_elapsed_ms"]) / packs if packs else 0.0
    source_tokens = int(totals["source_tokens_est"])
    wire_tokens = int(totals["wire_tokens_est"])
    compression_factor = source_tokens / wire_tokens if wire_tokens else 0.0
    rows = [
        ("projects", f"{len(snapshots)} total / {len(ok_rows)} ok"),
        ("requests", fmt_int(totals["requests"])),
        ("context_pack", fmt_int(totals["packs"])),
        ("avg pack ms", fmt_ms(average_pack_ms)),
        (
            "L0 pack cache",
            f"{_bar(cache_ratio, 18, color)} {cache_ratio * 100:5.1f}%",
        ),
        (
            "L1 frontier",
            f"{fmt_int(totals['l1_exact_hits'])} exact / "
            f"{fmt_int(totals['l1_approximate_hits'])} approximate",
        ),
        ("retrieval misses", fmt_int(totals["retrieval_misses"])),
        (
            "compression",
            f"{compression_factor:.2f}x source-to-wire" if compression_factor else "not observed",
        ),
        ("tokens saved est.", fmt_int(totals["saved_tokens_est"])),
        (
            "refs active",
            f"{fmt_int(totals['active_references'])} / {fmt_int(totals['total_references'])}",
        ),
    ]
    return _render_table(("metric", "value"), rows, aligns=("left", "right"))


def _legacy_summary_table(
    totals: dict[str, Any],
    snapshots: list[ProjectSnapshot],
    ok_rows: list[ProjectSnapshot],
    color: bool,
) -> list[str]:
    fragment_total = int(totals["fragment_hits"]) + int(totals["fragment_misses"])
    fragment_ratio = (
        int(totals["fragment_hits"]) / fragment_total if fragment_total else 0.0
    )
    rows = [
        ("projects", f"{len(snapshots)} total / {len(ok_rows)} ok"),
        ("requests", fmt_int(totals["requests"])),
        ("context_pack", fmt_int(totals["packs"])),
        (
            "fragment cache",
            f"{_bar(fragment_ratio, 18, color)} {fragment_ratio * 100:5.1f}%",
        ),
        ("candidate compact.", fmt_int(totals["tokens_spared_by_mcp"])),
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
    native = any(_is_native_metrics(snapshot.metrics or {}) for snapshot in snapshots)
    rows = [
        _project_cells(snapshot, color, widths, selected=index == selected_index)
        for index, snapshot in enumerate(snapshots)
    ]
    rendered = _render_table(
        (
            "",
            "project",
            "project id",
            "req",
            "pack",
            "avg ms",
            "L0 cache" if native else "cache",
            "L1 reuse" if native else "cand tok",
            "checks",
        ),
        rows,
        widths=(
            widths["selector"],
            widths["project"],
            widths["project_id"],
            widths["req"],
            widths["pack"],
            widths["avg_ms"],
            widths["cache"],
            widths["l1"],
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
            rendered[selected_row] = _style_full_line(
                rendered[selected_row], color, Ansi.REVERSE
            )
    return rendered


def _project_column_widths(width: int) -> dict[str, int]:
    widths = {
        "selector": 1,
        "project": 20,
        "project_id": 50,
        "req": 4,
        "pack": 4,
        "avg_ms": 6,
        "cache": 16,
        "l1": 9,
        "checks": 9,
    }
    target_width = min(DEFAULT_TERMINAL_COLUMNS, max(100, width))
    overhead = len(widths) * 3 + 1
    while overhead + sum(widths.values()) > target_width and widths["project_id"] > 12:
        widths["project_id"] -= 1
    while overhead + sum(widths.values()) > target_width and widths["project"] > 16:
        widths["project"] -= 1
    if widths["project"] < 26 and widths["project_id"] > 12:
        shift = min(26 - widths["project"], widths["project_id"] - 12)
        widths["project"] += shift
        widths["project_id"] -= shift
    extra = target_width - (overhead + sum(widths.values()))
    if extra > 0:
        widths["project"] += extra
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
    checks = _matrix_status(snapshot.matrix or {}, color)
    cache_bar_width = max(6, widths["cache"] - 9)
    if _is_native_metrics(metrics):
        cache_ratio = _cache_hit_ratio(metrics)
        l1 = _cache_l1(metrics)
        l1_reuse = f"{fmt_int(l1.get('exact_hits', 0))}/{fmt_int(l1.get('approximate_hits', 0))}"
    else:
        cache_ratio = _fragment_cache_ratio(metrics)
        l1_reuse = fmt_int(_tokens_spared_by_mcp(metrics))
    return (
        selector,
        project,
        project_id,
        fmt_int(requests),
        fmt_int(packs),
        fmt_ms(avg_ms),
        f"{_bar(cache_ratio, cache_bar_width, color)} {cache_ratio * 100:5.1f}%",
        l1_reuse,
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


def _controls(
    refresh_interval: float,
    color: bool,
    status_line: str = "",
) -> str:
    keys = (
        "keys: Up/Down select  Enter open  p performance  l detailed usage  i inspect  b state  Esc table  "
        "PgUp/PgDn navigate  +/- refresh  r reload  w warmup  P prune  q quit"
    )
    controls = (
        f"{keys}   refresh={fmt_seconds(refresh_interval)}"
        if not color
        else f"{_style(keys, color, Ansi.DIM)}   refresh={fmt_seconds(refresh_interval)}"
    )
    return f"{controls}   {status_line}" if status_line else controls


def _mcp_status_line(state: MonitorState, color: bool) -> str:
    detailed = (
        "unknown"
        if state.usage_enabled is None
        else "on" if state.usage_enabled else "off"
    )
    parts = [f"detailed={detailed}"]
    if state.mcp_error:
        parts.append(_style(f"mcp: {state.mcp_error}", color, Ansi.RED))
    elif state.mcp_status:
        parts.append(_style(f"mcp: {state.mcp_status}", color, Ansi.YELLOW))
    return "   ".join(parts)


def _browser_controls(
    state: MonitorState,
    color: bool,
    status_line: str = "",
) -> str:
    return _controls(state.refresh_interval, color, status_line=status_line)


def _measurement_check_rows(
    matrix: dict[str, Any],
    color: bool,
) -> list[tuple[str, str, str, str, str]]:

    fallback_descriptions = {
        "latency.context_pack.avg_elapsed_ms": "Average context-pack request latency",
        "latency.context_pack.p95_recent_ms": "Recent p95 context-pack request latency",
        "latency.context_pack.index_refresh_avg_ms": "Average index refresh time during context pack",
        "latency.context_pack.snippet_batch_avg_ms": "Average snippet batch time during context pack",
        "latency.context_pack.p95_elapsed_ms": "P95 context-pack request latency",
        "latency.context_pack.min_elapsed_ms": "Minimum context-pack request latency",
        "latency.context_pack.max_elapsed_ms": "Maximum context-pack request latency",
        "warmup.avg_elapsed_ms": "warmup latency",
        "latency.context_admin.warmup.avg_elapsed_ms": "Average warmup request latency",
        "warmup_latency_ms": "warmup latency",
        "token_savings": "Token savings",
        "tokens_saved": "Token savings",
        "compression_ratio": "Compression ratio",
        "candidates_per_selected": "Candidates per selected",
        "retrieval.context_pack.candidates_per_selected": "Ranked candidates per selected item",
        "cache_hit_ratio": "Cache hit ratio",
        "cache.context_pack_fragment_hit_ratio": "Context-pack fragment cache hit ratio",
        "cache.retrieval.search_term.hit_ratio": "Search-term fragment cache hit ratio",
        "cache.retrieval.file_summary.hit_ratio": "File-summary fragment cache hit ratio",
        "cache.retrieval.test_owner_paths.hit_ratio": "Test-owner fragment cache hit ratio",
        "cache.hit_ratio": "Overall cache hit ratio",
        "external_calls_saved": "External calls saved",
        "tooling.external_calls_saved_per_pack": "Estimated external tool calls avoided per pack",
        "contract_tokens_saved": "Contract tokens saved",
        "tooling.contract_tokens_saved_est": "Estimated contract tokens saved",
        "references_bytes_deferred": "Deferred references bytes",
        "references.bytes_deferred_est": "Bytes deferred behind local references",
        "tokens.context_pack.avg_saved_per_pack": "Average tokens saved per context pack",
        "tokens.context_pack.avg_candidate_compression_per_pack": "Average candidate compression per context pack",
        "tokens.context_pack.compression_ratio": "Output tokens as a share of baseline tokens",
    }
    table_descriptions = {
        "telemetry.context_pack.samples": "Observed context-pack requests",
        "tokens.context_pack.compression_factor_est": "Estimated source-to-wire factor",
        "tokens.context_pack.saved_est": "Estimated tokens kept off wire",
        "quality.required_anchor_recall": "Benchmark required-anchor recall",
        "quality.noise_ratio": "Benchmark retrieval noise",
    }

    checks = matrix.get("checks") if isinstance(matrix, dict) else None
    if not isinstance(checks, list):
        return []

    def _fallback_description(key: str) -> str:
        key_clean = key.strip().lower()
        if key_clean in fallback_descriptions:
            return fallback_descriptions[key_clean]
        humanized = re.sub(r"[._-]+", " ", key_clean)
        humanized = humanized.replace(" avg ", " average ").replace(" p95 ", " p95 ")
        humanized = humanized.replace(" est", " estimate")
        humanized = " ".join(humanized.split())
        if not humanized:
            return ""
        return humanized

    rows: list[tuple[str, str, str, str, str]] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        check_key = str(check.get("key") or "-")
        status = _check_status_label(check)
        description = table_descriptions.get(check_key.strip().lower())
        if description is None:
            description = check.get("description")
            if description is None:
                description = check.get("desc")
                if description is None:
                    description = _fallback_description(check_key)
        if not isinstance(description, str):
            description = str(description)
        operator = check.get("operator")
        hint = None
        if operator == "<=" or operator == "<":
            hint = "lower is better"
        elif operator == ">=" or operator == ">":
            hint = "higher is better"
        elif operator == "==" or operator == "=":
            hint = "target match expected"
        if hint and hint not in description.lower():
            description = f"{description} ({hint})"
        rows.append(
            (
                _trim(check_key, 72),
                _status_text(status, color),
                _metric_value(check.get("current")),
                _metric_value(check.get("target")),
                _trim(description.strip(), 72),
            )
        )
    return rows


RETRIEVAL_BREAKDOWN_STAGES = (
    "search_fragment_ms",
    "search_merge_ms",
    "search_summary_ms",
    "symbol_lookup_ms",
    "test_owner_summary_ms",
)


def _recent_context_pack_stage_ms(metrics: dict[str, Any], name: str) -> float | None:
    recent_rows = metrics.get("recent")
    if not isinstance(recent_rows, list):
        return None
    for row in reversed(recent_rows):
        if not isinstance(row, dict):
            continue
        operation = row.get("operation")
        if operation is None:
            operation = row.get("operation_name")
        if operation != "context_pack":
            continue
        stage_timings = row.get("stage_timings_ms")
        if not isinstance(stage_timings, dict):
            continue
        last_elapsed_ms = stage_timings.get(name)
        if isinstance(last_elapsed_ms, (int, float)):
            return float(last_elapsed_ms)
    return None


def _context_pack_stage_stats(metrics: dict[str, Any], name: str) -> dict[str, Any]:
    stages = metrics.get("benchmarks", {}).get("stage_latency_ms_by_operation", {})
    pack_stages = stages.get("context_pack", {}) if isinstance(stages, dict) else {}
    stats = pack_stages.get(name, {}) if isinstance(pack_stages, dict) else {}
    if isinstance(stats, dict) and "last_elapsed_ms" not in stats:
        recent_last_ms = _recent_context_pack_stage_ms(
            metrics.get("benchmarks", {}), name
        )
        if recent_last_ms is not None:
            stats = dict(stats)
            stats["last_elapsed_ms"] = recent_last_ms
    return stats if isinstance(stats, dict) else {}


def _performance_retrieval_bottleneck(metrics: dict[str, Any]) -> str:
    top_name = ""
    top_ms = -1.0
    for name in RETRIEVAL_BREAKDOWN_STAGES:
        stats = _context_pack_stage_stats(metrics, name)
        avg_ms = stats.get("avg_elapsed_ms")
        if not isinstance(avg_ms, (float, int)):
            continue
        if avg_ms > top_ms:
            top_ms = avg_ms
            top_name = name
    if top_name and top_ms > 0:
        return f"{top_name} ({fmt_ms(top_ms)} ms)"
    return "-"


def _performance_stage_rows(
    metrics: dict[str, Any],
) -> list[tuple[str, ...]]:
    if _is_native_metrics(metrics):
        operations = _mapping_at(metrics, ("requests", "by_operation"))
        rows = []
        for operation, stats in sorted(
            operations.items(), key=lambda row: (row[0] != "context_pack", row[0])
        ):
            if not isinstance(stats, dict):
                continue
            rows.append(
                (
                    operation,
                    fmt_ms(stats.get("avg_elapsed_ms", 0.0)),
                    fmt_ms(stats.get("p50_recent_ms", 0.0)),
                    fmt_ms(stats.get("p95_recent_ms", 0.0)),
                    fmt_ms(stats.get("min_elapsed_ms", 0.0)),
                    fmt_ms(stats.get("max_elapsed_ms", 0.0)),
                    fmt_ms(stats.get("last_elapsed_ms", 0.0)),
                )
            )
        return rows or [("context_pack", "-", "-", "-", "-", "-", "-")]
    rows: list[tuple[str, str, str, str, str]] = []
    for name in (
        "total_ms",
        "index_refresh_ms",
        "explicit_path_refresh_ms",
        "candidate_retrieval_ms",
        *RETRIEVAL_BREAKDOWN_STAGES,
        "snippet_batch_ms",
        "skill_guidance_ms",
        "cache_lookup_ms",
        "reference_write_ms",
        "response_assembly_ms",
    ):
        stats = _context_pack_stage_stats(metrics, name)
        if not isinstance(stats, dict):
            stats = {}
        rows.append(
            (
                name,
                fmt_ms(stats.get("avg_elapsed_ms", 0.0)),
                fmt_ms(stats.get("min_elapsed_ms", 0.0)),
                fmt_ms(stats.get("max_elapsed_ms", 0.0)),
                fmt_ms(stats.get("last_elapsed_ms", 0.0)),
            )
        )
    return rows


def _performance_cache_rows(
    metrics: dict[str, Any],
    background: dict[str, Any],
    freshness: dict[str, Any],
) -> list[tuple[str, str]]:
    if _is_native_metrics(metrics):
        l0 = _cache_l0(metrics)
        l1 = _cache_l1(metrics)
        retrieval = _mapping_at(metrics, ("retrieval",))
        references = _mapping_at(metrics, ("references",))
        pack_tokens = _mapping_at(metrics, ("tokens", "context_pack"))
        freshness_state = "dirty" if freshness.get("dirty") else "clean"
        return [
            ("engine", _native_engine_status(metrics)),
            (
                "L0 pack cache",
                _cache_hit_summary(l0),
            ),
            ("L0 miss causes", _l0_miss_reason_summary(l0)),
            (
                "L0 storage",
                f"{fmt_int(l0.get('entries', 0))} entries / "
                f"{fmt_bytes(l0.get('weighted_bytes', 0))}",
            ),
            (
                "L1 frontier",
                f"{fmt_int(l1.get('exact_hits', 0))} exact, "
                f"{fmt_int(l1.get('approximate_hits', 0))} approximate hits",
            ),
            ("retrieval misses", fmt_int(_native_retrieval_misses(metrics))),
            (
                "index",
                f"{retrieval.get('backend', '-')}, "
                f"{fmt_int(retrieval.get('doc_count', 0))} chunks",
            ),
            (
                "freshness",
                f"{freshness_state}, gen {fmt_int(freshness.get('generation', 0))}, "
                f"{fmt_int(freshness.get('refreshes', 0))} refreshes",
            ),
            (
                "references",
                f"{fmt_int(references.get('active_count', 0))} active / "
                f"{fmt_int(references.get('total_count', 0))} total",
            ),
            (
                "compression",
                _compression_factor_summary(pack_tokens),
            ),
            (
                "tokens saved est.",
                f"{fmt_int(pack_tokens.get('saved_tokens_est', 0))} source-to-wire, "
                f"{fmt_int(pack_tokens.get('delta_tokens_saved_est', 0))} delta",
            ),
            (
                "token estimates",
                f"{fmt_int(pack_tokens.get('selected_source_tokens_est', 0))} source / "
                f"{fmt_int(pack_tokens.get('evidence_card_tokens_est', 0))} cards / "
                f"{fmt_int(pack_tokens.get('wire_tokens_est', 0))} wire "
                f"({fmt_bytes(pack_tokens.get('wire_bytes', 0))})",
            ),
            ("background", str(background.get("status") or "-")),
        ]
    index_job = (
        background.get("index_refresh", {})
        if isinstance(background.get("index_refresh"), dict)
        else {}
    )
    cache_job = (
        background.get("cache_prune", {})
        if isinstance(background.get("cache_prune"), dict)
        else {}
    )
    auto_warmup_job = (
        background.get("cache_auto_warmup", {})
        if isinstance(background.get("cache_auto_warmup"), dict)
        else {}
    )
    return [
        ("freshness", str(freshness.get("state") or "-")),
        ("refresh reason", str(freshness.get("refresh_reason") or "-")),
        (
            "background refresh",
            _background_job_summary(index_job),
        ),
        ("cache maintenance", _background_job_summary(cache_job)),
        ("background queue", fmt_int(background.get("queue_depth", 0))),
        (
            "fragment hit ratio",
            f"{_fragment_cache_ratio(metrics) * 100:5.1f}%",
        ),
        (
            "search-term cache",
            _namespace_cache_summary(metrics, "retrieval.search_term"),
        ),
        (
            "file-summary cache",
            _namespace_cache_summary(metrics, "retrieval.file_summary"),
        ),
        (
            "test-owner cache",
            _namespace_cache_summary(metrics, "retrieval.test_owner_paths"),
        ),
        ("retrieval bottleneck", _performance_retrieval_bottleneck(metrics)),
        (
            "skill card cache",
            _namespace_cache_summary(metrics, "skill.compiled"),
        ),
        ("warmup", _warmup_summary(metrics)),
        ("auto warmup", _auto_warmup_summary(metrics, auto_warmup_job)),
    ]


def _background_job_summary(job: dict[str, Any]) -> str:
    status = str(job.get("status") or "idle")
    pending = "pending" if job.get("pending") else "idle"
    last_error = str(job.get("last_error") or "")
    if last_error:
        return f"{status} ({last_error})"
    completed = str(job.get("last_completed_at") or "")
    suffix = f", completed {completed[:19]}" if completed else ""
    return f"{status}/{pending}{suffix}"


def _namespace_cache_summary(metrics: dict[str, Any], namespace: str) -> str:
    cache = metrics.get("cache", {})
    cache = cache if isinstance(cache, dict) else {}
    by_namespace = cache.get("by_namespace", {})
    by_namespace = by_namespace if isinstance(by_namespace, dict) else {}
    row = by_namespace.get(namespace, {})
    if not isinstance(row, dict):
        return "-"
    return (
        f"{fmt_int(row.get('hits', 0))}/"
        f"{fmt_int(row.get('misses', 0))} h/m  "
        f"{float(row.get('hit_ratio', 0.0) or 0.0) * 100:5.1f}%"
    )


def _warmup_summary(metrics: dict[str, Any]) -> str:
    warmup = metrics.get("warmup", {})
    if not isinstance(warmup, dict):
        return "-"
    count = int(warmup.get("count", 0) or 0)
    if not count:
        return "not run"
    last_at = str(warmup.get("last_recorded_at") or "")
    last_suffix = f", last {last_at[:19]}" if last_at else ""
    return (
        f"{fmt_int(count)} runs, avg {fmt_ms(warmup.get('avg_elapsed_ms', 0.0))}, "
        f"last {fmt_ms(warmup.get('last_elapsed_ms', 0.0))}, "
        f"{fmt_int(warmup.get('last_query_count', 0))} queries"
        f"{last_suffix}"
    )


def _auto_warmup_summary(metrics: dict[str, Any], job: dict[str, Any]) -> str:
    warmup = metrics.get("warmup", {})
    if not isinstance(warmup, dict):
        warmup = {}
    auto_learn = warmup.get("auto_learn", {})
    auto_learn = auto_learn if isinstance(auto_learn, dict) else {}
    auto_count = int(warmup.get("auto_count", 0) or 0)
    status = str(
        auto_learn.get("last_auto_status")
        or warmup.get("last_auto_status")
        or job.get("status")
        or ""
    )
    reason = str(auto_learn.get("last_reason_code") or "")
    if not auto_count and not reason and not status:
        return "not run"
    pending = "pending" if job.get("pending") else "idle"
    parts = [f"{fmt_int(auto_count)} auto runs"]
    if status:
        parts.append(status)
    if pending:
        parts.append(pending)
    if reason:
        parts.append(f"reason {reason}")
    coverage = auto_learn.get("last_coverage", {})
    if isinstance(coverage, dict) and int(coverage.get("planned", 0) or 0):
        parts.append(
            f"coverage {fmt_int(coverage.get('completed', 0))}/{fmt_int(coverage.get('planned', 0))}"
        )
    deduplicated = int(auto_learn.get("deduplicated_count", 0) or 0)
    failures = int(auto_learn.get("failure_count", 0) or 0)
    if deduplicated:
        parts.append(f"{fmt_int(deduplicated)} dedup")
    if failures:
        parts.append(f"{fmt_int(failures)} failed")
    return ", ".join(parts)


def _state_rows(state: MonitorState) -> list[dict[str, Any]]:
    rows = (state.state_payload or {}).get("rows")
    if not isinstance(rows, list):
        return []
    return sorted(
        (row for row in rows if isinstance(row, dict)),
        key=_state_row_sort_key,
    )


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
            "namespace",
            "created_at",
            "updated_at",
            "expires_at",
            "preview",
            "state_class",
        )
    ).lower()
    return query in haystack


def _state_row_sort_key(row: dict[str, Any]) -> tuple[str, float, str]:
    return (
        _state_row_class(row).lower(),
        -_state_row_timestamp(row),
        str(row.get("key") or ""),
    )


def _state_row_class(row: dict[str, Any]) -> str:
    namespace = str(row.get("namespace") or "").strip()
    if namespace:
        return namespace
    key = str(row.get("key") or "")
    if key.startswith("cache:"):
        cache_key = key.removeprefix("cache:")
        if ":" in cache_key:
            return cache_key.split(":", 1)[0]
        return "cache"
    if ":" in key:
        return key.split(":", 1)[0]
    schema = str(row.get("schema") or "").strip()
    return schema.split(".", 1)[0] if schema else "-"


def _state_row_timestamp(row: dict[str, Any]) -> float:
    for timestamp_field in ("created_at", "updated_at", "expires_at"):
        value = str(row.get(timestamp_field) or "").strip()
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return 0.0


def _state_visible_count(height: int, entry_open: bool) -> int:
    reserved = 22 if entry_open else 13
    return max(3, height - reserved)


def _state_entry_visible_count(height: int) -> int:
    return max(3, height - 13)


def _clamp_content_scroll_offset(
    offset: int,
    visible_count: int,
    line_count: int,
) -> int:
    if line_count <= 0:
        return 0
    max_offset = max(0, line_count - visible_count)
    return max(0, min(offset, max_offset))


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
        _trim(_state_row_class(row), 22),
        _trim(str(row.get("value_type") or "-"), 8),
        fmt_int(row.get("size_chars", 0)),
        _trim(str(row.get("created_at") or "-"), 20),
        _trim(str(row.get("schema") or "-"), 22),
        _trim(str(row.get("status") or "-"), 10),
    )


def _state_table_widths(width: int) -> tuple[int, ...]:
    key_width = max(24, min(42, width - 94))
    class_width = max(12, min(22, width - 114))
    return (1, key_width, class_width, 8, 8, 20, 22, 10)


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
                "| " + wrapped + " " * max(0, width - 4 - _visible_len(wrapped)) + " |"
            )
    boxed.append(border)
    return boxed


def _status_text(status: str, color: bool) -> str:
    if status == "pass":
        return _style(status, color, Ansi.GREEN)
    if status == "critical":
        return _style(status, color, Ansi.BOLD + Ansi.RED)
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
                *(
                    (_visible_len(row[index]) for row in text_rows if index < len(row))
                    or [0]
                ),
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
    counts = {"pass": 0, "critical": 0, "fail": 0, "insufficient": 0}
    for check in checks:
        if isinstance(check, dict):
            status = _check_status_label(check)
            if status in counts:
                counts[status] += 1
    if counts["critical"]:
        return _style(f"{counts['critical']} critical", color, Ansi.BOLD + Ansi.RED)
    if counts["fail"]:
        return _style(f"{counts['fail']} fail", color, Ansi.RED)
    if counts["insufficient"]:
        pending = f"{counts['insufficient']} pending"
        if counts["pass"]:
            pending = f"{counts['pass']} pass, {pending}"
        return _style(pending, color, Ansi.YELLOW)
    return _style(f"{counts['pass']} pass", color, Ansi.GREEN)


def _check_status_label(check: dict[str, Any]) -> str:
    status = str(check.get("status") or "-").strip().lower()
    if status != "fail":
        return status
    return "critical" if _check_severity(check) == "critical" else "fail"


def _check_severity(check: dict[str, Any]) -> str:
    severity = (
        str(check.get("severity") or check.get("priority") or check.get("level") or "")
        .strip()
        .lower()
    )
    if severity:
        return severity
    key = str(check.get("key") or "").strip().lower()
    if key in CRITICAL_MATRIX_KEYS:
        return "critical"
    return ""


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
        "context_admin(mode='measurement_report'); pending means the project "
        "has not been opened for a quality run. Cache bars show L0 pack-cache hits."
    )


def _style(text: str, color: bool, code: str) -> str:
    return f"{code}{text}{Ansi.RESET}" if color else text


def _style_full_line(text: str, color: bool, code: str) -> str:
    if not color:
        return text
    return f"{code}{text.replace(Ansi.RESET, Ansi.RESET + code)}{Ansi.RESET}"


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


def _tokens_spared_by_mcp(metrics: dict[str, Any]) -> int:
    tokens = metrics.get("tokens", {})
    if isinstance(tokens, dict) and "tokens_spared_by_mcp_est" in tokens:
        return _int_at(metrics, ("tokens", "tokens_spared_by_mcp_est"))
    return _int_at(metrics, ("tokens", "estimated_input_tokens_saved"))


def _mapping_at(payload: dict[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return {}
        value = value.get(key)
    return value if isinstance(value, dict) else {}


def _is_native_metrics(metrics: dict[str, Any]) -> bool:
    """Recognize the native server without relying on its version string."""

    return bool(_mapping_at(metrics, ("cache", "l0"))) or (
        _mapping_at(metrics, ("retrieval",)).get("backend") == "tantivy"
    )


def _cache_l0(metrics: dict[str, Any]) -> dict[str, Any]:
    l0 = _mapping_at(metrics, ("cache", "l0"))
    if l0:
        return l0
    cache = _mapping_at(metrics, ("cache",))
    return {"hits": cache.get("hits", 0), "misses": cache.get("misses", 0)}


def _cache_l1(metrics: dict[str, Any]) -> dict[str, Any]:
    return _mapping_at(metrics, ("cache", "l1"))


def _cache_hit_ratio(metrics: dict[str, Any]) -> float:
    cache = _mapping_at(metrics, ("cache",))
    ratio = cache.get("hit_ratio")
    if isinstance(ratio, (int, float)):
        return max(0.0, min(1.0, float(ratio)))
    if _is_native_metrics(metrics):
        l0 = _cache_l0(metrics)
        hits = _int_at(l0, ("hits",))
        misses = _int_at(l0, ("misses",))
        total = hits + misses
        return hits / total if total else 0.0
    return _fragment_cache_ratio(metrics)


def _cache_hit_summary(cache: dict[str, Any]) -> str:
    hits = _int_at(cache, ("hits",))
    misses = _int_at(cache, ("misses",))
    total = hits + misses
    ratio = hits / total if total else 0.0
    return f"{fmt_int(hits)}/{fmt_int(misses)} h/m  {ratio * 100:5.1f}%"


def _l0_miss_reason_summary(cache: dict[str, Any]) -> str:
    reasons = _mapping_at(cache, ("miss_reasons",))
    cold = _int_at(reasons, ("cold_or_invalidated",))
    variants = _int_at(reasons, ("request_variant",))
    invalidations = _int_at(cache, ("invalidations",))
    coalesced = _int_at(cache, ("singleflight_hits",))
    return (
        f"cold {fmt_int(cold)}, variant {fmt_int(variants)}, "
        f"invalidations {fmt_int(invalidations)}, coalesced {fmt_int(coalesced)}"
    )


def _compression_factor_summary(pack_tokens: dict[str, Any]) -> str:
    factor = pack_tokens.get("compression_factor_est")
    ratio = pack_tokens.get("compression_ratio_est")
    if not isinstance(factor, (int, float)) or float(factor) <= 0:
        return "not observed"
    ratio_text = (
        f" ({float(ratio) * 100:.1f}% returned)"
        if isinstance(ratio, (int, float))
        else ""
    )
    return f"{float(factor):.2f}x source-to-wire{ratio_text}"


def _context_pack_count(metrics: dict[str, Any]) -> int:
    return _int_at(metrics, ("requests", "by_operation", "context_pack", "count"))


def _context_pack_avg_ms(metrics: dict[str, Any]) -> float:
    return _float_at(
        metrics, ("requests", "by_operation", "context_pack", "avg_elapsed_ms")
    )


def _native_retrieval_misses(metrics: dict[str, Any]) -> int:
    misses = _int_at(metrics, ("cache", "l1", "retrieval_misses"))
    return misses if misses else _int_at(metrics, ("retrieval", "misses"))


def _native_engine_status(metrics: dict[str, Any]) -> str:
    background = _mapping_at(metrics, ("background",))
    return str(background.get("status") or "active")


def _fragment_cache_ratio(metrics: dict[str, Any]) -> float:
    explicit = _float_at(metrics, ("cache", "context_pack_fragment_hit_ratio"))
    if explicit:
        return explicit
    hits = _int_at(metrics, ("cache", "context_pack_fragment_hits"))
    misses = _int_at(metrics, ("cache", "context_pack_fragment_misses"))
    total = hits + misses
    return (hits / total) if total else 0.0


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
    if sequence in {"i", "I"}:
        return "inspection"
    if sequence == "p":
        return "performance"
    if sequence == "l":
        return "monitor_usage"
    if sequence == "P":
        return "prune"
    if sequence in {"w", "W"}:
        return "warmup"
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
        "inspection": "i",
        "performance": "p",
        "monitor_usage": "l",
        "prune": "P",
        "warmup": "w",
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
            state.state_entry_scroll_offset = 0
            return "redraw"
        if key == "up":
            state.state_entry_scroll_offset = max(
                0, state.state_entry_scroll_offset - 1
            )
            return "redraw"
        if key == "down":
            state.state_entry_scroll_offset += 1
            return "redraw"
        if key == "page_up":
            state.state_entry_scroll_offset = max(
                0, state.state_entry_scroll_offset - 10
            )
            return "redraw"
        if key == "page_down":
            state.state_entry_scroll_offset += 10
            return "redraw"
        if key == "home":
            state.state_entry_scroll_offset = 0
            return "redraw"
        if key == "end":
            state.state_entry_scroll_offset = 1_000_000
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

    if state.view == "inspection":
        if state.inspection_viewer_index is not None:
            if key == "escape":
                state.inspection_viewer_index = None
                state.inspection_content_scroll_offset = 0
                return "redraw"
            if key == "up":
                state.inspection_content_scroll_offset = max(
                    0, state.inspection_content_scroll_offset - 1
                )
                return "redraw"
            if key == "down":
                state.inspection_content_scroll_offset += 1
                return "redraw"
            if key == "page_up":
                state.inspection_content_scroll_offset = max(
                    0, state.inspection_content_scroll_offset - 10
                )
                return "redraw"
            if key == "page_down":
                state.inspection_content_scroll_offset += 10
                return "redraw"
            if key == "home":
                state.inspection_content_scroll_offset = 0
                return "redraw"
            if key == "end":
                state.inspection_content_scroll_offset = 1_000_000
                return "redraw"
            return "ignore"

        item = _inspection_item_at(state)
        item_count = len(_inspection_rows(state.inspection_payload or {}))
        if key == "up":
            state.inspection_selected_index = _clamped_index(
                state.inspection_selected_index - 1,
                item_count,
            )
            return "redraw"
        if key == "down":
            state.inspection_selected_index = _clamped_index(
                state.inspection_selected_index + 1,
                item_count,
            )
            return "redraw"
        if key == "page_up":
            state.inspection_selected_index = _clamped_index(
                state.inspection_selected_index - 10, item_count
            )
            return "redraw"
        if key == "page_down":
            state.inspection_selected_index = _clamped_index(
                state.inspection_selected_index + 10, item_count
            )
            return "redraw"
        if key == "home":
            state.inspection_selected_index = 0
            return "redraw"
        if key == "end":
            state.inspection_selected_index = _clamped_index(1_000_000, item_count)
            return "redraw"
        if key == "enter" and item:
            kind, value = item
            state.inspection_viewer_index = state.inspection_selected_index
            state.inspection_content_scroll_offset = 0
            if (
                kind == "resource"
                and "content" not in value
                and str(value.get("uri") or "")
            ):
                return "inspection_resource"
            return "redraw"
        if key == "escape":
            state.view = "table"
            return "redraw"

    if key in {"quit", "interrupt"}:
        return "quit"
    if key == "refresh" and state.view != "state":
        return "refresh"
    if key == "warmup" and state.view != "state" and row_count:
        return "warmup"
    if key == "prune" and state.view != "state" and row_count:
        return "prune"
    if key == "monitor_usage":
        return "monitor_usage"
    if key == "inspection":
        state.view = "inspection"
        return "inspection"
    if key == "browser" and row_count:
        state.view = "state"
        state.state_entry = None
        state.state_selected_index = 0
        state.state_scroll_offset = 0
        state.state_entry_scroll_offset = 0
        return "browser"
    if key == "performance" and row_count and state.view != "state":
        state.view = "performance"
        return "performance"
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
        state.state_selected_index = _clamped_index(
            state_row_count - 1, state_row_count
        )
        return "redraw"
    if key == "search" and state.view == "state":
        state.state_search = ""
        state.state_search_active = True
        state.state_entry = None
        state.state_selected_index = 0
        state.state_scroll_offset = 0
        state.state_entry_scroll_offset = 0
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
            state.state_entry_scroll_offset = 0
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


def _submit_mcp_operation(
    executor: ThreadPoolExecutor,
    kind: str,
    work: Callable[[], Any],
) -> PendingMcpOperation:
    return PendingMcpOperation(kind=kind, future=executor.submit(work))


def _set_mcp_loading(state: MonitorState, message: str) -> None:
    state.mcp_status = message
    state.mcp_error = ""


def _clear_mcp_status(state: MonitorState) -> None:
    state.mcp_status = ""
    state.mcp_error = ""


def _set_mcp_error(state: MonitorState, exc: Exception) -> None:
    state.mcp_status = ""
    state.mcp_error = friendly_mcp_error(exc)


def _fetch_state_browser_result(
    client: Any,
    snapshots: list[ProjectSnapshot],
    selected_index: int,
) -> StateBrowserResult:
    if not snapshots:
        return StateBrowserResult(target=None, error="no project selected")
    selected = _clamped_index(selected_index, len(snapshots))
    target = snapshots[selected].target
    try:
        return StateBrowserResult(
            target=target, payload=fetch_state_browser(client, target)
        )
    except Exception as exc:
        return StateBrowserResult(target=target, error=friendly_mcp_error(exc))


def _apply_state_browser_result(
    state: MonitorState,
    result: StateBrowserResult,
) -> None:
    state.state_target = result.target
    state.state_entry = None
    state.state_entry_scroll_offset = 0
    if result.error:
        state.state_payload = None
        state.state_error = result.error
        return
    state.state_payload = result.payload
    state.state_error = ""
    state.state_selected_index = _clamped_index(
        state.state_selected_index, _state_row_count(state)
    )
    state.state_scroll_offset = _adjust_scroll_offset(
        state.state_scroll_offset,
        state.state_selected_index,
        _state_visible_count(
            shutil.get_terminal_size(
                (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
            ).lines,
            False,
        ),
        _state_row_count(state),
    )


def _selected_state_key(state: MonitorState) -> str:
    rows = _filtered_state_rows(state)
    if not rows:
        state.state_error = "no state row selected"
        return ""
    row = rows[_clamped_index(state.state_selected_index, len(rows))]
    key = str(row.get("key") or "")
    if not key or state.state_target is None:
        state.state_error = "no state key selected"
        return ""
    return key


def _fetch_state_entry_result(
    client: Any,
    target: ProjectTarget | None,
    key: str,
) -> StateEntryResult:
    if target is None:
        return StateEntryResult(target=None, key=key, error="no state key selected")
    try:
        return StateEntryResult(
            target=target,
            key=key,
            payload=fetch_state_browser(client, target, state_key=key),
        )
    except Exception as exc:
        return StateEntryResult(target=target, key=key, error=friendly_mcp_error(exc))


def _apply_state_entry_result(state: MonitorState, result: StateEntryResult) -> None:
    state.state_target = result.target
    if result.error:
        state.state_error = result.error
        return
    state.state_entry = result.payload
    state.state_entry_scroll_offset = 0
    state.state_entry_preview_source_id = 0
    state.state_entry_preview = ""
    state.state_entry_preview_truncated = False
    state.state_entry_preview_width = 0
    state.state_entry_preview_lines = []
    state.state_error = ""
    state.view = "state"


def _fetch_warmup_result(
    client: Any,
    snapshots: list[ProjectSnapshot],
    selected_index: int,
) -> WarmupResult:
    if not snapshots:
        return WarmupResult(target=None, error="no project selected")
    selected = _clamped_index(selected_index, len(snapshots))
    target = snapshots[selected].target
    try:
        return WarmupResult(target=target, payload=warmup_project(client, target))
    except Exception as exc:
        return WarmupResult(target=target, error=friendly_mcp_error(exc))


def _fetch_monitor_usage_result(
    client: Any,
    snapshots: list[ProjectSnapshot],
    selected_index: int,
    toggle: bool,
) -> MonitorUsageResult:
    target = None
    if snapshots:
        target = snapshots[_clamped_index(selected_index, len(snapshots))].target
    try:
        status = monitor_usage(client, "status")
        if toggle:
            action = "disable" if status.get("enabled") else "enable"
            status = monitor_usage(client, action)
        report = monitor_usage(client, "report", target)
        return MonitorUsageResult(payload={"status": status, "report": report})
    except Exception as exc:
        return MonitorUsageResult(error=friendly_mcp_error(exc))


def _fetch_mcp_inspection_result(client: Any) -> McpInspectionResult:
    try:
        return McpInspectionResult(payload=inspect_mcp_surface(client))
    except Exception as exc:
        return McpInspectionResult(error=friendly_mcp_error(exc))


def _apply_monitor_usage_result(
    state: MonitorState, result: MonitorUsageResult
) -> None:
    state.usage_status_requested = True
    if result.error:
        state.mcp_error = result.error
        state.mcp_status = ""
        return
    payload = result.payload or {}
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    report = payload.get("report") if isinstance(payload.get("report"), dict) else {}
    state.usage_enabled = bool(status.get("enabled"))
    state.usage_report = report
    state.mcp_status = (
        "detailed usage enabled" if state.usage_enabled else "detailed usage disabled"
    )
    state.mcp_error = ""


def _apply_mcp_inspection_result(state: MonitorState, result: McpInspectionResult) -> None:
    state.inspection_payload = result.payload
    state.inspection_error = result.error
    state.inspection_selected_index = 0
    state.inspection_viewer_index = None
    state.inspection_content_scroll_offset = 0


def _apply_mcp_resource_content_result(
    state: MonitorState, result: McpResourceContentResult
) -> None:
    resource = _inspection_resource_for_uri(state, result.uri)
    if resource is None:
        return
    resource.pop("error", None)
    resource.pop("content", None)
    state.inspection_content_scroll_offset = 0
    if result.error:
        resource["error"] = result.error
    else:
        resource["content"] = result.content


def _apply_warmup_result(state: MonitorState, result: WarmupResult) -> None:
    if result.error:
        state.mcp_status = ""
        state.mcp_error = result.error
        return
    payload = result.payload or {}
    project = _project_name(result.target) if result.target else "-"
    state.mcp_status = f"warmed {project}: {_warmup_result_summary(payload)}"
    state.mcp_error = ""


def _fetch_prune_result(
    client: Any,
    snapshots: list[ProjectSnapshot],
    selected_index: int,
) -> PruneResult:
    if not snapshots:
        return PruneResult(target=None, error="no project selected")
    selected = _clamped_index(selected_index, len(snapshots))
    target = snapshots[selected].target
    try:
        return PruneResult(target=target, payload=prune_project(client, target))
    except Exception as exc:
        return PruneResult(target=target, error=friendly_mcp_error(exc))


def _apply_prune_result(state: MonitorState, result: PruneResult) -> None:
    if result.error:
        state.mcp_status = ""
        state.mcp_error = result.error
        return
    payload = result.payload or {}
    project = _project_name(result.target) if result.target else "-"
    removed = "removed generated state" if payload.get("removed") else "nothing removed"
    state.mcp_status = f"pruned {project}: {removed}"
    state.mcp_error = ""


def _warmup_result_summary(payload: dict[str, Any]) -> str:
    terms = _int_at(payload, ("search_cache", "query_count"))
    files = _int_at(payload, ("index", "file_count"))
    parts = [f"{fmt_int(terms)} terms"]

    file_summary_cache = payload.get("file_summary_cache")
    if isinstance(file_summary_cache, dict):
        summaries = _int_at(payload, ("file_summary_cache", "summary_count"))
        hits = _int_at(payload, ("file_summary_cache", "hits"))
        misses = _int_at(payload, ("file_summary_cache", "misses"))
        parts.append(
            f"{fmt_int(summaries)} summaries ({fmt_int(hits)} hit/{fmt_int(misses)} miss)"
        )

    hot_chunks = payload.get("hot_chunks")
    if isinstance(hot_chunks, dict):
        parts.append(
            f"{fmt_int(_int_at(payload, ('hot_chunks', 'target_count')))} hot chunks"
        )

    test_owner_targets = payload.get("test_owner_targets")
    if isinstance(test_owner_targets, dict):
        parts.append(
            f"{fmt_int(_int_at(payload, ('test_owner_targets', 'target_count')))} test targets"
        )

    parts.append(f"{fmt_int(files)} files")
    return ", ".join(parts)


def _apply_mcp_operation_result(
    state: MonitorState,
    snapshots: list[ProjectSnapshot],
    pending: PendingMcpOperation,
) -> list[ProjectSnapshot]:
    result = pending.future.result()
    _clear_mcp_status(state)
    if pending.kind == "refresh":
        refreshed = result if isinstance(result, list) else []
        state.selected_index = _clamped_index(state.selected_index, len(refreshed))
        state.last_updated = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return refreshed
    if pending.kind == "state_browser":
        if isinstance(result, StateBrowserResult):
            _apply_state_browser_result(state, result)
        return snapshots
    if pending.kind == "state_entry":
        if isinstance(result, StateEntryResult):
            _apply_state_entry_result(state, result)
        return snapshots
    if pending.kind == "warmup":
        if isinstance(result, WarmupResult):
            _apply_warmup_result(state, result)
        return snapshots
    if pending.kind == "prune":
        if isinstance(result, PruneResult):
            _apply_prune_result(state, result)
        return snapshots
    if pending.kind == "monitor_usage":
        if isinstance(result, MonitorUsageResult):
            _apply_monitor_usage_result(state, result)
        return snapshots
    if pending.kind == "inspection":
        if isinstance(result, McpInspectionResult):
            _apply_mcp_inspection_result(state, result)
        return snapshots
    if pending.kind == "inspection_resource":
        if isinstance(result, McpResourceContentResult):
            _apply_mcp_resource_content_result(state, result)
        return snapshots
    return snapshots


def _load_state_browser(
    client: Any,
    state: MonitorState,
    snapshots: list[ProjectSnapshot],
) -> None:
    result = _fetch_state_browser_result(client, snapshots, state.selected_index)
    _apply_state_browser_result(state, result)


def _load_state_entry(client: Any, state: MonitorState) -> None:
    key = _selected_state_key(state)
    if not key:
        return
    result = _fetch_state_entry_result(client, state.state_target, key)
    _apply_state_entry_result(state, result)


def _needs_initial_usage_status(
    state: MonitorState,
    snapshots: list[ProjectSnapshot],
    pending: PendingMcpOperation | None,
) -> bool:
    return bool(snapshots) and pending is None and not state.usage_status_requested


def run_interactive_monitor(client: Any, args: argparse.Namespace, color: bool) -> int:
    state = MonitorState(refresh_interval=max(MIN_INTERVAL, args.interval))
    snapshots: list[ProjectSnapshot] = []
    force_refresh = True
    next_refresh = 0.0
    dirty = True
    pending: PendingMcpOperation | None = None
    queued_browser_load = False
    queued_refresh = False
    queued_warmup = False
    queued_prune = False
    fd = sys.stdin.fileno()

    with (
        ThreadPoolExecutor(max_workers=1, thread_name_prefix="monitor-mcp") as executor,
        RawTerminal(enabled=True),
    ):
        while True:
            now = time.monotonic()
            if pending and pending.future.done():
                completed_kind = pending.kind
                try:
                    snapshots = _apply_mcp_operation_result(state, snapshots, pending)
                except Exception as exc:
                    _set_mcp_error(state, exc)
                    completed_kind = ""
                pending = None
                if completed_kind != "monitor_usage":
                    next_refresh = time.monotonic() + state.refresh_interval
                if queued_browser_load and state.view == "state":
                    queued_browser_load = False
                    state.state_payload = None
                    state.state_entry = None
                    state.state_error = ""
                    state.state_entry_scroll_offset = 0
                    _set_mcp_loading(state, "loading state browser...")
                    pending = _submit_mcp_operation(
                        executor,
                        "state_browser",
                        lambda: _fetch_state_browser_result(
                            client, snapshots, state.selected_index
                        ),
                    )
                elif queued_warmup:
                    queued_warmup = False
                    _set_mcp_loading(state, "warming selected project...")
                    pending = _submit_mcp_operation(
                        executor,
                        "warmup",
                        lambda: _fetch_warmup_result(
                            client, snapshots, state.selected_index
                        ),
                    )
                elif queued_prune:
                    queued_prune = False
                    _set_mcp_loading(state, "pruning selected project...")
                    pending = _submit_mcp_operation(
                        executor,
                        "prune",
                        lambda: _fetch_prune_result(
                            client, snapshots, state.selected_index
                        ),
                    )
                elif queued_refresh:
                    queued_refresh = False
                    force_refresh = True
                elif completed_kind in {"warmup", "prune"} and not state.mcp_error:
                    force_refresh = True
                else:
                    force_refresh = False
                dirty = True

            browser_open = state.view in {"state", "inspection"}
            if _needs_initial_usage_status(state, snapshots, pending):
                state.usage_status_requested = True
                _set_mcp_loading(state, "loading detailed usage status...")
                pending = _submit_mcp_operation(
                    executor,
                    "monitor_usage",
                    lambda: _fetch_monitor_usage_result(
                        client, snapshots, state.selected_index, False
                    ),
                )
                dirty = True
            if pending is None and (
                force_refresh or (not browser_open and now >= next_refresh)
            ):
                _set_mcp_loading(state, "loading metrics...")
                pending = _submit_mcp_operation(
                    executor,
                    "refresh",
                    lambda: collect_snapshots(
                        client,
                        project_ids=args.project_id,
                        root_uri=args.root_uri,
                        include_matrix=not args.no_matrix,
                    ),
                )
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
                        shutil.get_terminal_size(
                            (DEFAULT_TERMINAL_COLUMNS, DEFAULT_TERMINAL_LINES)
                        ).columns,
                        state,
                    )
                )
                sys.stdout.write("\n")
                sys.stdout.flush()
                dirty = False

            if pending:
                timeout = 0.1
            elif state.view == "state":
                timeout = 0.5
            else:
                timeout = min(0.5, max(0.0, next_refresh - time.monotonic()))
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
            if action == "browser" and pending is None:
                state.state_payload = None
                state.state_entry = None
                state.state_error = ""
                state.state_entry_scroll_offset = 0
                _set_mcp_loading(state, "loading state browser...")
                pending = _submit_mcp_operation(
                    executor,
                    "state_browser",
                    lambda: _fetch_state_browser_result(
                        client, snapshots, state.selected_index
                    ),
                )
                dirty = True
            elif action == "browser":
                queued_browser_load = True
                _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
            if action == "inspection" and pending is None:
                state.inspection_payload = None
                state.inspection_error = ""
                _set_mcp_loading(state, "inspecting MCP tools and resources...")
                pending = _submit_mcp_operation(
                    executor,
                    "inspection",
                    lambda: _fetch_mcp_inspection_result(client),
                )
                dirty = True
            elif action == "inspection":
                _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
            if action == "inspection_resource" and pending is None:
                resource = _selected_inspection_resource(state)
                name = str((resource or {}).get("name") or (resource or {}).get("uri") or "resource")
                uri = str((resource or {}).get("uri") or "")
                _set_mcp_loading(state, f"reading {name}...")
                pending = _submit_mcp_operation(
                    executor,
                    "inspection_resource",
                    lambda: inspect_mcp_resource_content(client, uri),
                )
                dirty = True
            elif action == "inspection_resource":
                _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
            if action == "state_entry" and pending is None:
                key = _selected_state_key(state)
                if key:
                    target = state.state_target
                    state.state_entry = None
                    state.state_entry_scroll_offset = 0
                    _set_mcp_loading(state, "loading state entry...")
                    pending = _submit_mcp_operation(
                        executor,
                        "state_entry",
                        lambda: _fetch_state_entry_result(client, target, key),
                    )
                dirty = True
            if action == "refresh":
                if pending is None:
                    force_refresh = True
                else:
                    queued_refresh = True
                    _set_mcp_loading(state, "waiting for current MCP request...")
                    dirty = True
            if action == "warmup":
                if pending is None:
                    _set_mcp_loading(state, "warming selected project...")
                    pending = _submit_mcp_operation(
                        executor,
                        "warmup",
                        lambda: _fetch_warmup_result(
                            client, snapshots, state.selected_index
                        ),
                    )
                else:
                    queued_warmup = True
                    _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
            if action == "prune":
                if pending is None:
                    _set_mcp_loading(state, "pruning selected project...")
                    pending = _submit_mcp_operation(
                        executor,
                        "prune",
                        lambda: _fetch_prune_result(
                            client, snapshots, state.selected_index
                        ),
                    )
                else:
                    queued_prune = True
                    _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
            if action in {"performance", "monitor_usage"}:
                if pending is None:
                    toggle = action == "monitor_usage"
                    _set_mcp_loading(
                        state,
                        "updating detailed usage..." if toggle else "loading detailed usage...",
                    )
                    pending = _submit_mcp_operation(
                        executor,
                        "monitor_usage",
                        lambda toggle=toggle: _fetch_monitor_usage_result(
                            client, snapshots, state.selected_index, toggle
                        ),
                    )
                else:
                    _set_mcp_loading(state, "waiting for current MCP request...")
                dirty = True
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
            f"Refresh interval in seconds. Default is {DEFAULT_INTERVAL:g} in an "
            "interactive terminal and one-shot output otherwise. 0 runs once and "
            "exits."
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
        help="Skip slower measurement_matrix calls and render metrics only.",
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
    if args.interval > 0:
        args.interval = max(MIN_INTERVAL, args.interval)

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
