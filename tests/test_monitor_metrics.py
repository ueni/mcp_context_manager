from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from threading import Event, Lock
from typing import Any


def load_monitor_module() -> Any:
    path = Path(__file__).resolve().parents[1] / "monitor-metrics.py"
    spec = importlib.util.spec_from_file_location("monitor_metrics", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _metric_payload(self, project_id: str) -> dict[str, Any]:
        return {
            "schema": "context_metrics.v1",
            "project_id": project_id,
            "requests": {
                "total": 10,
                "by_operation": {
                    "context_pack": {"count": 3, "avg_elapsed_ms": 42.5}
                },
            },
            "cache": {
                "hits": 6,
                "misses": 2,
                "hit_ratio": 0.75,
                "context_pack_fragment_hits": 8,
                "context_pack_fragment_misses": 2,
                "context_pack_fragment_hit_ratio": 0.8,
                "by_namespace": {
                    "context_pack.retrieval": {
                        "hits": 2,
                        "misses": 1,
                        "hit_ratio": 0.667,
                    }
                },
            },
            "tokens": {
                "estimated_input_tokens_saved": 12345,
                "tokens_spared_by_mcp_est": 12000,
            },
            "references": {"bytes_deferred_est": 4096},
            "index_freshness": {
                "state": "last_good",
                "refresh_reason": "last_good_index",
                "background_refresh_pending": True,
            },
            "background": {
                "queue_depth": 1,
                "index_refresh": {
                    "kind": "index_refresh",
                    "status": "running",
                    "pending": True,
                    "last_error": "",
                },
                "cache_prune": {
                    "kind": "cache_prune",
                    "status": "complete",
                    "pending": False,
                    "last_completed_at": "2026-07-01T12:00:00+00:00",
                    "last_error": "",
                },
            },
            "benchmarks": {
                "stage_latency_ms_by_operation": {
                    "context_pack": {
                        "total_ms": {
                            "avg_elapsed_ms": 42.5,
                            "min_elapsed_ms": 20.0,
                            "max_elapsed_ms": 80.0,
                        },
                        "index_refresh_ms": {
                            "avg_elapsed_ms": 3.0,
                            "min_elapsed_ms": 0.5,
                            "max_elapsed_ms": 12.0,
                        },
                        "candidate_retrieval_ms": {
                            "avg_elapsed_ms": 15.0,
                            "min_elapsed_ms": 0.0,
                            "max_elapsed_ms": 60.0,
                        },
                    }
                },
            },
        }

    def _matrix_payload(self) -> dict[str, Any]:
        return {
            "schema": "context_measurement_matrix.v1",
            "checks": [
                {"key": "a", "status": "pass"},
                {"key": "b", "status": "pass"},
            ],
        }

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((name, arguments))
        mode = arguments.get("mode")
        project_id = arguments.get("project_id", "")
        if mode == "projects":
            return {
                "schema": "context_projects.list.v1",
                "projects": [
                    {"project_id": "beta-456", "name": "Beta", "source": "known"},
                    {"project_id": "alpha-123", "name": "Alpha", "source": "known"},
                ],
            }
        if mode == "metrics":
            return self._metric_payload(str(project_id))
        if mode == "measurement_matrix":
            return self._matrix_payload()
        if mode == "metrics_and_matrix":
            return {
                "schema": "context_metrics_and_matrix.v1",
                "metrics": self._metric_payload(str(project_id)),
                "matrix": self._matrix_payload(),
            }
        if mode == "warmup":
            return {
                "schema": "context_cache.warmup.v1",
                "project_id": project_id,
                "index": {"file_count": 12},
                "search_cache": {"query_count": 3},
            }
        if mode == "state_browser":
            state_key = str(arguments.get("state_key") or "")
            if state_key:
                return {
                    "schema": "context_state_browser.v1",
                    "mode": "entry",
                    "entry": {
                        "key": state_key,
                        "value_type": "dict",
                        "size_chars": 42,
                        "schema": "debug.sample.v1",
                        "status": "active",
                        "created_at": "2026-07-01T12:00:00+00:00",
                        "updated_at": "2026-07-01T12:00:00+00:00",
                        "expires_at": "",
                        "namespace": "debug.sample",
                        "preview": "\n".join(
                            [
                                "{",
                                '  "hello": "world",',
                                '  "line": 1',
                                "}",
                                "line 05",
                                "line 06",
                                "line 07",
                                "line 08",
                                "line 09",
                                "line 10",
                                "line 11",
                                "line 12",
                            ]
                        ),
                    },
                }
            return {
                "schema": "context_state_browser.v1",
                "mode": "list",
                "rows": [
                    {
                        "key": "cache:abc",
                        "value_type": "dict",
                        "size_chars": 42,
                        "schema": "debug.sample.v1",
                        "status": "active",
                        "created_at": "2026-07-01T10:00:00+00:00",
                        "updated_at": "2026-07-01T10:00:00+00:00",
                        "expires_at": "",
                        "namespace": "debug.sample",
                        "preview": "cache preview",
                    },
                    {
                        "key": "memory:def",
                        "value_type": "dict",
                        "size_chars": 64,
                        "schema": "memory.sample.v1",
                        "status": "active",
                        "created_at": "2026-07-01T12:00:00+00:00",
                        "updated_at": "2026-07-01T12:00:00+00:00",
                        "expires_at": "",
                        "preview": "memory preview",
                    },
                    {
                        "key": "reference:ghi",
                        "value_type": "str",
                        "size_chars": 96,
                        "schema": "reference.sample.v1",
                        "status": "active",
                        "created_at": "2026-07-01T13:00:00+00:00",
                        "updated_at": "2026-07-01T13:00:00+00:00",
                        "expires_at": "",
                        "preview": "reference preview",
                    },
                    {
                        "key": "cache:xyz",
                        "value_type": "dict",
                        "size_chars": 12,
                        "schema": "debug.sample.v1",
                        "status": "active",
                        "created_at": "2026-07-01T11:00:00+00:00",
                        "updated_at": "2026-07-01T11:00:00+00:00",
                        "expires_at": "",
                        "namespace": "debug.sample",
                        "preview": "second cache preview",
                    },
                ],
                "prefix_counts": [
                    {"prefix": "cache:", "count": 2},
                    {"prefix": "memory:", "count": 1},
                    {"prefix": "reference:", "count": 1},
                ],
            }
        raise AssertionError(f"unexpected call: {name} {arguments}")


class ConcurrentMetricsClient(FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self._lock = Lock()
        self._release = Event()
        self._active_metrics = 0
        self.metrics_started = 0
        self.max_active_metrics = 0

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        mode = arguments.get("mode")
        if mode != "metrics":
            return super().call_tool(name, arguments)

        with self._lock:
            self.calls.append((name, arguments))
            self._active_metrics += 1
            self.metrics_started += 1
            self.max_active_metrics = max(
                self.max_active_metrics, self._active_metrics
            )
            if self.metrics_started >= 2:
                self._release.set()
        self._release.wait(timeout=0.5)
        with self._lock:
            self._active_metrics -= 1
        project_id = arguments.get("project_id", "")
        return {
            "schema": "context_metrics.v1",
            "project_id": project_id,
            "requests": {"total": 1, "by_operation": {}},
            "cache": {
                "hits": 0,
                "misses": 0,
                "hit_ratio": 0.0,
                "context_pack_fragment_hits": 0,
                "context_pack_fragment_misses": 0,
                "context_pack_fragment_hit_ratio": 0.0,
            },
            "tokens": {},
            "references": {},
        }


class FailingProjectsClient(FakeClient):
    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if arguments.get("mode") == "projects":
            self.calls.append((name, arguments))
            raise RuntimeError("projects unavailable")
        return super().call_tool(name, arguments)


class OldStateBrowserServerClient(FakeClient):
    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if arguments.get("mode") == "state_browser":
            self.calls.append((name, arguments))
            raise RuntimeError(
                "MCP tool returned an error: {'content': [{'type': 'text', "
                "'text': \"Error executing tool context_admin: 1 validation "
                "error for context_adminArguments\\nmode\\n  Input should be "
                "'health', 'projects', 'index_refresh', 'index_status', "
                "'cache_stats', 'cache_prune', 'budget', 'contracts', "
                "'metrics', 'measurement_matrix' or 'benchmark' "
                "[type=literal_error, input_value='state_browser', "
                "input_type=str]\"}], 'isError': True}"
            )
        return super().call_tool(name, arguments)


def test_extract_tool_payload_from_text_content() -> None:
    monitor = load_monitor_module()

    payload = monitor.extract_tool_payload(
        {
            "content": [
                {
                    "type": "text",
                    "text": '{"schema": "context_metrics.v1", "requests": {"total": 2}}',
                }
            ]
        }
    )

    assert payload["schema"] == "context_metrics.v1"
    assert payload["requests"]["total"] == 2


def test_collect_snapshots_enumerates_projects_and_calls_metrics() -> None:
    monitor = load_monitor_module()
    client = FakeClient()

    snapshots = monitor.collect_snapshots(
        client,
        project_ids=[],
        root_uri="",
        include_matrix=True,
    )

    assert [row.target.project_id for row in snapshots] == ["alpha-123", "beta-456"]
    assert all(row.metrics["schema"] == "context_metrics.v1" for row in snapshots)
    assert ("context_admin", {"mode": "projects"}) in client.calls
    assert (
        "context_admin",
        {"mode": "metrics_and_matrix", "project_id": "alpha-123"},
    ) in client.calls
    assert (
        "context_admin",
        {"mode": "metrics_and_matrix", "project_id": "beta-456"},
    ) in client.calls


def test_collect_snapshots_fetches_project_metrics_in_parallel() -> None:
    monitor = load_monitor_module()
    client = ConcurrentMetricsClient()

    snapshots = monitor.collect_snapshots(
        client,
        project_ids=[],
        root_uri="",
        include_matrix=False,
        max_workers=2,
    )

    assert [row.target.project_id for row in snapshots] == ["alpha-123", "beta-456"]
    assert client.max_active_metrics == 2


def test_active_refresh_project_ids_targets_selected_project() -> None:
    monitor = load_monitor_module()
    snapshots = [
        monitor.ProjectSnapshot(target=monitor.ProjectTarget(project_id="alpha-123")),
        monitor.ProjectSnapshot(target=monitor.ProjectTarget(project_id="beta-456")),
    ]
    table_state = monitor.MonitorState(
        selected_index=1, view="table", refresh_interval=5.0
    )
    detail_state = monitor.MonitorState(
        selected_index=1, view="detail", refresh_interval=5.0
    )
    performance_state = monitor.MonitorState(
        selected_index=0, view="performance", refresh_interval=5.0
    )
    state_state = monitor.MonitorState(
        selected_index=0,
        view="state",
        refresh_interval=5.0,
        state_target=monitor.ProjectTarget(project_id="alpha-123"),
    )

    assert monitor._active_refresh_project_ids(["alpha-123", "beta-456"], "", table_state, snapshots) == [
        "alpha-123",
        "beta-456",
    ]
    assert monitor._active_refresh_project_ids(["alpha-123", "beta-456"], "", detail_state, snapshots) == [
        "beta-456",
    ]
    assert monitor._active_refresh_project_ids(["alpha-123", "beta-456"], "", performance_state, snapshots) == [
        "alpha-123",
    ]
    assert monitor._active_refresh_project_ids(["alpha-123", "beta-456"], "", state_state, snapshots) == [
        "alpha-123",
    ]


def test_collect_snapshots_project_id_survives_project_discovery_error() -> None:
    monitor = load_monitor_module()
    client = FailingProjectsClient()

    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["manual-project"],
        root_uri="",
        include_matrix=False,
    )

    assert len(snapshots) == 1
    assert snapshots[0].target.project_id == "manual-project"
    assert snapshots[0].metrics["schema"] == "context_metrics.v1"
    assert (
        "context_admin",
        {"mode": "metrics", "project_id": "manual-project"},
    ) in client.calls


def test_render_dashboard_contains_visual_summary() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=True,
    )

    rendered = monitor.render_dashboard(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
    )

    assert "mcp-context-manager metrics" in rendered
    assert "| cache hit          | [##############----]  75.0% |" in rendered
    assert "| fragment cache     | [##############----]  80.0% |" in rendered
    assert "| token spared/saved |                       12.0k |" in rendered
    assert "| project" in rendered
    assert "| Alpha" in rendered
    assert "| alpha-123" in rendered
    assert "|   10 |    3 |" in rendered
    assert "|  80.0% |" in rendered
    assert "2 pass" in rendered


def test_render_dashboard_keeps_long_project_names_readable() -> None:
    monitor = load_monitor_module()
    snapshot = monitor.ProjectSnapshot(
        target=monitor.ProjectTarget(
            project_id="akkodis-conan-center-index-1234567890abcdef",
            name="akkodis-conan-center-index",
            source="known",
        ),
        metrics={
            "requests": {
                "total": 4,
                "by_operation": {
                    "context_pack": {"count": 4, "avg_elapsed_ms": 413.9}
                },
            },
            "cache": {
                "hits": 0,
                "misses": 4,
                "hit_ratio": 0.0,
                "context_pack_fragment_hits": 1,
                "context_pack_fragment_misses": 3,
                "context_pack_fragment_hit_ratio": 0.25,
            },
            "tokens": {
                "estimated_input_tokens_saved": 9400,
                "tokens_spared_by_mcp_est": 8700,
            },
            "references": {"bytes_deferred_est": 4096},
        },
        matrix={"checks": [{"status": "fail"}, {"status": "fail"}]},
    )

    rendered = monitor.render_dashboard(
        [snapshot],
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
    )

    assert "| akkodis-conan-center-index" in rendered
    assert "akkodis-conan-center-index (" not in rendered
    assert "2 fail" in rendered


def test_interactive_key_bindings_update_state() -> None:
    monitor = load_monitor_module()
    state = monitor.MonitorState(selected_index=0, refresh_interval=5.0)

    assert monitor.decode_key("\x1b[B") == "down"
    assert monitor.decode_key("\x1b[A") == "up"
    assert monitor.decode_key("\r") == "enter"
    assert monitor.decode_key("\x1b") == "escape"
    assert monitor.decode_key("\x1b[5~") == "page_up"
    assert monitor.decode_key("\x1b[6~") == "page_down"
    assert monitor.decode_key("/") == "search"
    assert monitor.decode_key("\x7f") == "backspace"
    assert monitor.decode_key("x") == "text:x"
    assert monitor.decode_key("+") == "plus"
    assert monitor.decode_key("-") == "minus"
    assert monitor.decode_key("b") == "browser"
    assert monitor.decode_key("p") == "performance"
    assert monitor.decode_key("w") == "warmup"

    assert monitor.handle_key("down", state, row_count=3) == "redraw"
    assert state.selected_index == 1
    assert monitor.handle_key("enter", state, row_count=3) == "redraw"
    assert state.view == "detail"
    assert monitor.handle_key("escape", state, row_count=3) == "redraw"
    assert state.view == "table"
    assert monitor.handle_key("plus", state, row_count=3) == "redraw"
    assert state.refresh_interval == 6.0
    assert monitor.handle_key("minus", state, row_count=3) == "redraw"
    assert state.refresh_interval == 5.0
    assert monitor.handle_key("warmup", state, row_count=3) == "warmup"
    assert monitor.handle_key("browser", state, row_count=3) == "browser"
    assert state.view == "state"
    assert monitor.handle_key("down", state, row_count=3, state_row_count=2) == "redraw"
    assert state.state_selected_index == 1
    assert monitor.handle_key("enter", state, row_count=3, state_row_count=2) == "state_entry"
    assert monitor.handle_key("refresh", state, row_count=3, state_row_count=2) == "ignore"
    assert monitor.handle_key("warmup", state, row_count=3, state_row_count=2) == "ignore"
    assert monitor.handle_key("plus", state, row_count=3, state_row_count=2) == "ignore"
    state.state_search_active = True
    assert monitor.handle_key("warmup", state, row_count=3, state_row_count=2) == "redraw"
    assert state.state_search == "w"

    state.view = "table"
    assert monitor.handle_key("performance", state, row_count=3) == "redraw"
    assert state.view == "performance"


def test_default_refresh_interval_is_60_seconds() -> None:
    monitor = load_monitor_module()

    assert monitor.DEFAULT_INTERVAL == 60.0
    assert monitor.MonitorState().refresh_interval == 60.0


def test_render_monitor_screen_marks_selection_and_shows_detail() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=True,
    )
    state = monitor.MonitorState(
        selected_index=0,
        view="table",
        refresh_interval=5.0,
        last_updated="2026-07-05T13:00:00+00:00",
    )

    table = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    assert "| > | Alpha" in table
    assert "Enter details" in table
    assert "p performance" in table
    assert "w warmup" in table
    assert "refresh=5.0s" in table

    state.view = "detail"
    detail = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    assert "mcp-context-manager project details" in detail
    assert "| project            | Alpha" in detail
    assert "| project id         | alpha-123" in detail
    assert "| fragment cache     | 8/2 h/m   80.0%" in detail
    assert "token spared/saved" in detail
    assert "| a                        |         pass" in detail

    state.view = "performance"
    performance = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    assert "mcp-context-manager performance" in performance
    assert "| total_ms" in performance
    assert "| index_refresh_ms" in performance
    assert "freshness" in performance
    assert "last_good" in performance
    assert "background queue" in performance
    assert "2/1 h/m" in performance


def test_mcp_loading_status_renders_in_controls() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=True,
    )
    state = monitor.MonitorState(
        selected_index=0,
        view="table",
        refresh_interval=5.0,
        mcp_status="loading metrics...",
    )

    rendered = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    line = next(line for line in rendered.splitlines() if "r reload" in line)
    assert "mcp: loading metrics..." in line
    assert rendered.splitlines().count("mcp: loading metrics...") == 0


def test_selected_project_row_highlight_survives_cache_bar_reset() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=True,
    )

    rendered = monitor.render_dashboard(
        snapshots,
        url="http://localhost:8000/mcp",
        color=True,
        width=120,
        selected_index=0,
    )

    selected_line = next(line for line in rendered.splitlines() if "Alpha" in line)
    assert f"{monitor.Ansi.RESET}{monitor.Ansi.REVERSE}" in selected_line
    assert selected_line.endswith(monitor.Ansi.RESET)


def test_tokens_spared_by_mcp_prefers_explicit_metric_and_falls_back() -> None:
    monitor = load_monitor_module()

    assert (
        monitor._tokens_spared_by_mcp(
            {
                "tokens": {
                    "estimated_input_tokens_saved": 99,
                    "tokens_spared_by_mcp_est": 7,
                }
            }
        )
        == 7
    )
    assert (
        monitor._tokens_spared_by_mcp({"tokens": {"estimated_input_tokens_saved": 99}})
        == 99
    )


def test_state_browser_fetches_selected_project_and_renders_entry() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, view="state", refresh_interval=5.0)

    monitor._load_state_browser(client, state, snapshots)

    assert state.state_payload["schema"] == "context_state_browser.v1"
    assert state.state_target.project_id == "alpha-123"
    assert (
        "context_admin",
        {
            "mode": "state_browser",
            "project_id": "alpha-123",
            "max_entries": 80,
            "max_output_chars": 1200,
        },
    ) in client.calls

    rendered = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    assert "mcp-context-manager state browser" in rendered
    assert "| class        |" in rendered
    assert "created" in rendered
    assert rendered.index("cache:xyz") < rendered.index("cache:abc")
    assert "cache:abc" in rendered
    assert "| > | cache:xyz" in rendered

    monitor._load_state_entry(client, state)
    assert state.view == "state"
    assert state.state_entry is not None
    detail = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )

    assert "mcp-context-manager state entry" in detail
    assert "cache:xyz" in detail
    assert '"hello": "world"' in detail

    assert (
        monitor.handle_key(
            "escape",
            state,
            row_count=len(snapshots),
            state_row_count=monitor._state_row_count(state),
        )
        == "redraw"
    )
    assert state.view == "state"
    assert state.state_entry is None


def test_state_entry_view_scrolls_content() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, view="state", refresh_interval=5.0)
    monitor._load_state_browser(client, state, snapshots)
    monitor._load_state_entry(client, state)

    first_page = monitor.render_state_browser(
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        height=21,
        state=state,
    )
    assert "mcp-context-manager state entry" in first_page
    assert "preview lines 1-8 / 12" in first_page
    assert '"hello": "world"' in first_page
    assert "line 08" in first_page
    assert "line 09" not in first_page

    assert (
        monitor.handle_key(
            "page_down",
            state,
            row_count=len(snapshots),
            state_row_count=monitor._state_row_count(state),
        )
        == "redraw"
    )
    second_page = monitor.render_state_browser(
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        height=21,
        state=state,
    )
    assert "preview lines 5-12 / 12" in second_page
    assert "line 05" in second_page
    assert "line 10" in second_page
    assert '"hello": "world"' not in second_page

    assert (
        monitor.handle_key(
            "home",
            state,
            row_count=len(snapshots),
            state_row_count=monitor._state_row_count(state),
        )
        == "redraw"
    )
    assert state.state_entry_scroll_offset == 0


def test_background_mcp_operation_result_updates_state() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, view="state", refresh_interval=5.0)

    with monitor.ThreadPoolExecutor(max_workers=1) as executor:
        pending = monitor._submit_mcp_operation(
            executor,
            "state_browser",
            lambda: monitor._fetch_state_browser_result(client, snapshots, 0),
        )
        pending.future.result(timeout=2)
        updated = monitor._apply_mcp_operation_result(state, snapshots, pending)

    assert updated == snapshots
    assert state.mcp_status == ""
    assert state.mcp_error == ""
    assert state.state_target.project_id == "alpha-123"
    assert state.state_payload["schema"] == "context_state_browser.v1"


def test_warmup_selected_project_calls_admin_and_sets_status() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, refresh_interval=5.0)

    with monitor.ThreadPoolExecutor(max_workers=1) as executor:
        pending = monitor._submit_mcp_operation(
            executor,
            "warmup",
            lambda: monitor._fetch_warmup_result(client, snapshots, 0),
        )
        pending.future.result(timeout=2)
        updated = monitor._apply_mcp_operation_result(state, snapshots, pending)

    assert updated == snapshots
    assert state.mcp_error == ""
    assert state.mcp_status == "warmed Alpha: 3 queries, 12 files"
    assert (
        "context_admin",
        {"mode": "warmup", "project_id": "alpha-123"},
    ) in client.calls


def test_state_browser_filters_rows_with_search_input() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, view="state", refresh_interval=5.0)
    monitor._load_state_browser(client, state, snapshots)

    assert monitor.handle_key("search", state, row_count=1, state_row_count=4) == "redraw"
    for key in ("text:m", "text:e", "text:m"):
        assert monitor.handle_key(key, state, row_count=1, state_row_count=4) == "redraw"

    assert state.state_search == "mem"
    assert state.state_search_active
    assert monitor._state_row_count(state) == 1

    rendered = monitor.render_state_browser(
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        height=20,
        state=state,
    )

    assert "rows:     1 / 4" in rendered
    assert "search:   mem  (typing)" in rendered
    assert "memory:def" in rendered
    assert "cache:abc" not in rendered

    assert monitor.handle_key("enter", state, row_count=1, state_row_count=1) == "redraw"
    assert not state.state_search_active


def test_state_browser_scrolls_visible_window() -> None:
    monitor = load_monitor_module()
    client = FakeClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(
        selected_index=0,
        view="state",
        refresh_interval=5.0,
        state_selected_index=3,
    )
    monitor._load_state_browser(client, state, snapshots)

    rendered = monitor.render_state_browser(
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        height=16,
        state=state,
    )

    assert "rows:     4 / 4" in rendered
    assert "window: 2-4" in rendered
    assert "cache:abc" in rendered
    assert "memory:def" in rendered
    assert "reference:ghi" in rendered
    assert "| > | reference:ghi" in rendered
    assert "cache:xyz" not in rendered


def test_state_browser_old_server_error_is_actionable() -> None:
    monitor = load_monitor_module()
    client = OldStateBrowserServerClient()
    snapshots = monitor.collect_snapshots(
        client,
        project_ids=["alpha-123"],
        root_uri="",
        include_matrix=False,
    )
    state = monitor.MonitorState(selected_index=0, view="state", refresh_interval=5.0)

    monitor._load_state_browser(client, state, snapshots)

    assert state.state_payload is None
    assert "does not support context_admin(mode='state_browser')" in state.state_error
    assert "docker compose up -d --build" in state.state_error
    rendered = monitor.render_monitor_screen(
        snapshots,
        url="http://localhost:8000/mcp",
        color=False,
        width=120,
        state=state,
    )
    assert "Rebuild and restart" in rendered


def test_lmdb_write_transaction_error_is_actionable() -> None:
    monitor = load_monitor_module()

    message = monitor.friendly_mcp_error(
        RuntimeError(
            "MCP tool returned an error: Error executing tool context_admin: "
            "A write transaction is already active on this environment. "
            "Only one top-level write transaction is allowed at a time."
        )
    )

    assert "concurrent LMDB writes" in message
    assert "docker compose up -d --build" in message
