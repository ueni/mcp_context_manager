from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
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
            return {
                "schema": "context_metrics.v1",
                "project_id": project_id,
                "requests": {
                    "total": 10,
                    "by_operation": {
                        "context_pack": {"count": 3, "avg_elapsed_ms": 42.5}
                    },
                },
                "cache": {"hits": 6, "misses": 2, "hit_ratio": 0.75},
                "tokens": {
                    "estimated_input_tokens_saved": 12345,
                    "tokens_spared_by_mcp_est": 12000,
                },
                "references": {"bytes_deferred_est": 4096},
            }
        if mode == "measurement_matrix":
            return {
                "schema": "context_measurement_matrix.v1",
                "checks": [
                    {"key": "a", "status": "pass"},
                    {"key": "b", "status": "pass"},
                ],
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
                        "expires_at": "",
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
                        "expires_at": "",
                        "preview": "cache preview",
                    },
                    {
                        "key": "memory:def",
                        "value_type": "dict",
                        "size_chars": 64,
                        "schema": "memory.sample.v1",
                        "status": "active",
                        "expires_at": "",
                        "preview": "memory preview",
                    },
                    {
                        "key": "reference:ghi",
                        "value_type": "str",
                        "size_chars": 96,
                        "schema": "reference.sample.v1",
                        "status": "active",
                        "expires_at": "",
                        "preview": "reference preview",
                    },
                    {
                        "key": "cache:xyz",
                        "value_type": "dict",
                        "size_chars": 12,
                        "schema": "debug.sample.v1",
                        "status": "active",
                        "expires_at": "",
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
        {"mode": "metrics", "project_id": "alpha-123"},
    ) in client.calls
    assert (
        "context_admin",
        {"mode": "measurement_matrix", "project_id": "beta-456"},
    ) in client.calls


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
    assert "| cache hit     | [##############----]  75.0% |" in rendered
    assert "| project" in rendered
    assert "| Alpha" in rendered
    assert "| alpha-123" in rendered
    assert "|    10 |     3 |" in rendered
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
            "cache": {"hits": 0, "misses": 4, "hit_ratio": 0.0},
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
    assert monitor.handle_key("browser", state, row_count=3) == "browser"
    assert state.view == "state"
    assert monitor.handle_key("down", state, row_count=3, state_row_count=2) == "redraw"
    assert state.state_selected_index == 1
    assert monitor.handle_key("enter", state, row_count=3, state_row_count=2) == "state_entry"
    assert monitor.handle_key("refresh", state, row_count=3, state_row_count=2) == "ignore"
    assert monitor.handle_key("plus", state, row_count=3, state_row_count=2) == "ignore"


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
    assert "| project       | Alpha" in detail
    assert "| project id    | alpha-123" in detail
    assert "mcp spared" in detail
    assert "| a                        |         pass" in detail


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
    assert "cache:abc" in rendered
    assert "| > | cache:abc" in rendered

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

    assert "mcp-context-manager state browser" in detail
    assert "state entry overlay" in detail
    assert "cache:abc" in detail
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


def test_state_entry_overlay_scrolls_content() -> None:
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
    assert "preview lines 1-3 / 12" in first_page
    assert '"hello": "world"' in first_page
    assert "line 05" not in first_page

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
    assert "preview lines 10-12 / 12" in second_page
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
    assert "memory:def" in rendered
    assert "reference:ghi" in rendered
    assert "| > | cache:xyz" in rendered
    assert "cache:abc" not in rendered


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
