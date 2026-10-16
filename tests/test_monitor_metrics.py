"""Regression tests for the monitor's project selection policy."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).parents[1] / "monitor.py"
SPEC = importlib.util.spec_from_file_location("monitor_metrics", MODULE_PATH)
assert SPEC and SPEC.loader
monitor_metrics = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor_metrics
SPEC.loader.exec_module(monitor_metrics)


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def call_tool(self, name: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((name, arguments))
        if arguments["mode"] == "cached_projects":
            return {
                "projects": [
                    {"project_id": "one", "name": "one"},
                    {"project_id": "two", "name": "two"},
                ]
            }
        return {
            "metrics": {"schema": "context_metrics.v1"},
            "matrix": {"schema": "context_measurement_matrix.v1"},
        }


class MonitorMcpTimeoutTests(unittest.TestCase):
    def test_warmup_uses_long_deadline_without_weakening_normal_calls(self) -> None:
        class RecordingClient(monitor_metrics.McpHttpClient):
            def __init__(self) -> None:
                super().__init__("http://localhost:8000/mcp", timeout=5.0)
                self.timeouts: list[float | None] = []

            def initialize(self) -> None:
                return

            def rpc(
                self,
                method: str,
                params: dict[str, object] | None = None,
                expect_result: bool = True,
                timeout: float | None = None,
            ) -> dict[str, object]:
                self.timeouts.append(timeout)
                return {"content": [{"type": "text", "text": "{}"}]}

        client = RecordingClient()

        client.call_tool("context_admin", {"mode": "warmup"})
        client.call_tool("context_admin", {"mode": "metrics"})

        self.assertEqual(
            client.timeouts,
            [monitor_metrics.DEFAULT_WARMUP_TIMEOUT, 5.0],
        )

    def test_warmup_preserves_larger_configured_deadline(self) -> None:
        client = monitor_metrics.McpHttpClient(
            "http://localhost:8000/mcp", timeout=90.0
        )

        self.assertEqual(
            client._timeout_for_tool("context_admin", {"mode": "warmup"}),
            90.0,
        )


class MonitorUsageControlTests(unittest.TestCase):
    def test_startup_requests_status_once_and_controls_show_state(self) -> None:
        state = monitor_metrics.MonitorState()
        snapshots = [
            monitor_metrics.ProjectSnapshot(
                target=monitor_metrics.ProjectTarget(project_id="one")
            )
        ]

        self.assertTrue(
            monitor_metrics._needs_initial_usage_status(state, snapshots, None)
        )
        self.assertIn(
            "detailed=unknown", monitor_metrics._mcp_status_line(state, False)
        )
        state.usage_status_requested = True
        state.usage_enabled = False
        self.assertFalse(
            monitor_metrics._needs_initial_usage_status(state, snapshots, None)
        )
        self.assertIn("detailed=off", monitor_metrics._mcp_status_line(state, False))
        state.usage_enabled = True
        self.assertIn("detailed=on", monitor_metrics._mcp_status_line(state, False))

    def test_l_key_is_a_nonblocking_monitor_usage_action(self) -> None:
        state = monitor_metrics.MonitorState(view="performance")

        self.assertEqual(monitor_metrics.decode_key("l"), "monitor_usage")
        self.assertEqual(
            monitor_metrics.handle_key("monitor_usage", state, row_count=1),
            "monitor_usage",
        )

    def test_report_is_scoped_to_selected_project(self) -> None:
        client = FakeClient()
        target = monitor_metrics.ProjectTarget(project_id="one")

        monitor_metrics.monitor_usage(client, "report", target)

        self.assertEqual(
            client.calls,
            [
                (
                    "context_admin",
                    {"mode": "monitor_usage", "action": "report", "project_id": "one"},
                )
            ],
        )


class McpInspectionViewTests(unittest.TestCase):
    def test_inspection_bounds_content_without_a_truncation_suffix(self) -> None:
        preview = monitor_metrics._bounded_text("last_elapsed_ms" + "x" * 32, 16)

        self.assertEqual(preview, "last_elapsed_msx")
        self.assertLessEqual(len(preview), 16)
        self.assertNotIn("…", preview)
        self.assertNotIn("(truncated)", preview)

    def test_catalogue_uses_one_table_and_resources_are_read_on_demand(self) -> None:
        class InspectionClient:
            def __init__(self) -> None:
                self.read_uris: list[str] = []

            def list_tools(self) -> dict[str, object]:
                return {
                    "tools": [
                        {
                            "name": "context_pack",
                            "description": "Build compact context.",
                        }
                    ]
                }

            def list_resources(self) -> dict[str, object]:
                return {
                    "resources": [
                        {
                            "uri": "repo://instructions/test",
                            "name": "Agent instructions",
                            "description": "Agent rules.",
                        },
                        {
                            "uri": "repo://summary",
                            "name": "Repository summary",
                            "description": "Repository overview.",
                        },
                    ]
                }

            def read_resource(self, uri: str) -> dict[str, object]:
                self.read_uris.append(uri)
                return {"contents": [{"uri": uri, "text": "Use context_pack first."}]}

        client = InspectionClient()
        payload = monitor_metrics.inspect_mcp_surface(client)
        self.assertEqual(client.read_uris, [])
        state = monitor_metrics.MonitorState(
            view="inspection", inspection_payload=payload
        )
        rendered = monitor_metrics.render_mcp_inspection(
            "http://localhost:8000/mcp", False, 120, state
        )

        self.assertIn("| type", rendered)
        self.assertIn("| context_pack", rendered)
        self.assertIn("Build compact context.", rendered)
        self.assertIn("| resource", rendered)
        self.assertIn("Agent instructions", rendered)
        self.assertIn("repo://instructions/test", rendered)
        self.assertNotIn("TOOLS", rendered)
        self.assertNotIn("SELECTED RESOURCE", rendered)

        self.assertEqual(monitor_metrics.handle_key("enter", state, 0), "redraw")
        self.assertEqual(state.inspection_viewer_index, 0)
        tool_viewer = monitor_metrics.render_mcp_inspection("url", False, 120, state)
        self.assertIn("TOOL  context_pack", tool_viewer)
        self.assertIn('"name": "context_pack"', tool_viewer)
        self.assertIn('"description": "Build compact context."', tool_viewer)
        self.assertEqual(monitor_metrics.handle_key("escape", state, 0), "redraw")
        self.assertIsNone(state.inspection_viewer_index)

        self.assertEqual(monitor_metrics.handle_key("down", state, 0), "redraw")
        self.assertEqual(monitor_metrics.handle_key("down", state, 0), "redraw")
        self.assertEqual(state.inspection_selected_index, 2)
        self.assertEqual(
            monitor_metrics.handle_key("enter", state, 0), "inspection_resource"
        )
        self.assertEqual(state.inspection_viewer_index, 2)
        self.assertIn(
            "Loading resource content...",
            monitor_metrics.render_mcp_inspection("url", False, 120, state),
        )

        result = monitor_metrics.inspect_mcp_resource_content(client, "repo://summary")
        monitor_metrics._apply_mcp_resource_content_result(state, result)
        self.assertEqual(client.read_uris, ["repo://summary"])
        self.assertIn(
            "Use context_pack first.",
            monitor_metrics.render_mcp_inspection("url", False, 120, state),
        )

    def test_inspection_renders_resource_read_failures_and_is_discoverable(
        self,
    ) -> None:
        class FailingResourceClient:
            def list_tools(self) -> dict[str, object]:
                return {"tools": []}

            def list_resources(self) -> dict[str, object]:
                return {"resources": [{"uri": "repo://broken"}]}

            def read_resource(self, _uri: str) -> dict[str, object]:
                raise RuntimeError("read failed")

        client = FailingResourceClient()
        payload = monitor_metrics.inspect_mcp_surface(client)
        state = monitor_metrics.MonitorState(
            view="inspection", inspection_payload=payload
        )
        self.assertEqual(
            monitor_metrics.handle_key("enter", state, 0), "inspection_resource"
        )
        result = monitor_metrics.inspect_mcp_resource_content(client, "repo://broken")
        monitor_metrics._apply_mcp_resource_content_result(state, result)
        rendered = monitor_metrics.render_mcp_inspection("url", False, 120, state)

        self.assertIn("ERROR: read failed", rendered)
        self.assertEqual(monitor_metrics.decode_key("i"), "inspection")
        self.assertEqual(
            monitor_metrics.handle_key("inspection", monitor_metrics.MonitorState(), 0),
            "inspection",
        )
        self.assertIn("i inspect", monitor_metrics._controls(60, False))
        self.assertIn("Enter open", monitor_metrics._controls(60, False))

    def test_resource_viewer_prettifies_json_scrolls_and_returns_to_catalogue(
        self,
    ) -> None:
        class JsonResourceClient:
            def read_resource(self, uri: str) -> dict[str, object]:
                return {
                    "contents": [
                        {
                            "uri": uri,
                            "text": json.dumps(
                                {
                                    "outer": {"items": list(range(24))},
                                    "full": ("x" * 2_100) + "END-OF-FULL-RESOURCE",
                                },
                                separators=(",", ":"),
                            ),
                        }
                    ]
                }

        state = monitor_metrics.MonitorState(
            view="inspection",
            inspection_payload={
                "tools": [
                    {
                        "name": "context_pack",
                        "description": "Build compact cited context.",
                    }
                ],
                "resources": [{"uri": "repo://metrics", "name": "Repository metrics"}],
                "resource_count": 1,
            },
        )
        result = monitor_metrics.inspect_mcp_resource_content(
            JsonResourceClient(), "repo://metrics"
        )
        self.assertGreater(len(result.content), 2_000)
        self.assertIn("END-OF-FULL-RESOURCE", result.content)
        state.inspection_selected_index = 1
        self.assertEqual(
            monitor_metrics.handle_key("enter", state, 0), "inspection_resource"
        )
        monitor_metrics._apply_mcp_resource_content_result(state, result)
        rendered = monitor_metrics.render_mcp_inspection(
            "url", False, 100, state, height=22
        )

        self.assertIn('"outer": {', rendered)
        self.assertIn("RESOURCE  Repository metrics", rendered)
        self.assertIn("lines 1-", rendered)
        self.assertIn("Up/Down scroll", rendered)
        self.assertEqual(monitor_metrics.handle_key("down", state, 0), "redraw")
        self.assertEqual(state.inspection_content_scroll_offset, 1)
        self.assertEqual(monitor_metrics.handle_key("page_down", state, 0), "redraw")
        self.assertEqual(state.inspection_content_scroll_offset, 11)
        rendered_after_scroll = monitor_metrics.render_mcp_inspection(
            "url", False, 100, state, height=22
        )
        self.assertIn("lines 12-", rendered_after_scroll)
        self.assertEqual(monitor_metrics.handle_key("home", state, 0), "redraw")
        self.assertEqual(state.inspection_content_scroll_offset, 0)
        self.assertEqual(monitor_metrics.handle_key("end", state, 0), "redraw")
        rendered_at_end = monitor_metrics.render_mcp_inspection(
            "url", False, 100, state, height=22
        )
        self.assertIn("END-OF-FULL-RESOURCE", rendered_at_end)
        self.assertEqual(monitor_metrics.handle_key("escape", state, 0), "redraw")
        self.assertIsNone(state.inspection_viewer_index)
        catalogue = monitor_metrics.render_mcp_inspection("url", False, 100, state)
        self.assertIn("CATALOGUE", catalogue)


class MonitorProjectSelectionTests(unittest.TestCase):
    def test_default_snapshot_uses_cached_projects_without_discovery(self) -> None:
        client = FakeClient()

        snapshots = monitor_metrics.collect_snapshots(
            client, project_ids=[], root_uri="", include_matrix=True
        )

        self.assertEqual(
            [snapshot.target.project_id for snapshot in snapshots], ["one", "two"]
        )
        self.assertEqual(
            client.calls,
            [
                ("context_admin", {"mode": "cached_projects"}),
                ("context_admin", {"mode": "measurement_report", "project_id": "one"}),
                ("context_admin", {"mode": "measurement_report", "project_id": "two"}),
            ],
        )

    def test_explicit_projects_do_not_trigger_workspace_discovery(self) -> None:
        client = FakeClient()

        snapshots = monitor_metrics.collect_snapshots(
            client,
            project_ids=["one", "two"],
            root_uri="",
            include_matrix=True,
        )

        self.assertEqual(
            [snapshot.target.project_id for snapshot in snapshots], ["one", "two"]
        )
        self.assertEqual(
            client.calls,
            [
                ("context_admin", {"mode": "measurement_report", "project_id": "one"}),
                ("context_admin", {"mode": "measurement_report", "project_id": "two"}),
            ],
        )

    def test_cached_root_uri_source_prefers_project_id_for_actions(self) -> None:
        client = FakeClient()
        target = monitor_metrics.ProjectTarget(
            project_id="demo-project",
            name="demo-project",
            source="root_uri",
        )

        monitor_metrics.fetch_state_browser(client, target)
        monitor_metrics.warmup_project(client, target)
        monitor_metrics.prune_project(client, target)

        self.assertEqual(
            client.calls,
            [
                (
                    "context_admin",
                    {
                        "mode": "state_browser",
                        "project_id": "demo-project",
                        "max_entries": 80,
                        "max_output_chars": 1200,
                    },
                ),
                (
                    "context_admin",
                    {"mode": "warmup", "project_id": "demo-project"},
                ),
                (
                    "context_admin",
                    {"mode": "cache_prune", "project_id": "demo-project"},
                ),
            ],
        )

    def test_explicit_root_uri_is_used_without_project_id(self) -> None:
        client = FakeClient()
        target = monitor_metrics.ProjectTarget(
            project_id="",
            name="file:///workspace/demo-project",
            source="root_uri",
        )

        monitor_metrics.warmup_project(client, target)

        self.assertEqual(
            client.calls,
            [
                (
                    "context_admin",
                    {
                        "mode": "warmup",
                        "root_uri": "file:///workspace/demo-project",
                    },
                )
            ],
        )


class MonitorStateEntryViewTests(unittest.TestCase):
    def test_renders_top_level_state_browser_entry(self) -> None:
        state = monitor_metrics.MonitorState()
        state.state_target = monitor_metrics.ProjectTarget(project_id="demo")
        state.state_entry = {
            "schema": "context_state_browser.v1",
            "mode": "entry",
            "key": "cache:demo",
            "value": {"answer": 42},
            "preview": '{"answer":42}',
            "value_type": "dict",
            "size_chars": 13,
            "status": "active",
            "expires_at": "2099-01-01T00:00:00Z",
        }

        rendered = monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 100, state, height=30
        )

        self.assertIn("cache:demo", rendered)
        self.assertIn("dict", rendered)
        self.assertIn('"answer": 42', rendered)

    def test_renders_value_only_state_browser_entry(self) -> None:
        state = monitor_metrics.MonitorState()
        state.state_target = monitor_metrics.ProjectTarget(project_id="demo")
        state.state_entry = {
            "schema": "context_state_browser.v1",
            "mode": "entry",
            "key": "memory:demo",
            "found": True,
            "value": {"records": [1, 2]},
        }

        rendered = monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 100, state, height=30
        )

        self.assertIn("dict", rendered)
        self.assertIn('"records": [', rendered)
        self.assertIn("present", rendered)
        self.assertIn("not set", rendered)

    def test_keeps_non_json_preview_unchanged(self) -> None:
        state = monitor_metrics.MonitorState()
        state.state_entry = {"key": "cache:raw", "preview": "not-json"}

        rendered = monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 100, state, height=30
        )

        self.assertIn("not-json", rendered)

    def test_renders_stored_metadata_and_bounds_large_preview(self) -> None:
        state = monitor_metrics.MonitorState()
        state.state_target = monitor_metrics.ProjectTarget(project_id="demo")
        state.state_entry = {
            "schema": "context_state_browser.v1",
            "mode": "entry",
            "key": "memory:demo",
            "found": True,
            "value": {
                "schema": "context_memory.entry.v1",
                "status": "ready",
                "expires_at": "2099-01-01T00:00:00Z",
                "records": ["x" * 1024] * 100,
            },
        }

        rendered = monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 80, state, height=24
        )

        self.assertIn("context_memory.entry.v1", rendered)
        self.assertIn("ready", rendered)
        self.assertIn("2099-01-01T00:00:00Z", rendered)
        self.assertIn("(truncated)", rendered)
        self.assertLessEqual(
            len(state.state_entry_preview),
            monitor_metrics.STATE_ENTRY_PREVIEW_LIMIT,
        )
        cached_lines = state.state_entry_preview_lines
        monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 80, state, height=24
        )
        self.assertIs(state.state_entry_preview_lines, cached_lines)


class MonitorWikiBrowserTests(unittest.TestCase):
    def test_wiki_status_and_browse_requests_are_selected_project_scoped(self) -> None:
        class WikiClient:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict[str, object]]] = []

            def call_tool(
                self, name: str, arguments: dict[str, object]
            ) -> dict[str, object]:
                self.calls.append((name, arguments))
                if arguments["mode"] == "wiki":
                    return {
                        "schema": "agent_wiki.status.v1",
                        "generation": 4,
                        "chunk_count": 12,
                        "project_count": 2,
                    }
                if arguments["mode"] == "wiki_search":
                    return {
                        "schema": "agent_wiki.search.v1",
                        "results": [
                            {
                                "page_id": "transport",
                                "revision": "abc123",
                                "title": "Transport behavior",
                                "summary": "Retries are bounded.",
                                "source_status": "current",
                            }
                        ],
                    }
                return {
                    "schema": "agent_wiki.get.v1",
                    "status": "resolved",
                    "page_id": "transport",
                    "revision": "abc123",
                    "project_id": "one",
                    "source_status": "current",
                    "reference": {"kind": "wiki"},
                    "page": {
                        "title": "Transport behavior",
                        "summary": "Retries are bounded.",
                        "claims": [
                            {
                                "text": "Retries stop after three attempts.",
                                "status": "verified",
                                "citations": [{"source_id": "source-a"}],
                            }
                        ],
                    },
                }

        client = WikiClient()
        target = monitor_metrics.ProjectTarget(project_id="one", name="one")
        result = monitor_metrics._fetch_wiki_browser_result(
            client,
            [monitor_metrics.ProjectSnapshot(target=target)],
            0,
            "",
        )

        self.assertEqual(result.status["generation"], 4)
        self.assertEqual(result.results[0]["page_id"], "transport")
        self.assertEqual(
            client.calls,
            [
                (
                    "context_admin",
                    {"mode": "wiki", "action": "status", "project_id": "one"},
                ),
                (
                    "context_lookup",
                    {
                        "mode": "wiki_search",
                        "query": "",
                        "wiki_scope": "selected_project",
                        "max_results": 40,
                        "project_id": "one",
                    },
                ),
            ],
        )

        page_result = monitor_metrics._fetch_wiki_page_result(
            client, target, "transport"
        )
        self.assertEqual(page_result.payload["reference"]["kind"], "wiki")
        self.assertEqual(
            client.calls[-1],
            (
                "context_lookup",
                {
                    "mode": "wiki_get",
                    "path": "transport",
                    "wiki_scope": "selected_project",
                    "project_id": "one",
                },
            ),
        )

    def test_wiki_view_renders_status_pages_and_page_citations(self) -> None:
        state = monitor_metrics.MonitorState(
            view="wiki",
            mcp_status="loading wiki status and pages...",
            wiki_target=monitor_metrics.ProjectTarget(project_id="one", name="demo"),
            wiki_status={
                "generation": 4,
                "chunk_count": 12,
                "project_count": 2,
                "history": {"status": "idle"},
                "source_refresh": {"status": "current"},
            },
            wiki_results=[
                {
                    "page_id": "transport",
                    "title": "Transport behavior",
                    "source_status": "current",
                }
            ],
        )
        listing = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 120, state, height=30
        )

        self.assertIn("generation 4 / 12 chunks", listing)
        self.assertIn("Transport behavior", listing)
        self.assertIn("current", listing)
        self.assertIn("loading wiki status and pages", listing)

        state.wiki_page = {
            "schema": "agent_wiki.get.v1",
            "status": "resolved",
            "page_id": "transport",
            "revision": "abc123",
            "project_id": "one",
            "source_status": "current",
            "reference": {"kind": "wiki", "page_id": "transport"},
            "page": {
                "title": "Transport behavior",
                "summary": "Retries are bounded.",
                "claims": [
                    {
                        "text": "Retries stop after three attempts.",
                        "status": "verified",
                        "citations": [{"source_id": "source-a"}],
                    }
                ],
            },
        }
        detail = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 120, state, height=30
        )
        self.assertIn("Retries stop after three attempts.", detail)
        self.assertIn("citation: source-a", detail)

    def test_wiki_page_wraps_long_text_for_vertical_browsing(self) -> None:
        state = monitor_metrics.MonitorState(
            view="wiki",
            wiki_target=monitor_metrics.ProjectTarget(project_id="one", name="demo"),
            wiki_page={
                "schema": "agent_wiki.get.v1",
                "status": "resolved",
                "page_id": "long-page",
                "revision": "abc123",
                "project_id": "one",
                "source_status": "current",
                "reference": {"kind": "wiki", "page_id": "long-page"},
                "page": {
                    "title": "Long page",
                    "summary": "A detailed summary with enough words to wrap across several terminal lines.",
                    "claims": [
                        {
                            "text": "A claim with enough words to wrap and remain readable in a narrow terminal window.",
                            "status": "verified",
                            "citations": [{"source_id": "source-a"}],
                        }
                    ],
                },
            },
        )

        rendered = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 36, state, height=30
        )

        self.assertIn("A detailed summary", rendered)
        self.assertIn("terminal", rendered)
        self.assertIn("lines.", rendered)
        self.assertIn("remain readable", rendered)
        self.assertTrue(all(len(line) <= 36 for line in rendered.splitlines()))

    def test_wiki_navigation_and_empty_or_error_results_are_safe(self) -> None:
        state = monitor_metrics.MonitorState()
        self.assertEqual(monitor_metrics.decode_key("v"), "wiki")
        self.assertEqual(monitor_metrics.handle_key("wiki", state, row_count=1), "wiki")
        self.assertEqual(state.view, "wiki")
        self.assertEqual(monitor_metrics.handle_key("search", state, 1), "redraw")
        self.assertTrue(state.wiki_query_active)
        self.assertEqual(monitor_metrics.handle_key("text:chip", state, 1), "redraw")
        self.assertEqual(state.wiki_query, "chip")
        self.assertEqual(monitor_metrics.handle_key("enter", state, 1), "wiki_search")

        state.wiki_results = [{"page_id": "transport"}]
        self.assertEqual(monitor_metrics.handle_key("enter", state, 1), "wiki_page")
        state.wiki_page = {"page_id": "transport", "page": {}}
        self.assertEqual(monitor_metrics.handle_key("escape", state, 1), "redraw")
        self.assertIsNone(state.wiki_page)
        self.assertEqual(state.view, "wiki")

        state.wiki_error = "wiki service unavailable"
        rendered = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 100, state, height=24
        )
        self.assertIn("ERROR: wiki service unavailable", rendered)
        state.wiki_error = ""
        state.wiki_query = ""
        state.wiki_results = []
        rendered = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 100, state, height=24
        )
        self.assertIn("No wiki pages stored for this project.", rendered)
        state.wiki_query = "missing"
        rendered = monitor_metrics.render_wiki_browser(
            "http://localhost:8000/mcp", False, 100, state, height=24
        )
        self.assertIn("No matching wiki pages.", rendered)
        self.assertEqual(monitor_metrics.handle_key("escape", state, 1), "redraw")
        self.assertEqual(state.view, "table")


def native_metrics(*, status: str = "idle") -> dict[str, object]:
    return {
        "schema": "context_metrics.v2",
        "requests": {
            "total": 24,
            "by_operation": {
                "context_pack": {
                    "count": 20,
                    "avg_elapsed_ms": 21.5,
                    "min_elapsed_ms": 10.0,
                    "max_elapsed_ms": 40.0,
                    "last_elapsed_ms": 20.0,
                    "p50_recent_ms": 19.0,
                    "p95_recent_ms": 35.0,
                },
                "context_lookup.search": {
                    "count": 4,
                    "avg_elapsed_ms": 3.0,
                    "min_elapsed_ms": 2.0,
                    "max_elapsed_ms": 5.0,
                    "last_elapsed_ms": 3.0,
                    "p50_recent_ms": 3.0,
                    "p95_recent_ms": 5.0,
                },
            },
        },
        "tokens": {
            "context_pack": {
                "selected_source_tokens_est": 900,
                "evidence_card_tokens_est": 240,
                "returned_evidence_tokens_est": 120,
                "wire_tokens_est": 180,
                "wire_bytes": 720,
                "saved_tokens_est": 720,
                "continuation_wire_tokens_avoided_est": 80,
                "compression_factor_est": 5.0,
            }
        },
        "context_pack": {"candidate_count": 12, "retrieval_micros": 4000},
        "cache": {
            "lookup": {"hits": 12, "misses": 4, "entries": 7, "bytes": 65536},
            "file": {
                "hits": 8,
                "misses": 2,
                "hit_ratio": 0.8,
                "entries": 10,
                "bytes": 4096,
                "latency_total_micros": 250,
            },
            "git_diff": {
                "hits": 3,
                "misses": 2,
                "hit_ratio": 0.6,
                "regenerations": 2,
                "invalidations": 1,
                "patch_eligible": 1,
                "reload_fallbacks": 1,
                "latency_total_micros": 5000,
            },
            "symbols": {
                "queries": 5,
                "posting_hits": 4,
                "posting_misses": 1,
                "candidate_count": 12,
                "latency_total_micros": 1000,
            },
        },
        "continuation": {"requests": 4, "reuses": 3, "fallbacks": 1},
        "references": {"active_count": 4, "total_count": 6},
        "index_freshness": {"generation": 3, "dirty": False, "refreshes": 2},
        "background": {"status": status},
    }


def quality_matrix(*, status: str = "pass", current: object = 1.0) -> dict[str, object]:
    return {
        "schema": "context_measurement_matrix.v1",
        "checks": [
            {
                "key": "quality.required_anchor_recall",
                "current": current,
                "target": 1.0,
                "operator": ">=",
                "status": status,
            },
            {
                "key": "quality.noise_ratio",
                "current": current,
                "target": 0.3,
                "operator": "<=",
                "status": status,
            },
        ],
    }


class MonitorNativeMetricsRenderingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = monitor_metrics.ProjectSnapshot(
            target=monitor_metrics.ProjectTarget(project_id="native", name="native"),
            metrics=native_metrics(),
            matrix=quality_matrix(),
        )

    def test_dashboard_uses_combined_cache_and_index_counters(self) -> None:
        rendered = monitor_metrics.render_dashboard(
            [self.snapshot], "http://localhost:8000/mcp", color=False, width=160
        )

        self.assertIn("max cache hit", rendered)
        self.assertIn("80.0%", rendered)
        self.assertNotIn("L0", rendered)
        self.assertNotIn("L1", rendered)
        self.assertIn("2 pass", rendered)
        self.assertNotIn("candidate compact.", rendered)

    def test_performance_and_detail_views_do_not_render_retired_stages(self) -> None:
        state = monitor_metrics.MonitorState(
            last_updated="2026-07-17T21:00:00Z",
            usage_enabled=True,
            usage_report={
                "buckets": [
                    {
                        "request_count": 2,
                        "elapsed_micros_total": 5000,
                        "input_tokens_est": 100,
                        "wire_tokens_est": 20,
                        "delta": {
                            "base_pack_requests": 2,
                            "continuation_requests": 4,
                            "continuation_reuses": 3,
                            "continuation_fallbacks": 1,
                            "continuation_wire_tokens_avoided_est": 80,
                        },
                        "tokens_saved_est": 40,
                        "index": {"refresh_updated": 1},
                        "client_profiles": [
                            {
                                "client_profile": "codex",
                                "request_count": 1,
                                "elapsed_micros_total": 2000,
                                "input_tokens_est": 60,
                                "wire_tokens_est": 10,
                                "tokens_saved_est": 50,
                                "routes": {"explore": 1},
                                "delta": {"base_pack_requests": 1},
                            },
                            {
                                "client_profile": "other",
                                "request_count": 1,
                                "elapsed_micros_total": 3000,
                                "input_tokens_est": 40,
                                "wire_tokens_est": 10,
                                "tokens_saved_est": 30,
                                "routes": {"explore": 1},
                                "delta": {"base_pack_requests": 1},
                            },
                        ],
                    }
                ],
                "rejection_buckets": [
                    {
                        "error_classes": {
                            "schema": 1,
                            "root_policy": 2,
                            "project_selection": 3,
                            "internal": 4,
                        }
                    }
                ],
            },
        )
        performance = monitor_metrics.render_performance_view(
            self.snapshot,
            "http://localhost:8000/mcp",
            color=False,
            width=160,
            state=state,
        )
        detail = monitor_metrics.render_project_detail(
            self.snapshot,
            "http://localhost:8000/mcp",
            color=False,
            width=160,
            state=state,
        )

        self.assertIn("context_pack", performance)
        self.assertIn("21.5", performance)
        self.assertIn("direct retrieval", performance)
        self.assertIn("max cache hit", performance)
        self.assertIn("8 hits / 2 misses (80.0%)", performance)
        self.assertNotIn("L0", performance)
        self.assertNotIn("L1", performance)
        self.assertIn("context_lookup.search", performance)
        self.assertIn("p95 ms", performance)
        self.assertIn("Git diff cache", performance)
        self.assertIn("symbol postings", performance)
        self.assertIn("5.00x source-to-wire", performance)
        self.assertIn("720 source-to-wire", performance)
        self.assertIn(
            "3/4 reused, 1 fallbacks, 80 wire tokens avoided",
            performance,
        )
        self.assertIn("monitor-only detailed usage", performance)
        self.assertIn("2 requests / 2.50 avg ms", performance)
        self.assertIn("delta requests", performance)
        self.assertIn(
            "schema 1 / root policy 2 / project selection 3 / internal 4",
            performance,
        )
        self.assertIn("client profile usage share", performance)
        self.assertIn("codex", performance)
        self.assertIn("other", performance)
        self.assertIn("1 / 50.0%", performance)
        self.assertNotIn("search_fragment_ms", performance)
        self.assertIn("7 entries", detail)
        self.assertIn("max cache hit", detail)
        self.assertIn("8 hits / 2 misses (80.0%)", detail)
        self.assertIn("4 active / 6 total", detail)
        self.assertIn("900 source / 240 cards / 120 returned / 180 wire (720B)", detail)
        self.assertIn("quality.required_anchor_recall", detail)

    def test_max_cache_hit_selects_highest_layer_rate(self) -> None:
        cases = (
            ("file", (8, 2, 0.8)),
            ("git_diff", (9, 1, 0.9)),
            ("lookup", (99, 1, 0.99)),
        )

        for winner, expected in cases:
            with self.subTest(winner=winner):
                metrics = native_metrics()
                metrics["cache"] = {
                    name: {"hits": 1, "misses": 1}
                    for name in ("file", "git_diff", "lookup")
                }
                metrics["cache"][winner] = {
                    "hits": expected[0],
                    "misses": expected[1],
                }

                self.assertEqual(
                    monitor_metrics._combined_cache_stats(metrics), expected
                )

    def test_dashboard_uses_max_cache_hit_across_projects(self) -> None:
        other_metrics = native_metrics()
        other_metrics["cache"] = {
            "file": {"hits": 0, "misses": 100},
            "git_diff": {"hits": 3, "misses": 2},
            "lookup": {"hits": 12, "misses": 4},
        }
        other_snapshot = monitor_metrics.ProjectSnapshot(
            target=monitor_metrics.ProjectTarget(project_id="other", name="other"),
            metrics=other_metrics,
        )
        totals = monitor_metrics._aggregate_totals([self.snapshot, other_snapshot])

        summary = "\n".join(
            monitor_metrics._summary_table(
                totals, [self.snapshot, other_snapshot], [self.snapshot], color=False
            )
        )

        self.assertEqual(totals["cache_hit_ratio"], 0.8)
        self.assertIn("80.0%", summary)

        higher_metrics = native_metrics()
        higher_metrics["cache"]["git_diff"] = {"hits": 9, "misses": 1}
        higher_snapshot = monitor_metrics.ProjectSnapshot(
            target=monitor_metrics.ProjectTarget(project_id="higher", name="higher"),
            metrics=higher_metrics,
        )
        higher_totals = monitor_metrics._aggregate_totals(
            [self.snapshot, other_snapshot, higher_snapshot]
        )
        self.assertEqual(higher_totals["cache_hit_ratio"], 0.9)

    def test_combined_cache_handles_missing_and_zero_counters(self) -> None:
        self.assertEqual(
            monitor_metrics._combined_cache_stats({"cache": {"lookup": None}}),
            (0, 0, 0.0),
        )
        self.assertEqual(
            monitor_metrics._combined_cache_summary({"cache": {}}),
            "0 hits / 0 misses (0.0%)",
        )

    def test_detail_check_descriptions_are_concise(self) -> None:
        matrix = {
            "checks": [
                {
                    "key": "telemetry.context_pack.samples",
                    "current": 4,
                    "target": 1,
                    "operator": ">=",
                    "status": "pass",
                    "description": "Context-pack requests observed by the native telemetry ledger.",
                },
                {
                    "key": "tokens.context_pack.compression_factor_est",
                    "current": 5.0,
                    "target": 1.0,
                    "operator": ">=",
                    "status": "pass",
                    "description": "Selected source tokens divided by returned wire tokens; estimates use characters divided by four.",
                },
                {
                    "key": "tokens.context_pack.saved_est",
                    "current": 720,
                    "target": 0,
                    "operator": ">=",
                    "status": "pass",
                    "description": "Estimated selected-source tokens not sent in the returned pack.",
                },
                {
                    "key": "quality.required_anchor_recall",
                    "current": None,
                    "target": 1.0,
                    "operator": ">=",
                    "status": "insufficient",
                    "description": "Requires a benchmark or differential quality corpus; runtime traffic cannot establish anchor recall.",
                },
                {
                    "key": "quality.noise_ratio",
                    "current": None,
                    "target": 0.3,
                    "operator": "<=",
                    "status": "insufficient",
                    "description": "Requires a benchmark or differential quality corpus; runtime traffic cannot establish retrieval noise.",
                },
            ]
        }
        snapshot = monitor_metrics.ProjectSnapshot(
            target=self.snapshot.target,
            metrics=self.snapshot.metrics,
            matrix=matrix,
        )

        rendered = monitor_metrics.render_project_detail(
            snapshot,
            "http://localhost:8000/mcp",
            color=False,
            width=160,
            state=monitor_metrics.MonitorState(),
        )
        descriptions = [
            row[4] for row in monitor_metrics._measurement_check_rows(matrix, False)
        ]

        for description in (
            "Observed context-pack requests (higher is better)",
            "Estimated source-to-wire factor (higher is better)",
            "Estimated tokens kept off wire (higher is better)",
            "Benchmark required-anchor recall (higher is better)",
            "Benchmark retrieval noise (lower is better)",
        ):
            self.assertIn(description, rendered)
        self.assertTrue(all(len(description) <= 54 for description in descriptions))
        self.assertTrue(all("…" not in description for description in descriptions))
        self.assertNotIn("native telemetry ledger", rendered)
        self.assertNotIn("characters divided by four", rendered)
        self.assertNotIn("runtime traffic cannot establish", rendered)

    def test_unloaded_quality_checks_are_pending_not_false_passes(self) -> None:
        matrix = quality_matrix(status="insufficient", current=None)

        self.assertEqual(
            monitor_metrics._matrix_status(matrix, color=False), "2 pending"
        )


if __name__ == "__main__":
    unittest.main()
