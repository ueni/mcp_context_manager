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
        client = monitor_metrics.McpHttpClient("http://localhost:8000/mcp", timeout=90.0)

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
        self.assertIn("detailed=unknown", monitor_metrics._mcp_status_line(state, False))
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
            [("context_admin", {"mode": "monitor_usage", "action": "report", "project_id": "one"})],
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
                return {"tools": [{"name": "context_pack", "description": "Build compact context."}]}

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
        state = monitor_metrics.MonitorState(view="inspection", inspection_payload=payload)
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

    def test_inspection_renders_resource_read_failures_and_is_discoverable(self) -> None:
        class FailingResourceClient:
            def list_tools(self) -> dict[str, object]:
                return {"tools": []}

            def list_resources(self) -> dict[str, object]:
                return {"resources": [{"uri": "repo://broken"}]}

            def read_resource(self, _uri: str) -> dict[str, object]:
                raise RuntimeError("read failed")

        client = FailingResourceClient()
        payload = monitor_metrics.inspect_mcp_surface(client)
        state = monitor_metrics.MonitorState(view="inspection", inspection_payload=payload)
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

    def test_resource_viewer_prettifies_json_scrolls_and_returns_to_catalogue(self) -> None:
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

        self.assertEqual([snapshot.target.project_id for snapshot in snapshots], ["one", "two"])
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

        self.assertEqual([snapshot.target.project_id for snapshot in snapshots], ["one", "two"])
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
            "key": "frontier:fr_demo",
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
            "key": "frontier:fr_demo",
            "found": True,
            "value": {
                "schema": "retrieval_frontier.v1",
                "status": "ready",
                "expires_at": "2099-01-01T00:00:00Z",
                "records": ["x" * 1024] * 100,
            },
        }

        rendered = monitor_metrics.render_state_entry_view(
            "http://localhost:8000/mcp", False, 80, state, height=24
        )

        self.assertIn("retrieval_frontier.v1", rendered)
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


def native_metrics(*, status: str = "idle") -> dict[str, object]:
    return {
        "schema": "context_metrics.v1",
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
                "delta_tokens_saved_est": 120,
                "compression_factor_est": 5.0,
                "compression_ratio_est": 0.2,
            }
        },
        "cache": {
            "hits": 12,
            "misses": 4,
            "hit_ratio": 0.75,
            "l0": {
                "hits": 12,
                "misses": 4,
                "singleflight_hits": 2,
                "entries": 7,
                "weighted_bytes": 65_536,
                "miss_reasons": {
                    "cold_or_invalidated": 1,
                    "request_variant": 3,
                },
                "invalidations": 4,
            },
            "l1": {
                "exact_hits": 8,
                "approximate_hits": 3,
                "retrieval_misses": 2,
            },
        },
        "retrieval": {"backend": "tantivy", "doc_count": 99, "misses": 2},
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

    def test_dashboard_uses_native_l0_l1_counters(self) -> None:
        rendered = monitor_metrics.render_dashboard(
            [self.snapshot], "http://localhost:8000/mcp", color=False, width=160
        )

        self.assertIn("L0 pack cache", rendered)
        self.assertIn("75.0%", rendered)
        self.assertIn("8 exact / 3 approximate", rendered)
        self.assertIn("L1 reuse", rendered)
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
                        "cache_outcomes": {"l0_hit": 3, "l0_miss": 1},
                        "frontier_outcomes": {
                            "exact_hit": 2,
                            "admitted": 1,
                            "capacity_fallback": 1,
                        },
                        "delta": {"base_pack_requests": 2},
                        "delta_tokens_saved_est": 40,
                        "index": {"refresh_updated": 1},
                    }
                ]
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
        self.assertIn("L0 storage", performance)
        self.assertIn("cold 1, variant 3, invalidations 4, coalesced 2", performance)
        self.assertIn("8 exact, 3 approximate hits", performance)
        self.assertIn("context_lookup.search", performance)
        self.assertIn("p95 ms", performance)
        self.assertIn("5.00x source-to-wire", performance)
        self.assertIn("720 source-to-wire, 120 delta", performance)
        self.assertIn("monitor-only detailed usage", performance)
        self.assertIn("2 requests / 2.50 avg ms", performance)
        self.assertIn("3 hits / 1 misses", performance)
        self.assertIn("2 hits / 1 admitted / 1 fallbacks", performance)
        self.assertIn("2 requests / 40 tokens saved", performance)
        self.assertNotIn("search_fragment_ms", performance)
        self.assertIn("7 entries, 64.0KiB", detail)
        self.assertIn("4 active / 6 total", detail)
        self.assertIn("900 source / 240 cards / 120 returned / 180 wire (720B)", detail)
        self.assertIn("quality.required_anchor_recall", detail)

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

        self.assertEqual(monitor_metrics._matrix_status(matrix, color=False), "2 pending")


class MonitorLegacyMetricsRenderingTests(unittest.TestCase):
    def test_legacy_payload_keeps_its_fragment_cache_labels(self) -> None:
        snapshot = monitor_metrics.ProjectSnapshot(
            target=monitor_metrics.ProjectTarget(project_id="legacy"),
            metrics={
                "requests": {"total": 1, "by_operation": {"context_pack": {"count": 1}}},
                "cache": {
                    "context_pack_fragment_hits": 2,
                    "context_pack_fragment_misses": 2,
                },
                "tokens": {"estimated_input_tokens_saved": 20},
            },
            matrix={"checks": []},
        )

        rendered = monitor_metrics.render_dashboard(
            [snapshot], "http://localhost:8000/mcp", color=False, width=160
        )

        self.assertIn("fragment cache", rendered)
        self.assertIn("cand tok", rendered)
        self.assertNotIn("L0 pack cache", rendered)


if __name__ == "__main__":
    unittest.main()
