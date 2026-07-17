"""Regression tests for the monitor's project selection policy."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).parents[1] / "monitor-metrics.py"
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
                ("context_admin", {"mode": "metrics_and_matrix", "project_id": "one"}),
                ("context_admin", {"mode": "metrics_and_matrix", "project_id": "two"}),
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
                ("context_admin", {"mode": "metrics_and_matrix", "project_id": "one"}),
                ("context_admin", {"mode": "metrics_and_matrix", "project_id": "two"}),
            ],
        )


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
        state = monitor_metrics.MonitorState(last_updated="2026-07-17T21:00:00Z")
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
