#!/usr/bin/env python3
"""Normalize per-project native state-import manifests into cutover evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--phase", required=True)
    args = parser.parse_args()

    projects = []
    for line in args.input.read_text(encoding="utf-8").splitlines():
        project_id, encoded = line.split("\t", 1)
        projects.append({"project_id": project_id, "manifest": json.loads(encoded)})
    projects.sort(key=lambda row: row["project_id"])
    output = {
        "schema": "context_cutover_state_import.v1",
        "phase": args.phase,
        "source_access": "read_only_no_lock",
        "target": "isolated per-project rust-v2 overlays",
        "project_count": len(projects),
        "source_count": sum(row["manifest"]["source_count"] for row in projects),
        "imported_count": sum(row["manifest"]["imported_count"] for row in projects),
        "skipped_expired_count": sum(
            row["manifest"]["skipped_expired_count"] for row in projects
        ),
        "skipped_invalid_count": sum(
            row["manifest"]["skipped_invalid_count"] for row in projects
        ),
        "projects": projects,
    }
    if output["skipped_invalid_count"] != 0:
        raise RuntimeError("final durable-state import skipped invalid records")
    if output["source_count"] != output["imported_count"] + output["skipped_expired_count"]:
        raise RuntimeError("state import counts do not reconcile")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {key: value for key, value in output.items() if key != "projects"},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
