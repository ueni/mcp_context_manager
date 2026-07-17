#!/usr/bin/env python3
"""Black-box smoke test for a running native Streamable HTTP endpoint."""

from __future__ import annotations

import argparse
import json

from smoke_native_mcp import (
    http_json,
    initialize_message,
    request,
    tool_json,
    wait_for_health,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--host-header")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--root-uri", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--reference-id", required=True)
    parser.add_argument("--expected-hash", required=True)
    args = parser.parse_args()

    health = wait_for_health(
        args.port, host=args.host, host_header=args.host_header
    )
    assert health["ok"] is True and health["native"] is True
    status, headers, initialized = http_json(
        args.port,
        initialize_message(),
        host=args.host,
        host_header=args.host_header,
    )
    assert status == 200 and initialized is not None
    assert initialized["result"]["serverInfo"]["name"] == "mcp-context-manager"
    session_id = headers["mcp-session-id"]
    status, _, _ = http_json(
        args.port,
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        session_id=session_id,
        host=args.host,
        host_header=args.host_header,
    )
    assert status == 202

    status, _, projects_response = http_json(
        args.port,
        request(
            2,
            "tools/call",
            {"name": "context_admin", "arguments": {"mode": "projects"}},
        ),
        session_id=session_id,
        host=args.host,
        host_header=args.host_header,
    )
    assert status == 200 and projects_response is not None
    projects = tool_json(projects_response)
    assert any(
        project["project_id"] == args.project_id for project in projects["projects"]
    )

    status, _, pack_response = http_json(
        args.port,
        request(
            3,
            "tools/call",
            {
                "name": "context_pack",
                "arguments": {
                    "prompt": "Verify native cutover state and transport",
                    "focus_paths": ["src/contextd/src/lib.rs"],
                    "root_uri": args.root_uri,
                    "project_id": args.project_id,
                    "cache_strategy": "fresh",
                },
            },
        ),
        session_id=session_id,
        host=args.host,
        host_header=args.host_header,
    )
    assert status == 200 and pack_response is not None
    pack = tool_json(pack_response)
    assert pack["v"] == 2 and pack["paths"] == ["src/contextd/src/lib.rs"]

    status, _, reference_response = http_json(
        args.port,
        request(
            4,
            "tools/call",
            {
                "name": "result_reference_resolve",
                "arguments": {
                    "reference_id": args.reference_id,
                    "expected_hash": args.expected_hash,
                    "project_id": args.project_id,
                },
            },
        ),
        session_id=session_id,
        host=args.host,
        host_header=args.host_header,
    )
    assert status == 200 and reference_response is not None
    resolved = tool_json(reference_response)
    assert resolved["status"] == "resolved"
    print(
        json.dumps(
            {
                "health": "ok",
                "mcp": "ok",
                "project_id": args.project_id,
                "pack_id": pack["id"],
                "imported_reference": args.reference_id,
                "reference_status": resolved["status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
