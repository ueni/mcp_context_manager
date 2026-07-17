#!/usr/bin/env python3
"""Smoke the production Docker image through health and stateful MCP HTTP."""

from __future__ import annotations

import argparse
import http.client
import json
import subprocess
import time
from pathlib import Path

from smoke_native_mcp import PROTOCOL_VERSION, initialize_message, pack_message, request


def image_json(
    host: str,
    method: str,
    path: str,
    *,
    payload: dict[str, object] | None = None,
    session_id: str | None = None,
) -> tuple[int, dict[str, str], dict[str, object] | None]:
    headers = {"Host": "localhost:8000", "Accept": "application/json, text/event-stream"}
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload, separators=(",", ":"))
    if session_id:
        headers["Mcp-Session-Id"] = session_id
        headers["MCP-Protocol-Version"] = PROTOCOL_VERSION
    connection = http.client.HTTPConnection(host, 8000, timeout=5)
    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    raw = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    content_type = response_headers.get("content-type", "")
    if not raw:
        parsed = None
    elif "text/event-stream" in content_type:
        data_lines = [
            line.removeprefix(b"data:").strip()
            for line in raw.splitlines()
            if line.startswith(b"data:") and line.removeprefix(b"data:").strip()
        ]
        parsed = json.loads(data_lines[-1])
    else:
        parsed = json.loads(raw)
    return response.status, response_headers, parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("--name", default="mcp-context-manager-native-smoke")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    args = parser.parse_args()
    subprocess.run(
        ["docker", "rm", "-f", args.name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    container = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            args.name,
            "--tmpfs",
            "/state:rw,uid=1000,gid=1000",
            "-v",
            f"{args.repo.resolve()}:/workspace-roots:ro",
            args.image,
        ],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()
    try:
        host = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container,
            ],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        deadline = time.monotonic() + 15
        while True:
            try:
                status, _, health = image_json(host, "GET", "/healthz")
                if status == 200 and health is not None and health.get("ok") is True:
                    break
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("Docker image health did not become ready")
            time.sleep(0.05)

        status, headers, initialized = image_json(
            host, "POST", "/mcp", payload=initialize_message()
        )
        assert status == 200 and initialized is not None and initialized["id"] == 1
        session_id = headers["mcp-session-id"]
        status, _, _ = image_json(
            host,
            "POST",
            "/mcp",
            payload={"jsonrpc": "2.0", "method": "notifications/initialized"},
            session_id=session_id,
        )
        assert status == 202
        status, _, pack = image_json(
            host,
            "POST",
            "/mcp",
            payload=pack_message(2),
            session_id=session_id,
        )
        assert status == 200 and pack is not None
        content = pack["result"]["content"]
        assert len(content) == 1 and json.loads(content[0]["text"])["v"] == 2
        status, _, tools = image_json(
            host,
            "POST",
            "/mcp",
            payload=request(3, "tools/list"),
            session_id=session_id,
        )
        assert status == 200 and tools is not None and len(tools["result"]["tools"]) == 6
    finally:
        subprocess.run(["docker", "stop", container], check=True, stdout=subprocess.DEVNULL)
    print(json.dumps({"image": args.image, "health": "ok", "mcp": "ok", "shutdown": "ok"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
