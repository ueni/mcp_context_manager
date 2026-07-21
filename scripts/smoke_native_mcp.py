#!/usr/bin/env python3
"""Black-box native MCP initialization, lookup, and pack smoke test."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import socket
import subprocess
import time
from urllib.parse import urlsplit
from pathlib import Path
from typing import Any

PROTOCOL_VERSION = "2025-03-26"
COLD_MCP_RESPONSE_TIMEOUT_SECONDS = float(
    os.environ.get("MCP_SMOKE_COLD_RESPONSE_TIMEOUT_SECONDS", "30")
)


def request(request_id: int, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
    }
    if params is not None:
        message["params"] = params
    return message


def initialize_message() -> dict[str, Any]:
    return request(
        1,
        "initialize",
        {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "native-milestone-zero", "version": "1.0"},
        },
    )


def write_stdio(process: subprocess.Popen[str], message: dict[str, Any]) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()


def read_stdio(process: subprocess.Popen[str]) -> dict[str, Any]:
    assert process.stdout is not None
    line = process.stdout.readline()
    if not line:
        stderr = process.stderr.read() if process.stderr is not None else ""
        raise RuntimeError(f"native stdio server closed unexpectedly: {stderr}")
    return json.loads(line)


def tool_json(response: dict[str, Any]) -> dict[str, Any]:
    content = response["result"]["content"]
    assert len(content) == 1 and content[0]["type"] == "text"
    return json.loads(content[0]["text"])


def assert_native_tools(tools: dict[str, Any]) -> None:
    names = {tool["name"] for tool in tools["result"]["tools"]}
    assert names == {
        "context_lookup",
        "context_admin",
        "context_memory",
        "context_pack",
        "health",
        "result_reference_resolve",
    }


def lookup_message(request_id: int) -> dict[str, Any]:
    return request(
        request_id,
        "tools/call",
        {
            "name": "context_lookup",
            "arguments": {
                "mode": "search",
                "query": "ProjectEngine context_pack_cached retrieval",
                "path": "src/context-core/src/lib.rs",
                "max_results": 4,
            },
        },
    )


def pack_message(request_id: int) -> dict[str, Any]:
    return request(
        request_id,
        "tools/call",
        {
            "name": "context_pack",
            "arguments": {
                "prompt": "Debug native context_pack retrieval latency",
                "focus_paths": ["src/context-core/src/lib.rs"],
                "client_profile": "codex",
            },
        },
    )


def assert_lookup(response: dict[str, Any]) -> None:
    lookup = tool_json(response)
    assert lookup["schema"] == "context_search.v1"
    assert lookup["count"] > 0
    assert any(
        result["path"] == "src/context-core/src/lib.rs"
        for result in lookup["results"]
    )


def assert_pack(response: dict[str, Any]) -> None:
    pack = tool_json(response)
    assert pack["v"] == 2
    assert pack["id"].startswith("pk_")
    assert pack["paths"] == ["src/context-core/src/lib.rs"]
    assert len(pack["evidence"]) == 1
    assert pack["more"].startswith("ctxref-")


def assert_resources(resources: dict[str, Any]) -> None:
    uris = {resource["uri"] for resource in resources["result"]["resources"]}
    assert "repo://summary" in uris
    assert "repo://instructions/context-pack" in uris


def assert_summary_resource(response: dict[str, Any]) -> None:
    contents = response["result"]["contents"]
    assert len(contents) == 1
    summary = json.loads(contents[0]["text"])
    assert summary["schema"] == "workspace_facts.v1"
    assert summary["file_count"] > 0


def smoke_stdio(binary: Path) -> None:
    environment = os.environ.copy()
    environment["MCP_CONTEXT_STATE_DIR"] = "/tmp/mcp-context-native-smoke-stdio"
    process = subprocess.Popen(
        [str(binary), "--transport", "stdio"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        text=True,
    )
    try:
        write_stdio(process, initialize_message())
        initialized = read_stdio(process)
        assert initialized["id"] == 1
        assert initialized["result"]["serverInfo"]["name"] == "mcp-context-manager"

        write_stdio(process, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        write_stdio(process, request(2, "tools/list"))
        tools = read_stdio(process)
        assert tools["id"] == 2
        assert_native_tools(tools)

        write_stdio(process, lookup_message(3))
        assert_lookup(read_stdio(process))
        write_stdio(process, pack_message(4))
        assert_pack(read_stdio(process))
        write_stdio(process, request(5, "resources/list"))
        assert_resources(read_stdio(process))
        write_stdio(
            process,
            request(6, "resources/read", {"uri": "repo://summary"}),
        )
        assert_summary_resource(read_stdio(process))
    finally:
        process.terminate()
        process.wait(timeout=5)


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def http_json(
    port: int,
    message: dict[str, Any],
    *,
    session_id: str | None = None,
    host: str = "127.0.0.1",
    host_header: str | None = None,
    accept: str = "application/json, text/event-stream",
    content_type: str = "application/json",
) -> tuple[int, dict[str, str], dict[str, Any] | None]:
    headers = {
        "Accept": accept,
        "Content-Type": content_type,
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
        headers["MCP-Protocol-Version"] = PROTOCOL_VERSION
    if host_header is not None:
        headers["Host"] = host_header
    connection = http.client.HTTPConnection(
        host, port, timeout=COLD_MCP_RESPONSE_TIMEOUT_SECONDS
    )
    connection.request(
        "POST",
        "/mcp",
        body=json.dumps(message, separators=(",", ":")),
        headers=headers,
    )
    response = connection.getresponse()
    body = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    content_type = response_headers.get("content-type", "")
    if not body:
        parsed = None
    elif "text/event-stream" in content_type:
        data_lines = [
            line.removeprefix(b"data:").strip()
            for line in body.splitlines()
            if line.startswith(b"data:") and line.removeprefix(b"data:").strip()
        ]
        if not data_lines:
            raise RuntimeError(f"HTTP MCP returned empty SSE data: {body!r}")
        parsed = json.loads(data_lines[-1])
    else:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"HTTP MCP returned non-JSON: status={response.status} "
                f"headers={response_headers!r} body={body!r}"
            ) from error
    return response.status, response_headers, parsed


def mcp_delete(port: int, session_id: str) -> int:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(
        "DELETE",
        "/mcp",
        headers={
            "Mcp-Session-Id": session_id,
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        },
    )
    response = connection.getresponse()
    response.read()
    connection.close()
    return response.status


def raw_rest(
    port: int,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(method, path, headers=headers or {})
    response = connection.getresponse()
    body = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    return response.status, response_headers, body


def rest_json(
    port: int,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    host: str = "127.0.0.1",
    host_header: str | None = None,
) -> tuple[int, dict[str, Any]]:
    request_headers = dict(headers or {})
    body = None
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":"))
        request_headers["Content-Type"] = "application/json"
    if host_header is not None:
        request_headers["Host"] = host_header
    connection = http.client.HTTPConnection(host, port, timeout=5)
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    response_body = json.loads(response.read())
    connection.close()
    return response.status, response_body


def read_sse_event(response: http.client.HTTPResponse) -> tuple[str, str]:
    event = "message"
    data: list[str] = []
    while True:
        line = response.readline()
        if not line:
            raise RuntimeError("legacy SSE stream closed before an event arrived")
        text = line.decode("utf-8").rstrip("\r\n")
        if not text:
            if data:
                return event, "\n".join(data)
            event = "message"
            continue
        if text.startswith(":"):
            continue
        field, separator, value = text.partition(":")
        if not separator:
            continue
        value = value.removeprefix(" ")
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)


def legacy_sse_post(port: int, path: str, message: dict[str, Any]) -> int:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(
        "POST",
        path,
        body=json.dumps(message, separators=(",", ":")),
        headers={"Content-Type": "application/json"},
    )
    response = connection.getresponse()
    response.read()
    connection.close()
    return response.status


def smoke_legacy_sse(
    port: int,
    expected_public_base: str,
    *,
    host_header: str | None = None,
) -> None:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        headers = {"Accept": "text/event-stream"}
        if host_header is not None:
            headers["Host"] = host_header
        connection.request("GET", "/legacy/sse", headers=headers)
        response = connection.getresponse()
        assert response.status == 200
        assert "text/event-stream" in response.getheader("Content-Type", "")
        event, endpoint = read_sse_event(response)
        assert event == "endpoint"
        parsed_endpoint = urlsplit(endpoint)
        assert f"{parsed_endpoint.scheme}://{parsed_endpoint.netloc}" == expected_public_base
        assert parsed_endpoint.path == "/legacy/messages"
        assert parsed_endpoint.query.startswith("session_id=")
        message_path = parsed_endpoint.path
        if parsed_endpoint.query:
            message_path = f"{message_path}?{parsed_endpoint.query}"

        assert legacy_sse_post(port, message_path, initialize_message()) == 202
        event, initialized = read_sse_event(response)
        assert event == "message"
        initialized_message = json.loads(initialized)
        assert initialized_message["id"] == 1
        assert initialized_message["result"]["serverInfo"]["name"] == "mcp-context-manager"

        assert (
            legacy_sse_post(
                port,
                message_path,
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
            )
            == 202
        )
        assert legacy_sse_post(port, message_path, request(2, "tools/list")) == 202
        event, tools = read_sse_event(response)
        assert event == "message"
        assert_native_tools(json.loads(tools))
    finally:
        connection.close()


def wait_for_health(
    port: int,
    *,
    headers: dict[str, str] | None = None,
    host: str = "127.0.0.1",
    host_header: str | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status, body = rest_json(
                port,
                "GET",
                "/healthz",
                headers=headers,
                host=host,
                host_header=host_header,
            )
            if status == 200:
                return body
        except (OSError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
        time.sleep(0.05)
    raise RuntimeError(f"native HTTP server did not become healthy: {last_error}")


def smoke_http(binary: Path) -> None:
    port = unused_port()
    environment = os.environ.copy()
    environment.pop("MCP_HTTP_BEARER_TOKEN", None)
    environment.pop("MCP_HTTP_AUTHORIZATION_SERVERS", None)
    environment.update(
        {
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "MCP_CONTEXT_STATE_DIR": "/tmp/mcp-context-native-smoke-http",
            "MCP_HTTP_PUBLIC_BASE_URL": "https://context.example",
        }
    )
    process = subprocess.Popen(
        [str(binary), "--transport", "streamable-http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        text=True,
    )
    try:
        health = wait_for_health(port)
        assert health["status"] == "ok"
        assert health["native"] is True

        status, headers, initialized = http_json(port, initialize_message())
        assert status == 200
        assert initialized is not None and initialized["id"] == 1
        session_id = headers["mcp-session-id"]

        status, _, body = http_json(
            port,
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            session_id=session_id,
        )
        assert status == 202 and body is None

        status, _, tools = http_json(
            port,
            request(2, "tools/list"),
            session_id=session_id,
        )
        assert status == 200
        assert tools is not None and tools["id"] == 2
        assert_native_tools(tools)

        status, _, lookup = http_json(port, lookup_message(3), session_id=session_id)
        assert status == 200 and lookup is not None
        assert_lookup(lookup)
        status, _, pack = http_json(port, pack_message(4), session_id=session_id)
        assert status == 200 and pack is not None
        assert_pack(pack)
        mcp_pack = tool_json(pack)
        status, _, resources = http_json(
            port, request(5, "resources/list"), session_id=session_id
        )
        assert status == 200 and resources is not None
        assert_resources(resources)
        status, _, summary = http_json(
            port,
            request(6, "resources/read", {"uri": "repo://summary"}),
            session_id=session_id,
        )
        assert status == 200 and summary is not None
        assert_summary_resource(summary)

        status, tools_payload = rest_json(port, "GET", "/v1/mcp/tools")
        assert status == 200
        assert tools_payload["schema"] == "context_http.mcp_tools.v1"
        assert tools_payload["tool_count"] == 6
        assert "health" in tools_payload["tools"]
        assert tools_payload["legacy_sse_endpoint"] == "/legacy/sse"
        smoke_legacy_sse(port, "https://context.example")
        rest_arguments = pack_message(7)["params"]["arguments"]
        status, rest_pack = rest_json(
            port,
            "POST",
            "/v1/context/pack",
            payload=rest_arguments,
        )
        assert status == 200 and rest_pack == mcp_pack
        status, resolved = rest_json(
            port,
            "GET",
            f"/v1/context/references/{rest_pack['more']}",
        )
        assert status == 200 and resolved["status"] == "resolved"
        status, rejected = rest_json(
            port,
            "POST",
            "/v1/context/pack",
            payload={"prompt": "valid", "unexpected": True},
        )
        assert status == 400 and rejected["error"] == "bad_request"
        status, _ = rest_json(
            port,
            "GET",
            "/healthz",
            headers={"Host": "attacker.example"},
        )
        assert status == 403
        status, _ = rest_json(
            port,
            "GET",
            "/healthz",
            headers={"Origin": "https://attacker.example"},
        )
        assert status == 403
        status, _ = rest_json(
            port,
            "GET",
            "/healthz",
            headers={"Origin": f"http://127.0.0.1:{port}"},
        )
        assert status == 200

        status, _, case_initialized = http_json(
            port,
            initialize_message(),
            accept="Application/JSON ; q=1, Text/Event-Stream; q=0.5",
            content_type="Application/JSON ; charset=utf-8",
        )
        assert status == 200 and case_initialized is not None
        status, _, rejected = http_json(
            port,
            initialize_message(),
            accept="application/json;q=0, text/event-stream",
        )
        assert status == 406 and rejected is not None

        assert mcp_delete(port, "nonexistent-session") == 404
        assert mcp_delete(port, session_id) == 202
        assert mcp_delete(port, session_id) == 404
        status, _, _ = raw_rest(port, "GET", "/mcp/unexpected")
        assert status == 404
    finally:
        process.terminate()
        process.wait(timeout=5)


def smoke_legacy_sse_default_host(binary: Path) -> None:
    port = unused_port()
    environment = os.environ.copy()
    environment.pop("MCP_HTTP_BEARER_TOKEN", None)
    environment.pop("MCP_HTTP_PUBLIC_BASE_URL", None)
    environment.pop("MCP_HTTP_AUTHORIZATION_SERVERS", None)
    environment.update(
        {
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "MCP_CONTEXT_STATE_DIR": "/tmp/mcp-context-native-smoke-legacy-host",
            "MCP_HTTP_ALLOWED_HOSTS": (
                f"127.0.0.1,127.0.0.1:{port},mcp-context-manager:8000"
            ),
        }
    )
    process = subprocess.Popen(
        [str(binary), "--transport", "streamable-http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        text=True,
    )
    try:
        wait_for_health(port)
        smoke_legacy_sse(
            port,
            "http://mcp-context-manager:8000",
            host_header="mcp-context-manager:8000",
        )
    finally:
        process.terminate()
        process.wait(timeout=5)


def smoke_http_security(binary: Path) -> None:
    port = unused_port()
    token = "native-smoke-bearer-token"
    environment = os.environ.copy()
    environment.update(
        {
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "MCP_CONTEXT_STATE_DIR": "/tmp/mcp-context-native-smoke-auth",
            "MCP_HTTP_BEARER_TOKEN": token,
            "MCP_HTTP_PUBLIC_BASE_URL": f"http://LOCALHOST:{port}",
            "MCP_HTTP_AUTHORIZATION_SERVERS": "https://auth.example.com",
        }
    )
    process = subprocess.Popen(
        [str(binary), "--transport", "streamable-http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        text=True,
    )
    authorization = {"Authorization": f"Bearer {token}"}
    try:
        health = wait_for_health(port, headers=authorization)
        assert health["ok"] is True
        status, response_headers, body = raw_rest(port, "GET", "/healthz")
        assert status == 401
        challenge = response_headers["www-authenticate"]
        metadata_url = (
            f"http://LOCALHOST:{port}/.well-known/oauth-protected-resource/mcp"
        )
        assert challenge == f'Bearer resource_metadata="{metadata_url}"'
        assert json.loads(body)["error"] == "unauthorized"
        status, metadata = rest_json(
            port, "GET", "/.well-known/oauth-protected-resource/mcp"
        )
        assert status == 200
        assert metadata == {
            "resource": f"http://LOCALHOST:{port}/mcp",
            "authorization_servers": ["https://auth.example.com"],
            "bearer_methods_supported": ["header"],
        }
        status, root_metadata = rest_json(
            port, "GET", "/.well-known/oauth-protected-resource"
        )
        assert status == 200 and root_metadata == metadata
        status, _ = rest_json(
            port,
            "GET",
            "/.well-known/oauth-protected-resource/mcp",
            headers={"Host": "attacker.example"},
        )
        assert status == 403
        status, _ = rest_json(
            port,
            "GET",
            "/.well-known/oauth-protected-resource/mcp",
            headers={"Origin": "https://attacker.example"},
        )
        assert status == 403
        status, health = rest_json(
            port, "GET", "/healthz", headers=authorization
        )
        assert status == 200 and health["native"] is True
        status, _ = rest_json(
            port,
            "GET",
            "/healthz",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert status == 401
    finally:
        process.terminate()
        process.wait(timeout=5)


def smoke_http_auth_configuration(binary: Path) -> None:
    port = unused_port()
    environment = os.environ.copy()
    environment.pop("MCP_HTTP_BEARER_TOKEN", None)
    environment.pop("MCP_HTTP_PUBLIC_BASE_URL", None)
    environment.pop("MCP_HTTP_AUTHORIZATION_SERVERS", None)
    environment.update(
        {
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "MCP_HTTP_BEARER_TOKEN": "incomplete-auth-config",
        }
    )
    process = subprocess.run(
        [str(binary), "--transport", "streamable-http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        text=True,
        timeout=5,
        check=False,
    )
    assert process.returncode != 0
    assert "MCP_HTTP_PUBLIC_BASE_URL is required" in process.stderr


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"binary does not exist: {binary}")

    smoke_stdio(binary)
    smoke_http(binary)
    smoke_legacy_sse_default_host(binary)
    smoke_http_security(binary)
    smoke_http_auth_configuration(binary)
    print(
        json.dumps(
            {
                "binary": str(binary),
                "stdio": "ok",
                "http": "ok",
                "legacy_sse": "ok",
                "security": "ok",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
