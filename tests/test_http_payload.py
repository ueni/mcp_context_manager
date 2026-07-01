from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mcp_context_manager.server import (
    _mcp_roots,
    _mcp_tools_http_payload,
    _normalize_context_pack_http_payload,
)


def test_context_pack_http_payload_requires_prompt() -> None:
    with pytest.raises(ValueError, match="prompt is required"):
        _normalize_context_pack_http_payload({})


def test_context_pack_http_payload_accepts_task_alias() -> None:
    payload = _normalize_context_pack_http_payload(
        {"task": "review auth code", "max_items": 2}
    )

    assert payload == {"prompt": "review auth code", "max_items": 2}


def test_context_pack_http_payload_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError, match="unsupported fields: unexpected"):
        _normalize_context_pack_http_payload(
            {"prompt": "review auth code", "unexpected": True}
        )


def test_mcp_tools_http_payload_lists_transport_endpoints() -> None:
    payload = _mcp_tools_http_payload(
        [
            "context_pack",
            "context_lookup",
            "context_memory",
            "context_admin",
            "result_reference_resolve",
        ]
    )

    assert payload == {
        "schema": "context_http.mcp_tools.v1",
        "mcp_endpoint": "/mcp",
        "legacy_sse_endpoint": "/legacy/sse",
        "tool_count": 5,
        "tools": [
            "context_pack",
            "context_lookup",
            "context_memory",
            "context_admin",
            "result_reference_resolve",
        ],
    }


def test_mcp_roots_returns_empty_when_session_has_no_roots_support() -> None:
    class MethodNotFoundSession:
        def list_roots(self) -> object:
            raise RuntimeError("Method not found")

    roots = asyncio.run(_mcp_roots(SimpleNamespace(session=MethodNotFoundSession())))

    assert roots == []


def test_mcp_roots_still_raises_unexpected_errors() -> None:
    class BrokenSession:
        def list_roots(self) -> object:
            raise RuntimeError("connection closed")

    with pytest.raises(RuntimeError, match="connection closed"):
        asyncio.run(_mcp_roots(SimpleNamespace(session=BrokenSession())))
