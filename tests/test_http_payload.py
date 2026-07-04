from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from typing import Annotated, get_args, get_origin, get_type_hints

import pytest

from mcp_context_manager import server as server_module
from mcp_context_manager.server import (
    MCP_SERVER_INSTRUCTIONS,
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


def test_create_mcp_advertises_server_instructions(monkeypatch, service) -> None:
    captured: dict[str, object] = {}

    class FakeFastMCP:
        def __init__(self, name: str, **kwargs: object):
            captured["name"] = name
            captured.update(kwargs)

        def tool(self):
            return lambda fn: fn

        def resource(self, *_args: object, **_kwargs: object):
            return lambda fn: fn

        def prompt(self, *_args: object, **_kwargs: object):
            return lambda fn: fn

    monkeypatch.setattr(server_module, "FastMCP", FakeFastMCP)

    server_module.create_mcp(service)

    assert captured["name"] == "mcp-context-manager"
    assert captured["instructions"] == MCP_SERVER_INSTRUCTIONS
    assert "call context_pack first" in MCP_SERVER_INSTRUCTIONS


def test_create_mcp_tool_parameters_have_llm_descriptions(
    monkeypatch, service
) -> None:
    registered_tools: dict[str, object] = {}

    class FakeFastMCP:
        def __init__(self, _name: str, **_kwargs: object):
            pass

        def tool(self):
            def decorator(fn):
                registered_tools[fn.__name__] = fn
                return fn

            return decorator

        def resource(self, *_args: object, **_kwargs: object):
            return lambda fn: fn

        def prompt(self, *_args: object, **_kwargs: object):
            return lambda fn: fn

    monkeypatch.setattr(server_module, "FastMCP", FakeFastMCP)

    server_module.create_mcp(service)

    assert set(registered_tools) == {
        "context_pack",
        "context_lookup",
        "context_memory",
        "context_admin",
        "result_reference_resolve",
    }
    for tool_name, tool in registered_tools.items():
        hints = get_type_hints(tool, include_extras=True)
        for name in inspect.signature(tool).parameters:
            if name == "ctx":
                continue
            description = _annotation_description(hints[name])
            assert description, f"{tool_name}.{name} is missing a description"

    pack_hints = get_type_hints(registered_tools["context_pack"], include_extras=True)
    assert "current coding" in _annotation_description(pack_hints["prompt"])
    lookup_hints = get_type_hints(
        registered_tools["context_lookup"], include_extras=True
    )
    assert "search text" in _annotation_description(lookup_hints["mode"])


def _annotation_description(annotation: object) -> str:
    if get_origin(annotation) is not Annotated:
        for nested in get_args(annotation):
            description = _annotation_description(nested)
            if description:
                return description
        return ""
    for metadata in get_args(annotation)[1:]:
        description = getattr(metadata, "description", "")
        if isinstance(description, str):
            return description
    return ""


def test_mcp_roots_returns_empty_when_session_has_no_roots_support() -> None:
    class MethodNotFoundSession:
        def list_roots(self) -> object:
            raise RuntimeError("Method not found")

    roots = asyncio.run(_mcp_roots(SimpleNamespace(session=MethodNotFoundSession())))

    assert roots == []


def test_mcp_roots_returns_empty_when_roots_list_is_not_supported() -> None:
    class RootsNotSupportedSession:
        def list_roots(self) -> object:
            raise RuntimeError("List roots not supported")

    roots = asyncio.run(_mcp_roots(SimpleNamespace(session=RootsNotSupportedSession())))

    assert roots == []


def test_mcp_roots_returns_empty_when_roots_list_times_out() -> None:
    class HangingSession:
        async def list_roots(self) -> object:
            await asyncio.sleep(60)

    roots = asyncio.run(
        _mcp_roots(SimpleNamespace(session=HangingSession()), timeout_seconds=0.01)
    )

    assert roots == []


def test_mcp_roots_still_raises_unexpected_errors() -> None:
    class BrokenSession:
        def list_roots(self) -> object:
            raise RuntimeError("connection closed")

    with pytest.raises(RuntimeError, match="connection closed"):
        asyncio.run(_mcp_roots(SimpleNamespace(session=BrokenSession())))
