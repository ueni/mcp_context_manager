from __future__ import annotations

import pytest

from mcp_context_manager.server import _normalize_context_pack_http_payload


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
