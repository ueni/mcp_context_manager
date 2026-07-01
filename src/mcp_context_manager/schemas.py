from __future__ import annotations

from typing import Any

CONTEXT_PACK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "summary", "items", "omitted", "metrics"],
    "properties": {
        "schema": {"const": "context_pack.v1"},
        "summary": {"type": "object"},
        "items": {"type": "array"},
        "omitted": {"type": "array"},
        "metrics": {"type": "object"},
    },
}

TOOL_OUTPUT_SCHEMAS: dict[str, dict[str, Any]] = {
    "context_pack": CONTEXT_PACK_SCHEMA,
    "context_lookup": {"type": "object", "required": ["schema"], "properties": {"schema": {"type": "string"}}},
    "context_memory": {"type": "object", "required": ["schema"], "properties": {"schema": {"type": "string"}}},
    "context_admin": {"type": "object", "required": ["schema"], "properties": {"schema": {"type": "string"}}},
    "result_reference_resolve": {"type": "object", "required": ["schema", "status"], "properties": {"schema": {"type": "string"}, "status": {"type": "string"}}},
}


def output_contracts(tool_name: str = "") -> dict[str, Any]:
    if tool_name:
        if tool_name not in TOOL_OUTPUT_SCHEMAS:
            raise ValueError(f"unknown tool contract: {tool_name}")
        return {
            "schema": "tool_output_contract.v1",
            "tool_name": tool_name,
            "outputSchema": TOOL_OUTPUT_SCHEMAS[tool_name],
        }
    return {
        "schema": "tool_output_contracts.v1",
        "contracts": {
            name: {"tool_name": name, "outputSchema": schema}
            for name, schema in sorted(TOOL_OUTPUT_SCHEMAS.items())
        },
    }
