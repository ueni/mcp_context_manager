from __future__ import annotations

import json
from typing import Any

from .util import estimate_tokens

CONTEXT_PACK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "summary", "items"],
    "properties": {
        "schema": {
            "enum": [
                "context_pack.v1",
                "context_pack.minimal.v1",
                "context_pack.compact.v2",
                "context_pack.normal.v2",
                "context_pack.verbose.v2",
            ]
        },
        "summary": {"type": ["object", "string"]},
        "items": {"type": "array"},
        "omitted": {"type": "array"},
        "omitted_ref": {"type": "string"},
        "diagnostics_ref": {"type": "string"},
        "metrics": {"type": "object"},
        "skill_guidance": {"type": "object"},
    },
}

CONTEXT_METRICS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "requests", "cache", "retrieval", "tokens", "benchmarks"],
    "properties": {
        "schema": {"const": "context_metrics.v1"},
        "requests": {"type": "object"},
        "cache": {"type": "object"},
        "retrieval": {"type": "object"},
        "tokens": {"type": "object"},
        "tooling": {"type": "object"},
        "benchmarks": {"type": "object"},
    },
}

MEASUREMENT_MATRIX_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "checks", "metric_sources"],
    "properties": {
        "schema": {"const": "context_measurement_matrix.v1"},
        "checks": {"type": "array"},
        "metric_sources": {"type": "object"},
    },
}

CONTEXT_METRICS_AND_MATRIX_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "metrics", "matrix"],
    "properties": {
        "schema": {"const": "context_metrics_and_matrix.v1"},
        "metrics": {"type": "object"},
        "matrix": {"type": "object"},
    },
}

CONTEXT_BENCHMARK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "runs", "measurement_matrix"],
    "properties": {
        "schema": {"const": "context_benchmark.v1"},
        "runs": {"type": "array"},
        "measurement_matrix": {"type": "object"},
    },
}

CONTEXT_CACHE_WARMUP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["schema", "index", "search_cache", "cache", "omitted"],
    "properties": {
        "schema": {"const": "context_cache.warmup.v1"},
        "index": {"type": "object"},
        "search_cache": {"type": "object"},
        "cache": {"type": "object"},
        "omitted": {"type": "array"},
    },
}

TOOL_OUTPUT_SCHEMAS: dict[str, dict[str, Any]] = {
    "context_pack": CONTEXT_PACK_SCHEMA,
    "context_lookup": {"type": "object", "required": ["schema"], "properties": {"schema": {"type": "string"}}},
    "context_memory": {"type": "object", "required": ["schema"], "properties": {"schema": {"type": "string"}}},
    "context_admin": {
        "oneOf": [
            CONTEXT_METRICS_SCHEMA,
            CONTEXT_METRICS_AND_MATRIX_SCHEMA,
            MEASUREMENT_MATRIX_SCHEMA,
            CONTEXT_BENCHMARK_SCHEMA,
            CONTEXT_CACHE_WARMUP_SCHEMA,
            {
                "type": "object",
                "required": ["schema"],
                "properties": {"schema": {"type": "string"}},
            },
        ]
    },
    "result_reference_resolve": {"type": "object", "required": ["schema", "status"], "properties": {"schema": {"type": "string"}, "status": {"type": "string"}}},
}

TOOL_DESCRIPTIONS: dict[str, str] = {
    "context_pack": "Build compact cited repo context.",
    "context_lookup": "Search, snippet, tree, symbols, or refs.",
    "context_memory": "Read and manage compact repo memory.",
    "context_admin": "Inspect health, index, cache, contracts, metrics.",
    "result_reference_resolve": "Resolve a stored local evidence reference.",
}

TOOL_INPUT_PARAMS: dict[str, dict[str, str]] = {
    "context_pack": {
        "prompt": "Task text.",
        "changed_files": "Changed repo paths.",
        "focus_paths": "Paths to prioritize.",
        "memory_session": "Memory session key.",
        "max_output_chars": "Hard output budget.",
        "output_profile": "minimal, compact, normal, verbose.",
        "max_items": "Max context items.",
        "refresh_index": "Force index refresh.",
        "client_profile": "codex, claude, copilot, generic.",
        "model_profile": "openai, anthropic, github, unknown.",
        "evidence_policy": "summary_first, snippet_first, reference_first.",
        "diagnostics": "none, summary, full.",
        "include_request_prompt": "Echo raw prompt.",
        "include_runtime_metadata": "Inline volatile runtime metadata.",
        "max_source_tokens": "Source token budget.",
        "max_diagnostic_tokens": "Diagnostic token budget.",
        "cache_strategy": "stable, fresh, cold.",
        "project_id": "Project selector.",
        "root_uri": "Repo file URI.",
    },
    "context_lookup": {
        "mode": "search, snippet, tree, symbols, references, impact, related_symbols, test_owners, chunk, explain_cache.",
        "query": "Search or symbol terms.",
        "path": "Repo-relative path.",
        "start_line": "Snippet start line.",
        "end_line": "Snippet end line.",
        "max_results": "Max result rows.",
        "max_entries": "Max tree or memory rows.",
        "max_depth": "Tree depth.",
        "include_globs": "Result glob filters.",
        "project_id": "Project selector.",
        "root_uri": "Repo file URI.",
    },
    "context_memory": {
        "mode": "get, upsert, summary, decision, validate, compact.",
        "namespace": "Memory namespace.",
        "key": "Entry key.",
        "value": "Structured value.",
        "ttl_days": "Optional TTL.",
        "confidence": "0.0 to 1.0.",
        "source": "Provenance label.",
        "tags": "Search tags.",
        "focus": "Summary focus.",
        "summary": "Compact summary text.",
        "topic": "Decision topic.",
        "decision": "Decision payload.",
        "decided_by": "human or llm.",
        "rationale": "Decision rationale.",
        "include_expired": "Include expired rows.",
        "max_entries": "Max rows.",
        "project_id": "Project selector.",
        "root_uri": "Repo file URI.",
    },
    "context_admin": {
        "mode": "health, projects, index, cache, warmup, budget, contracts, metrics, state_browser, quality_eval, cache_plan, profile_calibrate, instructions, resource_proxy, schema_minify.",
        "path": "Repo-relative path.",
        "max_files": "Index file cap.",
        "max_entries": "Max rows.",
        "max_age_minutes": "Cache prune age, default 30 days.",
        "max_output_chars": "Budget override.",
        "default_output_profile": "Budget output profile.",
        "tool_name": "Filter to one tool.",
        "contract_profile": "compact or verbose contracts.",
        "state_prefix": "Generated-state key prefix for state_browser.",
        "state_key": "Exact generated-state key for state_browser entry view.",
        "project_id": "Project selector.",
        "root_uri": "Repo file URI.",
    },
    "result_reference_resolve": {
        "reference_id": "Reference id.",
        "reference": "Full reference object.",
        "expected_hash": "Expected sha256.",
        "project_id": "Project selector.",
        "root_uri": "Repo file URI.",
    },
}

TOOL_OUTPUT_SCHEMA_NAMES: dict[str, list[str]] = {
    "context_pack": [
        "context_pack.v1",
        "context_pack.minimal.v1",
        "context_pack.compact.v2",
        "context_pack.normal.v2",
        "context_pack.verbose.v2",
    ],
    "context_lookup": [
        "context_search.v1",
        "context_snippet.v1",
        "context_tree.v1",
        "context_symbols.v1",
        "context_references.list.v1",
        "context_lookup.impact.v1",
        "context_lookup.related_symbols.v1",
        "context_lookup.test_owners.v1",
        "context_lookup.chunk.v1",
        "context_lookup.explain_cache.v1",
    ],
    "context_memory": [
        "context_memory.get.v1",
        "context_memory.upsert.v1",
        "context_memory.summary_upsert.v1",
        "context_memory.decision_record.v1",
        "context_memory.validate.v1",
        "context_memory.compact.v1",
    ],
    "context_admin": [
        "context_admin.health.v1",
        "context_index.refresh.v1",
        "context_index.status.v1",
        "context_cache.stats.v1",
        "context_cache.prune.v1",
        "context_cache.warmup.v1",
        "context_budget.v1",
        "tool_output_contracts.v1",
        "context_metrics.v1",
        "context_metrics_and_matrix.v1",
        "context_measurement_matrix.v1",
        "context_benchmark.v1",
        "context_state_browser.v1",
        "context_quality_eval.v1",
        "context_budget_plan.v1",
        "context_profile_calibration.v1",
        "context_resource_proxy.v1",
    ],
    "result_reference_resolve": ["mcp_result_reference.resolve.v1"],
}


def output_contracts(tool_name: str = "", profile: str = "verbose") -> dict[str, Any]:
    if profile not in {"verbose", "compact"}:
        raise ValueError("contract_profile must be compact or verbose")
    if tool_name:
        if tool_name not in TOOL_OUTPUT_SCHEMAS:
            raise ValueError(f"unknown tool contract: {tool_name}")
        payload = _tool_contract(tool_name, profile=profile)
        payload["metrics"] = contract_size_metrics(tool_name=tool_name)
        return payload
    payload = {
        "schema": "tool_output_contracts.compact.v1"
        if profile == "compact"
        else "tool_output_contracts.v1",
        "contract_version": 1,
        "profile": profile,
        "stability": "stable",
        "contracts": {
            name: _tool_contract_body(name, profile=profile)
            for name in sorted(TOOL_OUTPUT_SCHEMAS)
        },
    }
    payload["metrics"] = contract_size_metrics()
    return payload


def contract_size_metrics(tool_name: str = "") -> dict[str, Any]:
    verbose = _contracts_payload(tool_name=tool_name, profile="verbose")
    compact = _contracts_payload(tool_name=tool_name, profile="compact")
    verbose_chars = _payload_chars(verbose)
    compact_chars = _payload_chars(compact)
    verbose_tokens = estimate_tokens(verbose)
    compact_tokens = estimate_tokens(compact)
    return {
        "schema": "tool_contract_metrics.v1",
        "tool_name": tool_name,
        "tokenizer": "offline_estimator",
        "tokenizer_available": True,
        "token_count_source": "estimate",
        "contract_chars": verbose_chars,
        "contract_tokens_est": verbose_tokens,
        "compact_contract_chars": compact_chars,
        "compact_contract_tokens_est": compact_tokens,
        "compact_contract_tokens_saved_est": max(0, verbose_tokens - compact_tokens),
    }


def _contracts_payload(tool_name: str, profile: str) -> dict[str, Any]:
    if tool_name:
        return _tool_contract(tool_name, profile=profile, include_metrics=False)
    return {
        "schema": "tool_output_contracts.compact.v1"
        if profile == "compact"
        else "tool_output_contracts.v1",
        "contract_version": 1,
        "profile": profile,
        "stability": "stable",
        "contracts": {
            name: _tool_contract_body(name, profile=profile)
            for name in sorted(TOOL_OUTPUT_SCHEMAS)
        },
    }


def _tool_contract(
    tool_name: str, profile: str, include_metrics: bool = True
) -> dict[str, Any]:
    payload = {
        "schema": "tool_output_contract.compact.v1"
        if profile == "compact"
        else "tool_output_contract.v1",
        "contract_version": 1,
        "profile": profile,
        **_tool_contract_body(tool_name, profile=profile),
    }
    if include_metrics:
        payload["metrics"] = contract_size_metrics(tool_name=tool_name)
    return payload


def _tool_contract_body(tool_name: str, profile: str) -> dict[str, Any]:
    if profile == "compact":
        return {
            "tool_name": tool_name,
            "stability": "stable",
            "description": TOOL_DESCRIPTIONS[tool_name],
            "parameters": TOOL_INPUT_PARAMS[tool_name],
            "output_schema_names": TOOL_OUTPUT_SCHEMA_NAMES[tool_name],
        }
    return {
        "tool_name": tool_name,
        "stability": "stable",
        "description": TOOL_DESCRIPTIONS[tool_name],
        "parameters": {
            name: {
                "description": description,
                "stability": "stable",
            }
            for name, description in TOOL_INPUT_PARAMS[tool_name].items()
        },
        "output_schema_names": TOOL_OUTPUT_SCHEMA_NAMES[tool_name],
        "outputSchema": TOOL_OUTPUT_SCHEMAS[tool_name],
    }


def _payload_chars(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, sort_keys=True, separators=(",", ":")))
