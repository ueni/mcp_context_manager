from __future__ import annotations

import contextlib
from typing import Any

from .config import ContextConfig
from .context import ContextService

try:  # pragma: no cover - optional transport dependency is integration-tested.
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:  # pragma: no cover
    FastMCP = None  # type: ignore[assignment]

try:  # pragma: no cover - optional HTTP dependency is integration-tested.
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, PlainTextResponse
    from starlette.routing import Mount, Route
except ModuleNotFoundError:  # pragma: no cover
    Starlette = None  # type: ignore[assignment]


SERVICE = ContextService.from_env()


def create_mcp(service: ContextService | None = None) -> Any:
    if FastMCP is None:
        raise RuntimeError("mcp[cli] is not installed")
    svc = service or SERVICE
    mcp = FastMCP("mcp-context-manager")

    @mcp.tool()
    def context_pack(
        prompt: str,
        changed_files: list[str] | None = None,
        focus_paths: list[str] | None = None,
        memory_session: str = "default",
        max_output_chars: int | None = None,
        output_profile: str | None = None,
        max_items: int = 8,
        refresh_index: bool = False,
    ) -> dict[str, Any]:
        """Build a compact, cited repository context pack for a coding task."""
        return svc.context_pack(
            prompt=prompt,
            changed_files=changed_files,
            focus_paths=focus_paths,
            memory_session=memory_session,
            max_output_chars=max_output_chars,
            output_profile=output_profile,
            max_items=max_items,
            refresh_index=refresh_index,
        )

    @mcp.tool()
    def context_lookup(
        mode: str = "search",
        query: str = "",
        path: str = ".",
        start_line: int = 1,
        end_line: int | None = None,
        max_results: int = 20,
        max_entries: int = 200,
        max_depth: int = 2,
        include_globs: list[str] | None = None,
    ) -> dict[str, Any]:
        """Search, read snippets, list trees, query symbols, or list references."""
        return svc.context_lookup(
            mode=mode,
            query=query,
            path=path,
            start_line=start_line,
            end_line=end_line,
            max_results=max_results,
            max_entries=max_entries,
            max_depth=max_depth,
            include_globs=include_globs,
        )

    @mcp.tool()
    def context_memory(
        mode: str = "get",
        namespace: str | None = None,
        key: str | None = None,
        value: Any = None,
        ttl_days: int | None = None,
        confidence: float = 1.0,
        source: str = "agent",
        tags: list[str] | None = None,
        focus: str = "",
        summary: str = "",
        topic: str = "",
        decision: Any = None,
        decided_by: str = "llm",
        rationale: str = "",
        include_expired: bool = False,
        max_entries: int = 100,
    ) -> dict[str, Any]:
        """Manage compact repository-local context memory."""
        return svc.context_memory(
            mode=mode,
            namespace=namespace,
            key=key,
            value=value,
            ttl_days=ttl_days,
            confidence=confidence,
            source=source,
            tags=tags,
            focus=focus,
            summary=summary,
            topic=topic,
            decision=decision,
            decided_by=decided_by,
            rationale=rationale,
            include_expired=include_expired,
            max_entries=max_entries,
        )

    @mcp.tool()
    def context_admin(
        mode: str = "health",
        path: str = ".",
        max_files: int = 5000,
        max_age_minutes: int = 1440,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
        tool_name: str = "",
    ) -> dict[str, Any]:
        """Read health, index, cache, budget, and output contract metadata."""
        return svc.context_admin(
            mode=mode,
            path=path,
            max_files=max_files,
            max_age_minutes=max_age_minutes,
            max_output_chars=max_output_chars,
            default_output_profile=default_output_profile,
            tool_name=tool_name,
        )

    @mcp.tool()
    def result_reference_resolve(
        reference_id: str = "",
        reference: dict[str, Any] | None = None,
        expected_hash: str = "",
    ) -> dict[str, Any]:
        """Resolve a local result reference after boundary, expiry, and hash checks."""
        return svc.result_reference_resolve(reference_id, reference, expected_hash)

    @mcp.resource("repo://summary")
    def repo_summary_resource() -> str:
        return svc.repo_summary_resource()

    @mcp.resource("repo://file/{path}")
    def repo_file_resource(path: str) -> str:
        return svc.repo_file_resource(path)

    @mcp.resource("repo://tree/{path}")
    def repo_tree_resource(path: str) -> str:
        return svc.repo_tree_resource(path)

    @mcp.resource("repo://context/{reference_id}")
    def repo_context_resource(reference_id: str) -> str:
        return svc.repo_context_resource(reference_id)

    @mcp.prompt()
    def build_context_pack(task: str) -> str:
        return (
            "Call context_pack with the user's task, then answer using only the "
            "returned cited snippets unless you resolve a returned reference."
            f"\n\nTask: {task}"
        )

    @mcp.prompt()
    def review_with_context_pack(task: str) -> str:
        return (
            "Build a review-focused context pack. Prioritize changed files, tests, "
            "security-sensitive paths, and omitted evidence references."
            f"\n\nReview task: {task}"
        )

    @mcp.prompt()
    def debug_with_context_pack(task: str) -> str:
        return (
            "Build a debug-focused context pack. Prioritize error terms, stack frames, "
            "nearby symbols, tests, and recent memory."
            f"\n\nDebug task: {task}"
        )

    return mcp


def create_http_app(service: ContextService | None = None) -> Any:
    if Starlette is None:
        raise RuntimeError("uvicorn/starlette is not installed")
    svc = service or SERVICE
    mcp = create_mcp(svc)

    async def root(_request: Any) -> PlainTextResponse:
        return PlainTextResponse("mcp-context-manager")

    async def healthz(_request: Any) -> JSONResponse:
        return JSONResponse(svc.context_admin(mode="health"))

    async def context_pack_http(request: Any) -> JSONResponse:
        payload = await request.json()
        return JSONResponse(svc.context_pack(**payload))

    async def reference_http(_request: Any) -> JSONResponse:
        reference_id = _request.path_params["reference_id"]
        return JSONResponse(svc.result_reference_resolve(reference_id=reference_id))

    @contextlib.asynccontextmanager
    async def lifespan(_app: Any):
        async with mcp.session_manager.run():
            yield

    return Starlette(
        routes=[
            Route("/", root, methods=["GET"]),
            Route("/healthz", healthz, methods=["GET"]),
            Route("/v1/context/pack", context_pack_http, methods=["POST"]),
            Route("/v1/context/references/{reference_id}", reference_http, methods=["GET"]),
            Mount("/", app=mcp.streamable_http_app()),
        ],
        lifespan=lifespan,
    )


def main() -> None:
    config = ContextConfig.from_env()
    service = ContextService(config)
    if config.transport in {"stdio", "direct"}:
        create_mcp(service).run()
        return
    if config.transport in {"http", "streamable-http", "streamable_http"}:
        import uvicorn

        uvicorn.run(create_http_app(service), host=config.host, port=config.port)
        return
    raise ValueError("Unsupported MCP_TRANSPORT. Expected stdio or streamable-http.")


if __name__ == "__main__":
    main()
