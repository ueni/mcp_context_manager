from __future__ import annotations

import contextlib
import inspect
from typing import Any

from .config import ContextConfig
from .context import ContextService
from .manager import ProjectContextService

try:  # pragma: no cover - optional transport dependency is integration-tested.
    from mcp.server.fastmcp import Context as MCPContext
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:  # pragma: no cover
    MCPContext = Any  # type: ignore[assignment]
    FastMCP = None  # type: ignore[assignment]

try:  # pragma: no cover - optional HTTP dependency is integration-tested.
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, PlainTextResponse
    from starlette.routing import Mount, Route
except ModuleNotFoundError:  # pragma: no cover
    Starlette = None  # type: ignore[assignment]


SERVICE = ProjectContextService.from_env()


async def _mcp_roots(ctx: Any) -> list[Any]:
    if ctx is None:
        return []
    session = getattr(ctx, "session", None)
    if session is None:
        request_context = getattr(ctx, "request_context", None)
        session = getattr(request_context, "session", None)
    if session is None or not hasattr(session, "list_roots"):
        return []
    result = session.list_roots()
    if inspect.isawaitable(result):
        result = await result
    roots = getattr(result, "roots", result)
    if roots is None:
        return []
    return list(roots)


def create_mcp(service: ProjectContextService | ContextService | None = None) -> Any:
    if FastMCP is None:
        raise RuntimeError("mcp[cli] is not installed")
    svc = _project_service(service)
    mcp = FastMCP("mcp-context-manager")

    @mcp.tool()
    async def context_pack(
        ctx: MCPContext,
        prompt: str,
        changed_files: list[str] | None = None,
        focus_paths: list[str] | None = None,
        memory_session: str = "default",
        max_output_chars: int | None = None,
        output_profile: str | None = None,
        max_items: int = 8,
        refresh_index: bool = False,
        project_id: str | None = None,
        root_uri: str | None = None,
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
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=await _mcp_roots(ctx),
        )

    @mcp.tool()
    async def context_lookup(
        ctx: MCPContext,
        mode: str = "search",
        query: str = "",
        path: str = ".",
        start_line: int = 1,
        end_line: int | None = None,
        max_results: int = 20,
        max_entries: int = 200,
        max_depth: int = 2,
        include_globs: list[str] | None = None,
        project_id: str | None = None,
        root_uri: str | None = None,
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
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=await _mcp_roots(ctx),
        )

    @mcp.tool()
    async def context_memory(
        ctx: MCPContext,
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
        project_id: str | None = None,
        root_uri: str | None = None,
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
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=await _mcp_roots(ctx),
        )

    @mcp.tool()
    async def context_admin(
        ctx: MCPContext,
        mode: str = "health",
        path: str = ".",
        max_files: int = 5000,
        max_age_minutes: int = 1440,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
        tool_name: str = "",
        project_id: str | None = None,
        root_uri: str | None = None,
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
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=await _mcp_roots(ctx),
        )

    @mcp.tool()
    async def result_reference_resolve(
        ctx: MCPContext,
        reference_id: str = "",
        reference: dict[str, Any] | None = None,
        expected_hash: str = "",
        project_id: str | None = None,
        root_uri: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a local result reference after boundary, expiry, and hash checks."""
        return svc.result_reference_resolve(
            reference_id=reference_id,
            reference=reference,
            expected_hash=expected_hash,
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=await _mcp_roots(ctx),
        )

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

    @mcp.resource("repo://project/{project_id}/summary")
    def repo_project_summary_resource(project_id: str) -> str:
        return svc.repo_summary_resource(project_id=project_id)

    @mcp.resource("repo://project/{project_id}/file/{path}")
    def repo_project_file_resource(project_id: str, path: str) -> str:
        return svc.repo_file_resource(path, project_id=project_id)

    @mcp.resource("repo://project/{project_id}/tree/{path}")
    def repo_project_tree_resource(project_id: str, path: str) -> str:
        return svc.repo_tree_resource(path, project_id=project_id)

    @mcp.resource("repo://project/{project_id}/context/{reference_id}")
    def repo_project_context_resource(project_id: str, reference_id: str) -> str:
        return svc.repo_context_resource(reference_id, project_id=project_id)

    @mcp.prompt()
    def build_context_pack(task: str = "") -> str:
        prompt = (
            "Call context_pack with the user's task, then answer using only the "
            "returned cited snippets unless you resolve a returned reference."
        )
        return f"{prompt}\n\nTask: {task}" if task else prompt

    @mcp.prompt()
    def review_with_context_pack(task: str = "") -> str:
        prompt = (
            "Build a review-focused context pack. Prioritize changed files, tests, "
            "security-sensitive paths, and omitted evidence references."
        )
        return f"{prompt}\n\nReview task: {task}" if task else prompt

    @mcp.prompt()
    def debug_with_context_pack(task: str = "") -> str:
        prompt = (
            "Build a debug-focused context pack. Prioritize error terms, stack frames, "
            "nearby symbols, tests, and recent memory."
        )
        return f"{prompt}\n\nDebug task: {task}" if task else prompt

    return mcp


def create_http_app(service: ProjectContextService | ContextService | None = None) -> Any:
    if Starlette is None:
        raise RuntimeError("uvicorn/starlette is not installed")
    svc = _project_service(service)
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
        return JSONResponse(
            svc.result_reference_resolve(
                reference_id=reference_id,
                project_id=_request.query_params.get("project_id"),
                root_uri=_request.query_params.get("root_uri"),
            )
        )

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


def _project_service(
    service: ProjectContextService | ContextService | None,
) -> ProjectContextService:
    if service is None:
        return SERVICE
    if isinstance(service, ProjectContextService):
        return service
    return ProjectContextService(service.config)


def main() -> None:
    config = ContextConfig.from_env()
    service = ProjectContextService(config)
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
