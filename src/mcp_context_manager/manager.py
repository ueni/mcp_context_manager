from __future__ import annotations

import json
import re
from typing import Any

from .config import ContextConfig
from .context import ContextService
from .projects import ProjectRegistry, ProjectRoot


class ProjectContextService:
    def __init__(
        self,
        config: ContextConfig,
        registry: ProjectRegistry | None = None,
    ):
        self.config = config
        self.registry = registry or ProjectRegistry(config)
        self._services: dict[str, ContextService] = {}

    @classmethod
    def from_env(cls) -> "ProjectContextService":
        config = ContextConfig.from_env()
        return cls(config)

    def context_pack(
        self,
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
        mcp_roots: list[Any] | None = None,
    ) -> dict[str, Any]:
        service, project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=[*(changed_files or []), *(focus_paths or [])],
        )
        result = service.context_pack(
            prompt=prompt,
            changed_files=changed_files,
            focus_paths=focus_paths,
            memory_session=memory_session,
            max_output_chars=max_output_chars,
            output_profile=output_profile,
            max_items=max_items,
            refresh_index=refresh_index,
        )
        result["project"] = project.public_metadata()
        return result

    def context_lookup(
        self,
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
        mcp_roots: list[Any] | None = None,
    ) -> dict[str, Any]:
        service, project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=[path],
        )
        result = service.context_lookup(
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
        result.setdefault("project_id", project.project_id)
        return result

    def context_memory(
        self,
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
        mcp_roots: list[Any] | None = None,
    ) -> dict[str, Any]:
        service, project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
        )
        result = service.context_memory(
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
        result.setdefault("project_id", project.project_id)
        return result

    def context_admin(
        self,
        mode: str = "health",
        path: str = ".",
        max_files: int = 5000,
        max_age_minutes: int = 1440,
        max_output_chars: int | None = None,
        default_output_profile: str | None = None,
        tool_name: str = "",
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> dict[str, Any]:
        if mode == "projects":
            return self._project_list(mcp_roots=mcp_roots)
        if mode == "contracts":
            return ContextService(self.config).context_admin(
                mode="contracts",
                tool_name=tool_name,
            )
        if mode == "health" and not project_id and not root_uri:
            visible = self.registry.roots_from_mcp(mcp_roots or [])
            if len(visible) != 1:
                return {
                    "schema": "context_admin.health.v1",
                    "ok": True,
                    "mode": "global" if visible else "legacy",
                    "visible_project_count": len(visible),
                    "state_dir": str(self.config.state_dir),
                    "projects": self._project_list(mcp_roots=mcp_roots),
                }
        service, project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=[path],
        )
        result = service.context_admin(
            mode=mode,
            path=path,
            max_files=max_files,
            max_age_minutes=max_age_minutes,
            max_output_chars=max_output_chars,
            default_output_profile=default_output_profile,
            tool_name=tool_name,
        )
        result.setdefault("project_id", project.project_id)
        return result

    def result_reference_resolve(
        self,
        reference_id: str = "",
        reference: dict[str, Any] | None = None,
        expected_hash: str = "",
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> dict[str, Any]:
        project_id = project_id or self._project_id_from_reference(reference)
        service, project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
        )
        result = service.result_reference_resolve(
            reference_id=reference_id,
            reference=reference,
            expected_hash=expected_hash,
        )
        result.setdefault("project_id", project.project_id)
        return result

    def repo_summary_resource(
        self,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> str:
        service, _project = self._service_for_request(
            project_id=project_id, root_uri=root_uri, mcp_roots=mcp_roots
        )
        return service.repo_summary_resource()

    def repo_file_resource(
        self,
        path: str,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> str:
        service, _project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=[path],
        )
        return service.repo_file_resource(path)

    def repo_tree_resource(
        self,
        path: str,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> str:
        service, _project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=[path],
        )
        return service.repo_tree_resource(path)

    def repo_context_resource(
        self,
        reference_id: str,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> str:
        resolved = self.result_reference_resolve(
            reference_id=reference_id,
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
        )
        return json.dumps(resolved, indent=2, sort_keys=True)

    def repo_metrics_resource(
        self,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
    ) -> str:
        service, _project = self._service_for_request(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
        )
        return service.repo_metrics_resource()

    def _service_for_request(
        self,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
        path_hints: list[str] | None = None,
    ) -> tuple[ContextService, ProjectRoot]:
        project = self.registry.resolve_project(
            project_id=project_id,
            root_uri=root_uri,
            mcp_roots=mcp_roots,
            path_hints=path_hints,
        )
        cache_key = project.project_id
        service = self._services.get(cache_key)
        if service is None:
            service = ContextService(project.to_config(self.config))
            self._services[cache_key] = service
        return service, project

    def _project_list(self, mcp_roots: list[Any] | None = None) -> dict[str, Any]:
        listing = self.registry.list_projects(mcp_roots=mcp_roots)
        if listing["count"] == 0:
            legacy = self.registry.legacy_project()
            listing["projects"] = [legacy.public_metadata()]
            listing["count"] = 1
        return listing

    def _project_id_from_reference(self, reference: dict[str, Any] | None) -> str:
        if not reference:
            return ""
        project_id = str(reference.get("project_id") or "")
        if project_id:
            return project_id
        uri = str(reference.get("resolver", {}).get("uri") or "")
        match = re.match(r"repo://project/([^/]+)/context/", uri)
        return match.group(1) if match else ""
