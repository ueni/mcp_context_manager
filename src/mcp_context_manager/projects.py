from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse, urlunparse

from .config import ContextConfig
from .util import load_json_file, now_iso, save_json_file, sha256_text


@dataclass(frozen=True)
class ProjectRoot:
    project_id: str
    root_uri: str
    root_hash: str
    local_path: Path
    state_dir: Path
    name: str
    source: str
    mapped: bool = False
    legacy: bool = False

    def to_config(self, base_config: ContextConfig) -> ContextConfig:
        if self.legacy:
            return base_config.with_project(
                repo_path=self.local_path,
                state_dir=self.state_dir,
                project_id=self.project_id,
                root_uri=self.root_uri,
            )
        return base_config.with_project(
            repo_path=self.local_path,
            state_dir=self.state_dir,
            project_id=self.project_id,
            root_uri=self.root_uri,
        )

    def public_metadata(self) -> dict[str, Any]:
        return {
            "schema": "context_project.v1",
            "project_id": self.project_id,
            "name": self.name,
            "source": self.source,
            "root": {
                "uri_hash": self.root_hash,
                "scheme": "file",
                "mapped": self.mapped,
            },
            "state": {
                "state_key": f"projects/{self.project_id}",
                "exists": self.state_dir.exists(),
                "index_exists": (self.state_dir / "index" / "context.sqlite3").exists(),
                "memory_exists": (
                    self.state_dir / "memory" / "context_memory.json"
                ).exists(),
                "cache_exists": (self.state_dir / "cache" / "tool_cache.json").exists(),
            },
        }


class ProjectRegistry:
    def __init__(self, config: ContextConfig):
        self.config = config

    def resolve_project(
        self,
        project_id: str | None = None,
        root_uri: str | None = None,
        mcp_roots: list[Any] | None = None,
        path_hints: list[str] | None = None,
    ) -> ProjectRoot:
        visible_roots = self.roots_from_mcp(mcp_roots or [])
        if root_uri:
            project = self.project_from_uri(root_uri, source="root_uri")
            self.remember_project(project)
            return project
        if project_id:
            project = self._project_by_id(project_id, visible_roots)
            self.remember_project(project)
            return project
        if visible_roots:
            if len(visible_roots) == 1:
                self.remember_project(visible_roots[0])
                return visible_roots[0]
            inferred = self._infer_from_paths(visible_roots, path_hints or [])
            if inferred is not None:
                self.remember_project(inferred)
                return inferred
            ids = ", ".join(root.project_id for root in visible_roots)
            raise ValueError(
                "ambiguous project: multiple MCP roots are visible; pass "
                f"project_id or root_uri. visible_project_ids={ids}"
            )
        if not self.legacy_fallback_safe():
            status = self.legacy_fallback_status()
            raise ValueError(
                "project selection required: MCP roots are unavailable and "
                "REPO_PATH is configured as a project parent; pass project_id "
                f"or root_uri. reason={status['reason']}"
            )
        return self.legacy_project()

    def list_projects(self, mcp_roots: list[Any] | None = None) -> dict[str, Any]:
        visible = self.roots_from_mcp(mcp_roots or [])
        known_by_id = {project.project_id: project for project in self.known_projects()}
        for project in visible:
            known_by_id[project.project_id] = project
        projects = [project.public_metadata() for project in sorted(known_by_id.values(), key=lambda item: item.project_id)]
        return {
            "schema": "context_projects.list.v1",
            "count": len(projects),
            "projects": projects,
            "selection": {
                "default": "mcp_roots",
                "legacy_repo_path_fallback": not bool(visible)
                and self.legacy_fallback_safe(),
                "project_selection_required": (not bool(visible))
                and not self.legacy_fallback_safe(),
                "ambiguous_without_project": len(visible) > 1,
                "legacy_fallback": self.legacy_fallback_status(),
            },
        }

    def roots_from_mcp(self, roots: list[Any]) -> list[ProjectRoot]:
        projects: list[ProjectRoot] = []
        seen: set[str] = set()
        for root in roots:
            root_uri, name = self._root_uri_and_name(root)
            if not root_uri:
                continue
            project = self.project_from_uri(root_uri, name=name, source="mcp_roots")
            if project.project_id in seen:
                continue
            seen.add(project.project_id)
            projects.append(project)
        return projects

    def legacy_project(self) -> ProjectRoot:
        root_uri = canonical_file_uri(self.config.repo_path)
        root_hash = sha256_text(root_uri)
        project_id = self.config.project_id or f"legacy-{root_hash[:12]}"
        return ProjectRoot(
            project_id=project_id,
            root_uri=root_uri,
            root_hash=root_hash,
            local_path=self.config.repo_path,
            state_dir=self.config.state_dir,
            name=self.config.repo_path.name or "project",
            source="repo_path",
            legacy=True,
        )

    def legacy_fallback_safe(self) -> bool:
        return bool(self.legacy_fallback_status()["safe"])

    def legacy_fallback_status(self) -> dict[str, Any]:
        if not self.config.allowed_roots:
            return {"safe": True, "reason": "single_project_config"}
        repo_path = _norm_abs_posix(str(self.config.repo_path))
        configured_parents = {
            _allowed_root_to_path(allowed) for allowed in self.config.allowed_roots
        }
        configured_parents.update(
            _norm_abs_posix(local_prefix)
            for _host_prefix, local_prefix in self.config.root_mappings
        )
        if repo_path in configured_parents:
            return {"safe": False, "reason": "repo_path_is_project_parent"}
        return {"safe": True, "reason": "repo_path_is_project_root"}

    def project_from_uri(
        self, root_uri: str, name: str = "", source: str = "root_uri"
    ) -> ProjectRoot:
        canonical_uri = canonicalize_root_uri(root_uri)
        parsed = urlparse(canonical_uri)
        host_path = unquote(parsed.path)
        self._ensure_allowed_host_path(host_path)
        local_path, mapped = self._map_host_path(host_path)
        if not local_path.exists():
            raise ValueError(f"project root is not readable after mapping: {root_uri}")
        if not local_path.is_dir():
            raise ValueError(f"project root must be a directory: {root_uri}")
        root_hash = sha256_text(canonical_uri)
        display_name = name.strip() or Path(host_path).name or "project"
        project_id = f"{_slug(display_name)}-{root_hash[:12]}"
        return ProjectRoot(
            project_id=project_id,
            root_uri=canonical_uri,
            root_hash=root_hash,
            local_path=local_path.resolve(),
            state_dir=(self.config.state_dir / "projects" / project_id).resolve(),
            name=display_name,
            source=source,
            mapped=mapped,
        )

    def known_projects(self) -> list[ProjectRoot]:
        projects_dir = self.config.state_dir / "projects"
        if not projects_dir.is_dir():
            return []
        projects: list[ProjectRoot] = []
        for metadata_path in sorted(projects_dir.glob("*/project.json")):
            metadata = load_json_file(metadata_path, {})
            root_uri = str(metadata.get("root_uri") or "")
            if not root_uri:
                continue
            try:
                project = self.project_from_uri(
                    root_uri,
                    name=str(metadata.get("name") or ""),
                    source="state",
                )
            except ValueError:
                continue
            projects.append(project)
        return projects

    def remember_project(self, project: ProjectRoot) -> None:
        if project.legacy:
            return
        metadata_path = project.state_dir / "project.json"
        current = load_json_file(metadata_path, {})
        created_at = current.get("created_at") or now_iso()
        save_json_file(
            metadata_path,
            {
                "schema": "context_project.metadata.v1",
                "project_id": project.project_id,
                "root_uri": project.root_uri,
                "root_hash": project.root_hash,
                "name": project.name,
                "source": project.source,
                "created_at": created_at,
                "updated_at": now_iso(),
            },
        )

    def _project_by_id(
        self, project_id: str, visible_roots: list[ProjectRoot]
    ) -> ProjectRoot:
        candidates = [*visible_roots, *self.known_projects()]
        if self.legacy_fallback_safe():
            candidates.append(self.legacy_project())
        for project in candidates:
            if project.project_id == project_id:
                return project
        raise ValueError(f"unknown project_id: {project_id}")

    def _infer_from_paths(
        self, roots: list[ProjectRoot], path_hints: list[str]
    ) -> ProjectRoot | None:
        matches: dict[str, ProjectRoot] = {}
        for raw_hint in path_hints:
            hint = raw_hint.strip()
            if not hint or hint == ".":
                continue
            for root in roots:
                if self._path_hint_matches_root(hint, root):
                    matches[root.project_id] = root
        if len(matches) == 1:
            return next(iter(matches.values()))
        return None

    def _path_hint_matches_root(self, hint: str, root: ProjectRoot) -> bool:
        raw = Path(hint)
        if raw.is_absolute():
            try:
                raw.resolve().relative_to(root.local_path)
                return True
            except ValueError:
                return False
        try:
            candidate = (root.local_path / hint).resolve()
            candidate.relative_to(root.local_path)
        except ValueError:
            return False
        return candidate.exists()

    def _root_uri_and_name(self, root: Any) -> tuple[str, str]:
        if isinstance(root, str):
            return root, ""
        if isinstance(root, dict):
            return str(root.get("uri") or ""), str(root.get("name") or "")
        uri = getattr(root, "uri", "")
        name = getattr(root, "name", "")
        if hasattr(uri, "unicode_string"):
            uri = uri.unicode_string()
        return str(uri or ""), str(name or "")

    def _map_host_path(self, host_path: str) -> tuple[Path, bool]:
        normalized = _norm_abs_posix(host_path)
        mappings = sorted(
            self.config.root_mappings,
            key=lambda item: len(_norm_abs_posix(item[0])),
            reverse=True,
        )
        for host_prefix, local_prefix in mappings:
            host_prefix = _norm_abs_posix(host_prefix)
            if normalized == host_prefix or normalized.startswith(host_prefix + "/"):
                rel = posixpath.relpath(normalized, host_prefix)
                local = Path(local_prefix)
                if rel != ".":
                    local = local / Path(rel)
                return local.resolve(), True
        return Path(normalized).resolve(), False

    def _ensure_allowed_host_path(self, host_path: str) -> None:
        if not self.config.allowed_roots:
            legacy = _norm_abs_posix(str(self.config.repo_path))
            normalized = _norm_abs_posix(host_path)
            if normalized == legacy:
                return
            raise ValueError(
                "MCP_CONTEXT_ALLOWED_ROOTS is required before using MCP roots "
                "outside the legacy REPO_PATH"
            )
        normalized = _norm_abs_posix(host_path)
        for allowed in self.config.allowed_roots:
            allowed_path = _allowed_root_to_path(allowed)
            if normalized == allowed_path or normalized.startswith(allowed_path + "/"):
                return
        raise ValueError("MCP root is outside MCP_CONTEXT_ALLOWED_ROOTS")


def canonicalize_root_uri(root_uri: str) -> str:
    raw = str(root_uri).strip()
    if not raw:
        raise ValueError("root_uri is required")
    parsed = urlparse(raw)
    if not parsed.scheme:
        return canonical_file_uri(Path(raw))
    if parsed.scheme != "file":
        raise ValueError("only file:// MCP roots are supported")
    if parsed.netloc not in {"", "localhost"}:
        raise ValueError("only local file:// MCP roots are supported")
    path = _norm_abs_posix(unquote(parsed.path))
    return urlunparse(("file", "", quote(path), "", "", ""))


def canonical_file_uri(path: Path) -> str:
    normalized = _norm_abs_posix(str(path.resolve()))
    return urlunparse(("file", "", quote(normalized), "", "", ""))


def _allowed_root_to_path(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return _norm_abs_posix(unquote(parsed.path))
    return _norm_abs_posix(value)


def _norm_abs_posix(path: str) -> str:
    normalized = posixpath.normpath(path.replace("\\", "/"))
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized.rstrip("/") or "/"


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug or "project")[:40].strip("-") or "project"
