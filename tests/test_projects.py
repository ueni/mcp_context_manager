from __future__ import annotations

import json
from pathlib import Path

import pytest

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService
from mcp_context_manager.manager import ProjectContextService
from mcp_context_manager.projects import ProjectRegistry, canonicalize_root_uri


def write_file(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def make_repo(root: Path, marker: str) -> Path:
    root.mkdir(parents=True)
    write_file(root, "src/app.py", f"def marker():\n    return '{marker}'\n")
    write_file(root, "README.md", f"# {marker}\n")
    return root


def test_root_uri_canonicalization_and_project_state_isolation(tmp_path: Path) -> None:
    repo_a = make_repo(tmp_path / "alpha project", "alpha")
    repo_b = make_repo(tmp_path / "beta", "beta")
    config = ContextConfig(
        repo_path=repo_a.resolve(),
        state_dir=(tmp_path / "state").resolve(),
        allowed_roots=(str(tmp_path),),
    )
    registry = ProjectRegistry(config)

    canonical = canonicalize_root_uri(repo_a.as_uri() + "/")
    assert canonical == canonicalize_root_uri(repo_a.as_uri())

    project_a = registry.project_from_uri(repo_a.as_uri(), name="Alpha Project")
    project_a_again = registry.project_from_uri(repo_a.as_uri() + "/", name="Alpha Project")
    project_b = registry.project_from_uri(repo_b.as_uri(), name="Beta")

    assert project_a.project_id == project_a_again.project_id
    assert project_a.project_id != project_b.project_id
    assert project_a.state_dir != project_b.state_dir
    assert project_a.state_dir == config.state_dir / "projects" / project_a.project_id


def test_project_context_service_auto_indexes_and_isolates_memory(tmp_path: Path) -> None:
    repo_a = make_repo(tmp_path / "alpha", "alpha")
    repo_b = make_repo(tmp_path / "beta", "beta")
    manager = ProjectContextService(
        ContextConfig(
            repo_path=repo_a.resolve(),
            state_dir=(tmp_path / "state").resolve(),
            allowed_roots=(str(tmp_path),),
        )
    )
    roots_a = [{"uri": repo_a.as_uri(), "name": "Alpha"}]
    roots_b = [{"uri": repo_b.as_uri(), "name": "Beta"}]

    pack = manager.context_pack("review marker alpha", mcp_roots=roots_a, max_items=2)
    project_a = pack["project"]["project_id"]
    assert pack["items"]
    assert (tmp_path / "state" / "projects" / project_a / "index" / "context.sqlite3").exists()

    manager.context_memory(
        mode="upsert",
        namespace="workspace",
        key="marker",
        value={"project": "alpha"},
        mcp_roots=roots_a,
    )
    manager.context_memory(
        mode="upsert",
        namespace="workspace",
        key="marker",
        value={"project": "beta"},
        mcp_roots=roots_b,
    )

    memory_a = manager.context_memory(mode="get", namespace="workspace", mcp_roots=roots_a)
    memory_b = manager.context_memory(mode="get", namespace="workspace", mcp_roots=roots_b)

    assert memory_a["entries"][0]["value"] == {"project": "alpha"}
    assert memory_b["entries"][0]["value"] == {"project": "beta"}

    metrics_a = manager.context_admin(mode="metrics", mcp_roots=roots_a)
    metrics_b = manager.context_admin(mode="metrics", mcp_roots=roots_b)
    metrics_resource_a = json.loads(
        manager.repo_metrics_resource(project_id=metrics_a["project_id"])
    )

    assert metrics_a["path"] != metrics_b["path"]
    assert metrics_a["requests"]["by_operation"]["context_pack"]["count"] == 1
    assert metrics_b["requests"]["by_operation"].get("context_pack", {}).get("count", 0) == 0
    assert metrics_resource_a["project_id"] == metrics_a["project_id"]


def test_multi_root_requests_are_ambiguous_unless_paths_disambiguate(
    tmp_path: Path,
) -> None:
    repo_a = make_repo(tmp_path / "alpha", "alpha")
    repo_b = make_repo(tmp_path / "beta", "beta")
    write_file(repo_a, "src/only_alpha.py", "ALPHA_ONLY = True\n")
    manager = ProjectContextService(
        ContextConfig(
            repo_path=repo_a.resolve(),
            state_dir=(tmp_path / "state").resolve(),
            allowed_roots=(str(tmp_path),),
        )
    )
    roots = [
        {"uri": repo_a.as_uri(), "name": "Alpha"},
        {"uri": repo_b.as_uri(), "name": "Beta"},
    ]

    with pytest.raises(ValueError, match="ambiguous project"):
        manager.context_pack("review marker", mcp_roots=roots)

    pack = manager.context_pack(
        "review alpha-only file",
        focus_paths=["src/only_alpha.py"],
        mcp_roots=roots,
        max_items=2,
    )
    assert pack["project"]["name"] == "Alpha"
    assert any(item["path"] == "src/only_alpha.py" for item in pack["items"])


def test_docker_host_to_container_root_mapping(tmp_path: Path) -> None:
    host_parent = tmp_path / "host-source"
    container_parent = tmp_path / "workspace-roots"
    host_repo = host_parent / "demo"
    container_repo = make_repo(container_parent / "demo", "mapped")
    config = ContextConfig(
        repo_path=container_repo.resolve(),
        state_dir=(tmp_path / "state").resolve(),
        allowed_roots=(str(host_parent),),
        root_mappings=((str(host_parent), str(container_parent)),),
    )
    registry = ProjectRegistry(config)

    project = registry.project_from_uri(host_repo.as_uri(), name="Demo")

    assert project.local_path == container_repo.resolve()
    assert project.mapped is True
    assert project.project_id.endswith(project.root_hash[:12])


def test_unsafe_global_parent_requires_explicit_project_selection(
    tmp_path: Path,
) -> None:
    host_parent = tmp_path / "host-source"
    container_parent = tmp_path / "workspace-roots"
    host_repo = host_parent / "demo"
    make_repo(container_parent / "demo", "mapped")
    container_parent.mkdir(exist_ok=True)
    manager = ProjectContextService(
        ContextConfig(
            repo_path=container_parent.resolve(),
            state_dir=(tmp_path / "state").resolve(),
            allowed_roots=(str(host_parent),),
            root_mappings=((str(host_parent), str(container_parent)),),
        )
    )

    with pytest.raises(ValueError, match="project selection required"):
        manager.context_pack("review mapped marker")

    pack = manager.context_pack(
        "review mapped marker",
        root_uri=host_repo.as_uri(),
        max_items=2,
    )
    assert pack["items"]
    assert pack["project"]["root"]["mapped"] is True
    assert pack["project"]["name"] == "demo"

    health = manager.context_admin(mode="health")
    assert health["ok"] is True
    assert health["project_selection_required"] is True
    assert health["visible_project_count"] == 0
    assert health["projects"]["count"] == 1


def test_legacy_single_project_fallback_still_works(tmp_path: Path) -> None:
    repo = make_repo(tmp_path / "repo", "legacy")
    manager = ProjectContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    pack = manager.context_pack("review legacy marker", max_items=2)

    assert pack["items"]
    assert pack["project"]["source"] == "repo_path"
    assert pack["project"]["project_id"].startswith("legacy-")


def test_hash_refresh_updates_changed_files_and_removes_deleted(
    tmp_path: Path,
) -> None:
    repo = make_repo(tmp_path / "repo", "alpha")
    service = ContextService(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    first = service.context_admin(mode="index_refresh")
    second = service.context_admin(mode="index_refresh")
    assert first["updated_count"] >= 2
    assert second["updated_count"] == 0
    assert second["unchanged_count"] >= 2

    write_file(repo, "src/app.py", "def marker():\n    return 'changed'\n")
    changed = service.context_admin(mode="index_refresh")
    assert changed["updated_count"] == 1
    assert service.context_lookup(mode="search", query="changed")["results"]

    (repo / "README.md").unlink()
    removed = service.context_admin(mode="index_refresh")
    assert removed["removed_count"] == 1
    assert all(row["path"] != "README.md" for row in service.index.files())
