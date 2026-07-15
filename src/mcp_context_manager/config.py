from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_PROJECT_MARKERS = (
    ".git",
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "CMakeLists.txt",
)


@dataclass(frozen=True)
class ContextConfig:
    repo_path: Path
    state_dir: Path
    max_read_bytes: int = 262_144
    max_output_chars: int = 12_000
    default_output_profile: str = "compact"
    transport: str = "stdio"
    host: str = "127.0.0.1"
    port: int = 8000
    bearer_token: str = ""
    lmdb_map_size: int = 1_073_741_824
    token_counter_mode: str = "estimate"
    target_tokenizer: str = "cl100k_base"
    auto_learn_cache: bool = True
    auto_learn_min_packs: int = 3
    auto_learn_min_interval_seconds: int = 900
    auto_learn_max_entries: int = 12
    project_id: str = ""
    root_uri: str = ""
    allowed_roots: tuple[str, ...] = field(default_factory=tuple)
    root_mappings: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    project_discovery_max_depth: int = 4
    project_markers: tuple[str, ...] = DEFAULT_PROJECT_MARKERS

    @classmethod
    def from_env(cls) -> "ContextConfig":
        repo_path = Path(os.getenv("REPO_PATH", os.getcwd())).resolve()
        state_dir = Path(
            os.getenv("MCP_CONTEXT_STATE_DIR", str(repo_path / ".mcp-context-manager"))
        )
        if not state_dir.is_absolute():
            state_dir = repo_path / state_dir
        return cls(
            repo_path=repo_path,
            state_dir=state_dir.resolve(),
            allowed_roots=_split_env_list(os.getenv("MCP_CONTEXT_ALLOWED_ROOTS", "")),
            root_mappings=_parse_root_mappings(
                os.getenv("MCP_CONTEXT_ROOT_MAPPINGS", "")
            ),
            project_discovery_max_depth=max(
                0, int(os.getenv("MCP_CONTEXT_PROJECT_DISCOVERY_MAX_DEPTH", "4"))
            ),
            project_markers=_project_markers_from_env(),
            max_read_bytes=max(1024, int(os.getenv("MAX_READ_BYTES", "262144"))),
            max_output_chars=max(1024, int(os.getenv("MAX_OUTPUT_CHARS", "12000"))),
            default_output_profile=os.getenv("MCP_CONTEXT_OUTPUT_PROFILE", "compact"),
            transport=os.getenv("MCP_TRANSPORT", "stdio").strip().lower(),
            host=os.getenv("HOST", "127.0.0.1"),
            port=int(os.getenv("PORT", "8000")),
            bearer_token=os.getenv("MCP_HTTP_BEARER_TOKEN", "").strip(),
            lmdb_map_size=max(
                16_777_216,
                int(os.getenv("MCP_CONTEXT_LMDB_MAP_SIZE", "1073741824")),
            ),
            token_counter_mode=os.getenv(
                "MCP_CONTEXT_TOKEN_COUNTER", "estimate"
            ).strip().lower(),
            target_tokenizer=os.getenv(
                "MCP_CONTEXT_TARGET_TOKENIZER", "cl100k_base"
            ).strip(),
            auto_learn_cache=_env_bool(
                os.getenv("MCP_CONTEXT_AUTO_LEARN_CACHE", "1"),
                default=True,
            ),
            auto_learn_min_packs=max(
                1, int(os.getenv("MCP_CONTEXT_AUTO_LEARN_MIN_PACKS", "3"))
            ),
            auto_learn_min_interval_seconds=max(
                0,
                int(
                    os.getenv(
                        "MCP_CONTEXT_AUTO_LEARN_MIN_INTERVAL_SECONDS", "900"
                    )
                ),
            ),
            auto_learn_max_entries=max(
                1, int(os.getenv("MCP_CONTEXT_AUTO_LEARN_MAX_ENTRIES", "12"))
            ),
        )

    def with_project(
        self,
        repo_path: Path,
        state_dir: Path,
        project_id: str = "",
        root_uri: str = "",
    ) -> "ContextConfig":
        return ContextConfig(
            repo_path=repo_path.resolve(),
            state_dir=state_dir.resolve(),
            project_id=project_id,
            root_uri=root_uri,
            allowed_roots=self.allowed_roots,
            root_mappings=self.root_mappings,
            project_discovery_max_depth=self.project_discovery_max_depth,
            project_markers=self.project_markers,
            max_read_bytes=self.max_read_bytes,
            max_output_chars=self.max_output_chars,
            default_output_profile=self.default_output_profile,
            transport=self.transport,
            host=self.host,
            port=self.port,
            bearer_token=self.bearer_token,
            lmdb_map_size=self.lmdb_map_size,
            token_counter_mode=self.token_counter_mode,
            target_tokenizer=self.target_tokenizer,
            auto_learn_cache=self.auto_learn_cache,
            auto_learn_min_packs=self.auto_learn_min_packs,
            auto_learn_min_interval_seconds=self.auto_learn_min_interval_seconds,
            auto_learn_max_entries=self.auto_learn_max_entries,
        )

    @property
    def store_path(self) -> Path:
        return self.state_dir / "store" / "context.lmdb"

    @property
    def references_dir(self) -> Path:
        return self.state_dir / "references"

    @property
    def tantivy_index_dir(self) -> Path:
        return self.state_dir / "tantivy-index"

    def ensure_state_dirs(self) -> None:
        for path in [
            self.store_path.parent,
            self.references_dir,
            self.state_dir / "traces",
        ]:
            path.mkdir(parents=True, exist_ok=True)

    def resolve_repo_path(self, path: str | Path) -> Path:
        raw = Path(path)
        if raw.is_absolute():
            candidate = raw.resolve()
        else:
            candidate = (self.repo_path / raw).resolve()
        try:
            candidate.relative_to(self.repo_path)
        except ValueError as exc:
            raise ValueError(f"path escapes repository boundary: {path}") from exc
        return candidate

    def repo_relative(self, path: str | Path) -> str:
        resolved = self.resolve_repo_path(path)
        return str(resolved.relative_to(self.repo_path)).replace("\\", "/") or "."

    def display_path(self, path: str | Path) -> str:
        resolved = Path(path).resolve()
        try:
            return str(resolved.relative_to(self.repo_path)).replace("\\", "/") or "."
        except ValueError:
            pass
        try:
            state_relative = resolved.relative_to(self.state_dir)
        except ValueError:
            return "[REDACTED_HOST_PATH]"
        prefix = self._state_display_prefix()
        relative = str(state_relative).replace("\\", "/")
        if relative == ".":
            return prefix
        return f"{prefix}/{relative}"

    def _state_display_prefix(self) -> str:
        if (
            self.project_id
            and self.state_dir.name == self.project_id
            and self.state_dir.parent.name == "projects"
        ):
            return f"state/projects/{self.project_id}"
        return "state"


def _split_env_list(value: str) -> tuple[str, ...]:
    if not value.strip():
        return ()
    raw_parts: list[str] = []
    for chunk in value.split(os.pathsep):
        raw_parts.extend(chunk.split(","))
    return tuple(part.strip() for part in raw_parts if part.strip())


def _project_markers_from_env() -> tuple[str, ...]:
    value = os.getenv("MCP_CONTEXT_PROJECT_MARKERS")
    if value is None:
        return DEFAULT_PROJECT_MARKERS
    return _split_env_list(value)


def _env_bool(value: str | None, default: bool = False) -> bool:
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_root_mappings(value: str) -> tuple[tuple[str, str], ...]:
    mappings: list[tuple[str, str]] = []
    for item in _split_env_list(value):
        if "=" not in item:
            continue
        host_prefix, local_prefix = item.split("=", 1)
        host_prefix = host_prefix.strip().rstrip("/")
        local_prefix = local_prefix.strip().rstrip("/")
        if host_prefix and local_prefix:
            mappings.append((host_prefix or "/", local_prefix or "/"))
    return tuple(mappings)
