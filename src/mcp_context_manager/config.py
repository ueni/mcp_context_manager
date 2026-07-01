from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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
            max_read_bytes=max(1024, int(os.getenv("MAX_READ_BYTES", "262144"))),
            max_output_chars=max(1024, int(os.getenv("MAX_OUTPUT_CHARS", "12000"))),
            default_output_profile=os.getenv("MCP_CONTEXT_OUTPUT_PROFILE", "compact"),
            transport=os.getenv("MCP_TRANSPORT", "stdio").strip().lower(),
            host=os.getenv("HOST", "127.0.0.1"),
            port=int(os.getenv("PORT", "8000")),
            bearer_token=os.getenv("MCP_HTTP_BEARER_TOKEN", "").strip(),
        )

    @property
    def index_db_path(self) -> Path:
        return self.state_dir / "index" / "context.sqlite3"

    @property
    def references_dir(self) -> Path:
        return self.state_dir / "references"

    @property
    def memory_path(self) -> Path:
        return self.state_dir / "memory" / "context_memory.json"

    @property
    def cache_path(self) -> Path:
        return self.state_dir / "cache" / "tool_cache.json"

    @property
    def budget_path(self) -> Path:
        return self.state_dir / "memory" / "token_budget.json"

    def ensure_state_dirs(self) -> None:
        for path in [
            self.index_db_path.parent,
            self.references_dir,
            self.memory_path.parent,
            self.cache_path.parent,
            self.state_dir / "reports",
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
            return str(resolved)
