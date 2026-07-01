from __future__ import annotations

from pathlib import Path

import pytest

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.context import ContextService


def write_file(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture()
def sample_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    write_file(
        repo,
        "src/auth.py",
        """
class AuthService:
    def login(self, user, password):
        if not user:
            raise ValueError("missing user")
        return issue_token(user)


def issue_token(user):
    return f"token-for-{user}"


def revoke_token(token):
    return token.startswith("token-for-")
""".strip()
        + "\n",
    )
    write_file(
        repo,
        "tests/test_auth.py",
        """
from src.auth import issue_token


def test_issue_token():
    assert issue_token("alice") == "token-for-alice"
""".strip()
        + "\n",
    )
    write_file(
        repo,
        "README.md",
        """
# Demo

Authentication is handled in src/auth.py. Update this guide when login behavior changes.
""".strip()
        + "\n",
    )
    write_file(repo, "config/settings.toml", "feature_flag = true\n")
    return repo


@pytest.fixture()
def service(sample_repo: Path) -> ContextService:
    config = ContextConfig(
        repo_path=sample_repo.resolve(),
        state_dir=(sample_repo / ".mcp-context-manager").resolve(),
        max_output_chars=6000,
    )
    return ContextService(config)
