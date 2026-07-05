from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .token_counter import estimate_tokens as _estimate_tokens

SECRET_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
    re.compile(
        r"(?i)\b[A-Za-z0-9_-]*(api[_-]?key|token|secret|password)[A-Za-z0-9_-]*\s*[:=]\s*['\"]?[^'\"\s]+"
    ),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}"),
    re.compile(r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
]

FILE_URI_WINDOWS_HOST_PATH_RE = re.compile(
    r"\bfile://(?:localhost)?/[A-Za-z]:[/\\][^\s:'\",)>\]}]+"
)
FILE_URI_HOST_PATH_RE = re.compile(r"\bfile://(?:localhost)?/[^\s:'\",)>\]}]+")
POSIX_HOST_PATH_RE = re.compile(
    r"(?<![\w:/.-])/(home|Users|var|tmp|etc|opt|root|data|mnt|workspace-roots|workspace|private)"
    r"(?:/[^\s:'\",)>\]}]+)?(?=$|[\s:'\",)>\]}])"
)
WINDOWS_HOST_PATH_RE = re.compile(
    r"(?<![\w:/.-])[A-Za-z]:[/\\]"
    r"(?:Users|Windows|ProgramData|Temp|tmp|workspace-roots|workspace|data|mnt|repo)"
    r"(?:[/\\][^\s:'\",)>\]}]+)?(?=$|[\s:'\",)>\]}])"
)

PROMPT_INJECTION_PATTERNS = {
    "instruction_override": re.compile(
        r"(?i)\b(ignore|override|forget|bypass)\b.{0,40}\b(previous|system|developer|instruction|rules)\b"
    ),
    "tool_manipulation": re.compile(
        r"(?i)\b(call|invoke|run|execute|use)\b.{0,40}\b(tool|shell|command|curl|wget)\b"
    ),
    "credential_exfiltration": re.compile(
        r"(?i)\b(send|exfiltrate|upload|print|reveal|show)\b.{0,40}\b(secret|token|password|key|credential)\b"
    ),
    "role_switch": re.compile(r"(?i)\b(system|assistant|developer)\s*:\s*"),
}

CODE_EXTENSIONS = {
    ".py",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".java",
    ".go",
    ".rs",
    ".c",
    ".h",
    ".cc",
    ".cpp",
    ".hpp",
    ".cs",
    ".php",
    ".rb",
    ".swift",
    ".kt",
    ".kts",
    ".sh",
    ".bash",
    ".zsh",
    ".ps1",
    ".sql",
    ".html",
    ".css",
    ".scss",
}

TEXT_EXTENSIONS = CODE_EXTENSIONS | {
    ".md",
    ".rst",
    ".txt",
    ".toml",
    ".yaml",
    ".yml",
    ".json",
    ".ini",
    ".cfg",
    ".xml",
    ".csv",
    ".dockerfile",
}

RUNTIME_SKIP_DIRS = {
    ".cache",
    ".git",
    ".mcp-context-manager",
    ".mypy_cache",
    ".nox",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}

RUNTIME_SECRET_FILENAMES = {
    ".netrc",
    ".npmrc",
    ".pypirc",
    "credentials",
    "credentials.json",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
}

RUNTIME_SECRET_SUFFIXES = {".key", ".pem", ".p12", ".pfx"}

ROUTE_TERMS = {
    "debug": {"bug", "debug", "error", "failure", "traceback", "crash", "fix"},
    "review": {"review", "diff", "change", "pr", "merge", "regression"},
    "test": {"test", "pytest", "coverage", "unit", "integration", "verify"},
    "docs": {"doc", "docs", "readme", "documentation", "guide"},
    "security": {"security", "secret", "token", "auth", "permission", "vulnerability"},
    "coding": {"implement", "code", "feature", "refactor", "api", "function"},
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def expiry_iso(ttl_days: int | None) -> str:
    if ttl_days is None:
        return ""
    return (datetime.now(timezone.utc) + timedelta(days=ttl_days)).isoformat()


def is_expired(value: str | None) -> bool:
    parsed = parse_iso(value)
    return parsed is not None and parsed < datetime.now(timezone.utc)


def estimate_tokens(text_or_value: Any) -> int:
    return _estimate_tokens(text_or_value)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def trim_text(text: str, max_chars: int) -> tuple[str, bool]:
    if max_chars < 1:
        return "", bool(text)
    if len(text) <= max_chars:
        return text, False
    return text[: max_chars - 15].rstrip() + "\n...[truncated]", True


def normalize_query_terms(text: str, max_terms: int = 12) -> list[str]:
    seen: set[str] = set()
    terms: list[str] = []
    for token in re.findall(r"[A-Za-z0-9_]{3,}", text.lower()):
        if token in seen or token.isdigit():
            continue
        seen.add(token)
        terms.append(token)
        if len(terms) >= max_terms:
            break
    return terms


def classify_route(prompt: str) -> str:
    terms = set(normalize_query_terms(prompt, max_terms=80))
    if terms.intersection({"debug", "traceback", "crash", "failure"}):
        return "debug"
    if terms.intersection({"review", "pr", "merge"}):
        return "review"
    if terms.intersection({"implement", "code", "feature", "refactor"}):
        return "coding"
    if terms.intersection({"security", "secret", "token", "auth", "vulnerability"}):
        return "security"
    if terms.intersection({"test", "pytest", "coverage", "verify"}):
        return "test"
    if terms.intersection({"doc", "docs", "readme", "documentation"}):
        return "docs"
    best_route = "general"
    best_score = 0
    for route, route_terms in ROUTE_TERMS.items():
        score = len(terms.intersection(route_terms))
        if score > best_score:
            best_route = route
            best_score = score
    return best_route


def redact_text(text: str) -> tuple[str, list[str]]:
    redactions: list[str] = []
    out = text
    for idx, pattern in enumerate(SECRET_PATTERNS):
        before = out
        out = pattern.sub(f"[REDACTED_SECRET_{idx}]", out)
        if out != before:
            redactions.append(f"secret_pattern_{idx}")

    before = out
    out = FILE_URI_WINDOWS_HOST_PATH_RE.sub("file://[REDACTED_HOST_PATH]", out)
    out = FILE_URI_HOST_PATH_RE.sub("file://[REDACTED_HOST_PATH]", out)
    if out != before:
        redactions.append("host_path_uri")

    before = out
    out = POSIX_HOST_PATH_RE.sub("[REDACTED_HOST_PATH]", out)
    out = WINDOWS_HOST_PATH_RE.sub("[REDACTED_HOST_PATH]", out)
    if out != before:
        redactions.append("host_path")
    return out, sorted(set(redactions))


def redaction_metadata(
    categories: list[str] | None = None, redaction_count: int = 0
) -> dict[str, Any]:
    category_list = sorted(set(categories or []))
    return {
        "redacted": bool(category_list or redaction_count),
        "redaction_count": int(redaction_count),
        "categories": category_list,
    }


def merge_redaction_metadata(*items: dict[str, Any] | None) -> dict[str, Any]:
    categories: list[str] = []
    redaction_count = 0
    redacted = False
    for item in items:
        if not isinstance(item, dict):
            continue
        redacted = redacted or bool(item.get("redacted"))
        redaction_count += int(item.get("redaction_count", 0) or 0)
        raw_categories = item.get("categories", [])
        if isinstance(raw_categories, list):
            categories.extend(str(category) for category in raw_categories)
    merged = redaction_metadata(categories, redaction_count)
    if redacted and not merged["redacted"]:
        merged["redacted"] = True
    return merged


def sanitize_json(value: Any) -> tuple[Any, dict[str, Any]]:
    categories: list[str] = []
    redaction_count = 0

    def walk(item: Any) -> Any:
        nonlocal redaction_count
        if isinstance(item, str):
            sanitized, redactions = redact_text(item)
            if sanitized != item:
                redaction_count += 1
                categories.extend(redactions)
            return sanitized
        if isinstance(item, dict):
            sanitized_dict: dict[Any, Any] = {}
            for key, child in item.items():
                sanitized_key = walk(key) if isinstance(key, str) else key
                sanitized_dict[sanitized_key] = walk(child)
            return sanitized_dict
        if isinstance(item, (list, tuple)):
            return [walk(child) for child in item]
        if isinstance(item, (bool, int, float)) or item is None:
            return item
        return walk(str(item))

    sanitized_value = walk(value)
    return sanitized_value, redaction_metadata(categories, redaction_count)


def contains_sensitive_text(value: str) -> bool:
    redacted, redactions = redact_text(value)
    return redacted != value or bool(redactions)


def prompt_injection_signals(text: str) -> dict[str, Any]:
    categories: list[str] = []
    for name, pattern in PROMPT_INJECTION_PATTERNS.items():
        if pattern.search(text):
            categories.append(name)
    return {
        "schema": "prompt_injection_signals.v1",
        "detected": bool(categories),
        "categories": categories,
    }


def is_likely_binary(path: Path) -> bool:
    try:
        chunk = path.read_bytes()[:4096]
    except OSError:
        return True
    return b"\0" in chunk


def should_skip_path(path: Path, repo_path: Path, state_dir: Path) -> bool:
    try:
        rel = path.relative_to(repo_path)
    except ValueError:
        return True
    try:
        path.relative_to(state_dir)
        return True
    except ValueError:
        pass
    parts = rel.parts
    if any(part in RUNTIME_SKIP_DIRS for part in parts):
        return True
    if any(part.endswith(".egg-info") for part in parts):
        return True
    name = path.name
    lower_name = name.lower()
    if lower_name == ".env" or lower_name.startswith(".env."):
        return True
    if lower_name in RUNTIME_SECRET_FILENAMES:
        return True
    if path.suffix.lower() in RUNTIME_SECRET_SUFFIXES:
        return True
    return False


def language_for_path(path: str) -> str:
    suffix = Path(path).suffix.lower()
    return {
        ".py": "python",
        ".js": "javascript",
        ".jsx": "javascript",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".c": "c",
        ".h": "c",
        ".cc": "cpp",
        ".cpp": "cpp",
        ".hpp": "cpp",
        ".cs": "csharp",
        ".md": "markdown",
        ".rst": "rst",
        ".json": "json",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".toml": "toml",
    }.get(suffix, suffix.lstrip(".") or "text")


def git_value(repo_path: Path, *args: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=repo_path,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def git_snapshot(repo_path: Path) -> dict[str, Any]:
    git_dir = git_value(repo_path, "rev-parse", "--git-dir")
    worktree = git_value(repo_path, "rev-parse", "--show-toplevel")
    head = git_value(repo_path, "rev-parse", "HEAD")
    branch = git_value(repo_path, "branch", "--show-current")
    status = git_value(repo_path, "status", "--porcelain=v1", "-z")
    unstaged_diff = git_value(repo_path, "diff", "--no-ext-diff", "--full-index")
    staged_diff = git_value(
        repo_path, "diff", "--cached", "--no-ext-diff", "--full-index"
    )
    is_git_repo = bool(git_dir or worktree or (repo_path / ".git").exists())
    status_hash = sha256_text(status) if status else ""
    changes_payload = "\0".join(
        part for part in [status, unstaged_diff, staged_diff] if part
    )
    changes_hash = sha256_text(changes_payload) if changes_payload else status_hash
    worktree_matches_path = False
    if worktree:
        try:
            worktree_matches_path = Path(worktree).resolve() == repo_path.resolve()
        except OSError:
            worktree_matches_path = False
    if not head:
        return {
            "schema": "git_snapshot.v1",
            "is_git_repo": is_git_repo,
            "available": False,
            "worktree_matches_path": worktree_matches_path,
            "git_head": "",
            "git_head_short": "",
            "git_branch": branch,
            "git_status_hash": status_hash,
            "git_changes_hash": changes_hash,
            "dirty": bool(status),
        }
    return {
        "schema": "git_snapshot.v1",
        "is_git_repo": True,
        "available": True,
        "worktree_matches_path": worktree_matches_path,
        "git_head": head,
        "git_head_short": head[:12],
        "git_branch": branch,
        "git_status_hash": status_hash,
        "git_changes_hash": changes_hash,
        "dirty": bool(status),
    }


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json_file(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)
