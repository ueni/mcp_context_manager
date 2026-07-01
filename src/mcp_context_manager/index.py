from __future__ import annotations

import ast
import fnmatch
import re
import sqlite3
from pathlib import Path
from typing import Any

from .config import ContextConfig
from .util import (
    TEXT_EXTENSIONS,
    git_value,
    is_likely_binary,
    language_for_path,
    normalize_query_terms,
    now_iso,
    prompt_injection_signals,
    redact_text,
    sha256_bytes,
    should_skip_path,
    trim_text,
)


class ContextIndex:
    def __init__(self, config: ContextConfig):
        self.config = config

    def connect(self) -> sqlite3.Connection:
        self.config.ensure_state_dirs()
        conn = sqlite3.connect(self.config.index_db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self, conn: sqlite3.Connection) -> bool:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
              path TEXT PRIMARY KEY,
              size INTEGER NOT NULL,
              mtime_ns INTEGER NOT NULL,
              sha256 TEXT NOT NULL,
              extension TEXT NOT NULL,
              language TEXT NOT NULL,
              line_count INTEGER NOT NULL,
              indexed_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbols (
              path TEXT NOT NULL,
              name TEXT NOT NULL,
              kind TEXT NOT NULL,
              line_start INTEGER NOT NULL,
              line_end INTEGER NOT NULL,
              signature TEXT NOT NULL,
              PRIMARY KEY (path, name, kind, line_start)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imports (
              path TEXT NOT NULL,
              target TEXT NOT NULL,
              line INTEGER NOT NULL,
              PRIMARY KEY (path, target, line)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        fts_enabled = True
        try:
            conn.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS fts_files USING fts5(path UNINDEXED, content)"
            )
        except sqlite3.OperationalError:
            fts_enabled = False
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            ("fts_enabled", "true" if fts_enabled else "false"),
        )
        conn.commit()
        return fts_enabled

    def _fts_enabled(self, conn: sqlite3.Connection) -> bool:
        self.initialize(conn)
        row = conn.execute("SELECT value FROM meta WHERE key='fts_enabled'").fetchone()
        return bool(row and row["value"] == "true")

    def _iter_candidate_files(self, root: Path, max_files: int) -> list[Path]:
        files: list[Path] = []
        if root.is_file():
            candidates = [root]
        else:
            candidates = sorted(root.rglob("*"))
        for path in candidates:
            if len(files) >= max_files:
                break
            if not path.is_file():
                continue
            if should_skip_path(path, self.config.repo_path, self.config.state_dir):
                continue
            if path.stat().st_size > self.config.max_read_bytes:
                continue
            suffix = path.suffix.lower()
            if suffix and suffix not in TEXT_EXTENSIONS:
                continue
            if is_likely_binary(path):
                continue
            files.append(path)
        return files

    def refresh(self, path: str = ".", max_files: int = 5000) -> dict[str, Any]:
        root = self.config.resolve_repo_path(path)
        if not root.exists():
            raise FileNotFoundError(path)
        indexed_at = now_iso()
        files = self._iter_candidate_files(root, max_files=max_files)
        with self.connect() as conn:
            fts_enabled = self.initialize(conn)
            conn.execute("DELETE FROM files")
            conn.execute("DELETE FROM symbols")
            conn.execute("DELETE FROM imports")
            if fts_enabled:
                conn.execute("DELETE FROM fts_files")
            symbol_count = 0
            import_count = 0
            for file_path in files:
                rel = str(file_path.relative_to(self.config.repo_path)).replace("\\", "/")
                raw = file_path.read_bytes()
                text = raw.decode("utf-8", errors="replace")
                stat = file_path.stat()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO files
                    (path, size, mtime_ns, sha256, extension, language, line_count, indexed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rel,
                        int(stat.st_size),
                        int(stat.st_mtime_ns),
                        sha256_bytes(raw),
                        file_path.suffix.lower(),
                        language_for_path(rel),
                        len(text.splitlines()),
                        indexed_at,
                    ),
                )
                if fts_enabled:
                    conn.execute(
                        "INSERT INTO fts_files(path, content) VALUES (?, ?)",
                        (rel, text),
                    )
                symbols, imports = self.extract_file_intel(rel, text)
                symbol_count += len(symbols)
                import_count += len(imports)
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO symbols
                    (path, name, kind, line_start, line_end, signature)
                    VALUES (:path, :name, :kind, :line_start, :line_end, :signature)
                    """,
                    symbols,
                )
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO imports(path, target, line)
                    VALUES (:path, :target, :line)
                    """,
                    imports,
                )
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("generated_at", indexed_at))
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("git_head", git_value(self.config.repo_path, "rev-parse", "HEAD")))
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("git_branch", git_value(self.config.repo_path, "branch", "--show-current")))
            conn.commit()
        return {
            "schema": "context_index.refresh.v1",
            "generated_at": indexed_at,
            "index_path": str(self.config.index_db_path.relative_to(self.config.repo_path)),
            "file_count": len(files),
            "symbol_count": symbol_count,
            "import_count": import_count,
            "fts_enabled": fts_enabled,
        }

    def status(self) -> dict[str, Any]:
        with self.connect() as conn:
            fts_enabled = self.initialize(conn)
            file_count = conn.execute("SELECT COUNT(*) AS count FROM files").fetchone()["count"]
            symbol_count = conn.execute("SELECT COUNT(*) AS count FROM symbols").fetchone()["count"]
            meta = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM meta")}
        return {
            "schema": "context_index.status.v1",
            "index_path": str(self.config.index_db_path.relative_to(self.config.repo_path)),
            "exists": self.config.index_db_path.exists(),
            "file_count": int(file_count),
            "symbol_count": int(symbol_count),
            "fts_enabled": fts_enabled,
            "generated_at": meta.get("generated_at", ""),
            "git_head": meta.get("git_head", ""),
            "git_branch": meta.get("git_branch", ""),
        }

    def files(self, limit: int = 1000) -> list[dict[str, Any]]:
        with self.connect() as conn:
            self.initialize(conn)
            rows = conn.execute(
                "SELECT path, size, extension, language, line_count FROM files ORDER BY path LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def extract_file_intel(
        self, rel_path: str, text: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if rel_path.endswith(".py"):
            return self._extract_python_intel(rel_path, text)
        return self._extract_generic_symbols(rel_path, text), []

    def _extract_python_intel(
        self, rel_path: str, text: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        symbols: list[dict[str, Any]] = []
        imports: list[dict[str, Any]] = []
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return symbols, imports
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                kind = "class" if isinstance(node, ast.ClassDef) else "function"
                signature = node.name
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    args = [arg.arg for arg in node.args.args]
                    signature = f"{node.name}({', '.join(args)})"
                symbols.append(
                    {
                        "path": rel_path,
                        "name": node.name,
                        "kind": kind,
                        "line_start": int(getattr(node, "lineno", 1)),
                        "line_end": int(getattr(node, "end_lineno", getattr(node, "lineno", 1))),
                        "signature": signature,
                    }
                )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(
                        {"path": rel_path, "target": alias.name.split(".", 1)[0], "line": int(node.lineno)}
                    )
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(
                    {"path": rel_path, "target": node.module.split(".", 1)[0], "line": int(node.lineno)}
                )
        return symbols, imports

    def _extract_generic_symbols(self, rel_path: str, text: str) -> list[dict[str, Any]]:
        symbols: list[dict[str, Any]] = []
        patterns = [
            ("function", re.compile(r"\bfunction\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")),
            ("function", re.compile(r"\bdef\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")),
            ("class", re.compile(r"\b(class|struct|interface)\s+([A-Za-z_][A-Za-z0-9_]*)")),
        ]
        for idx, line in enumerate(text.splitlines(), start=1):
            for kind, pattern in patterns:
                match = pattern.search(line)
                if not match:
                    continue
                name = match.group(match.lastindex or 1)
                if kind == "class" and match.lastindex and match.lastindex > 1:
                    name = match.group(2)
                symbols.append(
                    {
                        "path": rel_path,
                        "name": name,
                        "kind": kind,
                        "line_start": idx,
                        "line_end": idx,
                        "signature": line.strip()[:160],
                    }
                )
        return symbols

    def search(
        self,
        query: str,
        path: str = ".",
        max_results: int = 20,
        include_globs: list[str] | None = None,
    ) -> dict[str, Any]:
        terms = normalize_query_terms(query, max_terms=8)
        if not terms:
            raise ValueError("query must contain at least one searchable term")
        root_rel = self.config.repo_relative(path)
        if root_rel == ".":
            root_rel = ""
        with self.connect() as conn:
            fts_enabled = self._fts_enabled(conn)
            rows: list[dict[str, Any]] = []
            if fts_enabled:
                fts_query = " OR ".join(f'"{term}"' for term in terms)
                try:
                    sql_rows = conn.execute(
                        """
                        SELECT path, snippet(fts_files, 1, '', '', ' ... ', 12) AS excerpt
                        FROM fts_files
                        WHERE fts_files MATCH ?
                        LIMIT ?
                        """,
                        (fts_query, max_results * 4),
                    ).fetchall()
                    rows = [{"path": row["path"], "excerpt": row["excerpt"], "source": "fts"} for row in sql_rows]
                except sqlite3.OperationalError:
                    rows = []
            if not rows:
                rows = self._fallback_search(terms, root_rel=root_rel, limit=max_results * 4)

        filtered: list[dict[str, Any]] = []
        for row in rows:
            rel = row["path"]
            if root_rel and not rel.startswith(root_rel.rstrip("/") + "/") and rel != root_rel:
                continue
            if include_globs and not any(fnmatch.fnmatch(rel, glob) for glob in include_globs):
                continue
            score = sum(2.0 for term in terms if term in rel.lower())
            score += sum(1.0 for term in terms if term in str(row.get("excerpt", "")).lower())
            filtered.append({**row, "score": round(score, 4), "terms": terms})
        filtered.sort(key=lambda item: (-float(item["score"]), item["path"]))
        results = filtered[:max_results]
        return {
            "schema": "context_search.v1",
            "query": query,
            "terms": terms,
            "count": len(results),
            "results": results,
            "index": self.status(),
        }

    def _fallback_search(self, terms: list[str], root_rel: str, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        root = self.config.repo_path / root_rel if root_rel else self.config.repo_path
        for path in self._iter_candidate_files(root, max_files=5000):
            rel = str(path.relative_to(self.config.repo_path)).replace("\\", "/")
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            except OSError:
                continue
            for idx, line in enumerate(lines, start=1):
                low = line.lower()
                if any(term in low for term in terms):
                    rows.append(
                        {
                            "path": rel,
                            "line": idx,
                            "excerpt": line.strip()[:240],
                            "source": "scan",
                        }
                    )
                    break
            if len(rows) >= limit:
                break
        return rows

    def symbols(self, query: str = "", limit: int = 50) -> dict[str, Any]:
        terms = normalize_query_terms(query, max_terms=8)
        with self.connect() as conn:
            self.initialize(conn)
            if terms:
                like = [f"%{term}%" for term in terms]
                clauses = " OR ".join(["LOWER(name) LIKE ?" for _ in like])
                rows = conn.execute(
                    f"""
                    SELECT path, name, kind, line_start, line_end, signature
                    FROM symbols WHERE {clauses}
                    ORDER BY path, line_start LIMIT ?
                    """,
                    (*like, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT path, name, kind, line_start, line_end, signature
                    FROM symbols ORDER BY path, line_start LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return {"schema": "context_symbols.v1", "count": len(rows), "symbols": [dict(row) for row in rows]}

    def snippet(
        self,
        path: str,
        start_line: int = 1,
        end_line: int | None = None,
        context_before: int = 0,
        context_after: int = 0,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        if start_line < 1:
            raise ValueError("start_line must be >= 1")
        file_path = self.config.resolve_repo_path(path)
        if not file_path.is_file():
            raise FileNotFoundError(path)
        if file_path.stat().st_size > self.config.max_read_bytes:
            raise ValueError("file exceeds max_read_bytes")
        if is_likely_binary(file_path):
            raise ValueError("binary file is not readable as a snippet")
        text = file_path.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        total = len(lines)
        requested_end = end_line if end_line is not None else start_line
        start = max(1, start_line - max(0, context_before))
        end = min(total, requested_end + max(0, context_after))
        if end < start:
            end = start
        content = "\n".join(lines[start - 1 : end])
        content, truncated = trim_text(content, max_chars)
        content, redactions = redact_text(content)
        return {
            "schema": "context_snippet.v1",
            "path": self.config.repo_relative(file_path),
            "start_line": start,
            "end_line": end,
            "requested": {"start_line": start_line, "end_line": requested_end},
            "total_lines": total,
            "content": content,
            "truncated": truncated,
            "redactions": redactions,
            "prompt_injection_signals": prompt_injection_signals(content),
        }

    def tree(self, path: str = ".", max_entries: int = 200, max_depth: int = 2) -> dict[str, Any]:
        root = self.config.resolve_repo_path(path)
        if not root.exists():
            raise FileNotFoundError(path)
        base_depth = len(root.relative_to(self.config.repo_path).parts)
        entries: list[dict[str, Any]] = []
        candidates = [root] if root.is_file() else sorted(root.rglob("*"))
        for candidate in candidates:
            if len(entries) >= max_entries:
                break
            if candidate == root:
                continue
            if should_skip_path(candidate, self.config.repo_path, self.config.state_dir):
                continue
            depth = len(candidate.relative_to(self.config.repo_path).parts) - base_depth
            if depth > max_depth:
                continue
            entries.append(
                {
                    "path": str(candidate.relative_to(self.config.repo_path)).replace("\\", "/"),
                    "type": "dir" if candidate.is_dir() else "file",
                    "size": int(candidate.stat().st_size) if candidate.is_file() else 0,
                }
            )
        return {"schema": "context_tree.v1", "path": self.config.repo_relative(root), "count": len(entries), "entries": entries}

    def workspace_facts(self) -> dict[str, Any]:
        files = self.files(limit=10000)
        ext_counts: dict[str, int] = {}
        for row in files:
            ext = row.get("extension") or "[none]"
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
        top_extensions = [
            {"extension": ext, "count": count}
            for ext, count in sorted(ext_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
        ]
        return {
            "schema": "workspace_facts.v1",
            "file_count": len(files),
            "top_extensions": top_extensions,
            "has_tests_dir": (self.config.repo_path / "tests").is_dir()
            or (self.config.repo_path / "test").is_dir(),
            "has_readme": any((self.config.repo_path / name).is_file() for name in ["README.md", "README.rst", "README.txt"]),
            "is_git_repo": (self.config.repo_path / ".git").exists(),
            "git_branch": git_value(self.config.repo_path, "branch", "--show-current"),
            "git_head": git_value(self.config.repo_path, "rev-parse", "--short", "HEAD"),
            "index": self.status(),
        }
