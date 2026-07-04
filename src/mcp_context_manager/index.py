from __future__ import annotations

import ast
import fnmatch
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

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
    sha256_text,
    should_skip_path,
    trim_text,
)


@dataclass(frozen=True)
class CandidateFile:
    path: Path
    size: int
    mtime_ns: int


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

    def _iter_candidate_files(self, root: Path, max_files: int) -> list[CandidateFile]:
        files: list[CandidateFile] = []
        if root.is_file():
            candidates = [root]
        else:
            candidates = self._walk_candidates(root)
        for path in candidates:
            if len(files) >= max_files:
                break
            if path.is_symlink():
                continue
            if not path.is_file():
                continue
            if should_skip_path(path, self.config.repo_path, self.config.state_dir):
                continue
            stat = path.stat()
            if stat.st_size > self.config.max_read_bytes:
                continue
            suffix = path.suffix.lower()
            if suffix and suffix not in TEXT_EXTENSIONS:
                continue
            files.append(
                CandidateFile(
                    path=path,
                    size=int(stat.st_size),
                    mtime_ns=int(stat.st_mtime_ns),
                )
            )
        return files

    def _walk_candidates(self, root: Path) -> Iterator[Path]:
        try:
            children = sorted(root.iterdir(), key=lambda path: path.name)
        except OSError:
            return
        for child in children:
            if should_skip_path(child, self.config.repo_path, self.config.state_dir):
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                yield from self._walk_candidates(child)
                continue
            yield child

    def refresh(self, path: str = ".", max_files: int = 5000) -> dict[str, Any]:
        root = self.config.resolve_repo_path(path)
        if not root.exists():
            raise FileNotFoundError(path)
        whole_repo_refresh = root == self.config.repo_path
        indexed_at = now_iso()
        files = self._iter_candidate_files(root, max_files=max_files)
        with self.connect() as conn:
            fts_enabled = self.initialize(conn)
            existing_rows = {
                row["path"]: row
                for row in conn.execute(
                    "SELECT path, size, mtime_ns, sha256 FROM files"
                ).fetchall()
                if self._rel_in_refresh_scope(row["path"], root)
            }
            current_rels = {
                str(candidate.path.relative_to(self.config.repo_path)).replace("\\", "/")
                for candidate in files
            }
            removed_count = 0
            updated_count = 0
            unchanged_count = 0
            for rel in sorted(set(existing_rows) - current_rels):
                self._delete_file_rows(conn, rel, fts_enabled)
                removed_count += 1
            for candidate in files:
                file_path = candidate.path
                rel = str(file_path.relative_to(self.config.repo_path)).replace("\\", "/")
                existing = existing_rows.get(rel)
                if (
                    existing
                    and int(existing["size"]) == candidate.size
                    and int(existing["mtime_ns"]) == candidate.mtime_ns
                ):
                    unchanged_count += 1
                    continue
                if is_likely_binary(file_path):
                    if existing:
                        self._delete_file_rows(conn, rel, fts_enabled)
                        removed_count += 1
                    continue
                raw = file_path.read_bytes()
                text = raw.decode("utf-8", errors="replace")
                digest = sha256_bytes(raw)
                self._delete_file_rows(conn, rel, fts_enabled)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO files
                    (path, size, mtime_ns, sha256, extension, language, line_count, indexed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rel,
                        candidate.size,
                        candidate.mtime_ns,
                        digest,
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
                updated_count += 1
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
            previous_generated = conn.execute(
                "SELECT value FROM meta WHERE key='generated_at'"
            ).fetchone()
            generated_at = (
                indexed_at
                if updated_count or removed_count or previous_generated is None
                else previous_generated["value"]
            )
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("generated_at", generated_at))
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("git_head", git_value(self.config.repo_path, "rev-parse", "HEAD")))
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("git_branch", git_value(self.config.repo_path, "branch", "--show-current")))
            if whole_repo_refresh:
                signature = self.refresh_signature()
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                    ("refresh_signature", signature["signature"]),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                    (
                        "refresh_signature_available",
                        "true" if signature["available"] else "false",
                    ),
                )
            conn.commit()
            total_files = conn.execute("SELECT COUNT(*) AS count FROM files").fetchone()["count"]
            total_symbols = conn.execute("SELECT COUNT(*) AS count FROM symbols").fetchone()["count"]
            total_imports = conn.execute("SELECT COUNT(*) AS count FROM imports").fetchone()["count"]
        return {
            "schema": "context_index.refresh.v1",
            "generated_at": generated_at,
            "index_path": self.config.display_path(self.config.index_db_path),
            "file_count": int(total_files),
            "symbol_count": int(total_symbols),
            "import_count": int(total_imports),
            "files_considered": len(files),
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "removed_count": removed_count,
            "fts_enabled": fts_enabled,
        }

    def refresh_if_needed(
        self, path: str = ".", max_files: int = 5000, force: bool = False
    ) -> dict[str, Any]:
        if force:
            result = self.refresh(path=path, max_files=max_files)
            result["skipped"] = False
            result["reason"] = "forced"
            return result
        root = self.config.resolve_repo_path(path)
        if root != self.config.repo_path:
            result = self.refresh(path=path, max_files=max_files)
            result["skipped"] = False
            result["reason"] = "scoped_refresh"
            return result
        signature = self.refresh_signature()
        if not signature["available"]:
            result = self.refresh(path=path, max_files=max_files)
            result["skipped"] = False
            result["reason"] = "signature_unavailable"
            return result
        with self.connect() as conn:
            self.initialize(conn)
            meta = {
                row["key"]: row["value"]
                for row in conn.execute("SELECT key, value FROM meta")
            }
            file_count = conn.execute("SELECT COUNT(*) AS count FROM files").fetchone()[
                "count"
            ]
        if file_count and meta.get("refresh_signature") == signature["signature"]:
            status = self.status()
            return {
                "schema": "context_index.refresh.v1",
                "generated_at": status["generated_at"],
                "index_path": status["index_path"],
                "file_count": status["file_count"],
                "symbol_count": status["symbol_count"],
                "import_count": status.get("import_count", 0),
                "files_considered": 0,
                "updated_count": 0,
                "unchanged_count": status["file_count"],
                "removed_count": 0,
                "fts_enabled": status["fts_enabled"],
                "skipped": True,
                "reason": "signature_unchanged",
            }
        result = self.refresh(path=path, max_files=max_files)
        result["skipped"] = False
        result["reason"] = "signature_changed"
        return result

    def refresh_signature(self) -> dict[str, Any]:
        if not (self.config.repo_path / ".git").exists():
            return {
                "schema": "context_index.refresh_signature.v1",
                "available": False,
                "signature": "",
                "source": "none",
            }
        git_head = git_value(self.config.repo_path, "rev-parse", "HEAD")
        status = git_value(self.config.repo_path, "status", "--porcelain=v1", "-z")
        if not git_head:
            return {
                "schema": "context_index.refresh_signature.v1",
                "available": False,
                "signature": "",
                "source": "git",
            }
        status_hash = sha256_text(status)
        return {
            "schema": "context_index.refresh_signature.v1",
            "available": True,
            "signature": f"git:{git_head}:{status_hash}",
            "source": "git",
            "git_head": git_head,
            "git_status_hash": status_hash,
        }

    def _delete_file_rows(
        self, conn: sqlite3.Connection, rel: str, fts_enabled: bool
    ) -> None:
        conn.execute("DELETE FROM files WHERE path = ?", (rel,))
        conn.execute("DELETE FROM symbols WHERE path = ?", (rel,))
        conn.execute("DELETE FROM imports WHERE path = ?", (rel,))
        if fts_enabled:
            try:
                conn.execute("DELETE FROM fts_files WHERE path = ?", (rel,))
            except sqlite3.OperationalError:
                pass

    def _rel_in_refresh_scope(self, rel: str, root: Path) -> bool:
        if root == self.config.repo_path:
            return True
        root_rel = str(root.relative_to(self.config.repo_path)).replace("\\", "/")
        if root.is_file():
            return rel == root_rel
        return rel == root_rel or rel.startswith(root_rel.rstrip("/") + "/")

    def status(self) -> dict[str, Any]:
        with self.connect() as conn:
            fts_enabled = self.initialize(conn)
            file_count = conn.execute("SELECT COUNT(*) AS count FROM files").fetchone()["count"]
            symbol_count = conn.execute("SELECT COUNT(*) AS count FROM symbols").fetchone()["count"]
            import_count = conn.execute("SELECT COUNT(*) AS count FROM imports").fetchone()["count"]
            meta = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM meta")}
        return {
            "schema": "context_index.status.v1",
            "index_path": self.config.display_path(self.config.index_db_path),
            "exists": self.config.index_db_path.exists(),
            "file_count": int(file_count),
            "symbol_count": int(symbol_count),
            "import_count": int(import_count),
            "fts_enabled": fts_enabled,
            "generated_at": meta.get("generated_at", ""),
            "git_head": meta.get("git_head", ""),
            "git_branch": meta.get("git_branch", ""),
            "refresh_signature": meta.get("refresh_signature", ""),
            "refresh_signature_available": meta.get(
                "refresh_signature_available", "false"
            )
            == "true",
        }

    def files(self, limit: int = 1000) -> list[dict[str, Any]]:
        with self.connect() as conn:
            self.initialize(conn)
            rows = conn.execute(
                "SELECT path, size, extension, language, line_count FROM files ORDER BY path LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows if self._runtime_visible_rel(row["path"])]

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
        root_path = self.config.resolve_repo_path(path)
        root_rel = self.config.repo_relative(root_path)
        if root_rel == ".":
            root_rel = ""
        with self.connect() as conn:
            fts_enabled = self._fts_enabled(conn)
            rows: list[dict[str, Any]] = []
            if fts_enabled:
                fts_query = " OR ".join(f'"{term}"' for term in terms)
                try:
                    path_clause = ""
                    params: list[Any] = [fts_query]
                    if root_rel:
                        if root_path.is_file():
                            path_clause = "AND path = ?"
                            params.append(root_rel)
                        else:
                            path_clause = (
                                "AND (path = ? OR path LIKE ? ESCAPE '\\')"
                            )
                            prefix = _sqlite_like_escape(root_rel.rstrip("/"))
                            params.extend([root_rel, f"{prefix}/%"])
                    params.append(max_results * 4)
                    sql_rows = conn.execute(
                        f"""
                        SELECT path, snippet(fts_files, 1, '', '', ' ... ', 12) AS excerpt
                        FROM fts_files
                        WHERE fts_files MATCH ? {path_clause}
                        LIMIT ?
                        """,
                        tuple(params),
                    ).fetchall()
                    rows = [{"path": row["path"], "excerpt": row["excerpt"], "source": "fts"} for row in sql_rows]
                except sqlite3.OperationalError:
                    rows = []
            if not rows:
                rows = self._fallback_search(terms, root_rel=root_rel, limit=max_results * 4)

        filtered: list[dict[str, Any]] = []
        for row in rows:
            rel = row["path"]
            if not self._runtime_visible_rel(rel):
                continue
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
        for candidate in self._iter_candidate_files(root, max_files=5000):
            path = candidate.path
            rel = str(path.relative_to(self.config.repo_path)).replace("\\", "/")
            if is_likely_binary(path):
                continue
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
        symbols = [dict(row) for row in rows if self._runtime_visible_rel(row["path"])]
        return {"schema": "context_symbols.v1", "count": len(symbols), "symbols": symbols}

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
        if should_skip_path(file_path, self.config.repo_path, self.config.state_dir):
            raise ValueError("path is excluded by runtime skip rules")
        stat = file_path.stat()
        rel = self.config.repo_relative(file_path)
        text = self._indexed_text(rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns))
        source = "index" if text is not None else "file"
        if text is None and stat.st_size > self.config.max_read_bytes:
            raise ValueError("file exceeds max_read_bytes")
        if text is None and is_likely_binary(file_path):
            raise ValueError("binary file is not readable as a snippet")
        if text is None:
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
            "source": source,
            "prompt_injection_signals": prompt_injection_signals(content),
        }

    def first_matching_line(self, path: str, terms: list[str]) -> int:
        file_path = self.config.resolve_repo_path(path)
        try:
            stat = file_path.stat()
        except OSError:
            return 1
        rel = self.config.repo_relative(file_path)
        text = self._indexed_text(rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns))
        if text is None:
            try:
                text = file_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                return 1
        for idx, line in enumerate(text.splitlines(), start=1):
            low = line.lower()
            if any(term in low for term in terms):
                return idx
        return 1

    def _indexed_text(self, rel: str, size: int, mtime_ns: int) -> str | None:
        with self.connect() as conn:
            if not self._fts_enabled(conn):
                return None
            row = conn.execute(
                """
                SELECT f.content
                FROM fts_files f
                JOIN files meta ON meta.path = f.path
                WHERE f.path = ?
                  AND meta.size = ?
                  AND meta.mtime_ns = ?
                LIMIT 1
                """,
                (rel, size, mtime_ns),
            ).fetchone()
        if row is None:
            return None
        return str(row["content"])

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

    def _runtime_visible_rel(self, rel_path: str) -> bool:
        try:
            path = self.config.resolve_repo_path(rel_path)
        except ValueError:
            return False
        return path.exists() and not should_skip_path(
            path, self.config.repo_path, self.config.state_dir
        )


def _sqlite_like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
