from __future__ import annotations

import ast
import fnmatch
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .config import ContextConfig
from .store import ContextStore
from .tantivy_index import (
    TANTIVY_SCHEMA_VERSION,
    TANTIVY_SEARCH_MODE,
    TantivySearchIndex,
    tantivy_refresh_signature,
)
from .util import (
    TEXT_EXTENSIONS,
    git_snapshot,
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

TOKEN_RE = re.compile(r"[A-Za-z0-9_]{3,}")
MAX_INDEX_TERMS_PER_FILE = 4000


@dataclass(frozen=True)
class CandidateFile:
    path: Path
    size: int
    mtime_ns: int


class ContextIndex:
    def __init__(self, config: ContextConfig):
        self.config = config
        self.store = ContextStore(config)
        self._status_cache: dict[str, Any] | None = None
        self._status_cache_at = 0.0
        self._status_lock = threading.Lock()
        self._status_refresh_future = None
        self._tantivy = TantivySearchIndex(config.tantivy_index_dir)

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
        existing_rows = self._existing_file_rows(root)
        current_rels = {
            str(candidate.path.relative_to(self.config.repo_path)).replace("\\", "/")
            for candidate in files
        }
        removed_count = 0
        updated_count = 0
        unchanged_count = 0
        previous_generated = self._get_meta("generated_at")
        prepared_updates: list[dict[str, Any]] = []
        prepared_deletes: list[tuple[str, dict[str, Any] | None]] = []
        git = git_snapshot(self.config.repo_path)
        refresh_signature = (
            self._refresh_signature_from_git_or_files(git, files)
            if whole_repo_refresh
            else None
        )

        for rel in sorted(set(existing_rows) - current_rels):
            prepared_deletes.append((rel, existing_rows.get(rel)))
            removed_count += 1
        for candidate in files:
            file_path = candidate.path
            rel = str(file_path.relative_to(self.config.repo_path)).replace("\\", "/")
            existing = existing_rows.get(rel)
            if (
                existing
                and int(existing.get("size", -1)) == candidate.size
                and int(existing.get("mtime_ns", -1)) == candidate.mtime_ns
                and isinstance(existing.get("summary"), dict)
            ):
                unchanged_count += 1
                continue
            if is_likely_binary(file_path):
                if existing:
                    prepared_deletes.append((rel, existing))
                    removed_count += 1
                continue
            raw = file_path.read_bytes()
            text = raw.decode("utf-8", errors="replace")
            digest = sha256_bytes(raw)
            symbols, imports = self.extract_file_intel(rel, text)
            term_rows = self._term_rows(rel, text)
            prepared_updates.append(
                {
                    "rel": rel,
                    "existing": existing,
                    "symbols": symbols,
                    "imports": imports,
                    "term_rows": term_rows,
                    "record": {
                        "path": rel,
                        "size": candidate.size,
                        "mtime_ns": candidate.mtime_ns,
                        "sha256": digest,
                        "extension": file_path.suffix.lower(),
                        "language": language_for_path(rel),
                        "line_count": len(text.splitlines()),
                        "indexed_at": indexed_at,
                        "summary": self._file_summary_payload(
                            rel=rel,
                            text=text,
                            symbols=symbols,
                            size=candidate.size,
                            source="index",
                        ),
                        "content": text,
                        "terms": sorted(term_rows),
                    },
                }
            )
            updated_count += 1

        with self.store.write_txn() as txn:
            for rel, existing in prepared_deletes:
                self._delete_file_rows(rel, existing, txn)
            for update in prepared_updates:
                rel = str(update["rel"])
                self._delete_file_rows(rel, update.get("existing"), txn)
                self.store.put_json(_file_key(rel), update["record"], txn=txn)
                for row in update["symbols"]:
                    self.store.put_json(_symbol_key(row), row, txn=txn)
                for row in update["imports"]:
                    self.store.put_json(_import_key(row), row, txn=txn)
                for term, row in update["term_rows"].items():
                    self.store.put_json(_term_key(term, rel), row, txn=txn)
            generated_at = (
                indexed_at
                if updated_count or removed_count or not previous_generated
                else previous_generated
            )
            self._set_meta("generated_at", generated_at, txn)
            self._set_meta("git_head", str(git.get("git_head", "")), txn)
            self._set_meta("git_branch", str(git.get("git_branch", "")), txn)
            self._set_meta(
                "git_status_hash", str(git.get("git_status_hash", "")), txn
            )
            self._set_meta(
                "git_changes_hash", str(git.get("git_changes_hash", "")), txn
            )
            if whole_repo_refresh:
                signature = refresh_signature or {
                    "available": False,
                    "signature": "",
                }
                self._set_meta("refresh_signature", str(signature["signature"]), txn)
                self._set_meta(
                    "refresh_signature_available",
                    "true" if signature["available"] else "false",
                    txn,
                )

        self._sync_tantivy_sidecar(
            deleted_paths=[
                rel
                for rel, _existing in prepared_deletes
            ]
            + [str(update["rel"]) for update in prepared_updates],
            records=[update["record"] for update in prepared_updates],
        )
        self._invalidate_status_cache()
        status = self.status(use_cache=False)
        return {
            "schema": "context_index.refresh.v1",
            "generated_at": generated_at,
            "index_available": True,
            "file_count": status["file_count"],
            "symbol_count": status["symbol_count"],
            "import_count": status["import_count"],
            "files_considered": len(files),
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "removed_count": removed_count,
            "fts_enabled": True,
            "search_mode": TANTIVY_SEARCH_MODE,
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
        signature = self.refresh_signature(max_files=max_files)
        if not signature["available"]:
            result = self.refresh(path=path, max_files=max_files)
            result["skipped"] = False
            result["reason"] = "signature_unavailable"
            return result
        status = self.status()
        if (
            status["file_count"]
            and status.get("refresh_signature") == signature["signature"]
        ):
            return {
                "schema": "context_index.refresh.v1",
                "generated_at": status["generated_at"],
                "index_available": True,
                "file_count": status["file_count"],
                "symbol_count": status["symbol_count"],
                "import_count": status.get("import_count", 0),
                "files_considered": 0,
                "updated_count": 0,
                "unchanged_count": status["file_count"],
                "removed_count": 0,
                "fts_enabled": status["fts_enabled"],
                "search_mode": status["search_mode"],
                "skipped": True,
                "reason": "signature_unchanged",
            }
        result = self.refresh(path=path, max_files=max_files)
        result["skipped"] = False
        result["reason"] = "signature_changed"
        return result

    def refresh_signature(self, max_files: int = 5000) -> dict[str, Any]:
        git = git_snapshot(self.config.repo_path)
        if git.get("available") and git.get("worktree_matches_path"):
            return self._git_refresh_signature(git)
        files_signature = self._file_metadata_refresh_signature(max_files=max_files)
        if files_signature["available"]:
            files_signature["git_available"] = bool(git.get("available"))
            files_signature["git_worktree_matches_path"] = bool(
                git.get("worktree_matches_path")
            )
        return files_signature

    def _refresh_signature_from_git_or_files(
        self, git: dict[str, Any], files: list[CandidateFile]
    ) -> dict[str, Any]:
        if git.get("available") and git.get("worktree_matches_path"):
            return self._git_refresh_signature(git)
        files_signature = self._file_metadata_refresh_signature_for_candidates(files)
        if files_signature["available"]:
            files_signature["git_available"] = bool(git.get("available"))
            files_signature["git_worktree_matches_path"] = bool(
                git.get("worktree_matches_path")
            )
        return files_signature

    def _git_refresh_signature(self, git: dict[str, Any]) -> dict[str, Any]:
        git_head = str(git.get("git_head", ""))
        status_hash = str(git.get("git_status_hash", ""))
        changes_hash = str(git.get("git_changes_hash", "")) or status_hash
        return {
            "schema": "context_index.refresh_signature.v1",
            "available": True,
            "signature": f"git:{git_head}:{changes_hash}",
            "source": "git",
            "git_head": git_head,
            "git_branch": str(git.get("git_branch", "")),
            "git_status_hash": status_hash,
            "git_changes_hash": changes_hash,
        }

    def _file_metadata_refresh_signature(self, max_files: int = 5000) -> dict[str, Any]:
        try:
            files = self._iter_candidate_files(self.config.repo_path, max_files=max_files)
        except OSError:
            files = []
        return self._file_metadata_refresh_signature_for_candidates(files)

    def _file_metadata_refresh_signature_for_candidates(
        self, files: list[CandidateFile]
    ) -> dict[str, Any]:
        if not files:
            return {
                "schema": "context_index.refresh_signature.v1",
                "available": False,
                "signature": "",
                "source": "none",
            }
        rows = []
        for candidate in files:
            rel = str(candidate.path.relative_to(self.config.repo_path)).replace(
                "\\", "/"
            )
            rows.append(f"{rel}:{candidate.size}:{candidate.mtime_ns}")
        return {
            "schema": "context_index.refresh_signature.v1",
            "available": True,
            "signature": "files:" + sha256_text("\n".join(sorted(rows))),
            "source": "file_metadata",
            "file_count": len(rows),
        }

    def _existing_file_rows(self, root: Path) -> dict[str, dict[str, Any]]:
        if root == self.config.repo_path:
            prefix = "index:file:"
        else:
            root_rel = str(root.relative_to(self.config.repo_path)).replace("\\", "/")
            if root.is_file():
                row = self.store.get_json(_file_key(root_rel))
                return {root_rel: row} if isinstance(row, dict) else {}
            prefix = f"index:file:{root_rel.rstrip('/')}/"
        return {
            str(row.get("path")): row
            for _key, row in self.store.iter_json(prefix)
            if isinstance(row, dict)
            and self._rel_in_refresh_scope(str(row.get("path")), root)
        }

    def _delete_file_rows(
        self, rel: str, existing: dict[str, Any] | None, txn: Any
    ) -> None:
        terms = self._existing_terms(rel, existing)
        legacy_terms_unknown = isinstance(existing, dict) and not terms
        self.store.delete(_file_key(rel), txn=txn)
        self.store.delete_prefix(_symbol_prefix(rel), txn=txn)
        self.store.delete_prefix(_import_prefix(rel), txn=txn)
        if terms:
            for term in terms:
                self.store.delete(_term_key(term, rel), txn=txn)
            return
        if legacy_terms_unknown:
            return

    def _existing_terms(self, rel: str, existing: dict[str, Any] | None) -> list[str]:
        if not isinstance(existing, dict):
            return []
        terms = existing.get("terms")
        if isinstance(terms, list):
            return [str(term) for term in terms]
        content = existing.get("content")
        if isinstance(content, str) and content:
            return sorted(self._term_rows(rel, content))
        return []

    def _rel_in_refresh_scope(self, rel: str, root: Path) -> bool:
        if root == self.config.repo_path:
            return True
        root_rel = str(root.relative_to(self.config.repo_path)).replace("\\", "/")
        if root.is_file():
            return rel == root_rel
        return rel == root_rel or rel.startswith(root_rel.rstrip("/") + "/")

    def stored_refresh_signature(self) -> tuple[str, bool]:
        signature = self._get_meta("refresh_signature")
        available = self._get_meta("refresh_signature_available") == "true"
        return signature, bool(signature and available)

    def status(self, use_cache: bool = True, allow_stale: bool = True) -> dict[str, Any]:
        now = time.time()
        stale_status: dict[str, Any] | None = None
        if use_cache:
            with self._status_lock:
                if (
                    self._status_cache is not None
                    and now - self._status_cache_at < 1.0
                ):
                    return dict(self._status_cache)
                if self._status_cache is not None:
                    stale_status = dict(self._status_cache)
        if use_cache and stale_status is not None and allow_stale:
            self._refresh_status_in_background()
            return stale_status
        status = self._compute_status()
        return status

    def _refresh_status_in_background(self) -> None:
        try:
            from .runtime import io_executor
        except Exception:
            return
        with self._status_lock:
            future = self._status_refresh_future
            if future is not None and not future.done():
                return
            self._status_refresh_future = io_executor().submit(self._compute_status)

    def _compute_status(self) -> dict[str, Any]:
        meta = self._meta()
        tantivy_meta = self._tantivy.status_metadata()
        file_count = self.store.count("index:file:")
        symbol_count = self.store.count("index:symbol:")
        import_count = self.store.count("index:import:")
        status = {
            "schema": "context_index.status.v1",
            "index_available": self.store.exists(),
            "exists": self.store.exists(),
            "file_count": int(file_count),
            "symbol_count": int(symbol_count),
            "import_count": int(import_count),
            "fts_enabled": True,
            "search_mode": TANTIVY_SEARCH_MODE,
            "search_backend_version": tantivy_meta["backend_version"],
            "tantivy": {
                **tantivy_meta,
                "doc_count": int(meta.get("tantivy_doc_count", "0") or 0),
                "refresh_signature": meta.get("tantivy_refresh_signature", ""),
                "sidecar_signature": meta.get("tantivy_sidecar_signature", ""),
            },
            "generated_at": meta.get("generated_at", ""),
            "git_head": meta.get("git_head", ""),
            "git_branch": meta.get("git_branch", ""),
            "git_status_hash": meta.get("git_status_hash", ""),
            "git_changes_hash": meta.get("git_changes_hash", ""),
            "refresh_signature": meta.get("refresh_signature", ""),
            "refresh_signature_available": meta.get(
                "refresh_signature_available", "false"
            )
            == "true",
        }
        with self._status_lock:
            self._status_cache = dict(status)
            self._status_cache_at = time.time()
            future = self._status_refresh_future
            if future is not None and future.done():
                self._status_refresh_future = None
        return status

    def _invalidate_status_cache(self) -> None:
        with self._status_lock:
            self._status_cache = None
            self._status_cache_at = 0.0

    def files(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = []
        for _key, row in self.store.iter_json("index:file:"):
            if not isinstance(row, dict):
                continue
            if not self._runtime_visible_rel(str(row.get("path", ""))):
                continue
            rows.append(
                {
                    "path": row.get("path", ""),
                    "size": int(row.get("size", 0)),
                    "extension": row.get("extension", ""),
                    "language": row.get("language", ""),
                    "line_count": int(row.get("line_count", 0)),
                }
            )
        rows.sort(key=lambda item: str(item["path"]))
        return rows[:limit]

    def file_summary(
        self,
        path: str,
        max_chars: int = 480,
        matched_line: int | None = None,
    ) -> dict[str, Any]:
        file_path = self.config.resolve_repo_path(path)
        if not file_path.is_file():
            raise FileNotFoundError(path)
        if should_skip_path(file_path, self.config.repo_path, self.config.state_dir):
            raise ValueError("path is excluded by runtime skip rules")
        stat = file_path.stat()
        rel = self.config.repo_relative(file_path)
        row = self._indexed_row(rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns))
        if matched_line is not None:
            text = row.get("content") if row else None
            if not isinstance(text, str):
                if stat.st_size > self.config.max_read_bytes:
                    raise ValueError("file exceeds max_read_bytes")
                if is_likely_binary(file_path):
                    raise ValueError("binary file is not readable")
                text = file_path.read_text(encoding="utf-8", errors="replace")
            symbols, _imports = self.extract_file_intel(rel, text)
            summary = self._file_summary_payload(
                rel=rel,
                text=text,
                symbols=symbols,
                size=int(stat.st_size),
                source="index" if row else "file",
                anchor_line=max(1, int(matched_line)),
            )
        elif row and isinstance(row.get("summary"), dict):
            summary = dict(row["summary"])
            summary["source"] = "index"
        else:
            if stat.st_size > self.config.max_read_bytes:
                raise ValueError("file exceeds max_read_bytes")
            if is_likely_binary(file_path):
                raise ValueError("binary file is not readable")
            text = file_path.read_text(encoding="utf-8", errors="replace")
            symbols, _imports = self.extract_file_intel(rel, text)
            summary = self._file_summary_payload(
                rel=rel,
                text=text,
                symbols=symbols,
                size=int(stat.st_size),
                source="file",
            )

        excerpt = str(summary.get("excerpt") or summary.get("content") or "")
        excerpt, truncated = trim_text(excerpt, max_chars)
        summary["excerpt"] = excerpt
        summary["content"] = excerpt
        summary["truncated"] = bool(summary.get("truncated", False) or truncated)
        summary["schema"] = "context_file_summary.v1"
        summary["prompt_injection_signals"] = prompt_injection_signals(excerpt)
        return summary

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
                        "line_end": int(
                            getattr(node, "end_lineno", getattr(node, "lineno", 1))
                        ),
                        "signature": signature,
                    }
                )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(
                        {
                            "path": rel_path,
                            "target": alias.name.split(".", 1)[0],
                            "line": int(node.lineno),
                        }
                    )
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(
                    {
                        "path": rel_path,
                        "target": node.module.split(".", 1)[0],
                        "line": int(node.lineno),
                    }
                )
        return symbols, imports

    def _extract_generic_symbols(self, rel_path: str, text: str) -> list[dict[str, Any]]:
        symbols: list[dict[str, Any]] = []
        patterns = [
            ("function", re.compile(r"\bfunction\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")),
            ("function", re.compile(r"\bdef\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")),
            (
                "class",
                re.compile(r"\b(class|struct|interface)\s+([A-Za-z_][A-Za-z0-9_]*)"),
            ),
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
        allow_fallback: bool = True,
    ) -> dict[str, Any]:
        terms = normalize_query_terms(query, max_terms=8)
        if not terms:
            raise ValueError("query must contain at least one searchable term")
        root_rel = self._search_root_rel(path)
        rows = self._tantivy_search_rows(
            terms=terms,
            query=query,
            root_rel=root_rel,
            max_results=max_results,
            include_globs=include_globs,
        )
        return {
            "schema": "context_search.v1",
            "query": query,
            "terms": terms,
            "count": len(rows),
            "results": rows,
            "index": self.status(),
        }

    def search_fragment(
        self,
        term: str,
        path: str = ".",
        max_results: int = 20,
        include_globs: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        terms = normalize_query_terms(term, max_terms=1)
        if not terms:
            raise ValueError("term must contain at least one searchable term")
        root_rel = self._search_root_rel(path)
        return self._tantivy_search_rows(
            terms=terms,
            query=term,
            root_rel=root_rel,
            max_results=max_results,
            include_globs=include_globs,
        )

    def _search_root_rel(self, path: str) -> str:
        root_path = self.config.resolve_repo_path(path)
        root_rel = self.config.repo_relative(root_path)
        return "" if root_rel == "." else root_rel

    def search_backend_version(self) -> str:
        return self._tantivy.backend_version()

    def _path_like_query_rel(self, query: str | None) -> str:
        query_path = (query or "").strip().replace("\\", "/").strip()
        normalized = re.sub(r"/+", "/", query_path).strip("./")
        if not normalized:
            return ""
        normalized_lower = normalized.lower()
        if not (
            "/" in normalized_lower
            or "\\" in query_path
            or re.search(r"\.[A-Za-z0-9_]{1,12}$", normalized_lower)
        ):
            return ""
        return normalized

    def _tantivy_search_rows(
        self,
        terms: list[str],
        root_rel: str,
        max_results: int,
        include_globs: list[str] | None,
        query: str | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_tantivy_available()
        rows: list[dict[str, Any]] = []
        candidate_limit = self._tantivy_candidate_limit(max_results)
        scoped = bool(root_rel or include_globs)
        max_candidate_limit = (
            max(candidate_limit, self.store.count("index:file:"))
            if scoped
            else candidate_limit
        )
        seen_paths: set[str] = set()
        while True:
            hits = self._tantivy.search(terms, limit=candidate_limit)
            for hit in hits:
                rel = str(hit.get("path", ""))
                if not rel or rel in seen_paths:
                    continue
                seen_paths.add(rel)
                if root_rel and rel != root_rel and not rel.startswith(
                    root_rel.rstrip("/") + "/"
                ):
                    continue
                if include_globs and not any(
                    fnmatch.fnmatch(rel, glob) for glob in include_globs
                ):
                    continue
                row = self._tantivy_lmdb_search_row(
                    rel=rel,
                    terms=terms,
                    tantivy_score=float(hit.get("tantivy_score", 0.0) or 0.0),
                )
                if row is not None:
                    rows.append(row)
            if (
                not scoped
                or len(rows) >= max_results
                or len(hits) < candidate_limit
                or candidate_limit >= max_candidate_limit
            ):
                break
            next_limit = min(max_candidate_limit, max(candidate_limit * 2, candidate_limit + 200))
            if next_limit <= candidate_limit:
                break
            candidate_limit = next_limit
        exact_rel = self._path_like_query_rel(query)
        exact_rel_lower = exact_rel.lower()
        if exact_rel and not any(
            str(row.get("path", "")).lower() == exact_rel_lower for row in rows
        ):
            exact_row = self._tantivy_lmdb_search_row(
                rel=exact_rel,
                terms=terms,
                tantivy_score=10_000.0,
            )
            if exact_row is not None:
                rows.append(exact_row)
        return self._rank_search_rows(
            rows=rows,
            terms=terms,
            query=query,
            root_rel=root_rel,
            max_results=max_results,
            include_globs=include_globs,
        )

    def _tantivy_candidate_limit(self, max_results: int) -> int:
        requested = max(1, int(max_results))
        return min(max(requested * 50, 200), 2000)

    def _tantivy_lmdb_search_row(
        self, rel: str, terms: list[str], tantivy_score: float
    ) -> dict[str, Any] | None:
        indexed = self.store.get_json(_file_key(rel), {})
        if not isinstance(indexed, dict):
            return None
        content = indexed.get("content")
        if not isinstance(content, str):
            return None
        line, excerpt = self._first_matching_excerpt(rel, content, terms)
        haystack = f"{rel}\n{content}".lower()
        unique_terms = sorted(set(terms))
        term_hits = sum(1 for term in unique_terms if term in haystack)
        term_count = sum(haystack.count(term) for term in unique_terms)
        return {
            "path": rel,
            "line": line,
            "excerpt": excerpt,
            "source": TANTIVY_SEARCH_MODE,
            "term_hits": term_hits,
            "term_count": term_count,
            "tantivy_score": round(float(tantivy_score), 6),
        }

    def _first_matching_excerpt(
        self, rel: str, content: str, terms: list[str]
    ) -> tuple[int, str]:
        for idx, line in enumerate(content.splitlines(), start=1):
            low = line.lower()
            if any(term in low for term in terms):
                return idx, line.strip()[:240]
        indexed = self.store.get_json(_file_key(rel), {})
        summary = indexed.get("summary", {}) if isinstance(indexed, dict) else {}
        if isinstance(summary, dict) and summary.get("excerpt"):
            return int(summary.get("start_line", 1) or 1), str(summary["excerpt"])[:240]
        return 1, rel

    def _ensure_tantivy_available(self) -> None:
        expected_doc_count = self.store.count("index:file:")
        if self._tantivy_meta_current(expected_doc_count) and self._tantivy.is_healthy(
            expected_doc_count=expected_doc_count
        ):
            return
        self._rebuild_tantivy_from_lmdb()

    def _sync_tantivy_sidecar(
        self,
        deleted_paths: list[str],
        records: list[dict[str, Any]],
    ) -> None:
        expected_doc_count = self.store.count("index:file:")
        if (
            not deleted_paths
            and not records
            and self._tantivy_meta_current(expected_doc_count)
            and self._tantivy.is_healthy(expected_doc_count=expected_doc_count)
        ):
            return
        if not self._tantivy_meta_compatible() or not self._tantivy.is_healthy():
            doc_count = self._rebuild_tantivy_from_lmdb()
        elif not deleted_paths and not records:
            doc_count = self._rebuild_tantivy_from_lmdb()
        else:
            doc_count = self._tantivy.apply_changes(
                deleted_paths=deleted_paths,
                records=records,
            )
            if doc_count != self.store.count("index:file:"):
                doc_count = self._rebuild_tantivy_from_lmdb()
        self._store_tantivy_meta(doc_count)

    def _rebuild_tantivy_from_lmdb(self) -> int:
        records = self._all_file_records()
        doc_count = self._tantivy.rebuild(records)
        self._store_tantivy_meta(doc_count)
        return doc_count

    def _all_file_records(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for _key, row in self.store.iter_json("index:file:"):
            if not isinstance(row, dict):
                continue
            if not isinstance(row.get("content"), str):
                continue
            rows.append(row)
        rows.sort(key=lambda row: str(row.get("path", "")))
        return rows

    def _tantivy_meta_current(self, expected_doc_count: int) -> bool:
        if not self._tantivy_meta_compatible():
            return False
        meta = self._meta()
        try:
            return int(meta.get("tantivy_doc_count", "0") or 0) == int(
                expected_doc_count
            )
        except (TypeError, ValueError):
            return False

    def _tantivy_meta_compatible(self) -> bool:
        meta = self._meta()
        if meta.get("tantivy_schema_version", "") != TANTIVY_SCHEMA_VERSION:
            return False
        if meta.get("tantivy_package_version", "") != self._tantivy.package_version():
            return False
        return meta.get("tantivy_backend_version", "") == self._tantivy.backend_version()

    def _store_tantivy_meta(self, doc_count: int) -> None:
        metadata = self._tantivy.status_metadata()
        refresh_signature, refresh_signature_available = self.stored_refresh_signature()
        sidecar_signature = tantivy_refresh_signature(
            TANTIVY_SCHEMA_VERSION,
            int(doc_count),
            [refresh_signature] if refresh_signature else [],
        )
        with self.store.write_txn() as txn:
            self._set_meta("tantivy_schema_version", TANTIVY_SCHEMA_VERSION, txn)
            self._set_meta("tantivy_package_version", metadata["package_version"], txn)
            self._set_meta("tantivy_engine_version", metadata["engine_version"], txn)
            self._set_meta("tantivy_backend_version", metadata["backend_version"], txn)
            self._set_meta("tantivy_doc_count", str(int(doc_count)), txn)
            self._set_meta("tantivy_refresh_signature", refresh_signature, txn)
            self._set_meta(
                "tantivy_refresh_signature_available",
                "true" if refresh_signature_available else "false",
                txn,
            )
            self._set_meta("tantivy_sidecar_signature", sidecar_signature, txn)

    def _rank_search_rows(
        self,
        rows: list[dict[str, Any]],
        terms: list[str],
        query: str | None,
        root_rel: str,
        max_results: int,
        include_globs: list[str] | None,
    ) -> list[dict[str, Any]]:
        normalized_path_query = self._path_like_query_rel(query).lower()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            rel = row["path"]
            rel_lower = str(rel).lower()
            if not self._runtime_visible_rel(rel):
                continue
            if root_rel and not rel.startswith(root_rel.rstrip("/") + "/") and rel != root_rel:
                continue
            if include_globs and not any(fnmatch.fnmatch(rel, glob) for glob in include_globs):
                continue
            score = float(row.get("tantivy_score", 0.0) or 0.0)
            if normalized_path_query and rel_lower == normalized_path_query:
                score += 10_000.0
            score += sum(2.0 for term in terms if term in rel_lower)
            score += sum(
                1.0 for term in terms if term in str(row.get("excerpt", "")).lower()
            )
            score += float(row.get("term_hits", 0)) * 2.5
            score += min(float(row.get("term_count", 0)), 8.0) * 0.25
            public_row = {key: value for key, value in row.items() if key != "_base_score"}
            public_row["score"] = round(score, 4)
            public_row["terms"] = terms
            filtered.append(public_row)
        filtered.sort(key=lambda item: (-float(item["score"]), item["path"]))
        return filtered[:max_results]

    def symbols(self, query: str = "", limit: int = 50) -> dict[str, Any]:
        terms = normalize_query_terms(query, max_terms=8)
        rows = []
        for _key, row in self.store.iter_json("index:symbol:"):
            if not isinstance(row, dict):
                continue
            if not self._runtime_visible_rel(str(row.get("path", ""))):
                continue
            if terms:
                haystack = " ".join(
                    [
                        str(row.get("path", "")),
                        str(row.get("name", "")),
                        str(row.get("signature", "")),
                    ]
                ).lower()
                if not any(term in haystack for term in terms):
                    continue
            rows.append(row)
        rows.sort(key=lambda item: (str(item.get("path", "")), int(item.get("line_start", 0))))
        symbols = rows[:limit]
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
        text = self._indexed_text(
            rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns)
        )
        source = "index" if text is not None else "file"
        if text is None and stat.st_size > self.config.max_read_bytes:
            raise ValueError("file exceeds max_read_bytes")
        if text is None and is_likely_binary(file_path):
            raise ValueError("binary file is not readable as a snippet")
        if text is None:
            text = file_path.read_text(encoding="utf-8", errors="replace")
        return self._snippet_payload(
            rel=rel,
            text=text,
            source=source,
            start_line=start_line,
            end_line=end_line,
            context_before=context_before,
            context_after=context_after,
            max_chars=max_chars,
        )

    def snippet_batch(
        self, requests: list[dict[str, Any]], indexed_only: bool = True
    ) -> list[dict[str, Any]]:
        text_cache: dict[str, tuple[str, str]] = {}
        results: list[dict[str, Any]] = []
        for request in requests:
            path = str(request.get("path", ""))
            try:
                start_line = int(request.get("start_line", 1) or 1)
                if start_line < 1:
                    raise ValueError("start_line must be >= 1")
                file_path = self.config.resolve_repo_path(path)
                if not file_path.is_file():
                    raise FileNotFoundError(path)
                if should_skip_path(file_path, self.config.repo_path, self.config.state_dir):
                    raise ValueError("path is excluded by runtime skip rules")
                stat = file_path.stat()
                rel = self.config.repo_relative(file_path)
                cache_key = f"{rel}:{int(stat.st_size)}:{int(stat.st_mtime_ns)}"
                if cache_key not in text_cache:
                    request_indexed_only = bool(request.get("indexed_only", indexed_only))
                    text = self._indexed_text(
                        rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns)
                    )
                    source = "index"
                    if text is None:
                        if request_indexed_only:
                            raise ValueError("indexed content unavailable")
                        if stat.st_size > self.config.max_read_bytes:
                            raise ValueError("file exceeds max_read_bytes")
                        if is_likely_binary(file_path):
                            raise ValueError("binary file is not readable as a snippet")
                        text = file_path.read_text(encoding="utf-8", errors="replace")
                        source = "file"
                    text_cache[cache_key] = (text, source)
                text, source = text_cache[cache_key]
                results.append(
                    self._snippet_payload(
                        rel=rel,
                        text=text,
                        source=source,
                        start_line=start_line,
                        end_line=request.get("end_line"),
                        context_before=int(request.get("context_before", 0) or 0),
                        context_after=int(request.get("context_after", 0) or 0),
                        max_chars=int(request.get("max_chars", 4000) or 4000),
                    )
                )
            except Exception as exc:
                results.append(
                    {
                        "schema": "context_snippet.error.v1",
                        "path": path,
                        "error": type(exc).__name__,
                    }
                )
        return results

    def indexed_path_current(self, path: str) -> bool:
        root = self.config.resolve_repo_path(path)
        if not root.exists():
            return False
        if root.is_dir():
            root_rel = self.config.repo_relative(root)
            prefix = "" if root_rel == "." else root_rel.rstrip("/") + "/"
            for _key, row in self.store.iter_json("index:file:"):
                if not isinstance(row, dict):
                    continue
                rel = str(row.get("path", ""))
                if not prefix or rel.startswith(prefix):
                    return True
            return False
        if not root.is_file():
            return False
        if should_skip_path(root, self.config.repo_path, self.config.state_dir):
            return False
        stat = root.stat()
        rel = self.config.repo_relative(root)
        return (
            self._indexed_text(rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns))
            is not None
        )

    def _snippet_payload(
        self,
        rel: str,
        text: str,
        source: str,
        start_line: int,
        end_line: Any,
        context_before: int,
        context_after: int,
        max_chars: int,
    ) -> dict[str, Any]:
        lines = text.splitlines()
        total = len(lines)
        requested_end = int(end_line) if end_line is not None else start_line
        start = max(1, start_line - max(0, context_before))
        end = min(total, requested_end + max(0, context_after))
        if end < start:
            end = start
        content = "\n".join(lines[start - 1 : end])
        content, truncated = trim_text(content, max_chars)
        content, redactions = redact_text(content)
        return {
            "schema": "context_snippet.v1",
            "path": rel,
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
        text = self._indexed_text(
            rel, size=int(stat.st_size), mtime_ns=int(stat.st_mtime_ns)
        )
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
        row = self._indexed_row(rel, size=size, mtime_ns=mtime_ns)
        if not row:
            return None
        content = row.get("content")
        return content if isinstance(content, str) else None

    def _indexed_row(self, rel: str, size: int, mtime_ns: int) -> dict[str, Any] | None:
        row = self.store.get_json(_file_key(rel), {})
        if not isinstance(row, dict):
            return None
        if int(row.get("size", -1)) != size:
            return None
        if int(row.get("mtime_ns", -1)) != mtime_ns:
            return None
        return row

    def tree(
        self, path: str = ".", max_entries: int = 200, max_depth: int = 2
    ) -> dict[str, Any]:
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
                    "path": str(candidate.relative_to(self.config.repo_path)).replace(
                        "\\", "/"
                    ),
                    "type": "dir" if candidate.is_dir() else "file",
                    "size": int(candidate.stat().st_size) if candidate.is_file() else 0,
                }
            )
        return {
            "schema": "context_tree.v1",
            "path": self.config.repo_relative(root),
            "count": len(entries),
            "entries": entries,
        }

    def workspace_facts(self) -> dict[str, Any]:
        files = self.files(limit=10000)
        git = git_snapshot(self.config.repo_path)
        ext_counts: dict[str, int] = {}
        for row in files:
            ext = row.get("extension") or "[none]"
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
        top_extensions = [
            {"extension": ext, "count": count}
            for ext, count in sorted(
                ext_counts.items(), key=lambda item: (-item[1], item[0])
            )[:8]
        ]
        return {
            "schema": "workspace_facts.v1",
            "file_count": len(files),
            "top_extensions": top_extensions,
            "has_tests_dir": (self.config.repo_path / "tests").is_dir()
            or (self.config.repo_path / "test").is_dir(),
            "has_readme": any(
                (self.config.repo_path / name).is_file()
                for name in ["README.md", "README.rst", "README.txt"]
            ),
            "is_git_repo": bool(git.get("is_git_repo")),
            "git_branch": str(git.get("git_branch", "")),
            "git_head": str(git.get("git_head_short", "")),
            "index": self.status(),
        }

    def _file_summary_payload(
        self,
        rel: str,
        text: str,
        symbols: list[dict[str, Any]],
        size: int,
        source: str,
        anchor_line: int | None = None,
    ) -> dict[str, Any]:
        lines = text.splitlines()
        if anchor_line is not None:
            first_line = max(1, min(anchor_line, max(1, len(lines))) - 2)
            end_line = min(len(lines), first_line + 5)
            picked = lines[first_line - 1 : end_line]
        else:
            first_line = 1
            picked = []
            for idx, line in enumerate(lines, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                if not picked:
                    first_line = idx
                picked.append(stripped)
                if len("\n".join(picked)) >= 480 or len(picked) >= 4:
                    break
            end_line = min(len(lines), first_line + max(0, len(picked) - 1))
        excerpt_raw = "\n".join(picked)
        excerpt_raw, truncated = trim_text(excerpt_raw, 480)
        excerpt, redactions = redact_text(excerpt_raw)
        title_hint = Path(rel).name
        if symbols:
            title_hint = str(symbols[0].get("signature") or symbols[0].get("name") or title_hint)
        elif picked:
            title_hint = picked[0][:160]
        return {
            "schema": "context_file_summary.v1",
            "path": rel,
            "start_line": first_line,
            "end_line": end_line,
            "title_hint": title_hint[:160],
            "excerpt": excerpt,
            "content": excerpt,
            "line_count": len(lines),
            "language": language_for_path(rel),
            "size": int(size),
            "source_chars": len(text),
            "source": source,
            "truncated": truncated,
            "redactions": redactions,
            "prompt_injection_signals": prompt_injection_signals(excerpt),
        }

    def _term_rows(self, rel: str, text: str) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for idx, line in enumerate(text.splitlines(), start=1):
            for term in _tokens(line):
                row = rows.get(term)
                if row is None:
                    rows[term] = {
                        "path": rel,
                        "term": term,
                        "first_line": idx,
                        "excerpt": line.strip()[:240],
                        "count": 1,
                    }
                else:
                    row["count"] = int(row.get("count", 0)) + 1
            if len(rows) >= MAX_INDEX_TERMS_PER_FILE:
                break
        for term in _tokens(rel.replace("/", " ").replace(".", " ")):
            row = rows.get(term)
            if row is None:
                rows[term] = {
                    "path": rel,
                    "term": term,
                    "first_line": 1,
                    "excerpt": rel,
                    "count": 1,
                }
            else:
                row["count"] = int(row.get("count", 0)) + 1
        return rows

    def _meta(self) -> dict[str, str]:
        meta: dict[str, str] = {}
        for key, value in self.store.iter_json("index:meta:"):
            if isinstance(value, str):
                meta[key.removeprefix("index:meta:")] = value
            else:
                meta[key.removeprefix("index:meta:")] = str(value)
        return meta

    def _get_meta(self, key: str) -> str:
        value = self.store.get_json(f"index:meta:{key}", "")
        return value if isinstance(value, str) else str(value or "")

    def _set_meta(self, key: str, value: str, txn: Any) -> None:
        self.store.put_json(f"index:meta:{key}", value or "", txn=txn)

    def _runtime_visible_rel(self, rel_path: str) -> bool:
        try:
            path = self.config.resolve_repo_path(rel_path)
        except ValueError:
            return False
        return path.exists() and not should_skip_path(
            path, self.config.repo_path, self.config.state_dir
        )


def _tokens(text: str) -> Iterator[str]:
    for token in TOKEN_RE.findall(text.lower()):
        if token.isdigit():
            continue
        yield token


def _rel_token(rel: str) -> str:
    return sha256_text(rel)[:24]


def _file_key(rel: str) -> str:
    return f"index:file:{rel}"


def _symbol_prefix(rel: str) -> str:
    return f"index:symbol:{_rel_token(rel)}:"


def _symbol_key(row: dict[str, Any]) -> str:
    rel = str(row.get("path", ""))
    line = int(row.get("line_start", 0))
    name = str(row.get("name", ""))
    kind = str(row.get("kind", ""))
    digest = sha256_text(f"{rel}:{kind}:{name}:{line}")[:16]
    return f"{_symbol_prefix(rel)}{line:010d}:{digest}"


def _import_prefix(rel: str) -> str:
    return f"index:import:{_rel_token(rel)}:"


def _import_key(row: dict[str, Any]) -> str:
    rel = str(row.get("path", ""))
    line = int(row.get("line", 0))
    target = str(row.get("target", ""))
    digest = sha256_text(f"{rel}:{target}:{line}")[:16]
    return f"{_import_prefix(rel)}{line:010d}:{digest}"


def _term_prefix(term: str) -> str:
    return f"index:term:{term}:"


def _term_key(term: str, rel: str) -> str:
    return f"{_term_prefix(term)}{rel}"
