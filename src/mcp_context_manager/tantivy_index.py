from __future__ import annotations

import re
import shutil
import threading
from contextlib import contextmanager
from importlib import metadata
from pathlib import Path
from typing import Any, Iterable, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - POSIX in supported standalone targets.
    fcntl = None  # type: ignore[assignment]

from .util import sha256_text

TANTIVY_SCHEMA_VERSION = "mcp-context-manager.tantivy.v1"
TANTIVY_SEARCH_MODE = "tantivy"

_PATH_TEXT_RE = re.compile(r"[^A-Za-z0-9_]+")
_SIDECAR_LOCKS: dict[str, threading.RLock] = {}
_SIDECAR_LOCKS_GUARD = threading.Lock()


class TantivySearchIndex:
    def __init__(self, index_dir: Path):
        self.index_dir = index_dir
        self._tantivy: Any | None = None
        self._schema: Any | None = None
        self._index: Any | None = None
        self._lock = threading.Lock()

    def backend_version(self) -> str:
        return f"{TANTIVY_SEARCH_MODE}:{TANTIVY_SCHEMA_VERSION}:{self.package_version()}"

    def package_version(self) -> str:
        try:
            return metadata.version("tantivy")
        except metadata.PackageNotFoundError:
            tantivy = self._module()
            return str(getattr(tantivy, "__version__", "unknown"))

    def engine_version(self) -> str:
        return str(getattr(self._module(), "__version__", "unknown"))

    def status_metadata(self) -> dict[str, Any]:
        return {
            "schema_version": TANTIVY_SCHEMA_VERSION,
            "package_version": self.package_version(),
            "engine_version": self.engine_version(),
            "backend_version": self.backend_version(),
        }

    def is_healthy(self, expected_doc_count: int | None = None) -> bool:
        with self._sidecar_lock(), self._lock:
            try:
                index = self._open_existing()
                doc_count = self._doc_count(index=index)
            except Exception:
                return False
        if expected_doc_count is None:
            return True
        return doc_count == int(expected_doc_count)

    def doc_count(self, index: Any | None = None) -> int:
        with self._sidecar_lock(), self._lock:
            return self._doc_count(index=index)

    def _doc_count(self, index: Any | None = None) -> int:
        index = index or self._open_or_create()
        searcher = index.searcher()
        return int(getattr(searcher, "num_docs", 0) or 0)

    def rebuild(self, rows: Iterable[dict[str, Any]]) -> int:
        with self._sidecar_lock(), self._lock:
            if self.index_dir.exists():
                shutil.rmtree(self.index_dir)
            self.index_dir.mkdir(parents=True, exist_ok=True)
            self._index = self._create_index()
            records = list(rows)
            self._write_full(records)
            return self._doc_count(index=self._index)

    def apply_changes(
        self,
        deleted_paths: Iterable[str],
        records: Iterable[dict[str, Any]],
    ) -> int:
        with self._sidecar_lock(), self._lock:
            index = self._open_or_create()
            writer = index.writer()
            try:
                for rel in sorted({str(path) for path in deleted_paths if str(path)}):
                    writer.delete_documents("path", rel)
                for record in records:
                    doc = self._document(record)
                    if doc is not None:
                        writer.add_document(doc)
                writer.commit()
            except Exception:
                try:
                    writer.rollback()
                finally:
                    raise
            finally:
                del writer
            index.reload()
            return self._doc_count(index=index)

    def search(self, terms: list[str], limit: int) -> list[dict[str, Any]]:
        if not terms:
            return []
        with self._sidecar_lock(), self._lock:
            index = self._open_or_create()
            index.reload()
            query_text = " ".join(terms)
            query = index.parse_query(
                query_text,
                ["path_text", "content"],
                conjunction_by_default=False,
                allow_regexes=False,
            )
            searcher = index.searcher()
            result = searcher.search(query, limit=max(1, int(limit)), count=True)
            rows: list[dict[str, Any]] = []
            for score, address in result.hits:
                doc = searcher.doc(address).to_dict()
                rel = self._first(doc, "path")
                if not rel:
                    continue
                rows.append(
                    {
                        "path": rel,
                        "tantivy_score": float(score),
                        "source": TANTIVY_SEARCH_MODE,
                    }
                )
            return rows

    def _write_full(self, records: list[dict[str, Any]]) -> None:
        index = self._index or self._open_or_create()
        writer = index.writer()
        try:
            writer.delete_all_documents()
            for record in records:
                doc = self._document(record)
                if doc is not None:
                    writer.add_document(doc)
            writer.commit()
        except Exception:
            try:
                writer.rollback()
            finally:
                raise
        finally:
            del writer
        index.reload()

    def _open_existing(self) -> Any:
        tantivy = self._module()
        if not self.index_dir.exists() or not tantivy.Index.exists(str(self.index_dir)):
            raise FileNotFoundError(str(self.index_dir))
        self._index = tantivy.Index.open(str(self.index_dir))
        return self._index

    def _open_or_create(self) -> Any:
        try:
            return self._open_existing()
        except Exception:
            self.index_dir.mkdir(parents=True, exist_ok=True)
            self._index = self._create_index()
            return self._index

    def _create_index(self) -> Any:
        tantivy = self._module()
        return tantivy.Index(self._build_schema(), path=str(self.index_dir))

    def _build_schema(self) -> Any:
        if self._schema is not None:
            return self._schema
        tantivy = self._module()
        builder = tantivy.SchemaBuilder()
        builder.add_text_field(
            "path",
            stored=True,
            tokenizer_name="raw",
            index_option="basic",
        )
        builder.add_text_field("path_text", stored=False)
        builder.add_text_field("content", stored=False)
        builder.add_text_field(
            "language",
            stored=True,
            tokenizer_name="raw",
            index_option="basic",
        )
        builder.add_text_field(
            "sha256",
            stored=True,
            tokenizer_name="raw",
            index_option="basic",
        )
        builder.add_unsigned_field("mtime_ns", stored=True, indexed=False, fast=True)
        self._schema = builder.build()
        return self._schema

    def _document(self, record: dict[str, Any]) -> Any | None:
        rel = str(record.get("path", ""))
        content = record.get("content")
        if not rel or not isinstance(content, str):
            return None
        tantivy = self._module()
        doc = tantivy.Document()
        doc.add_text("path", rel)
        doc.add_text("path_text", self._path_text(rel))
        doc.add_text("content", content)
        doc.add_text("language", str(record.get("language", "")))
        doc.add_text("sha256", str(record.get("sha256", "")))
        try:
            mtime_ns = max(0, int(record.get("mtime_ns", 0) or 0))
        except (TypeError, ValueError):
            mtime_ns = 0
        doc.add_unsigned("mtime_ns", mtime_ns)
        return doc

    def _module(self) -> Any:
        if self._tantivy is None:
            import tantivy

            self._tantivy = tantivy
        return self._tantivy

    def _path_text(self, rel: str) -> str:
        path = Path(rel)
        pieces = [
            rel,
            path.name,
            path.stem,
            _PATH_TEXT_RE.sub(" ", rel),
        ]
        return " ".join(piece for piece in pieces if piece)

    def _first(self, doc: dict[str, Any], field: str) -> str:
        values = doc.get(field, [])
        if isinstance(values, list) and values:
            return str(values[0])
        return str(values or "")

    @contextmanager
    def _sidecar_lock(self) -> Iterator[None]:
        lock_path = self.index_dir.parent / ".tantivy-index.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_key = str(lock_path.resolve())
        with _SIDECAR_LOCKS_GUARD:
            local_lock = _SIDECAR_LOCKS.setdefault(lock_key, threading.RLock())
        with local_lock:
            if fcntl is None:
                yield
                return
            with lock_path.open("a+b") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def tantivy_refresh_signature(schema_version: str, doc_count: int, paths: list[str]) -> str:
    payload = "\n".join([schema_version, str(doc_count), *sorted(paths)])
    return "tantivy:" + sha256_text(payload)
