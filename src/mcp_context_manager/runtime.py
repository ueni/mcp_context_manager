from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from .util import now_iso

_SERVICE_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="mcp-service",
)
_IO_EXECUTOR = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="mcp-io",
)
_CPU_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="mcp-cpu",
)


def service_executor() -> ThreadPoolExecutor:
    return _SERVICE_EXECUTOR


def io_executor() -> ThreadPoolExecutor:
    return _IO_EXECUTOR


def cpu_executor() -> ThreadPoolExecutor:
    return _CPU_EXECUTOR


class BackgroundJobRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[tuple[str, str], Future[Any]] = {}
        self._state: dict[tuple[str, str], dict[str, Any]] = {}

    def submit(
        self,
        project_id: str,
        kind: str,
        work: Callable[[], Any],
        *,
        executor: ThreadPoolExecutor | None = None,
        min_interval_seconds: float = 0.0,
    ) -> dict[str, Any]:
        key = (project_id, kind)
        now = time.time()
        with self._lock:
            future = self._jobs.get(key)
            if future is not None and not future.done():
                state = self._state.setdefault(key, {})
                state["deduplicated_count"] = int(state.get("deduplicated_count", 0)) + 1
                return self._public_job(project_id, kind, future, state, deduplicated=True)
            state = self._state.setdefault(key, {})
            last_started = float(state.get("last_started_monotonic", 0.0) or 0.0)
            if (
                min_interval_seconds > 0
                and last_started
                and now - last_started < min_interval_seconds
            ):
                return {
                    "project_id": project_id,
                    "kind": kind,
                    "status": "throttled",
                    "pending": False,
                    "deduplicated": False,
                    "last_started_at": state.get("last_started_at", ""),
                    "last_completed_at": state.get("last_completed_at", ""),
                    "last_error": state.get("last_error", ""),
                }
            state.update(
                {
                    "status": "queued",
                    "pending": True,
                    "last_started_monotonic": now,
                    "last_started_at": now_iso(),
                    "last_error": "",
                }
            )
            future = (executor or _IO_EXECUTOR).submit(self._run, key, work)
            self._jobs[key] = future
            return self._public_job(project_id, kind, future, state, deduplicated=False)

    def _run(self, key: tuple[str, str], work: Callable[[], Any]) -> Any:
        with self._lock:
            state = self._state.setdefault(key, {})
            state["status"] = "running"
            state["pending"] = True
        try:
            result = work()
        except Exception as exc:
            with self._lock:
                state = self._state.setdefault(key, {})
                state["status"] = "failed"
                state["pending"] = False
                state["last_completed_at"] = now_iso()
                state["last_error"] = type(exc).__name__
            raise
        with self._lock:
            state = self._state.setdefault(key, {})
            state["status"] = "complete"
            state["pending"] = False
            state["last_completed_at"] = now_iso()
            state["last_error"] = ""
            if isinstance(result, dict):
                state["last_result"] = {
                    name: result.get(name)
                    for name in (
                        "schema",
                        "skipped",
                        "reason",
                        "file_count",
                        "updated_count",
                        "unchanged_count",
                        "removed_count",
                    )
                    if name in result
                }
        return result

    def status(self, project_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            rows: list[dict[str, Any]] = []
            for (row_project_id, kind), state in sorted(self._state.items()):
                if project_id and row_project_id != project_id:
                    continue
                future = self._jobs.get((row_project_id, kind))
                rows.append(
                    self._public_job(row_project_id, kind, future, state, deduplicated=False)
                )
        pending = [row for row in rows if row["pending"]]
        return {
            "schema": "background_jobs.v1",
            "project_id": project_id or "",
            "active_count": len(pending),
            "queue_depth": len(pending),
            "jobs": rows,
        }

    def _public_job(
        self,
        project_id: str,
        kind: str,
        future: Future[Any] | None,
        state: dict[str, Any],
        *,
        deduplicated: bool,
    ) -> dict[str, Any]:
        pending = bool(future is not None and not future.done())
        status = str(state.get("status") or ("running" if pending else "idle"))
        if pending and status in {"queued", "running"}:
            public_status = status
        elif future is not None and future.done() and future.exception() is not None:
            public_status = "failed"
        else:
            public_status = status
        return {
            "project_id": project_id,
            "kind": kind,
            "status": public_status,
            "pending": pending,
            "deduplicated": deduplicated,
            "deduplicated_count": int(state.get("deduplicated_count", 0) or 0),
            "last_started_at": state.get("last_started_at", ""),
            "last_completed_at": state.get("last_completed_at", ""),
            "last_error": state.get("last_error", ""),
            "last_result": state.get("last_result", {}),
        }


background_jobs = BackgroundJobRegistry()
