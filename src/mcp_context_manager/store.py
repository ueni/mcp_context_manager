from __future__ import annotations

import json
import threading
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, Iterator, Protocol

import lmdb

from .config import ContextConfig


class StoreAdapter(Protocol):
    @property
    def path(self) -> Path:
        ...

    def exists(self) -> bool:
        ...

    def write_txn(self) -> AbstractContextManager[Any]:
        ...

    def get_bytes(self, key: bytes, txn: Any = None) -> bytes | None:
        ...

    def put_bytes(self, key: bytes, value: bytes, txn: Any = None) -> None:
        ...

    def delete_key(self, key: bytes, txn: Any = None) -> None:
        ...

    def iter_raw(self, prefix: bytes, txn: Any = None) -> Iterator[tuple[bytes, bytes]]:
        ...

    def count_raw(self, prefix: bytes, txn: Any = None) -> int:
        ...


class LmdbStoreAdapter:
    _envs: dict[Path, Any] = {}
    _env_locks: dict[Path, threading.Lock] = {}
    _guard = threading.Lock()

    def __init__(self, config: ContextConfig):
        self.config = config

    @property
    def path(self) -> Path:
        return self.config.store_path

    def exists(self) -> bool:
        return self.path.exists()

    @classmethod
    def close_path(cls, path: Path) -> None:
        resolved = path.resolve()
        with cls._guard:
            env = cls._envs.pop(resolved, None)
            cls._env_locks.pop(resolved, None)
        if env is not None:
            env.close()

    @contextmanager
    def write_txn(self) -> Iterator[Any]:
        with self._env_lock():
            with self._env().begin(write=True) as txn:
                yield txn

    def get_bytes(self, key: bytes, txn: Any = None) -> bytes | None:
        if txn is not None:
            return txn.get(key)
        with self._env().begin() as read_txn:
            return read_txn.get(key)

    def put_bytes(self, key: bytes, value: bytes, txn: Any = None) -> None:
        if txn is not None:
            txn.put(key, value)
            return
        with self.write_txn() as write_txn:
            write_txn.put(key, value)

    def delete_key(self, key: bytes, txn: Any = None) -> None:
        if txn is not None:
            txn.delete(key)
            return
        with self.write_txn() as write_txn:
            write_txn.delete(key)

    def iter_raw(self, prefix: bytes, txn: Any = None) -> Iterator[tuple[bytes, bytes]]:
        if txn is not None:
            yield from self._iter_raw_txn(prefix, txn)
            return
        with self._env().begin() as read_txn:
            yield from self._iter_raw_txn(prefix, read_txn)

    def count_raw(self, prefix: bytes, txn: Any = None) -> int:
        if txn is not None:
            return self._count_raw_txn(prefix, txn)
        with self._env().begin() as read_txn:
            return self._count_raw_txn(prefix, read_txn)

    def _env(self) -> Any:
        path = self.path.resolve()
        with self._guard:
            env = self._envs.get(path)
            if env is not None:
                return env
            self.config.ensure_state_dirs()
            env = lmdb.open(
                str(path),
                subdir=True,
                create=True,
                map_size=self.config.lmdb_map_size,
                max_dbs=1,
                max_readers=126,
                readahead=False,
                meminit=False,
            )
            self._envs[path] = env
            self._env_locks.setdefault(path, threading.Lock())
            return env

    def _env_lock(self) -> threading.Lock:
        path = self.path.resolve()
        with self._guard:
            lock = self._env_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._env_locks[path] = lock
            return lock

    def _iter_raw_txn(self, prefix: bytes, txn: Any) -> Iterator[tuple[bytes, bytes]]:
        cursor = txn.cursor()
        if not cursor.set_range(prefix):
            return
        for raw_key, raw_value in cursor:
            if not raw_key.startswith(prefix):
                break
            yield raw_key, raw_value

    def _count_raw_txn(self, prefix: bytes, txn: Any) -> int:
        cursor = txn.cursor()
        if not cursor.set_range(prefix):
            return 0
        count = 0
        for raw_key, _raw_value in cursor:
            if not raw_key.startswith(prefix):
                break
            count += 1
        return count


class ContextStore:
    def __init__(self, config: ContextConfig, adapter: StoreAdapter | None = None):
        self.config = config
        self.adapter = adapter or LmdbStoreAdapter(config)

    @property
    def path(self) -> Path:
        return self.adapter.path

    @property
    def backend(self) -> str:
        return "lmdb"

    def exists(self) -> bool:
        return self.adapter.exists()

    def get_json(self, key: str, default: Any = None, txn: Any = None) -> Any:
        raw = self.adapter.get_bytes(self._key(key), txn=txn)
        if raw is None:
            return default
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return default

    def put_json(self, key: str, value: Any, txn: Any = None) -> None:
        raw = json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")
        self.adapter.put_bytes(self._key(key), raw, txn=txn)

    def delete(self, key: str, txn: Any = None) -> None:
        self.adapter.delete_key(self._key(key), txn=txn)

    def iter_json(self, prefix: str, txn: Any = None) -> list[tuple[str, Any]]:
        rows: list[tuple[str, Any]] = []
        for raw_key, raw_value in self.adapter.iter_raw(self._key(prefix), txn=txn):
            try:
                key = raw_key.decode("utf-8")
                value = json.loads(raw_value.decode("utf-8"))
            except Exception:
                continue
            rows.append((key, value))
        return rows

    def count(self, prefix: str, txn: Any = None) -> int:
        return self.adapter.count_raw(self._key(prefix), txn=txn)

    def delete_prefix(self, prefix: str, txn: Any = None) -> int:
        if txn is not None:
            keys = [key for key, _value in self.adapter.iter_raw(self._key(prefix), txn=txn)]
            for key in keys:
                self.adapter.delete_key(key, txn=txn)
            return len(keys)
        with self.write_txn() as write_txn:
            return self.delete_prefix(prefix, txn=write_txn)

    @contextmanager
    def write_txn(self) -> Iterator[Any]:
        with self.adapter.write_txn() as txn:
            yield txn

    def _key(self, key: str) -> bytes:
        return key.encode("utf-8")
