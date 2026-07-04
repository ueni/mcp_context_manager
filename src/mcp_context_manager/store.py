from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import lmdb

from .config import ContextConfig


class ContextStore:
    _envs: dict[Path, Any] = {}

    def __init__(self, config: ContextConfig):
        self.config = config

    @property
    def path(self) -> Path:
        return self.config.store_path

    @property
    def backend(self) -> str:
        return "lmdb"

    def exists(self) -> bool:
        return self.path.exists()

    def get_json(self, key: str, default: Any = None, txn: Any = None) -> Any:
        raw = self._get(key, txn=txn)
        if raw is None:
            return default
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return default

    def put_json(self, key: str, value: Any, txn: Any = None) -> None:
        raw = json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")
        if txn is not None:
            txn.put(self._key(key), raw)
            return
        with self.write_txn() as write_txn:
            write_txn.put(self._key(key), raw)

    def delete(self, key: str, txn: Any = None) -> None:
        if txn is not None:
            txn.delete(self._key(key))
            return
        with self.write_txn() as write_txn:
            write_txn.delete(self._key(key))

    def iter_json(self, prefix: str, txn: Any = None) -> list[tuple[str, Any]]:
        if txn is not None:
            return list(self._iter_json_txn(prefix, txn))
        with self._env().begin() as read_txn:
            return list(self._iter_json_txn(prefix, read_txn))

    def count(self, prefix: str, txn: Any = None) -> int:
        return len(self.iter_json(prefix, txn=txn))

    def delete_prefix(self, prefix: str, txn: Any = None) -> int:
        if txn is not None:
            keys = [key for key, _value in self._iter_raw_txn(prefix, txn)]
            for key in keys:
                txn.delete(key)
            return len(keys)
        with self.write_txn() as write_txn:
            return self.delete_prefix(prefix, txn=write_txn)

    @contextmanager
    def write_txn(self) -> Iterator[Any]:
        with self._env().begin(write=True) as txn:
            yield txn

    def _get(self, key: str, txn: Any = None) -> bytes | None:
        if txn is not None:
            return txn.get(self._key(key))
        with self._env().begin() as read_txn:
            return read_txn.get(self._key(key))

    def _iter_json_txn(self, prefix: str, txn: Any) -> Iterator[tuple[str, Any]]:
        for raw_key, raw_value in self._iter_raw_txn(prefix, txn):
            try:
                key = raw_key.decode("utf-8")
                value = json.loads(raw_value.decode("utf-8"))
            except Exception:
                continue
            yield key, value

    def _iter_raw_txn(self, prefix: str, txn: Any) -> Iterator[tuple[bytes, bytes]]:
        raw_prefix = self._key(prefix)
        cursor = txn.cursor()
        if not cursor.set_range(raw_prefix):
            return
        for raw_key, raw_value in cursor:
            if not raw_key.startswith(raw_prefix):
                break
            yield raw_key, raw_value

    def _env(self) -> Any:
        path = self.path.resolve()
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
        return env

    def _key(self, key: str) -> bytes:
        return key.encode("utf-8")
