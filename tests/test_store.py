from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

from mcp_context_manager.config import ContextConfig
from mcp_context_manager.store import ContextStore


def test_context_store_adapter_preserves_json_operations(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    store = ContextStore(
        ContextConfig(
            repo_path=repo.resolve(),
            state_dir=(repo / ".mcp-context-manager").resolve(),
        )
    )

    store.put_json("demo:a", {"value": 1})
    with store.write_txn() as txn:
        store.put_json("demo:b", {"value": 2}, txn=txn)
        store.put_json("other:c", {"value": 3}, txn=txn)

    assert store.get_json("demo:a") == {"value": 1}
    assert store.count("demo:") == 2
    assert [key for key, _value in store.iter_json("demo:")] == [
        "demo:a",
        "demo:b",
    ]

    store.adapter.put_bytes(b"demo:raw", b"not-json")
    assert store.count("demo:") == 3
    assert [key for key, _value in store.iter_json("demo:")] == [
        "demo:a",
        "demo:b",
    ]

    with store.write_txn() as txn:
        removed = store.delete_prefix("demo:", txn=txn)

    assert removed == 3
    assert store.get_json("demo:a") is None
    assert store.get_json("other:c") == {"value": 3}


def test_context_store_serializes_concurrent_lmdb_writes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    config = ContextConfig(
        repo_path=repo.resolve(),
        state_dir=(repo / ".mcp-context-manager").resolve(),
    )
    first_store = ContextStore(config)
    second_store = ContextStore(config)
    entered = Event()
    release = Event()

    def hold_write_txn() -> None:
        with first_store.write_txn() as txn:
            first_store.put_json("demo:first", {"value": 1}, txn=txn)
            entered.set()
            assert release.wait(timeout=2)

    def write_while_first_txn_is_open() -> None:
        assert entered.wait(timeout=2)
        second_store.put_json("demo:second", {"value": 2})

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(hold_write_txn)
        second = executor.submit(write_while_first_txn_is_open)
        assert entered.wait(timeout=2)
        time.sleep(0.05)
        assert not second.done()
        release.set()
        first.result(timeout=2)
        second.result(timeout=2)

    assert first_store.get_json("demo:first") == {"value": 1}
    assert second_store.get_json("demo:second") == {"value": 2}
