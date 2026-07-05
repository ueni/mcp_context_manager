from __future__ import annotations

from pathlib import Path

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

    with store.write_txn() as txn:
        removed = store.delete_prefix("demo:", txn=txn)

    assert removed == 2
    assert store.get_json("demo:a") is None
    assert store.get_json("other:c") == {"value": 3}
