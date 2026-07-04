#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mcp_context_manager.config import ContextConfig  # noqa: E402
from mcp_context_manager.context import ContextService  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the offline context_pack latency/token benchmark."
    )
    parser.add_argument(
        "--repo",
        default=str(REPO_ROOT),
        help="Repository path to benchmark. Defaults to this checkout.",
    )
    parser.add_argument(
        "--state-dir",
        default="",
        help="State directory. Defaults to <repo>/.mcp-context-manager.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5000,
        help="Maximum files to visit during benchmark index refresh.",
    )
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    state_dir = (
        Path(args.state_dir).resolve()
        if args.state_dir
        else (repo / ".mcp-context-manager").resolve()
    )
    service = ContextService(ContextConfig(repo_path=repo, state_dir=state_dir))
    result = service.context_admin(mode="benchmark", max_files=args.max_files)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
