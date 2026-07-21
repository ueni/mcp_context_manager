"""Tests for the devcontainer's dynamic MCP network connection."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / ".devcontainer" / "connect-mcp-network.sh"


class DevcontainerNetworkTests(unittest.TestCase):
    def run_script(
        self, state: dict[str, object]
    ) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake_docker = temp / "docker"
            log = temp / "docker.log"
            state_file = temp / "state.json"
            fake_docker.write_text(
                textwrap.dedent(
                    """
                    #!/usr/bin/env python3
                    import json
                    import os
                    import sys

                    state_path = os.environ["FAKE_DOCKER_STATE"]
                    log_path = os.environ["FAKE_DOCKER_LOG"]
                    with open(state_path, encoding="utf-8") as stream:
                        state = json.load(stream)
                    args = sys.argv[1:]
                    with open(log_path, "a", encoding="utf-8") as stream:
                        json.dump(args, stream)
                        stream.write("\\n")

                    if args[:1] == ["ps"]:
                        if state["sibling"]:
                            print("sibling-id")
                        raise SystemExit(0)

                    if args[:1] == ["inspect"]:
                        target = args[-1]
                        format_arg = args[args.index("--format") + 1]
                        if ".Id" in format_arg:
                            print("current-id" if target == "devcontainer-id" else target)
                        elif "Aliases" in format_arg:
                            networks = state["sibling_networks"]
                            for network in networks:
                                if isinstance(network, str):
                                    network = {"name": network, "aliases": ["mcp-context-manager"]}
                                for alias in network["aliases"]:
                                    print(network["name"], alias)
                        elif "NetworkSettings.Networks" in format_arg:
                            networks = state["current_networks"] if target == "current-id" else state["sibling_networks"]
                            print("\\n".join(networks))
                        raise SystemExit(0)

                    if args[:2] == ["network", "inspect"]:
                        raise SystemExit(0 if args[-1] == state["network"] else 1)

                    if args[:2] == ["network", "connect"]:
                        state["current_networks"].append(args[2])
                        with open(state_path, "w", encoding="utf-8") as stream:
                            json.dump(state, stream)
                        raise SystemExit(0)

                    raise SystemExit(2)
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            fake_docker.chmod(fake_docker.stat().st_mode | stat.S_IXUSR)
            state_file.write_text(json.dumps(state), encoding="utf-8")
            env = {
                **os.environ,
                "DOCKER_BIN": str(fake_docker),
                "FAKE_DOCKER_LOG": str(log),
                "FAKE_DOCKER_STATE": str(state_file),
                "DEVCONTAINER_ID": "devcontainer-id",
            }
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            calls = [
                json.loads(line)
                for line in log.read_text(encoding="utf-8").splitlines()
            ]
            return result, calls

    def test_missing_sibling_is_non_fatal(self) -> None:
        result, calls = self.run_script(
            {
                "sibling": False,
                "network": "dynamic-project_net",
                "sibling_networks": [],
                "current_networks": [],
            }
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("no running mcp-context-manager sibling found", result.stderr)
        self.assertFalse(any(call[:2] == ["network", "connect"] for call in calls))

    def test_dynamic_network_connection_is_idempotent(self) -> None:
        state = {
            "sibling": True,
            "network": "dynamic-project_net",
            "sibling_networks": ["dynamic-project_net"],
            "current_networks": ["workspace_default"],
        }
        first, first_calls = self.run_script(state)
        self.assertEqual(first.returncode, 0)
        self.assertIn(
            ["network", "connect", "dynamic-project_net", "current-id"], first_calls
        )

        state["current_networks"].append("dynamic-project_net")
        second, second_calls = self.run_script(state)
        self.assertEqual(second.returncode, 0)
        self.assertFalse(any(call[:2] == ["network", "connect"] for call in second_calls))

    def test_selects_network_with_service_alias(self) -> None:
        result, calls = self.run_script(
            {
                "sibling": True,
                "network": "service-network",
                "sibling_networks": [
                    {"name": "first-network", "aliases": ["other-service"]},
                    {"name": "service-network", "aliases": ["mcp-context-manager"]},
                ],
                "current_networks": [],
            }
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn(
            ["network", "connect", "service-network", "current-id"], calls
        )

    def test_no_aliased_network_is_non_fatal(self) -> None:
        result, calls = self.run_script(
            {
                "sibling": True,
                "network": "unaliased-network",
                "sibling_networks": [
                    {"name": "unaliased-network", "aliases": ["other-service"]}
                ],
                "current_networks": [],
            }
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("no sibling network has the mcp-context-manager DNS alias", result.stderr)
        self.assertFalse(any(call[:2] == ["network", "connect"] for call in calls))


if __name__ == "__main__":
    unittest.main()
