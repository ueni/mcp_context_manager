"""Offline worker checks, including actual Poppler extraction when installed."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "wiki_worker", Path(__file__).parents[1] / "scripts/wiki_ingest_worker.py"
)
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)


def pdf_fixture(path):
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R 5 0 R] /Count 2 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 7 0 R >> >> /Contents 4 0 R >>",
        None,
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 7 0 R >> >> /Contents 6 0 R >>",
        None,
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for index, text in [(3, b"Quasar voltage 3.3V"), (5, b"Timeout limit 400ms")]:
        stream = b"BT /F1 12 Tf 20 200 Td (" + text + b") Tj ET\n"
        objects[index] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"endstream"
        )
    data = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, value in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(f"{index} 0 obj\n".encode() + value + b"\nendobj\n")
    xref = len(data)
    data.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:
        data.extend(f"{offset:010} 00000 n \n".encode())
    data.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    )
    path.write_bytes(data)


class WorkerTests(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("MCP_WIKI_TEST_BINARY"), "native integration binary not selected"
    )
    def test_companion_imports_actual_pdf_through_native_mcp(self):
        binary = Path(os.environ["MCP_WIKI_TEST_BINARY"]).resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            repo.mkdir()
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            environment = os.environ.copy()
            for name in [
                "MCP_HTTP_BEARER_TOKEN",
                "MCP_HTTP_AUTHORIZATION_SERVERS",
                "MCP_CONTEXT_TOKEN",
            ]:
                environment.pop(name, None)
            environment.update(
                {
                    "HOST": "127.0.0.1",
                    "PORT": str(port),
                    "REPO_PATH": str(repo),
                    "MCP_CONTEXT_STATE_DIR": str(root / "state"),
                    "MCP_CONTEXT_ALLOWED_ROOTS": str(repo),
                    "MCP_CONTEXT_ROOT_MAPPINGS": "",
                }
            )
            process = subprocess.Popen(
                [str(binary), "--transport", "streamable-http"],
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                client = None
                last_error = None
                for _ in range(100):
                    try:
                        client = worker.MCPClient(f"http://127.0.0.1:{port}/mcp")
                        break
                    except Exception as error:
                        last_error = error
                        if process.poll() is not None:
                            self.fail("native MCP exited before readiness")
                        time.sleep(0.05)
                self.assertIsNotNone(client, repr(last_error))
                # Query the default project without guessing its generated identifier.
                result = client.rpc(
                    "tools/call",
                    {"name": "context_admin", "arguments": {"mode": "active_projects"}},
                )
                active = json.loads(result["content"][0]["text"])
                if not active["projects"]:
                    client.rpc(
                        "tools/call",
                        {
                            "name": "context_pack",
                            "arguments": {"prompt": "initialize wiki project"},
                        },
                    )
                    result = client.rpc(
                        "tools/call",
                        {
                            "name": "context_admin",
                            "arguments": {"mode": "active_projects"},
                        },
                    )
                    active = json.loads(result["content"][0]["text"])
                project = active["projects"][0]["project_id"]
                client.admin(
                    project,
                    "ingest_submit",
                    {
                        "url": "https://publisher.example/device.pdf",
                        "publisher": "Example",
                        "title": "Quasar datasheet",
                        "version": "1",
                        "license": "Local-use fixture",
                        "rights_status": "permitted",
                        "rights_evidence": "https://publisher.example/rights",
                    },
                )
                with patch.object(
                    worker,
                    "download_pdf",
                    side_effect=lambda url, hosts, path: pdf_fixture(path),
                ):
                    imported = worker.process_one(
                        client, project, {"publisher.example"}
                    )
                self.assertEqual(imported["status"], "imported")
                response = client.rpc(
                    "tools/call",
                    {
                        "name": "context_lookup",
                        "arguments": {
                            "mode": "wiki_search",
                            "query": "Quasar voltage",
                            "project_id": project,
                        },
                    },
                )
                search = json.loads(response["content"][0]["text"])
                reference = next(
                    row["reference"]
                    for row in search["results"]
                    if row["source_kind"] == "datasheet"
                )
                response = client.rpc(
                    "tools/call",
                    {
                        "name": "result_reference_resolve",
                        "arguments": {"reference": reference},
                    },
                )
                source = json.loads(response["content"][0]["text"])
                self.assertIn("Quasar voltage 3.3V", source["excerpt"])
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def test_blocks_unapproved_scheme_host_and_credentials_before_dns(self):
        with patch.object(worker.socket, "getaddrinfo") as resolve:
            for url in [
                "http://publisher.test/doc.pdf",
                "https://other.test/a",
                "https://user@publisher.test/a",
                "https://publisher.test:8443/a",
            ]:
                with self.assertRaises(ValueError):
                    worker.publisher_target(url, {"publisher.test"})
            resolve.assert_not_called()

    def test_blocks_private_mixed_and_ipv4_mapped_dns(self):
        for ips in [
            ["127.0.0.1"],
            ["93.184.216.34", "10.0.0.1"],
            ["::ffff:127.0.0.1"],
            ["169.254.169.254"],
        ]:
            answers = [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443)) for ip in ips
            ]
            with patch.object(worker.socket, "getaddrinfo", return_value=answers):
                with self.assertRaises(ValueError):
                    worker.publisher_target(
                        "https://publisher.test/a", {"publisher.test"}
                    )

    def test_tls_connect_pins_address_and_verifies_original_hostname(self):
        context = Mock()
        with patch.object(
            worker.ssl, "create_default_context", return_value=context
        ), patch.object(worker.socket, "create_connection") as connect:
            connection = worker.PinnedHTTPS("publisher.test", "93.184.216.34")
            connection.connect()
            connect.assert_called_once_with(("93.184.216.34", 443), timeout=10)
            context.wrap_socket.assert_called_once_with(
                connect.return_value, server_hostname="publisher.test"
            )

    def test_redirect_revalidates_target_and_download_size_is_bounded(self):
        connection = Mock()
        response = connection.getresponse.return_value
        response.status = 302
        response.getheader.return_value = "https://private.test/a"
        with tempfile.TemporaryDirectory() as directory, patch.object(
            worker, "PinnedHTTPS", return_value=connection
        ), patch.object(
            worker,
            "publisher_target",
            side_effect=[("publisher.test", "93.184.216.34"), ValueError("blocked")],
        ) as validate:
            with self.assertRaises(ValueError):
                worker.download_pdf(
                    "https://publisher.test/a",
                    {"publisher.test"},
                    Path(directory) / "a.pdf",
                )
            self.assertEqual(validate.call_count, 2)
        response.status = 200
        response.getheader.side_effect = lambda name, default=None: (
            str(worker.MAX_PDF_BYTES + 1) if name == "Content-Length" else default
        )
        with tempfile.TemporaryDirectory() as directory, patch.object(
            worker, "PinnedHTTPS", return_value=connection
        ), patch.object(
            worker, "publisher_target", return_value=("publisher.test", "93.184.216.34")
        ):
            path = Path(directory) / "a.pdf"
            with self.assertRaises(ValueError):
                worker.download_pdf(
                    "https://publisher.test/a", {"publisher.test"}, path
                )
            self.assertFalse(path.exists())

    @unittest.skipUnless(
        shutil.which("pdftotext"), "optional poppler-utils dependency unavailable"
    )
    def test_actual_pdf_extraction_preserves_page_locations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pdf_fixture(root / "source.pdf")
            text, starts = worker.extract_pdf(root / "source.pdf", root)
            self.assertEqual(len(starts), 2)
            self.assertEqual(starts[0], 1)
            self.assertIn("Quasar voltage 3.3V", text)
            self.assertIn("Timeout limit 400ms", text)
            self.assertEqual(text.splitlines()[starts[1] - 1], "[PDF page 2]")

    def test_extraction_timeout_and_missing_dependency(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            worker.shutil, "which", return_value="/usr/bin/pdftotext"
        ), patch.object(
            worker.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired("pdftotext", 25),
        ) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                worker.extract_pdf(Path(directory) / "a.pdf", Path(directory))
            self.assertEqual(run.call_args.kwargs["timeout"], 25)
        client = Mock()
        client.admin.return_value = {"status": "claimed", "job_id": "abc", "attempt": 2}
        with patch.object(worker.shutil, "which", return_value=None):
            with self.assertRaises(FileNotFoundError):
                worker.process_one(client, "project", {"publisher.test"})
        client.admin.assert_called_with(
            "project",
            "ingest_fail",
            {"job_id": "abc", "attempt": 2, "failure": "missing_dependency"},
        )


if __name__ == "__main__":
    unittest.main()
