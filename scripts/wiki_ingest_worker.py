#!/usr/bin/env python3
"""Optional, agent-only datasheet companion. The MCP server never fetches PDFs."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import ipaddress
import json
import os
import queue
from pathlib import Path
import resource
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import urllib.parse
import urllib.request

MAX_PDF_BYTES = 25 * 1024 * 1024
MAX_TEXT_BYTES = 4 * 1024 * 1024


def publisher_target(url: str, allowed_hosts: set[str]) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or not host
        or parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.port not in (None, 443)
        or host not in allowed_hosts
    ):
        raise ValueError("publisher URL must use HTTPS and an explicitly allowed host")
    answers: queue.Queue = queue.Queue(maxsize=1)

    def resolve() -> None:
        try:
            answers.put(socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM))
        except OSError:
            answers.put(None)

    threading.Thread(target=resolve, daemon=True).start()
    try:
        addresses = answers.get(timeout=10)
    except queue.Empty as error:
        raise ValueError("publisher DNS lookup timed out") from error
    if not addresses:
        raise ValueError("publisher host did not resolve")
    ips = sorted({address[4][0] for address in addresses})
    # Reject the entire result if any answer is private, including IPv4-mapped IPv6.
    for address in ips:
        ip = ipaddress.ip_address(address)
        mapped = getattr(ip, "ipv4_mapped", None)
        if not ip.is_global or (mapped is not None and not mapped.is_global):
            raise ValueError("publisher DNS returned a non-public address")
    return host, ips[0]


class PinnedHTTPS(http.client.HTTPSConnection):
    """Pin the validated IP while retaining certificate/SNI checks for the publisher."""

    def __init__(self, host: str, address: str):
        super().__init__(host, timeout=10, context=ssl.create_default_context())
        self.address = address

    def connect(self) -> None:
        raw = socket.create_connection((self.address, 443), timeout=self.timeout)
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


def download_pdf(url: str, allowed_hosts: set[str], destination: Path) -> None:
    for _ in range(4):
        host, address = publisher_target(url, allowed_hosts)
        parsed = urllib.parse.urlsplit(url)
        connection = PinnedHTTPS(host, address)

        connection.connect()
        transport = connection.sock

        def cancel() -> None:
            if transport is not None:
                try:
                    transport.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            connection.close()

        timer = threading.Timer(30, cancel)
        timer.daemon = True
        timer.start()
        try:
            path = urllib.parse.urlunsplit(
                ("", "", parsed.path or "/", parsed.query, "")
            )
            connection.request("GET", path, headers={"Accept": "application/pdf"})
            response = connection.getresponse()
            if response.status in (301, 302, 303, 307, 308):
                location = response.getheader("Location")
                if not location:
                    raise ValueError("publisher redirect has no location")
                url = urllib.parse.urljoin(url, location)
                continue
            if response.status != 200:
                raise ValueError("publisher download failed")
            if response.getheader("Content-Encoding", "identity") != "identity":
                raise ValueError("compressed HTTP document bodies are unsupported")
            length = response.getheader("Content-Length")
            if length and (int(length) < 0 or int(length) > MAX_PDF_BYTES):
                raise ValueError("publisher PDF exceeds the download limit")
            received = 0
            with destination.open("wb") as target:
                while data := response.read(65536):
                    received += len(data)
                    if received > MAX_PDF_BYTES:
                        raise ValueError("publisher PDF exceeds the download limit")
                    target.write(data)
            with destination.open("rb") as source:
                if source.read(5) != b"%PDF-":
                    raise ValueError("download is not a PDF")
            return
        finally:
            timer.cancel()
            connection.close()
    raise ValueError("publisher redirect limit exceeded")


def extraction_limits() -> None:
    resource.setrlimit(resource.RLIMIT_CPU, (20, 20))
    resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024,) * 2)
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_TEXT_BYTES + 1,) * 2)


def extract_pdf(pdf: Path, directory: Path) -> tuple[str, list[int]]:
    executable = shutil.which("pdftotext")
    if executable is None:
        raise FileNotFoundError("install poppler-utils to provide pdftotext")
    output = directory / "normalized.txt"
    subprocess.run(
        [executable, "-layout", "-enc", "UTF-8", str(pdf), str(output)],
        check=True,
        timeout=25,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=extraction_limits,
    )
    if output.stat().st_size > MAX_TEXT_BYTES:
        raise ValueError("normalized document exceeds 4 MiB")
    pages = output.read_text(encoding="utf-8").split("\f")
    if pages and not pages[-1].strip():
        pages.pop()
    if not pages or len(pages) > 10000 or not any(page.strip() for page in pages):
        raise ValueError("PDF needs OCR or contains no extractable text")
    lines: list[str] = []
    starts = []
    for number, page in enumerate(pages, 1):
        starts.append(len(lines) + 1)
        lines.append(f"[PDF page {number}]")
        lines.extend(page.splitlines())
    text = "\n".join(lines) + "\n"
    if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
        raise ValueError("normalized document exceeds 4 MiB")
    return text, starts


class MCPClient:
    """Small Streamable HTTP client; credentials are read from the environment."""

    def __init__(self, url: str):
        self.url = url
        self.session = None
        self.sequence = 0
        # Do not route the configured local MCP endpoint through environment proxies.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "wiki-ingest-worker", "version": "1"},
            },
        )
        self.rpc("notifications/initialized", notification=True)

    def rpc(self, method: str, params: dict | None = None, notification: bool = False):
        self.sequence += 1
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        if not notification:
            message["id"] = self.sequence
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2025-03-26",
        }
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        if token := os.environ.get("MCP_CONTEXT_TOKEN"):
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            self.url, json.dumps(message).encode(), headers
        )
        with self.opener.open(request, timeout=60) as response:
            self.session = response.headers.get("Mcp-Session-Id", self.session)
            if notification:
                return None
            content_type = response.headers.get("Content-Type", "")
            if "text/event-stream" in content_type:
                for _ in range(1000):
                    line = response.readline(1024 * 1024)
                    if not line:
                        break
                    if line.startswith(b"data:") and line[5:].strip():
                        candidate = json.loads(line[5:])
                        if candidate.get("id") == self.sequence:
                            result = candidate
                            break
                else:
                    raise ValueError("bounded MCP event response exceeded")
                if "result" not in locals():
                    raise ValueError("MCP response was incomplete")
            else:
                result = json.loads(response.read(8 * 1024 * 1024 + 1))
        if "error" in result:
            raise ValueError("MCP request failed")
        return result["result"]

    def admin(self, project_id: str, action: str, wiki: dict | None = None) -> dict:
        args = {"mode": "wiki", "action": action, "project_id": project_id}
        if wiki is not None:
            args["wiki"] = wiki
        result = self.rpc("tools/call", {"name": "context_admin", "arguments": args})
        if result.get("isError"):
            raise ValueError("MCP wiki operation failed")
        for block in result.get("content", []):
            if block.get("type") == "text":
                return json.loads(block["text"])
        raise ValueError("MCP wiki operation returned no structured text")


def process_one(client: MCPClient, project_id: str, allowed_hosts: set[str]) -> dict:
    job = client.admin(project_id, "ingest_claim")
    if job["status"] == "empty":
        return job
    job_id = job["job_id"]
    attempt = job["attempt"]
    failure = "download_failed"
    try:
        if shutil.which("pdftotext") is None:
            failure = "missing_dependency"
            raise FileNotFoundError("install poppler-utils to provide pdftotext")
        with tempfile.TemporaryDirectory(prefix="wiki-ingest-") as staging:
            directory = Path(staging)
            pdf = directory / "source.pdf"
            download_pdf(job["url"], allowed_hosts, pdf)
            failure = "extraction_failed"
            text, starts = extract_pdf(pdf, directory)
            failure = "worker_failed"
            return client.admin(
                project_id,
                "ingest_import",
                {
                    "job_id": job_id,
                    "attempt": attempt,
                    "text": text,
                    "sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "page_starts": starts,
                },
            )
    except Exception:
        client.admin(
            project_id,
            "ingest_fail",
            {"job_id": job_id, "attempt": attempt, "failure": failure},
        )
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mcp-url", default="http://127.0.0.1:8000/mcp")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--allow-host", action="append", required=True)
    args = parser.parse_args()
    try:
        result = process_one(
            MCPClient(args.mcp_url),
            args.project_id,
            {host.lower() for host in args.allow_host},
        )
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in ("status", "job_id", "source_id", "sha256")
                    if key in result
                }
            )
        )
        return 0
    except FileNotFoundError:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason": "install poppler-utils to provide pdftotext",
                }
            )
        )
    except Exception:
        # Do not expose URLs, tokens, document contents, or untrusted exception messages.
        print(json.dumps({"status": "failed", "reason": "check ingestion job status"}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
