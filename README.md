# AgentTonic / mcp-context-manager

[![Build](https://github.com/ueni/mcp_context_manager/actions/workflows/build.yml/badge.svg)](https://github.com/ueni/mcp_context_manager/actions/workflows/build.yml)

`mcp-context-manager` is a native Rust Model Context Protocol server that builds
small, task-specific repository context packs. It exposes source files read-only
and writes only generated indexes, caches, memory, metrics, and result references
under the configured state directory.

Version 2 is a clean break for `context_pack`. The other public tools retain
their stable request and response schemas:

- `context_lookup`
- `context_memory`
- `context_admin`
- `result_reference_resolve`

The production server has no Python runtime dependency. Releases contain a
dynamic glibc executable, a static musl executable, a Docker image archive, a
CycloneDX SBOM, checksums, and a Sigstore signature bundle.

Implementation details are in [doc/technical-paper.md](doc/technical-paper.md).

## Context Pack v2

The default request uses balanced evidence and the fast cache strategy:

```json
{
  "prompt": "Review bearer authentication and its tests",
  "changed_files": ["src/contextd/src/lib.rs"],
  "focus_paths": ["scripts/smoke_native_mcp.py"],
  "client_profile": "codex"
}
```

Supported fields are:

| Field | Contract |
| --- | --- |
| `prompt` | Required task text. |
| `changed_files`, `focus_paths` | Repository-relative paths ranked first. |
| `memory_session` | Optional task-scoped memory key. |
| `client_profile`, `model_profile` | Client/provider hints. |
| `project_id`, `root_uri` | Explicit project selection. |
| `max_items` | Default `8`, range `1..32`. |
| `max_source_tokens` | Default `512`, range `0..4096`. |
| `evidence_policy` | `reference`, `balanced`, or `source`. |
| `cache_strategy` | `fast`, `stable`, or `fresh`. |
| `base_pack`, `known_evidence` | Optional session delta inputs. |

The MCP tool returns one raw JSON text item. The direct REST endpoint returns
the same UTF-8 bytes:

```json
{
  "v": 2,
  "id": "pk_0123456789abcdef",
  "route": "debug",
  "paths": ["src/contextd/src/lib.rs"],
  "evidence": [
    ["ev_0123456789abcdef", 1, 301, 303, "run_http", "sig: async fn run_http(...)", 13]
  ],
  "more": "ctxref-0123456789abcdef"
}
```

The compact evidence tuple contains the evidence id, policy/delta opcode,
inclusive line interval, symbol, evidence card, and estimated source tokens.
Diagnostics, metrics, and token accounting are available through
`context_admin` rather than being repeated in every pack.

## MCP-first workflow

Use `repo://instructions/codex-context-pack-first` as the portable source of
truth:

1. Call `context_pack` first for repository tasks.
2. Use `context_lookup` for targeted follow-up snippets, search, trees, symbols,
   references, impact, test ownership, chunks, and cache explanation.
3. Resolve raw referenced evidence before destructive changes, release claims,
   or security conclusions.
4. Use `context_admin` for health, projects, index, cache, metrics, contracts,
   benchmarks, quality, warmup, and generated-state inspection.
5. Store only structured, non-secret repository facts through `context_memory`.

Recommended Codex configuration:

```toml
[mcp_servers.mcp-context-manager]
url = "http://localhost:8000/mcp"
required = true
enabled_tools = [
  "context_pack",
  "context_lookup",
  "context_memory",
  "context_admin",
  "result_reference_resolve",
]
default_tools_approval_mode = "auto"
```

## Run with Docker Compose

Build the locked musl artifact and image, then start the service:

```bash
cmake --preset local
cmake --build --preset standalone
docker compose up --build
```

For several repositories under one host parent, keep that parent mounted and
select a narrow default repository so startup does not index the whole parent:

```bash
MCP_CONTEXT_HOST_ROOT=/home/user/source \
MCP_CONTEXT_REPO_PATH=/workspace-roots/mcp-context-manager \
docker compose up --build
```

The repository devcontainer does not start `mcp-context-manager`. On each
startup, it looks for an already-running Compose service with the
`mcp-context-manager` service label and connects to that service's network when
needed. If the service is not running, startup remains usable with a warning.
From inside the devcontainer, use `http://mcp-context-manager:8000/mcp`; legacy
SSE clients can use `http://mcp-context-manager:8000/legacy/sse`.

The mapping is then:

```text
host allowed root:  /home/user/source
container mount:    /workspace-roots
default repository: /workspace-roots/mcp-context-manager
```

Clients continue to send host-side URIs such as
`file:///home/user/source/my-repo`; `MCP_CONTEXT_ROOT_MAPPINGS` maps them into
the read-only container mount.

The image runs as UID/GID `1000` by default. Override the build args when the
state volume is owned by another user:

```bash
MCP_CONTEXT_UID=$(id -u) MCP_CONTEXT_GID=$(id -g) docker compose up --build
```

Do not remove the named state volume during an upgrade or rollback. Native Rust
state is isolated beside the Python v1 layout under each project:

```text
<project-state>/
  store/                    # untouched Python v1 LMDB, when present
  references/               # untouched Python v1 external references
  rust-v2/
    state.lmdb/
    index/
    server.lock
```

## Run a standalone executable

Dynamic glibc:

```bash
chmod +x mcp-context-manager-2.0.0-linux-x86_64-glibc
MCP_TRANSPORT=streamable-http \
HOST=127.0.0.1 \
PORT=8000 \
REPO_PATH="$PWD" \
MCP_CONTEXT_ALLOWED_ROOTS="$PWD" \
MCP_CONTEXT_STATE_DIR="$HOME/.local/state/mcp-context-manager" \
./mcp-context-manager-2.0.0-linux-x86_64-glibc
```

Static musl:

```bash
chmod +x mcp-context-manager-2.0.0-linux-x86_64-musl
MCP_TRANSPORT=streamable-http \
HOST=127.0.0.1 \
PORT=8000 \
REPO_PATH="$PWD" \
MCP_CONTEXT_ALLOWED_ROOTS="$PWD" \
MCP_CONTEXT_STATE_DIR="$HOME/.local/state/mcp-context-manager" \
./mcp-context-manager-2.0.0-linux-x86_64-musl
```

Use `--version` to inspect the embedded release version.

## HTTP transport and security

Streamable HTTP clients connect to `http://localhost:8000/mcp`. Existing
legacy SSE clients can continue using `http://localhost:8000/legacy/sse`; the
server sends the per-session message endpoint as the initial `endpoint` SSE
event. `/mcp` remains the primary transport. All HTTP routes enforce exact Host
validation. Browser requests with an `Origin` header also require an exact
allowed origin. Optional bearer authentication applies to every route,
including health, except the OAuth Protected Resource Metadata discovery
route:

```bash
MCP_HTTP_BEARER_TOKEN='replace-me' \
MCP_HTTP_PUBLIC_BASE_URL='https://context.example' \
MCP_HTTP_AUTHORIZATION_SERVERS='https://authorization.example' \
MCP_HTTP_ALLOWED_HOSTS='context.example,context.example:443' \
MCP_HTTP_ALLOWED_ORIGINS='https://context.example' \
MCP_TRANSPORT=streamable-http \
mcp-context-manager
```

Bearer tokens are compared in constant time. This is a pre-provisioned-token
boundary, not an OAuth authorization-server or JWT validator: deployments are
responsible for provisioning an audience-bound token for the configured MCP
resource. When bearer authentication is enabled, the public base URL and at
least one HTTPS authorization-server URL are required; the public base may use
HTTP only for loopback development. Do not place bearer tokens in repository
files or generated context memory. When set, `MCP_HTTP_PUBLIC_BASE_URL` is also authoritative for the
legacy SSE message URI, so HTTPS reverse-proxy deployments never infer a public
scheme from forwarding headers. Without it, legacy SSE preserves the validated
request `Host` with a local `http` scheme.

HTTP routes:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Native process health and version. |
| `GET /mcp/healthz` | Health under the MCP base path. |
| `GET /.well-known/oauth-protected-resource[/mcp]` | Unauthenticated RFC 9728 Protected Resource Metadata discovery; Host and Origin validation still apply. |
| `POST /mcp` | Stateful Streamable HTTP MCP endpoint. Sessions expire after 30 idle minutes. |
| `GET /legacy/sse` | Backward-compatible SSE MCP endpoint; sends the per-session `POST` endpoint. |
| `POST /legacy/messages?session_id=...` | Legacy SSE client message endpoint, issued by `/legacy/sse`. |
| `GET /v1/mcp/tools` | Diagnostic list of public context tool names. |
| `POST /v1/context/pack` | Direct `context_pack.v2`; accepts `prompt` or the REST-only alias `task`. |
| `GET /v1/context/references/{reference_id}` | Direct reference resolution. |

Stdio remains the default transport when `MCP_TRANSPORT` is omitted.

## Configuration

| Variable | Purpose |
| --- | --- |
| `REPO_PATH` | Default repository root. |
| `MCP_CONTEXT_STATE_DIR` | Project-state root; Rust creates an isolated `rust-v2` overlay. |
| `MCP_CONTEXT_PROJECT_ID` | Optional explicit id for the default project. |
| `MCP_CONTEXT_ALLOWED_ROOTS` | Host roots allowed for `root_uri` project selection. |
| `MCP_CONTEXT_ROOT_MAPPINGS` | Comma-separated host-to-container mappings such as `/home/user/source=/workspace-roots`. |
| `MCP_CONTEXT_HOST_ROOT` | Compose helper for the host directory mounted at `/workspace-roots`. |
| `MCP_CONTEXT_REPO_PATH` | Compose helper for a narrow default path below `/workspace-roots`. |
| `MCP_CONTEXT_UID`, `MCP_CONTEXT_GID` | Compose image user ids, default `1000:1000`. |
| `MCP_TRANSPORT` | `stdio` or `streamable-http`. |
| `HOST`, `PORT` | HTTP bind address and port. |
| `MCP_HTTP_BEARER_TOKEN` | Optional pre-provisioned bearer token for HTTP routes other than Protected Resource Metadata discovery. |
| `MCP_HTTP_PUBLIC_BASE_URL` | Optional canonical public HTTP origin; required with bearer auth. Controls metadata identifiers and legacy SSE message URIs when set; otherwise legacy SSE uses the validated request Host. |
| `MCP_HTTP_AUTHORIZATION_SERVERS` | Comma-separated authorization-server URLs advertised by Protected Resource Metadata. Required with bearer auth. |
| `MCP_HTTP_ALLOWED_HOSTS` | Comma-separated exact Host values. |
| `MCP_HTTP_ALLOWED_ORIGINS` | Comma-separated exact browser origins. |

Public request paths must be repository-relative. Traversal, host absolute
paths, symlink roots, and roots outside the configured boundary are rejected.
Repository content is treated as untrusted: prompt-injection signals are
reported, while secrets and absolute host paths are redacted before output or
persistence.

## Native architecture

The Cargo workspace is split by responsibility:

| Crate | Responsibility |
| --- | --- |
| `contextd` | RMCP stdio/HTTP transport, Axum REST routes, lifecycle, project registry, and HTTP security. |
| `context-core` | Contracts, routing, ranking, deterministic selection, compression, caches, deltas, and encoding. |
| `context-index` | Repository scanning, generic and Tree-sitter chunking, Tantivy search, and freshness watching. |
| `context-store` | Versioned Postcard/LMDB records, memory, references, import manifests, frontiers, and telemetry. |
| `context-testkit` | Native benchmark and synthetic test support. |
| `xtask` | Release versioning, license policy, and CycloneDX SBOM generation. |

The index accepts every regular UTF-8 file, rejects NUL/non-UTF-8 binary data,
does not follow symlinks, and skips generated directories. Supported languages
use Tree-sitter symbol chunks; all other text uses deterministic 80-line
windows with eight-line overlap. Tantivy readers persist across requests and
reload only after committed generations.

The fast path uses:

- a 64 MiB Moka L0 cache of immutable encoded response bytes;
- a 128 MiB in-memory and 256 MiB LMDB frontier cache;
- deterministic reranking after approximate frontier reuse;
- a 50 ms coalescing filesystem watcher plus polling fallback;
- synchronous refresh for `changed_files` and `cache_strategy="fresh"`;
- direct serialization into a preallocated byte buffer.

## Build, test, and release

Run validation inside the repository devcontainer:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo xtask license-check
cargo audit --deny warnings
cargo xtask sbom dist/mcp-context-manager.cdx.json
```

Build and smoke both standalone targets and the image:

```bash
cmake --preset local
cmake --build --preset standalone
python3 scripts/smoke_native_mcp.py dist/mcp-context-manager-linux-x86_64-glibc
python3 scripts/smoke_native_mcp.py dist/mcp-context-manager-linux-x86_64-musl
cmake --build --preset docker-image-archive
python3 scripts/smoke_native_image.py mcp-context-manager:local
```

Record a release version with the Rust task runner:

```bash
cargo xtask release-version 2.0.0
```

The release workflow builds locked glibc and musl executables, validates target
and linkage, emits the Docker archive and SBOM, runs advisory and license checks,
creates checksums, signs them, and publishes the versioned artifacts.

## Cutover evidence

Frozen Python v1 contracts and baselines remain under `tests/golden/python-v1`
and `benchmarks/baselines`. Native contract, differential, latency, token,
quality, freshness, state-import, cutover, and rollback evidence is under
`tests/golden/rust-v2` and `benchmarks/results`.

The pre-migration dirty worktree patch is retained as
`benchmarks/baselines/python-v1-pre-rust-dirty-worktree.patch`; it is not part
of the production runtime.
