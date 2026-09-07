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

`context_lookup` line ranges for `snippet` and `chunk` modes are inclusive.
Starts below line 1 normalize to line 1. Snippet requests whose `end_line` is
less than `start_line` return a validation error; chunk requests retain their
documented behavior of normalizing such ends to the start. Partially overlapping
ranges clamp to the final line. Empty files and ranges whose normalized start is
past end-of-file return a validation error. Every successful range therefore
satisfies `1 <= start_line <= end_line <= file_line_count`; chunk metadata and
its `detail_lookup` use the same normalized interval.

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
| `prompt` | Required task text after trimming. Empty or whitespace-only values are rejected by MCP and REST with `prompt is required`. |
| `changed_files`, `focus_paths` | Repository-relative paths ranked first. |
| `memory_session` | Optional explicit continuation key for iterative turns; project-local, 24-hour TTL. |
| `client_profile`, `model_profile` | Client/provider hints. |
| `project_id`, `root_uri` | Explicit project selection. |
| `max_items` | Default `8`, range `1..32`. |
| `max_source_tokens` | Default `512`, range `0..4096`. |
| `evidence_policy` | `reference`, `balanced`, or `source`. |
| `cache_strategy` | `fast`, `stable`, or `fresh`. |
| `base_pack`, `known_evidence` | Optional manual delta inputs; non-empty explicit values override continuation-derived state. |

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
  "more": "ctxref-0123456789abcdef",
  "reuse": {
    "delta_applied": false,
    "source": "continuation",
    "status": "missing",
    "wire_tokens_avoided_est": 0
  }
}
```

For an iterative coding, testing, or review task, send the same caller-chosen
`memory_session` on each turn. The first successful request reports `missing`
and initializes compact project state; later valid requests report `reused`
and automatically apply the prior pack snapshot and acknowledged evidence.
The response-level `reuse` object reports whether a delta was applied and the
estimated evidence-card wire tokens avoided. `base_pack` or a non-empty
`known_evidence` list selects manual delta state for that request and reports
`explicit_override`.

Continuation state stores only a hashed session key, pack/evidence identifiers,
generation/signature, and timestamps. It is limited to 256 records per project
and expires after 24 hours. Missing state (including first use or the same key
in another project), expired state, a stale index generation, or a missing pack
falls back to a full pack with status `missing`, `expired`,
`stale_generation`, or `missing_pack`. No transport identity is inferred.

The compact evidence tuple contains the evidence id, policy/delta opcode,
inclusive line interval, symbol, evidence card, and estimated source tokens.
Diagnostics, metrics, and token accounting are available through
`context_admin` rather than being repeated in every pack.

## Usage and efficiency reporting

Project catalogue calls are explicitly row-bounded. For
`context_admin(mode="projects")`, `active_projects`, and `cached_projects`,
`max_entries` defaults to 20 and accepts `1..=1000`. The returned `projects`
array never exceeds that cap. `count` remains the returned row count, while
`total_count`, `returned_count`, `omitted_count`, and `truncated` distinguish the
complete matching catalogue from the inline subset. Catalogue ordering remains
deterministic by project id; explicit discovery persists the complete catalogue
before bounding the response.

`context_admin(mode="monitor_usage")` controls an opt-in, process-global
30-day usage ledger. Successful `context_pack` requests are grouped into the
bounded client profiles `codex`, `claude`, `copilot`, `generic`, `missing`, and
`other`. Reports include request counts and bounded latency, input-to-wire,
token-saving, raw cache outcomes, opportunity-normalized reuse, miss causes,
coarse repeat distance, frontier, route, and delta-adoption summaries. Rejected
attempts are counted only as `schema`, `root_policy`, `project_selection`, or
`internal`.

The report measures client-profile usage and usage share among requests observed
by this ledger. It is not an organization-wide adoption rate because no
external denominator is available. Raw profile values, prompts, responses,
paths, root URIs, error values, credentials, and agent identifiers are never
stored. Monitoring is disabled by default; the disabled request path is an
atomic flag check and does not change context-pack behavior.

`action="report"` returns `context_monitor_usage.report.v4`. `max_entries`
defaults to 20 and bounds `buckets`, `rejection_buckets`, and every returned
bucket's `client_profiles` rows. `max_output_chars` defaults to 12,000 and
bounds the serialized inline report with a maximum 2,048-character allowance
for truncation counts and retrieval metadata. The `truncation` object always
states the effective limits, returned and omitted row counts, and reasons. If
any rows are omitted, `truncation.retrieval.reference` identifies a complete
v3 report retained in generated state for 24 hours; pass that object to
`result_reference_resolve`. Source repositories remain read-only.

Raw L0 hit rate uses all L0 hits and misses. Exact effectiveness uses only
requests whose semantic L0 identity was observed earlier in the same project;
frontier effectiveness uses L0 misses with an earlier canonical term/scope
identity; delta adoption uses related earlier exact/frontier packs as its
denominator. Lineage opportunity counts matching salted identities in another
worktree derived from the same Git common directory, but never shares cache or
source state across projects. Synthetic repeated-request hit rate is a cache
mechanics benchmark, not evidence of production reuse adoption.

Replay exact, unique, expiry, invalidation, restart, and linked-worktree
telemetry scenarios with:

```bash
cargo run -p context-testkit --bin benchmark-reuse-opportunity --quiet
cargo run -p context-testkit --bin benchmark-worktree-frontier --quiet
```

## MCP-first workflow

Use `repo://instructions/context-pack` as the portable source of
truth:

1. Call `context_pack` first for repository tasks.
2. For iterative turns, keep one explicit `memory_session` and inspect the
   top-level `reuse` status; use manual delta fields only when overriding it.
3. Use `context_lookup` for targeted follow-up snippets, search, trees, symbols,
   references, impact, test ownership, chunks, and cache explanation.
4. Resolve raw referenced evidence before destructive changes, release claims,
   or security conclusions.
5. Use `context_admin` for health, projects, index, cache, metrics, contracts,
   benchmarks, quality, warmup, and generated-state inspection.
6. Store only structured, non-secret repository facts through `context_memory`.

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

## Governed project reference corpus

An optional `reference-corpus/manifest.json` lets a project rank locally staged
technical guides and standards beside repository evidence. Reference hits use
the synthetic `@corpus/<source>/<hash-prefix>` path, so they cannot be confused
with repository files. Their evidence symbol carries bounded source, version,
licence, freshness, and prompt-injection provenance; `context_pack.more` remains
the resolvable local reference for a bounded deferred excerpt.

The strict schema is documented in
[`doc/reference-corpus-manifest.schema.json`](doc/reference-corpus-manifest.schema.json).
It admits at most 32 sources, 4 MiB per normalized source, and 16 MiB per project.
Only regular, non-symlink UTF-8 text beneath `reference-corpus/` is read. The
declared original media type may be `application/pdf`, but `normalized_path`
must point to externally converted text with
`normalized_media_type="text/plain; charset=utf-8"`. The server has no network
client, downloader, PDF extractor, or OCR path.

Rights status must be `permitted` with explicit licence evidence before content
is indexed. `metadata_only` records are accepted only without a content path or
hash; unknown or missing rights are rejected. Content hashes deduplicate equal
snapshots and participate in deterministic chunk identity and refresh
invalidation. Corpus-derived index state has no age expiry; current, stale, and
superseded status is still surfaced. Project-scope mismatch, traversal,
absolute paths, symlinks, binary data, unsupported media, and hash mismatch are
rejected.

Agent use is deliberately repository-and-corpus first: call `context_pack`, use
`context_lookup(mode="search")` for a targeted query, and resolve `more` only
when more evidence is necessary. If no suitable source exists, advise an
external agent, job, or human to acquire and rights-check it, normalize it to
UTF-8, and stage it locally. Do not treat public accessibility as permission.

Re-run the labelled RFC/W3C spike from the repository root with:

```bash
cargo run -p context-testkit --bin benchmark-reference-corpus --quiet
```

The bounded recorded report is
[`benchmarks/results/governed-reference-corpus-spike.json`](benchmarks/results/governed-reference-corpus-spike.json).

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
| `MCP_CONTEXT_BLOCKING_CONCURRENCY` | Global cap for repository-heavy blocking jobs. Defaults to `max(1, min(2, available_parallelism / 2))`. |
| `MCP_CONTEXT_REQUEST_TIMEOUT_SECS` | End-to-end `context_pack` server budget, default `48`, kept below the common 60-second client timeout. |
| `MCP_CONTEXT_CANCELLATION_GRACE_SECS` | Portion of the server budget reserved for cooperative blocking-job cancellation, default `2`. |
| `MCP_CONTEXT_FRONTIER_LINEAGES_FILE` | Absolute path to an operator-owned `context_frontier_lineages.v1` JSON manifest that explicitly groups 2–64 allowed Git worktree roots. Omit to disable cross-worktree frontier reuse. |
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

Cold engine construction, cache/state access, and pack construction run behind
the global blocking-job cap rather than on a Tokio async worker. Background
refresh scans and Tantivy updates use the independent refresh cap described
below. Requests that expire while queued never start. Running
requests receive cooperative cancellation between files and chunks and before
an index commit or generation swap. Governed-lineage Git commands have a fixed
five-second execution cap, bounded output, and are killed when request
cancellation wins. Cooperative phases stop within cancellation grace. If a
blocking phase unexpectedly outlives that grace, the response stays attached
until the job releases its global permit and active-job counter; Tokio cannot
abort a live `spawn_blocking` closure safely. Cancellation remains latched so
the request job cannot commit cache, frontier, manifest, or initial index state.
An already scheduled refresh may still publish after its caller leaves. REST reports
queue/deadline or fail-closed freshness exhaustion as HTTP 503; MCP returns a
stable retryable `context_pack busy`, `context_pack timed out`, or
`context_pack freshness unavailable` diagnostic with warmup/retry guidance.

Use `context_admin(mode="warmup")` before a latency-sensitive normal request.
The server also schedules the default project only after stdio/HTTP transport
readiness. `context_admin(mode="warmup")`, `health`, `metrics`, and
`measurement_report` expose its `queued`, `building`, `ready`, or `failed`
lifecycle through `runtime.warmup.state`; startup never walks every allowed
root.

Increasing a client timeout can be a bounded fallback, but it does not replace
the server concurrency cap or cancellation budget. Generated agent/build/cache
trees such as `.workingdir/`, `.worktrees/`, `.openclaw/`, `target/`, and
`node_modules/` are excluded consistently from discovery, watching,
signatures, indexing, and governed Git lineage checks at any directory depth.

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
- an optional 256 MiB persistent pool of path-free immutable frontier records for explicitly governed, clean worktrees that share one Git common directory, HEAD, source signature, and index signature;
- a 50 ms coalescing filesystem watcher plus polling fallback;
- a bounded changed-path journal and per-file fingerprints for atomic
  incremental changed/deleted/renamed/untracked refresh;
- a validated, atomically replaced index snapshot under generated project
  state so a restart can reuse a complete warm generation;
- direct serialization into a preallocated byte buffer.

Watcher registration is established before the baseline index scan. Every
incremental result is checked against a second independently computed source
signature before publication, and a monotonic event epoch prevents a refresh
from clearing events that arrive during its signature scan, index build, or
snapshot write. Journal overflow, unsafe paths, corpus/config changes,
ambiguous events, mutation during refresh, and corrupt/incomplete persisted
state fail closed to a clean full rebuild or a bounded retryable freshness
error. Readers obtain one immutable index handle and therefore observe only the
old or new complete generation.

Refresh runs once per project in a background worker and survives request
cancellation. `MCP_CONTEXT_REFRESH_CONCURRENCY` bounds concurrent refreshes
across projects (default 2, maximum 32), independently of request permits.
`fast` and `stable` packs use the available snapshot, revalidate candidate
files, and overlay current contents of explicitly named files. Changed or
deleted stale candidates are omitted, including from deferred evidence.
`changed_files` queues work without waiting; `cache_strategy="fresh"` waits
for shared verification within the caller deadline without cancelling it.

The compact `freshness` response field is diagnostic: `current` means no
known pending source changes, `refreshing` means retrieval may be incomplete,
and `unverified` means the watcher cannot certify completeness. Watcher failure
queues authoritative verification and prevents ordinary cache reuse. Use
`fresh` when completeness is required. These states do not promise a filesystem
transaction across concurrent editor writes.

Incremental refresh replaces only changed paths' Tantivy documents and pins
old readers until publication. Metadata copying and full signature scans still
scale with repository size. A discarded candidate forces a clean rebase before
another update can reuse its underlying writer.

Cross-worktree reuse is disabled by default. A deployment may opt in with an
operator-owned manifest such as:

```json
{
  "schema": "context_frontier_lineages.v1",
  "lineages": [
    {
      "id": "mcp-context-manager-v2",
      "roots": ["/workspace-roots/builder", "/workspace-roots/verifier", "/workspace-roots/gatekeeper"]
    }
  ]
}
```

Every root must also be inside `MCP_CONTEXT_ALLOWED_ROOTS`. The id and root
list provide governance only; they never prove equivalence by themselves. The
server invokes Git read-only (`GIT_OPTIONAL_LOCKS=0`) to require a clean linked
worktree, a shared Git common directory, and an identical commit. It then
requires exact source and index signatures. If Git is unavailable or any proof
fails, retrieval remains project-local.

## Build, test, and release

Run validation inside the repository devcontainer:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run -p context-testkit --bin benchmark-context-pack-load --quiet
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

The load benchmark gates cold start, warm L0, one-file refresh, an edit burst,
a deterministic large-file disk-pressure case, concurrent same-project calls,
multiple projects, and current-thread event-loop lag against the common
60-second MCP SDK budget. A bounded reference run is recorded in
[`benchmarks/results/issue-29-context-pack-load.json`](benchmarks/results/issue-29-context-pack-load.json).

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
