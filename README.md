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
and expires after 24 hours. A prior pack may survive an unrelated generation
change when all of its source paths still match; changed dependencies or
journal overflow fall back with `stale_dependencies`. Missing state (including
first use or the same key in another project), expired state, an unverifiable
generation, or a missing pack falls back to a full pack with status `missing`,
`expired`, `stale_generation`, or `missing_pack`. No transport identity is
inferred.

The compact evidence tuple contains the evidence id, policy/delta opcode,
inclusive line interval, symbol, evidence card, and estimated source tokens.
Diagnostics, metrics, and token accounting are available through
`context_admin` rather than being repeated in every pack.

`knowledge` is an additive, stable pack field containing at most four compact
project-memory records. A record is injected only when it is non-expired,
non-redacted, and supported by `source_paths` whose persisted write-time
fingerprints still match the current indexed files. `source_paths` is an
additive `context_memory` write field accepting at most 16 paths; an explicit
upsert refreshes that source basis. Legacy rows without a basis are reported
as `unverified`, remain queryable, and are not injected into packs. Knowledge
is bounded evidence, not an instruction channel.

## Agent-only shared project wiki

The wiki keeps cited project knowledge beyond a single agent context window.
There is no human UI or model service. Agents add structured claims and
consolidate revisioned pages; the native server stores and searches generated
state while repository sources remain read-only.

| Operation | Interface |
| --- | --- |
| Retrieve relevant knowledge | `context_pack` returns bounded `wiki` entries and durable references. |
| Search shared knowledge | `context_lookup(mode="wiki_search", query=...)`; defaults to all admitted projects. Set `wiki_scope="selected_project"` to restrict it. |
| Read a current page | `context_lookup(mode="wiki_get", path="page-id", project_id=...)`. |
| Publish or revise | `context_memory(mode="wiki_upsert", wiki=...)`; supply `expected_revision` when revising. |
| Consolidate | `wiki_consolidate` publishes a replacement and links `supersedes` pages atomically. |
| Read exact historical evidence | Pass the returned `wiki` or `wiki_source` reference object to `result_reference_resolve`. |
| Refresh/deepen sources | `context_admin(mode="wiki", action="refresh"/"history_deepen", max_files=1000)`. |
| Inspect generated indexing state | `context_admin(mode="wiki", action="status")`. |

In the interactive monitor, press `v` to browse the selected project's wiki;
the initial list is bounded to 40 current pages. Press `/` to search, Enter to
open a page or run the search, and Esc to return to the results or dashboard.
An empty `wiki_search` query lists current pages up to `max_results` within the
requested scope; set `wiki_scope="selected_project"` to restrict it to one
project.

Wiki writes use the typed `wiki` tool-schema payload: `page_id`, optional
`expected_revision`, `title`, `summary`, `claims`, `sources`, optional `tags`,
`applicability`, `supersedes`, and `superseded_revisions`. Consolidation must
provide the current hash of every superseded page in that map. Each claim has `text`, a status (`verified`,
`unverified`, or `historical`), and citations naming declared `source_id`s.
The agent assesses claim correctness; the server validates source identity and
freshness. Markdown sources need the indexed path, fingerprint, and SHA-256. Git sources
must name an imported commit and its indexed hash. Datasheet sources must match
an imported document. Search returns compact source metadata; resolve the source
reference for complete provenance. Claims are evidence, never instructions.

Projects enter the shared index when activated through normal authorized project
selection. Registered projects are restored in background after server readiness;
their relative registrations are checked against current allowed roots. Markdown
and the latest 1,000 reachable Git commits are indexed in background. Git import
is bounded, resumable, and explicitly deepen-able. A Git record is historical
evidence, not proof of the installed or current implementation. Search uses a
published Tantivy snapshot and does not invoke Git, download documents, or scan
repository files. Source changes invalidate cached knowledge; stale claims stay
available for explicit inspection and are withheld from automatic packs.

Before context compaction, agents should publish reusable findings, consolidate
related pages, and call `context_memory(mode="compact", focus=...)`. Its
`wiki_references` preserve exact revisions. Page revisions have no age expiry;
explicit project-state deletion removes them. Clients must follow the tool
workflow; this server does not control native client context compaction.

### Optional online datasheet companion

Submit an ingestion job with `context_admin(mode="wiki", action="ingest_submit",
wiki={url, publisher, title, version, license, rights_evidence, rights_status="permitted"})`. Public
availability alone is not permission: supply an HTTPS rights-evidence reference permitting local use.
The native server only queues and validates these jobs.

Install Python 3 and `poppler-utils` in the companion environment, then run:

```bash
python3 scripts/wiki_ingest_worker.py --project-id PROJECT_ID \
  --mcp-url http://127.0.0.1:8000/mcp --allow-host publisher.example
```

Each invocation claims one job. Hosts must be explicitly allowed, including
redirect destinations. Downloads require HTTPS and public DNS addresses; TLS
uses the validated address with the publisher hostname. The worker bounds DNS,
network, file size, and extraction resources. PDFs are temporary; normalized
text, document hashes, page locations, and provenance enter generated state.
Scanned PDFs fail with an extraction error; OCR is not included. Inspect jobs
using `ingest_status`; `ingest_retry` accepts failed jobs or claims whose
ten-minute lease expired. Completion requires the matching claim attempt.
Optional MCP authentication is read from `MCP_CONTEXT_TOKEN` and never printed.

Reproduce the 10,000/100,000-chunk search baseline with:

```bash
cargo run -p context-testkit --bin benchmark-wiki --release
```

It reports build/first-query latency, warm p50/p95/p99, bounded results, and
identifier recall. These measurements establish a baseline; no absolute wiki
latency SLA is asserted. See the [recorded sample](benchmarks/baselines/agent-wiki-native.json)
and [validation report](doc/wiki-validation.md). Existing native pack benchmark
gates remain separate.

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
token-saving, route, and explicit-delta and continuation summaries. Rejected
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

Only current usage-bucket schemas are reported. Older generated buckets are
ignored; no cache-outcome or request-identity tracker is maintained.

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
From inside the devcontainer, use `http://mcp-context-manager:8000/mcp`.

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

Keep the named state volume when upgrading so native project state persists:

```text
<project-state>/
  rust-v2/
    state.lmdb/
    index/
    server.lock
```

The state volume also keeps authored wiki revisions and source records in LMDB.
Lookup and Git-diff caches are checkpointed in the background to the project's
`rust-v2/index/context-cache.checkpoint.v1.json`; cache snapshots are bounded,
atomic, and restored only for the same project and repository generation. The
`cache_stats.persistence` fields are diagnostic status, not a persistence API.
lookup cache keeps its original 30-second expiry across restarts. Raw lookup
queries are omitted, and cache responses that need redaction are not checkpointed.
Git diffs are checkpointed only when UTF-8 and unchanged by secret sanitization;
restore also checks the Git head, index, and source-file metadata. Keep the named
volume to retain this state across container upgrades and restarts.

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

Streamable HTTP clients connect to `http://localhost:8000/mcp`. All HTTP routes
enforce exact Host validation. Browser requests with an `Origin` header also
require an exact allowed origin. Optional bearer authentication applies to
every route, including health, except the OAuth Protected Resource Metadata
discovery route:

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
files or generated context memory. When set, `MCP_HTTP_PUBLIC_BASE_URL` is
authoritative for metadata identifiers.

HTTP routes:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Native process health and version. |
| `GET /mcp/healthz` | Health under the MCP base path. |
| `GET /.well-known/oauth-protected-resource[/mcp]` | Unauthenticated RFC 9728 Protected Resource Metadata discovery; Host and Origin validation still apply. |
| `POST /mcp` | Stateful Streamable HTTP MCP endpoint. Sessions expire after 30 idle minutes. |
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
| `MCP_CONTEXT_ALLOWED_ROOTS` | Host roots allowed for `root_uri` project selection. |
| `MCP_CONTEXT_ROOT_MAPPINGS` | Comma-separated host-to-container mappings such as `/home/user/source=/workspace-roots`. A mapped URI resolving to the configured default repository reuses its engine and state. |
| `MCP_CONTEXT_WATCH_POLL_INTERVAL_SECS` | Metadata-snapshot fallback interval when the native watcher cannot register, bounded to 1–300 seconds and defaulting to 2. |
| `MCP_CONTEXT_HOST_ROOT` | Compose helper for the host directory mounted at `/workspace-roots`. |
| `MCP_CONTEXT_REPO_PATH` | Compose helper for a narrow default path below `/workspace-roots`. |
| `MCP_CONTEXT_UID`, `MCP_CONTEXT_GID` | Compose image user ids, default `1000:1000`. |
| `MCP_TRANSPORT` | `stdio` or `streamable-http`. |
| `HOST`, `PORT` | HTTP bind address and port. |
| `MCP_HTTP_BEARER_TOKEN` | Optional pre-provisioned bearer token for HTTP routes other than Protected Resource Metadata discovery. |
| `MCP_HTTP_PUBLIC_BASE_URL` | Optional canonical public HTTP origin; required with bearer auth. Controls metadata identifiers. |
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
an index commit or generation swap. Cooperative phases stop within cancellation grace. If a
blocking phase unexpectedly outlives that grace, the response stays attached
until the job releases its global permit and active-job counter; Tokio cannot
abort a live `spawn_blocking` closure safely. Cancellation remains latched so
the request job cannot commit pack snapshots, references, or initial index state.
An already scheduled refresh may still publish after its caller leaves. REST reports
queue/deadline or fail-closed freshness exhaustion as HTTP 503; MCP returns a
stable retryable `context_pack busy`, `context_pack timed out`, or
`context_pack freshness unavailable` diagnostic with warmup/retry guidance.

Use `context_admin(mode="warmup")` to initialize the engine and wait for known
index changes before a latency-sensitive request. Warmup does not generate packs.
The server also schedules the default project only after stdio/HTTP transport
readiness. `context_admin(mode="warmup")`, `health`, `metrics`, and
`measurement_report` expose its `queued`, `building`, `ready`, or `failed`
lifecycle through `runtime.warmup.state`; startup never walks every allowed
root.

Increasing a client timeout can be a bounded fallback, but it does not replace
the server concurrency cap or cancellation budget. Generated agent/build/cache
trees such as `.workingdir/`, `.worktrees/`, `.openclaw/`, `target/`, and
`node_modules/` are excluded consistently from discovery, watching,
signatures, indexing, and Git diff collection at any directory depth.

## Native architecture

The Cargo workspace is split by responsibility:

| Crate | Responsibility |
| --- | --- |
| `contextd` | RMCP stdio/HTTP transport, Axum REST routes, lifecycle, project registry, and HTTP security. |
| `context-core` | Contracts, routing, ranking, deterministic selection, compression, caches, deltas, and encoding. |
| `context-index` | Repository scanning, generic and Tree-sitter chunking, Tantivy search, and freshness watching. |
| `context-store` | Versioned Postcard/LMDB records, memory, references, wiki revisions, and telemetry. |
| `context-testkit` | Native benchmark and synthetic test support. |
| `xtask` | Release versioning, license policy, and CycloneDX SBOM generation. |

The index accepts every regular UTF-8 file, rejects NUL/non-UTF-8 binary data,
does not follow symlinks, and skips generated directories. Supported languages
use Tree-sitter symbol chunks; all other text uses deterministic 80-line
windows with eight-line overlap. Tantivy readers persist across requests and
reload only after committed generations.

Each pack performs indexed retrieval, deterministic selection, and encoding.
The request path uses:

- a 50 ms coalescing filesystem watcher plus a metadata-snapshot fallback that prunes generated directories before traversal;
- a bounded changed-path journal and per-file fingerprints for atomic
  incremental changed/deleted/renamed/untracked refresh;
- a validated, atomically replaced index snapshot under generated project
  state so a restart can reuse a complete warm generation;
- direct serialization into a preallocated byte buffer.

Lookup cache diagnostics are exposed in the per-result `cache` object and in
metrics; they are diagnostic fields rather than retrieved project evidence.
Lookup caching is bounded to 256 entries or 16 MiB with a 30-second TTL;
stable request hashes identify entries while the stored generation and source
signature determine whether an entry can be used.
Pack snapshots retain the evidence identities needed for explicit deltas and
continuation memory. They do not serve cached pack responses or retrieval results.

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
Requests read one immutable published index generation. Watcher events queue
background file-cache and index updates. Pack requests wait for known pending
changes, including explicit `changed_files`, to publish before retrieval.

The compact `freshness` response field is diagnostic: `current` means no
known pending source changes, `refreshing` means retrieval may be incomplete,
and `unverified` means the watcher cannot certify completeness. Watcher failure
requires authoritative verification before pack generation. These
states do not promise a filesystem transaction across concurrent editor writes.

Administrative metrics expose the bounded watcher diagnostic under
`index_freshness.watcher`: `backend` is `native`, `metadata_snapshot`, or
`failed`; `status` is `starting`, `healthy`, or `failed`; and `failure` uses a
small category such as `setup_create`, `setup_register`, `runtime`, or
`disconnect`. Error text and repository paths are never included.

Incremental refresh replaces only changed paths' Tantivy documents and pins
old readers until publication. Metadata copying and full signature scans still
scale with repository size. A discarded candidate forces a clean rebase before
another update can reuse its underlying writer.

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

The load benchmark gates cold start, repeated packs, one-file refresh, an edit burst,
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

Frozen Python v1 contracts and baselines under `tests/golden/python-v1` and
`benchmarks/baselines` are archived cutover evidence only; the repository has no
runnable Python v1 compatibility source. Native contract, differential,
latency, token, quality, freshness, state-import, cutover, and rollback
evidence is retained under `tests/golden/rust-v2` and `benchmarks/results` as
historical artifacts.

The pre-migration dirty worktree patch is retained as
`benchmarks/baselines/python-v1-pre-rust-dirty-worktree.patch`; it is not part
of the production runtime.
