# <img align="left" src="doc/assets/agenttonic-banner.svg" alt="AgentTonic" width="100%" hspace="0" vspace="10">

[![Build](https://github.com/ueni/mcp_context_manager/actions/workflows/build.yml/badge.svg)](https://github.com/ueni/mcp_context_manager/actions/workflows/build.yml)

`AgentTonic` (mcp-context-manager) is a focused Model Context Protocol (MCP)
server for context management. It builds small, task-specific context packets
from the sources you expose, such as files, symbols, snippets, memory, metrics,
and references, so an agent can start from relevant signal instead of spending
tokens on broad discovery and repeated reads.

The server is read-only for source files. It writes only generated state such as
indexes, cache entries, metrics, memory, and result references under the
configured state directory.

## What It Is

The project is a compact context authority for one or more workspaces,
repositories, or project scopes exposed through MCP roots or explicit `root_uri`
/ `project_id` selection.

It provides:

- Bounded context packs for coding, review, debug, test, docs, security, and
  general context-heavy tasks.
- Targeted lookup for search hits, snippets, trees, symbols, references,
  related symbols, test owners, chunks, and cache explanation.
- Project-scoped generated state for indexes, caches, memory, references, and
  metrics.
- Stable local result references for omitted or bulky evidence.
- Tools-only fallbacks for MCP clients that do not support resources or prompts.

The implementation details are described in
[`doc/technical-paper.md`](doc/technical-paper.md).

## Why Use It

Modern coding agents are powerful, but context discovery is still expensive.
Without a context layer, every turn can become another round of broad search,
tree walking, repeated reads, and repeated explanations of the same project
facts. `AgentTonic` turns that scattered discovery work into one fast,
bounded, evidence-backed context packet.

Use it when you want agents to:

- Start with signal, not noise. The first tool call returns the files, symbols,
  snippets, tests, memory, and references most likely to matter for the task.
- Spend tokens on reasoning instead of context archaeology. Compact packs
  summarize and rank evidence while keeping full details available on demand.
- Stay fast across follow-up turns. Incremental indexing, fragment caches for
  retrieval, and chunk reuse keep unchanged context work from being
  repeated.
- Review and debug with traceable evidence. Every selected item carries path,
  line hints, reasons, confidence, provenance, and a `detail_lookup` route back
  to raw snippets.
- Keep large evidence out of the prompt until it is actually needed. Omitted
  candidates and diagnostics stay behind local references that can be resolved
  later.
- Work cleanly across many workspaces or repositories. MCP roots, explicit
  `root_uri`, and `project_id` keep indexes, cache, memory, metrics, and
  references isolated per project.
- Optimize without guessing. Metrics, benchmark runs, and gold-anchor fixtures
  show whether token savings, latency, cache reuse, and retrieval recall are
  actually improving.

The result is a tighter agent loop: less context churn, fewer repeated local
reads, smaller prompts, and better evidence discipline before code changes.

## Implemented Techniques

`mcp-context-manager` combines several token, latency, and safety techniques:

- Summary-first `context_pack` output with `minimal`, `compact`, `normal`, and
  `verbose` profiles.
- Client profiles for Codex, Claude, Copilot, and generic MCP hosts.
- Prompt echo and volatile runtime metadata disabled by default.
- Diagnostics and omitted evidence moved behind `diagnostics_ref` and
  `omitted_ref`.
- Deterministic public JSON fields and confidence buckets for compact output.
- Incremental indexing by file metadata, content digest, symbols, imports, and
  search terms.
- Git or file-metadata refresh signatures to skip unchanged repository scans.
- Chunk-addressed summary metadata by chunk id, line range, content digest,
  extractor version, and redaction version.
- Fragment caches for search terms and file summaries.
- Optional skill guidance compiled from non-secret `context_memory` records in
  `skills/<provider>` namespaces, cached as compact cards and returned by
  `context_pack` only when relevant.
- Route-aware candidate ranking using task terms, explicit paths, changed files,
  symbol matches, related symbols, likely tests, and diversity limits.
- Budget planning before final serialization, with low-value or bulky evidence
  deferred to references.
- Repository-local structured memory with facts, summaries, decisions, TTLs,
  validation, and compaction.
- Prompt-injection signal detection on returned repository text.
- Secret and host-path redaction before generated-state storage.
- Gold-anchor retrieval-quality fixtures and measurement-matrix benchmarks.

## Skill Guidance Convention

Agents can share reusable, non-secret skill material without adding tools or
parameters. Store raw skill records through `context_memory(mode="upsert")`
under namespaces such as `skills/codex`, `skills/claude`, `skills/copilot`, or
`skills/custom`. Use stable keys for skill ids and values with fields such as
`name`, `description`, `triggers`, `body` or `instructions`, `source`, and
optional `version`.

Pre-summarized skill records can use `context_memory(mode="summary_upsert")` in
the same namespaces. The next relevant `context_pack` ranks matching records,
compiles compact deterministic skill cards, caches them internally under
`skill.compiled`, and returns an optional `skill_guidance` field. Raw skill
bodies are not returned.

## Why LMDB Instead Of SQLite

The generated-state workload is closer to a local key-value cache than a
relational application database. `mcp-context-manager` stores indexes, term
rows, cache fragments, memory rows, metrics, and reference metadata as small
JSON documents behind stable key prefixes. LMDB fits that shape directly.

Why that matters for coding agents:

- Fast read-heavy access: context packs repeatedly scan prefix ranges such as
  indexed files, symbols, terms, cache entries, and metrics.
- Fewer moving parts on the hot path: lookup code reads JSON rows by key or
  prefix instead of planning SQL queries, joining tables, or maintaining a
  relational schema for every cache variant.
- Low operational overhead: LMDB is embedded, local, and does not need schema
  migrations for every new diagnostic or cache payload.
- Transactional generated state: index refreshes can update file, symbol,
  import, and term rows together.
- Batched refresh writes: a changed file can replace its `index:file`,
  `index:symbol`, `index:import`, and `index:term` rows in one write
  transaction, keeping the index consistent without extra coordination.
- Deterministic storage model: sorted JSON values plus stable key prefixes make
  cache entries and diagnostics easy to inspect and compare.
- Good fit for disposable state: generated indexes and caches can be pruned,
  rebuilt, or isolated per project without treating them as authoritative
  source data.

That shape is why the context-pack hot path stays small: the server can refresh
only changed paths, iterate contiguous key ranges for search and metrics, reuse
cached fragments, and write reference metadata without paying for relational
query planning or schema evolution. SQLite would work, but most of its strength
would sit unused because the server does not need joins, foreign keys, or ad hoc
analytics to build a compact context pack.

SQLite is excellent when the data model is relational and ad hoc SQL queries
are the product. This server mostly needs bounded prefix lookup, fast local
reuse, and simple project-scoped cache state, so LMDB keeps the hot path small.

## MCP Tools

The public tool surface is intentionally small:

| Tool | Purpose |
| --- | --- |
| `context_pack` | Build a task-focused context pack with ranked evidence and references. |
| `context_lookup` | Search, snippet, tree, symbols, references, impact, related symbols, test owners, chunk, or cache explanation. |
| `context_memory` | Store, retrieve, validate, and compact structured repository-local memory. |
| `context_admin` | Health, projects, index, cache/warmup, budget, contracts, metrics, measurement matrix, benchmark, generated-state browsing, quality evaluation, cache planning, profile calibration, instructions, resource proxy, and schema minification. |
| `result_reference_resolve` | Resolve local result references after boundary, expiry, and hash checks. |

Project-aware tools accept optional `project_id` or `root_uri`. If omitted, the
server uses MCP roots from the client. If multiple roots are visible and the
project cannot be inferred from path hints, the request is rejected as
ambiguous.

## MCP Resources And Prompts

Resources mirror bounded repository data for hosts that support them:

- `repo://summary`
- `repo://file/{path}`
- `repo://tree/{path}`
- `repo://context/{reference_id}`
- `repo://metrics`
- `repo://instructions/codex-context-pack-first`
- `repo://project/{project_id}/summary`
- `repo://project/{project_id}/file/{path}`
- `repo://project/{project_id}/tree/{path}`
- `repo://project/{project_id}/context/{reference_id}`
- `repo://project/{project_id}/metrics`
- `repo://project/{project_id}/instructions/codex-context-pack-first`

For tools-only clients, use:

```json
{"mode": "instructions"}
```

or:

```json
{"mode": "resource_proxy", "path": "repo://summary"}
```

with `context_admin`.

## Agent Onboarding

Repository-side MCP configuration can make this server available and provide
server instructions, but it cannot force a model to call a tool on every turn.
Pair MCP configuration with global or project agent instructions.

Use this prompt when onboarding an agent or MCP host:

```text
Adopt the mcp-context-manager MCP instructions into your global agent
instructions outside this repository. Use
repo://instructions/codex-context-pack-first as the source of truth for
repository tasks. Keep context_pack first, set client_profile per request, and
pass output_profile only when intentionally overriding the client/default
profile.
```

Use this prompt when creating or updating an `AGENTS.md` file:

```text
Create or update AGENTS.md for this repository. Preserve existing project
instructions, and add the mcp-context-manager MCP-first workflow: use
repo://instructions/codex-context-pack-first as the source of truth, call
context_pack before broad repository inspection, set client_profile per
request, and pass output_profile only when intentionally overriding the
client/default profile.
```

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

`required = true` fails startup or resume when the enabled server cannot
initialize. It does not guarantee a tool call on every turn.

## Profiles

`context_pack` is summary-first. The fastest profile is `minimal`; `compact`,
`normal`, and `verbose` progressively expose more inline diagnostics. Raw prompt
echo and volatile runtime metadata are opt-in.

Client profiles tune defaults without changing the public tool surface:

| Profile | Default intent |
| --- | --- |
| `codex` | Prefer fast minimal packs for MCP-first agent loops. |
| `claude` | Prefer compact summaries plus stable references for compaction-friendly sessions. |
| `copilot` | Keep a tools-only path through `context_admin(mode="instructions")` and `context_admin(mode="resource_proxy")`. |
| `generic` | Use the configured default output profile. |

Profiles are selected by the MCP caller on each `context_pack` request. The
server does not auto-detect the host client. Explicit `output_profile` wins over
`client_profile`; if `output_profile` is omitted, `client_profile="codex"` uses
`minimal`, and other clients use the configured default output profile.
`model_profile` is a provider hint, so use `client_profile="claude"` with
`model_profile="anthropic"` for Claude and `client_profile="copilot"` with
`model_profile="github"` for GitHub Copilot. `context_admin(mode="profile_calibrate")`
reports recommendations only; it does not mutate server or session state.

## Run With Docker Compose

Single-repo default from this checkout:

```bash
docker compose up --build
```

Global parent mode for multiple repositories under one host directory:

```bash
MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up --build
```

The image runs as non-root UID/GID `1000` by default. If your repository is
owned by another user, build with matching IDs:

```bash
MCP_CONTEXT_UID=$(id -u) MCP_CONTEXT_GID=$(id -g) docker compose up --build
```

If an existing named state volume was created with a different UID/GID, recreate
it after changing these build args:

```bash
docker compose down -v
MCP_CONTEXT_UID=$(id -u) MCP_CONTEXT_GID=$(id -g) docker compose up --build
```

Compose mounts the host parent read-only:

```text
Host:      /home/user/source
Container: /workspace-roots
Mapping:   /home/user/source=/workspace-roots
Allowed:   /home/user/source
```

An MCP root URI such as `file:///home/user/source/my-repo` is resolved inside
the container as `/workspace-roots/my-repo`.

The service image is immutable at runtime. Releases publish a pinned Docker
image archive and a standalone executable; updating production means deploying a
new release artifact.

## Non-Docker Run

Install the package in your Python environment, then run:

```bash
MCP_CONTEXT_ALLOWED_ROOTS=/home/user/source \
MCP_CONTEXT_STATE_DIR=/home/user/.local/state/mcp-context-manager \
mcp-context-manager
```

For HTTP transport:

```bash
MCP_TRANSPORT=streamable-http \
HOST=127.0.0.1 \
PORT=8000 \
MCP_CONTEXT_ALLOWED_ROOTS=/home/user/source \
MCP_CONTEXT_STATE_DIR=/home/user/.local/state/mcp-context-manager \
mcp-context-manager
```

Streamable HTTP clients should point at:

```yaml
mcpServers:
  - name: context-manager
    type: streamable-http
    url: http://localhost:8000/mcp
```

The legacy SSE compatibility endpoint is available at
`http://localhost:8000/legacy/sse`.

Useful HTTP endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Health and index status. |
| `GET /mcp/healthz` | Health and index status for clients or proxies scoped to the MCP base path. |
| `GET /v1/mcp/tools` | Diagnostic list of advertised MCP tool names and transport endpoints. |
| `POST /v1/context/pack` | Direct HTTP fallback for context packs. Accepts `prompt` or `task`. |
| `GET /v1/context/references/{reference_id}` | Direct HTTP fallback for resolving result references. |
| `POST /mcp` | MCP Streamable HTTP endpoint. |
| `GET /legacy/sse` | MCP legacy SSE compatibility endpoint. |

The stable `version` field in `context_admin(mode="health")` and both health
endpoints matches `serverInfo.version` from MCP initialization.

## Multiple Roots

Multiple roots come from the MCP client through the MCP Roots protocol. Compose
only mounts and maps the allowed parent directory.

Selection order for a request:

1. explicit `root_uri`
2. explicit `project_id`
3. the only visible MCP root
4. path hints such as `focus_paths`, `changed_files`, or `path`
5. legacy `REPO_PATH` fallback only in safe single-project configurations

Example explicit request payload:

```json
{
  "root_uri": "file:///home/user/source/my-repo",
  "prompt": "review auth handling",
  "focus_paths": ["src/auth.py"]
}
```

If a client exposes `/home/user/source` as one root, the server treats that
whole directory as one project. For per-repo isolation, expose or pass roots like
`file:///home/user/source/my-repo`.

## Configuration

| Variable | Purpose |
| --- | --- |
| `REPO_PATH` | Legacy single-project fallback root. |
| `MCP_CONTEXT_STATE_DIR` | Generated state directory. |
| `MCP_CONTEXT_ALLOWED_ROOTS` | Host paths allowed for MCP root URIs. Required for global roots outside `REPO_PATH`. |
| `MCP_CONTEXT_ROOT_MAPPINGS` | Host-to-container path mappings, such as `/home/user/source=/workspace-roots`. |
| `MCP_CONTEXT_HOST_ROOT` | Compose helper for the host parent mounted at `/workspace-roots`. |
| `MCP_CONTEXT_UID` / `MCP_CONTEXT_GID` | Compose build args for the non-root container user. Defaults to `1000:1000`. |
| `MCP_TRANSPORT` | `stdio` by default, or `streamable-http`. |
| `HOST` / `PORT` | HTTP bind settings. Compose binds the published port to localhost. |
| `MAX_READ_BYTES` | Maximum file bytes read for snippets/indexing. |
| `MAX_OUTPUT_CHARS` | Default output budget. |
| `MCP_CONTEXT_OUTPUT_PROFILE` | Default profile: `minimal`, `compact`, `normal`, or `verbose`. |
| `MCP_CONTEXT_LMDB_MAP_SIZE` | LMDB map size in bytes. Defaults to `1073741824` and is clamped to at least `16777216`. |
| `MCP_CONTEXT_TOKEN_COUNTER` | `estimate` by default, or `target` to try an optional target tokenizer. |
| `MCP_CONTEXT_TARGET_TOKENIZER` | Target tokenizer name for `target` mode, defaulting to `cl100k_base`. |

## Metrics And Evaluation

Use `context_admin(mode="measurement_matrix")` to get the pass/fail target
matrix for context-pack speed and token economy. Current metrics include
context-pack latency, index-refresh latency, saved input tokens, compression
ratio, candidate-to-selected ratio, cache hit ratios, external calls saved, and
reference bytes deferred.

Token savings are measured as:

```text
tokens_spared_by_mcp_est =
estimated_input_tokens_saved =
  max(0, baseline_input_tokens_est - output_tokens_est)
```

`baseline_input_tokens_est` estimates the candidate evidence an agent would
likely inspect without ranking. `output_tokens_est` estimates the selected pack
items returned to the model.

Use `context_admin(mode="quality_eval")` to run retrieval-quality fixtures from
`benchmarks/gold_anchors/*.json`. The report includes anchor recall@3/5, first
anchor rank, noise ratio, required-anchor omissions, detail-lookup resolution,
stale-context rate, and regression rows.

Run the built-in offline benchmark:

```bash
python3 benchmarks/context_pack_benchmark.py --repo .
```

The benchmark covers forced cold refresh, warm cache reuse, repeated prompt
reuse, prompt-variation fragment reuse, and compact focused retrieval.

Reuse `benchmarks/cache_hit_prompts.json` for realistic cache-hit benchmark prompts
covering mcp-context-manager and feso paths. The suite is designed for:

- cold run: execute each prompt once from a cold cache,
- warm run: repeat unchanged prompts to measure hit behavior,
- variant run: execute wording-variant prompts (`mcp_retrieval_variant`) to test fragment reuse,
- explicit-path stress: run prompts with explicit file/directory focus (`mcp_test_owner`, `mcp_dir_focus`, `mcp_docs`) on a clean run.

Track: `fragment_hits`, `fragment_misses`, `fragment_hit_ratio`,
`search_fragment_ms`, `search_summary_ms`, `test_owner_summary_ms`, and
`file_summary_memo_hits`.

## Live Metrics Monitor

`monitor-metrics.py` is a terminal dashboard that queries `context_admin(mode="projects")`,
then fetches `context_admin(mode="metrics")` and
`context_admin(mode="measurement_matrix")` for each selected project.

Run it interactively against a local server:

```bash
python3 monitor-metrics.py --url http://127.0.0.1:8000/mcp
```

Render one snapshot and exit:

```bash
python3 monitor-metrics.py --url http://127.0.0.1:8000/mcp --once
```

Monitor one known project or root URI:

```bash
python3 monitor-metrics.py --project-id my-repo-123abc
python3 monitor-metrics.py --root-uri file:///home/user/source/my-repo
```

Run a pinned remote version in one line:

```bash
VER="RELEASE_VERSION"; curl -fsSL "https://raw.githubusercontent.com/ueni/mcp_context_manager/v${VER}/monitor-metrics.py" | python3 - --url http://127.0.0.1:8000/mcp --once
```

The dashboard shows request volume, `context_pack` latency, cache hit bars,
estimated MCP-spared tokens, deferred reference bytes, measurement-matrix
status, and bounded generated-state rows. It shows a yellow warning when the
connected MCP server version differs from the version expected by the monitor.

## Generated State

In global mode, each selected project gets isolated state:

```text
<state_dir>/projects/<slug>-<root_hash>/store/
<state_dir>/projects/<slug>-<root_hash>/references/
```

Generated state holds the repository index, search term index, cache, metrics,
memory, budgets, and result-reference metadata. Large result references may use
the project `references/` area while public responses keep only stable
identifiers, hashes, TTLs, and resolver URIs.

Generated state should not be committed unless it is an intentional fixture or
documented sample.

## Release Packaging

The production `Dockerfile` builds an Alpine-based runtime image around a
prebuilt standalone server executable. It does not build the Python package from
source inside the runtime image.

Local build tasks are orchestrated through CMake presets. Container package
manager and pip downloads are cached under `.downloads/`, so repeated builds do
not fetch the same apt, apk, and pip artifacts every time.
When running inside the devcontainer, the presets use `HOST_WORKSPACE_FOLDER`
as the host-visible Docker bind mount path.

```bash
cmake --preset local
cmake --build --preset standalone
cmake --build --preset docker-image
cmake --build --preset docker-image-archive
```

If Docker requires sudo on the host, use the matching presets:

```bash
cmake --preset local-sudo-docker
cmake --build --preset standalone-sudo-docker
```

The Docker image target uses `dist/mcp-context-manager-linux-x86_64-musl` as
`SERVER_BINARY`. The Dockerfile checks that the binary is musl-linked because
the runtime base image is Alpine.

GitHub Actions owns release artifacts:

- `Build glibc and musl executables` runs the CMake `standalone-ci` preset and
  caches `.downloads/` between runs.
- `Build docker image archive` runs the CMake `docker-image-archive-ci` preset.
- `Smoke test glibc executable` and `Smoke test musl executable` verify the
  standalone servers before packaging.
- `Release` is manually dispatched from a branch with a version such as
  `1.2.0`. It updates `pyproject.toml` and the standalone monitor's expected
  server version; creates a `Release v1.2.0` commit and annotated tag; builds
  with the CMake `release-ci` preset; verifies that both executables report the
  requested version; writes and signs `SHA256SUMS`; atomically pushes the commit
  and tag; and publishes all artifacts on the GitHub release.

Verify downloaded release artifacts:

```bash
sha256sum -c SHA256SUMS
```

Use the Docker image archive:

```bash
gzip -dc mcp-context-manager-0.2.0-linux-x86_64-musl-image.tar.gz | docker load
docker run --rm \
  -p 127.0.0.1:8000:8000 \
  -e MCP_CONTEXT_ALLOWED_ROOTS=/workspace-roots \
  -e MCP_CONTEXT_ROOT_MAPPINGS="$PWD=/workspace-roots" \
  -e MCP_CONTEXT_STATE_DIR=/state \
  -v "$PWD:/workspace-roots:ro" \
  -v mcp-context-state:/state \
  mcp-context-manager:0.2.0
```

Use the glibc standalone server executable:

```bash
chmod +x mcp-context-manager-0.2.0-linux-x86_64-glibc
MCP_TRANSPORT=streamable-http \
HOST=127.0.0.1 \
PORT=8000 \
MCP_CONTEXT_ALLOWED_ROOTS="$PWD" \
MCP_CONTEXT_STATE_DIR="$HOME/.local/state/mcp-context-manager" \
./mcp-context-manager-0.2.0-linux-x86_64-glibc
```

Run a self-update from the standalone executable (auto-updates and relaunches):

```bash
./mcp-context-manager-0.2.0-linux-x86_64-glibc --update
```

Use the musl standalone server executable on Alpine-compatible hosts:

```bash
chmod +x mcp-context-manager-0.2.0-linux-x86_64-musl
MCP_TRANSPORT=streamable-http \
HOST=127.0.0.1 \
PORT=8000 \
MCP_CONTEXT_ALLOWED_ROOTS="$PWD" \
MCP_CONTEXT_STATE_DIR="$HOME/.local/state/mcp-context-manager" \
./mcp-context-manager-0.2.0-linux-x86_64-musl --update
``` 

The updater defaults to `ueni/mcp_context_manager` when `--update-repo` or
`MCP_CONTEXT_UPDATE_REPO` is not provided and uses `--update` without extra
input to fetch the latest release.
It replaces the running executable and restarts it using the same non-update
arguments so it continues as the same invocation.
You can still override with `--update-repo`/`MCP_CONTEXT_UPDATE_REPO` and
`--update-target`/`MCP_CONTEXT_UPDATE_TARGET`.

Use `--update-version` only when you want to install a specific version.

## Development

Run tests and lint:

```bash
python3 -m pytest
python3 -m ruff check .
```

The test suite is offline by default and uses temporary sample repositories.
