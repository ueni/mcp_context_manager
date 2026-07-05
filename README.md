# mcp-context-manager

Focused MCP server for compact, project-scoped repository context packs.

The server helps coding agents retrieve the smallest useful context for a task:
repo facts, search hits, snippets, symbols, memory, and large-result references.
It is read-only for source files. Generated state is stored under the configured
state directory.

## What It Does

- Builds bounded context packs for coding, review, debug, test, docs, security,
  and general tasks.
- Indexes files incrementally by content hash and refreshes changed files before
  lookup and pack operations.
- Keeps per-project index, cache, memory, and references isolated.
- Uses MCP Roots as the preferred project boundary model.
- Supports Docker global mode by mapping host root URIs to container paths.
- Preserves large evidence through local result references instead of dumping
  bulky payloads into model context.

## MCP Tools

- `context_pack`: build a compact task-focused context pack.
- `context_lookup`: search, read snippets, list trees, query symbols, or list
  references.
- `context_memory`: manage project-local compact memory and decisions.
- `context_admin`: health, index, cache, budget, contracts, and project listing.
- `result_reference_resolve`: resolve large local result references.

Project-aware tools accept optional `project_id` or `root_uri`. If omitted, the
server uses MCP Roots from the client. If multiple roots are visible and the
project cannot be inferred from request paths, the request is rejected as
ambiguous. Clients that do not implement MCP `roots/list` fall back to explicit
`project_id` / `root_uri` selection or legacy `REPO_PATH`.

## MCP Client Configuration

Streamable HTTP clients should point at the MCP endpoint:

```yaml
mcpServers:
  - name: context-manager
    type: streamable-http
    url: http://localhost:8000/mcp
```

Some clients still have better compatibility with the older SSE transport. The
server exposes a compatibility endpoint for those clients:

```yaml
mcpServers:
  - name: context-manager
    type: sse
    url: http://localhost:8000/legacy/sse
```

If a client reports `Method not found` for a tool such as `context_pack`, verify
the server-visible tool names:

```bash
curl http://localhost:8000/v1/mcp/tools
```

The response lists MCP tool names and transport endpoints. MCP tools are called
through the protocol method `tools/call` with the tool name in `params.name`;
they are not JSON-RPC methods named `context_pack`, `context_admin`, and so on.

## MCP Resources

- `repo://summary`: workspace facts for an unambiguous project.
- `repo://file/{path}`: bounded file content for an unambiguous project.
- `repo://tree/{path}`: bounded tree listing for an unambiguous project.
- `repo://context/{reference_id}`: resolved result reference for an unambiguous
  project.
- `repo://metrics`: compact metrics for an unambiguous project.
- `repo://instructions/codex-context-pack-first`: portable guidance telling
  coding agents to call `context_pack` before broad repository inspection.
- `repo://project/{project_id}/summary`
- `repo://project/{project_id}/file/{path}`
- `repo://project/{project_id}/tree/{path}`
- `repo://project/{project_id}/context/{reference_id}`
- `repo://project/{project_id}/metrics`
- `repo://project/{project_id}/instructions/codex-context-pack-first`

Metrics include request counts, cache hits and misses, estimated saved input
tokens, baseline/output token estimates, retrieval counts, route totals, stage
timings, tooling-call savings, and recent latency benchmarks. They are stored
under project-local generated state and do not include prompts, query text, file
contents, or secrets.

## Codex Speed Guidance

Repository-side MCP configuration can make this server available and can provide
strong server instructions, but it cannot force the model to call a tool on every
turn. The practical speed path is to make the first `context_pack` call fast and
useful enough that agents do not need broad `rg`, tree, or whole-file reads.

To make MCP usage as mandatory as Codex supports, configure the server as
required and limit the advertised tool surface to this server's public tools:

```toml
# ~/.codex/config.toml or trusted-project .codex/config.toml
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
default_tools_approval_mode = "approve"
```

`required = true` fails startup or resume when the enabled MCP server cannot
initialize. It does not force every model turn to call a tool. Pair it with
`AGENTS.md`, the server `instructions` field, and review/CI checks that reject
work started with broad local inspection instead of `context_pack`.

Agents can read `repo://instructions/codex-context-pack-first` or use the
`use_context_pack_first` prompt. For global multi-root sessions, use
`repo://project/{project_id}/instructions/codex-context-pack-first` after
selecting a project.

`context_pack` is summary-first: pack `items` are compact file summaries with
`path`, line hints, score, reason codes, `title_hint`, short `content`, and a
stable `detail_lookup` object pointing back to `context_lookup(mode="snippet")`
for the full evidence. `source_chars` and `deferred_chars` estimate how much
source text was intentionally kept out of the model prompt.

## Measurement Matrix And Benchmarks

Use `context_admin(mode="measurement_matrix")` to get the exact pass/fail target
matrix for context-pack speed and token economy. Current targets:

| Key | Pass Target |
| --- | --- |
| `latency.context_pack.avg_elapsed_ms` | `<= 750 ms` |
| `latency.context_pack.p95_recent_ms` | `<= 1500 ms` after at least 3 samples |
| `latency.context_pack.index_refresh_avg_ms` | `<= 250 ms` |
| `tokens.context_pack.avg_saved_per_pack` | `>= 500 estimated input tokens` |
| `tokens.context_pack.avg_tokens_spared_by_mcp_per_pack` | `>= 500 estimated input tokens` |
| `tokens.context_pack.compression_ratio` | `<= 0.70` |
| `retrieval.context_pack.candidates_per_selected` | `<= 8.0` |
| `cache.hit_ratio` | `>= 0.20` after at least 2 cacheable requests |
| `cache.context_pack_retrieval_hit_ratio` | `>= 0.20` after at least 2 pack retrievals |
| `tooling.external_calls_saved_per_pack` | `>= 2.0 estimated calls` |
| `tooling.contract_tokens_saved_est` | `>= 1 estimated token` |
| `references.bytes_deferred_est` | `>= 1 byte` |

Token savings are measured as:

```text
tokens_spared_by_mcp_est =
estimated_input_tokens_saved =
  max(0, baseline_input_tokens_est - output_tokens_est)
```

`baseline_input_tokens_est` estimates the candidate evidence an agent would
likely inspect without ranking. `output_tokens_est` estimates the selected pack
items returned to the model. The formula is intentionally conservative: if the
compact JSON is larger than the candidate evidence estimate, savings are `0`.
`tokens_spared_by_mcp_est` is the explicit MCP-facing name for the same current
estimate; `estimated_input_tokens_saved` remains for compatibility.

## Live Metrics Monitor

`monitor-metrics.py` is a small terminal dashboard that reads metrics directly
from MCP tool calls. It calls `context_admin(mode="projects")`, then
`context_admin(mode="metrics")` and `context_admin(mode="measurement_matrix")`
for each project. It does not use MCP resources or direct state-file reads.

Run it interactively:

```bash
python3 monitor-metrics.py --url http://127.0.0.1:8000/mcp
```

Controls:

| Key | Action |
| --- | --- |
| `Up` / `Down` | Select a project row. |
| `Enter` | Open details for the selected project. |
| `b` | Browse sanitized generated state for the selected project. |
| `Esc` | Return to the project table, or close a state-entry overlay. |
| `+` / `-` | Increase or decrease the refresh interval. |
| `r` | Refresh immediately. |
| `q` | Quit. |

In the state browser, `Up` / `Down` scroll the row selection, `PgUp` / `PgDn`
jump through rows, `/` starts a search filter, and `Enter` opens the selected
entry as an overlay. With an entry open, `Up` / `Down`, `PgUp` / `PgDn`, and
`Home` / `End` scroll the content preview. The browser uses the loaded snapshot
until you leave and re-enter it; periodic refresh is disabled there so
inspection does not jump. Interactive MCP calls run in the background and show a
short loading status instead of freezing key handling.

Render one snapshot and exit:

```bash
python3 monitor-metrics.py --once
```

Print refreshed snapshots without key handling:

```bash
python3 monitor-metrics.py --interval 5 --non-interactive
```

Monitor one known project or root:

```bash
python3 monitor-metrics.py --project-id my-repo-123abc
python3 monitor-metrics.py --root-uri file:///home/user/source/my-repo
```

The dashboard shows request volume, `context_pack` latency, cache hit bars,
estimated MCP-spared tokens, deferred reference bytes, and measurement-matrix
status.
The state browser calls `context_admin(mode="state_browser")` for the selected
project and shows bounded, redacted generated-state rows with searchable,
scrollable entry inspection.

Run the built-in offline benchmark through MCP/admin:

```bash
python3 benchmarks/context_pack_benchmark.py --repo .
```

The benchmark executes four deterministic pack runs: forced cold refresh, warm
cache reuse, repeated prompt reuse, and compact focused retrieval. It returns
per-run stage timings, token estimates, external tool-call savings, reference
bytes deferred, a compact contract sample, and the same measurement matrix used
by live metrics.

## Run With Docker Compose

Single-repo default from this checkout:

```bash
docker compose up --build
```

Global parent mode for multiple repositories under one host directory:

```bash
MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up --build
```

The image runs as non-root UID/GID `1000` by default. This matches the first
Ubuntu/WSL user and lets the container read owner-only repository files while
the source mount stays read-only. If your repository is owned by another user,
build with matching IDs:

```bash
MCP_CONTEXT_UID=$(id -u) MCP_CONTEXT_GID=$(id -g) docker compose up --build
```

If an existing named state volume was created with a different UID/GID, recreate
it after changing these build args:

```bash
docker compose down -v
MCP_CONTEXT_UID=$(id -u) MCP_CONTEXT_GID=$(id -g) docker compose up --build
```

This mounts the host parent read-only:

```text
Host:      /home/user/source
Container: /workspace-roots
Mapping:   /home/user/source=/workspace-roots
Allowed:   /home/user/source
```

An MCP root URI such as:

```text
file:///home/user/source/my-repo
```

is resolved inside the container as:

```text
/workspace-roots/my-repo
```

Request paths are then repository-relative, for example `src/app.py`.

## Multiple Roots

Multiple roots come from the MCP client via the MCP Roots protocol. Compose only
mounts and maps the allowed parent directory.

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

If the MCP client exposes `/home/user/source` as one root, the server treats that
whole directory as one project. For per-repo isolation, expose or pass roots like
`file:///home/user/source/my-repo`. When `REPO_PATH` is a configured parent such
as `/workspace-roots`, unqualified project-scoped calls require `root_uri` or
`project_id`; health remains available and reports that project selection is
required.

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

Useful HTTP endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Health and index status. |
| `GET /v1/mcp/tools` | Diagnostic list of advertised MCP tool names and transport endpoints. |
| `POST /v1/context/pack` | Direct HTTP fallback for context packs. Accepts `prompt` or `task`. |
| `GET /v1/context/references/{reference_id}` | Direct HTTP fallback for resolving result references. |
| `POST /mcp` | MCP Streamable HTTP endpoint. |
| `GET /legacy/sse` | MCP legacy SSE compatibility endpoint. |

## Configuration

| Variable | Purpose |
| --- | --- |
| `REPO_PATH` | Legacy single-project fallback root. |
| `MCP_CONTEXT_STATE_DIR` | Generated state directory. |
| `MCP_CONTEXT_ALLOWED_ROOTS` | Host paths allowed for MCP root URIs. Required for global roots outside `REPO_PATH`. |
| `MCP_CONTEXT_ROOT_MAPPINGS` | Host-to-container path mappings, such as `/home/user/source=/workspace-roots`. |
| `MCP_CONTEXT_HOST_ROOT` | Compose helper for the host parent mounted at `/workspace-roots`. |
| `MCP_CONTEXT_UID` / `MCP_CONTEXT_GID` | Compose build args for the non-root container user. Defaults to `1000:1000` for Ubuntu/WSL repository ownership. |
| `MCP_TRANSPORT` | `stdio` by default, or `streamable-http`. |
| `HOST` / `PORT` | HTTP bind settings. Compose binds the published port to localhost. |
| `MAX_READ_BYTES` | Maximum file bytes read for snippets/indexing. |
| `MAX_OUTPUT_CHARS` | Default output budget. |
| `MCP_CONTEXT_OUTPUT_PROFILE` | `compact`, `normal`, or `verbose`. |
| `MCP_CONTEXT_TOKEN_COUNTER` | `estimate` by default, or `target` to try an optional target tokenizer. |
| `MCP_CONTEXT_TARGET_TOKENIZER` | Target tokenizer name for `target` mode, defaulting to `cl100k_base`. |

## Generated State

In global mode, each selected project gets isolated state:

```text
<state_dir>/projects/<slug>-<root_hash>/store/
<state_dir>/projects/<slug>-<root_hash>/references/
```

Generated state holds the repository index, search term index, cache, metrics,
memory, budgets, and result-reference metadata. Large result references may use
the project `references/` area while public responses keep only stable
identifiers, hashes, TTLs, and resolver URIs. The root hash is derived from the
canonical MCP root URI. Generated state should not be committed unless it is an
intentional fixture or documented sample.

## Development

Run tests and lint:

```bash
python3 -m pytest
python3 -m ruff check .
```

The test suite is offline by default and uses temporary sample repositories.
