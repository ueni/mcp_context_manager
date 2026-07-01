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
ambiguous.

## MCP Resources

- `repo://summary`: workspace facts for an unambiguous project.
- `repo://file/{path}`: bounded file content for an unambiguous project.
- `repo://tree/{path}`: bounded tree listing for an unambiguous project.
- `repo://context/{reference_id}`: resolved result reference for an unambiguous
  project.
- `repo://metrics`: compact metrics for an unambiguous project.
- `repo://project/{project_id}/summary`
- `repo://project/{project_id}/file/{path}`
- `repo://project/{project_id}/tree/{path}`
- `repo://project/{project_id}/context/{reference_id}`
- `repo://project/{project_id}/metrics`

Metrics include request counts, cache hits and misses, estimated saved input
tokens, retrieval counts, route totals, and recent latency benchmarks. They are
stored under project-local generated state and do not include prompts, query
text, file contents, or secrets.

## Run With Docker Compose

Single-repo default from this checkout:

```bash
docker compose up --build
```

Global parent mode for multiple repositories under one host directory:

```bash
MCP_CONTEXT_HOST_ROOT=/home/user/source docker compose up --build
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
5. legacy `REPO_PATH` fallback when no MCP roots are available

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
`file:///home/user/source/my-repo`.

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

## Configuration

| Variable | Purpose |
| --- | --- |
| `REPO_PATH` | Legacy single-project fallback root. |
| `MCP_CONTEXT_STATE_DIR` | Generated state directory. |
| `MCP_CONTEXT_ALLOWED_ROOTS` | Host paths allowed for MCP root URIs. Required for global roots outside `REPO_PATH`. |
| `MCP_CONTEXT_ROOT_MAPPINGS` | Host-to-container path mappings, such as `/home/user/source=/workspace-roots`. |
| `MCP_CONTEXT_HOST_ROOT` | Compose helper for the host parent mounted at `/workspace-roots`. |
| `MCP_TRANSPORT` | `stdio` by default, or `streamable-http`. |
| `HOST` / `PORT` | HTTP bind settings. Compose binds the published port to localhost. |
| `MAX_READ_BYTES` | Maximum file bytes read for snippets/indexing. |
| `MAX_OUTPUT_CHARS` | Default output budget. |
| `MCP_CONTEXT_OUTPUT_PROFILE` | `compact`, `normal`, or `verbose`. |

## Generated State

In global mode, each selected project gets isolated state:

```text
<state_dir>/projects/<slug>-<root_hash>/index/context.sqlite3
<state_dir>/projects/<slug>-<root_hash>/memory/context_memory.json
<state_dir>/projects/<slug>-<root_hash>/cache/tool_cache.json
<state_dir>/projects/<slug>-<root_hash>/references/
<state_dir>/projects/<slug>-<root_hash>/reports/context_metrics.json
```

The root hash is derived from the canonical MCP root URI. Generated state should
not be committed unless it is an intentional fixture or documented sample.

## Development

Run tests and lint:

```bash
python3 -m pytest
python3 -m ruff check .
```

The test suite is offline by default and uses temporary sample repositories.
