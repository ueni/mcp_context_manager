# Technical Paper: mcp-context-manager Implementation

## Abstract

`mcp-context-manager` is a project-scoped MCP server that reduces coding-agent
token use and request latency by returning compact, ranked repository context
instead of raw repository dumps. It keeps source access read-only, stores
generated state in an isolated project state directory, and preserves omitted
raw evidence through local result references.

The design goal is narrow: make the first repository-context call useful,
bounded, cacheable, and measurable. The server is not a general agent and does
not mutate source files.

## System Boundary

The server has five public tools:

- `context_pack`
- `context_lookup`
- `context_memory`
- `context_admin`
- `result_reference_resolve`

The implementation keeps public routing in `server.py`, project selection in
`manager.py` and `projects.py`, workflow logic in `context.py`, indexing in
`index.py`, storage in `store.py`, references in `references.py`, memory in
`memory.py`, metrics in `metrics.py`, schemas in `schemas.py`, and redaction /
classification helpers in `util.py`.

Source repositories are treated as read-only. The server writes generated state
only under the configured state directory. In multi-root mode, each repository
gets its own project state:

```text
<state_dir>/projects/<project_id>/store/
<state_dir>/projects/<project_id>/references/
```

## Transport And Public Surface

`server.py` builds the MCP surface with FastMCP and also exposes HTTP fallback
endpoints when `streamable-http` transport is enabled. The public MCP tool
descriptions are intentionally compact. Tool parameters use strict enums for
profiles, lookup modes, diagnostics levels, cache strategies, and admin modes.

The server instructions advertise an MCP-first workflow:

1. call `context_pack` before broad repository inspection;
2. use `context_lookup` for targeted follow-up;
3. resolve references before high-risk claims or destructive work;
4. use admin and memory tools only for their bounded purposes.

Resources expose bounded project data for clients that support MCP resources.
Tools-only clients can use `context_admin(mode="instructions")` and
`context_admin(mode="resource_proxy")` for equivalent access.

Reusable skill guidance uses the existing memory surface rather than a new
tool. Agents store non-secret skill records in `context_memory` namespaces such
as `skills/codex`, `skills/claude`, `skills/copilot`, or `skills/custom`. Raw
records are normal memory entries keyed by stable skill id; pre-summarized
records are normal memory summaries. During `context_pack`, matching non-expired
skill rows are compiled into deterministic compact cards and cached under the
internal `skill.compiled` cache namespace. The response includes optional
`skill_guidance` only when relevant cards exist, and never returns raw skill
bodies.

## Project Resolution

`ProjectContextService` routes every project-aware call to a project-local
`ContextService`. Project resolution is handled by `ProjectRegistry`:

1. explicit `root_uri`;
2. explicit `project_id`;
3. a single visible MCP root;
4. inference from path hints;
5. legacy `REPO_PATH` fallback only when safe.

Root URIs are canonicalized and checked against allowed roots. Docker global
mode uses host-to-container path mappings so a host URI such as
`file:///home/user/source/repo` can be resolved to a container path such as
`/workspace-roots/repo`. Project ids combine a slug with a root URI hash, which
keeps generated state isolated even when repositories have the same basename.

## Generated-State Storage

`ContextStore` wraps an LMDB adapter. Values are stored as JSON with stable key
prefixes such as:

- `index:file:`
- `index:symbol:`
- `index:import:`
- `index:term:`
- `cache:`
- `memory:`
- `metrics:`

LMDB gives transactional writes and prefix iteration while keeping the runtime
state local to the selected project. JSON values are serialized with sorted
keys, which helps deterministic cache entries and repeatable diagnostics.

## Repository Index

`ContextIndex` is responsible for bounded repository discovery and indexing.
It walks candidate files deterministically, skips symlinks, generated state,
binary files, excluded paths, files larger than `MAX_READ_BYTES`, and unknown
non-text extensions.

For each indexed file it stores:

- repository-relative path;
- size and mtime;
- content digest;
- language and extension;
- line count;
- compact summary payload;
- full text content for bounded lookup;
- normalized search terms;
- extracted symbols;
- extracted imports.

Python files use `ast` extraction for functions, classes, and imports. Other
text files use lightweight regular expressions for common function/class forms.

Index refresh avoids unnecessary work through two signatures:

- Git signature when the repository and worktree match the indexed path.
- File metadata signature when Git is unavailable or not applicable.

Scoped refresh is used for explicit paths named by the request. That lets a
changed file be refreshed without forcing a full repository walk.

## Context-Pack Pipeline

`ContextService.context_pack` is the main workflow. It is deterministic and
bounded:

1. Validate and normalize request parameters.
2. Refresh the repository index if stale, forced, fresh, or cold.
3. Resolve output profile from explicit `output_profile`, `client_profile`, and
   configured default.
4. Classify the task route.
5. Normalize query terms.
6. Collect explicit paths from prompt text, changed files, and focus paths.
7. Refresh explicit paths.
8. Retrieve compact memory for the route and session.
9. Build or reuse retrieval candidates.
10. Apply profile shaping and budget selection.
11. Store omitted/full evidence behind a result reference.
12. Compute token, latency, cache, and reference metrics.
13. Return a compact packet plus `omitted_ref` and `diagnostics_ref`.

The public result includes selected items with path, line hints, reason codes,
confidence, content, provenance, prompt-injection signals, and `detail_lookup`
instructions for full snippets. Raw prompt echo is disabled by default.
Volatile runtime metadata is omitted unless requested or required by the output
profile.

## Output Profiles And Client Profiles

Output profiles control how much information is returned inline:

- `minimal`: cache-stable schema, route, summary, items, references, and compact
  request metadata.
- `compact`: selected summaries plus compact metrics and cache summaries.
- `normal`: more diagnostics and runtime metadata.
- `verbose`: full diagnostics intended for inspection rather than normal agent
  loops.

Client profiles are request hints:

- `codex`
- `claude`
- `copilot`
- `generic`

The caller sets `client_profile` per request. The server does not detect the
host automatically. An explicit `output_profile` always wins. If
`output_profile` is omitted, `client_profile="codex"` uses `minimal`; other
clients use the configured default output profile.

`model_profile` is a provider-family hint and does not imply `client_profile`.
`context_admin(mode="profile_calibrate")` reports recommendations but does not
mutate server or session state.

## Candidate Retrieval And Ranking

Candidate retrieval combines several sources:

- explicit paths from user input;
- changed files;
- focus paths;
- term-index search hits;
- indexed symbols;
- import and related-symbol heuristics;
- test-owner heuristics;
- memory summaries and decisions.

The ranking model favors explicit paths and changed files, then route-relevant
terms, symbol matches, related files, likely tests, and diversity. Generated,
duplicate, stale, or low-score candidates are omitted with reason codes.

Route classification uses compact term heuristics for coding, review, debug,
test, docs, security, and general tasks. Route-specific ranking keeps context
shape appropriate, for example prioritizing changed files and tests for review
or stack-frame-like symbols and nearby tests for debug.

## Chunk And Fragment Cache

The server has both request-level retrieval caching and smaller reusable
fragments.

Request-level retrieval cache keys include route, normalized terms, explicit
paths, and refresh signature. This allows exact repeated context-pack retrieval
to skip candidate building.

Fragment cache entries support reuse across prompt variations:

- `retrieval.search_term` caches term search results.
- `retrieval.file_summary` caches summary fragments.

Chunk metadata splits files into fixed line windows. Each chunk records:

- schema;
- path;
- file_digest;
- chunk_id;
- start_line;
- end_line;
- content_digest;
- extractor_version;
- redaction_version;

This lets unchanged chunks keep their cached summaries even when unrelated files
or query terms change. Cache diagnostics expose hit ratios and miss reasons
through compact summaries and `diagnostics_ref`.

## Budget Planning

Context packs enforce a hard character budget and source-token budget before
final serialization. Candidate selection uses:

- maximum item count;
- per-profile content budget;
- per-path diversity limits;
- profile-specific content shaping.

Selected evidence is summarized. Bulky snippets, omitted candidates, retrieval
diagnostics, and full cache details are stored behind references. This avoids
building a huge JSON object and trimming after the fact.

## Result References

`ResultReferences` stores raw or bulky local evidence with metadata:

- reference id;
- producer tool;
- project id;
- created and expiry timestamps;
- content hash;
- size;
- sensitivity/redaction metadata;
- resolver instructions.

`result_reference_resolve` verifies project boundary, expiry, and optional
expected hash before returning the payload. The design keeps normal tool output
small while preserving evidence traceability.

## Memory Model

`context_memory` stores repository-local structured memory. It supports:

- relevant memory retrieval;
- fact upsert;
- summary upsert;
- decision records;
- validation;
- compaction.

Entries include namespace, source, confidence, created/updated timestamps, TTL,
tags, and provenance. Decision records include `decided_by`, with human
decisions treated as higher authority than model decisions.

Memory retrieval is constrained to namespace scope plus optional slices for
compact summaries and effective decisions. Prompt-facing context prioritizes
summaries and effective decisions over raw rows. Decision ordering in
`effective_decisions` favors human-vs-LLM source, then confidence, then most
recent update. Validation checks expired rows, stale/unsafe paths, and missing
metadata.

## Safety And Redaction

The safety boundary is repository-relative:

- path traversal and repository-escape paths are rejected at request boundaries;
- generated state is excluded from repository indexing;
- binary and oversized files are skipped;
- repository text is treated as untrusted and may be stored internally for
  retrieval;
- prompt-injection patterns are surfaced as metadata;
- secrets and host paths are redacted before model-facing summaries and
  snippets, and before memory/reference payloads are persisted. Internal index
  records may still contain raw repository text. Absolute host paths may be
  accepted when they resolve to in-repo files.

The model-facing context pack is useful for triage, but raw referenced evidence
must be inspected before destructive edits, release claims, or security
conclusions.

## Metrics And Evaluation

`ContextMetrics` records tool events, latency, cache hits, route counts, token
estimates, external-call savings, and reference bytes deferred. Metrics are
available through `context_admin(mode="metrics")` and summarized in
`repo://metrics`.

The measurement matrix checks:

- context-pack average and p95 latency;
- index refresh latency;
- estimated saved input tokens;
- compression ratio;
- candidate-to-selected ratio;
- cache hit ratios;
- external calls saved;
- reference bytes deferred.

Gold-anchor evaluation reads `benchmarks/gold_anchors/*.json` and verifies that
required paths or symbols appear in top-ranked context. It reports recall@3,
recall@5, first-anchor rank, noise ratio, required omissions,
detail-lookup resolution, stale-context rate, and regressions.

## HTTP And Monitor Support

The HTTP fallback exposes:

- health;
- advertised tool list;
- direct context-pack call;
- direct reference resolution;
- streamable MCP endpoint;
- legacy SSE endpoint.

`monitor-metrics.py` is an MCP client that lists projects, fetches metrics and
measurement matrices, and browses bounded generated state through admin tools.
It avoids direct state-file reads.

## Limitations

The server intentionally uses cheap, local heuristics. Symbol and relationship
extraction is not a full language server. Cache reuse depends on stable
signatures and bounded file reads. Client profiles are caller-provided hints,
not automatic host detection. MCP instructions can guide agents, but they cannot
technically force every model turn to call a tool.

These constraints keep the server fast, local, offline-testable, and focused on
compact repository context.
