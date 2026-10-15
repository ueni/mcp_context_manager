# Native Rust Architecture

## Abstract

`mcp-context-manager` 2.0 is a native Rust context authority for coding agents.
It turns a task, explicit paths, repository state, durable memory, and retrieval
evidence into a deterministic `context_pack.v2` response while keeping bulky
evidence behind authenticated local references. Source repositories remain
read-only.

The system optimizes four properties together: repository-boundary safety,
retrieval quality, compact output, and low warm-request latency. None of those
properties may be weakened to improve another.

## System boundary

The production workspace contains six components:

```text
RMCP stdio / Streamable HTTP / Axum REST
                    |
              ProjectRegistry
                    |
          Arc<ProjectEngine>
                    |
     change journal + freshness gate
                    |
        native Tantivy + chunk store
                    |
       deterministic selection/cards
                    |
        direct context_pack.v2 bytes
                    |
        async state/metrics writers
```

- `contextd` owns RMCP, Axum, lifecycle, configuration, HTTP security, and
  multi-project routing.
- `context-core` owns public contracts, selection, ranking, compact encoding,
  caches, freshness coordination, memory, and administrative behavior.
- `context-index` owns safe repository traversal, chunking, Tree-sitter
  extractors, Tantivy generations, and watchers.
- `context-store` owns the isolated versioned LMDB overlay, durable imports,
  references, wiki revisions, snapshots, and telemetry.
- `context-testkit` owns synthetic repositories and native acceptance
  benchmarks.
- `xtask` owns release versioning, dependency-license policy, and SBOM output.

The server is not a workflow runner, release orchestrator, or source mutation
service.

## Public contracts

Five context tools are public:

- `context_pack`
- `context_lookup`
- `context_memory`
- `context_admin`
- `result_reference_resolve`

Each mode declares its response schema. Cache statistics, pruning, and runtime
metrics use v2 diagnostic schemas; index warmup uses `context_index.warmup.v1`.
`context_pack` returns one compact v2 object:

```json
{
  "v": 2,
  "id": "pk_<digest>",
  "route": "debug",
  "paths": ["src/contextd/src/lib.rs"],
  "evidence": [
    ["ev_<digest>", 1, 301, 303, "run_http", "evidence card", 13]
  ],
  "more": "ctxref-<digest>"
}
```

MCP returns the object as one raw JSON text item. REST returns the same encoded
bytes. The tuple opcode is `0..2` for reference/balanced/source evidence and
`3..5` for add/drop/replace session deltas. Line intervals are inclusive.

Both transports require `prompt` to contain non-whitespace task text after
trimming. Empty and whitespace-only values are rejected before project loading
with the stable validation diagnostic `prompt is required`.

Stable diagnostics and accounting are queried through `context_admin` rather
than repeated in every response.

`context_admin(mode="contracts")` accepts the exact lowercase
`contract_profile` values `compact` and `verbose`. Omitting the field
deterministically selects `compact`; values are not trimmed or case-normalized,
and unsupported values are rejected before contract generation. The MCP input
schema exposes the two accepted values as an enum, and the response's top-level
`profile` always names the accepted profile used to produce it.

`context_admin(mode="warmup")` initializes the selected engine and waits for
known dirty or unverified index state to refresh. It returns
`context_index.warmup.v1` and does not construct a pack or seed retrieval.
`context_admin(mode="cache_stats")` returns `context_cache.stats.v2` for the
retained lookup cache. `cache_prune` returns `context_cache.prune.v2` after
pruning lookup entries, inactive deferred-reference mappings, and expired
references. These are diagnostic schemas; pack evidence tuples are unchanged.

`context_admin(mode="monitor_usage")` controls a process-global, default-off
30-day aggregate ledger. Version 4 buckets preserve deterministic day ordering
and the fixed-order client-profile dimension: `codex`, `claude`, `copilot`,
`generic`, `missing`, and `other`. Unknown non-empty values are coalesced into
`other`; raw profile values are not retained. Each profile bucket contains
request counts and bounded latency, input-to-wire, token-saving, explicit-delta,
and continuation totals. Route shares use the fixed `debug`, `review`,
`implementation`, and `explore` categories.

The public report envelope is `context_monitor_usage.report.v4`. It keeps the
newest rows while preserving chronological order, applies `max_entries`
(default 20) independently to usage buckets, rejection buckets, and each
returned client-profile collection, and applies `max_output_chars` (default
12,000) to serialized inline output. Up to 2,048 additional characters are
reserved solely for stable truncation counts, limits, reasons, and retrieval
metadata. Character-pressure removal is deterministic: oldest rejection rows
are removed first, then oldest usage rows. Any omitted evidence is stored as a
complete `context_monitor_usage.report.v3` payload in generated global-monitor
state behind a project-bound, hash-checked, 24-hour result reference. Report
generation and resolution never write repository source.

Only generated LMDB state is written. Only current configuration and bucket
schemas are accepted. Older generated configuration defaults to disabled;
older usage buckets are ignored and replaced as new requests are recorded.
No cache-hit counters or request-identity tracker are retained.

Rejected context-pack attempts use a separate process-global daily ledger with
only four stable classes: `schema`, `root_policy`, `project_selection`, and
`internal`. Raw prompts, responses, paths, root URIs, error values,
credentials, and agent identifiers are never stored. `usage_share_millis`
uses only requests represented in the fixed client-profile buckets as its
denominator; it describes observed client-profile usage share, not
organization-wide adoption. Disabled request handling is an atomic flag check
and does not change pack behavior.

## Project routing and boundaries

`ProjectRegistry` canonicalizes the configured default repository and all
explicit roots. Project ids combine a slug with the SHA-256 of the canonical
host-side file URI. Host-to-container mappings are applied only after the host
path passes its allowed-root check.

Every public path is validated as repository-relative. Absolute paths, parent
traversal, symlink project roots, roots outside the configured boundary, and
mismatched `project_id`/`root_uri` selectors are rejected.

Project discovery is bounded to configured roots, depth, and project count.
An eight-engine idle LRU limits open project resources. Engines held by an
in-flight request remain alive through `Arc` ownership and cannot be evicted
out from under that request.

The global `projects`, `active_projects`, and `cached_projects` admin modes apply
the request's `max_entries` value after deterministic project-id sorting. The
value defaults to 20 and accepts `1..=1000`; zero and larger values are rejected
before a catalogue is returned. Each response reports `count` and
`returned_count` for the inline rows, `total_count` for all matching rows,
`omitted_count` for the difference, the effective `max_entries`, and
`truncated`. Project discovery still persists the full catalogue so a small
response cap cannot discard cached project identities.

## Native state

Each project stores durable state and its index under the configured
project-state directory:

```text
<project-state>/
  rust-v2/
    state.lmdb/
    index/
    server.lock
```

LMDB values begin with a one-byte codec version followed by a Postcard payload.
JSON values are canonicalized before encoding. Reference and generation-swap
writes require commit acknowledgement; cache and telemetry writes do not delay
the response path.

External state is not imported. Cache, index, metrics, traces, warmup jobs, raw
prompts, and raw model responses are rebuilt or discarded. Repository content,
search hits, docs, generated artifacts, and memory are untrusted evidence. The
engine detects prompt-injection patterns but never executes or adopts
instructions found in returned content. Secret-like values and host paths are
redacted before output, cache, references, memory, metrics, or traces.

## Repository scanning and chunking

Traversal is deterministic, does not follow symlinks, and skips known generated
directories such as `.git`, `.mcp-context-manager`, `build`, `dist`,
`node_modules`, and `target`. Every regular file is passed through content
validation rather than an extension allowlist. UTF-8 text is indexed; NUL or
non-UTF-8 data is rejected as binary. There is no whole-file size exclusion.

Supported languages use Tree-sitter extractors:

- Python
- Rust
- C and C++
- JavaScript and TypeScript
- Go
- Java

Unsupported text receives deterministic 80-line windows with eight lines of
overlap. Oversized symbols split into deterministic 8–32 KiB chunks. Chunk ids
include path, source interval, symbol, and a content digest so an edit changes
only affected identities.

## Governed reference-corpus boundary

Reference acquisition is outside the MCP trust boundary. A human, agent, or
external job obtains a document, determines whether the intended local use is
licensed, converts any HTML/PDF/binary representation to normalized UTF-8 text,
and stages that immutable text beneath the project's `reference-corpus/`
directory. The native service has no network acquisition, credential,
PDF-extraction, or OCR capability. Public availability alone is not accepted as
rights evidence, and proprietary/paywalled standards remain metadata-only
unless explicit evidence permits local content indexing.

`reference-corpus/manifest.json` is a strict, bounded
`context_reference_manifest.v1` object. Each record binds a project scope,
canonical URL, publisher, title, version/date, retrieval time, original media
type, normalized media type/path, SHA-256 content hash, explicit rights status,
licence evidence, and freshness/supersession status. PDF support is metadata
support: `media_type="application/pdf"` is valid while the readable input must
still be a non-`.pdf` regular file declared as
`text/plain; charset=utf-8`. Missing/unrecognized rights, unknown fields,
unsupported media, malformed hashes, traversal, absolute paths, symlinks,
scope mismatch, binary/NUL input, and per-source/project bound violations fail
closed. `metadata_only` records cannot name readable content.

The regular repository walker excludes `reference-corpus/`. The corpus loader
validates it separately, then assigns indexed chunks synthetic
`@corpus/<source>/<hash-prefix>` paths. This keeps repository and corpus
evidence distinguishable while allowing one deterministic Tantivy ranking.
Equal content hashes are indexed once; metadata, freshness, path, version, and
content hashes feed the project refresh signature, while chunk ids include the
content digest. A changed or superseded manifest therefore invalidates affected
pack identities deterministically.

Only search-optimized chunks exist in the live index. Generated durable state
contains compact pack provenance and bounded deferred excerpts, never
the staged raw source or a raw full-text copy. Corpus index state has no
age-based expiry; it is invalidated when the governed source changes or the
project is removed. Ordinary bounded result references may expire under
their existing integrity policy without expiring the corpus index itself.

Corpus provenance is carried compactly in the evidence symbol: source id,
version, licence, freshness, and prompt-injection detection. Imported text is
always inert evidence; detection is a warning, never an instruction execution
path. A pack's `more` id resolves to a project-bound, hash-verified local
reference and exposes no host path. Agents search repository and corpus evidence
first, query targeted terms with `context_lookup(mode="search")`, inspect
freshness/licensing, and resolve deferred evidence only when needed. They advise
external acquisition and rights verification only when a suitable governed
source is absent.

The labelled RFC/W3C spike fixture and benchmark compare repository-only and
corpus-enabled fixed-query top-k hit rate. The recorded report includes cold and
warm p50/p95, aggregate selected-source and wire tokens, estimated savings,
stale signalling and the latency/chunk-count effect
at corpus sizes zero and two. These diagnostics do not change the stable compact
pack tuple; the `reference_corpus` block in index status is diagnostic.

## Tantivy retrieval

Each project holds a persistent Tantivy `IndexReader`. A committed writer
generation triggers one reader reload and an atomic `Arc<ProjectIndex>` swap;
requests do not reopen the index or hydrate complete files.

A request normalizes at most eight terms and issues one bounded Boolean query.
The initial candidate limit is `min(max(max_items * 8, 64), 256)`. It escalates
once to 512 only when explicit paths or required concepts are absent.

Explicit changed/focus paths are selected before lexical candidates. Without
explicit paths, deterministic scoring, source order, and one-hit-per-path
selection prevent duplicate evidence from crowding out coverage.

## Evidence cards and compression

Balanced evidence derives deterministic cards from the selected symbol:

- declaration or signature;
- guard or assertion;
- representative call;
- return/error behavior;
- state write;
- related test when available.

Overlapping intervals and gaps of at most three lines are merged before
encoding. Card token costs are computed once. Serialization writes directly to
a preallocated byte vector; there is no whole-response convergence loop.

When `base_pack` is supplied, the engine compares the previous evidence
snapshot with the current one and returns add/drop/replace deltas. Evidence ids
already listed in `known_evidence` are omitted.

### Explicit iterative continuation

`memory_session` is the opt-in continuation identifier for iterative packs. It
is never inferred from MCP, HTTP, or transport sessions. The project engine
hashes the validated caller value and persists a `context_pack.continuation.v1`
record in that project's generated LMDB state. A record contains only the prior
pack id, its bounded evidence-id set, index generation and refresh signature,
and update/expiry timestamps. Raw prompts, source text, session strings,
secrets, repository paths, and host paths are not stored.

On the next request with the same identifier, a valid record supplies the
effective `base_pack` and `known_evidence`. A request with an explicit
`base_pack` or non-empty `known_evidence` list uses that manual delta state
instead and reports `explicit_override`. Continuation records expire after 24
hours and are pruned to 256 entries per project. The same identifier in another
project resolves as project-local missing state; no global identity registry is
consulted.

Fallback is deterministic and visible in the response-level `reuse` object:
`missing`, `expired`, `stale_generation`, `stale_dependencies`, and
`missing_pack` all produce a full pack and advance the continuation only after
that pack succeeds. A generation change or pending source events invalidate
the derived delta state. `reused` reports valid derived delta
state. `delta_applied` and
`wire_tokens_avoided_est` are emitted once at the response level, not copied
into evidence cards.

Continuation requests are serialized by a per-project continuation gate across
state resolution, pack construction, and state advancement. This prevents two
concurrent turns from reading and then overwriting the same predecessor.
Every request retrieves and builds a pack. Replay advances from the latest
successful snapshot, while failed builds leave the record unchanged.

The direct native benchmark includes a three-turn implementation/test/review
sequence. It compares manual full-pack wire-token estimates with automatic
continuation output and gates on positive token reduction plus identical pack
ids and paths, establishing no retrieval-quality regression for the paired
turns. The retained issue run is
`benchmarks/results/issue-12-continuation-reuse.json`.

## Direct pack generation and retained caches

Each pack performs a new project-local indexed search followed by deterministic
selection and encoding. No encoded-response or retrieval-frontier cache is
consulted. Immutable pack snapshots support deltas and continuation only.
Deferred result references remain integrity-checked before reuse.

The short-lived `context_lookup` cache is bounded to 256 entries or 16 MiB with
a 30-second TTL. Its generation/signature-keyed `cache` response object and
metrics are diagnostic, not repository evidence. File snapshots, symbol
postings, Git-diff snapshots, and the shared wiki search index remain in use.

## Freshness

A `notify` watcher coalesces events for 50 ms and owns ordinary dirty-state
transitions. There is no fixed two-second request-path content hash. When the
native watcher is unavailable, a metadata-snapshot fallback traverses only
non-ignored regular files, does not follow symlinks, and compares bounded file
metadata fingerprints at a two-second interval by default. The interval can be
set with `MCP_CONTEXT_WATCH_POLL_INTERVAL_SECS` (1–300 seconds). Watcher
errors conservatively dirty the project. Explicit `changed_files` queues
verification. Pack requests wait for known pending changes or watcher-failure
verification before retrieval, using their cancellation control while waiting.
Watcher events schedule background refreshes, and a completed refresh atomically
publishes a new generation. Requests then read one complete published snapshot.
The diagnostic `freshness` string is `current`, `refreshing`, or `unverified`;
it describes retrieval completeness rather than a filesystem transaction.

Native or polling registration becomes healthy (or latches fail-closed) before
the baseline snapshot load/scan begins, eliminating the pre-registration
mutation gap. Watcher events advance a monotonic epoch under the changed-path
journal lock. Refresh consumes an epoch only after a stable initial signature,
a complete replacement build, a second authoritative source signature, and the
atomic generation swap. An epoch advance at any of those boundaries preserves
the journal and retries from the current source; repeated churn ends in the
bounded retryable freshness diagnostic instead of publishing stale evidence.

Thread-spawn failure, failure of both watcher backends, runtime watcher errors,
and event-channel disconnection mark the project for background full
verification. A later successful metadata snapshot may recover the fallback
backend, while the dirty/overflow journal remains fail-closed until that full
verification completes. Administrative metrics expose only bounded backend and
failure categories, without raw errors or paths. Available-snapshot lookup
responses report `unverified` while verification is pending. A failed strict
pack verification returns the retryable
`context_pack freshness unavailable` error.

Each project coalesces requests into one detached refresh worker. A separate
process-wide permit pool (`MCP_CONTEXT_REFRESH_CONCURRENCY`, default 2, capped
at 32) avoids deadlock with requests waiting under request execution permits.
Tantivy updates delete/add only changed-path documents; manual readers preserve
the prior committed view. A writer-generation guard rejects updates based on
an abandoned candidate, allowing a clean rebuild. Metadata copying, snapshot
persistence, and signature scans remain proportional to repository size.

One shared generated-tree policy is consumed by project discovery, watcher
filtering, signature traversal, and index traversal. This prevents churn under
`.workingdir/` and equivalent agent/build/cache trees from triggering refresh.

Lookup cache keys include the published generation and source signature.
Pack generation always performs indexed retrieval against a published snapshot.

## Blocking work, deadlines, and cancellation

The transport computes one `context_pack` deadline at request entry (48 seconds
by default) and reserves the final two seconds as a cooperative cancellation
grace. Queue acquisition, cold engine construction, signature scan, full index
build/refresh, Tantivy work, cache/state access, and serialization-heavy pack
construction run in `spawn_blocking` behind one configurable global semaphore.
The default permit count is `max(1, min(2, available_parallelism / 2))`.
Existing per-project construction and refresh locks remain the singleflight
boundary, so same-project cold callers share one build.

A request dropped while queued drops its semaphore future and never starts.
After a running request expires, its cancellation control is checked between
files and chunks and before initial index publication. Background refreshes
have independent lifetimes and may publish after a waiting request expires.
Cooperative repository phases terminate within
cancellation grace. If an unexpected operating-system or library phase
outlives that grace, the async request remains attached to the blocking join
until its permit and active-job drop guards have run; aborting a live
`spawn_blocking` closure cannot stop it and would abandon executor capacity.
The cancellation flag remains latched, so the closure cannot publish pack
snapshots, references, or initial index state. Permits and active/queued counters use drop
guards. Queue
exhaustion and deadline expiry return stable retryable diagnostics before the
usual 60-second MCP client budget. Phase measurements remain bounded and
content-free: the request ledger records refresh checks/updates, retrieval,
pack-build and total latency. Public runtime
measurements add queue wait, engine load, queue/active/singleflight depth,
cancellations, timeouts, deadline remaining, and post-timeout work; freshness
measurements add signature scan, full/incremental refresh, index-state I/O,
cache/state I/O, retrieval, and serialization totals. They contain neither
prompts, contents, secrets, nor raw local paths. Blocking-job tests assert
queue, waiter, permit, and active counts return to baseline.

The watcher maintains a coalesced 1,024-path journal. A refresh computes the
authoritative source signature, applies changed/deleted/renamed/untracked file
deltas to immutable chunks and fingerprints, builds a complete replacement
Tantivy reader, recomputes the authoritative source signature, verifies event
epoch and signature parity, persists a schema/version/generation snapshot by
sync-plus-rename, and only then swaps the `Arc` visible to readers while holding
the journal publication lock. Events from a consumed snapshot are cleared only
after that swap; newer events remain dirty. Overflow, unsafe or corpus paths,
ambiguous events, signature mismatch, mid-refresh mutation, missing state,
incomplete `.pending` state, and corrupt snapshots all use the documented
full-rebuild/retry fallback. Persisted snapshots are accepted only after current
source and fingerprint validation.

Transport startup schedules only the default project through the same bounded
path after stdio service creation or HTTP listener binding. The public runtime
state progresses through `queued`, `building`, and `ready`, or terminates at
`failed`; allowed roots are not enumerated by startup warmup.

## References and memory

Reference ids and payload hashes are verified before resolution. Expired,
tampered, foreign-project, traversal-bearing, or unavailable records return a
bounded status rather than raw data.

Memory supports facts, summaries, decisions, validation, and compaction under
project-local namespaces. Values are sanitized before persistence. Secrets,
bearer tokens, raw prompts, raw model responses, and host absolute paths are not
valid durable memory. `source_paths` is an additive write field accepting at
most 16 repository-relative paths; each write stores its opaque path-to-
fingerprint source basis. Pack `knowledge` is an additive stable field,
bounded to four records; only non-expired, non-redacted records whose stored
basis still matches indexed fingerprints are injected. An explicit upsert is
required to refresh a changed fact. Legacy rows without a basis are reported
as `unverified` and excluded from packs. Source validity is reported as
current, stale, or unverified by memory reads/validation, while cache and
freshness indicators remain diagnostic fields.

## Agent knowledge projection

The additive stable `context_pack.v2.wiki` field holds bounded source-linked
knowledge with owner project, applicability, freshness, and immutable reference
objects. Wiki and repository cards share the request item/source-token budgets.
`wiki_search` and `wiki_get`, typed `wiki_upsert`/`wiki_consolidate`, and the
administrative `wiki` actions extend existing tools without adding tool names.
`wiki_scope=all_authorized` is the search default; `selected_project` restricts
the active selector. Responses use `agent_wiki.*.v1` schemas. Page/source
identifiers, revision hashes, content, source status, and citations are stable;
generation counters, timings, and indexing counts are diagnostic.

LMDB stores immutable wiki revisions and compare-and-swap current pointers.
Consolidation checks and publishes every affected pointer in one transaction.
Old revisions remain addressable without TTL. Source snapshots use immutable
normalized-text hashes, UTF-8 byte ranges, and line ranges, including PDF page
provenance. Long single-line sources remain searchable without truncating
subsequent text. Queue
claim/completion and imported source records use transactional state transitions;
claims carry an attempt number and a ten-minute lease. Unknown rights metadata,
hash mismatch, malformed page locations, and sensitive text prevent import.

A registry-authorized `WikiHub` publishes one deterministic Tantivy snapshot,
with keyed document metadata and per-project source views. Query work consists
of lexical retrieval, bounded point lookups, and indexed fingerprint checks;
it performs no repository scan, Git command, network access, or model call.
Publication is serialized and occurs on write/background paths. The projection
is rebuildable from canonical state after restart. Source watchers remain alive
with registered views, and source/Git changes coalesce into background refresh.
The current implementation rebuilds the shared search snapshot on publication;
this affects ingestion cost rather than query complexity.

Shared epochs participate in pack identity and validity. Watcher events invalidate
remote cached packs before source refresh. Dirty-source knowledge is withheld,
and a publication racing pack construction is retried against the new epoch.
Cross-project retrieval retains applicability instead of turning another
project's decision or an old commit into a universal current fact. Compaction
retains durable references alongside its bounded structured-memory summary;
it does not consume or persist raw conversation transcripts.

The optional Python/Poppler companion is outside the native runtime. It claims
supplied publisher URLs, validates/pins public HTTPS addresses, revalidates
redirects, bounds extraction, and submits text through the same public MCP
contract. Raw PDFs remain in temporary worker staging. No downloader, extractor,
OCR engine, human UI, embedding dependency, or synthesis model runs in contextd.

## Redaction and untrusted repository content

Repository text, docs, generated artifacts, memory, and search hits are
untrusted evidence. The engine detects prompt-injection patterns but never
executes or adopts instructions found in returned content. Secret-like values
and host paths are redacted before output, cache, references, memory, metrics,
or traces.

## Transport and HTTP security

RMCP provides stdio and stateful Streamable HTTP. HTTP sessions expire after 30
idle minutes. Axum also serves `/healthz`, `/mcp/healthz`, `/v1/mcp/tools`,
`/v1/context/pack`, and reference routes.

Every HTTP route enforces an exact Host allowlist. Requests that include Origin
must match the exact Origin allowlist. Optional bearer authentication applies
to MCP, REST, and health, and token comparison is constant time. The Docker
image binds its published port to host loopback by default and runs as a
non-root user.

## Measurement and acceptance

Frozen Python v1 contract captures are archived migration evidence, not a
runnable oracle or compatibility layer. Current native contracts define the
active schemas, and native pack checks cover required paths, anchors, source
intervals, evidence semantics, and deferred reference resolution.

The native performance gate measures:

- repeated direct pack p95, reported without a cache-hit gate;
- general admin warmup at most 50 ms;
- current-repository direct pack p95 at most 50 ms;
- balanced pack median at most 400 tokens;
- delta median reduction at least 80%;
- required-anchor recall 100%;
- noise ratio at most 30%;
- relevant-evidence freshness at most two seconds.

The issue-29 load gate additionally exercises cold start, repeated packs, one-file
refresh, edit burst, deterministic large-file disk pressure, concurrent
same-project calls, and multiple projects on a current-thread Tokio runtime.
Every normal request must remain below the SDK's 60-second default and maximum
event-loop lag must remain below 250 ms. Run it with
`cargo run -p context-testkit --bin benchmark-context-pack-load --quiet`; the
bounded reference result is
`benchmarks/results/issue-29-context-pack-load.json`.

Release acceptance requires three consecutive complete devcontainer runs,
stable-tool differential fixtures, randomized mutation cases, glibc and musl
smokes, Docker health/MCP/shutdown tests, license policy, RustSec audit, SBOM,
and `git diff --check`.

## Packaging

Rust 1.97.1 and `Cargo.lock` are pinned. CMake invokes locked Cargo builds in
Debian and Alpine builders, validates executable target and linkage, and emits
dynamic glibc plus static PIE musl artifacts. The final Alpine image contains
the musl server, runtime certificates, curl, Git, and zlib. Git supports
read-only history ingestion and Git-diff snapshots; the server does not mutate
source repositories. No
Python interpreter or production Python dependency is present.

`cargo xtask release-version` updates the workspace, lockfile, and standalone
monitor contract. `cargo xtask license-check` enforces the dependency license
allowlist. `cargo xtask sbom` generates deterministic CycloneDX JSON. CI adds a
RustSec advisory scan, checksums, and Sigstore signing.
