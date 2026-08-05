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
      +-------------+----------------+
      |             |                |
  L0 wire cache  L1 frontier   change journal
      |             |                |
      +--------> ContextEngine <------+
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
  references, frontiers, snapshots, and telemetry.
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

The non-pack tools preserve their stable v1 schemas. `context_pack` is the only
intentional breaking contract and returns one compact v2 object:

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

Stable diagnostics and accounting are queried through `context_admin` rather
than repeated in every response.

`context_admin(mode="warmup")` performs one normal freshness refresh and
optionally admits the supplied prompt through the shared L0 path without
recording a user `context_pack` operation. It does not issue generic retrieval
seeds.
`context_admin(mode="cache_prune")` invalidates L0 and deletes persistent L1
rows in one bounded transaction, classifying each row once in expired-negative,
stale-signature, then age order. The response reports bounded counts and timing,
never the raw warmup prompt.

`context_admin(mode="monitor_usage")` controls a process-global, default-off
30-day aggregate ledger. Version 3 buckets preserve deterministic day ordering
and the fixed-order client-profile dimension: `codex`, `claude`, `copilot`,
`generic`, `missing`, and `other`. Unknown non-empty values are coalesced into
`other`; raw profile values are not retained. Each profile bucket contains
request counts and bounded latency, input-to-wire, token-saving, cache,
frontier, route, and delta-reuse totals plus integer-derived ratios. They also
contain fixed-name opportunity, effectiveness, miss-cause, and repeat-distance
counters. Route
shares use the fixed `debug`, `review`, `implementation`, and `explore`
categories.

The denominators are explicit:

- raw L0 hit rate is `L0 hits including singleflight / (L0 hits + L0 misses)`;
- exact effectiveness is `exact L0 hits / requests with an earlier retained
  semantic L0 identity in the same project` (an actual hit remains eligible if
  the bounded tracker has evicted its older sketch);
- frontier effectiveness is `frontier exact-or-negative hits / L0 misses or
  uncached packs with an earlier retained canonical term/scope identity`;
- delta adoption is `requests using base-pack or known-evidence input / requests
  with an earlier related exact or frontier identity`; and
- lineage opportunity is a count, not a hit rate: a matching exact or frontier
  sketch was observed under another project id with the same Git common-dir
  lineage sketch.

Lineage detection does not reuse cache entries, evidence, source state, or
project identifiers. It derives a versioned salted sketch from the local Git
common directory, so unrelated repositories are not joined. Non-Git roots use
their own canonical root only as ephemeral hash input. The host value is never
stored.

Each L0 miss receives exactly one cause in this precedence order: an earlier
exact identity outside the 30-minute idle window is `expired`; a changed source
signature is `invalidated_generation_or_signature`; an earlier exact identity
with the same signature is `cold_or_restart`; otherwise evidence-state,
scope/options, and canonical request variants are distinguished before the
remaining cold case. Repeat distances are stored only as 15-minute slots and
reported as same-slot, within the 30-minute idle window, 30 minutes to two
hours, two hours to one day, or one to 30 days.

Only generated LMDB state is written. The identity tracker retains at most
2,048 entries and 30 days. It stores 96-bit SHA-256 sketches under a persisted
128-bit salt and a version tag; raw prompts, source text, paths, repository
URLs, client values, evidence values, and precise timestamps are absent.
Configuration v1 and bucket v1/v2 data are read compatibly and reported as v3
with zero opportunity counters for history that predates these measurements.

Rejected context-pack attempts use a separate process-global daily ledger with
only four stable classes: `schema`, `root_policy`, `project_selection`, and
`internal`. Raw prompts, responses, paths, root URIs, error values,
credentials, and agent identifiers are never stored. `usage_share_millis`
uses only requests represented in the fixed client-profile buckets as its
denominator; it describes observed client-profile usage share, not
organization-wide adoption. Disabled request handling is an atomic flag check
and does not change cache behavior.

The replayable `benchmark-reuse-opportunity` testkit binary covers exact
repeats, unique requests, idle expiry, source invalidation, engine restart, and
linked-worktree variants. Its synthetic repeat hit rate validates cache
mechanics only; production adoption decisions must use opportunity-normalized
monitor data.

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
accepted range is `1..=1000`; zero and larger values are rejected before a
catalogue is returned. Each response reports `count` and `returned_count` for
the inline rows, `total_count` for all matching rows, `omitted_count` for the
difference, the effective `max_entries`, and `truncated`. Project discovery
still persists the full catalogue so a small response cap cannot discard cached
project identities.

## Rollback-safe state

Rust never opens the Python v1 LMDB environment for writing. Each project uses
an adjacent overlay:

```text
<project-state>/
  store/context.lmdb/       # Python v1, untouched
  references/               # Python v1, untouched
  rust-v2/
    state.lmdb/
    index/
    server.lock
```

LMDB values begin with a one-byte codec version followed by a Postcard payload.
JSON values are canonicalized before encoding. Reference and generation-swap
writes require commit acknowledgement; cache and telemetry writes do not delay
the response path.

The v1 importer opens the source with LMDB read-only, no-lock, and no-read-ahead
flags. It imports only durable memory and unexpired references. Cache, index,
metrics, traces, warmup jobs, raw prompts, and raw model responses are rebuilt
or discarded. Imported records retain their project and reference ids. A
canonical digest and reconciled source/imported/expired/invalid counts make the
operation idempotent and auditable.

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
frontiers and packs deterministically without cross-project reuse.

Only search-optimized chunks exist in the live index. Generated durable state
contains compact pack/frontier provenance and bounded deferred excerpts, never
the staged raw source or a raw full-text copy. Corpus-derived index/frontier
state has no age-based expiry (`expires_at_ms=0` for positive frontiers); it is
removed only by deterministic source invalidation, explicit project removal,
or administrative pruning. Ordinary bounded result references may expire under
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
repeat-request L0 hit rate, stale signalling, and the latency/chunk-count effect
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
`missing`, `expired`, `stale_generation`, and `missing_pack` all produce a full
pack and advance the continuation only after that pack succeeds. `reused`
reports valid derived delta state. `delta_applied` and
`wire_tokens_avoided_est` are emitted once at the response level, not copied
into evidence cards.

Continuation requests are serialized by a per-project continuation gate across
state resolution, pack construction, and state advancement. This prevents two
concurrent turns from reading and then overwriting the same predecessor.
Continuation responses are not admitted to L0 because their meaning advances
per successful turn; ordinary and explicit-delta requests retain Moka
singleflight. Replay therefore advances from the latest successful snapshot,
while failed builds leave the record unchanged.

The direct native benchmark includes a three-turn implementation/test/review
sequence. It compares manual full-pack wire-token estimates with automatic
continuation output and gates on positive token reduction plus identical pack
ids and paths, establishing no retrieval-quality regression for the paired
turns. The retained issue run is
`benchmarks/results/issue-12-continuation-reuse.json`.

## Cache hierarchy

### L0 wire cache

L0 is a 64 MiB weighted Moka future cache of immutable encoded response bytes,
reference ids, and a validity certificate. It uses 30-minute time-to-idle
expiry and `try_get_with` singleflight, so concurrent identical misses perform
one build. Every apparent hit revalidates its generation, refresh signature,
and referenced bodies (existence, expiry, and content hash) before serving the
cached bytes.

### L1 frontier cache

L1 stores ranked frontiers rather than final responses. The in-memory budget is
128 MiB and the persistent LMDB budget is 256 MiB. A frontier records ordered
chunk ids, scores, cumulative token costs, dependencies, canonical terms and
scope, source generation, candidate capacity, and the cutoff.

Positive frontiers are admitted only after two identical observations within
30 minutes. Serving is exact-only and requires matching canonical terms, scope,
generation, and sufficient candidate capacity; route is diagnostic only.
Missing source candidates fall back to a new search. Negative entries expire
after 30 seconds and are also exact-only.

### Governed cross-worktree frontier pool

Cross-worktree reuse is opt-in through an operator-owned lineage manifest; no
request field, repository name, directory name, remote URL, or repository file
can create membership. The governance digest is combined with the canonical
Git common-directory digest, so assigning the same manifest id to unrelated
repositories cannot join them. Eligibility is re-proved for every lookup and
publication: the worktree must be clean, its top level must equal the selected
project root, and its commit, source signature, and deterministic index
signature must match exactly. Governed reference corpora are always excluded.

The shared 256 MiB persistent pool stores immutable positive frontier records
under content-addressed keys. Candidate and dependency identities are SHA-256
digests of project-local chunk ids. Shared records contain no prompts, raw
terms, paths, project ids, generations, pack snapshots, references,
continuations, memory, metrics, redaction state, or source text. Negative
frontiers are never shared.

On an apparent hit, the requester verifies capacity, record shape, every
candidate and dependency address, its own source/index signatures, and a
stable local generation and Git proof. It reranks the candidates using its own
index. Any missing/deleted candidate, dirty worktree, divergent head, changed
file, stale index, governed corpus, or concurrent generation change causes a
deterministic project-local search. Project-local L1 remains authoritative and
is checked first.

`context_admin(mode="metrics")` reports eligible lineage opportunities, safe
hits, fallbacks, estimated search latency saved, aggregate isolation
rejections, and fixed rejection-reason counters. The latency estimate is
project-local: it subtracts the requesting lookup time from the accepted 50 ms
warm-miss envelope; producer timing is never stored in a shared record.
`cache_stats` reports the
shared budget without keys or paths. `cache_prune` applies the requested age to
shared generated records as well as local cache state; reopening the global
state reloads records within the same fixed budget.
The opt-in usage monitor from the reuse-opportunity work records the same five
bounded lineage totals under `lineage_frontier`, globally and per normalized
client profile, without prompts, paths, ids, or source values.

## Freshness

A `notify` watcher coalesces events for 50 ms. A content signature poll is the
fallback when an event is unavailable. Fast-cache validity is bounded to two
seconds. `changed_files` and `cache_strategy="fresh"` refresh synchronously and
cannot return a response certified against an older generation.

Generation, source signature, and explicit-path signatures participate in
cache validity. A cached response is served only when its certificate still
matches the active index.

## References and memory

Reference ids and payload hashes are verified before resolution. Expired,
tampered, foreign-project, traversal-bearing, or unavailable records return a
bounded status rather than raw data. Imported external reference bodies are
inlined only after path validation against the Python reference directory.

Memory supports facts, summaries, decisions, validation, and compaction under
project-local namespaces. Values are sanitized before persistence. Secrets,
bearer tokens, raw prompts, raw model responses, and host absolute paths are not
valid durable memory.

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

The frozen Python v1 oracle defines stable non-pack schemas. Native pack
differentials compare required paths, anchors, source intervals, evidence
semantics, and deferred reference resolution rather than v1 envelope bytes.

The native performance gate measures:

- L0 server p95 at most 2 ms;
- general admin warmup at most 50 ms;
- a prompt admitted by admin warmup returns through L0 at most 2 ms;
- local MCP L0 p95 at most 10 ms;
- L1 p95 at most 15 ms;
- current-repository warm miss p95 at most 50 ms;
- balanced pack median at most 400 tokens;
- delta median reduction at least 80%;
- required-anchor recall 100%;
- noise ratio at most 30%;
- relevant-evidence freshness at most two seconds.

Release acceptance requires three consecutive complete devcontainer runs,
stable-tool differential fixtures, randomized mutation cases, glibc and musl
smokes, Docker health/MCP/shutdown tests, license policy, RustSec audit, SBOM,
and `git diff --check`.

## Packaging

Rust 1.97.1 and `Cargo.lock` are pinned. CMake invokes locked Cargo builds in
Debian and Alpine builders, validates executable target and linkage, and emits
dynamic glibc plus static PIE musl artifacts. The final Alpine image contains
the musl server, runtime certificates, curl, Git, and zlib. Git is used only for
shell-free, read-only governed-worktree lineage proofs with
`GIT_OPTIONAL_LOCKS=0`; it does not authorize source-repository mutation. No
Python interpreter or production Python dependency is present.

`cargo xtask release-version` updates the workspace, lockfile, and standalone
monitor contract. `cargo xtask license-check` enforces the dependency license
allowlist. `cargo xtask sbom` generates deterministic CycloneDX JSON. CI adds a
RustSec advisory scan, checksums, and Sigstore signing.
