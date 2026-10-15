# Direct pack generation validation

Removal base: `f094201a1cc64edf848ea443ed20358fe7fae04f`.
All builds, format checks, tests, and benchmarks ran in the repository
devcontainer at `/workspaces/mcp-context-manager`. The running MCP service was
not restarted. Historical benchmark and contract captures remain unchanged.

## Removed behavior

The encoded-response cache, local and persistent retrieval-frontier caches,
shared-worktree pool, lineage configuration, Moka dependency, cache admission
and hit tracking, prompt warming, and cache-only benchmarks are removed.
There are no compatibility aliases or migration readers. Old generated cache
records are left unused on disk.

Each pack now retrieves from the published index and builds its response.
Known dirty or unverified source state is refreshed before pack retrieval.
Lookup caching, file/index and Git-diff snapshots, wiki state, result
references, and explicit/automatic continuation remain in use. Startup warmup
initializes the index without constructing a synthetic pack.

Diagnostic contracts are `context_metrics.v2`, `context_cache.stats.v2`,
`context_cache.prune.v2`, and `context_index.warmup.v1`. The opt-in usage ledger
accepts only `context_monitor_usage.bucket.v4`; cache-specific request fields
and telemetry are removed. The compact pack contract remains v2.

## Functional validation

- Core: 58 tests pass without exclusions. Additional final checks exercise
  synchronized builds on eight native threads, dirty-index warmup without pack
  creation, and loaded/unloaded metrics.
- Server: 36 tests pass, including startup lifecycle, request cancellation,
  bounded execution, project isolation, wiki restoration, and public metrics.
- Store: six tests pass. Testkit/xtask targets and doc tests compile and pass.
- Monitor: 21 tests pass. PDF worker: seven tests pass, including real Poppler
  extraction and native HTTP ingestion/search/reference resolution.
- Release server and retained benchmark binaries build successfully. Native
  smoke passes stdio, HTTP, and transport security checks.
- Rustfmt and scoped Black checks pass. No Markdown/TOML formatter is configured;
  Cargo validates the manifests. `git diff --check` passes.

The preserved refresh tests now await publication explicitly. Cancellation
coverage checks that cancelling a waiting request leaves the background refresh
alive, then verifies the next pack sees the new source. Generation-race,
watcher-failure, reference-integrity, continuation, and wiki checks remain.

The full workspace test command still stops at the previously reproduced
`context-index::tests::symbol_postings_preserve_case_insensitive_substring_results`
failure (20 index tests pass, one fails). Strict workspace Clippy still fails
on the unchanged redundant `is_current_usage_bucket` closure. The existing
contextd unused-import/dead-field warnings also remain. A review check allowing
only these baseline lint categories passes; no source lint suppressions were added.

## Measured release behavior

Single-run observations, not new latency guarantees:

| Measurement | Result |
| --- | ---: |
| Repeated direct pack p50 / p95 | 1.03 / 1.61 ms |
| Labelled-query direct pack p95 | 10.47 ms |
| Persistent MCP repeated pack p95, usage enabled | 3.05 ms |
| Required-anchor recall / noise | 100% / 0% |
| Balanced pack median | 184 estimated tokens |
| Explicit delta reduction | 100% |
| Automatic continuation token reduction | 40.1% |
| Corpus repeated pack p95 / recall | 0.088 ms / 100% |
| Wiki search p95 at 100,000 chunks / identifier recall | 2.96 ms / 100% |
| Wiki search p95 during publication at 100,000 chunks | 3.29 ms |
| Load benchmark maximum event-loop lag | 1.22 ms |

All retained benchmark gates pass, including the 50 ms direct-pack gate,
continuation quality, SDK request budget, and event-loop responsiveness.
The formerly recorded cached-response p95 was 0.132 ms. Repeated requests now
pay for retrieval and encoding; the different code snapshots and execution
paths do not establish a controlled performance improvement or regression.

Reproduce with the release binaries after building:

```bash
cargo build --release -p contextd -p context-testkit --all-features --bins --offline
target/release/benchmark-m3
target/release/benchmark-context-pack-load
target/release/benchmark-reference-corpus
target/release/benchmark-wiki
python3 scripts/benchmark_native_m3.py target/release/mcp-context-manager \
  target/release/benchmark-m3 --output /tmp/native-m3.json --run 1 --monitor-usage
python3 scripts/smoke_native_mcp.py target/release/mcp-context-manager
MCP_WIKI_TEST_BINARY=target/release/mcp-context-manager \
  python3 -m unittest discover -s tests -p test_wiki_ingest_worker.py
```
