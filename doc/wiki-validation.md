# Agent wiki validation

Implementation base: `1dc471ea71ac3f5c7d4f43aba7c9afd245f30ce8`.
Validation runs in the repository devcontainer, with generated test state outside
the checkout. The running MCP service is not replaced by these checks.

## Functional checks

- Core wiki tests cover typed publication, immutable revision resolution,
  transactional consolidation, shared-project cache invalidation, item/source
  budgets, explicit source priority, compaction references, Git history, and
  governed datasheet reuse without acquisition.
- Store tests cover compare-and-swap conflicts, concurrent writers, immutable
  revisions, and existing references/codecs. Shared LMDB handles let a project
  engine reload while its wiki source view remains alive.
- Registry tests cover cross-project retrieval, rejected outside roots,
  citation-owner/root mismatch, relative-registration restoration, and eviction.
- Python worker tests use offline network mocks plus real two-page Poppler
  extraction. The optional native integration test drives claim, import, search,
  and citation resolution through Streamable HTTP. Publisher downloads are not
  exercised against a live external publisher.
- Native MCP smoke checks cover stdio, HTTP, and transport security.

Reproduce the feature checks with:

```bash
cargo test -p context-core wiki --all-features
cargo test -p context-index corpus --all-features
cargo test -p context-index wiki --all-features
cargo test -p context-store -p contextd --all-features
cargo build -p contextd --all-features
MCP_WIKI_TEST_BINARY=target/debug/mcp-context-manager \
  python3 -m unittest discover -s tests -p test_wiki_ingest_worker.py
python3 scripts/smoke_native_mcp.py target/debug/mcp-context-manager
```

Rust sources use rustfmt; the Python companion/tests use Black. No repository
Markdown/TOML formatter is configured. The Cargo manifest is validated by Cargo.

## Baseline failures

The unchanged base checkout reproduces these core failures:

- `cancelled_fresh_waiter_does_not_cancel_background_refresh`
- `detailed_usage_is_global_default_off_bounded_and_prompt_safe`
- `events_during_signature_scan_or_build_are_not_lost`
- `failed_watcher_forces_ordinary_full_freshness_verification`
- `fresh_changed_file_rebuilds_and_changes_evidence_identity`
- `l0_cache_uses_semantic_request_fields_after_freshness`
- `overflow_falls_back_to_full_and_readers_only_see_complete_generations`
- `reuse_state_is_restart_stable_discards_old_usage_schemas_redacted_and_bounded`

It also fails `packs_inject_only_current_source_linked_memory`, which this change
fixes by withholding dirty-source knowledge and binding writes to refreshed
source fingerprints. The unchanged index package reproduces
`symbol_postings_preserve_case_insensitive_substring_results`.

Consequently, a complete workspace test pass is not claimed. Strict Clippy also
finds existing dead-code, unused-import, and redundant-closure warnings. The
reviewed lint check permits only those categories:

```bash
CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --all-features -- \
  -D warnings -A dead_code -A unused_imports -A clippy::redundant_closure
```

## Performance

`benchmark-wiki` measures deterministic top-10 lexical retrieval across four
synthetic projects at 10,000 and 100,000 chunks, including queries while a
replacement snapshot is built and published. It checks identifier recall and
complete bounded results. This is an in-process search measurement, not network
latency or an end-to-end agent-quality evaluation. The first measured release
run returned 100% labelled identifier recall and warm p95 of about 0.25 ms and
2.1 ms respectively. The [recorded sample](../benchmarks/baselines/agent-wiki-native.json) includes
the final concurrent run: 100,000-chunk warm p95 3.76 ms and publication-time
p95 3.09 ms, with full identifier recall.

The pre-change native M3 benchmark passed ten gates but failed its existing
50 ms warm-miss gate at p95 94.93 ms. This pre-existing gate remains distinct
from the wiki search baseline. No absolute wiki SLA has been introduced.

The final core run passed 67 of 75 tests, with exactly the eight pre-existing
failures above. Store tests passed 6/6, routing/transport package tests 37/37,
focused core wiki tests 5/5, index wiki tests 2/2, corpus checks 4/4, and worker
checks 7/7. Native stdio/HTTP/security smoke checks passed. The reference-corpus
benchmark passed every gate.

| Native M3 measurement | Before | After |
| --- | ---: | ---: |
| Balanced pack median estimated tokens | 181 | 184 |
| L0 p95 ms | 0.181 | 0.132 |
| L1 p95 ms | 8.03 | 8.91 |
| Warm miss p95 ms | 94.93 | 88.56 |
| Required anchor recall | 100% | 100% |

These single-run comparisons are descriptive, not a statistical improvement
claim. The same ten M3 gates passed before and after; the warm-miss gate failed
both times. Formatter checks and reviewed lint passed. Strict Clippy remains
blocked by unchanged baseline warnings.

Shared index publication currently rebuilds the projection. Its cost grows with
corpus size and remains outside query execution. Rights, normalized-text bounds,
history batch limits, and the one-million-chunk projection cap still apply.
