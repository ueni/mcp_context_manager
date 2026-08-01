use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use context_core::{ContextAdminRequest, ContextPackRequest, ContextPackV2, ProjectEngine};
use serde_json::{Value, json};

const PROJECT_SCOPE: &str = "reference-spike";
const QUERIES: [(&str, &str); 2] = [
    ("TRACE request content cacheable", "rfc9110"),
    (
        "traceparent HTTP header vendor tracestate",
        "w3c-trace-context",
    ),
];

fn percentile(samples: &[f64], percentile: usize) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[(ordered.len() * percentile).div_ceil(100).saturating_sub(1)]
}

fn request(prompt: &str) -> ContextPackRequest {
    serde_json::from_value(json!({
        "prompt": prompt,
        "client_profile": "codex",
        "max_items": 4,
        "evidence_policy": "balanced",
        "cache_strategy": "fast"
    }))
    .expect("static benchmark request")
}

fn pack_hit(pack: &ContextPackV2, source_id: &str) -> bool {
    pack.paths
        .iter()
        .any(|path| path.starts_with(&format!("@corpus/{source_id}/")))
}

fn copy_repository_only(fixture: &Path, target: &Path) -> Result<()> {
    fs::write(
        target.join("repository-notes.txt"),
        fs::read(fixture.join("repository-notes.txt"))?,
    )?;
    Ok(())
}

async fn admin(engine: &ProjectEngine, mode: &str) -> Result<Value> {
    let request: ContextAdminRequest = serde_json::from_value(json!({"mode": mode}))?;
    Ok(serde_json::from_slice(
        &engine.context_admin(&request).await?,
    )?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let fixture = env::var_os("REFERENCE_CORPUS_FIXTURE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("benchmarks/fixtures/reference-corpus-spike"));
    ensure!(
        fixture.join("reference-corpus/manifest.json").is_file(),
        "fixture manifest missing"
    );

    let repository_root = tempfile::tempdir()?;
    copy_repository_only(&fixture, repository_root.path())?;
    let repository_state = tempfile::tempdir()?;
    let repository = ProjectEngine::build_with_state(
        repository_root.path(),
        repository_state.path(),
        PROJECT_SCOPE,
    )?;
    let corpus_state = tempfile::tempdir()?;
    let corpus = ProjectEngine::build_with_state(&fixture, corpus_state.path(), PROJECT_SCOPE)?;

    let mut repository_hits = 0usize;
    let mut corpus_hits = 0usize;
    let mut cold_ms = Vec::new();
    for (prompt, expected) in QUERIES {
        let state = tempfile::tempdir()?;
        let cold = ProjectEngine::build_with_state(&fixture, state.path(), PROJECT_SCOPE)?;
        let started = Instant::now();
        let pack: ContextPackV2 =
            serde_json::from_slice(&cold.context_pack_cached(&request(prompt)).await?)?;
        cold_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        corpus_hits += usize::from(pack_hit(&pack, expected));

        let repository_pack: ContextPackV2 =
            serde_json::from_slice(&repository.context_pack_cached(&request(prompt)).await?)?;
        repository_hits += usize::from(pack_hit(&repository_pack, expected));
    }

    let mut warm_ms = Vec::new();
    let mut repository_warm_ms = Vec::new();
    for (prompt, _) in QUERIES {
        corpus.context_pack_cached(&request(prompt)).await?;
        for _ in 0..20 {
            let started = Instant::now();
            corpus.context_pack_cached(&request(prompt)).await?;
            warm_ms.push(started.elapsed().as_secs_f64() * 1_000.0);

            let started = Instant::now();
            repository.context_pack_cached(&request(prompt)).await?;
            repository_warm_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        }
    }

    let stale_pack: ContextPackV2 = serde_json::from_slice(
        &corpus
            .context_pack_cached(&request("traceparent HTTP header vendor tracestate"))
            .await?,
    )?;
    let stale_source_signalled = stale_pack.evidence.iter().any(|card| {
        card.4.contains("source=w3c-trace-context") && card.4.contains("freshness=stale")
    });

    let metrics = admin(&corpus, "metrics").await?;
    let repository_index = admin(&repository, "index_status").await?;
    let corpus_index = admin(&corpus, "index_status").await?;
    let token_metrics = metrics
        .pointer("/tokens/context_pack")
        .context("context-pack token metrics")?;
    let repeat_request_cache_hit_rate = metrics
        .pointer("/cache/hit_ratio")
        .and_then(Value::as_f64)
        .context("cache hit ratio")?;
    let fixed_query_count = QUERIES.len();
    let corpus_hit_rate = corpus_hits as f64 / fixed_query_count as f64;
    let repository_only_hit_rate = repository_hits as f64 / fixed_query_count as f64;

    let report = json!({
        "schema": "governed_reference_corpus_spike.v1",
        "fixture": {
            "documents": ["RFC 9110", "W3C Trace Context"],
            "fixed_query_count": fixed_query_count,
            "top_k": 4
        },
        "quality": {
            "repository_only_top_k_hit_rate": repository_only_hit_rate,
            "corpus_top_k_hit_rate": corpus_hit_rate,
            "stale_source_signalled": stale_source_signalled
        },
        "latency_ms": {
            "cold_p50": percentile(&cold_ms, 50),
            "cold_p95": percentile(&cold_ms, 95),
            "warm_p50": percentile(&warm_ms, 50),
            "warm_p95": percentile(&warm_ms, 95)
        },
        "tokens": {
            "selected_source_est": token_metrics["selected_source_tokens_est"],
            "wire_est": token_metrics["wire_tokens_est"],
            "estimated_savings": token_metrics["saved_tokens_est"]
        },
        "repeat_request_cache_hit_rate": repeat_request_cache_hit_rate,
        "corpus_size_effects": [
            {
                "source_count": repository_index["reference_corpus"]["source_count"],
                "chunk_count": repository_index["reference_corpus"]["chunk_count"],
                "warm_p95_ms": percentile(&repository_warm_ms, 95)
            },
            {
                "source_count": corpus_index["reference_corpus"]["source_count"],
                "chunk_count": corpus_index["reference_corpus"]["chunk_count"],
                "warm_p95_ms": percentile(&warm_ms, 95)
            }
        ],
        "gates": {
            "corpus_top_k_hit_rate_100pct": corpus_hit_rate == 1.0,
            "repository_and_corpus_distinguishable": repository_only_hit_rate == 0.0,
            "repeat_cache_hit_rate_gte_90pct": repeat_request_cache_hit_rate >= 0.90,
            "stale_source_signalled": stale_source_signalled
        }
    });
    ensure!(
        report["gates"]
            .as_object()
            .is_some_and(|gates| { gates.values().all(|value| value.as_bool() == Some(true)) }),
        "one or more governed reference-corpus spike gates failed"
    );
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
