use std::{env, fs, path::PathBuf, time::Instant};

use anyhow::{Context, Result, ensure};
use context_core::{ContextAdminRequest, ContextPackRequest, ContextPackV2, ProjectEngine};
use serde_json::{Value, json};

fn percentile_95(samples: &[f64]) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[(ordered.len() * 95).div_ceil(100).saturating_sub(1)]
}

fn request(value: Value) -> Result<ContextPackRequest> {
    serde_json::from_value(value).context("build context_pack request")
}

fn token_estimate(bytes: &[u8]) -> usize {
    bytes.len().div_ceil(4)
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = env::var_os("REPO_PATH")
        .map(PathBuf::from)
        .map_or_else(env::current_dir, Ok)?;
    let state = env::var_os("MCP_CONTEXT_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/mcp-context-native-m3-direct"));
    let engine = ProjectEngine::build_with_state(&root, &state, "benchmark")?;
    let general_warmup: ContextAdminRequest = serde_json::from_value(json!({"mode": "warmup"}))?;
    let general_warmup_started = Instant::now();
    let general_warmup_response: Value =
        serde_json::from_slice(&engine.context_admin(&general_warmup).await?)?;
    let general_warmup_ms = general_warmup_started.elapsed().as_secs_f64() * 1_000.0;
    let general_warmup_retrieval_ms = general_warmup_response["stage_timings_ms"]["retrieval"]
        .as_f64()
        .context("general warmup retrieval timing")?;
    ensure!(
        general_warmup_response["search_cache"]["query_count"]
            .as_u64()
            .is_some_and(|count| count == 0),
        "general warmup unexpectedly executed generic retrieval seeds"
    );

    let prompt_warmup_text = "benchmark prompt warmup exact cache admission";
    let prompt_warmup: ContextAdminRequest = serde_json::from_value(json!({
        "mode": "warmup",
        "prompt": prompt_warmup_text,
        "path": "src/context-core/src/lib.rs"
    }))?;
    let prompt_warmup_started = Instant::now();
    engine.context_admin(&prompt_warmup).await?;
    let prompt_warmup_ms = prompt_warmup_started.elapsed().as_secs_f64() * 1_000.0;
    let prompt_request = request(json!({
        "prompt": prompt_warmup_text,
        "focus_paths": ["src/context-core/src/lib.rs"]
    }))?;
    let prompt_l0_started = Instant::now();
    engine.context_pack_cached(&prompt_request).await?;
    let post_prompt_warmup_l0_ms = prompt_l0_started.elapsed().as_secs_f64() * 1_000.0;
    let base_request = request(json!({
        "prompt": "Profile native context pack retrieval latency and identify the measured hot path",
        "focus_paths": ["src/context-core/src/lib.rs", "src/context-index/src/lib.rs"],
        "client_profile": "codex",
        "evidence_policy": "balanced",
        "cache_strategy": "fast"
    }))?;
    let full_bytes = engine.context_pack_cached(&base_request).await?;
    let full: ContextPackV2 = serde_json::from_slice(&full_bytes)?;
    let more = full
        .more
        .as_deref()
        .context("benchmark pack must carry an active more reference")?;
    ensure!(
        engine.store().reference_is_active(more)?,
        "benchmark more reference must be active before L0 samples"
    );

    let mut l0 = Vec::new();
    for _ in 0..100 {
        let started = Instant::now();
        let bytes = engine.context_pack_cached(&base_request).await?;
        ensure!(bytes == full_bytes, "L0 changed deterministic wire bytes");
        l0.push(started.elapsed().as_secs_f64() * 1_000.0);
    }

    let mut l1 = Vec::new();
    for iteration in 0..40 {
        let mut related = base_request.clone();
        related.known_evidence = vec![format!("client-held-{iteration}")];
        let started = Instant::now();
        engine.context_pack_cached(&related).await?;
        l1.push(started.elapsed().as_secs_f64() * 1_000.0);
    }

    let baseline: Value = serde_json::from_slice(&fs::read(
        root.join("benchmarks/corpora/native-quality.json"),
    )?)?;
    let cases = baseline["cases"]
        .as_array()
        .context("baseline cases must be an array")?;
    let mut warm_misses = Vec::new();
    let mut tokens = Vec::new();
    let mut recall = 1.0_f64;
    let mut noise = 0.0_f64;
    for repetition in 0..4 {
        let miss_state = tempfile::tempdir()?;
        let miss_engine = ProjectEngine::build_with_state(
            &root,
            miss_state.path(),
            format!("miss-benchmark-{repetition}"),
        )?;
        for case in cases {
            let pack_request = request(json!({
                "prompt": case["prompt"],
                "focus_paths": case["focus_paths"],
                "client_profile": "codex",
                "evidence_policy": "balanced",
                "cache_strategy": "fast"
            }))?;
            let started = Instant::now();
            let bytes = miss_engine.context_pack_cached(&pack_request).await?;
            warm_misses.push(started.elapsed().as_secs_f64() * 1_000.0);
            tokens.push(token_estimate(&bytes));
            let pack: ContextPackV2 = serde_json::from_slice(&bytes)?;
            let required = case["required_paths"]
                .as_array()
                .context("required paths")?;
            let allowed = case["allowed_paths"].as_array().context("allowed paths")?;
            let case_recall = required
                .iter()
                .filter(|path| {
                    path.as_str()
                        .is_some_and(|path| pack.paths.iter().any(|item| item == path))
                })
                .count() as f64
                / required.len() as f64;
            recall = recall.min(case_recall);
            let noisy = pack
                .paths
                .iter()
                .filter(|path| !allowed.iter().any(|allowed| allowed.as_str() == Some(path)))
                .count();
            noise += noisy as f64 / pack.paths.len().max(1) as f64;
        }
    }
    noise /= warm_misses.len() as f64;

    let mut delta_request = base_request.clone();
    delta_request.base_pack = Some(full.id.clone());
    let delta_bytes = engine.context_pack_cached(&delta_request).await?;
    let delta: ContextPackV2 = serde_json::from_slice(&delta_bytes)?;
    let full_evidence_tokens = full
        .evidence
        .iter()
        .map(|card| u64::from(card.6))
        .sum::<u64>();
    let delta_evidence_tokens = delta
        .evidence
        .iter()
        .map(|card| u64::from(card.6))
        .sum::<u64>();
    let delta_reduction = if full_evidence_tokens == 0 {
        1.0
    } else {
        1.0 - delta_evidence_tokens as f64 / full_evidence_tokens as f64
    };

    let mutation_root = tempfile::tempdir()?;
    let mutation_state = tempfile::tempdir()?;
    let mutation_path = mutation_root.path().join("fresh.py");
    fs::write(&mutation_path, "def fresh_anchor():\n    return 1\n")?;
    let mutation_engine =
        ProjectEngine::build_with_state(mutation_root.path(), mutation_state.path(), "mutation")?;
    let mutation_request = request(json!({
        "prompt": "fresh_anchor",
        "focus_paths": ["fresh.py"]
    }))?;
    let before: ContextPackV2 = serde_json::from_slice(
        &mutation_engine
            .context_pack_cached(&mutation_request)
            .await?,
    )?;
    fs::write(
        &mutation_path,
        "def fresh_anchor():\n    if True:\n        return 2\n",
    )?;
    let mut fresh_request = mutation_request;
    fresh_request.changed_files = vec!["fresh.py".to_owned()];
    fresh_request.cache_strategy = context_core::CacheStrategy::Fresh;
    let refresh_started = Instant::now();
    let after: ContextPackV2 =
        serde_json::from_slice(&mutation_engine.context_pack_cached(&fresh_request).await?)?;
    let freshness_ms = refresh_started.elapsed().as_secs_f64() * 1_000.0;
    ensure!(
        before.id != after.id,
        "fresh request returned stale evidence"
    );

    tokens.sort_unstable();
    let balanced_tokens_median = tokens[tokens.len() / 2];
    let summary = json!({
        "l0_server_p95_ms": percentile_95(&l0),
        "l1_p95_ms": percentile_95(&l1),
        "warm_miss_p95_ms": percentile_95(&warm_misses),
        "balanced_pack_tokens_median": balanced_tokens_median,
        "delta_reduction": delta_reduction,
        "required_anchor_recall": recall,
        "noise_ratio": noise,
        "freshness_ms": freshness_ms,
        "general_warmup_ms": general_warmup_ms,
        "general_warmup_retrieval_ms": general_warmup_retrieval_ms,
        "prompt_warmup_ms": prompt_warmup_ms,
        "post_prompt_warmup_l0_ms": post_prompt_warmup_l0_ms,
    });
    let gates = json!({
        "l0_server_p95_lte_2ms": summary["l0_server_p95_ms"].as_f64().unwrap() <= 2.0,
        "l1_p95_lte_15ms": summary["l1_p95_ms"].as_f64().unwrap() <= 15.0,
        "warm_miss_p95_lte_50ms": summary["warm_miss_p95_ms"].as_f64().unwrap() <= 50.0,
        "balanced_pack_median_lte_400_tokens": balanced_tokens_median <= 400,
        "delta_reduction_gte_80pct": delta_reduction >= 0.80,
        "required_anchor_recall_100pct": recall == 1.0,
        "noise_ratio_lte_30pct": noise <= 0.30,
        "freshness_lte_2s": freshness_ms <= 2_000.0,
        "general_warmup_lte_50ms": general_warmup_ms <= 50.0,
        "post_prompt_warmup_l0_lte_2ms": post_prompt_warmup_l0_ms <= 2.0,
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "rust_native_milestone_3.direct.v1",
            "summary": summary,
            "gates": gates,
        }))?
    );
    ensure!(
        gates
            .as_object()
            .is_some_and(|items| items.values().all(|value| value.as_bool() == Some(true))),
        "one or more direct Milestone 3 gates failed"
    );
    Ok(())
}
