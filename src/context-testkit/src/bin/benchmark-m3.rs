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
    ensure!(
        general_warmup_response["schema"] == "context_index.warmup.v1"
            && general_warmup_response.get("search_cache").is_none(),
        "general warmup must initialize only the index"
    );

    let base_request = request(json!({
        "prompt": "Profile native context pack retrieval latency and identify the measured hot path",
        "focus_paths": ["src/context-core/src/lib.rs", "src/context-index/src/lib.rs"],
        "client_profile": "codex",
        "evidence_policy": "balanced"
    }))?;
    let full_bytes = engine.context_pack_generate(&base_request).await?;
    let full: ContextPackV2 = serde_json::from_slice(&full_bytes)?;
    let more = full
        .more
        .as_deref()
        .context("benchmark pack must carry an active more reference")?;
    ensure!(
        engine.store().reference_is_active(more)?,
        "benchmark reference must be active before repeated samples"
    );
    let mut repeated = Vec::new();
    for _ in 0..100 {
        let started = Instant::now();
        let bytes = engine.context_pack_generate(&base_request).await?;
        ensure!(
            bytes == full_bytes,
            "direct retrieval changed deterministic wire bytes"
        );
        repeated.push(started.elapsed().as_secs_f64() * 1_000.0);
    }

    let baseline: Value = serde_json::from_slice(&fs::read(
        root.join("benchmarks/corpora/native-quality.json"),
    )?)?;
    let cases = baseline["cases"]
        .as_array()
        .context("baseline cases must be an array")?;
    let mut direct_samples = Vec::new();
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
                "evidence_policy": "balanced"
            }))?;
            let started = Instant::now();
            let bytes = miss_engine.context_pack_generate(&pack_request).await?;
            direct_samples.push(started.elapsed().as_secs_f64() * 1_000.0);
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
    noise /= direct_samples.len() as f64;

    let mut delta_request = base_request.clone();
    delta_request.base_pack = Some(full.id.clone());
    let delta_bytes = engine.context_pack_generate(&delta_request).await?;
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

    let manual_state = tempfile::tempdir()?;
    let continuation_state = tempfile::tempdir()?;
    let manual_engine = ProjectEngine::build_with_state(&root, manual_state.path(), "manual-full")?;
    let continuation_engine =
        ProjectEngine::build_with_state(&root, continuation_state.path(), "continuation")?;
    let iterative_prompts = [
        "iterative context pack evidence reuse implementation testing review turn one",
        "iterative context pack evidence reuse implementation testing review turn two",
        "iterative context pack evidence reuse implementation testing review turn three",
    ];
    let mut manual_full_wire_tokens = 0_usize;
    let mut continuation_wire_tokens = 0_usize;
    let mut continuation_quality_equal = true;
    for prompt in iterative_prompts {
        let mut manual_request = base_request.clone();
        manual_request.prompt = prompt.to_owned();
        let manual_bytes = manual_engine.context_pack_generate(&manual_request).await?;
        let manual_pack: ContextPackV2 = serde_json::from_slice(&manual_bytes)?;
        manual_full_wire_tokens =
            manual_full_wire_tokens.saturating_add(token_estimate(&manual_bytes));

        let mut continuation_request = manual_request;
        continuation_request.memory_session = Some("benchmark-iterative-reuse".to_owned());
        let continuation_bytes = continuation_engine
            .context_pack_generate(&continuation_request)
            .await?;
        let continuation_pack: ContextPackV2 = serde_json::from_slice(&continuation_bytes)?;
        continuation_wire_tokens =
            continuation_wire_tokens.saturating_add(token_estimate(&continuation_bytes));
        continuation_quality_equal &=
            continuation_pack.id == manual_pack.id && continuation_pack.paths == manual_pack.paths;
    }
    let continuation_token_reduction = if manual_full_wire_tokens == 0 {
        0.0
    } else {
        1.0 - continuation_wire_tokens as f64 / manual_full_wire_tokens as f64
    };

    tokens.sort_unstable();
    let balanced_tokens_median = tokens[tokens.len() / 2];
    let mut repeated_ordered = repeated.clone();
    repeated_ordered.sort_by(f64::total_cmp);
    let repeated_pack_p50_ms = repeated_ordered[repeated_ordered.len() / 2];
    let summary = json!({
        "repeated_pack_p50_ms": repeated_pack_p50_ms,
        "repeated_pack_p95_ms": percentile_95(&repeated),
        "direct_pack_p95_ms": percentile_95(&direct_samples),
        "balanced_pack_tokens_median": balanced_tokens_median,
        "delta_reduction": delta_reduction,
        "required_anchor_recall": recall,
        "noise_ratio": noise,
        "general_warmup_ms": general_warmup_ms,
        "iterative_manual_full_wire_tokens_est": manual_full_wire_tokens,
        "iterative_continuation_wire_tokens_est": continuation_wire_tokens,
        "iterative_continuation_token_reduction": continuation_token_reduction,
        "iterative_continuation_quality_equal": continuation_quality_equal,
    });
    let gates = json!({
        "direct_pack_p95_lte_50ms": summary["direct_pack_p95_ms"].as_f64().unwrap() <= 50.0,
        "balanced_pack_median_lte_400_tokens": balanced_tokens_median <= 400,
        "delta_reduction_gte_80pct": delta_reduction >= 0.80,
        "required_anchor_recall_100pct": recall == 1.0,
        "noise_ratio_lte_30pct": noise <= 0.30,
        "general_warmup_lte_50ms": general_warmup_ms <= 50.0,
        "iterative_continuation_reduces_tokens": continuation_token_reduction > 0.0,
        "iterative_continuation_no_quality_regression": continuation_quality_equal,
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
