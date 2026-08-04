use std::{fs, path::Path, process::Command, sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use context_core::{
    ContextAdminRequest, ContextPackRequest, ContextPackV2, GovernedFrontierLineage, ProjectEngine,
    SharedFrontierCache, UsageMonitor,
};
use serde_json::{Value, json};

fn git(root: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git")
        .env("GIT_AUTHOR_DATE", "2026-08-04T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-08-04T00:00:00Z")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()?;
    ensure!(output.status.success(), "Git fixture command failed");
    Ok(())
}

fn request(variant: &str) -> Result<ContextPackRequest> {
    serde_json::from_value(json!({
        "prompt": "review governed frontier handoff anchor",
        "focus_paths": ["src/lib.rs"],
        "known_evidence": [variant],
        "client_profile": "codex",
    }))
    .context("benchmark request")
}

fn engine(
    root: &Path,
    state: &Path,
    project_id: &str,
    pool: Arc<SharedFrontierCache>,
) -> Result<ProjectEngine> {
    ProjectEngine::build_with_governed_frontiers(
        root,
        state,
        project_id,
        Arc::new(UsageMonitor::open(state.join("monitor"))?),
        GovernedFrontierLineage::new("b".repeat(64))?,
        pool,
    )
}

async fn metrics(engine: &ProjectEngine) -> Result<Value> {
    let request: ContextAdminRequest = serde_json::from_value(json!({"mode": "metrics"}))?;
    Ok(serde_json::from_slice(
        &engine.context_admin(&request).await?,
    )?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let builder_root = fixture.path().join("builder");
    let verifier_root = fixture.path().join("verifier");
    let gatekeeper_root = fixture.path().join("gatekeeper");
    fs::create_dir_all(builder_root.join("src"))?;
    fs::write(
        builder_root.join("src/lib.rs"),
        "pub fn governed_frontier_handoff_anchor() { assert!(true); }\n",
    )?;
    git(&builder_root, &["init", "--quiet"])?;
    git(&builder_root, &["config", "user.name", "Benchmark"])?;
    git(
        &builder_root,
        &["config", "user.email", "benchmark@example.test"],
    )?;
    git(&builder_root, &["add", "src/lib.rs"])?;
    git(
        &builder_root,
        &["commit", "--quiet", "-m", "benchmark fixture"],
    )?;
    git(
        &builder_root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            verifier_root.to_str().context("verifier path")?,
        ],
    )?;
    git(
        &builder_root,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            gatekeeper_root.to_str().context("gatekeeper path")?,
        ],
    )?;

    let state = tempfile::tempdir()?;
    let pool = Arc::new(SharedFrontierCache::open(state.path().join("shared"))?);
    let builder = engine(
        &builder_root,
        &state.path().join("builder"),
        "builder",
        Arc::clone(&pool),
    )?;
    builder
        .context_pack_cached(&request("builder-one")?)
        .await?;
    let builder_pack: ContextPackV2 = serde_json::from_slice(
        &builder
            .context_pack_cached(&request("builder-two")?)
            .await?,
    )?;

    let baseline =
        ProjectEngine::build_with_state(&verifier_root, state.path().join("baseline"), "baseline")?;
    let baseline_started = Instant::now();
    let baseline_pack: ContextPackV2 =
        serde_json::from_slice(&baseline.context_pack_cached(&request("baseline")?).await?)?;
    let baseline_ms = baseline_started.elapsed().as_secs_f64() * 1_000.0;

    let verifier = engine(
        &verifier_root,
        &state.path().join("verifier"),
        "verifier",
        Arc::clone(&pool),
    )?;
    let verifier_started = Instant::now();
    let verifier_pack: ContextPackV2 =
        serde_json::from_slice(&verifier.context_pack_cached(&request("verifier")?).await?)?;
    let verifier_ms = verifier_started.elapsed().as_secs_f64() * 1_000.0;

    let gatekeeper = engine(
        &gatekeeper_root,
        &state.path().join("gatekeeper"),
        "gatekeeper",
        pool,
    )?;
    let gatekeeper_started = Instant::now();
    let gatekeeper_pack: ContextPackV2 = serde_json::from_slice(
        &gatekeeper
            .context_pack_cached(&request("gatekeeper")?)
            .await?,
    )?;
    let gatekeeper_ms = gatekeeper_started.elapsed().as_secs_f64() * 1_000.0;
    let verifier_metrics = metrics(&verifier).await?;
    let gatekeeper_metrics = metrics(&gatekeeper).await?;
    let safe_hits = verifier_metrics
        .pointer("/cache/l1/lineage/safe_hits")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        + gatekeeper_metrics
            .pointer("/cache/l1/lineage/safe_hits")
            .and_then(Value::as_u64)
            .unwrap_or_default();
    let latency_saved = verifier_metrics
        .pointer("/cache/l1/lineage/latency_saved_micros_est")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        + gatekeeper_metrics
            .pointer("/cache/l1/lineage/latency_saved_micros_est")
            .and_then(Value::as_u64)
            .unwrap_or_default();
    let quality_preserved = builder_pack.paths == baseline_pack.paths
        && verifier_pack.paths == baseline_pack.paths
        && gatekeeper_pack.paths == baseline_pack.paths
        && verifier_pack.evidence == baseline_pack.evidence
        && gatekeeper_pack.evidence == baseline_pack.evidence;
    let isolation_rejections = verifier_metrics
        .pointer("/cache/l1/lineage/isolation_rejections")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        + gatekeeper_metrics
            .pointer("/cache/l1/lineage/isolation_rejections")
            .and_then(Value::as_u64)
            .unwrap_or_default();
    ensure!(
        safe_hits == 2,
        "both handoffs must use safe shared frontiers"
    );
    ensure!(
        latency_saved > 0,
        "shared frontiers must avoid measured search work"
    );
    ensure!(
        quality_preserved,
        "retrieval quality changed across handoffs"
    );
    ensure!(isolation_rejections == 0, "valid worktrees were rejected");

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema": "worktree_frontier_handoff_benchmark.v1",
            "scenarios": ["builder", "verifier", "gatekeeper", "project_local_baseline"],
            "summary": {
                "baseline_local_search_ms": baseline_ms,
                "verifier_shared_ms": verifier_ms,
                "gatekeeper_shared_ms": gatekeeper_ms,
                "safe_shared_hits": safe_hits,
                "local_searches_avoided": safe_hits,
                "latency_saved_micros_est": latency_saved,
                "quality_preserved": quality_preserved,
                "isolation_rejections": isolation_rejections,
            },
            "gates": {
                "builder_to_verifier_to_gatekeeper_modeled": true,
                "two_local_searches_avoided": true,
                "positive_measured_search_work_avoided": true,
                "identical_paths_and_evidence": true,
                "no_isolation_regression": true,
            }
        }))?
    );
    Ok(())
}
