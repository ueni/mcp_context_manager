use std::{fs, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use context_core::{ContextPackRequest, ProjectEngine, UsageMonitor};
use context_store::StateStore;
use serde_json::{Value, json};

fn request(prompt: &str) -> ContextPackRequest {
    serde_json::from_value(json!({
        "prompt": prompt,
        "client_profile": "codex",
        "focus_paths": ["src.rs"],
    }))
    .expect("static benchmark request")
}

fn linked_worktree(root: &Path, common: &Path, name: &str) -> Result<()> {
    fs::create_dir_all(root)?;
    fs::write(root.join("src.rs"), "fn reuse_anchor() {}\n")?;
    let git_dir = common.join("worktrees").join(name);
    fs::create_dir_all(&git_dir)?;
    fs::write(git_dir.join("commondir"), "../..\n")?;
    fs::write(
        root.join(".git"),
        format!("gitdir: {}\n", git_dir.display()),
    )?;
    Ok(())
}

fn sum(report: &Value, section: &str, field: &str) -> u64 {
    report["buckets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|bucket| bucket.pointer(&format!("/{section}/{field}")))
        .filter_map(Value::as_u64)
        .sum()
}

#[tokio::main]
async fn main() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let common = fixture.path().join("common.git");
    fs::create_dir_all(&common)?;
    let root_a = fixture.path().join("worktree-a");
    let root_b = fixture.path().join("worktree-b");
    linked_worktree(&root_a, &common, "a")?;
    linked_worktree(&root_b, &common, "b")?;
    let state = tempfile::tempdir()?;
    let global_state = state.path().join("global");
    let monitor = Arc::new(UsageMonitor::open(&global_state)?);
    monitor.action("enable", None)?;

    let engine = ProjectEngine::build_with_state_and_monitor(
        &root_a,
        state.path().join("project-a"),
        "worktree-a",
        Arc::clone(&monitor),
    )?;
    let exact = request("reuse exact anchor");
    engine.context_pack_cached(&exact).await?;
    engine.context_pack_cached(&exact).await?;
    engine
        .context_pack_cached(&request("unique production request"))
        .await?;
    fs::write(
        root_a.join("src.rs"),
        "fn reuse_anchor() {}\nfn changed_generation() {}\n",
    )?;
    let mut invalidated = exact.clone();
    invalidated.changed_files = vec!["src.rs".to_owned()];
    engine.context_pack_cached(&invalidated).await?;
    drop(engine);

    let restarted = ProjectEngine::build_with_state_and_monitor(
        &root_a,
        state.path().join("project-a"),
        "worktree-a",
        Arc::clone(&monitor),
    )?;
    restarted.context_pack_cached(&exact).await?;
    drop(restarted);
    drop(monitor);

    let store = StateStore::open(&global_state)?;
    let mut tracker = store
        .get_json("monitor:reuse:state")?
        .context("reuse tracker")?;
    for entry in tracker["entries"].as_array_mut().context("reuse entries")? {
        let slot = entry["last_slot"].as_u64().unwrap_or_default();
        entry["last_slot"] = Value::from(slot.saturating_sub(3));
    }
    store.put_json("monitor:reuse:state", &tracker)?;
    drop(store);

    let monitor = Arc::new(UsageMonitor::open(&global_state)?);
    let expired = ProjectEngine::build_with_state_and_monitor(
        &root_a,
        state.path().join("project-a"),
        "worktree-a",
        Arc::clone(&monitor),
    )?;
    expired.context_pack_cached(&exact).await?;
    drop(expired);
    let variant = ProjectEngine::build_with_state_and_monitor(
        &root_b,
        state.path().join("project-b"),
        "worktree-b",
        Arc::clone(&monitor),
    )?;
    variant.context_pack_cached(&exact).await?;
    drop(variant);

    let report = monitor.action("report", None)?;
    let exact_opportunities = sum(&report, "reuse_opportunities", "exact");
    let lineage_opportunities = sum(&report, "reuse_opportunities", "lineage");
    let expired_misses = sum(&report, "miss_causes", "expired");
    let invalidated_misses = sum(
        &report,
        "miss_causes",
        "invalidated_generation_or_signature",
    );
    ensure!(exact_opportunities >= 3, "exact opportunity gate");
    ensure!(lineage_opportunities >= 1, "lineage opportunity gate");
    ensure!(expired_misses >= 1, "expiry classification gate");
    ensure!(invalidated_misses >= 1, "invalidation classification gate");

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema": "reuse_opportunity_benchmark.v1",
            "scenarios": ["exact_repeat", "unique_request", "expiry", "invalidation", "restart", "worktree_variant"],
            "summary": {
                "exact_opportunities": exact_opportunities,
                "lineage_opportunities": lineage_opportunities,
                "expired_misses": expired_misses,
                "invalidated_misses": invalidated_misses,
            },
            "gates": {
                "exact_opportunity_recorded": true,
                "lineage_isolated_opportunity_recorded": true,
                "expiry_classified": true,
                "invalidation_classified": true,
            }
        }))?
    );
    Ok(())
}
