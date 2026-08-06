use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use context_core::ContextPackRequest;
use contextd::ProjectRegistry;
use serde_json::{Value, json};

const SDK_BUDGET_MS: f64 = 60_000.0;
const EVENT_LOOP_GATE_MS: f64 = 250.0;

fn fixture_project(
    root: &Path,
    name: &str,
    files: usize,
    bytes_per_file: usize,
) -> Result<PathBuf> {
    let project = root.join(name);
    fs::create_dir_all(project.join("src"))?;
    fs::write(
        project.join("Cargo.toml"),
        format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n"),
    )?;
    for index in 0..files {
        let mut source = format!("pub fn load_anchor_{index}() -> usize {{ {index} }}\n");
        if source.len() < bytes_per_file {
            source.push_str(
                &"// bounded disk pressure fixture\n"
                    .repeat((bytes_per_file - source.len()).div_ceil(33)),
            );
        }
        fs::write(project.join("src").join(format!("file_{index}.rs")), source)?;
    }
    Ok(project)
}

fn request(
    prompt: &str,
    root_uri: Option<&Path>,
    changed_files: Vec<String>,
) -> Result<ContextPackRequest> {
    serde_json::from_value(json!({
        "prompt": prompt,
        "root_uri": root_uri.map(|root| format!("file://{}", root.display())),
        "changed_files": changed_files,
        "focus_paths": ["src"],
        "max_items": 4,
        "max_source_tokens": 512,
        "client_profile": "codex"
    }))
    .context("load benchmark request")
}

async fn timed_pack(registry: &Arc<ProjectRegistry>, request: ContextPackRequest) -> Result<f64> {
    let started = Instant::now();
    let response = registry.context_pack_bounded(request).await?;
    let value: Value = serde_json::from_slice(&response)?;
    ensure!(
        value["v"] == 2,
        "bounded request did not return context_pack.v2"
    );
    Ok(started.elapsed().as_secs_f64() * 1_000.0)
}

fn observe_event_loop(stop: Arc<AtomicBool>, max_lag_micros: Arc<AtomicU64>) {
    tokio::spawn(async move {
        let interval = Duration::from_millis(5);
        let mut previous = Instant::now();
        while !stop.load(Ordering::Acquire) {
            tokio::time::sleep(interval).await;
            let elapsed = previous.elapsed();
            previous = Instant::now();
            let lag = elapsed.saturating_sub(interval);
            max_lag_micros.fetch_max(
                u64::try_from(lag.as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let root = fixture.path();
    let project_a = fixture_project(root, "project-a", 24, 2_048)?;
    let project_b = fixture_project(root, "project-b", 24, 2_048)?;
    let slow_project = fixture_project(root, "slow-disk-pressure", 256, 16_384)?;
    let state = tempfile::tempdir()?;
    let registry = Arc::new(ProjectRegistry::new(
        project_a.clone(),
        state.path().to_path_buf(),
        vec![root.canonicalize()?],
        Vec::new(),
    )?);

    let stop = Arc::new(AtomicBool::new(false));
    let max_lag_micros = Arc::new(AtomicU64::new(0));
    observe_event_loop(Arc::clone(&stop), Arc::clone(&max_lag_micros));

    let cold_start_ms =
        timed_pack(&registry, request("cold load anchor", None, Vec::new())?).await?;
    let warm_cache_ms =
        timed_pack(&registry, request("cold load anchor", None, Vec::new())?).await?;

    fs::write(
        project_a.join("src/file_0.rs"),
        "pub fn load_anchor_0() -> usize { 9001 }\n",
    )?;
    let one_file_refresh_ms = timed_pack(
        &registry,
        request(
            "one file refresh anchor",
            None,
            vec!["src/file_0.rs".to_owned()],
        )?,
    )
    .await?;

    let mut burst = Vec::new();
    for index in 0..32 {
        let relative = format!("src/burst_{index}.rs");
        fs::write(
            project_a.join(&relative),
            format!("pub fn burst_anchor_{index}() -> usize {{ {index} }}\n"),
        )?;
        burst.push(relative);
    }
    let edit_burst_ms = timed_pack(&registry, request("edit burst anchor", None, burst)?).await?;

    let slow_disk_pressure_ms = timed_pack(
        &registry,
        request("slow disk pressure anchor", Some(&slow_project), Vec::new())?,
    )
    .await?;

    let same_started = Instant::now();
    let mut same_jobs = Vec::new();
    for index in 0..8 {
        let registry = Arc::clone(&registry);
        same_jobs.push(tokio::spawn(async move {
            registry
                .context_pack_bounded(request(
                    &format!("same project anchor {index}"),
                    None,
                    Vec::new(),
                )?)
                .await?;
            Result::<()>::Ok(())
        }));
    }
    for job in same_jobs {
        job.await??;
    }
    let concurrent_same_project_ms = same_started.elapsed().as_secs_f64() * 1_000.0;

    let multiple_started = Instant::now();
    let mut multiple_jobs = Vec::new();
    for index in 0..8 {
        let registry = Arc::clone(&registry);
        let selected = if index % 2 == 0 {
            project_a.clone()
        } else {
            project_b.clone()
        };
        multiple_jobs.push(tokio::spawn(async move {
            registry
                .context_pack_bounded(request(
                    &format!("multiple project anchor {index}"),
                    Some(&selected),
                    Vec::new(),
                )?)
                .await?;
            Result::<()>::Ok(())
        }));
    }
    for job in multiple_jobs {
        job.await??;
    }
    let multiple_projects_ms = multiple_started.elapsed().as_secs_f64() * 1_000.0;

    stop.store(true, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let event_loop_max_lag_ms = max_lag_micros.load(Ordering::Relaxed) as f64 / 1_000.0;
    let normal_request_gates = [
        cold_start_ms,
        warm_cache_ms,
        one_file_refresh_ms,
        edit_burst_ms,
        slow_disk_pressure_ms,
        concurrent_same_project_ms,
        multiple_projects_ms,
    ];
    ensure!(
        normal_request_gates
            .iter()
            .all(|elapsed| *elapsed < SDK_BUDGET_MS),
        "a normal bounded load scenario exceeded the SDK budget"
    );
    ensure!(
        event_loop_max_lag_ms < EVENT_LOOP_GATE_MS,
        "event-loop responsiveness gate failed"
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema": "context_pack_load_benchmark.v1",
            "fixture": {
                "ordinary_project_files": 24,
                "slow_disk_pressure_files": 256,
                "slow_disk_pressure_bytes_per_file": 16_384,
                "same_project_requests": 8,
                "multiple_project_requests": 8
            },
            "milliseconds": {
                "cold_start": cold_start_ms,
                "warm_cache_hit": warm_cache_ms,
                "one_file_refresh": one_file_refresh_ms,
                "edit_burst": edit_burst_ms,
                "slow_disk_pressure": slow_disk_pressure_ms,
                "concurrent_same_project": concurrent_same_project_ms,
                "multiple_projects": multiple_projects_ms,
                "event_loop_max_lag": event_loop_max_lag_ms
            },
            "gates": {
                "sdk_default_budget_ms": SDK_BUDGET_MS,
                "all_normal_requests_within_sdk_budget": true,
                "event_loop_max_lag_gate_ms": EVENT_LOOP_GATE_MS,
                "event_loop_responsive": true,
                "bounded_runtime": registry.runtime_status()
            }
        }))?
    );
    Ok(())
}
