//! Reproducible shared lexical-search baseline; no model/network acquisition.
use anyhow::{Result, ensure};
use context_index::WikiSearchIndex;
use serde_json::json;
use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

fn percentile(samples: &[f64], percent: usize) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)]
}

fn main() -> Result<()> {
    let mut measurements = Vec::new();
    for count in [10_000usize, 100_000] {
        let pages=(0..count).map(|index|json!({"page_id":format!("page{index:06}"),"project_id":format!("project{}",index%4),
            "revision":"0123456789abcdef","title":format!("Component anchor{index}"),
            "summary":format!("Datasheet voltage specification for unique anchor{index}. Historical decisions and timing constraints."),
            "tags":["datasheet","voltage"],"current":true})).collect::<Vec<_>>();
        let started = Instant::now();
        let index = WikiSearchIndex::build(&pages)?;
        let build_ms = started.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        index.search("voltage", None, 10)?;
        let first_query_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut samples = Vec::new();
        let mut recall = 0usize;
        for iteration in 0..200 {
            let target = (iteration * 499) % count;
            let query = if iteration % 4 == 0 {
                "voltage".to_owned()
            } else {
                format!("anchor{target}")
            };
            let started = Instant::now();
            let hits = index.search(&query, None, 10)?;
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
            ensure!(hits.len() <= 10, "unbounded wiki results");
            if iteration % 4 != 0 {
                ensure!(
                    hits.iter()
                        .any(|hit| hit.page_id == format!("page{target:06}")),
                    "required identifier missing"
                );
                recall += 1;
            }
        }
        let shared = Arc::new(RwLock::new(Arc::new(index)));
        let reader = Arc::clone(&shared);
        let publication = std::thread::spawn(move || -> Result<()> {
            let replacement = Arc::new(WikiSearchIndex::build(&pages)?);
            *reader.write().unwrap() = replacement;
            Ok(())
        });
        let mut during = Vec::new();
        for iteration in 0..200 {
            let query = if iteration % 4 == 0 {
                "voltage".to_owned()
            } else {
                format!("anchor{}", (iteration * 499) % count)
            };
            let started = Instant::now();
            let snapshot = shared.read().unwrap().clone();
            let hits = snapshot.search(&query, None, 10)?;
            ensure!(
                !hits.is_empty() && hits.len() <= 10,
                "incomplete publication view"
            );
            during.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        publication.join().expect("publication thread")?;
        measurements.push(json!({"chunks":count,"projects":4,"queries":200,"build_ms":build_ms,"first_query_ms":first_query_ms,
            "warm_ms":{"p50":percentile(&samples,50),"p95":percentile(&samples,95),"p99":percentile(&samples,99)},"identifier_recall":recall as f64/150.0}));
        measurements.last_mut().unwrap()["during_publication_ms"] = json!({"p50":percentile(&during,50),"p95":percentile(&during,95),"p99":percentile(&during,99)});
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema":"agent_wiki.benchmark.v1","profile":if cfg!(debug_assertions){"debug"}else{"release"},"measurements":measurements})
        )?
    );
    Ok(())
}
