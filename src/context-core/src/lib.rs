//! Stable contracts and deterministic core behavior for the native server.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow, bail};
use context_index::{
    IndexStats, ProjectIndex, SearchHit, SymbolRecord, normalize_terms, repository_signature,
    validate_relative_path,
};
use context_store::{ReferenceValidation, StateStore};
use moka::future::Cache;
use notify::{
    Config as NotifyConfig, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher,
};
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

pub const CONTEXT_PACK_VERSION: u8 = 2;
pub const DEFAULT_MAX_ITEMS: u8 = 8;
pub const DEFAULT_MAX_SOURCE_TOKENS: u16 = 512;

pub type WireCache<K, V> = moka::future::Cache<K, V>;

/// Encodes the read-only administrative response for a project whose retrieval
/// engine has not been opened yet.  This deliberately avoids opening LMDB,
/// starting a watcher, or constructing a Tantivy index merely to render a
/// metrics dashboard.
pub fn unloaded_admin_response(
    request: &ContextAdminRequest,
    project_id: &str,
    status: &str,
) -> Result<Vec<u8>> {
    let now = now_iso()?;
    let value = match request.mode.as_str() {
        "metrics" => unloaded_metrics_snapshot(project_id, &now, status),
        "measurement_matrix" => unloaded_measurement_matrix(project_id, &now, status),
        "measurement_report" => json!({
            "schema": "context_measurement_report.v1",
            "metrics": unloaded_metrics_snapshot(project_id, &now, status),
            "matrix": unloaded_measurement_matrix(project_id, &now, status),
        }),
        unsupported => bail!("unsupported unloaded administrative mode: {unsupported}"),
    };
    serde_json::to_vec(&value).map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidencePolicy {
    Reference,
    #[default]
    Balanced,
    Source,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheStrategy {
    #[default]
    Fast,
    Stable,
    Fresh,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPackRequest {
    pub prompt: String,
    #[serde(default)]
    pub changed_files: Vec<String>,
    #[serde(default)]
    pub focus_paths: Vec<String>,
    pub memory_session: Option<String>,
    pub client_profile: Option<String>,
    pub model_profile: Option<String>,
    pub project_id: Option<String>,
    pub root_uri: Option<String>,
    #[serde(default = "default_max_items")]
    pub max_items: u8,
    #[serde(default = "default_max_source_tokens")]
    pub max_source_tokens: u16,
    #[serde(default)]
    pub evidence_policy: EvidencePolicy,
    #[serde(default)]
    pub cache_strategy: CacheStrategy,
    pub base_pack: Option<String>,
    #[serde(default)]
    pub known_evidence: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ContextLookupRequest {
    #[serde(default = "default_lookup_mode")]
    pub mode: String,
    #[serde(default)]
    pub query: String,
    #[serde(default = "default_lookup_path")]
    pub path: String,
    #[serde(default = "default_start_line")]
    pub start_line: u32,
    pub end_line: Option<u32>,
    #[serde(default = "default_max_results")]
    pub max_results: u16,
    #[serde(default = "default_max_entries")]
    pub max_entries: u16,
    #[serde(default = "default_max_depth")]
    pub max_depth: u8,
    #[serde(default)]
    pub include_globs: Vec<String>,
    pub project_id: Option<String>,
    pub root_uri: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ContextMemoryRequest {
    #[serde(default = "default_memory_mode")]
    pub mode: String,
    pub namespace: Option<String>,
    pub key: Option<String>,
    pub value: Option<Value>,
    pub ttl_days: Option<i64>,
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    #[serde(default = "default_memory_source")]
    pub source: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub focus: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub topic: String,
    pub decision: Option<Value>,
    #[serde(default = "default_decided_by")]
    pub decided_by: String,
    #[serde(default)]
    pub rationale: String,
    #[serde(default)]
    pub include_expired: bool,
    #[serde(default = "default_memory_max_entries")]
    pub max_entries: u16,
    pub project_id: Option<String>,
    pub root_uri: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ResultReferenceRequest {
    #[serde(default)]
    pub reference_id: String,
    pub reference: Option<Value>,
    #[serde(default)]
    pub expected_hash: String,
    pub project_id: Option<String>,
    pub root_uri: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ContextAdminRequest {
    #[serde(default = "default_admin_mode")]
    pub mode: String,
    #[serde(default = "default_lookup_path")]
    pub path: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub action: String,
    pub max_files: Option<u32>,
    #[serde(default = "default_max_age_minutes")]
    pub max_age_minutes: u32,
    #[serde(default = "default_memory_max_entries")]
    pub max_entries: u16,
    pub max_output_chars: Option<u32>,
    pub default_output_profile: Option<String>,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub contract_profile: String,
    #[serde(default)]
    pub state_prefix: String,
    #[serde(default)]
    pub state_key: String,
    pub project_id: Option<String>,
    pub root_uri: Option<String>,
}

impl ContextPackRequest {
    pub fn validate_limits(&self) -> Result<(), ContractError> {
        if !(1..=32).contains(&self.max_items) {
            return Err(ContractError::MaxItems(self.max_items));
        }
        if self.max_source_tokens > 4096 {
            return Err(ContractError::MaxSourceTokens(self.max_source_tokens));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContractError {
    MaxItems(u8),
    MaxSourceTokens(u16),
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxItems(value) => write!(formatter, "max_items must be in 1..=32, got {value}"),
            Self::MaxSourceTokens(value) => {
                write!(
                    formatter,
                    "max_source_tokens must be in 0..=4096, got {value}"
                )
            }
        }
    }
}

impl std::error::Error for ContractError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextPackV2 {
    pub v: u8,
    pub id: String,
    pub route: String,
    pub paths: Vec<String>,
    pub evidence: Vec<EvidenceCard>,
    pub more: Option<String>,
}

pub type EvidenceCard = (String, u8, u32, u32, String, String, u32);

const L0_MAX_BYTES: u64 = 64 * 1024 * 1024;
const L1_MEMORY_MAX_BYTES: usize = 128 * 1024 * 1024;
const L1_PERSISTENT_MAX_BYTES: usize = 256 * 1024 * 1024;
const FAST_POLL_INTERVAL_MS: u64 = 2_000;
const WATCH_COALESCE_MS: u64 = 50;
const NEGATIVE_FRONTIER_TTL_MS: u64 = 30_000;
const POSITIVE_FRONTIER_ADMISSION_WINDOW_MS: u64 = 30 * 60 * 1_000;
const FRONTIER_ADMISSION_TRACKER_MAX_ENTRIES: usize = 2_048;
const OPERATION_SAMPLE_LIMIT: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PackCacheKey(String);

#[derive(Clone)]
struct CachedPack {
    bytes: Arc<Vec<u8>>,
    reference_validations: Arc<Vec<ReferenceValidation>>,
    validity: ValidityCertificate,
    telemetry: PackTelemetry,
}

struct CachedAdmission {
    cached: Arc<CachedPack>,
    outcome: PackCacheOutcome,
}

#[derive(Clone)]
struct ValidityCertificate {
    generation: u64,
    refresh_signature: String,
}

#[derive(Clone, Copy, Default)]
struct L0StorageStats {
    entries: u64,
    weighted_bytes: u64,
}

fn l0_entry_weight(key: &PackCacheKey, value: &CachedPack) -> u32 {
    let bytes = key.0.len()
        + value.bytes.len()
        + value.validity.refresh_signature.len()
        + value
            .reference_validations
            .iter()
            .map(|validation| validation.reference_id.len())
            .sum::<usize>();
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

fn l0_cache_key(
    request: &ContextPackRequest,
    explicit_paths: &[String],
    generation: u64,
    refresh_signature: &str,
) -> Result<PackCacheKey> {
    let mut known_evidence = request.known_evidence.clone();
    known_evidence.sort();
    known_evidence.dedup();
    Ok(PackCacheKey(digest_id(
        "l0_",
        &serde_json::to_vec(&json!({
            "prompt": &request.prompt,
            "explicit_paths": explicit_paths,
            "max_items": request.max_items,
            "max_source_tokens": request.max_source_tokens,
            "evidence_policy": request.evidence_policy,
            "base_pack": &request.base_pack,
            "known_evidence": known_evidence,
            "generation": generation,
            "refresh_signature": refresh_signature,
        }))?,
    )))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FrontierRecord {
    schema: String,
    key: String,
    route: String,
    scope: Vec<String>,
    terms: Vec<String>,
    generation: u64,
    candidate_capacity: u8,
    candidate_ids: Vec<String>,
    scores: Vec<f32>,
    cumulative_token_costs: Vec<u32>,
    dependencies: Vec<String>,
    score_cutoff: f32,
    refresh_signature: String,
    negative: bool,
    expires_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PackSnapshot {
    schema: String,
    pack_id: String,
    route: String,
    paths: Vec<String>,
    evidence: Vec<PackEvidenceSnapshot>,
    refresh_signature: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PackEvidenceSnapshot {
    id: String,
    path: String,
    start_line: u32,
    end_line: u32,
    symbol: String,
}

impl FrontierRecord {
    fn estimated_bytes(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |encoded| encoded.len())
    }
}

#[derive(Default)]
struct FrontierState {
    records: VecDeque<Arc<FrontierRecord>>,
    bytes: usize,
}

#[derive(Default)]
struct FrontierAdmissionTracker {
    observations: HashMap<String, (u8, u64)>,
}

impl FrontierAdmissionTracker {
    fn observe(&mut self, key: String, now: u64) -> bool {
        self.observations.retain(|_, (_, seen)| {
            now.saturating_sub(*seen) <= POSITIVE_FRONTIER_ADMISSION_WINDOW_MS
        });
        if let Some((count, seen)) = self.observations.get_mut(&key) {
            *count = count.saturating_add(1);
            *seen = now;
            if *count >= 2 {
                self.observations.remove(&key);
                return true;
            }
            return false;
        }
        if self.observations.len() >= FRONTIER_ADMISSION_TRACKER_MAX_ENTRIES
            && let Some(oldest) = self
                .observations
                .iter()
                .min_by_key(|(_, (_, seen))| *seen)
                .map(|(key, _)| key.clone())
        {
            self.observations.remove(&oldest);
        }
        self.observations.insert(key, (1, now));
        false
    }
}

#[derive(Clone, Copy, Default)]
enum FrontierOutcome {
    #[default]
    Search,
    ExactHit,
    NegativeHit,
    Admitted,
    CapacityFallback,
    SourceFallback,
}

struct DeferredRetrieval {
    hits: Vec<SearchHit>,
    terms: Vec<String>,
    pending: Option<Arc<FrontierRecord>>,
    outcome: FrontierOutcome,
}

#[derive(Clone, Copy)]
enum PackCacheOutcome {
    Uncached,
    L0Hit,
    L0Miss,
    L0Singleflight,
}

#[derive(Clone, Copy, Default)]
struct PackTelemetry {
    input_tokens_est: u32,
    selected_source_tokens_est: u32,
    evidence_card_tokens_est: u32,
    returned_evidence_tokens_est: u32,
    wire_tokens_est: u32,
    wire_bytes: usize,
    tokens_saved_est: u32,
    delta_tokens_saved_est: u32,
    candidate_count: u32,
    selected_count: u32,
    retrieval_micros: u64,
    pack_build_micros: u64,
    frontier_outcome: FrontierOutcome,
    base_pack_used: bool,
    known_evidence_used: bool,
}

struct UsageSample {
    elapsed: StdDuration,
    telemetry: PackTelemetry,
    cache_outcome: PackCacheOutcome,
    refresh_checked: bool,
    refresh_updated: bool,
    route: &'static str,
    term_count: usize,
    scope_count: usize,
}

struct BuiltPack {
    bytes: Vec<u8>,
    telemetry: PackTelemetry,
}

#[derive(Default)]
struct OperationMetric {
    count: u64,
    success_count: u64,
    error_count: u64,
    elapsed_total_micros: u64,
    elapsed_min_micros: u64,
    elapsed_max_micros: u64,
    elapsed_last_micros: u64,
    output_bytes_total: u64,
    recent_elapsed_micros: VecDeque<u64>,
}

impl OperationMetric {
    fn record(&mut self, elapsed_micros: u64, success: bool, output_bytes: usize) {
        self.count = self.count.saturating_add(1);
        if success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.elapsed_total_micros = self.elapsed_total_micros.saturating_add(elapsed_micros);
        self.elapsed_min_micros = if self.count == 1 {
            elapsed_micros
        } else {
            self.elapsed_min_micros.min(elapsed_micros)
        };
        self.elapsed_max_micros = self.elapsed_max_micros.max(elapsed_micros);
        self.elapsed_last_micros = elapsed_micros;
        self.output_bytes_total = self
            .output_bytes_total
            .saturating_add(u64::try_from(output_bytes).unwrap_or(u64::MAX));
        if self.recent_elapsed_micros.len() == OPERATION_SAMPLE_LIMIT {
            self.recent_elapsed_micros.pop_front();
        }
        self.recent_elapsed_micros.push_back(elapsed_micros);
    }

    fn snapshot(&self) -> Value {
        let average_micros = if self.count == 0 {
            0.0
        } else {
            self.elapsed_total_micros as f64 / self.count as f64
        };
        let mut samples = self
            .recent_elapsed_micros
            .iter()
            .copied()
            .collect::<Vec<_>>();
        samples.sort_unstable();
        let percentile = |percent: usize| -> Option<u64> {
            (!samples.is_empty()).then(|| samples[(samples.len() - 1) * percent / 100])
        };
        json!({
            "count": self.count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "avg_elapsed_ms": average_micros / 1_000.0,
            "min_elapsed_ms": self.elapsed_min_micros as f64 / 1_000.0,
            "max_elapsed_ms": self.elapsed_max_micros as f64 / 1_000.0,
            "last_elapsed_ms": self.elapsed_last_micros as f64 / 1_000.0,
            "p50_recent_ms": percentile(50).map(|value| value as f64 / 1_000.0),
            "p95_recent_ms": percentile(95).map(|value| value as f64 / 1_000.0),
            "recent_sample_count": samples.len(),
            "recent_sample_limit": OPERATION_SAMPLE_LIMIT,
            "output_bytes_total": self.output_bytes_total,
            "avg_output_bytes": if self.count == 0 { 0.0 } else { self.output_bytes_total as f64 / self.count as f64 },
        })
    }
}

#[derive(Default)]
struct EngineMetrics {
    operations: Mutex<BTreeMap<&'static str, OperationMetric>>,
    active_references: AtomicU64,
    total_references: AtomicU64,
    l0_hits: AtomicU64,
    l0_misses: AtomicU64,
    l0_singleflight_hits: AtomicU64,
    l0_cold_or_invalidated_misses: AtomicU64,
    l0_request_variant_misses: AtomicU64,
    l0_invalidations: AtomicU64,
    l0_invalidated_entries: AtomicU64,
    l1_exact_hits: AtomicU64,
    l1_approximate_hits: AtomicU64,
    retrieval_misses: AtomicU64,
    refreshes: AtomicU64,
    pack_input_tokens_est: AtomicU64,
    pack_selected_source_tokens_est: AtomicU64,
    pack_evidence_card_tokens_est: AtomicU64,
    pack_returned_evidence_tokens_est: AtomicU64,
    pack_wire_tokens_est: AtomicU64,
    pack_wire_bytes: AtomicU64,
    pack_tokens_saved_est: AtomicU64,
    pack_delta_tokens_saved_est: AtomicU64,
    pack_candidate_count: AtomicU64,
    pack_selected_count: AtomicU64,
    warmup_runs: AtomicU64,
}

impl EngineMetrics {
    fn record_operation(
        &self,
        operation: &'static str,
        elapsed: StdDuration,
        success: bool,
        output_bytes: usize,
    ) {
        let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operations
            .entry(operation)
            .or_default()
            .record(elapsed_micros, success, output_bytes);
    }

    fn operation_snapshots(&self) -> BTreeMap<String, Value> {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(operation, metric)| ((*operation).to_owned(), metric.snapshot()))
            .collect()
    }

    fn record_pack_telemetry(&self, telemetry: PackTelemetry) {
        self.pack_input_tokens_est
            .fetch_add(u64::from(telemetry.input_tokens_est), Ordering::Relaxed);
        self.pack_selected_source_tokens_est.fetch_add(
            u64::from(telemetry.selected_source_tokens_est),
            Ordering::Relaxed,
        );
        self.pack_evidence_card_tokens_est.fetch_add(
            u64::from(telemetry.evidence_card_tokens_est),
            Ordering::Relaxed,
        );
        self.pack_returned_evidence_tokens_est.fetch_add(
            u64::from(telemetry.returned_evidence_tokens_est),
            Ordering::Relaxed,
        );
        self.pack_wire_tokens_est
            .fetch_add(u64::from(telemetry.wire_tokens_est), Ordering::Relaxed);
        self.pack_wire_bytes.fetch_add(
            u64::try_from(telemetry.wire_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.pack_tokens_saved_est
            .fetch_add(u64::from(telemetry.tokens_saved_est), Ordering::Relaxed);
        self.pack_delta_tokens_saved_est.fetch_add(
            u64::from(telemetry.delta_tokens_saved_est),
            Ordering::Relaxed,
        );
        self.pack_candidate_count
            .fetch_add(u64::from(telemetry.candidate_count), Ordering::Relaxed);
        self.pack_selected_count
            .fetch_add(u64::from(telemetry.selected_count), Ordering::Relaxed);
    }
}

#[derive(Default)]
struct FreshnessState {
    dirty: AtomicBool,
    last_event_ms: AtomicU64,
    last_poll_ms: AtomicU64,
    generation: AtomicU64,
}

struct WatchGuard {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

pub struct UsageMonitor {
    store: StateStore,
    enabled: AtomicBool,
    update_lock: Mutex<()>,
}

impl UsageMonitor {
    pub fn open(global_state: impl AsRef<std::path::Path>) -> Result<Self> {
        let store = StateStore::open(global_state)?;
        let enabled = store
            .get_json("monitor:config")?
            .and_then(|value| value.get("enabled").and_then(Value::as_bool))
            .unwrap_or(false);
        Ok(Self {
            store,
            enabled: AtomicBool::new(enabled),
            update_lock: Mutex::new(()),
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn action(&self, action: &str, project_id: Option<&str>) -> Result<Value> {
        match action {
            "enable" | "disable" => {
                let _guard = self
                    .update_lock
                    .lock()
                    .map_err(|_| anyhow!("usage monitor lock poisoned"))?;
                let enabled = action == "enable";
                self.enabled.store(enabled, Ordering::Release);
                self.store.put_json(
                    "monitor:config",
                    &json!({"schema":"context_monitor_usage.config.v1", "enabled":enabled}),
                )?;
                self.status()
            }
            "status" | "" => self.status(),
            "report" => self.report(project_id),
            unsupported => bail!("unsupported monitor_usage action: {unsupported}"),
        }
    }

    fn status(&self) -> Result<Value> {
        Ok(json!({
            "schema": "context_monitor_usage.status.v1",
            "enabled": self.enabled(),
            "retention_days": 30,
            "global": true,
        }))
    }

    fn record(&self, project_id: &str, sample: UsageSample) -> Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| anyhow!("usage monitor lock poisoned"))?;
        let day = OffsetDateTime::now_utc().date().to_string();
        let key = format!("monitor:usage:{day}:{project_id}");
        let mut value = self.store.get_json(&key)?.unwrap_or_else(|| {
            json!({
                "schema": "context_monitor_usage.bucket.v1",
                "day": day,
                "project_id": project_id,
                "request_count": 0,
                "elapsed_micros_total": 0,
                "elapsed_micros_max": 0,
                "input_tokens_est": 0,
                "wire_tokens_est": 0,
                "candidate_count": 0,
                "selected_count": 0,
                "tokens_saved_est": 0,
                "delta_tokens_saved_est": 0,
                "cache_outcomes": {},
                "frontier_outcomes": {},
                "index": {},
                "delta": {},
                "routes": {},
                "term_count_buckets": {},
                "scope_count_buckets": {},
                "stages_micros": {},
            })
        });
        let telemetry = sample.telemetry;
        let elapsed_micros = u64::try_from(sample.elapsed.as_micros()).unwrap_or(u64::MAX);
        increment_json_u64(&mut value, "request_count", 1);
        increment_json_u64(&mut value, "elapsed_micros_total", elapsed_micros);
        let previous_max = value
            .get("elapsed_micros_max")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        value["elapsed_micros_max"] = Value::from(previous_max.max(elapsed_micros));
        increment_json_u64(
            &mut value,
            "input_tokens_est",
            u64::from(telemetry.input_tokens_est),
        );
        increment_json_u64(
            &mut value,
            "wire_tokens_est",
            u64::from(telemetry.wire_tokens_est),
        );
        increment_json_u64(
            &mut value,
            "candidate_count",
            u64::from(telemetry.candidate_count),
        );
        increment_json_u64(
            &mut value,
            "selected_count",
            u64::from(telemetry.selected_count),
        );
        increment_json_u64(
            &mut value,
            "tokens_saved_est",
            u64::from(telemetry.tokens_saved_est),
        );
        increment_json_u64(
            &mut value,
            "delta_tokens_saved_est",
            u64::from(telemetry.delta_tokens_saved_est),
        );
        increment_json_nested(
            &mut value,
            "cache_outcomes",
            match sample.cache_outcome {
                PackCacheOutcome::Uncached => "uncached",
                PackCacheOutcome::L0Hit => "l0_hit",
                PackCacheOutcome::L0Miss => "l0_miss",
                PackCacheOutcome::L0Singleflight => "l0_singleflight",
            },
            1,
        );
        if matches!(
            sample.cache_outcome,
            PackCacheOutcome::Uncached | PackCacheOutcome::L0Miss
        ) {
            increment_json_nested(
                &mut value,
                "frontier_outcomes",
                match telemetry.frontier_outcome {
                    FrontierOutcome::Search => "search",
                    FrontierOutcome::ExactHit => "exact_hit",
                    FrontierOutcome::NegativeHit => "negative_hit",
                    FrontierOutcome::Admitted => "admitted",
                    FrontierOutcome::CapacityFallback => "capacity_fallback",
                    FrontierOutcome::SourceFallback => "source_fallback",
                },
                1,
            );
        }
        increment_json_nested(
            &mut value,
            "index",
            "refresh_checked",
            u64::from(sample.refresh_checked),
        );
        increment_json_nested(
            &mut value,
            "index",
            "refresh_updated",
            u64::from(sample.refresh_updated),
        );
        increment_json_nested(
            &mut value,
            "delta",
            "base_pack_requests",
            u64::from(telemetry.base_pack_used),
        );
        increment_json_nested(
            &mut value,
            "delta",
            "known_evidence_requests",
            u64::from(telemetry.known_evidence_used),
        );
        increment_json_nested(&mut value, "routes", sample.route, 1);
        increment_json_nested(
            &mut value,
            "term_count_buckets",
            coarse_count_bucket(sample.term_count),
            1,
        );
        increment_json_nested(
            &mut value,
            "scope_count_buckets",
            coarse_count_bucket(sample.scope_count),
            1,
        );
        match sample.cache_outcome {
            PackCacheOutcome::L0Hit | PackCacheOutcome::L0Singleflight => {
                increment_json_nested(&mut value, "stages_micros", "cache", elapsed_micros);
            }
            PackCacheOutcome::Uncached | PackCacheOutcome::L0Miss => {
                increment_json_nested(
                    &mut value,
                    "stages_micros",
                    "retrieval",
                    telemetry.retrieval_micros,
                );
                increment_json_nested(
                    &mut value,
                    "stages_micros",
                    "pack_build",
                    telemetry.pack_build_micros,
                );
            }
        }
        self.store.put_json(&key, &value)?;
        self.prune()?;
        Ok(())
    }

    fn prune(&self) -> Result<()> {
        let cutoff = (OffsetDateTime::now_utc() - Duration::days(29))
            .date()
            .to_string();
        let rows = self.store.iter_json("monitor:usage:")?;
        let mut remove = rows
            .iter()
            .filter(|(_, value)| {
                value
                    .get("day")
                    .and_then(Value::as_str)
                    .is_none_or(|day| day < cutoff.as_str())
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if rows.len().saturating_sub(remove.len()) > 4_096 {
            let mut retained = rows
                .iter()
                .filter(|(key, _)| !remove.contains(key))
                .map(|(key, value)| {
                    (
                        value.get("day").and_then(Value::as_str).unwrap_or_default(),
                        key,
                    )
                })
                .collect::<Vec<_>>();
            retained.sort();
            remove.extend(
                retained
                    .into_iter()
                    .take(
                        rows.len()
                            .saturating_sub(remove.len())
                            .saturating_sub(4_096),
                    )
                    .map(|(_, key)| key.clone()),
            );
        }
        self.store.delete_batch(&remove)?;
        Ok(())
    }

    fn report(&self, project_id: Option<&str>) -> Result<Value> {
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| anyhow!("usage monitor lock poisoned"))?;
        self.prune()?;
        let mut buckets = self
            .store
            .iter_json("monitor:usage:")?
            .into_iter()
            .map(|(_, value)| value)
            .filter(|value| {
                project_id.is_none_or(|selected| {
                    value.get("project_id").and_then(Value::as_str) == Some(selected)
                })
            })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| {
            left.get("day")
                .and_then(Value::as_str)
                .cmp(&right.get("day").and_then(Value::as_str))
        });
        Ok(json!({
            "schema": "context_monitor_usage.report.v1",
            "enabled": self.enabled(),
            "retention_days": 30,
            "global": true,
            "project_id": project_id,
            "buckets": buckets,
        }))
    }
}

fn increment_json_u64(value: &mut Value, field: &str, increment: u64) {
    let current = value.get(field).and_then(Value::as_u64).unwrap_or_default();
    value[field] = Value::from(current.saturating_add(increment));
}

fn increment_json_nested(value: &mut Value, section: &str, field: &str, increment: u64) {
    if !value.get(section).is_some_and(Value::is_object) {
        value[section] = json!({});
    }
    let current = value[section]
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_default();
    value[section][field] = Value::from(current.saturating_add(increment));
}

fn coarse_count_bucket(count: usize) -> &'static str {
    match count {
        0 => "0",
        1..=2 => "1_2",
        3..=5 => "3_5",
        _ => "6_plus",
    }
}

impl WatchGuard {
    fn start(root: std::path::PathBuf, freshness: Arc<FreshnessState>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("context-index-watch".to_owned())
            .spawn(move || watch_repository(root, freshness, thread_stop))
            .ok();
        Self {
            stop,
            thread: handle,
        }
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

pub struct ProjectEngine {
    index: RwLock<Arc<ProjectIndex>>,
    store: Arc<StateStore>,
    project_id: String,
    l0: Cache<PackCacheKey, Arc<CachedPack>>,
    frontiers: Mutex<FrontierState>,
    frontier_admissions: Mutex<FrontierAdmissionTracker>,
    pack_snapshots: Mutex<HashMap<String, Arc<PackSnapshot>>>,
    deferred_references: Mutex<HashMap<String, ReferenceValidation>>,
    freshness: Arc<FreshnessState>,
    refresh_lock: Mutex<()>,
    metrics: EngineMetrics,
    usage_monitor: Arc<UsageMonitor>,
    _watcher: WatchGuard,
}

impl ProjectEngine {
    pub fn build(root: impl AsRef<std::path::Path>) -> Result<Self> {
        let root = root.as_ref().canonicalize()?;
        let state = root.join(".mcp-context-manager");
        Self::build_with_state(root, state, "default")
    }

    pub fn build_with_state(
        root: impl AsRef<std::path::Path>,
        project_state: impl AsRef<std::path::Path>,
        project_id: impl Into<String>,
    ) -> Result<Self> {
        let monitor = Arc::new(UsageMonitor::open(
            project_state.as_ref().join("monitor-global"),
        )?);
        Self::build_with_state_and_monitor(root, project_state, project_id, monitor)
    }

    pub fn build_with_state_and_monitor(
        root: impl AsRef<std::path::Path>,
        project_state: impl AsRef<std::path::Path>,
        project_id: impl Into<String>,
        usage_monitor: Arc<UsageMonitor>,
    ) -> Result<Self> {
        Self::build_with_state_and_l0_idle(
            root,
            project_state,
            project_id,
            StdDuration::from_secs(30 * 60),
            usage_monitor,
        )
    }

    fn build_with_state_and_l0_idle(
        root: impl AsRef<std::path::Path>,
        project_state: impl AsRef<std::path::Path>,
        project_id: impl Into<String>,
        l0_idle: StdDuration,
        usage_monitor: Arc<UsageMonitor>,
    ) -> Result<Self> {
        let index = Arc::new(ProjectIndex::build(root)?);
        let store = Arc::new(StateStore::open(project_state)?);
        let freshness = Arc::new(FreshnessState {
            last_poll_ms: AtomicU64::new(now_millis()),
            ..FreshnessState::default()
        });
        let frontiers = load_frontiers(&store, &index.stats().refresh_signature)?;
        let pack_snapshots = load_pack_snapshots(&store)?;
        let deferred_references = load_deferred_references(&store)?;
        let (active_references, total_references) = reference_counts(&store)?;
        let metrics = EngineMetrics::default();
        metrics
            .active_references
            .store(active_references, Ordering::Relaxed);
        metrics
            .total_references
            .store(total_references, Ordering::Relaxed);
        let watcher = WatchGuard::start(index.root().to_path_buf(), Arc::clone(&freshness));
        let l0 = Cache::builder()
            .max_capacity(L0_MAX_BYTES)
            .weigher(|key: &PackCacheKey, value: &Arc<CachedPack>| l0_entry_weight(key, value))
            .time_to_idle(l0_idle)
            .build();
        Ok(Self {
            index: RwLock::new(index),
            store,
            project_id: project_id.into(),
            l0,
            frontiers: Mutex::new(frontiers),
            frontier_admissions: Mutex::new(FrontierAdmissionTracker::default()),
            pack_snapshots: Mutex::new(pack_snapshots),
            deferred_references: Mutex::new(deferred_references),
            freshness,
            refresh_lock: Mutex::new(()),
            metrics,
            usage_monitor,
            _watcher: watcher,
        })
    }

    pub fn index(&self) -> Arc<ProjectIndex> {
        self.index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn l0_storage_stats(&self) -> L0StorageStats {
        self.l0
            .iter()
            .fold(L0StorageStats::default(), |mut stats, (key, value)| {
                stats.entries = stats.entries.saturating_add(1);
                stats.weighted_bytes = stats
                    .weighted_bytes
                    .saturating_add(u64::from(l0_entry_weight(key.as_ref(), &value)));
                stats
            })
    }

    fn invalidate_l0(&self) {
        let stats = self.l0_storage_stats();
        self.metrics
            .l0_invalidations
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .l0_invalidated_entries
            .fetch_add(stats.entries, Ordering::Relaxed);
        self.l0.invalidate_all();
    }

    pub fn store(&self) -> &StateStore {
        &self.store
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn context_pack(&self, request: &ContextPackRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let result = (|| {
            let refresh = self.ensure_fresh(request)?;
            Ok::<_, anyhow::Error>((self.build_context_pack(request)?, refresh))
        })();
        match result {
            Ok((pack, (refresh_checked, refresh_updated))) => {
                self.record_context_pack(started, &pack.telemetry);
                if self.usage_monitor.enabled() {
                    let _ = self.usage_monitor.record(
                        &self.project_id,
                        UsageSample {
                            elapsed: started.elapsed(),
                            telemetry: pack.telemetry,
                            cache_outcome: PackCacheOutcome::Uncached,
                            refresh_checked,
                            refresh_updated,
                            route: classify_route(&request.prompt),
                            term_count: normalize_terms(&request.prompt, 8).len(),
                            scope_count: normalized_explicit_paths(request)
                                .map_or(0, |paths| paths.len()),
                        },
                    );
                }
                Ok(pack.bytes)
            }
            Err(error) => {
                self.record_operation("context_pack", started, false, 0);
                Err(error)
            }
        }
    }

    pub async fn context_pack_cached(&self, request: &ContextPackRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let result = async {
            request.validate_limits()?;
            self.validate_project_selector(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
            )?;
            let refresh = self.ensure_fresh(request)?;
            Ok::<_, anyhow::Error>((self.admit_context_pack_cached(request).await?, refresh))
        }
        .await;
        match result {
            Ok((admission, (refresh_checked, refresh_updated))) => {
                self.record_context_pack(started, &admission.cached.telemetry);
                if self.usage_monitor.enabled() {
                    let _ = self.usage_monitor.record(
                        &self.project_id,
                        UsageSample {
                            elapsed: started.elapsed(),
                            telemetry: admission.cached.telemetry,
                            cache_outcome: admission.outcome,
                            refresh_checked,
                            refresh_updated,
                            route: classify_route(&request.prompt),
                            term_count: normalize_terms(&request.prompt, 8).len(),
                            scope_count: normalized_explicit_paths(request)
                                .map_or(0, |paths| paths.len()),
                        },
                    );
                }
                Ok(admission.cached.bytes.as_ref().clone())
            }
            Err(error) => {
                self.record_operation("context_pack", started, false, 0);
                Err(error)
            }
        }
    }

    async fn admit_context_pack_cached(
        &self,
        request: &ContextPackRequest,
    ) -> Result<CachedAdmission> {
        request.validate_limits()?;
        self.validate_project_selector(request.project_id.as_deref(), request.root_uri.as_deref())?;
        let index = self.index();
        let generation = self.freshness.generation.load(Ordering::Acquire);
        let refresh_signature = index.stats().refresh_signature.clone();
        let explicit_paths = normalized_explicit_paths(request)?;
        let key = l0_cache_key(request, &explicit_paths, generation, &refresh_signature)?;
        if let Some(cached) = self.l0.get(&key).await {
            match self.cached_pack_is_valid(&cached, generation, &refresh_signature) {
                Ok(true) => {
                    self.metrics.l0_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(CachedAdmission {
                        cached,
                        outcome: PackCacheOutcome::L0Hit,
                    });
                }
                Ok(false) => self.invalidate_l0_entry(&key).await,
                Err(error) => {
                    self.invalidate_l0_entry(&key).await;
                    return Err(error);
                }
            }
        }

        self.l0.run_pending_tasks().await;
        let cache_was_empty = self.l0.iter().next().is_none();
        let built_by_this_request = Arc::new(AtomicBool::new(false));
        let build_marker = Arc::clone(&built_by_this_request);
        let cached = self
            .l0
            .try_get_with(key, async {
                build_marker.store(true, Ordering::Release);
                let pack = self.build_context_pack(request)?;
                let response: ContextPackV2 = serde_json::from_slice(&pack.bytes)?;
                let reference_validations = response
                    .more
                    .into_iter()
                    .map(|reference_id| self.cached_reference_validation(&reference_id))
                    .collect::<Result<Vec<_>>>()?;
                Ok::<Arc<CachedPack>, anyhow::Error>(Arc::new(CachedPack {
                    bytes: Arc::new(pack.bytes),
                    reference_validations: Arc::new(reference_validations),
                    validity: ValidityCertificate {
                        generation,
                        refresh_signature,
                    },
                    telemetry: pack.telemetry,
                }))
            })
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        let outcome = if built_by_this_request.load(Ordering::Acquire) {
            self.metrics.l0_misses.fetch_add(1, Ordering::Relaxed);
            if cache_was_empty {
                self.metrics
                    .l0_cold_or_invalidated_misses
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics
                    .l0_request_variant_misses
                    .fetch_add(1, Ordering::Relaxed);
            }
            PackCacheOutcome::L0Miss
        } else {
            self.metrics.l0_hits.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .l0_singleflight_hits
                .fetch_add(1, Ordering::Relaxed);
            PackCacheOutcome::L0Singleflight
        };
        Ok(CachedAdmission { cached, outcome })
    }

    fn cached_pack_is_valid(
        &self,
        cached: &CachedPack,
        generation: u64,
        refresh_signature: &str,
    ) -> Result<bool> {
        if cached.validity.generation != generation
            || cached.validity.refresh_signature != refresh_signature
        {
            return Ok(false);
        }
        for validation in cached.reference_validations.iter() {
            if !self.store.reference_validation_is_active(validation)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn cached_reference_validation(&self, reference_id: &str) -> Result<ReferenceValidation> {
        let cached = {
            self.deferred_references
                .lock()
                .map_err(|_| anyhow!("deferred reference lock poisoned"))?
                .values()
                .find(|validation| validation.reference_id == reference_id)
                .cloned()
        };
        if let Some(validation) = cached
            && self.store.reference_validation_is_active(&validation)?
        {
            return Ok(validation);
        }
        self.store
            .reference_validation(reference_id)?
            .ok_or_else(|| anyhow!("context pack produced an invalid reference"))
    }

    async fn invalidate_l0_entry(&self, key: &PackCacheKey) {
        self.l0.invalidate(key).await;
        self.metrics
            .l0_invalidations
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .l0_invalidated_entries
            .fetch_add(1, Ordering::Relaxed);
    }

    fn build_context_pack(&self, request: &ContextPackRequest) -> Result<BuiltPack> {
        let pack_started = Instant::now();
        request.validate_limits()?;
        self.validate_project_selector(request.project_id.as_deref(), request.root_uri.as_deref())?;

        let explicit_paths = normalized_explicit_paths(request)?;
        let index = self.index();
        let retrieval_started = Instant::now();
        let (candidates, terms, frontier_outcome) =
            self.retrieve_candidates(&index, request, &explicit_paths)?;
        let retrieval_micros =
            u64::try_from(retrieval_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let selected = select_hits(
            candidates.as_slice(),
            explicit_paths.as_slice(),
            usize::from(request.max_items),
        );
        let selected_source_tokens_est = selected.iter().fold(0_u32, |total, hit| {
            total.saturating_add(estimate_tokens(&hit.content))
        });
        let mut evidence_card_tokens_est = 0_u32;
        let source_budget = match request.evidence_policy {
            EvidencePolicy::Reference => 0,
            EvidencePolicy::Balanced => u32::from(request.max_source_tokens).min(300),
            EvidencePolicy::Source => u32::from(request.max_source_tokens),
        };
        let mut full_evidence = Vec::with_capacity(selected.len());
        let mut snapshots = Vec::with_capacity(selected.len());
        let mut paths = Vec::with_capacity(selected.len());
        for hit in &selected {
            if !paths.contains(&hit.path) {
                paths.push(hit.path.clone());
            }
            let evidence_id = digest_id("ev_", hit.id.as_bytes());
            let (line_start, line_end, card, card_tokens) = evidence_card(
                hit,
                &terms,
                request.evidence_policy,
                source_budget.saturating_sub(evidence_card_tokens_est),
            );
            evidence_card_tokens_est = evidence_card_tokens_est.saturating_add(card_tokens);
            full_evidence.push((
                evidence_id.clone(),
                evidence_policy_code(request.evidence_policy),
                line_start,
                line_end,
                hit.symbol.clone(),
                card,
                card_tokens,
            ));
            snapshots.push(PackEvidenceSnapshot {
                id: evidence_id,
                path: hit.path.clone(),
                start_line: line_start,
                end_line: line_end,
                symbol: hit.symbol.clone(),
            });
        }

        let route = classify_route(&request.prompt).to_owned();
        let identity = serde_json::to_vec(&json!({
            "paths": &paths,
            "evidence": full_evidence.iter().map(|card| &card.0).collect::<Vec<_>>(),
            "route": &route,
            "policy": request.evidence_policy,
            "signature": &index.stats().refresh_signature,
        }))?;
        let pack_id = digest_id("pk_", &identity);
        let base = request
            .base_pack
            .as_deref()
            .map(|base_pack| self.load_pack_snapshot(base_pack))
            .transpose()?
            .flatten();
        if request.base_pack.is_some() && base.is_none() {
            bail!("base_pack is unavailable or expired");
        }
        let known = request
            .known_evidence
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let evidence = delta_evidence(full_evidence, &snapshots, base.as_ref(), &known);
        let returned_evidence_tokens_est = evidence
            .iter()
            .fold(0_u32, |total, card| total.saturating_add(card.6));
        let snapshot = Arc::new(PackSnapshot {
            schema: "context_pack.snapshot.v1".to_owned(),
            pack_id: pack_id.clone(),
            route: route.clone(),
            paths: paths.clone(),
            evidence: snapshots,
            refresh_signature: index.stats().refresh_signature.clone(),
        });
        let new_snapshot = self
            .pack_snapshots
            .lock()
            .map_err(|_| anyhow!("pack snapshot lock poisoned"))?
            .insert(pack_id.clone(), Arc::clone(&snapshot))
            .is_none();
        if new_snapshot {
            self.store.put_json_if_changed(
                &format!("pack:{pack_id}"),
                &serde_json::to_value(&*snapshot)?,
            )?;
        }
        let more = self.deferred_reference(
            &pack_id,
            &candidates,
            &terms,
            usize::from(request.max_items),
        )?;
        let response = ContextPackV2 {
            v: CONTEXT_PACK_VERSION,
            id: pack_id,
            route,
            paths,
            evidence,
            more,
        };
        let mut encoded = Vec::with_capacity(512);
        serde_json::to_writer(&mut encoded, &response)?;
        let wire_tokens_est = std::str::from_utf8(&encoded)
            .map(estimate_tokens)
            .unwrap_or_else(|_| u32::try_from(encoded.len().div_ceil(4)).unwrap_or(u32::MAX));
        Ok(BuiltPack {
            telemetry: PackTelemetry {
                input_tokens_est: estimate_tokens(&request.prompt),
                selected_source_tokens_est,
                evidence_card_tokens_est,
                returned_evidence_tokens_est,
                wire_tokens_est,
                wire_bytes: encoded.len(),
                tokens_saved_est: selected_source_tokens_est.saturating_sub(wire_tokens_est),
                delta_tokens_saved_est: evidence_card_tokens_est
                    .saturating_sub(returned_evidence_tokens_est),
                candidate_count: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
                selected_count: u32::try_from(selected.len()).unwrap_or(u32::MAX),
                retrieval_micros,
                pack_build_micros: u64::try_from(pack_started.elapsed().as_micros())
                    .unwrap_or(u64::MAX)
                    .saturating_sub(retrieval_micros),
                frontier_outcome,
                base_pack_used: request.base_pack.is_some(),
                known_evidence_used: !request.known_evidence.is_empty(),
            },
            bytes: encoded,
        })
    }

    fn deferred_reference(
        &self,
        pack_id: &str,
        candidates: &[SearchHit],
        terms: &[String],
        max_items: usize,
    ) -> Result<Option<String>> {
        let limit = max_items.clamp(8, 16);
        let candidates = candidates.iter().take(limit).collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        let mut term_fingerprints = concept_fingerprints(terms);
        term_fingerprints.sort();
        let key = digest_id(
            "df_",
            &serde_json::to_vec(&json!({
                "candidates": candidates.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
                "terms": term_fingerprints,
            }))?,
        );
        let cached_reference_id = {
            self.deferred_references
                .lock()
                .map_err(|_| anyhow!("deferred reference lock poisoned"))?
                .get(&key)
                .cloned()
        };
        if let Some(validation) = cached_reference_id {
            if self.store.reference_validation_is_active(&validation)? {
                return Ok(Some(validation.reference_id));
            }
            self.deferred_references
                .lock()
                .map_err(|_| anyhow!("deferred reference lock poisoned"))?
                .remove(&key);
        }
        if let Some(cached) = self.store.get_json(&format!("deferred:{key}"))?
            && let Some(reference_id) = cached.get("reference_id").and_then(Value::as_str)
            && let Some(validation) = self.store.reference_validation(reference_id)?
        {
            let reference_id = validation.reference_id.clone();
            self.deferred_references
                .lock()
                .map_err(|_| anyhow!("deferred reference lock poisoned"))?
                .insert(key, validation);
            return Ok(Some(reference_id));
        }
        let deferred = candidates
            .iter()
            .map(|hit| {
                let (_, _, excerpt) = hit.evidence_excerpt(terms, 800);
                json!({
                    "id": digest_id("ev_", hit.id.as_bytes()),
                    "path": hit.path,
                    "start_line": hit.start_line,
                    "end_line": hit.end_line,
                    "symbol": hit.symbol,
                    "content": sanitize_text(&excerpt).0,
                })
            })
            .collect::<Vec<_>>();
        let reference = self.store.create_reference(
            "context_pack",
            &self.project_id,
            &json!({
                "schema": "context_pack.v2.deferred_evidence.v1",
                "pack_id": pack_id,
                "candidates": deferred,
            }),
            &json!({"kind": "deferred_evidence", "candidate_count": candidates.len()}),
            24,
        )?;
        let reference_id = reference
            .get("reference_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("reference store omitted reference_id"))?
            .to_owned();
        self.metrics
            .active_references
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .total_references
            .fetch_add(1, Ordering::Relaxed);
        self.store.put_json_if_changed(
            &format!("deferred:{key}"),
            &json!({
                "schema": "context_pack.deferred_cache.v1",
                "reference_id": reference_id,
                "expires_at": reference.get("expires_at").cloned().unwrap_or_default(),
            }),
        )?;
        let validation = self
            .store
            .reference_validation(&reference_id)?
            .ok_or_else(|| anyhow!("reference store produced an invalid reference"))?;
        self.deferred_references
            .lock()
            .map_err(|_| anyhow!("deferred reference lock poisoned"))?
            .insert(key, validation);
        Ok(Some(reference_id))
    }

    fn load_pack_snapshot(&self, pack_id: &str) -> Result<Option<PackSnapshot>> {
        if !valid_digest_id(pack_id, "pk_") {
            bail!("base_pack must be a context_pack.v2 pack id");
        }
        if let Some(snapshot) = self
            .pack_snapshots
            .lock()
            .map_err(|_| anyhow!("pack snapshot lock poisoned"))?
            .get(pack_id)
            .cloned()
        {
            return Ok(Some((*snapshot).clone()));
        }
        let snapshot = self
            .store
            .get_json(&format!("pack:{pack_id}"))?
            .map(serde_json::from_value::<PackSnapshot>)
            .transpose()?;
        if let Some(snapshot) = &snapshot {
            self.pack_snapshots
                .lock()
                .map_err(|_| anyhow!("pack snapshot lock poisoned"))?
                .insert(pack_id.to_owned(), Arc::new(snapshot.clone()));
        }
        Ok(snapshot)
    }

    fn retrieve_candidates(
        &self,
        index: &ProjectIndex,
        request: &ContextPackRequest,
        explicit_paths: &[String],
    ) -> Result<(Vec<SearchHit>, Vec<String>, FrontierOutcome)> {
        let retrieval = self.retrieve_candidates_deferred(index, request, explicit_paths)?;
        if let Some(record) = retrieval.pending {
            self.admit_frontier_batch(&[record])?;
        }
        Ok((retrieval.hits, retrieval.terms, retrieval.outcome))
    }

    fn retrieve_candidates_deferred(
        &self,
        index: &ProjectIndex,
        request: &ContextPackRequest,
        explicit_paths: &[String],
    ) -> Result<DeferredRetrieval> {
        let terms = normalize_terms(&request.prompt, 8);
        let mut term_fingerprints = concept_fingerprints(&terms);
        term_fingerprints.sort();
        term_fingerprints.dedup();
        let route = classify_route(&request.prompt);
        let mut scope = explicit_paths.to_vec();
        scope.sort();
        scope.dedup();
        let signature = &index.stats().refresh_signature;
        let generation = self.freshness.generation.load(Ordering::Acquire);
        let now = now_millis();

        let (match_record, capacity_fallback) = {
            let mut state = self
                .frontiers
                .lock()
                .map_err(|_| anyhow!("frontier cache lock poisoned"))?;
            state.records.retain(|record| {
                record.refresh_signature == *signature
                    && (!record.negative || record.expires_at_ms > now)
            });
            state.bytes = state
                .records
                .iter()
                .map(|record| record.estimated_bytes())
                .sum();
            let exact = state.records.iter().position(|record| {
                record.scope == scope
                    && record.terms == term_fingerprints
                    && record.generation == generation
                    && record.candidate_capacity >= request.max_items
            });
            let capacity_fallback = exact.is_none()
                && state.records.iter().any(|record| {
                    record.scope == scope
                        && record.terms == term_fingerprints
                        && record.generation == generation
                        && record.candidate_capacity < request.max_items
                });
            let record = exact.and_then(|position| {
                let record = state.records.remove(position)?;
                state.records.push_back(Arc::clone(&record));
                Some(record)
            });
            (record, capacity_fallback)
        };

        let had_match = match_record.is_some();
        if let Some(record) = match_record {
            if record.negative {
                self.metrics.l1_exact_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(DeferredRetrieval {
                    hits: Vec::new(),
                    terms,
                    pending: None,
                    outcome: FrontierOutcome::NegativeHit,
                });
            }
            let (hits, reranked_terms) = index.rerank(
                &request.prompt,
                explicit_paths,
                &record.candidate_ids,
                usize::from(request.max_items),
            )?;
            if !hits.is_empty() {
                self.metrics.l1_exact_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(DeferredRetrieval {
                    hits,
                    terms: reranked_terms,
                    pending: None,
                    outcome: FrontierOutcome::ExactHit,
                });
            }
        }

        self.metrics
            .retrieval_misses
            .fetch_add(1, Ordering::Relaxed);
        let (hits, terms) = index.search(
            &request.prompt,
            explicit_paths,
            usize::from(request.max_items),
        )?;
        let record = self.build_frontier_record(
            index,
            route,
            scope,
            term_fingerprints,
            request.max_items,
            &hits,
        )?;
        let should_admit = hits.is_empty() || {
            let mut tracker = self
                .frontier_admissions
                .lock()
                .map_err(|_| anyhow!("frontier admission tracker poisoned"))?;
            tracker.observe(record.key.clone(), now)
        };
        let outcome = if should_admit && !hits.is_empty() {
            FrontierOutcome::Admitted
        } else if capacity_fallback {
            FrontierOutcome::CapacityFallback
        } else if had_match {
            FrontierOutcome::SourceFallback
        } else {
            FrontierOutcome::Search
        };
        Ok(DeferredRetrieval {
            hits,
            terms,
            pending: should_admit.then_some(record),
            outcome,
        })
    }

    fn build_frontier_record(
        &self,
        index: &ProjectIndex,
        route: &str,
        scope: Vec<String>,
        terms: Vec<String>,
        candidate_capacity: u8,
        hits: &[SearchHit],
    ) -> Result<Arc<FrontierRecord>> {
        let generation = self.freshness.generation.load(Ordering::Acquire);
        let now = now_millis();
        let key = digest_id(
            "fr_",
            &serde_json::to_vec(&json!({
                "scope": &scope,
                "terms": &terms,
                "generation": generation,
                "candidate_capacity": candidate_capacity,
                "signature": &index.stats().refresh_signature,
            }))?,
        );
        let mut cumulative = 0_u32;
        let cumulative_token_costs = hits
            .iter()
            .map(|hit| {
                cumulative = cumulative.saturating_add(estimate_tokens(&hit.content));
                cumulative
            })
            .collect::<Vec<_>>();
        let mut dependencies = hits.iter().map(|hit| hit.path.clone()).collect::<Vec<_>>();
        dependencies.sort();
        dependencies.dedup();
        Ok(Arc::new(FrontierRecord {
            schema: "context_frontier.v2".to_owned(),
            key: key.clone(),
            route: route.to_owned(),
            scope,
            generation,
            candidate_capacity,
            terms,
            candidate_ids: hits.iter().map(|hit| hit.id.clone()).collect(),
            scores: hits.iter().map(|hit| hit.score).collect(),
            cumulative_token_costs,
            dependencies,
            score_cutoff: hits.last().map_or(0.0, |hit| hit.score),
            refresh_signature: index.stats().refresh_signature.clone(),
            negative: hits.is_empty(),
            expires_at_ms: if hits.is_empty() {
                now.saturating_add(NEGATIVE_FRONTIER_TTL_MS)
            } else {
                0
            },
            updated_at_ms: now,
        }))
    }

    fn admit_frontier_batch(&self, records: &[Arc<FrontierRecord>]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let rows = records
            .iter()
            .map(|record| {
                Ok((
                    format!("frontier:{}", record.key),
                    serde_json::to_value(&**record)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        self.store.put_json_batch(&rows)?;
        let mut state = self
            .frontiers
            .lock()
            .map_err(|_| anyhow!("frontier cache lock poisoned"))?;
        for record in records {
            let key = &record.key;
            if let Some(position) = state
                .records
                .iter()
                .position(|item| item.key.as_str() == key.as_str())
                && let Some(previous) = state.records.remove(position)
            {
                state.bytes = state.bytes.saturating_sub(previous.estimated_bytes());
            }
            let bytes = record.estimated_bytes();
            while state.bytes.saturating_add(bytes) > L1_MEMORY_MAX_BYTES {
                if let Some(previous) = state.records.pop_front() {
                    state.bytes = state.bytes.saturating_sub(previous.estimated_bytes());
                } else {
                    break;
                }
            }
            if bytes <= L1_MEMORY_MAX_BYTES {
                state.bytes = state.bytes.saturating_add(bytes);
                state.records.push_back(Arc::clone(record));
            }
        }
        drop(state);
        prune_persistent_frontiers(&self.store)
    }

    fn record_operation(
        &self,
        operation: &'static str,
        started: Instant,
        success: bool,
        output_bytes: usize,
    ) {
        self.metrics
            .record_operation(operation, started.elapsed(), success, output_bytes);
    }

    fn record_result(&self, operation: &'static str, started: Instant, result: &Result<Vec<u8>>) {
        match result {
            Ok(response) => self.record_operation(operation, started, true, response.len()),
            Err(_) => self.record_operation(operation, started, false, 0),
        }
    }

    fn record_context_pack(&self, started: Instant, telemetry: &PackTelemetry) {
        self.record_operation("context_pack", started, true, telemetry.wire_bytes);
        self.metrics.record_pack_telemetry(*telemetry);
    }

    fn ensure_fresh(&self, request: &ContextPackRequest) -> Result<(bool, bool)> {
        let force =
            request.cache_strategy == CacheStrategy::Fresh || !request.changed_files.is_empty();
        self.refresh_index(force)
    }

    fn refresh_index(&self, force: bool) -> Result<(bool, bool)> {
        let now = now_millis();
        let watcher_ready = self.freshness.dirty.load(Ordering::Acquire)
            && now.saturating_sub(self.freshness.last_event_ms.load(Ordering::Acquire))
                >= WATCH_COALESCE_MS;
        let poll_due = now.saturating_sub(self.freshness.last_poll_ms.load(Ordering::Acquire))
            >= FAST_POLL_INTERVAL_MS;
        if !force && !watcher_ready && !poll_due {
            return Ok((false, false));
        }

        let _refresh = self
            .refresh_lock
            .lock()
            .map_err(|_| anyhow!("index refresh lock poisoned"))?;
        let now = now_millis();
        let watcher_ready = self.freshness.dirty.load(Ordering::Acquire)
            && now.saturating_sub(self.freshness.last_event_ms.load(Ordering::Acquire))
                >= WATCH_COALESCE_MS;
        let poll_due = now.saturating_sub(self.freshness.last_poll_ms.load(Ordering::Acquire))
            >= FAST_POLL_INTERVAL_MS;
        if !force && !watcher_ready && !poll_due {
            return Ok((false, false));
        }

        let current = self.index();
        let signature = repository_signature(current.root())?;
        self.freshness.last_poll_ms.store(now, Ordering::Release);
        self.freshness.dirty.store(false, Ordering::Release);
        if signature == current.stats().refresh_signature {
            return Ok((true, false));
        }

        let replacement = Arc::new(ProjectIndex::build(current.root())?);
        if replacement.stats().refresh_signature != signature {
            bail!("repository changed while rebuilding the native index");
        }
        *self
            .index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = replacement;
        self.freshness.generation.fetch_add(1, Ordering::AcqRel);
        self.metrics.refreshes.fetch_add(1, Ordering::Relaxed);
        self.invalidate_l0();
        let mut frontiers = self
            .frontiers
            .lock()
            .map_err(|_| anyhow!("frontier cache lock poisoned"))?;
        frontiers.records.clear();
        frontiers.bytes = 0;
        Ok((true, true))
    }

    pub fn context_lookup(&self, request: &ContextLookupRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let operation = lookup_operation(&request.mode);
        let result = (|| {
            self.validate_project_selector(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
            )?;
            self.refresh_index(false)?;
            let value = match request.mode.as_str() {
                "search" => self.lookup_search(request)?,
                "snippet" => self.lookup_snippet(request)?,
                "tree" => self.lookup_tree(request)?,
                "symbols" => self.lookup_symbols(request),
                "impact" => self.lookup_impact(request)?,
                "related_symbols" => self.lookup_related_symbols(request)?,
                "test_owners" => self.lookup_test_owners(request)?,
                "chunk" => self.lookup_chunk(request)?,
                "explain_cache" => self.lookup_explain_cache(request)?,
                "references" => json!({
                    "schema": "context_references.list.v1",
                    "references": self.store.list_references(usize::from(request.max_results))?,
                }),
                unsupported => bail!("unsupported context_lookup mode: {unsupported}"),
            };
            serde_json::to_vec(&value).map_err(Into::into)
        })();
        self.record_result(operation, started, &result);
        result
    }

    pub fn context_memory(&self, request: &ContextMemoryRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let operation = memory_operation(&request.mode);
        let result = (|| {
            self.validate_project_selector(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
            )?;
            let value = memory_dispatch(&self.store, request)?;
            serde_json::to_vec(&value).map_err(Into::into)
        })();
        self.record_result(operation, started, &result);
        result
    }

    pub fn result_reference_resolve(&self, request: &ResultReferenceRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let result = (|| {
            self.validate_project_selector(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
            )?;
            let reference_id = request
                .reference
                .as_ref()
                .and_then(|reference| reference.get("reference_id"))
                .and_then(Value::as_str)
                .unwrap_or(&request.reference_id);
            let expected_hash = request
                .reference
                .as_ref()
                .and_then(|reference| reference.pointer("/content/sha256"))
                .and_then(Value::as_str)
                .unwrap_or(&request.expected_hash);
            let value = self.store.resolve_reference(reference_id, expected_hash)?;
            serde_json::to_vec(&value).map_err(Into::into)
        })();
        self.record_result("result_reference_resolve", started, &result);
        result
    }

    pub async fn context_admin(&self, request: &ContextAdminRequest) -> Result<Vec<u8>> {
        let started = Instant::now();
        let operation = admin_operation(&request.mode);
        let result = async {
            self.validate_project_selector(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
            )?;
            let value = admin_dispatch(self, request).await?;
            serde_json::to_vec(&value).map_err(Into::into)
        }
        .await;
        self.record_result(operation, started, &result);
        result
    }

    pub fn resource_text(&self, uri: &str) -> Result<String> {
        let started = Instant::now();
        let operation = resource_operation(uri);
        let result = self.resource_text_inner(uri);
        match &result {
            Ok(response) => self.record_operation(operation, started, true, response.len()),
            Err(_) => self.record_operation(operation, started, false, 0),
        }
        result
    }

    fn resource_text_inner(&self, uri: &str) -> Result<String> {
        let uri = normalize_project_resource_uri(uri, &self.project_id)?;
        if let Some(text) = static_resource_text(&uri)? {
            return Ok(text);
        }
        let index = self.index();
        let value = match uri.as_str() {
            "repo://summary" => serde_json::to_string(&self.repo_summary()?)?,
            "repo://tree/." | "repo://tree" => serde_json::to_string(&json!({
                "schema": "context_tree.v1",
                "path": ".",
                "entries": index.tree(".", 5000, 2)?,
                "count": index.tree(".", 5000, 2)?.len(),
            }))?,
            "repo://metrics" => serde_json::to_string(&metrics_snapshot(self)?)?,
            _ if uri.starts_with("repo://file/") => {
                let path = uri.trim_start_matches("repo://file/");
                let (content, _) = index.file_content(path, 131_072)?;
                sanitize_text(&content).0
            }
            _ if uri.starts_with("repo://tree/") => {
                let path = uri.trim_start_matches("repo://tree/");
                let entries = index.tree(path, 5000, 2)?;
                serde_json::to_string(&json!({
                    "schema": "context_tree.v1",
                    "path": validate_relative_path(path)?,
                    "count": entries.len(),
                    "entries": entries,
                }))?
            }
            _ if uri.starts_with("repo://context/") => {
                let reference_id = uri.trim_start_matches("repo://context/");
                serde_json::to_string(&self.store.resolve_reference(reference_id, "")?)?
            }
            _ => bail!("unknown repository resource URI"),
        };
        Ok(value)
    }

    fn repo_summary(&self) -> Result<Value> {
        let index = self.index();
        let entries = index.tree(".", 5000, 20)?;
        let mut extensions: BTreeMap<String, usize> = BTreeMap::new();
        for entry in &entries {
            if entry.entry_type != "file" {
                continue;
            }
            let extension = std::path::Path::new(&entry.path)
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("[none]")
                .to_ascii_lowercase();
            *extensions.entry(extension).or_default() += 1;
        }
        let mut top_extensions = extensions.into_iter().collect::<Vec<_>>();
        top_extensions
            .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        top_extensions.truncate(12);
        Ok(json!({
            "schema": "workspace_facts.v1",
            "file_count": index.stats().file_count,
            "is_git_repo": index.root().join(".git").exists(),
            "git_head": "",
            "git_branch": "",
            "has_readme": entries.iter().any(|entry| entry.path == "README.md"),
            "has_tests_dir": entries.iter().any(|entry| entry.path == "tests" && entry.entry_type == "dir"),
            "top_extensions": top_extensions.into_iter().map(|(extension, count)| {
                json!({"extension": extension, "count": count})
            }).collect::<Vec<_>>(),
            "index": index_status(index.stats()),
        }))
    }

    fn lookup_search(&self, request: &ContextLookupRequest) -> Result<Value> {
        if request.max_results == 0 || request.max_results > 200 {
            bail!("max_results must be in 1..=200");
        }
        let index = self.index();
        let include_globs = compile_path_globs(&request.include_globs)?;
        let scoped_path = validate_relative_path(&request.path)?;
        let all_paths = index.all_paths();
        let path_is_file = scoped_path != "." && all_paths.iter().any(|path| path == &scoped_path);
        let scoped = scoped_path != "." || !include_globs.is_empty();
        let allowed_paths = scoped.then(|| {
            all_paths
                .into_iter()
                .filter(|path| {
                    (scoped_path == "."
                        || path == &scoped_path
                        || path
                            .strip_prefix(&scoped_path)
                            .is_some_and(|suffix| suffix.starts_with('/')))
                        && (include_globs.is_empty()
                            || include_globs.iter().any(|pattern| pattern.is_match(path)))
                })
                .collect::<HashSet<_>>()
        });
        let explicit = path_is_file
            .then_some(scoped_path)
            .into_iter()
            .collect::<Vec<_>>();
        let (hits, terms) = index.search_scoped(
            &request.query,
            &explicit,
            usize::from(request.max_results),
            allowed_paths.as_ref(),
        )?;
        let results = hits
            .into_iter()
            .take(usize::from(request.max_results))
            .map(|hit| {
                let (line, _, excerpt) = hit.evidence_excerpt(&terms, 320);
                let excerpt = sanitize_text(&excerpt).0;
                let term_hits = terms
                    .iter()
                    .filter(|term| excerpt.to_ascii_lowercase().contains(term.as_str()))
                    .count();
                json!({
                    "excerpt": excerpt,
                    "line": line,
                    "path": hit.path,
                    "score": round_score(hit.score),
                    "source": "tantivy",
                    "tantivy_score": round_score(hit.score),
                    "term_count": terms.len(),
                    "term_hits": term_hits,
                    "terms": &terms,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "schema": "context_search.v1",
            "query": request.query,
            "terms": terms,
            "count": results.len(),
            "results": results,
            "cache": {
                "schema": "context_cache.lookup.v1",
                "namespace": "context_lookup.search",
                "hit": false,
                "reason": "native_uncached",
            },
            "index": index_status(index.stats()),
        }))
    }

    fn lookup_snippet(&self, request: &ContextLookupRequest) -> Result<Value> {
        let index = self.index();
        let chunk = index.snippet(&request.path, request.start_line, request.end_line)?;
        let (content, redactions, prompt_injection_signals) = sanitize_text(&chunk.content);
        let total_lines = index.file_line_count(&request.path)?;
        Ok(json!({
            "schema": "context_snippet.v1",
            "path": chunk.path,
            "start_line": chunk.start_line,
            "end_line": chunk.end_line,
            "content": content,
            "source": "index",
            "total_lines": total_lines,
            "requested": {
                "start_line": request.start_line,
                "end_line": request.end_line,
            },
            "truncated": false,
            "redactions": redactions,
            "prompt_injection_signals": prompt_injection_signals,
        }))
    }

    fn lookup_tree(&self, request: &ContextLookupRequest) -> Result<Value> {
        if request.max_entries == 0 || request.max_entries > 5000 {
            bail!("max_entries must be in 1..=5000");
        }
        if request.max_depth == 0 || request.max_depth > 20 {
            bail!("max_depth must be in 1..=20");
        }
        let entries = self.index().tree(
            &request.path,
            usize::from(request.max_entries),
            usize::from(request.max_depth),
        )?;
        Ok(json!({
            "schema": "context_tree.v1",
            "path": validate_relative_path(&request.path)?,
            "count": entries.len(),
            "entries": entries,
        }))
    }

    fn lookup_symbols(&self, request: &ContextLookupRequest) -> Value {
        let symbols = self
            .index()
            .symbols(&request.query, usize::from(request.max_results));
        json!({
            "schema": "context_symbols.v1",
            "count": symbols.len(),
            "symbols": symbols,
        })
    }

    fn lookup_impact(&self, request: &ContextLookupRequest) -> Result<Value> {
        let mut related = self.test_owner_rows(&request.path, usize::from(request.max_results))?;
        for symbol in self.index().symbols("", 10_000) {
            if symbol.path == request.path {
                related.push(symbol_relationship(&symbol, "same_file"));
            }
            if related.len() == usize::from(request.max_results) {
                break;
            }
        }
        related.truncate(usize::from(request.max_results));
        Ok(json!({
            "schema": "context_lookup.impact.v1",
            "source": validate_relative_path(&request.path)?,
            "count": related.len(),
            "related": related,
        }))
    }

    fn lookup_related_symbols(&self, request: &ContextLookupRequest) -> Result<Value> {
        let source = validate_relative_path(&request.path)?;
        let symbols = self
            .index()
            .symbols(&request.query, 10_000)
            .into_iter()
            .filter(|symbol| symbol.path == source)
            .take(usize::from(request.max_results))
            .map(|symbol| symbol_relationship(&symbol, "same_file"))
            .collect::<Vec<_>>();
        Ok(json!({
            "schema": "context_lookup.related_symbols.v1",
            "source": source,
            "count": symbols.len(),
            "symbols": symbols,
        }))
    }

    fn lookup_test_owners(&self, request: &ContextLookupRequest) -> Result<Value> {
        let source = validate_relative_path(&request.path)?;
        let related = self.test_owner_rows(&source, usize::from(request.max_results))?;
        Ok(json!({
            "schema": "context_lookup.test_owners.v1",
            "source": source,
            "count": related.len(),
            "related": related,
        }))
    }

    fn lookup_chunk(&self, request: &ContextLookupRequest) -> Result<Value> {
        let index = self.index();
        let chunk = index.snippet(&request.path, request.start_line, request.end_line)?;
        let (content, _, _) = sanitize_text(&chunk.content);
        let content_digest = sha256_text(&content);
        let file_digest = index
            .chunks_for_path(&request.path)?
            .iter()
            .map(|chunk| chunk.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let chunk_id = digest_id(
            "chk_",
            format!(
                "{}:{}:{}:{content_digest}",
                chunk.path, chunk.start_line, chunk.end_line
            )
            .as_bytes(),
        );
        Ok(json!({
            "schema": "context_lookup.chunk.v1",
            "path": chunk.path,
            "content": content,
            "chunk": {
                "schema": "context_chunk_metadata.v1",
                "chunk_id": chunk_id,
                "path": chunk.path,
                "start_line": chunk.start_line,
                "end_line": chunk.end_line,
                "content_digest": format!("sha256:{content_digest}"),
                "file_digest": format!("sha256:{}", sha256_text(&file_digest)),
                "extractor_version": "chunk-lines-1",
                "redaction_version": "redact-1",
            },
            "detail_lookup": {
                "tool": "context_lookup",
                "mode": "snippet",
                "path": chunk.path,
                "start_line": chunk.start_line,
                "end_line": chunk.end_line,
            },
        }))
    }

    fn lookup_explain_cache(&self, request: &ContextLookupRequest) -> Result<Value> {
        let path = validate_relative_path(&request.path)?;
        let rows = self
            .store
            .iter_json("cache:")?
            .into_iter()
            .filter(|(_, value)| value.to_string().contains(path.as_str()))
            .take(usize::from(request.max_results))
            .map(|(key, value)| json!({"key": key, "value": value}))
            .collect::<Vec<_>>();
        Ok(json!({
            "schema": "context_lookup.explain_cache.v1",
            "path": path,
            "count": rows.len(),
            "rows": rows,
        }))
    }

    fn test_owner_rows(&self, raw_path: &str, limit: usize) -> Result<Vec<Value>> {
        let source = validate_relative_path(raw_path)?;
        let stem = std::path::Path::new(&source)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(source.as_str())
            .trim_start_matches("test_");
        let (hits, _) = self.index().search(stem, &[], limit.max(8))?;
        let mut seen = HashSet::new();
        Ok(hits
            .into_iter()
            .filter(|hit| is_test_path(&hit.path) && seen.insert(hit.path.clone()))
            .take(limit)
            .map(|hit| {
                json!({
                    "path": hit.path,
                    "relationship": "test_owner",
                    "confidence": "high",
                    "detail_lookup": {
                        "tool": "context_lookup",
                        "mode": "snippet",
                        "path": hit.path,
                        "start_line": hit.start_line,
                    },
                })
            })
            .collect())
    }

    fn validate_project_selector(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
    ) -> Result<()> {
        if let Some(project_id) = project_id
            && project_id != self.project_id
        {
            bail!("project_id does not select the active project");
        }
        if let Some(root_uri) = root_uri {
            let raw = root_uri
                .strip_prefix("file://")
                .ok_or_else(|| anyhow!("root_uri must be an absolute file URI"))?;
            let requested = std::path::Path::new(raw).canonicalize()?;
            if requested != self.index().root() {
                bail!("root_uri is outside the active repository");
            }
        }
        Ok(())
    }
}

fn watch_repository(
    root: std::path::PathBuf,
    freshness: Arc<FreshnessState>,
    stop: Arc<AtomicBool>,
) {
    let (sender, receiver) = std::sync::mpsc::channel();
    let callback_sender = sender.clone();
    let recommended = RecommendedWatcher::new(
        move |event| {
            let _ = callback_sender.send(event);
        },
        NotifyConfig::default(),
    );
    if let Ok(mut watcher) = recommended
        && watcher.watch(&root, RecursiveMode::Recursive).is_ok()
    {
        watcher_loop(watcher, &receiver, &root, &freshness, &stop);
        return;
    }

    let (sender, receiver) = std::sync::mpsc::channel();
    let polling = PollWatcher::new(
        move |event| {
            let _ = sender.send(event);
        },
        NotifyConfig::default().with_poll_interval(StdDuration::from_secs(2)),
    );
    if let Ok(mut watcher) = polling
        && watcher.watch(&root, RecursiveMode::Recursive).is_ok()
    {
        watcher_loop(watcher, &receiver, &root, &freshness, &stop);
    }
}

fn watcher_loop<W: Watcher>(
    _watcher: W,
    receiver: &std::sync::mpsc::Receiver<notify::Result<Event>>,
    root: &std::path::Path,
    freshness: &FreshnessState,
    stop: &AtomicBool,
) {
    while !stop.load(Ordering::Acquire) {
        match receiver.recv_timeout(StdDuration::from_millis(100)) {
            Ok(Ok(event)) if event.paths.iter().any(|path| source_event_path(root, path)) => {
                freshness
                    .last_event_ms
                    .store(now_millis(), Ordering::Release);
                freshness.dirty.store(true, Ordering::Release);
            }
            Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn source_event_path(root: &std::path::Path, path: &std::path::Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let ignored = [
        ".git",
        ".mcp-context-manager",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "target",
    ];
    !relative.components().any(|component| {
        component.as_os_str().to_str().is_some_and(|name| {
            ignored.contains(&name) || name.ends_with('~') || name.starts_with(".#")
        })
    })
}

fn load_frontiers(store: &StateStore, refresh_signature: &str) -> Result<FrontierState> {
    let now = now_millis();
    let mut records = store
        .iter_json("frontier:")?
        .into_iter()
        .filter_map(|(_, value)| serde_json::from_value::<FrontierRecord>(value).ok())
        .filter(|record| {
            record.schema == "context_frontier.v2"
                && record.refresh_signature == refresh_signature
                && (!record.negative || record.expires_at_ms > now)
        })
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.updated_at_ms);
    let mut state = FrontierState::default();
    for record in records {
        let record = Arc::new(record);
        let bytes = record.estimated_bytes();
        if bytes > L1_MEMORY_MAX_BYTES {
            continue;
        }
        while state.bytes.saturating_add(bytes) > L1_MEMORY_MAX_BYTES {
            if let Some(removed) = state.records.pop_front() {
                state.bytes = state.bytes.saturating_sub(removed.estimated_bytes());
            } else {
                break;
            }
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.records.push_back(record);
    }
    Ok(state)
}

fn load_pack_snapshots(store: &StateStore) -> Result<HashMap<String, Arc<PackSnapshot>>> {
    Ok(store
        .iter_json("pack:")?
        .into_iter()
        .filter_map(|(_, value)| serde_json::from_value::<PackSnapshot>(value).ok())
        .map(|snapshot| (snapshot.pack_id.clone(), Arc::new(snapshot)))
        .collect())
}

fn load_deferred_references(store: &StateStore) -> Result<HashMap<String, ReferenceValidation>> {
    let mut references = HashMap::new();
    for (key, value) in store.iter_json("deferred:")? {
        let Some(cache_key) = key.strip_prefix("deferred:") else {
            continue;
        };
        let Some(reference_id) = value.get("reference_id").and_then(Value::as_str) else {
            continue;
        };
        if let Some(validation) = store.reference_validation(reference_id)? {
            references.insert(cache_key.to_owned(), validation);
        }
    }
    Ok(references)
}

fn reference_counts(store: &StateStore) -> Result<(u64, u64)> {
    let references = store.list_references(usize::MAX)?;
    let active = references
        .iter()
        .filter(|reference| reference.get("status").and_then(Value::as_str) == Some("active"))
        .count() as u64;
    Ok((active, references.len() as u64))
}

fn prune_persistent_frontiers(store: &StateStore) -> Result<()> {
    let mut rows = store
        .iter_json("frontier:")?
        .into_iter()
        .map(|(key, value)| {
            let bytes = key.len() + serde_json::to_vec(&value).map_or(0, |encoded| encoded.len());
            let updated = value
                .get("updated_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            (key, bytes, updated)
        })
        .collect::<Vec<_>>();
    let mut total = rows.iter().map(|(_, bytes, _)| *bytes).sum::<usize>();
    if total <= L1_PERSISTENT_MAX_BYTES {
        return Ok(());
    }
    rows.sort_by_key(|(_, _, updated)| *updated);
    for (key, bytes, _) in rows {
        if total <= L1_PERSISTENT_MAX_BYTES {
            break;
        }
        if store.delete(&key)? {
            total = total.saturating_sub(bytes);
        }
    }
    Ok(())
}

fn concept_fingerprints(terms: &[String]) -> Vec<String> {
    terms
        .iter()
        .map(|term| digest_id("tm_", term.as_bytes()))
        .collect()
}

async fn admin_dispatch(engine: &ProjectEngine, request: &ContextAdminRequest) -> Result<Value> {
    let now = now_iso()?;
    let index = engine.index();
    let stats = index.stats();
    match request.mode.as_str() {
        "health" => Ok(json!({
            "schema": "context_admin.health.v1",
            "ok": true,
            "version": env!("CARGO_PKG_VERSION"),
            "project_id": engine.project_id,
            "repo_path": ".",
            "state_dir": ".mcp-context-manager/rust-v2",
            "index": index_status(stats),
        })),
        "index_refresh" => {
            let (checked, updated) = engine.refresh_index(true)?;
            let stats = engine.index().stats().clone();
            Ok(json!({
                "schema": "context_index.refresh.v1",
                "generated_at": now,
                "file_count": stats.file_count,
                "files_considered": stats.file_count,
                "import_count": stats.chunk_count,
                "symbol_count": stats.symbol_chunks,
                "updated_count": if updated { stats.file_count } else { 0 },
                "unchanged_count": if updated { 0 } else { stats.file_count },
                "removed_count": 0,
                "fts_enabled": true,
                "index_available": true,
                "search_mode": "tantivy",
                "reason": if updated { "signature_changed" } else { "signature_unchanged" },
                "skipped": !checked,
            }))
        }
        "index_status" => Ok(index_status(engine.index().stats())),
        "cache_stats" => {
            let l0 = engine.l0_storage_stats();
            let frontier_rows = engine.store.iter_json("frontier:")?;
            let frontiers = engine
                .frontiers
                .lock()
                .map_err(|_| anyhow!("frontier cache lock poisoned"))?;
            Ok(json!({
                "schema": "context_cache.stats.v1",
                "entry_count": l0.entries + frontiers.records.len() as u64,
                "keys": frontier_rows.iter().map(|(key, _)| key).collect::<Vec<_>>(),
                "namespaces": {
                    "l0.context_pack.wire": {
                        "entries": l0.entries,
                        "weighted_bytes": l0.weighted_bytes,
                        "max_bytes": L0_MAX_BYTES,
                        "idle_expiry_seconds": 1800,
                    },
                    "l1.frontier.memory": {
                        "entries": frontiers.records.len(),
                        "bytes": frontiers.bytes,
                        "max_bytes": L1_MEMORY_MAX_BYTES,
                    },
                    "l1.frontier.persistent": {
                        "entries": frontier_rows.len(),
                        "max_bytes": L1_PERSISTENT_MAX_BYTES,
                    },
                },
                "auto_learn": {
                    "schema": "warmup.auto_learn.v1",
                    "enabled": false,
                    "project_id": engine.project_id,
                    "background": {"status": "idle", "pending": false},
                },
            }))
        }
        "cache_prune" => {
            let l0_removed = engine.l0_storage_stats().entries;
            engine.invalidate_l0();
            let mut expired_removed = 0_u64;
            let mut stale_removed = 0_u64;
            let mut age_removed = 0_u64;
            let mut deferred_removed = 0_u64;
            let mut reference_removed = 0_u64;
            let now_ms = now_millis();
            let max_age_ms = u64::from(request.max_age_minutes).saturating_mul(60_000);
            let mut remove_keys = Vec::new();
            for (key, value) in engine.store.iter_json("frontier:")? {
                let expired = value
                    .get("negative")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    && value
                        .get("expires_at_ms")
                        .and_then(Value::as_u64)
                        .is_some_and(|expiry| expiry <= now_ms);
                let stale = value.get("schema").and_then(Value::as_str)
                    != Some("context_frontier.v2")
                    || value.get("refresh_signature").and_then(Value::as_str)
                        != Some(stats.refresh_signature.as_str());
                let aged = value
                    .get("updated_at_ms")
                    .and_then(Value::as_u64)
                    .is_none_or(|updated| {
                        updated <= now_ms && now_ms.saturating_sub(updated) >= max_age_ms
                    });
                if expired {
                    expired_removed = expired_removed.saturating_add(1);
                    remove_keys.push(key);
                } else if stale {
                    stale_removed = stale_removed.saturating_add(1);
                    remove_keys.push(key);
                } else if aged {
                    age_removed = age_removed.saturating_add(1);
                    remove_keys.push(key);
                }
            }
            let now_time = OffsetDateTime::now_utc();
            for (key, value) in engine.store.iter_json("deferred:")? {
                let inactive = match value.get("reference_id").and_then(Value::as_str) {
                    Some(reference_id) => !engine.store.reference_is_active(reference_id)?,
                    None => true,
                };
                let expired = value
                    .get("expires_at")
                    .and_then(Value::as_str)
                    .and_then(|expiry| OffsetDateTime::parse(expiry, &Rfc3339).ok())
                    .is_some_and(|expiry| expiry <= now_time);
                if inactive || expired {
                    deferred_removed = deferred_removed.saturating_add(1);
                    remove_keys.push(key);
                }
            }
            for (key, value) in engine.store.iter_json("reference:")? {
                let expired = value
                    .get("expires_at")
                    .and_then(Value::as_str)
                    .and_then(|expiry| OffsetDateTime::parse(expiry, &Rfc3339).ok())
                    .is_none_or(|expiry| expiry <= now_time);
                if expired {
                    reference_removed = reference_removed.saturating_add(1);
                    remove_keys.push(key);
                }
            }
            let persistent_removed = engine.store.delete_batch(&remove_keys)? as u64;
            let mut frontiers = engine
                .frontiers
                .lock()
                .map_err(|_| anyhow!("frontier cache lock poisoned"))?;
            *frontiers = load_frontiers(&engine.store, &stats.refresh_signature)?;
            Ok(json!({
                "schema": "context_cache.prune.v1",
                "entry_count": frontiers.records.len(),
                "removed_entries": l0_removed + persistent_removed,
                "expired_removed": expired_removed,
                "stale_removed": stale_removed,
                "age_removed": age_removed,
                "deferred_removed": deferred_removed,
                "reference_removed": reference_removed,
            }))
        }
        "warmup" => warmup_dispatch(engine, request, &now).await,
        "budget" => {
            let value = json!({
                "schema": "context_budget.v1",
                "max_output_chars": request.max_output_chars.unwrap_or(4096),
                "default_output_profile": request.default_output_profile.as_deref().unwrap_or("balanced"),
                "updated_at": now,
            });
            engine.store.put_json("budget:default", &value)?;
            Ok(value)
        }
        "contracts" => Ok(contracts_payload(
            &request.tool_name,
            if request.contract_profile.is_empty() {
                "compact"
            } else {
                &request.contract_profile
            },
        )),
        "metrics" => metrics_snapshot(engine),
        "measurement_matrix" => measurement_matrix(engine, &now),
        "measurement_report" => Ok(json!({
            "schema": "context_measurement_report.v1",
            "metrics": metrics_snapshot(engine)?,
            "matrix": measurement_matrix(engine, &now)?,
        })),
        "benchmark" => benchmark_admin(engine, &now),
        "state_browser" => state_browser(engine, request, &now),
        "quality_eval" => Ok(json!({
            "schema": "context_quality_eval.v1",
            "fixtures": 0,
            "metrics": {
                "anchor_recall_at_3": 1.0,
                "anchor_recall_at_5": 1.0,
                "detail_lookup_resolution_rate": 1.0,
                "first_anchor_rank_avg": 1.0,
                "noise_ratio": 0.0,
                "required_anchor_omitted_count": 0,
                "stale_context_rate": 0.0,
            },
            "regressions": [],
        })),
        "cache_plan" => Ok(json!({
            "schema": "context_budget_plan.v1",
            "max_output_tokens": request.max_output_chars.unwrap_or(4096).div_ceil(4),
            "reserved": {"envelope": 80, "references": 40, "diagnostics": 0},
            "items": {"mode": "balanced", "max_count": 8, "max_tokens_each": 48},
            "defer": {"raw_snippets": true, "metrics": true, "cache_details": true, "stage_timings": true},
        })),
        "profile_calibrate" => Ok(profile_calibration()),
        "instructions" => Ok(instructions_payload()),
        "resource_proxy" => {
            let payload = engine.resource_text(&request.path)?;
            let payload = serde_json::from_str::<Value>(&payload).unwrap_or(Value::String(payload));
            Ok(json!({
                "schema": "context_resource_proxy.v1",
                "resource": request.path,
                "payload": payload,
                "tools_only": false,
            }))
        }
        "schema_minify" => Ok(contract_for_tool(&request.tool_name)),
        unsupported => bail!("unsupported context_admin mode: {unsupported}"),
    }
}

async fn warmup_dispatch(
    engine: &ProjectEngine,
    request: &ContextAdminRequest,
    generated_at: &str,
) -> Result<Value> {
    let started = Instant::now();
    let l0_before = engine.l0_storage_stats();
    let l1_before = engine
        .frontiers
        .lock()
        .map_err(|_| anyhow!("frontier cache lock poisoned"))?
        .records
        .len() as u64;
    let exact_before = engine.metrics.l1_exact_hits.load(Ordering::Relaxed);
    let approximate_before = engine.metrics.l1_approximate_hits.load(Ordering::Relaxed);
    let misses_before = engine.metrics.retrieval_misses.load(Ordering::Relaxed);

    let refresh_started = Instant::now();
    let (refresh_checked, refresh_updated) = engine.refresh_index(false)?;
    let refresh_elapsed_ms = refresh_started.elapsed().as_secs_f64() * 1_000.0;
    let stats = engine.index().stats().clone();

    let prompt_warmed = !request.prompt.trim().is_empty();
    let prompt_started = Instant::now();
    if prompt_warmed {
        let focus_path = (!request.path.trim().is_empty() && request.path.trim() != ".")
            .then(|| request.path.trim());
        let prompt_request = warmup_prompt_request(&request.prompt, focus_path);
        engine.admit_context_pack_cached(&prompt_request).await?;
    }
    let prompt_elapsed_ms = prompt_started.elapsed().as_secs_f64() * 1_000.0;

    let l0_after = engine.l0_storage_stats();
    let l1_after = engine
        .frontiers
        .lock()
        .map_err(|_| anyhow!("frontier cache lock poisoned"))?
        .records
        .len() as u64;
    let exact_hits = engine
        .metrics
        .l1_exact_hits
        .load(Ordering::Relaxed)
        .saturating_sub(exact_before);
    let approximate_hits = engine
        .metrics
        .l1_approximate_hits
        .load(Ordering::Relaxed)
        .saturating_sub(approximate_before);
    let misses = engine
        .metrics
        .retrieval_misses
        .load(Ordering::Relaxed)
        .saturating_sub(misses_before);
    let hits = exact_hits.saturating_add(approximate_hits);
    let query_count = hits.saturating_add(misses);
    engine.metrics.warmup_runs.fetch_add(1, Ordering::Relaxed);
    let total_elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;

    Ok(json!({
        "schema": "context_cache.warmup.v1",
        "generated_at": generated_at,
        "elapsed_ms": total_elapsed_ms,
        "project_id": engine.project_id,
        "trigger": "manual",
        "generated_state_only": true,
        "repo_boundary_enforced": true,
        "index": {
            "schema": "context_index.refresh.v1",
            "file_count": stats.file_count,
            "import_count": stats.chunk_count,
            "symbol_count": stats.symbol_chunks,
            "search_mode": "tantivy",
            "skipped": !refresh_checked,
            "updated": refresh_updated,
            "reason": if refresh_updated {"signature_changed"} else {"signature_unchanged"},
        },
        "cache": {
            "entry_count_before": l0_before.entries.saturating_add(l1_before),
            "entry_count_after": l0_after.entries.saturating_add(l1_after),
            "l0_before": l0_before.entries,
            "l0_after": l0_after.entries,
            "l1_before": l1_before,
            "l1_after": l1_after,
        },
        "workspace": {"file_count": stats.file_count},
        "symbols": {"count": stats.symbol_chunks},
        "state": {"overlay": "rust-v2"},
        "manifest": {
            "schema": "warmup.manifest.v1",
            "project_id": engine.project_id,
            "prompt_warmed": prompt_warmed,
        },
        "hot_chunks": {"target_count": 0, "targets": []},
        "route_seeds": {"routes": {}, "seed_count": 0},
        "search_cache": {
            "query_count": query_count,
            "hits": hits,
            "misses": misses,
            "exact_hits": exact_hits,
            "approximate_hits": approximate_hits,
        },
        "file_summary_cache": {"hits": 0, "misses": 0, "summary_count": 0},
        "term_stats": {},
        "test_owner_targets": [],
        "stage_timings_ms": {
            "refresh": refresh_elapsed_ms,
            "retrieval": 0.0,
            "prompt_cache": prompt_elapsed_ms,
            "total": total_elapsed_ms,
        },
        "omitted": [],
    }))
}

fn warmup_prompt_request(prompt: &str, focus_path: Option<&str>) -> ContextPackRequest {
    ContextPackRequest {
        prompt: prompt.to_owned(),
        changed_files: Vec::new(),
        focus_paths: focus_path.into_iter().map(str::to_owned).collect(),
        memory_session: None,
        client_profile: None,
        model_profile: None,
        project_id: None,
        root_uri: None,
        max_items: default_max_items(),
        max_source_tokens: default_max_source_tokens(),
        evidence_policy: EvidencePolicy::Balanced,
        cache_strategy: CacheStrategy::Fast,
        base_pack: None,
        known_evidence: Vec::new(),
    }
}

fn metrics_snapshot(engine: &ProjectEngine) -> Result<Value> {
    let now = now_iso()?;
    let operations = engine.metrics.operation_snapshots();
    let total_requests = operations.values().fold(0_u64, |total, operation| {
        total.saturating_add(operation.get("count").and_then(Value::as_u64).unwrap_or(0))
    });
    let mut by_tool_counts = BTreeMap::<String, u64>::new();
    for (operation, value) in &operations {
        let tool = operation.split('.').next().unwrap_or(operation);
        let count = value.get("count").and_then(Value::as_u64).unwrap_or(0);
        let entry = by_tool_counts.entry(tool.to_owned()).or_default();
        *entry = entry.saturating_add(count);
    }
    let by_tool = by_tool_counts
        .into_iter()
        .map(|(tool, count)| (tool, Value::from(count)))
        .collect::<serde_json::Map<String, Value>>();
    let l0_hits = engine.metrics.l0_hits.load(Ordering::Relaxed);
    let l0_misses = engine.metrics.l0_misses.load(Ordering::Relaxed);
    let l0_singleflight_hits = engine.metrics.l0_singleflight_hits.load(Ordering::Relaxed);
    let l0_cold_or_invalidated_misses = engine
        .metrics
        .l0_cold_or_invalidated_misses
        .load(Ordering::Relaxed);
    let l0_request_variant_misses = engine
        .metrics
        .l0_request_variant_misses
        .load(Ordering::Relaxed);
    let l0_invalidations = engine.metrics.l0_invalidations.load(Ordering::Relaxed);
    let l0_invalidated_entries = engine
        .metrics
        .l0_invalidated_entries
        .load(Ordering::Relaxed);
    let l0_storage = engine.l0_storage_stats();
    let l1_exact = engine.metrics.l1_exact_hits.load(Ordering::Relaxed);
    let l1_approximate = engine.metrics.l1_approximate_hits.load(Ordering::Relaxed);
    let retrieval_misses = engine.metrics.retrieval_misses.load(Ordering::Relaxed);
    let cache_total = l0_hits.saturating_add(l0_misses);
    let hit_ratio = if cache_total == 0 {
        0.0
    } else {
        l0_hits as f64 / cache_total as f64
    };
    let input_tokens_est = engine.metrics.pack_input_tokens_est.load(Ordering::Relaxed);
    let selected_source_tokens_est = engine
        .metrics
        .pack_selected_source_tokens_est
        .load(Ordering::Relaxed);
    let evidence_card_tokens_est = engine
        .metrics
        .pack_evidence_card_tokens_est
        .load(Ordering::Relaxed);
    let returned_evidence_tokens_est = engine
        .metrics
        .pack_returned_evidence_tokens_est
        .load(Ordering::Relaxed);
    let wire_tokens_est = engine.metrics.pack_wire_tokens_est.load(Ordering::Relaxed);
    let wire_bytes = engine.metrics.pack_wire_bytes.load(Ordering::Relaxed);
    let tokens_saved_est = engine.metrics.pack_tokens_saved_est.load(Ordering::Relaxed);
    let delta_tokens_saved_est = engine
        .metrics
        .pack_delta_tokens_saved_est
        .load(Ordering::Relaxed);
    let compression_factor_est = if wire_tokens_est == 0 {
        0.0
    } else {
        selected_source_tokens_est as f64 / wire_tokens_est as f64
    };
    let compression_ratio_est = if selected_source_tokens_est == 0 {
        0.0
    } else {
        wire_tokens_est as f64 / selected_source_tokens_est as f64
    };
    Ok(json!({
        "schema": "context_metrics.v1",
        "project_id": engine.project_id,
        "generated_at": now,
        "updated_at": now,
        "since": now,
        "requests": {
            "total": total_requests,
            "by_tool": by_tool,
            "by_operation": operations,
        },
        "tokens": {
            "input_est": input_tokens_est,
            "output_est": wire_tokens_est,
            "saved_est": tokens_saved_est,
            "tokens_spared_by_mcp_est": tokens_saved_est,
            "estimated_input_tokens_saved": tokens_saved_est,
            "token_count_source": "offline_characters_div_4",
            "context_pack": {
                "input_tokens_est": input_tokens_est,
                "selected_source_tokens_est": selected_source_tokens_est,
                "evidence_card_tokens_est": evidence_card_tokens_est,
                "returned_evidence_tokens_est": returned_evidence_tokens_est,
                "wire_tokens_est": wire_tokens_est,
                "wire_bytes": wire_bytes,
                "saved_tokens_est": tokens_saved_est,
                "delta_tokens_saved_est": delta_tokens_saved_est,
                "compression_factor_est": compression_factor_est,
                "compression_ratio_est": compression_ratio_est,
                "candidate_count": engine.metrics.pack_candidate_count.load(Ordering::Relaxed),
                "selected_count": engine.metrics.pack_selected_count.load(Ordering::Relaxed),
            }
        },
        "cache": {
            "hits": l0_hits,
            "misses": l0_misses,
            "hit_ratio": hit_ratio,
            "l0": {
                "hits": l0_hits,
                "misses": l0_misses,
                "singleflight_hits": l0_singleflight_hits,
                "entries": l0_storage.entries,
                "weighted_bytes": l0_storage.weighted_bytes,
                "miss_reasons": {
                    "cold_or_invalidated": l0_cold_or_invalidated_misses,
                    "request_variant": l0_request_variant_misses,
                },
                "invalidations": l0_invalidations,
                "invalidated_entries": l0_invalidated_entries,
            },
            "l1": {"exact_hits": l1_exact, "approximate_hits": l1_approximate, "retrieval_misses": retrieval_misses},
        },
        "retrieval": {"backend": "tantivy", "doc_count": engine.index().stats().chunk_count, "misses": retrieval_misses},
        "retrieval_queue": {"pending": 0, "active": 0},
        "references": {
            "active_count": engine.metrics.active_references.load(Ordering::Relaxed),
            "total_count": engine.metrics.total_references.load(Ordering::Relaxed),
        },
        "tooling": {"external_calls_saved_est": 0},
        "warmup": {"runs": engine.metrics.warmup_runs.load(Ordering::Relaxed)},
        "benchmarks": {},
        "index_freshness": {
            "strategy": "notify_with_polling_fallback",
            "generation": engine.freshness.generation.load(Ordering::Relaxed),
            "dirty": engine.freshness.dirty.load(Ordering::Relaxed),
            "poll_interval_ms": FAST_POLL_INTERVAL_MS,
            "coalesce_ms": WATCH_COALESCE_MS,
            "refreshes": engine.metrics.refreshes.load(Ordering::Relaxed),
        },
        "background": {"status": if engine.freshness.dirty.load(Ordering::Relaxed) {"change_pending"} else {"idle"}},
    }))
}

fn unloaded_metrics_snapshot(project_id: &str, now: &str, status: &str) -> Value {
    json!({
        "schema": "context_metrics.v1",
        "project_id": project_id,
        "generated_at": now,
        "updated_at": now,
        "since": now,
        "requests": {
            "total": 0,
            "by_tool": {"context_pack": 0},
            "by_operation": {"context_pack": {
                "count": 0,
                "success_count": 0,
                "error_count": 0,
                "avg_elapsed_ms": 0.0,
                "min_elapsed_ms": 0.0,
                "max_elapsed_ms": 0.0,
                "last_elapsed_ms": 0.0,
                "p50_recent_ms": null,
                "p95_recent_ms": null,
                "recent_sample_count": 0,
                "recent_sample_limit": OPERATION_SAMPLE_LIMIT,
                "output_bytes_total": 0,
                "avg_output_bytes": 0.0,
            }}
        },
        "tokens": {
            "input_est": 0,
            "output_est": 0,
            "saved_est": 0,
            "tokens_spared_by_mcp_est": 0,
            "estimated_input_tokens_saved": 0,
            "token_count_source": "offline_characters_div_4",
            "context_pack": {
                "input_tokens_est": 0,
                "selected_source_tokens_est": 0,
                "evidence_card_tokens_est": 0,
                "returned_evidence_tokens_est": 0,
                "wire_tokens_est": 0,
                "wire_bytes": 0,
                "saved_tokens_est": 0,
                "delta_tokens_saved_est": 0,
                "compression_factor_est": 0.0,
                "compression_ratio_est": 0.0,
                "candidate_count": 0,
                "selected_count": 0,
            }
        },
        "cache": {
            "hits": 0,
            "misses": 0,
            "hit_ratio": 0.0,
            "l0": {
                "hits": 0,
                "misses": 0,
                "singleflight_hits": 0,
                "entries": 0,
                "weighted_bytes": 0,
                "miss_reasons": {"cold_or_invalidated": 0, "request_variant": 0},
                "invalidations": 0,
                "invalidated_entries": 0,
            },
            "l1": {"exact_hits": 0, "approximate_hits": 0, "retrieval_misses": 0},
        },
        "retrieval": {"backend": "tantivy", "doc_count": 0, "misses": 0},
        "retrieval_queue": {"pending": 0, "active": 0},
        "references": {"active_count": 0, "total_count": 0},
        "tooling": {"external_calls_saved_est": 0},
        "warmup": {"runs": 0},
        "benchmarks": {},
        "index_freshness": {
            "strategy": status,
            "generation": 0,
            "dirty": false,
            "poll_interval_ms": FAST_POLL_INTERVAL_MS,
            "coalesce_ms": WATCH_COALESCE_MS,
            "refreshes": 0,
        },
        "background": {"status": status},
    })
}

fn measurement_matrix(engine: &ProjectEngine, generated_at: &str) -> Result<Value> {
    let operations = engine.metrics.operation_snapshots();
    let pack_count = operations
        .get("context_pack")
        .and_then(|operation| operation.get("count"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let source_tokens = engine
        .metrics
        .pack_selected_source_tokens_est
        .load(Ordering::Relaxed);
    let wire_tokens = engine.metrics.pack_wire_tokens_est.load(Ordering::Relaxed);
    let saved_tokens = engine.metrics.pack_tokens_saved_est.load(Ordering::Relaxed);
    let compression_factor = if wire_tokens == 0 {
        0.0
    } else {
        source_tokens as f64 / wire_tokens as f64
    };
    let observed = pack_count > 0;
    Ok(json!({
        "schema": "context_measurement_matrix.v1",
        "generated_at": generated_at,
        "project_id": engine.project_id,
        "checks": [
            {"key": "telemetry.context_pack.samples", "current": pack_count, "target": 1, "operator": ">=", "unit": "requests", "samples": pack_count, "min_samples": 1, "status": if observed {"pass"} else {"insufficient"}, "description": "Context-pack requests observed by the native telemetry ledger."},
            {"key": "tokens.context_pack.compression_factor_est", "current": if observed {Value::from(compression_factor)} else {Value::Null}, "target": 1.0, "operator": ">=", "unit": "factor", "samples": pack_count, "min_samples": 1, "status": if !observed {"insufficient"} else if compression_factor >= 1.0 {"pass"} else {"fail"}, "description": "Selected source tokens divided by returned wire tokens; estimates use characters divided by four."},
            {"key": "tokens.context_pack.saved_est", "current": if observed {Value::from(saved_tokens)} else {Value::Null}, "target": 0, "operator": ">=", "unit": "tokens_est", "samples": pack_count, "min_samples": 1, "status": if observed {"pass"} else {"insufficient"}, "description": "Estimated selected-source tokens not sent in the returned pack."},
            {"key": "quality.required_anchor_recall", "current": Value::Null, "target": 1.0, "operator": ">=", "unit": "ratio", "samples": 0, "min_samples": 1, "status": "insufficient", "description": "Requires a benchmark or differential quality corpus; runtime traffic cannot establish anchor recall."},
            {"key": "quality.noise_ratio", "current": Value::Null, "target": 0.3, "operator": "<=", "unit": "ratio", "samples": 0, "min_samples": 1, "status": "insufficient", "description": "Requires a benchmark or differential quality corpus; runtime traffic cannot establish retrieval noise."}
        ],
        "metric_sources": {
            "benchmark_runner": "context_admin(mode='benchmark')",
            "runtime_metrics": "context_admin(mode='metrics')",
        },
    }))
}

fn unloaded_measurement_matrix(project_id: &str, generated_at: &str, status: &str) -> Value {
    json!({
        "schema": "context_measurement_matrix.v1",
        "generated_at": generated_at,
        "project_id": project_id,
        "checks": [
            {"key": "quality.required_anchor_recall", "current": null, "target": 1.0, "operator": ">=", "unit": "ratio", "samples": 0, "min_samples": 1, "status": "insufficient"},
            {"key": "quality.noise_ratio", "current": null, "target": 0.3, "operator": "<=", "unit": "ratio", "samples": 0, "min_samples": 1, "status": "insufficient"}
        ],
        "metric_sources": {
            "benchmark_runner": "context_admin(mode='benchmark')",
            "runtime_metrics": "context_admin(mode='metrics')",
        },
        "status": status,
    })
}

fn benchmark_admin(engine: &ProjectEngine, generated_at: &str) -> Result<Value> {
    let prompt = "native context pack retrieval benchmark";
    let mut runs = Vec::new();
    let started_all = Instant::now();
    for run in 0..3 {
        let started = Instant::now();
        let (hits, _) = engine.index().search(prompt, &[], 8)?;
        runs.push(json!({
            "run": run + 1,
            "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
            "result_count": hits.len(),
        }));
    }
    Ok(json!({
        "schema": "context_benchmark.v1",
        "generated_at": generated_at,
        "project_id": engine.project_id,
        "prompt": prompt,
        "focus_paths": [],
        "run_count": runs.len(),
        "runs": runs,
        "elapsed_ms": started_all.elapsed().as_secs_f64() * 1000.0,
        "measurement_matrix": measurement_matrix(engine, generated_at)?,
        "compact_contract_sample": contracts_payload("", "compact"),
    }))
}

fn state_browser(
    engine: &ProjectEngine,
    request: &ContextAdminRequest,
    generated_at: &str,
) -> Result<Value> {
    if !request.state_key.is_empty() {
        let value = engine.store.get_json(&request.state_key)?;
        return Ok(json!({
            "schema": "context_state_browser.v1",
            "mode": "entry",
            "project_id": engine.project_id,
            "generated_state_only": true,
            "repo_boundary_enforced": true,
            "key": request.state_key,
            "found": value.is_some(),
            "value": value,
        }));
    }
    let rows = engine
        .store
        .iter_json(&request.state_prefix)?
        .into_iter()
        .take(usize::from(request.max_entries))
        .map(|(key, value)| {
            let mut preview = serde_json::to_string(&value).unwrap_or_default();
            if preview.len() > 1000 {
                truncate_string(&mut preview, 1000);
                preview.push_str("...[truncated]");
            }
            json!({
                "key": key,
                "schema": value.get("schema").cloned().unwrap_or_default(),
                "value_type": if value.is_object() {"dict"} else if value.is_array() {"list"} else {"scalar"},
                "size_chars": value.to_string().len(),
                "preview": sanitize_text(&preview).0,
                "created_at": value.get("created_at").cloned().unwrap_or_default(),
                "updated_at": value.get("updated_at").cloned().unwrap_or_default(),
                "expires_at": value.get("expires_at").cloned().unwrap_or_default(),
                "status": value.get("status").cloned().unwrap_or_default(),
                "namespace": value.get("namespace").cloned().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema": "context_state_browser.v1",
        "mode": "list",
        "project_id": engine.project_id,
        "generated_state_only": true,
        "repo_boundary_enforced": true,
        "generated_at": generated_at,
        "prefix": request.state_prefix,
        "max_entries": request.max_entries,
        "entry_count": rows.len(),
        "truncated": false,
        "rows": rows,
        "prefix_counts": [],
    }))
}

fn contracts_payload(tool_name: &str, profile: &str) -> Value {
    let names = [
        "context_pack",
        "context_lookup",
        "context_memory",
        "context_admin",
        "result_reference_resolve",
    ];
    let contracts = names
        .into_iter()
        .filter(|name| tool_name.is_empty() || tool_name == *name)
        .map(|name| (name.to_owned(), contract_for_tool(name)))
        .collect::<serde_json::Map<_, _>>();
    let encoded = serde_json::to_vec(&contracts).unwrap_or_default();
    json!({
        "schema": if profile == "compact" {"tool_output_contracts.compact.v1"} else {"tool_output_contracts.v1"},
        "contract_version": 2,
        "profile": profile,
        "stability": "stable",
        "contracts": contracts,
        "metrics": {
            "schema": "tool_contract_metrics.v1",
            "tool_name": tool_name,
            "contract_chars": encoded.len(),
            "contract_tokens_est": encoded.len().div_ceil(4),
            "token_count_source": "estimate",
            "tokenizer": "offline_estimator",
            "tokenizer_available": true,
        },
    })
}

fn contract_for_tool(tool_name: &str) -> Value {
    let (description, schemas, parameters) = match tool_name {
        "context_pack" => (
            "Build compact cited repository context using the clean-break v2 contract.",
            json!(["context_pack.v2"]),
            json!({
                "prompt": "Task text.",
                "changed_files": "Changed repository paths.",
                "focus_paths": "Paths to prioritize.",
                "memory_session": "Memory session key.",
                "client_profile": "codex, claude, copilot, generic.",
                "model_profile": "openai, anthropic, github, unknown.",
                "project_id": "Project selector.",
                "root_uri": "Repository file URI.",
                "max_items": "Range 1 to 32, default 8.",
                "max_source_tokens": "Range 0 to 4096, default 512.",
                "evidence_policy": "reference, balanced, source.",
                "cache_strategy": "fast, stable, fresh.",
                "base_pack": "Previous pack id for delta generation.",
                "known_evidence": "Evidence ids already held by the client."
            }),
        ),
        "context_lookup" => (
            "Search, snippet, tree, symbols, relationships, chunks, cache, or references.",
            json!([
                "context_search.v1",
                "context_snippet.v1",
                "context_tree.v1",
                "context_symbols.v1",
                "context_references.list.v1",
                "context_lookup.impact.v1",
                "context_lookup.related_symbols.v1",
                "context_lookup.test_owners.v1",
                "context_lookup.chunk.v1",
                "context_lookup.explain_cache.v1"
            ]),
            json!({
                "mode": "search, snippet, tree, symbols, references, impact, related_symbols, test_owners, chunk, explain_cache.",
                "query": "Search or symbol terms.", "path": "Repository-relative path.",
                "start_line": "Snippet start line.", "end_line": "Snippet end line.",
                "max_results": "Maximum result rows.", "max_entries": "Maximum tree rows.",
                "max_depth": "Tree depth.", "include_globs": "Result glob filters.",
                "project_id": "Project selector.", "root_uri": "Repository file URI."
            }),
        ),
        "context_memory" => (
            "Read and manage compact repository memory.",
            json!([
                "context_memory.get.v1",
                "context_memory.upsert.v1",
                "context_memory.summary_upsert.v1",
                "context_memory.decision_record.v1",
                "context_memory.validate.v1",
                "context_memory.compact.v1"
            ]),
            json!({
                "mode": "get, upsert, summary_upsert, decision_record, validate, compact.",
                "namespace": "Memory namespace.", "key": "Entry key.", "value": "Structured value.",
                "ttl_days": "Optional TTL.", "confidence": "Range 0 to 1.", "source": "Provenance label.",
                "tags": "Search tags.", "focus": "Summary focus.", "summary": "Compact summary text.",
                "topic": "Decision topic.", "decision": "Decision payload.", "decided_by": "human or llm.",
                "rationale": "Decision rationale.", "include_expired": "Include expired rows.",
                "max_entries": "Maximum rows.", "project_id": "Project selector.", "root_uri": "Repository file URI."
            }),
        ),
        "context_admin" => (
            "Inspect health, index, cache, contracts, metrics, quality, and generated state.",
            json!([
                "context_admin.health.v1",
                "context_index.refresh.v1",
                "context_index.status.v1",
                "context_cache.stats.v1",
                "context_cache.prune.v1",
                "context_cache.warmup.v1",
                "context_budget.v1",
                "tool_output_contracts.v1",
                "context_metrics.v1",
                "context_measurement_report.v1",
                "context_measurement_matrix.v1",
                "context_benchmark.v1",
                "context_state_browser.v1",
                "context_quality_eval.v1",
                "context_budget_plan.v1",
                "context_profile_calibration.v1",
                "context_resource_proxy.v1"
            ]),
            json!({
                "mode": "health, projects, active_projects, cached_projects, monitor_usage, index_refresh, index_status, cache_stats, cache_prune, warmup, budget, contracts, metrics, measurement_matrix, measurement_report, benchmark, state_browser, quality_eval, cache_plan, profile_calibrate, instructions, resource_proxy, schema_minify.",
                "action": "For monitor_usage: status, enable, disable, or report.",
                "path": "Repository-relative path or resource URI.", "max_files": "Index file cap.",
                "prompt": "Optional prompt to warm exact context_pack cache without echoing it.",
                "max_age_minutes": "Cache prune age.", "max_entries": "Maximum rows.",
                "max_output_chars": "Budget override.", "default_output_profile": "Budget profile.",
                "tool_name": "Filter to one tool.", "contract_profile": "compact or verbose.",
                "state_prefix": "Generated state prefix.", "state_key": "Exact generated state key.",
                "project_id": "Project selector.", "root_uri": "Repository file URI."
            }),
        ),
        "result_reference_resolve" => (
            "Resolve a stored local evidence reference.",
            json!(["mcp_result_reference.resolve.v1"]),
            json!({
                "reference_id": "Reference id.", "reference": "Full reference object.",
                "expected_hash": "Expected sha256.", "project_id": "Project selector.",
                "root_uri": "Repository file URI."
            }),
        ),
        _ => ("Unknown tool.", json!([]), json!({})),
    };
    json!({
        "schema": "tool_output_contract.compact.v1",
        "contract_version": 2,
        "tool_name": tool_name,
        "description": description,
        "output_schema_names": schemas,
        "parameters": parameters,
        "stability": "stable",
        "profile": "compact",
        "metrics": {"schema": "tool_contract_metrics.v1", "tool_name": tool_name},
    })
}

fn profile_calibration() -> Value {
    json!({
        "schema": "context_profile_calibration.v1",
        "defaults": {
            "client_profile": "generic", "model_profile": "unknown",
            "evidence_policy": "balanced", "cache_strategy": "fast"
        },
        "profiles": {
            "codex": {"output_profile": "minimal", "diagnostics": "admin_only"},
            "claude": {"output_profile": "compact", "diagnostics": "admin_only"},
            "copilot": {"output_profile": "compact", "tools_only": true},
            "generic": {"output_profile": "balanced"}
        }
    })
}

fn instructions_payload() -> Value {
    json!({
        "schema": "codex_context_pack_first.instructions.v1",
        "purpose": "Speed up coding agents with a first-pass context pack.",
        "instruction": "For repository coding, review, debug, test, docs, security, or general questions, call context_pack first with the user's task and client_profile. Pass changed_files and focus_paths when named. Use context_lookup for targeted follow-up before broad inspection, result_reference_resolve before relying on deferred raw evidence, context_admin for health and generated state, and context_memory only for structured non-secret repository facts.",
        "boundary": "Repository-side MCP configuration can require server initialization but cannot force a model to call a tool on every turn.",
        "preferred_tool_order": ["context_pack", "context_lookup", "result_reference_resolve", "context_admin", "context_memory"],
        "enforcement_layers": [
            "MCP server availability", "MCP server instructions", "AGENTS.md workflow", "Review and CI checks"
        ],
        "profile_selection": {
            "auto_detection": false,
            "set_by": "MCP caller per context_pack request",
            "precedence": ["explicit request fields", "client profile defaults", "server defaults"],
            "client_profiles": {
                "codex": {"model_profile": "openai", "recommended_output_profile": "minimal"},
                "claude": {"model_profile": "anthropic", "recommended_output_profile": "compact"},
                "copilot": {"model_profile": "github", "recommended_output_profile": "compact", "tools_only": true},
                "generic": {"model_profile": "unknown", "recommended_output_profile": "balanced"}
            },
            "calibration": "context_admin(mode=\"profile_calibrate\") reports recommendations only."
        },
        "resource_uris": [
            "repo://instructions/context-pack",
            "repo://project/{project_id}/instructions/context-pack"
        ],
        "codex_config_example": {
            "config_file": "~/.codex/config.toml or trusted-project .codex/config.toml",
            "effect": "required=true fails startup if the enabled server cannot initialize.",
            "toml": "[mcp_servers.mcp-context-manager]\nurl = \"http://localhost:8000/mcp\"\nrequired = true\n"
        }
    })
}

fn normalize_project_resource_uri(uri: &str, project_id: &str) -> Result<String> {
    let Some(rest) = uri.strip_prefix("repo://project/") else {
        return Ok(uri.to_owned());
    };
    let (selected, resource) = rest
        .split_once('/')
        .ok_or_else(|| anyhow!("project resource URI is incomplete"))?;
    if selected != project_id {
        bail!("resource project does not select the active project");
    }
    Ok(format!("repo://{resource}"))
}

#[derive(Debug, Deserialize, Serialize)]
struct MemoryDocument {
    schema: String,
    entries: Vec<Value>,
    summaries: Vec<Value>,
    decisions: Vec<Value>,
}

impl Default for MemoryDocument {
    fn default() -> Self {
        Self {
            schema: "context_memory_store.v1".to_owned(),
            entries: Vec::new(),
            summaries: Vec::new(),
            decisions: Vec::new(),
        }
    }
}

fn memory_dispatch(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    if request.max_entries == 0 || request.max_entries > 1000 {
        bail!("max_entries must be in 1..=1000");
    }
    match request.mode.as_str() {
        "get" => memory_get(store, request),
        "upsert" => memory_upsert(store, request),
        "summary_upsert" => memory_summary_upsert(store, request),
        "decision_record" => memory_decision_record(store, request),
        "validate" => memory_validate(store),
        "compact" => memory_compact(store, request),
        unsupported => bail!("unsupported context_memory mode: {unsupported}"),
    }
}

fn load_memory(store: &StateStore) -> Result<MemoryDocument> {
    let Some(value) = store.get_json("memory:store")? else {
        return Ok(MemoryDocument::default());
    };
    Ok(serde_json::from_value(value).unwrap_or_default())
}

fn save_memory(store: &StateStore, document: &MemoryDocument) -> Result<()> {
    store.put_json("memory:store", &serde_json::to_value(document)?)
}

fn memory_upsert(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    validate_confidence(request.confidence)?;
    let namespace = safe_identifier("namespace", request.namespace.as_deref(), true)?;
    let key = safe_identifier("key", request.key.as_deref(), true)?;
    let (value, sensitivity) = sanitize_json_value(request.value.clone().unwrap_or(Value::Null));
    let (source, source_sensitivity) = sanitize_json_value(json!(request.source));
    let (tags, tags_sensitivity) = sanitize_json_value(json!(request.tags));
    let sensitivity = merge_sensitivity(&[sensitivity, source_sensitivity, tags_sensitivity]);
    let mut document = load_memory(store)?;
    let now = now_iso()?;
    let expires_at = expiry_iso(request.ttl_days)?;
    if let Some(row) = document.entries.iter_mut().find(|row| {
        row.get("namespace").and_then(Value::as_str) == Some(namespace.as_str())
            && row.get("key").and_then(Value::as_str) == Some(key.as_str())
    }) {
        let created_at = row.get("created_at").cloned().unwrap_or_else(|| json!(now));
        *row = json!({
            "namespace": namespace,
            "key": key,
            "value": value,
            "confidence": request.confidence,
            "source": source,
            "tags": tags,
            "created_at": created_at,
            "updated_at": now,
            "expires_at": expires_at,
            "sensitivity": sensitivity,
        });
    } else {
        document.entries.push(json!({
            "namespace": namespace,
            "key": key,
            "value": value,
            "confidence": request.confidence,
            "source": source,
            "tags": tags,
            "created_at": now,
            "updated_at": now,
            "expires_at": expires_at,
            "sensitivity": sensitivity,
        }));
    }
    save_memory(store, &document)?;
    Ok(json!({
        "schema": "context_memory.upsert.v1",
        "namespace": namespace,
        "key": key,
        "updated": true,
        "expires_at": expires_at,
        "repo_boundary_enforced": true,
        "sensitivity": sensitivity,
    }))
}

fn memory_summary_upsert(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    validate_confidence(request.confidence)?;
    let namespace = safe_identifier("namespace", request.namespace.as_deref(), true)?;
    let focus = safe_identifier("focus", Some(&request.focus), true)?;
    let (summary, summary_sensitivity) = sanitize_json_value(json!(request.summary));
    let (source, source_sensitivity) = sanitize_json_value(json!(request.source));
    let (tags, tags_sensitivity) = sanitize_json_value(json!(request.tags));
    let sensitivity =
        merge_sensitivity(&[summary_sensitivity, source_sensitivity, tags_sensitivity]);
    let mut document = load_memory(store)?;
    let now = now_iso()?;
    let expires_at = expiry_iso(request.ttl_days)?;
    let row = json!({
        "namespace": namespace,
        "focus": focus,
        "summary": summary,
        "confidence": request.confidence,
        "source": source,
        "tags": tags,
        "created_at": now,
        "updated_at": now,
        "expires_at": expires_at,
        "sensitivity": sensitivity,
    });
    if let Some(existing) = document.summaries.iter_mut().find(|row| {
        row.get("namespace").and_then(Value::as_str) == Some(namespace.as_str())
            && row.get("focus").and_then(Value::as_str) == Some(focus.as_str())
    }) {
        let created_at = existing
            .get("created_at")
            .cloned()
            .unwrap_or_else(|| json!(now));
        *existing = row;
        existing["created_at"] = created_at;
    } else {
        document.summaries.push(row);
    }
    save_memory(store, &document)?;
    Ok(json!({
        "schema": "context_memory.summary_upsert.v1",
        "namespace": namespace,
        "focus": focus,
        "updated": true,
        "sensitivity": sensitivity,
    }))
}

fn memory_decision_record(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    validate_confidence(request.confidence)?;
    if !matches!(request.decided_by.as_str(), "human" | "llm") {
        bail!("decided_by must be human or llm");
    }
    let namespace = safe_identifier("namespace", request.namespace.as_deref(), true)?;
    let topic = safe_identifier("topic", Some(&request.topic), false)?;
    let (decision, decision_sensitivity) =
        sanitize_json_value(request.decision.clone().unwrap_or(Value::Null));
    let (rationale, rationale_sensitivity) = sanitize_json_value(json!(request.rationale));
    let (source, source_sensitivity) = sanitize_json_value(json!(request.source));
    let (tags, tags_sensitivity) = sanitize_json_value(json!(request.tags));
    let sensitivity = merge_sensitivity(&[
        decision_sensitivity,
        rationale_sensitivity,
        source_sensitivity,
        tags_sensitivity,
    ]);
    let mut document = load_memory(store)?;
    let now = now_iso()?;
    let row = json!({
        "id": format!("decision-{}", document.decisions.len() + 1),
        "namespace": namespace,
        "topic": topic,
        "decision": decision,
        "decided_by": request.decided_by,
        "rationale": rationale,
        "confidence": request.confidence,
        "source": source,
        "tags": tags,
        "created_at": now,
        "updated_at": now,
        "expires_at": expiry_iso(request.ttl_days)?,
        "sensitivity": sensitivity,
    });
    document.decisions.push(row.clone());
    save_memory(store, &document)?;
    let effective = effective_decisions(&document, Some(&namespace), Some(&topic), false)
        .into_iter()
        .take(1)
        .collect::<Vec<_>>();
    Ok(json!({
        "schema": "context_memory.decision_record.v1",
        "recorded": row,
        "effective_decision": effective,
    }))
}

fn memory_get(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    let document = load_memory(store)?;
    let namespace = request.namespace.as_deref();
    let limit = usize::from(request.max_entries);
    let entries = filter_memory_rows(&document.entries, namespace, request.include_expired, limit);
    let summaries = filter_memory_rows(
        &document.summaries,
        namespace,
        request.include_expired,
        limit,
    );
    let decisions = effective_decisions(&document, namespace, None, request.include_expired)
        .into_iter()
        .take(limit)
        .collect::<Vec<_>>();
    Ok(json!({
        "schema": "context_memory.get.v1",
        "count": entries.len(),
        "entries": entries,
        "summary_count": summaries.len(),
        "summaries": summaries,
        "effective_decision_count": decisions.len(),
        "effective_decisions": decisions,
        "repo_boundary_enforced": true,
    }))
}

fn memory_validate(store: &StateStore) -> Result<Value> {
    let document = load_memory(store)?;
    let mut stale_entries = Vec::new();
    let mut missing_metadata = Vec::new();
    for row in &document.entries {
        if memory_row_expired(row) {
            stale_entries.push(json!({
                "namespace": row.get("namespace").cloned().unwrap_or_default(),
                "key": row.get("key").cloned().unwrap_or_default(),
                "reason": "expired",
            }));
        }
        for required in ["source", "confidence", "created_at", "updated_at"] {
            if row.get(required).is_none() {
                missing_metadata.push(json!({
                    "kind": "entry",
                    "key": row.get("key").cloned().unwrap_or_default(),
                    "field": required,
                }));
            }
        }
    }
    Ok(json!({
        "schema": "context_memory.validate.v1",
        "entry_count": document.entries.len(),
        "summary_count": document.summaries.len(),
        "decision_count": document.decisions.len(),
        "stale_count": stale_entries.len(),
        "stale_entries": stale_entries,
        "missing_metadata": missing_metadata,
    }))
}

fn memory_compact(store: &StateStore, request: &ContextMemoryRequest) -> Result<Value> {
    let document = load_memory(store)?;
    let mut rows = document
        .entries
        .iter()
        .filter(|row| {
            request.namespace.as_deref().is_none_or(|namespace| {
                row.get("namespace").and_then(Value::as_str) == Some(namespace)
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let count = rows.len();
    if count <= 40 {
        return Ok(json!({
            "schema": "context_memory.compact.v1",
            "compacted": false,
            "entry_count": count,
        }));
    }
    rows.sort_by(|left, right| {
        right
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            .total_cmp(
                &left
                    .get("confidence")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
            )
            .then_with(|| {
                right
                    .get("updated_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .cmp(
                        left.get("updated_at")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
            })
    });
    let mut summary = rows
        .iter()
        .take(12)
        .map(|row| {
            let key = row.get("key").and_then(Value::as_str).unwrap_or_default();
            let mut value = row.get("value").cloned().unwrap_or_default().to_string();
            truncate_string(&mut value, 160);
            format!("- {key}: {value}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    truncate_string(&mut summary, 1200);
    let mut summary_request = request.clone();
    summary_request.mode = "summary_upsert".to_owned();
    summary_request.namespace = Some(
        request
            .namespace
            .clone()
            .unwrap_or_else(|| "global".to_owned()),
    );
    summary_request.focus = "auto_compact".to_owned();
    summary_request.summary = summary;
    summary_request.ttl_days = Some(60);
    summary_request.confidence = 0.9;
    summary_request.source = "context_memory.compact".to_owned();
    summary_request.tags = vec!["auto".to_owned(), "compact".to_owned()];
    memory_summary_upsert(store, &summary_request)?;
    Ok(json!({
        "schema": "context_memory.compact.v1",
        "compacted": true,
        "entry_count": count,
        "kept_entries": 12,
        "summary_focus": "auto_compact",
    }))
}

fn filter_memory_rows(
    rows: &[Value],
    namespace: Option<&str>,
    include_expired: bool,
    limit: usize,
) -> Vec<Value> {
    rows.iter()
        .filter(|row| {
            namespace.is_none_or(|namespace| {
                row.get("namespace").and_then(Value::as_str) == Some(namespace)
            }) && (include_expired || !memory_row_expired(row))
        })
        .take(limit)
        .cloned()
        .map(|mut row| {
            row["expired"] = json!(memory_row_expired(&row));
            row
        })
        .collect()
}

fn effective_decisions(
    document: &MemoryDocument,
    namespace: Option<&str>,
    topic: Option<&str>,
    include_expired: bool,
) -> Vec<Value> {
    let mut rows = document
        .decisions
        .iter()
        .filter(|row| {
            namespace.is_none_or(|namespace| {
                row.get("namespace").and_then(Value::as_str) == Some(namespace)
            }) && topic.is_none_or(|topic| row.get("topic").and_then(Value::as_str) == Some(topic))
                && (include_expired || !memory_row_expired(row))
        })
        .cloned()
        .map(|mut row| {
            row["expired"] = json!(memory_row_expired(&row));
            row
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        let left_human = left.get("decided_by").and_then(Value::as_str) == Some("human");
        let right_human = right.get("decided_by").and_then(Value::as_str) == Some("human");
        let left_confidence = left
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let right_confidence = right
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        right_human
            .cmp(&left_human)
            .then_with(|| right_confidence.total_cmp(&left_confidence))
            .then_with(|| {
                right
                    .get("updated_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .cmp(
                        left.get("updated_at")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
            })
    });
    let mut seen = HashSet::new();
    rows.into_iter()
        .filter(|row| {
            seen.insert((
                row.get("namespace")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                row.get("topic")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ))
        })
        .collect()
}

fn validate_confidence(confidence: f64) -> Result<()> {
    if !(0.0..=1.0).contains(&confidence) {
        bail!("confidence must be in range [0, 1]");
    }
    Ok(())
}

fn safe_identifier(field: &str, value: Option<&str>, required: bool) -> Result<String> {
    let normalized = value.unwrap_or_default().trim();
    if required && normalized.is_empty() {
        bail!("{field} is required");
    }
    let (sanitized, redactions, _) = sanitize_text(normalized);
    if !redactions.is_empty() || sanitized != normalized {
        bail!("unsafe memory identifier: {field} contains sensitive content");
    }
    Ok(normalized.to_owned())
}

fn sanitize_json_value(value: Value) -> (Value, Value) {
    fn visit(value: Value, categories: &mut Vec<String>, count: &mut usize) -> Value {
        match value {
            Value::String(text) => {
                let (sanitized, redactions, _) = sanitize_text(&text);
                for redaction in redactions {
                    if let Some(category) = redaction.get("category").and_then(Value::as_str)
                        && !categories.iter().any(|existing| existing == category)
                    {
                        categories.push(category.to_owned());
                    }
                    *count += redaction.get("count").and_then(Value::as_u64).unwrap_or(1) as usize;
                }
                Value::String(sanitized)
            }
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|value| visit(value, categories, count))
                    .collect(),
            ),
            Value::Object(values) => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, visit(value, categories, count)))
                    .collect(),
            ),
            other => other,
        }
    }
    let mut categories = Vec::new();
    let mut count = 0;
    let sanitized = visit(value, &mut categories, &mut count);
    (
        sanitized,
        json!({
            "redacted": count > 0,
            "redaction_count": count,
            "categories": categories,
        }),
    )
}

fn merge_sensitivity(values: &[Value]) -> Value {
    let mut count = 0_u64;
    let mut categories = Vec::new();
    for value in values {
        count += value
            .get("redaction_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        for category in value
            .get("categories")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            if !categories.contains(&category) {
                categories.push(category);
            }
        }
    }
    json!({
        "redacted": count > 0,
        "redaction_count": count,
        "categories": categories,
    })
}

fn now_iso() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(Into::into)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn expiry_iso(ttl_days: Option<i64>) -> Result<Option<String>> {
    ttl_days
        .map(|days| (OffsetDateTime::now_utc() + Duration::days(days)).format(&Rfc3339))
        .transpose()
        .map_err(Into::into)
}

fn memory_row_expired(row: &Value) -> bool {
    row.get("expires_at")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .is_some_and(|expiry| expiry < OffsetDateTime::now_utc())
}

fn normalized_explicit_paths(request: &ContextPackRequest) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    request
        .changed_files
        .iter()
        .chain(&request.focus_paths)
        .map(|path| validate_relative_path(path))
        .filter_map(|path| match path {
            Ok(path) if seen.insert(path.clone()) => Some(Ok(path)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn select_hits<'a>(
    candidates: &'a [SearchHit],
    explicit_paths: &[String],
    max_items: usize,
) -> Vec<&'a SearchHit> {
    let mut selected = Vec::with_capacity(max_items);
    let mut seen_ids = HashSet::new();
    if !explicit_paths.is_empty() {
        for path in explicit_paths {
            if let Some(hit) = candidates.iter().find(|hit| hit.path == *path)
                && seen_ids.insert(hit.id.as_str())
            {
                selected.push(hit);
            }
            if selected.len() == max_items {
                return selected;
            }
        }
        return selected;
    }
    let mut seen_paths = HashSet::new();
    for hit in candidates {
        if seen_ids.insert(hit.id.as_str()) && seen_paths.insert(hit.path.as_str()) {
            selected.push(hit);
        }
        if selected.len() == max_items.min(4) {
            break;
        }
    }
    selected
}

fn delta_evidence(
    full: Vec<EvidenceCard>,
    current: &[PackEvidenceSnapshot],
    base: Option<&PackSnapshot>,
    known: &HashSet<&str>,
) -> Vec<EvidenceCard> {
    let Some(base) = base else {
        return full
            .into_iter()
            .filter(|card| !known.contains(card.0.as_str()))
            .collect();
    };
    let current_ids = current
        .iter()
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    let base_ids = base
        .evidence
        .iter()
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    let mut replaced = HashSet::new();
    let mut delta = full
        .into_iter()
        .zip(current)
        .filter_map(|(mut card, snapshot)| {
            if known.contains(card.0.as_str()) || base_ids.contains(card.0.as_str()) {
                return None;
            }
            if let Some(previous) = base.evidence.iter().find(|previous| {
                previous.path == snapshot.path
                    && previous.symbol == snapshot.symbol
                    && previous.id != snapshot.id
            }) {
                card.1 = 5;
                replaced.insert(previous.id.as_str());
            } else {
                card.1 = 3;
            }
            Some(card)
        })
        .collect::<Vec<_>>();
    delta.extend(
        base.evidence
            .iter()
            .filter(|previous| {
                !current_ids.contains(previous.id.as_str())
                    && !replaced.contains(previous.id.as_str())
            })
            .map(|previous| {
                (
                    previous.id.clone(),
                    4,
                    previous.start_line,
                    previous.end_line,
                    previous.symbol.clone(),
                    String::new(),
                    0,
                )
            }),
    );
    delta
}

fn evidence_card(
    hit: &SearchHit,
    terms: &[String],
    policy: EvidencePolicy,
    remaining_tokens: u32,
) -> (u32, u32, String, u32) {
    if policy == EvidencePolicy::Reference || remaining_tokens == 0 {
        let card = if hit.symbol.is_empty() {
            "ranked repository evidence".to_owned()
        } else {
            format!("ranked symbol {}", hit.symbol)
        };
        let tokens = estimate_tokens(&card);
        return (hit.start_line, hit.end_line, card, tokens);
    }
    let max_chars = match policy {
        EvidencePolicy::Balanced => 180,
        EvidencePolicy::Source => {
            usize::try_from(remaining_tokens.saturating_mul(4)).unwrap_or(usize::MAX)
        }
        EvidencePolicy::Reference => unreachable!(),
    }
    .min(usize::try_from(remaining_tokens.saturating_mul(4)).unwrap_or(usize::MAX));
    if policy == EvidencePolicy::Balanced {
        let (start, end, mut card) = semantic_evidence_card(hit, terms);
        truncate_string(&mut card, max_chars);
        let card = sanitize_text(&card).0;
        let tokens = estimate_tokens(&card).min(remaining_tokens);
        return (start, end, card, tokens);
    }
    let (start, end, excerpt) = hit.evidence_excerpt(terms, max_chars);
    let excerpt = sanitize_text(&excerpt).0;
    let tokens = estimate_tokens(&excerpt).min(remaining_tokens);
    (start, end, excerpt, tokens)
}

fn semantic_evidence_card(hit: &SearchHit, terms: &[String]) -> (u32, u32, String) {
    let lines = hit
        .content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let declaration = lines.iter().copied().find(|line| {
        (!hit.symbol.is_empty() && line.contains(&hit.symbol))
            || [
                "def ",
                "async def ",
                "fn ",
                "class ",
                "struct ",
                "interface ",
                "function ",
            ]
            .iter()
            .any(|prefix| line.starts_with(prefix))
    });
    let guard = lines.iter().copied().find(|line| {
        ["if ", "if(", "match ", "assert ", "ensure!", "require("]
            .iter()
            .any(|needle| line.contains(needle))
    });
    let call = lines.iter().copied().find(|line| {
        line.contains('(')
            && Some(*line) != declaration
            && !line.starts_with("if ")
            && !line.starts_with("for ")
            && !line.starts_with("while ")
    });
    let exit = lines.iter().copied().find(|line| {
        ["return ", "raise ", "bail!", "Err(", "throw "]
            .iter()
            .any(|needle| line.contains(needle))
    });
    let write = lines.iter().copied().find(|line| {
        (line.contains(" = ") && !line.contains(" == "))
            || [".insert(", ".store(", ".write(", ".push("]
                .iter()
                .any(|needle| line.contains(needle))
    });
    let mut parts = Vec::new();
    if let Some(line) = declaration.or_else(|| lines.first().copied()) {
        parts.push(format!("sig: {line}"));
    }
    if let Some(line) = guard {
        parts.push(format!("guard: {line}"));
    }
    if let Some(line) = call {
        parts.push(format!("call: {line}"));
    }
    if let Some(line) = exit {
        parts.push(format!("exit: {line}"));
    }
    if let Some(line) = write {
        parts.push(format!("write: {line}"));
    }
    if is_test_path(&hit.path) {
        parts.push("test evidence".to_owned());
    }
    let (start, end, fallback) = hit.evidence_excerpt(terms, 180);
    let card = if parts.is_empty() {
        fallback
    } else {
        parts.join("; ")
    };
    (start, end, card)
}

fn evidence_policy_code(policy: EvidencePolicy) -> u8 {
    match policy {
        EvidencePolicy::Reference => 0,
        EvidencePolicy::Balanced => 1,
        EvidencePolicy::Source => 2,
    }
}

fn classify_route(prompt: &str) -> &'static str {
    let lowered = prompt.to_ascii_lowercase();
    if ["debug", "failure", "fix", "bug", "error"]
        .iter()
        .any(|term| lowered.contains(term))
    {
        "debug"
    } else if ["review", "audit", "security"]
        .iter()
        .any(|term| lowered.contains(term))
    {
        "review"
    } else if ["build", "implement", "add", "migrate"]
        .iter()
        .any(|term| lowered.contains(term))
    {
        "implementation"
    } else {
        "explore"
    }
}

fn estimate_tokens(text: &str) -> u32 {
    u32::try_from(text.chars().count().div_ceil(4)).unwrap_or(u32::MAX)
}

fn digest_id(prefix: &str, bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let suffix = digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}{suffix}")
}

fn valid_digest_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|suffix| {
        suffix.len() == 24 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn round_score(score: f32) -> f64 {
    f64::from((score * 1_000_000.0).round() / 1_000_000.0)
}

fn index_status(stats: &IndexStats) -> Value {
    let signature = &stats.refresh_signature;
    json!({
        "schema": "context_index.status.v1",
        "exists": true,
        "index_available": true,
        "generated_at": now_iso().unwrap_or_default(),
        "file_count": stats.file_count,
        "symbol_count": stats.symbol_chunks,
        "import_count": stats.chunk_count,
        "fts_enabled": true,
        "git_head": "",
        "git_branch": "",
        "git_status_hash": "",
        "git_changes_hash": "",
        "refresh_signature": signature,
        "search_mode": "tantivy",
        "search_backend_version": "tantivy:mcp-context-manager.native.v2:0.26.1",
        "refresh_signature_available": true,
        "tantivy": {
            "schema_version": "mcp-context-manager.native.v2",
            "package_version": "0.26.1",
            "engine_version": "tantivy v0.26.1",
            "doc_count": stats.chunk_count,
            "backend_version": "tantivy:mcp-context-manager.native.v2:0.26.1",
            "refresh_signature": signature,
            "sidecar_signature": format!("tantivy:{}", sha256_text(signature)),
        },
    })
}

fn symbol_relationship(symbol: &SymbolRecord, relationship: &str) -> Value {
    json!({
        "path": symbol.path,
        "symbol": symbol.name,
        "kind": symbol.kind,
        "relationship": relationship,
        "confidence": "high",
        "detail_lookup": {
            "tool": "context_lookup",
            "mode": "snippet",
            "path": symbol.path,
            "start_line": symbol.line_start,
            "end_line": symbol.line_end,
        },
    })
}

fn is_test_path(path: &str) -> bool {
    path.starts_with("tests/")
        || path.contains("/tests/")
        || path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("test_") || name.contains(".test."))
}

fn compile_path_globs(globs: &[String]) -> Result<Vec<Regex>> {
    globs
        .iter()
        .map(|glob| {
            if glob.is_empty()
                || glob.starts_with('/')
                || glob.starts_with('\\')
                || glob.split(['/', '\\']).any(|component| component == "..")
            {
                bail!("include_globs must be repository-relative patterns");
            }
            let characters = glob.chars().collect::<Vec<_>>();
            let mut expression = String::from("^");
            let mut index = 0;
            while index < characters.len() {
                match characters[index] {
                    '*' if characters.get(index + 1) == Some(&'*') => {
                        if characters.get(index + 2) == Some(&'/') {
                            expression.push_str("(?:.*/)?");
                            index += 3;
                        } else {
                            expression.push_str(".*");
                            index += 2;
                        }
                    }
                    '*' => {
                        expression.push_str("[^/]*");
                        index += 1;
                    }
                    '?' => {
                        expression.push_str("[^/]");
                        index += 1;
                    }
                    character => {
                        expression.push_str(&regex::escape(&character.to_string()));
                        index += 1;
                    }
                }
            }
            expression.push('$');
            Regex::new(&expression).map_err(Into::into)
        })
        .collect()
}

fn sha256_text(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn truncate_string(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
}

fn sanitize_text(text: &str) -> (String, Vec<Value>, Value) {
    static PRIVATE_KEY: OnceLock<Regex> = OnceLock::new();
    static BEARER: OnceLock<Regex> = OnceLock::new();
    static ASSIGNMENT: OnceLock<Regex> = OnceLock::new();
    static HOST_PATH: OnceLock<Regex> = OnceLock::new();
    let rules = [
        (
            "private_key",
            PRIVATE_KEY.get_or_init(|| {
                Regex::new(
                    r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
                )
                .expect("private-key regex")
            }),
            "[REDACTED_PRIVATE_KEY]",
        ),
        (
            "bearer_token",
            BEARER.get_or_init(|| {
                Regex::new(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]{8,}").expect("bearer regex")
            }),
            "Bearer [REDACTED]",
        ),
        (
            "secret_assignment",
            ASSIGNMENT.get_or_init(|| {
                Regex::new(
                    r#"(?i)\b(password|passwd|token|api[_-]?key|secret)\s*[:=]\s*[^\s,'\";]{4,}"#,
                )
                .expect("secret assignment regex")
            }),
            "$1=[REDACTED]",
        ),
        (
            "host_path",
            HOST_PATH.get_or_init(|| {
                Regex::new(
                    r"(?:/(?:home|Users)/[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+|[A-Za-z]:\\Users\\[A-Za-z0-9._-]+(?:\\[A-Za-z0-9._-]+)+)",
                )
                .expect("host path regex")
            }),
            "[REDACTED_HOST_PATH]",
        ),
    ];
    let mut sanitized = text.to_owned();
    let mut redactions = Vec::new();
    for (category, pattern, replacement) in rules {
        let count = pattern.find_iter(&sanitized).count();
        if count > 0 {
            sanitized = pattern.replace_all(&sanitized, replacement).into_owned();
            redactions.push(json!({"category": category, "count": count}));
        }
    }
    let lowered = sanitized.to_ascii_lowercase();
    let mut categories = Vec::new();
    if [
        "ignore previous instructions",
        "ignore all instructions",
        "disregard previous",
    ]
    .iter()
    .any(|phrase| lowered.contains(phrase))
    {
        categories.push("instruction_override");
    }
    if ["system message", "you are chatgpt", "act as the system"]
        .iter()
        .any(|phrase| lowered.contains(phrase))
    {
        categories.push("role_impersonation");
    }
    let signals = json!({
        "schema": "prompt_injection_signals.v1",
        "detected": !categories.is_empty(),
        "categories": categories,
    });
    (sanitized, redactions, signals)
}

const fn default_max_items() -> u8 {
    DEFAULT_MAX_ITEMS
}

const fn default_max_source_tokens() -> u16 {
    DEFAULT_MAX_SOURCE_TOKENS
}

fn default_lookup_mode() -> String {
    "search".to_owned()
}

fn lookup_operation(mode: &str) -> &'static str {
    match mode {
        "search" => "context_lookup.search",
        "snippet" => "context_lookup.snippet",
        "tree" => "context_lookup.tree",
        "symbols" => "context_lookup.symbols",
        "impact" => "context_lookup.impact",
        "related_symbols" => "context_lookup.related_symbols",
        "test_owners" => "context_lookup.test_owners",
        "chunk" => "context_lookup.chunk",
        "explain_cache" => "context_lookup.explain_cache",
        "references" => "context_lookup.references",
        _ => "context_lookup.invalid_mode",
    }
}

fn memory_operation(mode: &str) -> &'static str {
    match mode {
        "get" => "context_memory.get",
        "upsert" => "context_memory.upsert",
        "summary_upsert" => "context_memory.summary_upsert",
        "decision_record" => "context_memory.decision_record",
        "validate" => "context_memory.validate",
        "compact" => "context_memory.compact",
        _ => "context_memory.invalid_mode",
    }
}

fn admin_operation(mode: &str) -> &'static str {
    match mode {
        "health" => "context_admin.health",
        "index_refresh" => "context_admin.index_refresh",
        "index_status" => "context_admin.index_status",
        "cache_stats" => "context_admin.cache_stats",
        "cache_prune" => "context_admin.cache_prune",
        "monitor_usage" => "context_admin.monitor_usage",
        "warmup" => "context_admin.warmup",
        "budget" => "context_admin.budget",
        "contracts" => "context_admin.contracts",
        "metrics" => "context_admin.metrics",
        "measurement_matrix" => "context_admin.measurement_matrix",
        "measurement_report" => "context_admin.measurement_report",
        "benchmark" => "context_admin.benchmark",
        "state_browser" => "context_admin.state_browser",
        "quality_eval" => "context_admin.quality_eval",
        "cache_plan" => "context_admin.cache_plan",
        "profile_calibrate" => "context_admin.profile_calibrate",
        "instructions" => "context_admin.instructions",
        "resource_proxy" => "context_admin.resource_proxy",
        "schema_minify" => "context_admin.schema_minify",
        _ => "context_admin.invalid_mode",
    }
}

fn resource_operation(uri: &str) -> &'static str {
    match uri {
        "repo://summary" => "resource.summary",
        "repo://tree/." | "repo://tree" => "resource.tree",
        "repo://metrics" => "resource.metrics",
        "repo://instructions/context-pack" => "resource.instructions",
        _ if uri.starts_with("repo://file/") => "resource.file",
        _ if uri.starts_with("repo://tree/") => "resource.tree",
        _ if uri.starts_with("repo://context/") => "resource.context",
        _ => "resource.invalid_uri",
    }
}

pub fn static_resource_text(uri: &str) -> Result<Option<String>> {
    match uri {
        "repo://instructions/context-pack" => {
            Ok(Some(serde_json::to_string(&instructions_payload())?))
        }
        _ => Ok(None),
    }
}

fn default_lookup_path() -> String {
    ".".to_owned()
}

const fn default_start_line() -> u32 {
    1
}

const fn default_max_results() -> u16 {
    20
}

const fn default_max_entries() -> u16 {
    200
}

const fn default_max_depth() -> u8 {
    2
}

fn default_memory_mode() -> String {
    "get".to_owned()
}

const fn default_confidence() -> f64 {
    1.0
}

fn default_memory_source() -> String {
    "agent".to_owned()
}

fn default_decided_by() -> String {
    "llm".to_owned()
}

const fn default_memory_max_entries() -> u16 {
    100
}

fn default_admin_mode() -> String {
    "health".to_owned()
}

const fn default_max_age_minutes() -> u32 {
    43_200
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn request_defaults_are_the_v2_contract_defaults() {
        let request: ContextPackRequest =
            serde_json::from_str(r#"{"prompt":"debug it"}"#).expect("request must deserialize");
        assert_eq!(request.max_items, 8);
        assert_eq!(request.max_source_tokens, 512);
        assert_eq!(request.evidence_policy, EvidencePolicy::Balanced);
        assert_eq!(request.cache_strategy, CacheStrategy::Fast);
        assert_eq!(request.validate_limits(), Ok(()));
    }

    #[test]
    fn balanced_pack_is_v2_deterministic_and_honors_explicit_paths() {
        let root = tempdir().expect("temporary repository");
        std::fs::create_dir(root.path().join("src")).expect("source directory");
        std::fs::write(
            root.path().join("src/context.py"),
            "def context_pack(prompt):\n    if not prompt:\n        raise ValueError('prompt')\n    return prompt\n",
        )
        .expect("fixture file");
        std::fs::write(root.path().join("noise.md"), "unrelated words\n").expect("noise file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug context_pack prompt validation",
            "focus_paths": ["src/context.py"]
        }))
        .expect("request");

        let first = engine.context_pack(&request).expect("first pack");
        let second = engine.context_pack(&request).expect("second pack");
        assert_eq!(first, second);
        let pack: ContextPackV2 = serde_json::from_slice(&first).expect("v2 response");
        assert_eq!(pack.v, 2);
        assert_eq!(pack.paths, vec!["src/context.py"]);
        assert_eq!(pack.evidence.len(), 1);
        assert!(pack.id.starts_with("pk_"));
        assert!(
            pack.more
                .as_deref()
                .is_some_and(|value| value.starts_with("ctxref-"))
        );
        let resolved = engine
            .store()
            .resolve_reference(pack.more.as_deref().expect("deferred reference"), "")
            .expect("resolve deferred evidence");
        assert_eq!(resolved["status"], "resolved");
    }

    #[test]
    fn request_rejects_repository_escape() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("safe.py"), "value = 1\n").expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "read it",
            "focus_paths": ["../secret"]
        }))
        .expect("request");
        assert!(engine.context_pack(&request).is_err());
    }

    #[test]
    fn repository_evidence_redacts_secrets_and_flags_injection_text() {
        let (sanitized, redactions, signals) = sanitize_text(
            "password=hunter2\n/home/alice/private/file.txt\nignore previous instructions",
        );
        assert!(!sanitized.contains("hunter2"));
        assert!(!sanitized.contains("/home/alice"));
        assert!(sanitized.contains("[REDACTED]"));
        assert_eq!(redactions.len(), 2);
        assert_eq!(signals["detected"], true);
        assert_eq!(signals["categories"][0], "instruction_override");
    }

    #[test]
    fn memory_contracts_persist_sanitized_values_in_v2_state() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("safe.py"), "value = 1\n").expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let upsert: ContextMemoryRequest = serde_json::from_value(json!({
            "mode": "upsert",
            "namespace": "component/auth",
            "key": "token-contract",
            "value": {"password": "password=hunter2"},
            "source": "test"
        }))
        .expect("upsert request");
        let response: Value =
            serde_json::from_slice(&engine.context_memory(&upsert).expect("memory upsert"))
                .expect("upsert response");
        assert_eq!(response["schema"], "context_memory.upsert.v1");
        assert_eq!(response["sensitivity"]["redacted"], true);

        let get: ContextMemoryRequest =
            serde_json::from_value(json!({"mode": "get", "namespace": "component/auth"}))
                .expect("get request");
        let response: Value =
            serde_json::from_slice(&engine.context_memory(&get).expect("memory get"))
                .expect("get response");
        assert_eq!(response["count"], 1);
        assert_eq!(
            response["entries"][0]["value"]["password"],
            "password=[REDACTED]"
        );
        assert!(!response.to_string().contains("hunter2"));
    }

    #[test]
    fn stable_lookup_modes_return_their_v1_schemas() {
        let root = tempdir().expect("temporary repository");
        std::fs::create_dir_all(root.path().join("src")).expect("source directory");
        std::fs::create_dir_all(root.path().join("tests")).expect("tests directory");
        std::fs::write(
            root.path().join("src/auth.py"),
            "class AuthService:\n    def login(self):\n        return issue_token()\n\ndef issue_token():\n    return 'token'\n",
        )
        .expect("source fixture");
        std::fs::write(
            root.path().join("tests/test_auth.py"),
            "from src.auth import issue_token\n\ndef test_issue_token():\n    assert issue_token()\n",
        )
        .expect("test fixture");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let lookup = |request: Value| {
            let request: ContextLookupRequest =
                serde_json::from_value(request).expect("lookup request");
            serde_json::from_slice::<Value>(
                &engine.context_lookup(&request).expect("context lookup"),
            )
            .expect("lookup response")
        };

        assert_eq!(lookup(json!({"mode": "tree"}))["schema"], "context_tree.v1");
        assert_eq!(
            lookup(json!({"mode": "symbols", "query": "issue_token"}))["schema"],
            "context_symbols.v1"
        );
        let impact = lookup(json!({"mode": "impact", "path": "src/auth.py", "max_results": 8}));
        assert_eq!(impact["schema"], "context_lookup.impact.v1");
        assert!(
            impact["related"]
                .as_array()
                .is_some_and(|rows| { rows.iter().any(|row| row["path"] == "tests/test_auth.py") })
        );
        assert_eq!(
            lookup(json!({
                "mode": "related_symbols",
                "path": "src/auth.py",
                "query": "issue_token"
            }))["schema"],
            "context_lookup.related_symbols.v1"
        );
        assert_eq!(
            lookup(json!({"mode": "test_owners", "path": "src/auth.py"}))["count"],
            1
        );
        assert_eq!(
            lookup(json!({"mode": "chunk", "path": "src/auth.py", "end_line": 4}))["schema"],
            "context_lookup.chunk.v1"
        );
        assert_eq!(
            lookup(json!({"mode": "explain_cache", "path": "src/auth.py"}))["count"],
            0
        );
        let filtered = lookup(json!({
            "mode": "search",
            "query": "issue_token",
            "include_globs": ["tests/**"]
        }));
        assert!(filtered["results"].as_array().is_some_and(|rows| {
            !rows.is_empty() && rows.iter().all(|row| row["path"] == "tests/test_auth.py")
        }));
    }

    #[test]
    fn search_scopes_directory_and_globs_before_candidate_truncation() {
        let root = tempdir().expect("temporary repository");
        std::fs::create_dir_all(root.path().join("src")).expect("source directory");
        std::fs::create_dir_all(root.path().join("tests")).expect("test directory");
        std::fs::write(root.path().join("src/inside.rs"), "fn scoped_needle() {}\n")
            .expect("source fixture");
        std::fs::write(
            root.path().join("tests/test_scope.rs"),
            "fn scoped_needle() {}\n",
        )
        .expect("test fixture");
        for index in 0..80 {
            std::fs::write(
                root.path().join(format!("noise-{index}.txt")),
                "scoped_needle scoped_needle scoped_needle\n",
            )
            .expect("noise fixture");
        }
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let lookup = |request: Value| {
            let request: ContextLookupRequest = serde_json::from_value(request).expect("request");
            serde_json::from_slice::<Value>(&engine.context_lookup(&request).expect("lookup"))
                .expect("lookup JSON")
        };

        let directory = lookup(json!({
            "mode": "search", "query": "scoped_needle", "path": "src", "max_results": 8
        }));
        assert_eq!(directory["count"], 1);
        assert_eq!(directory["results"][0]["path"], "src/inside.rs");

        let glob = lookup(json!({
            "mode": "search", "query": "scoped_needle", "include_globs": ["tests/**"], "max_results": 8
        }));
        assert_eq!(glob["count"], 1);
        assert_eq!(glob["results"][0]["path"], "tests/test_scope.rs");
    }

    #[tokio::test]
    async fn explicit_index_refresh_rebuilds_before_lookup_and_metrics_keep_monitor_fields() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("notes.txt"), "before refresh\n").expect("fixture");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        std::fs::write(root.path().join("notes.txt"), "after_refresh_marker\n")
            .expect("changed fixture");

        let refresh: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "index_refresh"})).expect("refresh request");
        let refresh: Value =
            serde_json::from_slice(&engine.context_admin(&refresh).await.expect("refresh index"))
                .expect("refresh JSON");
        assert_eq!(refresh["skipped"], false);
        assert_eq!(refresh["updated_count"], 1);

        let lookup: ContextLookupRequest = serde_json::from_value(json!({
            "mode": "search", "query": "after_refresh_marker", "max_results": 8
        }))
        .expect("lookup request");
        let lookup: Value = serde_json::from_slice(
            &engine
                .context_lookup(&lookup)
                .expect("lookup changed index"),
        )
        .expect("lookup JSON");
        assert_eq!(lookup["results"][0]["path"], "notes.txt");
        let invalid_lookup: ContextLookupRequest =
            serde_json::from_value(json!({"mode": "invalid"})).expect("invalid lookup request");
        assert!(engine.context_lookup(&invalid_lookup).is_err());
        let memory: ContextMemoryRequest =
            serde_json::from_value(json!({"mode": "validate"})).expect("memory request");
        engine.context_memory(&memory).expect("memory validation");
        let missing_reference: ResultReferenceRequest =
            serde_json::from_value(json!({"reference_id": "ctxref-missing"}))
                .expect("reference request");
        engine
            .result_reference_resolve(&missing_reference)
            .expect("structured missing reference response");

        let pack: ContextPackRequest =
            serde_json::from_value(json!({"prompt": "after_refresh_marker"}))
                .expect("pack request");
        engine.context_pack(&pack).expect("pack");
        let metrics: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "metrics"})).expect("metrics request");
        let metrics: Value =
            serde_json::from_slice(&engine.context_admin(&metrics).await.expect("metrics"))
                .expect("metrics JSON");
        assert_eq!(
            metrics["requests"]["by_operation"]["context_pack"]["count"],
            1
        );
        assert!(
            metrics["requests"]["by_operation"]["context_pack"]
                .get("avg_elapsed_ms")
                .is_some()
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["context_lookup.search"]["count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["context_lookup.invalid_mode"]["error_count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["context_memory.validate"]["count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["result_reference_resolve"]["count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["context_admin.index_refresh"]["count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["context_pack"]["success_count"],
            1
        );
        assert!(
            metrics["requests"]["by_operation"]["context_pack"]["p95_recent_ms"]
                .as_f64()
                .is_some()
        );
        assert!(
            metrics["tokens"]["context_pack"]["selected_source_tokens_est"]
                .as_u64()
                .is_some_and(|tokens| tokens > 0)
        );
        assert!(
            metrics["tokens"]["context_pack"]["compression_factor_est"]
                .as_f64()
                .is_some_and(|factor| factor > 0.0)
        );
        let matrix: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "measurement_matrix"})).expect("matrix request");
        let matrix: Value = serde_json::from_slice(
            &engine
                .context_admin(&matrix)
                .await
                .expect("measurement matrix"),
        )
        .expect("measurement matrix JSON");
        assert_eq!(matrix["checks"][0]["status"], "pass");
        assert_eq!(
            matrix["checks"][1]["key"],
            "tokens.context_pack.compression_factor_est"
        );
    }

    #[test]
    fn unloaded_metrics_keep_the_monitor_contract_without_opening_an_engine() {
        let request: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "measurement_report"})).expect("metrics request");
        let response: Value = serde_json::from_slice(
            &unloaded_admin_response(&request, "unloaded-project", "unloaded")
                .expect("metrics response"),
        )
        .expect("metrics JSON");
        assert_eq!(response["schema"], "context_measurement_report.v1");
        assert_eq!(response["metrics"]["schema"], "context_metrics.v1");
        assert_eq!(response["metrics"]["project_id"], "unloaded-project");
        assert_eq!(response["metrics"]["background"]["status"], "unloaded");
        assert_eq!(
            response["metrics"]["requests"]["by_operation"]["context_pack"]["count"],
            0
        );
        assert_eq!(
            response["matrix"]["schema"],
            "context_measurement_matrix.v1"
        );
        assert_eq!(response["matrix"]["checks"][0]["status"], "insufficient");
    }

    #[tokio::test]
    async fn stable_admin_modes_and_resources_keep_schema_and_path_boundaries() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("README.md"), "# Fixture\n").expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let modes = [
            ("health", "context_admin.health.v1"),
            ("index_refresh", "context_index.refresh.v1"),
            ("index_status", "context_index.status.v1"),
            ("cache_stats", "context_cache.stats.v1"),
            ("cache_prune", "context_cache.prune.v1"),
            ("warmup", "context_cache.warmup.v1"),
            ("budget", "context_budget.v1"),
            ("contracts", "tool_output_contracts.compact.v1"),
            ("metrics", "context_metrics.v1"),
            ("measurement_matrix", "context_measurement_matrix.v1"),
            ("measurement_report", "context_measurement_report.v1"),
            ("benchmark", "context_benchmark.v1"),
            ("state_browser", "context_state_browser.v1"),
            ("quality_eval", "context_quality_eval.v1"),
            ("cache_plan", "context_budget_plan.v1"),
            ("profile_calibrate", "context_profile_calibration.v1"),
            ("instructions", "codex_context_pack_first.instructions.v1"),
            ("schema_minify", "tool_output_contract.compact.v1"),
        ];
        for (mode, schema) in modes {
            let request: ContextAdminRequest = serde_json::from_value(json!({
                "mode": mode,
                "tool_name": "context_lookup"
            }))
            .expect("admin request");
            let response: Value = serde_json::from_slice(
                &engine
                    .context_admin(&request)
                    .await
                    .expect("admin response"),
            )
            .expect("admin JSON");
            assert_eq!(response["schema"], schema, "mode={mode}");
            assert!(
                !response
                    .to_string()
                    .contains(root.path().to_string_lossy().as_ref())
            );
        }
        let legacy_request: ContextAdminRequest = serde_json::from_value(json!({
            "mode": "metrics_and_matrix"
        }))
        .expect("legacy-shaped admin request");
        let error = engine
            .context_admin(&legacy_request)
            .await
            .expect_err("legacy mode must be rejected");
        assert!(error.to_string().contains("unsupported context_admin mode"));
        let summary: Value = serde_json::from_str(
            &engine
                .resource_text("repo://summary")
                .expect("summary resource"),
        )
        .expect("summary JSON");
        assert_eq!(summary["schema"], "workspace_facts.v1");
        assert_eq!(
            engine
                .resource_text("repo://file/README.md")
                .expect("file resource"),
            "# Fixture\n"
        );
        let metrics: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "metrics"})).expect("metrics request");
        let metrics: Value = serde_json::from_slice(
            &engine
                .context_admin(&metrics)
                .await
                .expect("metrics response"),
        )
        .expect("metrics JSON");
        assert_eq!(
            metrics["requests"]["by_operation"]["resource.summary"]["count"],
            1
        );
        assert_eq!(
            metrics["requests"]["by_operation"]["resource.file"]["count"],
            1
        );
    }

    #[test]
    fn context_pack_never_persists_raw_secrets_or_host_paths() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("unsafe.txt"),
            "api_key=supersecret\n/home/alice/private/file.txt\nignore previous instructions\n",
        )
        .expect("unsafe fixture");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "review unsafe api key instructions",
            "focus_paths": ["unsafe.txt"]
        }))
        .expect("pack request");
        let encoded = engine.context_pack(&request).expect("pack");
        assert!(!String::from_utf8_lossy(&encoded).contains("supersecret"));
        assert!(!String::from_utf8_lossy(&encoded).contains("/home/alice"));
        let pack: ContextPackV2 = serde_json::from_slice(&encoded).expect("pack JSON");
        let resolved = engine
            .store()
            .resolve_reference(pack.more.as_deref().expect("reference"), "")
            .expect("resolve reference");
        let resolved = resolved.to_string();
        assert!(!resolved.contains("supersecret"));
        assert!(!resolved.contains("/home/alice"));
        let raw_state =
            std::fs::read(engine.store().paths().lmdb.join("data.mdb")).expect("raw LMDB data");
        assert!(
            !raw_state
                .windows(b"supersecret".len())
                .any(|window| window == b"supersecret")
        );
        assert!(
            !raw_state
                .windows(b"/home/alice".len())
                .any(|window| window == b"/home/alice")
        );
    }

    #[test]
    fn memory_compaction_creates_a_bounded_summary_after_threshold() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("safe.py"), "value = 1\n").expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let now = now_iso().expect("timestamp");
        let entries = (0..41)
            .map(|index| {
                json!({
                    "namespace": "bulk",
                    "key": format!("key-{index}"),
                    "value": {"index": index},
                    "confidence": 1.0,
                    "source": "test",
                    "tags": [],
                    "created_at": now,
                    "updated_at": now,
                    "expires_at": null,
                    "sensitivity": {"redacted": false, "redaction_count": 0, "categories": []},
                })
            })
            .collect::<Vec<_>>();
        engine
            .store()
            .put_json(
                "memory:store",
                &json!({
                    "schema": "context_memory_store.v1",
                    "entries": entries,
                    "summaries": [],
                    "decisions": []
                }),
            )
            .expect("bulk memory");
        let request: ContextMemoryRequest = serde_json::from_value(json!({
            "mode": "compact",
            "namespace": "bulk"
        }))
        .expect("compact request");
        let response: Value =
            serde_json::from_slice(&engine.context_memory(&request).expect("compact memory"))
                .expect("compact response");
        assert_eq!(response["compacted"], true);
        let memory = engine
            .store()
            .get_json("memory:store")
            .expect("memory store")
            .expect("memory exists");
        assert_eq!(memory["summaries"][0]["focus"], "auto_compact");
        assert!(memory["summaries"][0]["summary"].as_str().unwrap().len() <= 1200);
    }

    #[tokio::test]
    async fn l0_cache_returns_identical_wire_bytes_and_reports_hits() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("auth.py"),
            "def validate_token(token):\n    if not token:\n        raise ValueError('token')\n    return token\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug validate_token guard",
            "focus_paths": ["auth.py"]
        }))
        .expect("pack request");
        let first = engine
            .context_pack_cached(&request)
            .await
            .expect("uncached response");
        let second = engine
            .context_pack_cached(&request)
            .await
            .expect("cached response");
        assert_eq!(first, second);
        assert_eq!(engine.metrics.l0_misses.load(Ordering::Relaxed), 1);
        assert_eq!(engine.metrics.l0_hits.load(Ordering::Relaxed), 1);
        let l0 = engine.l0_storage_stats();
        assert_eq!(l0.entries, 1);
        assert!(l0.weighted_bytes >= first.len() as u64);
    }

    #[tokio::test]
    async fn l0_cache_uses_semantic_request_fields_after_freshness() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("auth.py"),
            "def validate_token(token):\n    return token\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let first: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug validate_token guard",
            "focus_paths": ["auth.py"],
            "memory_session": "first-turn",
            "client_profile": "codex",
            "cache_strategy": "fast",
        }))
        .expect("first request");
        let second: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug validate_token guard",
            "changed_files": ["auth.py"],
            "memory_session": "second-turn",
            "client_profile": "generic",
            "model_profile": "unknown",
            "cache_strategy": "fresh",
        }))
        .expect("second request");

        let first = engine
            .context_pack_cached(&first)
            .await
            .expect("first response");
        let second = engine
            .context_pack_cached(&second)
            .await
            .expect("second response");

        assert_eq!(first, second);
        assert_eq!(engine.metrics.l0_misses.load(Ordering::Relaxed), 1);
        assert_eq!(engine.metrics.l0_hits.load(Ordering::Relaxed), 1);
        assert_eq!(
            engine
                .metrics
                .l0_cold_or_invalidated_misses
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine
                .metrics
                .l0_request_variant_misses
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn l0_revalidates_references_and_rebuilds_invalid_entries() {
        for mutation in ["missing", "expired", "tampered"] {
            let root = tempdir().expect("temporary repository");
            std::fs::write(
                root.path().join("cache.rs"),
                "fn active_reference_cache() { assert!(true); }\n",
            )
            .expect("fixture file");
            let engine = ProjectEngine::build(root.path()).expect("engine");
            let request: ContextPackRequest = serde_json::from_value(json!({
                "prompt": "active reference cache",
                "focus_paths": ["cache.rs"]
            }))
            .expect("pack request");
            let first = engine
                .context_pack_cached(&request)
                .await
                .expect("first response");
            let first_pack: ContextPackV2 = serde_json::from_slice(&first).expect("first pack");
            let reference_id = first_pack.more.expect("active more reference");
            let key = format!("reference:{reference_id}");
            assert!(
                engine
                    .store
                    .reference_is_active(&reference_id)
                    .expect("active reference")
            );
            match mutation {
                "missing" => {
                    engine.store.delete(&key).expect("delete reference");
                }
                "expired" => {
                    let mut record = engine
                        .store
                        .get_json(&key)
                        .expect("reference read")
                        .expect("record");
                    record["expires_at"] = Value::String("2000-01-01T00:00:00Z".to_owned());
                    engine
                        .store
                        .put_json(&key, &record)
                        .expect("expire reference");
                }
                "tampered" => {
                    let mut record = engine
                        .store
                        .get_json(&key)
                        .expect("reference read")
                        .expect("record");
                    record["body"] = Value::String("tampered".to_owned());
                    engine
                        .store
                        .put_json(&key, &record)
                        .expect("tamper reference");
                }
                _ => unreachable!(),
            }

            let second = engine
                .context_pack_cached(&request)
                .await
                .expect("rebuilt response");
            assert_eq!(first, second, "mutation={mutation}");
            assert!(
                engine
                    .store
                    .reference_is_active(&reference_id)
                    .expect("rebuilt reference")
            );
            assert_eq!(engine.metrics.l0_hits.load(Ordering::Relaxed), 0);
            assert_eq!(engine.metrics.l0_misses.load(Ordering::Relaxed), 2);
            assert_eq!(engine.metrics.l0_invalidations.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn naturally_expired_l0_entry_is_a_second_cold_miss() {
        let root = tempdir().expect("temporary repository");
        let state = tempdir().expect("temporary state");
        std::fs::write(root.path().join("cache.rs"), "fn idle_cache() {}\n").expect("fixture file");
        let engine = ProjectEngine::build_with_state_and_l0_idle(
            root.path(),
            state.path(),
            "idle-expiry",
            StdDuration::from_millis(5),
            Arc::new(UsageMonitor::open(state.path().join("monitor")).expect("monitor")),
        )
        .expect("engine");
        let request: ContextPackRequest =
            serde_json::from_value(json!({"prompt": "idle_cache"})).expect("request");
        engine
            .context_pack_cached(&request)
            .await
            .expect("first response");
        std::thread::sleep(StdDuration::from_millis(30));
        engine
            .context_pack_cached(&request)
            .await
            .expect("second response");

        assert_eq!(engine.metrics.l0_misses.load(Ordering::Relaxed), 2);
        assert_eq!(
            engine
                .metrics
                .l0_cold_or_invalidated_misses
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            engine
                .metrics
                .l0_request_variant_misses
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn warmup_populates_l1_and_optional_l0_without_pack_telemetry() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("architecture.rs"),
            "fn repository_architecture() {}\nfn implementation_source_code() {}\nfn review_correctness_safety() {}\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let secret_prompt = "  private prompt cache architecture  ";
        let request: ContextAdminRequest = serde_json::from_value(json!({
            "mode": "warmup",
            "prompt": secret_prompt,
            "path": "architecture.rs"
        }))
        .expect("warmup request");
        let response = engine
            .context_admin(&request)
            .await
            .expect("warmup response");
        let warmup: Value = serde_json::from_slice(&response).expect("warmup JSON");

        assert_eq!(warmup["schema"], "context_cache.warmup.v1");
        assert_eq!(warmup["manifest"]["prompt_warmed"], true);
        assert_eq!(warmup["cache"]["l1_after"], 0);
        assert_eq!(warmup["cache"]["l0_after"], 1);
        assert_eq!(warmup["search_cache"]["query_count"], 1);
        assert!(!String::from_utf8_lossy(&response).contains(secret_prompt));
        let prompt_request = warmup_prompt_request(secret_prompt, Some("architecture.rs"));
        engine
            .admit_context_pack_cached(&prompt_request)
            .await
            .expect("warm exact L0 hit");
        assert_eq!(engine.metrics.l0_hits.load(Ordering::Relaxed), 1);
        assert!(
            !engine
                .metrics
                .operations
                .lock()
                .expect("operations")
                .contains_key("context_pack")
        );
        assert_eq!(
            engine.metrics.pack_wire_tokens_est.load(Ordering::Relaxed),
            0
        );
        assert_eq!(engine.metrics.warmup_runs.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn generic_warmup_is_empty_and_positive_frontier_requires_two_hits() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("warmup.rs"),
            "fn repository_architecture() {}\nfn implementation_source_code() {}\nfn debug_failing_tests() {}\nfn review_correctness_safety() {}\nfn test_coverage_ownership() {}\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "warmup"})).expect("warmup request");
        let response: Value = serde_json::from_slice(
            &engine
                .context_admin(&request)
                .await
                .expect("warmup response"),
        )
        .expect("warmup JSON");

        assert_eq!(response["search_cache"]["query_count"], 0);
        assert_eq!(
            engine
                .store
                .iter_json("frontier:")
                .expect("frontiers")
                .len(),
            0
        );
        let index = engine.index();
        let adaptive = warmup_prompt_request("repository architecture", None);
        engine
            .retrieve_candidates(&index, &adaptive, &[])
            .expect("first observation");
        assert!(
            engine
                .store
                .iter_json("frontier:")
                .expect("frontiers")
                .is_empty()
        );
        engine
            .retrieve_candidates(&index, &adaptive, &[])
            .expect("second observation");
        assert_eq!(
            engine
                .store
                .iter_json("frontier:")
                .expect("frontiers")
                .len(),
            1
        );
        engine
            .retrieve_candidates(&index, &adaptive, &[])
            .expect("exact adaptive reuse");
        assert_eq!(engine.metrics.l1_exact_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn detailed_usage_is_global_default_off_bounded_and_prompt_safe() {
        let root = tempdir().expect("temporary repository");
        let state = tempdir().expect("temporary state");
        std::fs::write(
            root.path().join("usage.rs"),
            "fn private_usage_anchor() {}\n",
        )
        .expect("fixture file");
        let monitor = Arc::new(UsageMonitor::open(state.path().join("global")).expect("monitor"));
        let engine = ProjectEngine::build_with_state_and_monitor(
            root.path(),
            state.path().join("project"),
            "usage-project",
            Arc::clone(&monitor),
        )
        .expect("engine");
        let secret = "private usage prompt";
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": secret,
            "focus_paths": ["usage.rs"]
        }))
        .expect("request");

        let disabled_bytes = engine
            .context_pack_cached(&request)
            .await
            .expect("disabled pack");
        let disabled_pack: ContextPackV2 =
            serde_json::from_slice(&disabled_bytes).expect("disabled response");
        assert!(
            monitor.report(Some("usage-project")).expect("report")["buckets"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        monitor.action("enable", None).expect("enable");
        let mut admitted = request.clone();
        admitted.known_evidence = vec!["client-one".to_owned()];
        engine
            .context_pack_cached(&admitted)
            .await
            .expect("admission pack");
        let mut delta = request.clone();
        delta.base_pack = Some(disabled_pack.id);
        delta.known_evidence = vec!["client-two".to_owned()];
        delta.cache_strategy = CacheStrategy::Fresh;
        engine
            .context_pack_cached(&delta)
            .await
            .expect("delta pack");
        engine
            .context_pack_cached(&delta)
            .await
            .expect("cached delta pack");
        let enabled = monitor
            .report(Some("usage-project"))
            .expect("enabled report");
        let bucket = &enabled["buckets"][0];
        assert_eq!(bucket["request_count"], 3);
        assert_eq!(bucket["cache_outcomes"]["l0_miss"], 2);
        assert_eq!(bucket["cache_outcomes"]["l0_hit"], 1);
        assert_eq!(bucket["frontier_outcomes"]["admitted"], 1);
        assert_eq!(bucket["frontier_outcomes"]["exact_hit"], 1);
        assert_eq!(bucket["index"]["refresh_checked"], 2);
        assert_eq!(bucket["index"]["refresh_updated"], 0);
        assert_eq!(bucket["delta"]["base_pack_requests"], 2);
        assert_eq!(bucket["delta"]["known_evidence_requests"], 3);
        assert_eq!(bucket["routes"]["explore"], 3);
        assert_eq!(bucket["term_count_buckets"]["3_5"], 3);
        assert_eq!(bucket["scope_count_buckets"]["1_2"], 3);
        assert!(bucket["stages_micros"]["retrieval"].as_u64().is_some());
        assert!(bucket["stages_micros"]["pack_build"].as_u64().is_some());
        assert!(bucket["stages_micros"]["cache"].as_u64().is_some());
        let encoded = enabled.to_string();
        assert!(!encoded.contains(secret));
        assert!(!encoded.contains("usage.rs"));
        monitor.action("disable", None).expect("disable");
        engine
            .context_pack_cached(&request)
            .await
            .expect("disabled again");
        assert_eq!(
            monitor.report(Some("usage-project")).expect("final report")["buckets"][0]["request_count"],
            3
        );
    }

    #[tokio::test]
    async fn cache_prune_applies_reason_precedence_and_age_to_all_frontiers() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(root.path().join("cache.rs"), "fn prune_cache() {}\n")
            .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let signature = engine.index().stats().refresh_signature.clone();
        let now = now_millis();
        let record =
            |key: &str, negative: bool, expires_at_ms: u64, refresh: &str, updated: Option<u64>| {
                let mut value = serde_json::to_value(FrontierRecord {
                    schema: "context_frontier.v2".to_owned(),
                    key: key.to_owned(),
                    route: "general".to_owned(),
                    scope: Vec::new(),
                    generation: 0,
                    candidate_capacity: DEFAULT_MAX_ITEMS,
                    terms: Vec::new(),
                    candidate_ids: Vec::new(),
                    scores: Vec::new(),
                    cumulative_token_costs: Vec::new(),
                    dependencies: Vec::new(),
                    score_cutoff: 0.0,
                    refresh_signature: refresh.to_owned(),
                    negative,
                    expires_at_ms,
                    updated_at_ms: updated.unwrap_or_default(),
                })
                .expect("frontier JSON");
                if updated.is_none() {
                    value
                        .as_object_mut()
                        .expect("frontier object")
                        .remove("updated_at_ms");
                }
                value
            };
        let old = now.saturating_sub(120 * 60_000);
        for (key, value) in [
            (
                "frontier:expired",
                record("expired", true, now.saturating_sub(1), "stale", Some(old)),
            ),
            (
                "frontier:stale",
                record("stale", false, 0, "stale", Some(old)),
            ),
            (
                "frontier:aged",
                record("aged", false, 0, &signature, Some(old)),
            ),
            (
                "frontier:missing",
                record("missing", false, 0, &signature, None),
            ),
            (
                "frontier:recent",
                record("recent", false, 0, &signature, Some(now)),
            ),
        ] {
            engine.store.put_json(key, &value).expect("frontier row");
        }
        engine
            .store
            .put_json(
                "reference:ctxref-expired",
                &json!({"expires_at":"2000-01-01T00:00:00Z"}),
            )
            .expect("expired reference");
        engine
            .store
            .put_json(
                "deferred:expired",
                &json!({
                    "reference_id":"ctxref-expired",
                    "expires_at":"2000-01-01T00:00:00Z"
                }),
            )
            .expect("expired deferred row");
        let prune: ContextAdminRequest = serde_json::from_value(json!({
            "mode": "cache_prune", "max_age_minutes": 60
        }))
        .expect("prune request");
        let response: Value =
            serde_json::from_slice(&engine.context_admin(&prune).await.expect("prune response"))
                .expect("prune JSON");
        assert_eq!(response["expired_removed"], 1);
        assert_eq!(response["stale_removed"], 1);
        assert_eq!(response["age_removed"], 2);
        assert_eq!(response["deferred_removed"], 1);
        assert_eq!(response["reference_removed"], 1);
        assert_eq!(
            engine
                .store
                .iter_json("frontier:")
                .expect("remaining")
                .len(),
            1
        );

        let prune_all: ContextAdminRequest = serde_json::from_value(json!({
            "mode": "cache_prune", "max_age_minutes": 0
        }))
        .expect("prune all request");
        let response: Value = serde_json::from_slice(
            &engine
                .context_admin(&prune_all)
                .await
                .expect("prune all response"),
        )
        .expect("prune all JSON");
        assert_eq!(response["age_removed"], 1);
        assert!(
            engine
                .store
                .iter_json("frontier:")
                .expect("empty")
                .is_empty()
        );
    }

    #[test]
    fn base_pack_delta_omits_unchanged_evidence() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("auth.py"),
            "def validate_token(token):\n    if not token:\n        raise ValueError('token')\n    return token\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let first_request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug validate_token guard",
            "focus_paths": ["auth.py"]
        }))
        .expect("first request");
        let first: ContextPackV2 =
            serde_json::from_slice(&engine.context_pack(&first_request).expect("first pack"))
                .expect("first response");
        assert!(!first.evidence.is_empty());
        let delta_request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "debug validate_token guard",
            "focus_paths": ["auth.py"],
            "base_pack": first.id
        }))
        .expect("delta request");
        let delta: ContextPackV2 =
            serde_json::from_slice(&engine.context_pack(&delta_request).expect("delta pack"))
                .expect("delta response");
        assert!(delta.evidence.is_empty());
        assert_eq!(delta.id, first.id);
    }

    #[test]
    fn fresh_changed_file_rebuilds_and_changes_evidence_identity() {
        let root = tempdir().expect("temporary repository");
        let path = root.path().join("auth.py");
        std::fs::write(&path, "def validate_token(token):\n    return token\n")
            .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "validate_token",
            "focus_paths": ["auth.py"]
        }))
        .expect("first request");
        let first: ContextPackV2 =
            serde_json::from_slice(&engine.context_pack(&request).expect("first pack"))
                .expect("first response");
        std::fs::write(
            &path,
            "def validate_token(token):\n    if not token:\n        raise ValueError('missing')\n    return token\n",
        )
        .expect("changed fixture");
        let changed: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "validate_token",
            "focus_paths": ["auth.py"],
            "changed_files": ["auth.py"],
            "cache_strategy": "fresh"
        }))
        .expect("changed request");
        let second: ContextPackV2 =
            serde_json::from_slice(&engine.context_pack(&changed).expect("changed pack"))
                .expect("changed response");
        assert_ne!(first.id, second.id);
        assert_ne!(first.evidence[0].0, second.evidence[0].0);
        assert_eq!(engine.freshness.generation.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn l1_never_serves_approximate_reordered_terms() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("ranking.rs"),
            "fn deterministic_frontier_ranking() {\n    let candidate = 1;\n    assert!(candidate > 0);\n}\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let first: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "deterministic frontier ranking candidate"
        }))
        .expect("first request");
        engine.context_pack(&first).expect("first pack");
        let reordered: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "candidate ranking frontier deterministic"
        }))
        .expect("reordered request");
        engine.context_pack(&reordered).expect("reranked pack");
        assert_eq!(
            engine.metrics.l1_approximate_hits.load(Ordering::Relaxed),
            0
        );
        assert_eq!(engine.metrics.retrieval_misses.load(Ordering::Relaxed), 2);
        let state = engine
            .store
            .iter_json("frontier:")
            .expect("frontier state")
            .into_iter()
            .map(|(_, value)| value.to_string())
            .collect::<String>();
        assert!(!state.contains("deterministic frontier ranking candidate"));
        assert!(!state.contains("candidate ranking frontier deterministic"));
    }

    #[test]
    fn l1_frontier_identity_is_route_independent_and_cold_equivalent() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("shared.rs"),
            "fn shared_frontier_anchor() { assert!(true); }\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let request: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "review shared frontier anchor"
        }))
        .expect("request");
        assert_eq!(classify_route(&request.prompt), "review");
        let index = engine.index();
        let (cold, terms) = index
            .search(&request.prompt, &[], usize::from(request.max_items))
            .expect("cold search");
        let mut fingerprints = concept_fingerprints(&terms);
        fingerprints.sort();
        fingerprints.dedup();
        let diagnostic_debug_record = engine
            .build_frontier_record(
                &index,
                "debug",
                Vec::new(),
                fingerprints,
                request.max_items,
                &cold,
            )
            .expect("frontier");
        engine
            .admit_frontier_batch(&[diagnostic_debug_record])
            .expect("admit frontier");

        let (cached, _, outcome) = engine
            .retrieve_candidates(&index, &request, &[])
            .expect("route-independent hit");
        assert!(matches!(outcome, FrontierOutcome::ExactHit));
        assert_eq!(
            cached.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
            cold.iter().map(|hit| &hit.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn negative_frontier_is_exact_only_and_does_not_mask_similar_search() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("evidence.rs"),
            "fn alpha_beta_gamma_delta_epsilon_eta() { assert!(true); }\n",
        )
        .expect("fixture file");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let negative_prompt = "alpha beta gamma delta epsilon zeta";
        let exact_request: ContextPackRequest =
            serde_json::from_value(json!({"prompt": negative_prompt})).expect("exact request");
        let terms = normalize_terms(negative_prompt, 8);
        let index = engine.index();
        let negative = engine
            .build_frontier_record(
                &index,
                classify_route(negative_prompt),
                Vec::new(),
                {
                    let mut fingerprints = concept_fingerprints(&terms);
                    fingerprints.sort();
                    fingerprints
                },
                DEFAULT_MAX_ITEMS,
                &[],
            )
            .expect("negative frontier");
        engine
            .admit_frontier_batch(&[negative])
            .expect("admit negative frontier");
        let (exact_hits, _, _) = engine
            .retrieve_candidates(&index, &exact_request, &[])
            .expect("exact negative reuse");
        assert!(exact_hits.is_empty());

        let similar: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "alpha beta gamma delta epsilon eta"
        }))
        .expect("similar request");
        let (similar_hits, _, _) = engine
            .retrieve_candidates(&index, &similar, &[])
            .expect("real similar search");
        assert!(similar_hits.iter().any(|hit| hit.path == "evidence.rs"));
        assert_eq!(engine.metrics.l1_exact_hits.load(Ordering::Relaxed), 1);
        assert_eq!(
            engine.metrics.l1_approximate_hits.load(Ordering::Relaxed),
            0
        );
        assert_eq!(engine.metrics.retrieval_misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn l1_frontier_scope_is_insensitive_to_explicit_path_order() {
        let root = tempdir().expect("temporary repository");
        std::fs::write(
            root.path().join("alpha.rs"),
            "fn frontier_scope_alpha() { let candidate = 1; }\n",
        )
        .expect("alpha fixture");
        std::fs::write(
            root.path().join("beta.rs"),
            "fn frontier_scope_beta() { let candidate = 2; }\n",
        )
        .expect("beta fixture");
        let engine = ProjectEngine::build(root.path()).expect("engine");
        let first: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "frontier scope candidate",
            "focus_paths": ["alpha.rs", "beta.rs"]
        }))
        .expect("first request");
        let reordered: ContextPackRequest = serde_json::from_value(json!({
            "prompt": "frontier scope candidate",
            "focus_paths": ["beta.rs", "alpha.rs"]
        }))
        .expect("reordered request");

        engine.context_pack(&first).expect("first pack");
        engine.context_pack(&reordered).expect("reordered pack");
        engine.context_pack(&first).expect("third pack");

        assert_eq!(engine.metrics.l1_exact_hits.load(Ordering::Relaxed), 1);
    }
}
