use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use context_core::{
    ContextAdminRequest, ContextPackRejectionClass, ContextPackRequest, GovernedFrontierLineage,
    ProjectEngine, ResultReferenceRequest, SharedFrontierCache, UsageMonitor,
};
use context_index::{WorkControl, is_ignored_repository_path};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use url::Url;
use walkdir::{DirEntry, WalkDir};

const ENGINE_CAPACITY: usize = 8;
const DISCOVERY_MAX_PROJECTS: usize = 100;
const DEFAULT_DISCOVERY_MAX_DEPTH: usize = 4;
const PROJECT_CATALOG_FILE: &str = "project-catalog.v1.json";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 48;
const DEFAULT_CANCELLATION_GRACE_SECS: u64 = 2;
const DEFAULT_PROJECT_MARKERS: &[&str] = &[
    ".git",
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "CMakeLists.txt",
];

#[derive(Clone)]
pub struct ProjectRegistry {
    base_root: PathBuf,
    state_root: PathBuf,
    allowed_roots: Vec<PathBuf>,
    root_mappings: Vec<(PathBuf, PathBuf)>,
    discovery_max_depth: usize,
    project_markers: Vec<String>,
    default_project_id: String,
    usage_monitor: Arc<UsageMonitor>,
    shared_frontiers: Option<Arc<SharedFrontierCache>>,
    governed_lineages: HashMap<PathBuf, GovernedFrontierLineage>,
    blocking: Arc<BlockingCoordinator>,
    #[cfg(test)]
    blocking_hook: Arc<Mutex<Option<BlockingHook>>>,
    state: Arc<Mutex<RegistryState>>,
}

#[cfg(test)]
type BlockingHook = Arc<dyn Fn(&WorkControl) -> Result<()> + Send + Sync>;

struct BlockingCoordinator {
    limit: usize,
    permits: Arc<Semaphore>,
    request_timeout: Duration,
    cancellation_grace: Duration,
    active: AtomicUsize,
    queued: AtomicUsize,
    cancellations: AtomicUsize,
    timeouts: AtomicUsize,
    completed: AtomicUsize,
    active_peak: AtomicUsize,
    queued_peak: AtomicUsize,
    singleflight_waiters: AtomicUsize,
    singleflight_waiters_peak: AtomicUsize,
    queue_wait_micros: AtomicU64,
    engine_load_micros: AtomicU64,
    pack_micros: AtomicU64,
    total_micros: AtomicU64,
    post_timeout_micros: AtomicU64,
    deadline_remaining_ms_last: AtomicU64,
    warmup_state: AtomicU8,
    engine_builds: AtomicUsize,
}

struct CountGuard<'a>(&'a AtomicUsize);

impl Drop for CountGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CancelOnDrop {
    control: WorkControl,
    blocking: Arc<BlockingCoordinator>,
    finished: bool,
}

impl CancelOnDrop {
    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.control.cancel();
        if !self.finished {
            self.blocking.cancellations.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct RegistryState {
    clock: u64,
    specs: HashMap<String, ProjectSpec>,
    engines: HashMap<String, CachedEngine>,
    construction_gates: HashMap<String, Arc<Mutex<()>>>,
}

struct CachedEngine {
    engine: Arc<ProjectEngine>,
    last_used: u64,
}

#[derive(Clone)]
struct ProjectSpec {
    project_id: String,
    name: String,
    root_hash: String,
    local_root: PathBuf,
    state_root: PathBuf,
    source: String,
    mapped: bool,
    legacy: bool,
    governed_lineage: Option<GovernedFrontierLineage>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontierLineageManifest {
    schema: String,
    lineages: Vec<FrontierLineageManifestEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontierLineageManifestEntry {
    id: String,
    roots: Vec<PathBuf>,
}

impl ProjectRegistry {
    pub fn from_env(base_root: PathBuf, state_root: PathBuf) -> Result<Self> {
        let allowed_roots =
            split_env_list(&env::var("MCP_CONTEXT_ALLOWED_ROOTS").unwrap_or_default())
                .into_iter()
                .map(|value| allowed_root_path(&value))
                .collect::<Result<Vec<_>>>()?;
        let root_mappings =
            split_env_list(&env::var("MCP_CONTEXT_ROOT_MAPPINGS").unwrap_or_default())
                .into_iter()
                .filter_map(|mapping| {
                    mapping
                        .split_once('=')
                        .map(|(host, local)| (host.to_owned(), local.to_owned()))
                })
                .map(|(host, local)| Ok((allowed_root_path(&host)?, PathBuf::from(local))))
                .collect::<Result<Vec<_>>>()?;
        let mut governance_boundaries = allowed_roots.clone();
        governance_boundaries.push(base_root.clone());
        let governed_lineages = load_frontier_lineage_manifest(
            env::var_os("MCP_CONTEXT_FRONTIER_LINEAGES_FILE").map(PathBuf::from),
            &governance_boundaries,
        )?;
        Self::new_with_lineages(
            base_root,
            state_root,
            allowed_roots,
            root_mappings,
            governed_lineages,
        )
    }

    pub fn new(
        base_root: PathBuf,
        state_root: PathBuf,
        allowed_roots: Vec<PathBuf>,
        root_mappings: Vec<(PathBuf, PathBuf)>,
    ) -> Result<Self> {
        Self::new_with_lineages(
            base_root,
            state_root,
            allowed_roots,
            root_mappings,
            HashMap::new(),
        )
    }

    fn new_with_lineages(
        base_root: PathBuf,
        state_root: PathBuf,
        allowed_roots: Vec<PathBuf>,
        mut root_mappings: Vec<(PathBuf, PathBuf)>,
        governed_lineages: HashMap<PathBuf, GovernedFrontierLineage>,
    ) -> Result<Self> {
        let base_root = base_root
            .canonicalize()
            .context("canonicalize default repository")?;
        if !base_root.is_dir() {
            bail!("default repository root is not a directory");
        }
        root_mappings.sort_by_key(|mapping| std::cmp::Reverse(mapping.0.as_os_str().len()));
        let root_uri = canonical_file_uri(&base_root)?;
        let root_hash = sha256_hex(root_uri.as_bytes());
        let project_id = env::var("MCP_CONTEXT_PROJECT_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("legacy-{}", &root_hash[..12]));
        let default_spec = ProjectSpec {
            project_id: project_id.clone(),
            name: base_root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("project")
                .to_owned(),
            root_hash,
            local_root: base_root.clone(),
            state_root: state_root.clone(),
            source: "repo_path".to_owned(),
            mapped: false,
            legacy: true,
            governed_lineage: governed_lineages.get(&base_root).cloned(),
        };
        let mut specs = HashMap::new();
        specs.insert(project_id.clone(), default_spec);
        let usage_monitor = Arc::new(UsageMonitor::open(state_root.join("global-monitor"))?);
        let shared_frontiers = (!governed_lineages.is_empty())
            .then(|| SharedFrontierCache::open(state_root.join("global-frontiers")))
            .transpose()?
            .map(Arc::new);
        let blocking_limit = blocking_limit_from_env();
        let request_timeout = duration_from_env(
            "MCP_CONTEXT_REQUEST_TIMEOUT_SECS",
            DEFAULT_REQUEST_TIMEOUT_SECS,
        );
        let cancellation_grace = duration_from_env(
            "MCP_CONTEXT_CANCELLATION_GRACE_SECS",
            DEFAULT_CANCELLATION_GRACE_SECS,
        );
        if cancellation_grace >= request_timeout {
            bail!("MCP_CONTEXT_CANCELLATION_GRACE_SECS must be below the request timeout");
        }
        Ok(Self {
            base_root,
            state_root,
            allowed_roots,
            root_mappings,
            discovery_max_depth: discovery_max_depth_from_env(),
            project_markers: project_markers_from_env(),
            default_project_id: project_id,
            usage_monitor,
            shared_frontiers,
            governed_lineages,
            blocking: Arc::new(BlockingCoordinator {
                limit: blocking_limit,
                permits: Arc::new(Semaphore::new(blocking_limit)),
                request_timeout,
                cancellation_grace,
                active: AtomicUsize::new(0),
                queued: AtomicUsize::new(0),
                cancellations: AtomicUsize::new(0),
                timeouts: AtomicUsize::new(0),
                completed: AtomicUsize::new(0),
                active_peak: AtomicUsize::new(0),
                queued_peak: AtomicUsize::new(0),
                singleflight_waiters: AtomicUsize::new(0),
                singleflight_waiters_peak: AtomicUsize::new(0),
                queue_wait_micros: AtomicU64::new(0),
                engine_load_micros: AtomicU64::new(0),
                pack_micros: AtomicU64::new(0),
                total_micros: AtomicU64::new(0),
                post_timeout_micros: AtomicU64::new(0),
                deadline_remaining_ms_last: AtomicU64::new(0),
                warmup_state: AtomicU8::new(0),
                engine_builds: AtomicUsize::new(0),
            }),
            #[cfg(test)]
            blocking_hook: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(RegistryState {
                clock: 1,
                specs,
                engines: HashMap::new(),
                construction_gates: HashMap::new(),
            })),
        })
    }

    pub fn engine_for(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
    ) -> Result<Arc<ProjectEngine>> {
        let usage_monitor = Arc::clone(&self.usage_monitor);
        let shared_frontiers = self.shared_frontiers.clone();
        self.engine_for_with_builder(project_id, root_uri, move |spec| {
            match (&spec.governed_lineage, &shared_frontiers) {
                (Some(lineage), Some(shared)) => ProjectEngine::build_with_governed_frontiers(
                    &spec.local_root,
                    &spec.state_root,
                    &spec.project_id,
                    usage_monitor,
                    lineage.clone(),
                    Arc::clone(shared),
                ),
                _ => ProjectEngine::build_with_state_and_monitor(
                    &spec.local_root,
                    &spec.state_root,
                    &spec.project_id,
                    usage_monitor,
                ),
            }
        })
    }

    fn engine_for_controlled(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
        control: &WorkControl,
    ) -> Result<Arc<ProjectEngine>> {
        let usage_monitor = Arc::clone(&self.usage_monitor);
        let shared_frontiers = self.shared_frontiers.clone();
        let blocking = Arc::clone(&self.blocking);
        let control = control.clone();
        let singleflight_waiter = blocking.singleflight_waiters.fetch_add(1, Ordering::AcqRel) + 1;
        blocking
            .singleflight_waiters_peak
            .fetch_max(singleflight_waiter, Ordering::Relaxed);
        let _singleflight_waiter = CountGuard(&blocking.singleflight_waiters);
        let build_metrics = Arc::clone(&blocking);
        self.engine_for_with_builder(project_id, root_uri, move |spec| {
            control.check()?;
            let engine = match (&spec.governed_lineage, &shared_frontiers) {
                (Some(lineage), Some(shared)) => {
                    ProjectEngine::build_with_governed_frontiers_controlled(
                        &spec.local_root,
                        &spec.state_root,
                        &spec.project_id,
                        usage_monitor,
                        lineage.clone(),
                        Arc::clone(shared),
                        Some(&control),
                    )
                }
                _ => ProjectEngine::build_with_state_and_monitor_controlled(
                    &spec.local_root,
                    &spec.state_root,
                    &spec.project_id,
                    usage_monitor,
                    Some(&control),
                ),
            }?;
            build_metrics.engine_builds.fetch_add(1, Ordering::Relaxed);
            Ok(engine)
        })
    }

    pub async fn context_pack_bounded(
        self: &Arc<Self>,
        request: ContextPackRequest,
    ) -> Result<Vec<u8>> {
        self.context_pack_bounded_inner(request, false).await
    }

    async fn context_pack_bounded_inner(
        self: &Arc<Self>,
        mut request: ContextPackRequest,
        warmup: bool,
    ) -> Result<Vec<u8>> {
        let started = Instant::now();
        let work_budget = self
            .blocking
            .request_timeout
            .saturating_sub(self.blocking.cancellation_grace);
        let work_deadline = started + work_budget;
        let control = WorkControl::new(work_deadline);
        let mut cancel_on_drop = CancelOnDrop {
            control: control.clone(),
            blocking: Arc::clone(&self.blocking),
            finished: false,
        };

        let queue_started = Instant::now();
        let queued_now = self.blocking.queued.fetch_add(1, Ordering::AcqRel) + 1;
        self.blocking
            .queued_peak
            .fetch_max(queued_now, Ordering::Relaxed);
        let queued = CountGuard(&self.blocking.queued);
        let permit = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(work_deadline),
            Arc::clone(&self.blocking.permits).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.blocking.queue_wait_micros.fetch_add(
                    u64::try_from(queue_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                self.blocking.total_micros.fetch_add(
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                cancel_on_drop.finish();
                return Err(anyhow!("context_pack blocking executor is unavailable"));
            }
            Err(_) => {
                self.blocking.timeouts.fetch_add(1, Ordering::Relaxed);
                self.blocking.queue_wait_micros.fetch_add(
                    u64::try_from(queue_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                self.blocking.total_micros.fetch_add(
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                self.blocking
                    .deadline_remaining_ms_last
                    .store(0, Ordering::Relaxed);
                return Err(anyhow!(
                    "context_pack busy; retry after warmup or when queued work completes"
                ));
            }
        };
        drop(queued);
        if warmup {
            self.blocking.warmup_state.store(2, Ordering::Release);
        }
        self.blocking.queue_wait_micros.fetch_add(
            u64::try_from(queue_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if control.check().is_err() {
            self.blocking.timeouts.fetch_add(1, Ordering::Relaxed);
            self.blocking.total_micros.fetch_add(
                u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.blocking
                .deadline_remaining_ms_last
                .store(0, Ordering::Relaxed);
            return Err(anyhow!(
                "context_pack busy; retry after warmup or when queued work completes"
            ));
        }

        let registry = Arc::clone(self);
        let job_control = control.clone();
        let blocking = Arc::clone(&self.blocking);
        #[cfg(test)]
        let blocking_hook = self
            .blocking_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let active = blocking.active.fetch_add(1, Ordering::AcqRel) + 1;
            blocking.active_peak.fetch_max(active, Ordering::Relaxed);
            let _active = CountGuard(&blocking.active);
            job_control.check()?;
            let engine_started = Instant::now();
            let engine = registry.engine_for_controlled(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
                &job_control,
            )?;
            blocking.engine_load_micros.fetch_add(
                u64::try_from(engine_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            job_control.check()?;
            #[cfg(test)]
            if let Some(hook) = blocking_hook {
                hook(&job_control)?;
            }
            request.project_id = Some(engine.project_id().to_owned());
            request.root_uri = None;
            let pack_started = Instant::now();
            let result = tokio::runtime::Handle::current()
                .block_on(engine.context_pack_cached_controlled(&request, Some(&job_control)));
            blocking.pack_micros.fetch_add(
                u64::try_from(pack_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            result
        });

        let result = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(work_deadline),
            &mut job,
        )
        .await
        {
            Ok(Ok(result)) => match result {
                Ok(response) => {
                    self.blocking.completed.fetch_add(1, Ordering::Relaxed);
                    Ok(response)
                }
                Err(error) if error.to_string() == "repository work cancelled before commit" => {
                    self.blocking.timeouts.fetch_add(1, Ordering::Relaxed);
                    self.blocking.cancellations.fetch_add(1, Ordering::Relaxed);
                    Err(anyhow!(
                        "context_pack timed out before the server deadline; retry after warmup"
                    ))
                }
                Err(error) => Err(error),
            },
            Ok(Err(error)) => Err(anyhow!("context_pack blocking job failed: {error}")),
            Err(_) => {
                self.blocking.timeouts.fetch_add(1, Ordering::Relaxed);
                self.blocking.cancellations.fetch_add(1, Ordering::Relaxed);
                control.cancel();
                let timeout_started = Instant::now();
                let result = match tokio::time::timeout(self.blocking.cancellation_grace, &mut job)
                    .await
                {
                    Ok(result) => {
                        let _ = result;
                        Err(anyhow!(
                            "context_pack timed out before the server deadline; retry after warmup"
                        ))
                    }
                    Err(_) => {
                        // A running `spawn_blocking` closure cannot be aborted. Keep the
                        // request attached until its drop guards release the global permit
                        // and active-job counter; returning here would abandon capacity and
                        // could starve every queued request.
                        let _ = (&mut job).await;
                        Err(anyhow!(
                            "context_pack cancellation grace expired; retry after warmup"
                        ))
                    }
                };
                self.blocking.post_timeout_micros.fetch_add(
                    u64::try_from(timeout_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                result
            }
        };
        self.blocking.total_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.blocking.deadline_remaining_ms_last.store(
            u64::try_from(
                work_deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        cancel_on_drop.finish();
        result
    }

    pub fn runtime_status(&self) -> Value {
        let warmup = match self.blocking.warmup_state.load(Ordering::Acquire) {
            1 => "queued",
            2 => "building",
            3 => "ready",
            4 => "failed",
            _ => "not_started",
        };
        json!({
            "schema": "context_runtime.status.v1",
            "blocking": {
                "limit": self.blocking.limit,
                "active": self.blocking.active.load(Ordering::Acquire),
                "active_peak": self.blocking.active_peak.load(Ordering::Relaxed),
                "queued": self.blocking.queued.load(Ordering::Acquire),
                "queued_peak": self.blocking.queued_peak.load(Ordering::Relaxed),
                "singleflight_waiters": self.blocking.singleflight_waiters.load(Ordering::Acquire),
                "singleflight_waiters_peak": self.blocking.singleflight_waiters_peak.load(Ordering::Relaxed),
                "completed": self.blocking.completed.load(Ordering::Relaxed),
                "engine_builds": self.blocking.engine_builds.load(Ordering::Relaxed),
                "cancellations": self.blocking.cancellations.load(Ordering::Relaxed),
                "timeouts": self.blocking.timeouts.load(Ordering::Relaxed),
                "queue_wait_micros_total": self.blocking.queue_wait_micros.load(Ordering::Relaxed),
                "engine_load_micros_total": self.blocking.engine_load_micros.load(Ordering::Relaxed),
                "pack_micros_total": self.blocking.pack_micros.load(Ordering::Relaxed),
                "total_micros": self.blocking.total_micros.load(Ordering::Relaxed),
                "post_timeout_micros_total": self.blocking.post_timeout_micros.load(Ordering::Relaxed),
                "deadline_remaining_ms_last": self.blocking.deadline_remaining_ms_last.load(Ordering::Relaxed),
                "request_timeout_ms": self.blocking.request_timeout.as_millis(),
                "cancellation_grace_ms": self.blocking.cancellation_grace.as_millis()
            },
            "warmup": {"state": warmup}
        })
    }

    pub fn start_post_readiness_warmup(self: &Arc<Self>) {
        if self
            .blocking
            .warmup_state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let request = ContextPackRequest {
                prompt: "repository warmup".to_owned(),
                changed_files: Vec::new(),
                focus_paths: Vec::new(),
                memory_session: None,
                client_profile: None,
                model_profile: None,
                project_id: None,
                root_uri: None,
                max_items: 1,
                max_source_tokens: 0,
                evidence_policy: Default::default(),
                cache_strategy: Default::default(),
                base_pack: None,
                known_evidence: Vec::new(),
            };
            let state = if registry
                .context_pack_bounded_inner(request, true)
                .await
                .is_ok()
            {
                3
            } else {
                4
            };
            registry
                .blocking
                .warmup_state
                .store(state, Ordering::Release);
        });
    }

    #[cfg(test)]
    pub(crate) fn blocking_snapshot(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.blocking.permits.available_permits(),
            self.blocking.active.load(Ordering::Acquire),
            self.blocking.queued.load(Ordering::Acquire),
            self.blocking.cancellations.load(Ordering::Relaxed),
            self.blocking.timeouts.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_blocking_configuration(
        mut self,
        limit: usize,
        request_timeout: Duration,
        cancellation_grace: Duration,
    ) -> Self {
        self.blocking = Arc::new(BlockingCoordinator {
            limit: limit.max(1),
            permits: Arc::new(Semaphore::new(limit.max(1))),
            request_timeout,
            cancellation_grace,
            active: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            cancellations: AtomicUsize::new(0),
            timeouts: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            active_peak: AtomicUsize::new(0),
            queued_peak: AtomicUsize::new(0),
            singleflight_waiters: AtomicUsize::new(0),
            singleflight_waiters_peak: AtomicUsize::new(0),
            queue_wait_micros: AtomicU64::new(0),
            engine_load_micros: AtomicU64::new(0),
            pack_micros: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
            post_timeout_micros: AtomicU64::new(0),
            deadline_remaining_ms_last: AtomicU64::new(0),
            warmup_state: AtomicU8::new(0),
            engine_builds: AtomicUsize::new(0),
        });
        self
    }

    #[cfg(test)]
    pub(crate) async fn hold_blocking_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&self.blocking.permits)
            .acquire_owned()
            .await
            .expect("test semaphore remains open")
    }

    #[cfg(test)]
    pub(crate) fn set_blocking_hook(&self, hook: BlockingHook) {
        *self
            .blocking_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
    }

    pub fn monitor_usage(&self, action: &str, project_id: Option<&str>) -> Result<Value> {
        self.usage_monitor.action(action, project_id)
    }

    pub fn bounded_monitor_usage(&self, request: &ContextAdminRequest) -> Result<Value> {
        let reference_project_id = request
            .project_id
            .as_deref()
            .unwrap_or(&self.default_project_id);
        self.usage_monitor.bounded_action(
            &request.action,
            request.project_id.as_deref(),
            reference_project_id,
            request.max_entries,
            request.max_output_chars,
        )
    }

    pub fn result_reference_resolve(&self, request: &ResultReferenceRequest) -> Result<Vec<u8>> {
        let engine = self.engine_for(request.project_id.as_deref(), request.root_uri.as_deref())?;
        let mut scoped = request.clone();
        scoped.project_id = Some(engine.project_id().to_owned());
        scoped.root_uri = None;
        if let Some(encoded) = self
            .usage_monitor
            .resolve_reference(&scoped, engine.project_id())?
        {
            return Ok(encoded);
        }
        engine.result_reference_resolve(&scoped)
    }

    pub fn record_context_pack_rejection(&self, class: ContextPackRejectionClass) {
        let _ = self.usage_monitor.record_rejection(class);
    }

    fn engine_for_with_builder<F>(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
        builder: F,
    ) -> Result<Arc<ProjectEngine>>
    where
        F: FnOnce(&ProjectSpec) -> Result<ProjectEngine>,
    {
        self.engine_for_with_builder_inner(project_id, root_uri, builder, || {})
    }

    #[cfg(test)]
    fn engine_for_with_builder_and_hook<F, H>(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
        builder: F,
        before_gate: H,
    ) -> Result<Arc<ProjectEngine>>
    where
        F: FnOnce(&ProjectSpec) -> Result<ProjectEngine>,
        H: FnOnce(),
    {
        self.engine_for_with_builder_inner(project_id, root_uri, builder, before_gate)
    }

    fn engine_for_with_builder_inner<F, H>(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
        builder: F,
        before_gate: H,
    ) -> Result<Arc<ProjectEngine>>
    where
        F: FnOnce(&ProjectSpec) -> Result<ProjectEngine>,
        H: FnOnce(),
    {
        let selected = self.select_project(project_id, root_uri)?;

        if let Some(engine) = self.cached_engine(&selected)? {
            return Ok(engine);
        }
        if !self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?
            .specs
            .contains_key(&selected)
        {
            self.discover_projects()?;
        }
        let (spec, gate) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?;
            let spec = state
                .specs
                .get(&selected)
                .cloned()
                .ok_or_else(|| anyhow!("unknown project_id"))?;
            let gate = Arc::clone(
                state
                    .construction_gates
                    .entry(selected.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            );
            prune_construction_gates(&mut state, &selected);
            (spec, gate)
        };
        before_gate();
        let _construction = gate
            .lock()
            .map_err(|_| anyhow!("project construction gate poisoned"))?;
        if let Some(engine) = self.cached_engine(&selected)? {
            return Ok(engine);
        }
        let engine = Arc::new(builder(&spec)?);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        state.clock += 1;
        let clock = state.clock;
        state.engines.insert(
            selected,
            CachedEngine {
                engine: Arc::clone(&engine),
                last_used: clock,
            },
        );
        prune_idle_engines(&mut state, &self.default_project_id);
        Ok(engine)
    }

    /// Resolves an administrative selector without opening state or an index.
    ///
    /// Metrics consumers use this path so inspecting an unloaded project cannot
    /// turn a dashboard refresh into an indexing job.
    pub fn cached_engine_for(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
    ) -> Result<(String, Option<Arc<ProjectEngine>>, bool)> {
        let selected = self.select_project(project_id, root_uri)?;
        let known = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?
            .specs
            .contains_key(&selected);
        // Metrics are a constant-time snapshot path. In particular, an
        // unknown selector must not trigger a workspace walk just to decide
        // whether an index should be opened.
        let engine = if known {
            self.cached_engine(&selected)?
        } else {
            None
        };
        Ok((selected, engine, known))
    }

    pub fn projects_payload(&self) -> Result<Value> {
        self.discover_projects()?;
        let payload = self.project_payload(false)?;
        self.persist_project_catalog(&payload);
        Ok(payload)
    }

    /// Returns only engines that are already resident in this process.
    ///
    /// Unlike project discovery, this is an in-memory snapshot and is safe to
    /// call from a frequent metrics poll.
    pub fn active_projects_payload(&self) -> Result<Value> {
        self.project_payload(true)
    }

    /// Lists a manifest written by explicit project discovery, without walking
    /// an allowed root. When no manifest exists, return resident engines only.
    pub fn cached_projects_payload(&self) -> Result<Value> {
        if let Ok(bytes) = std::fs::read(self.state_root.join(PROJECT_CATALOG_FILE))
            && let Ok(mut payload) = serde_json::from_slice::<Value>(&bytes)
            && payload.get("schema").and_then(Value::as_str) == Some("context_projects.list.v1")
            && payload.get("projects").and_then(Value::as_array).is_some()
        {
            payload["schema"] = Value::String("context_projects.cached.v1".to_owned());
            payload["catalogue"] = Value::String("persisted_discovery".to_owned());
            return Ok(payload);
        }
        let mut payload = self.project_payload(true)?;
        payload["schema"] = Value::String("context_projects.cached.v1".to_owned());
        payload["catalogue"] = Value::String("resident_engines_only".to_owned());
        Ok(payload)
    }

    fn persist_project_catalog(&self, payload: &Value) {
        let Ok(bytes) = serde_json::to_vec(payload) else {
            return;
        };
        let target = self.state_root.join(PROJECT_CATALOG_FILE);
        let temporary = self.state_root.join(format!(".{PROJECT_CATALOG_FILE}.tmp"));
        if std::fs::write(&temporary, bytes).is_ok() {
            let _ = std::fs::rename(temporary, target);
        }
    }

    fn project_payload(&self, active_only: bool) -> Result<Value> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        let mut projects = state
            .specs
            .values()
            // Python keeps this engine-only fallback out of the discovered
            // project list.  Otherwise a workspace parent appears alongside
            // the real projects found below it.
            .filter(|spec| !spec.legacy || active_only)
            .filter(|spec| !active_only || state.engines.contains_key(&spec.project_id))
            .map(ProjectSpec::public_metadata)
            .collect::<Vec<_>>();
        projects.sort_by(|left, right| {
            left.get("project_id")
                .and_then(Value::as_str)
                .cmp(&right.get("project_id").and_then(Value::as_str))
        });
        Ok(json!({
            "schema": if active_only {"context_projects.active.v1"} else {"context_projects.list.v1"},
            "count": projects.len(),
            "projects": projects,
            "selection": {
                "default": "explicit_selector_or_legacy_repo_path",
                "legacy_repo_path_fallback": true,
                "project_selection_required": false,
                "ambiguous_without_project": false,
                "legacy_fallback": {"safe": true, "reason": "configured_default_project"},
            },
        }))
    }

    pub fn default_engine(&self) -> Result<Arc<ProjectEngine>> {
        self.engine_for(None, None)
    }

    fn cached_engine(&self, project_id: &str) -> Result<Option<Arc<ProjectEngine>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        state.clock += 1;
        let clock = state.clock;
        if let Some(entry) = state.engines.get_mut(project_id) {
            entry.last_used = clock;
            return Ok(Some(Arc::clone(&entry.engine)));
        }
        Ok(None)
    }

    fn select_project(&self, project_id: Option<&str>, root_uri: Option<&str>) -> Result<String> {
        if let Some(root_uri) = root_uri {
            let spec = self.project_from_uri(root_uri, "root_uri")?;
            if let Some(project_id) = project_id
                && project_id != spec.project_id
            {
                bail!("project_id and root_uri select different projects");
            }
            let selected = spec.project_id.clone();
            self.state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?
                .specs
                .insert(selected.clone(), spec);
            Ok(selected)
        } else {
            Ok(project_id.unwrap_or(&self.default_project_id).to_owned())
        }
    }

    fn project_from_uri(&self, raw_uri: &str, source: &str) -> Result<ProjectSpec> {
        let uri = if raw_uri.contains("://") {
            Url::parse(raw_uri)?
        } else {
            Url::from_file_path(raw_uri)
                .map_err(|_| anyhow!("root_uri must be an absolute file URI"))?
        };
        if uri.scheme() != "file" || uri.host_str().is_some_and(|host| !host.is_empty()) {
            bail!("only local file:// repository roots are supported");
        }
        let host_root = uri
            .to_file_path()
            .map_err(|_| anyhow!("root_uri is not a valid local file URI"))?;
        self.ensure_allowed(&host_root)?;
        let (mapped_root, mapped) = self.map_host_path(&host_root);
        if mapped_root.symlink_metadata()?.file_type().is_symlink() {
            bail!("project root must not be a symlink");
        }
        let local_root = mapped_root
            .canonicalize()
            .context("project root is not readable after mapping")?;
        if !local_root.is_dir() {
            bail!("project root must be a directory");
        }
        let local_boundaries = if self.allowed_roots.is_empty() {
            vec![self.base_root.clone()]
        } else {
            self.allowed_roots
                .iter()
                .filter_map(|allowed| {
                    let (mapped, _) = self.map_host_path(allowed);
                    mapped.canonicalize().ok()
                })
                .collect::<Vec<_>>()
        };
        if !local_boundaries
            .iter()
            .any(|allowed| local_root == *allowed || local_root.starts_with(allowed))
        {
            bail!("mapped project root escapes its configured local boundary");
        }
        let canonical_uri = canonical_file_uri(&host_root)?;
        let root_hash = sha256_hex(canonical_uri.as_bytes());
        let name = host_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("project")
            .to_owned();
        let project_id = format!("{}-{}", slug(&name), &root_hash[..12]);
        Ok(ProjectSpec {
            state_root: self.state_root.join("projects").join(&project_id),
            project_id,
            name,
            root_hash,
            local_root,
            source: source.to_owned(),
            mapped,
            legacy: false,
            governed_lineage: self.governed_lineages.get(&host_root).cloned(),
        })
    }

    fn ensure_allowed(&self, host_root: &Path) -> Result<()> {
        if self.allowed_roots.is_empty() {
            if host_root == self.base_root {
                return Ok(());
            }
            bail!("MCP_CONTEXT_ALLOWED_ROOTS is required for roots outside REPO_PATH");
        }
        if self
            .allowed_roots
            .iter()
            .any(|allowed| host_root == allowed || host_root.starts_with(allowed))
        {
            Ok(())
        } else {
            bail!("MCP root is outside MCP_CONTEXT_ALLOWED_ROOTS")
        }
    }

    fn map_host_path(&self, host_root: &Path) -> (PathBuf, bool) {
        for (host_prefix, local_prefix) in &self.root_mappings {
            if let Ok(relative) = host_root.strip_prefix(host_prefix) {
                return (local_prefix.join(relative), true);
            }
        }
        (host_root.to_owned(), false)
    }

    fn discover_projects(&self) -> Result<()> {
        let scan_roots = if self.allowed_roots.is_empty() {
            vec![(self.base_root.clone(), self.base_root.clone())]
        } else {
            self.allowed_roots
                .iter()
                .map(|host| {
                    let (local, _) = self.map_host_path(host);
                    (host.clone(), local)
                })
                .collect::<Vec<_>>()
        };
        let mut discovered = Vec::new();
        for (host_root, local_root) in scan_roots {
            if !local_root.is_dir() {
                continue;
            }
            let skip_root = self.should_skip_scan_root_project(&local_root);
            let mut entries = WalkDir::new(&local_root)
                .follow_links(false)
                .max_depth(self.discovery_max_depth)
                .sort_by_file_name()
                .into_iter();
            while let Some(entry) = entries.next() {
                let Ok(entry) = entry else {
                    continue;
                };
                if !discovery_entry(&entry) {
                    if entry.file_type().is_dir() {
                        entries.skip_current_dir();
                    }
                    continue;
                }
                if !entry.file_type().is_dir() {
                    continue;
                }
                let Some(source) = self.project_candidate_source(entry.path()) else {
                    continue;
                };
                if entry.depth() == 0 && skip_root {
                    continue;
                }
                let Ok(relative) = entry.path().strip_prefix(&local_root) else {
                    entries.skip_current_dir();
                    continue;
                };
                let host_path = host_root.join(relative);
                let Ok(uri) = canonical_file_uri(&host_path) else {
                    entries.skip_current_dir();
                    continue;
                };
                if let Ok(spec) = self.project_from_uri(&uri, source) {
                    discovered.push(spec);
                    if discovered.len() == DISCOVERY_MAX_PROJECTS {
                        break;
                    }
                }
                // A marker directory owns its descendants.  Without this,
                // nested source and solution folders become phantom projects.
                entries.skip_current_dir();
            }
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        // Python rebuilds discovery results for every list operation.  Keep
        // explicit root selections and the legacy fallback, but replace prior
        // discovery rows so renamed/removed projects and old false positives
        // do not remain visible forever.
        state
            .specs
            .retain(|_, spec| spec.legacy || !spec.source.starts_with("discovered"));
        for spec in discovered {
            state.specs.entry(spec.project_id.clone()).or_insert(spec);
        }
        Ok(())
    }

    #[cfg(test)]
    fn cached_project_ids(&self) -> Result<Vec<String>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        Ok(state.engines.keys().cloned().collect())
    }

    fn should_skip_scan_root_project(&self, root: &Path) -> bool {
        !self.allowed_roots.is_empty()
            && !self.root_mappings.is_empty()
            && !self.has_non_git_project_marker(root)
            && self.has_child_project_candidate(root)
    }

    fn has_child_project_candidate(&self, root: &Path) -> bool {
        WalkDir::new(root)
            .follow_links(false)
            .min_depth(1)
            .max_depth(self.discovery_max_depth)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(discovery_entry)
            .filter_map(std::result::Result::ok)
            .any(|entry| {
                entry.file_type().is_dir() && self.project_candidate_source(entry.path()).is_some()
            })
    }

    fn project_candidate_source(&self, path: &Path) -> Option<&'static str> {
        if path.join(".git").exists() {
            Some("discovered_git")
        } else if self.has_project_marker(path) {
            Some("discovered_marker")
        } else {
            None
        }
    }

    fn has_non_git_project_marker(&self, path: &Path) -> bool {
        self.project_markers
            .iter()
            .any(|marker| marker != ".git" && path.join(marker).exists())
    }

    fn has_project_marker(&self, path: &Path) -> bool {
        self.project_markers
            .iter()
            .any(|marker| path.join(marker).exists())
    }
}

fn prune_construction_gates(state: &mut RegistryState, selected: &str) {
    while state.construction_gates.len() > DISCOVERY_MAX_PROJECTS {
        let candidate = state
            .construction_gates
            .iter()
            .find(|(project_id, gate)| {
                project_id.as_str() != selected && Arc::strong_count(gate) == 1
            })
            .map(|(project_id, _)| project_id.clone());
        let Some(project_id) = candidate else {
            break;
        };
        state.construction_gates.remove(&project_id);
    }
}

impl ProjectSpec {
    fn public_metadata(&self) -> Value {
        json!({
            "schema": "context_project.v1",
            "project_id": self.project_id,
            "name": self.name,
            "source": self.source,
            "root": {"uri_hash": self.root_hash, "scheme": "file", "mapped": self.mapped},
            "state": {
                "state_key": if self.legacy {"rust-v2".to_owned()} else {format!("projects/{}", self.project_id)},
                "exists": self.state_root.exists(),
                "store_exists": self.state_root.join("rust-v2/state.lmdb/data.mdb").exists(),
                "index_exists": self.state_root.join("rust-v2/index").exists(),
                "memory_exists": self.state_root.join("rust-v2/state.lmdb/data.mdb").exists(),
                "cache_exists": self.state_root.join("rust-v2/state.lmdb/data.mdb").exists(),
                "repo_boundary_enforced": true,
            },
            "frontier_lineage": {
                "governed": self.governed_lineage.is_some(),
                "identity": self.governed_lineage.as_ref().map(GovernedFrontierLineage::identity),
                "request_configurable": false,
            },
            "git": {"is_repo": self.local_root.join(".git").exists(), "available": false, "head": "", "branch": "", "status_hash": "", "changes_hash": "", "dirty": false},
        })
    }
}

fn prune_idle_engines(state: &mut RegistryState, default_project_id: &str) {
    while state.engines.len() > ENGINE_CAPACITY {
        let candidate = state
            .engines
            .iter()
            .filter(|(project_id, entry)| {
                project_id.as_str() != default_project_id && Arc::strong_count(&entry.engine) == 1
            })
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(project_id, _)| project_id.clone());
        let Some(project_id) = candidate else {
            break;
        };
        state.engines.remove(&project_id);
    }
}

fn load_frontier_lineage_manifest(
    manifest_path: Option<PathBuf>,
    allowed_roots: &[PathBuf],
) -> Result<HashMap<PathBuf, GovernedFrontierLineage>> {
    let Some(manifest_path) = manifest_path else {
        return Ok(HashMap::new());
    };
    if !manifest_path.is_absolute() {
        bail!("MCP_CONTEXT_FRONTIER_LINEAGES_FILE must be absolute");
    }
    let metadata = manifest_path
        .symlink_metadata()
        .context("stat governed frontier lineage manifest")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 256 * 1024 {
        bail!("governed frontier lineage manifest must be a regular file at most 256 KiB");
    }
    let manifest: FrontierLineageManifest = serde_json::from_slice(
        &std::fs::read(&manifest_path).context("read governed frontier lineage manifest")?,
    )
    .context("parse governed frontier lineage manifest")?;
    if manifest.schema != "context_frontier_lineages.v1" {
        bail!("unsupported governed frontier lineage manifest schema");
    }
    let mut governed = HashMap::new();
    for entry in manifest.lineages {
        if entry.id.is_empty()
            || entry.id.len() > 128
            || !entry
                .id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            bail!("governed frontier lineage id is invalid");
        }
        if entry.roots.len() < 2 || entry.roots.len() > 64 {
            bail!("a governed frontier lineage must contain 2..64 explicit roots");
        }
        let mut roots = Vec::new();
        for root in entry.roots {
            if !root.is_absolute()
                || root
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                bail!("governed frontier lineage roots must be absolute and normalized");
            }
            if !allowed_roots
                .iter()
                .any(|allowed| root == *allowed || root.starts_with(allowed))
            {
                bail!("governed frontier lineage root is outside configured boundaries");
            }
            roots.push(root);
        }
        roots.sort();
        roots.dedup();
        if roots.len() < 2 {
            bail!("governed frontier lineage roots must be distinct");
        }
        let identity = sha256_hex(&serde_json::to_vec(
            &json!({"id": entry.id, "roots": roots}),
        )?);
        let lineage = GovernedFrontierLineage::new(identity)?;
        for root in roots {
            if governed.insert(root, lineage.clone()).is_some() {
                bail!("a root may belong to only one governed frontier lineage");
            }
        }
    }
    Ok(governed)
}

fn split_env_list(value: &str) -> Vec<String> {
    value
        .split([',', ':'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn allowed_root_path(value: &str) -> Result<PathBuf> {
    if value.starts_with("file://") {
        return Url::parse(value)?
            .to_file_path()
            .map_err(|_| anyhow!("allowed root is not a local file URI"));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("allowed roots and mappings must use absolute paths");
    }
    Ok(path)
}

fn canonical_file_uri(path: &Path) -> Result<String> {
    Url::from_directory_path(path)
        .map(|uri| uri.to_string().trim_end_matches('/').to_owned())
        .map_err(|_| anyhow!("path cannot be represented as a file URI"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn slug(value: &str) -> String {
    let mut slug = value
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    while slug.contains("--") {
        slug = slug.replace("--", "-");
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "project".to_owned()
    } else {
        slug.chars().take(40).collect()
    }
}

fn discovery_entry(entry: &DirEntry) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    if entry.file_type().is_symlink() {
        return false;
    }
    !entry.file_type().is_dir()
        || !is_ignored_repository_path(std::path::Path::new(entry.file_name()))
}

fn blocking_limit_from_env() -> usize {
    env::var("MCP_CONTEXT_BLOCKING_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .saturating_div(2)
                .clamp(1, 2)
        })
}

fn duration_from_env(name: &str, default_seconds: u64) -> Duration {
    Duration::from_secs(
        env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default_seconds),
    )
}

fn discovery_max_depth_from_env() -> usize {
    env::var("MCP_CONTEXT_PROJECT_DISCOVERY_MAX_DEPTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_DISCOVERY_MAX_DEPTH)
}

fn project_markers_from_env() -> Vec<String> {
    let configured = env::var("MCP_CONTEXT_PROJECT_MARKERS").unwrap_or_default();
    let markers = configured
        .split([',', ':'])
        .map(str::trim)
        .filter(|marker| !marker.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if markers.is_empty() {
        DEFAULT_PROJECT_MARKERS
            .iter()
            .map(|marker| (*marker).to_owned())
            .collect()
    } else {
        markers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn same_project_construction_is_singleflight_and_retains_one_arc() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").expect("project marker");
        let registry = Arc::new(
            ProjectRegistry::new(
                root.path().to_owned(),
                state.path().to_owned(),
                Vec::new(),
                Vec::new(),
            )
            .expect("registry"),
        );
        let past_first_miss = Arc::new(Barrier::new(2));
        let builds = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let registry = Arc::clone(&registry);
            let past_first_miss = Arc::clone(&past_first_miss);
            let builds = Arc::clone(&builds);
            threads.push(std::thread::spawn(move || {
                registry
                    .engine_for_with_builder_and_hook(
                        None,
                        None,
                        |spec| {
                            builds.fetch_add(1, Ordering::SeqCst);
                            ProjectEngine::build_with_state(
                                &spec.local_root,
                                &spec.state_root,
                                &spec.project_id,
                            )
                        },
                        || {
                            past_first_miss.wait();
                        },
                    )
                    .expect("singleflight engine")
            }));
        }
        let first = threads.remove(0).join().expect("first thread");
        let second = threads.remove(0).join().expect("second thread");

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn different_projects_construct_concurrently() {
        let root = tempfile::tempdir().expect("workspace root");
        let state = tempfile::tempdir().expect("state root");
        let default = root.path().join("default");
        let first_root = root.path().join("first");
        let second_root = root.path().join("second");
        for repository in [&default, &first_root, &second_root] {
            std::fs::create_dir_all(repository).expect("repository directory");
            std::fs::write(repository.join("Cargo.toml"), "[workspace]\n").expect("project marker");
        }
        let registry = Arc::new(
            ProjectRegistry::new(
                default,
                state.path().to_owned(),
                vec![root.path().to_owned()],
                Vec::new(),
            )
            .expect("registry"),
        );
        let first_uri = canonical_file_uri(&first_root).expect("first URI");
        let second_uri = canonical_file_uri(&second_root).expect("second URI");
        let builders_ready = Arc::new(Barrier::new(2));
        let mut threads = Vec::new();
        for uri in [first_uri, second_uri] {
            let registry = Arc::clone(&registry);
            let builders_ready = Arc::clone(&builders_ready);
            threads.push(std::thread::spawn(move || {
                registry
                    .engine_for_with_builder(None, Some(&uri), |spec| {
                        builders_ready.wait();
                        ProjectEngine::build_with_state(
                            &spec.local_root,
                            &spec.state_root,
                            &spec.project_id,
                        )
                    })
                    .expect("parallel engine")
            }));
        }
        let first = threads.remove(0).join().expect("first thread");
        let second = threads.remove(0).join().expect("second thread");
        assert_ne!(first.project_id(), second.project_id());
    }

    #[test]
    fn registry_routes_allowed_projects_and_keeps_in_flight_engine_alive() {
        let root = tempfile::tempdir().expect("workspace root");
        let state = tempfile::tempdir().expect("state root");
        let mut repositories = Vec::new();
        for index in 0..10 {
            let repository = root.path().join(format!("repo-{index}"));
            std::fs::create_dir_all(&repository).expect("repository directory");
            std::fs::write(
                repository.join("Cargo.toml"),
                "[package]\nname='fixture'\nversion='0.1.0'\n",
            )
            .expect("project marker");
            repositories.push(repository);
        }
        let registry = ProjectRegistry::new(
            repositories[0].clone(),
            state.path().to_owned(),
            vec![root.path().to_owned()],
            Vec::new(),
        )
        .expect("registry");
        let held_uri = canonical_file_uri(&repositories[1]).expect("held URI");
        let held = registry
            .engine_for(None, Some(&held_uri))
            .expect("held engine");
        let held_id = held.project_id().to_owned();
        for repository in repositories.iter().skip(2) {
            let uri = canonical_file_uri(repository).expect("project URI");
            drop(
                registry
                    .engine_for(None, Some(&uri))
                    .expect("routed engine"),
            );
        }
        let cached = registry.cached_project_ids().expect("cached ids");
        assert!(cached.contains(&held_id));
        assert!(cached.len() <= ENGINE_CAPACITY + 1);
        drop(held);
        drop(registry.default_engine().expect("prune trigger"));
        assert!(registry.cached_project_ids().expect("pruned ids").len() <= ENGINE_CAPACITY);

        let outside = tempfile::tempdir().expect("outside root");
        std::fs::write(outside.path().join("Cargo.toml"), "[workspace]\n").expect("outside marker");
        let outside_uri = canonical_file_uri(outside.path()).expect("outside URI");
        assert!(registry.engine_for(None, Some(&outside_uri)).is_err());
    }

    #[test]
    fn metrics_resolution_does_not_open_the_default_engine() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").expect("project marker");

        let registry = ProjectRegistry::new(
            root.path().to_owned(),
            state.path().to_owned(),
            Vec::new(),
            Vec::new(),
        )
        .expect("registry");
        assert!(
            registry
                .cached_project_ids()
                .expect("cached projects")
                .is_empty()
        );

        let (project_id, engine, known) = registry
            .cached_engine_for(None, None)
            .expect("read-only default selection");
        assert!(project_id.starts_with("legacy-"));
        assert!(known);
        assert!(engine.is_none());
        assert!(
            registry
                .cached_project_ids()
                .expect("cached projects")
                .is_empty()
        );

        drop(registry.default_engine().expect("active project engine"));
        assert_eq!(
            registry
                .cached_project_ids()
                .expect("cached projects")
                .len(),
            1
        );
    }

    #[test]
    fn unknown_metrics_selector_never_triggers_project_discovery() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        let child = root.path().join("unrelated-project");
        std::fs::create_dir_all(&child).expect("child project");
        std::fs::write(child.join("Cargo.toml"), "[workspace]\n").expect("project marker");
        let registry = ProjectRegistry::new(
            root.path().to_owned(),
            state.path().to_owned(),
            vec![root.path().to_owned()],
            Vec::new(),
        )
        .expect("registry");

        let (project_id, engine, known) = registry
            .cached_engine_for(Some("unknown-project"), None)
            .expect("read-only unknown selection");
        assert_eq!(project_id, "unknown-project");
        assert!(!known);
        assert!(engine.is_none());
        assert!(
            registry
                .cached_project_ids()
                .expect("cached projects")
                .is_empty()
        );
    }

    #[test]
    fn active_projects_payload_only_reports_resident_engines() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        let child = root.path().join("undiscovered-project");
        std::fs::create_dir_all(&child).expect("child project");
        std::fs::write(child.join("Cargo.toml"), "[workspace]\n").expect("project marker");
        let registry = ProjectRegistry::new(
            root.path().to_owned(),
            state.path().to_owned(),
            vec![root.path().to_owned()],
            Vec::new(),
        )
        .expect("registry");

        let active = registry.active_projects_payload().expect("active projects");
        assert_eq!(active["schema"], "context_projects.active.v1");
        assert_eq!(active["count"], 0);

        drop(registry.default_engine().expect("active default engine"));
        let active = registry.active_projects_payload().expect("active projects");
        assert_eq!(active["count"], 1);
        assert_eq!(
            active["projects"][0]["name"],
            root.path()
                .file_name()
                .expect("temporary root name")
                .to_str()
                .expect("UTF-8 temporary root name")
        );
    }

    #[test]
    fn cached_projects_payload_reads_only_the_persisted_discovery_manifest() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        let child = root.path().join("persisted-project");
        std::fs::create_dir_all(&child).expect("child project");
        std::fs::write(child.join("Cargo.toml"), "[workspace]\n").expect("project marker");
        let registry = ProjectRegistry::new(
            root.path().to_owned(),
            state.path().to_owned(),
            vec![root.path().to_owned()],
            Vec::new(),
        )
        .expect("registry");

        let cached = registry.cached_projects_payload().expect("cached projects");
        assert_eq!(cached["schema"], "context_projects.cached.v1");
        assert_eq!(cached["catalogue"], "resident_engines_only");
        assert_eq!(cached["count"], 0);

        let discovered = registry.projects_payload().expect("explicit discovery");
        assert_eq!(discovered["count"], 1);
        let cached = registry.cached_projects_payload().expect("cached projects");
        assert_eq!(cached["catalogue"], "persisted_discovery");
        assert_eq!(cached["count"], 1);
        assert_eq!(cached["projects"][0]["name"], "persisted-project");
        assert!(
            registry
                .cached_project_ids()
                .expect("cached engines")
                .is_empty()
        );
    }

    #[test]
    fn discovery_stops_descending_after_a_project_marker() {
        let root = tempfile::tempdir().expect("workspace root");
        let state = tempfile::tempdir().expect("state root");
        let project = root.path().join("platform");
        let nested = project.join("src/generated/project");
        std::fs::create_dir_all(&nested).expect("nested project directory");
        std::fs::write(
            project.join("pyproject.toml"),
            "[project]\nname='platform'\n",
        )
        .expect("project marker");
        std::fs::write(nested.join("Cargo.toml"), "[workspace]\n").expect("nested marker");

        let registry = ProjectRegistry::new(
            root.path().to_owned(),
            state.path().to_owned(),
            vec![root.path().to_owned()],
            Vec::new(),
        )
        .expect("registry");
        registry.discover_projects().expect("discover projects");
        let roots = registry
            .state
            .lock()
            .expect("registry state")
            .specs
            .values()
            .map(|spec| spec.local_root.clone())
            .collect::<Vec<_>>();
        assert!(roots.contains(&project.canonicalize().expect("project root")));
        assert!(!roots.contains(&nested.canonicalize().expect("nested root")));
    }

    #[test]
    fn mapped_workspace_git_root_yields_child_projects_like_python() {
        let root = tempfile::tempdir().expect("temporary root");
        let host_root = root.path().join("host-source");
        let workspace = root.path().join("workspace-roots");
        let child = workspace.join("real-project");
        std::fs::create_dir_all(workspace.join(".git")).expect("workspace git marker");
        std::fs::create_dir_all(child.join(".git")).expect("child git marker");

        let registry = ProjectRegistry::new(
            workspace.clone(),
            root.path().join("state"),
            vec![host_root.clone()],
            vec![(host_root, workspace)],
        )
        .expect("registry");
        let projects = registry.projects_payload().expect("project list");
        assert_eq!(projects["count"], 1);
        assert_eq!(projects["projects"][0]["name"], "real-project");
        assert_eq!(projects["projects"][0]["source"], "discovered_git");
    }

    #[test]
    fn governed_lineage_manifest_requires_explicit_distinct_bounded_roots() {
        let root = tempfile::tempdir().expect("temporary root");
        let first = root.path().join("builder");
        let second = root.path().join("verifier");
        std::fs::create_dir_all(&first).expect("first root");
        std::fs::create_dir_all(&second).expect("second root");
        let manifest = root.path().join("lineages.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(&json!({
                "schema": "context_frontier_lineages.v1",
                "lineages": [{"id": "handoff", "roots": [&first, &second]}]
            }))
            .expect("manifest JSON"),
        )
        .expect("manifest file");
        let governed =
            load_frontier_lineage_manifest(Some(manifest.clone()), &[root.path().to_owned()])
                .expect("valid manifest");
        assert_eq!(governed.len(), 2);
        assert_eq!(governed[&first].identity(), governed[&second].identity());

        std::fs::write(
            &manifest,
            serde_json::to_vec(&json!({
                "schema": "context_frontier_lineages.v1",
                "lineages": [{"id": "handoff", "roots": [&first, &first]}]
            }))
            .expect("duplicate manifest JSON"),
        )
        .expect("duplicate manifest file");
        assert!(load_frontier_lineage_manifest(Some(manifest), &[root.path().to_owned()]).is_err());
    }
}
