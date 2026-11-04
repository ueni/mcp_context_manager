use std::{
    collections::{HashMap, HashSet},
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
    ContextAdminRequest, ContextPackRejectionClass, ContextPackRequest, ProjectEngine,
    ResultReferenceRequest, UsageMonitor, WikiHub,
};
use context_index::{WorkControl, is_ignored_repository_path};
use context_store::StateStore;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use url::Url;
use walkdir::{DirEntry, WalkDir};

const ENGINE_CAPACITY: usize = 8;
const DISCOVERY_MAX_PROJECTS: usize = 100;
const DISCOVERY_MAX_ENTRIES: usize = 10_000;
const DEFAULT_DISCOVERY_MAX_DEPTH: usize = 4;
const PROJECT_CATALOG_FILE: &str = "project-catalog.v1.json";
const WIKI_OWNER_CATALOG_FILE: &str = "wiki-owners.v1.json";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 48;
const DEFAULT_CANCELLATION_GRACE_SECS: u64 = 2;
const MISSING_PROJECT_GRACE: Duration = Duration::from_secs(5 * 60);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
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
    shared_wiki: Arc<WikiHub>,
    wiki_catalog_lock: Arc<Mutex<()>>,
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
    missing_since: HashMap<String, Instant>,
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
    repository_id: Option<String>,
    registered_worktree: bool,
    git_head: Option<String>,
    git_branch: Option<String>,
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
        Self::new(base_root, state_root, allowed_roots, root_mappings)
    }

    pub fn new(
        base_root: PathBuf,
        state_root: PathBuf,
        allowed_roots: Vec<PathBuf>,
        mut root_mappings: Vec<(PathBuf, PathBuf)>,
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
        let default_git =
            context_index::git::checkout_with_mappings(&base_root, &root_mappings).ok();
        let default_repository_id = default_git.as_ref().map_or_else(
            || local_repository_id(&root_hash),
            |identity| {
                repository_id(&normalize_common_dir_for_mappings(
                    &identity.common_dir,
                    &root_mappings,
                ))
            },
        );
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
            repository_id: Some(default_repository_id),
            registered_worktree: false,
            git_head: default_git.as_ref().map(|identity| identity.head.clone()),
            git_branch: default_git
                .as_ref()
                .and_then(|identity| identity.branch.clone()),
        };
        let mut specs = HashMap::new();
        specs.insert(project_id.clone(), default_spec);
        let usage_monitor = Arc::new(UsageMonitor::open(state_root.join("global-monitor"))?);
        let shared_wiki = Arc::new(WikiHub::new()?);
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
            shared_wiki,
            wiki_catalog_lock: Arc::new(Mutex::new(())),
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
                missing_since: HashMap::new(),
            })),
        })
    }

    pub fn engine_for(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
    ) -> Result<Arc<ProjectEngine>> {
        let usage_monitor = Arc::clone(&self.usage_monitor);
        let registry = self.clone();
        self.engine_for_with_builder(project_id, root_uri, move |spec| {
            ensure_checkout_state_marker(spec)?;
            let wiki_state = registry.migrate_repository_wiki(spec)?;
            build_project_engine(
                spec,
                wiki_state.as_deref(),
                registry.git_mappings(),
                usage_monitor,
                None,
            )
        })
    }

    fn engine_for_controlled(
        &self,
        project_id: Option<&str>,
        root_uri: Option<&str>,
        control: &WorkControl,
    ) -> Result<Arc<ProjectEngine>> {
        let usage_monitor = Arc::clone(&self.usage_monitor);
        let blocking = Arc::clone(&self.blocking);
        let control = control.clone();
        let singleflight_waiter = blocking.singleflight_waiters.fetch_add(1, Ordering::AcqRel) + 1;
        blocking
            .singleflight_waiters_peak
            .fetch_max(singleflight_waiter, Ordering::Relaxed);
        let _singleflight_waiter = CountGuard(&blocking.singleflight_waiters);
        let build_metrics = Arc::clone(&blocking);
        let registry = self.clone();
        self.engine_for_with_builder(project_id, root_uri, move |spec| {
            control.check()?;
            ensure_checkout_state_marker(spec)?;
            let wiki_state = registry.migrate_repository_wiki(spec)?;
            let engine = build_project_engine(
                spec,
                wiki_state.as_deref(),
                registry.git_mappings(),
                usage_monitor,
                Some(&control),
            )?;
            build_metrics.engine_builds.fetch_add(1, Ordering::Relaxed);
            Ok(engine)
        })
    }

    pub async fn context_pack_bounded(
        self: &Arc<Self>,
        request: ContextPackRequest,
    ) -> Result<Vec<u8>> {
        self.context_pack_bounded_inner(request).await
    }

    async fn context_pack_bounded_inner(
        self: &Arc<Self>,
        mut request: ContextPackRequest,
    ) -> Result<Vec<u8>> {
        let registry = Arc::clone(self);
        self.run_repository_work_bounded("context_pack", false, move |job_control| {
            let engine_started = Instant::now();
            let engine = registry.engine_for_controlled(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
                &job_control,
            )?;
            registry.blocking.engine_load_micros.fetch_add(
                u64::try_from(engine_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            job_control.check()?;
            #[cfg(test)]
            registry.run_blocking_hook(&job_control)?;
            request.project_id = Some(engine.project_id().to_owned());
            request.root_uri = None;
            let pack_started = Instant::now();
            let result = tokio::runtime::Handle::current()
                .block_on(engine.context_pack_with_control(&request, Some(&job_control)));
            registry.blocking.pack_micros.fetch_add(
                u64::try_from(pack_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            result
        })
        .await
    }

    pub async fn context_admin_warmup_bounded(
        self: &Arc<Self>,
        request: ContextAdminRequest,
    ) -> Result<Vec<u8>> {
        self.context_admin_warmup_bounded_inner(request, false)
            .await
    }

    async fn context_admin_warmup_bounded_inner(
        self: &Arc<Self>,
        mut request: ContextAdminRequest,
        startup: bool,
    ) -> Result<Vec<u8>> {
        let registry = Arc::clone(self);
        self.run_repository_work_bounded("context_admin warmup", startup, move |job_control| {
            let engine_started = Instant::now();
            let engine = registry.engine_for_controlled(
                request.project_id.as_deref(),
                request.root_uri.as_deref(),
                &job_control,
            )?;
            registry.blocking.engine_load_micros.fetch_add(
                u64::try_from(engine_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            job_control.check()?;
            #[cfg(test)]
            registry.run_blocking_hook(&job_control)?;
            request.project_id = Some(engine.project_id().to_owned());
            request.root_uri = None;
            tokio::runtime::Handle::current()
                .block_on(engine.context_admin_controlled(&request, Some(&job_control)))
        })
        .await
    }

    async fn run_repository_work_bounded<F>(
        self: &Arc<Self>,
        operation: &'static str,
        warmup: bool,
        work: F,
    ) -> Result<Vec<u8>>
    where
        F: FnOnce(WorkControl) -> Result<Vec<u8>> + Send + 'static,
    {
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
                return Err(anyhow!("{operation} blocking executor is unavailable"));
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
                    "{operation} busy; retry after warmup or when queued work completes"
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
                "{operation} busy; retry after warmup or when queued work completes"
            ));
        }

        let job_control = control.clone();
        let blocking = Arc::clone(&self.blocking);
        let mut job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let active = blocking.active.fetch_add(1, Ordering::AcqRel) + 1;
            blocking.active_peak.fetch_max(active, Ordering::Relaxed);
            let _active = CountGuard(&blocking.active);
            job_control.check()?;
            work(job_control)
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
                        "{operation} timed out before the server deadline; retry after warmup"
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
                            "{operation} timed out before the server deadline; retry after warmup"
                        ))
                    }
                    Err(_) => {
                        // A running `spawn_blocking` closure cannot be aborted. Keep the
                        // request attached until its drop guards release the global permit
                        // and active-job counter; returning here would abandon capacity and
                        // could starve every queued request.
                        let _ = (&mut job).await;
                        Err(anyhow!(
                            "{operation} cancellation grace expired; retry after warmup"
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
            let request: ContextAdminRequest =
                serde_json::from_value(json!({"mode": "warmup"})).expect("index warmup request");
            let state = if registry
                .context_admin_warmup_bounded_inner(request, true)
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
            let restore = Arc::clone(&registry);
            let _ = registry
                .run_repository_work_bounded("wiki restore", false, move |control| {
                    control.check()?;
                    restore.restore_wiki_projects()?;
                    Ok(Vec::new())
                })
                .await;
            loop {
                tokio::time::sleep(RECONCILE_INTERVAL).await;
                let registry = Arc::clone(&registry);
                let _ = tokio::task::spawn_blocking(move || {
                    registry.discover_projects()?;
                    registry.reconcile_missing_at(Instant::now())
                })
                .await;
            }
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

    #[cfg(test)]
    fn run_blocking_hook(&self, control: &WorkControl) -> Result<()> {
        if let Some(hook) = self
            .blocking_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            hook(control)?;
        }
        Ok(())
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
        if request
            .reference
            .as_ref()
            .and_then(|reference| reference.get("kind"))
            .and_then(Value::as_str)
            .is_some_and(|kind| matches!(kind, "wiki" | "wiki_source"))
        {
            let owner = request
                .reference
                .as_ref()
                .and_then(|reference| reference.get("project_id"))
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("wiki reference requires owner project_id"))?;
            let repository_id = request
                .reference
                .as_ref()
                .and_then(|reference| reference.get("repository_id"))
                .and_then(Value::as_str);
            if repository_id.is_none()
                && self.project_for_legacy_wiki_owner(owner)?.is_none()
                && let Some(archived_repository) = self.repository_for_legacy_wiki_owner(owner)?
            {
                return self.resolve_archived_wiki_reference(request, &archived_repository);
            }
            let engine = if let Some(root) = request.root_uri.as_deref() {
                self.engine_for(None, Some(root))?
            } else if let Some(project_id) = request.project_id.as_deref() {
                match self.engine_for(Some(project_id), None) {
                    Ok(engine) => engine,
                    Err(_) => {
                        if let Some(repository_id) = repository_id {
                            match self.project_for_repository(repository_id) {
                                Ok(selected) => self.engine_for(Some(&selected), None)?,
                                Err(_) => {
                                    return self
                                        .resolve_archived_wiki_reference(request, repository_id);
                                }
                            }
                        } else {
                            let engine = self.default_engine()?;
                            if owner != engine.project_id()
                                && !self.default_project_id_aliases().contains(owner)
                                && !engine.wiki_owner_matches(owner)?
                            {
                                return Err(anyhow!(
                                    "wiki reference owner does not select an admitted project"
                                ));
                            }
                            engine
                        }
                    }
                }
            } else if let Some(repository_id) = repository_id {
                let project_id = match self.project_for_repository(repository_id) {
                    Ok(project_id) => project_id,
                    Err(_) => return self.resolve_archived_wiki_reference(request, repository_id),
                };
                self.engine_for(Some(&project_id), None)?
            } else {
                let selected = self.project_for_legacy_wiki_owner(owner)?;
                match selected
                    .map(|project_id| self.engine_for(Some(&project_id), None))
                    .unwrap_or_else(|| self.engine_for(Some(owner), None))
                {
                    Ok(engine) => engine,
                    Err(_) => {
                        if let Some(repository_id) = self.repository_for_legacy_wiki_owner(owner)? {
                            return self.resolve_archived_wiki_reference(request, &repository_id);
                        }
                        let engine = self.default_engine()?;
                        if owner != engine.project_id()
                            && !self.default_project_id_aliases().contains(owner)
                            && !engine.wiki_owner_matches(owner)?
                        {
                            return Err(anyhow!(
                                "wiki reference owner does not select an admitted project"
                            ));
                        }
                        engine
                    }
                }
            };
            if let Some(repository_id) = repository_id {
                if engine.repository_id() != repository_id {
                    bail!("wiki reference belongs to another repository");
                }
            } else if engine.project_id() != owner && !engine.wiki_owner_matches(owner)? {
                bail!("wiki reference owner does not match the selected project");
            }
            let mut scoped = request.clone();
            scoped.project_id = Some(engine.project_id().to_owned());
            scoped.root_uri = None;
            return engine.result_reference_resolve(&scoped);
        }
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

    fn resolve_archived_wiki_reference(
        &self,
        request: &ResultReferenceRequest,
        repository_id: &str,
    ) -> Result<Vec<u8>> {
        if repository_id.is_empty()
            || !repository_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!("invalid archived repository id");
        }
        let repository_root = self.state_root.join("repositories").join(repository_id);
        let wiki_root = repository_root.join("wiki");
        let server_root = self.state_root.canonicalize()?;
        let mut current = self.state_root.join("repositories");
        for component in [repository_id, "wiki"] {
            current.push(component);
            let metadata = std::fs::symlink_metadata(&current)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("archived wiki state is not server-owned");
            }
        }
        let canonical_wiki = wiki_root.canonicalize()?;
        if !canonical_wiki.starts_with(server_root.join("repositories")) {
            bail!("archived wiki state escaped the server state root");
        }
        let store = StateStore::open(&wiki_root)?;
        let reference = request
            .reference
            .as_ref()
            .ok_or_else(|| anyhow!("archived wiki resolution requires a reference"))?;
        context_core::resolve_archived_wiki_reference(&store, reference, &request.expected_hash)
    }

    fn project_for_repository(&self, repository_id: &str) -> Result<String> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?;
        state
            .specs
            .values()
            .filter(|spec| spec.repository_id.as_deref() == Some(repository_id))
            .map(|spec| spec.project_id.clone())
            .min()
            .ok_or_else(|| anyhow!("repository_id is not an admitted project"))
    }

    fn project_for_legacy_wiki_owner(&self, owner: &str) -> Result<Option<String>> {
        let candidates = {
            let state = self
                .state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?;
            state
                .specs
                .values()
                .filter_map(|spec| {
                    spec.repository_id
                        .as_ref()
                        .map(|repo| (repo.clone(), spec.project_id.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (repository_id, project_id) in candidates {
            let wiki_state = self
                .state_root
                .join("repositories")
                .join(&repository_id)
                .join("wiki");
            let Ok(store) = StateStore::open(wiki_state) else {
                continue;
            };
            let owner_record = store.get_json(&format!("wiki:owner:{owner}"))?;
            if owner_record
                .as_ref()
                .and_then(|record| record["repository_id"].as_str())
                == Some(repository_id.as_str())
            {
                return Ok(Some(project_id));
            }
        }
        Ok(None)
    }

    fn migrate_repository_wiki(&self, spec: &ProjectSpec) -> Result<Option<PathBuf>> {
        let Some(repository_id) = spec.repository_id.as_deref() else {
            return Ok(None);
        };
        let wiki_state = self
            .state_root
            .join("repositories")
            .join(repository_id)
            .join("wiki");
        let shared = StateStore::open(&wiki_state)?;
        let mut sources = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?
            .specs
            .values()
            .filter(|candidate| candidate.repository_id.as_deref() == Some(repository_id))
            .map(|candidate| (candidate.project_id.clone(), candidate.state_root.clone()))
            .collect::<Vec<_>>();
        if !sources
            .iter()
            .any(|(project_id, _)| project_id == &spec.project_id)
        {
            sources.push((spec.project_id.clone(), spec.state_root.clone()));
        }
        sources.sort_by(|left, right| left.0.cmp(&right.0));
        for (project_id, project_state) in sources {
            if project_state == wiki_state {
                continue;
            }
            let legacy = StateStore::open(project_state)?;
            shared.migrate_wiki_from(&legacy, &project_id, repository_id)?;
            self.record_wiki_owner(repository_id, &project_id)?;
        }
        Ok(Some(wiki_state))
    }

    fn record_wiki_owner(&self, repository_id: &str, owner: &str) -> Result<()> {
        let _guard = self
            .wiki_catalog_lock
            .lock()
            .map_err(|_| anyhow!("wiki owner catalogue lock poisoned"))?;
        let path = self.state_root.join(WIKI_OWNER_CATALOG_FILE);
        let mut entries = std::fs::read(&path)
            .ok()
            .filter(|bytes| bytes.len() <= 256 * 1024)
            .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).ok())
            .unwrap_or_default();
        let row = json!({"owner":owner,"repository_id":repository_id});
        entries.retain(|entry| entry["owner"] != owner);
        entries.push(row);
        entries.sort_by(|left, right| left["owner"].as_str().cmp(&right["owner"].as_str()));
        if entries.len() > 1000 {
            entries.truncate(1000);
        }
        let temporary = self
            .state_root
            .join(format!(".{WIKI_OWNER_CATALOG_FILE}.tmp"));
        std::fs::write(&temporary, serde_json::to_vec(&entries)?)?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    fn repository_for_legacy_wiki_owner(&self, owner: &str) -> Result<Option<String>> {
        let path = self.state_root.join(WIKI_OWNER_CATALOG_FILE);
        let entries = std::fs::read(&path)
            .ok()
            .filter(|bytes| bytes.len() <= 256 * 1024)
            .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).ok())
            .unwrap_or_default();
        Ok(entries.into_iter().find_map(|entry| {
            (entry["owner"].as_str() == Some(owner))
                .then(|| entry["repository_id"].as_str().map(str::to_owned))
                .flatten()
        }))
    }

    fn persist_wiki_registration(&self, spec: &ProjectSpec) -> Result<()> {
        if spec.local_root == self.base_root {
            return Ok(());
        }
        let _guard = self
            .wiki_catalog_lock
            .lock()
            .map_err(|_| anyhow!("wiki catalogue lock poisoned"))?;
        let mut entry = None;
        for allowed in &self.allowed_roots {
            let (mapped, _) = self.map_host_path(allowed);
            let Ok(mapped) = mapped.canonicalize() else {
                continue;
            };
            if let Ok(relative) = spec.local_root.strip_prefix(mapped) {
                let relative = relative.to_string_lossy();
                entry = Some(
                    json!({"boundary":sha256_hex(allowed.to_string_lossy().as_bytes()),"relative":if relative.is_empty(){"."}else{relative.as_ref()}}),
                );
                break;
            }
        }
        let Some(entry) = entry else {
            return Ok(());
        };
        let target = self.state_root.join("wiki-projects.v1.json");
        let mut entries = std::fs::read(&target)
            .ok()
            .filter(|bytes| bytes.len() <= 256 * 1024)
            .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).ok())
            .unwrap_or_default();
        if !entries.contains(&entry) {
            entries.push(entry);
        }
        if entries.len() > 1000 {
            bail!("wiki project catalogue exceeds 1000 projects");
        }
        let temporary = self.state_root.join(".wiki-projects.v1.tmp");
        std::fs::write(&temporary, serde_json::to_vec(&entries)?)?;
        std::fs::rename(temporary, target)?;
        Ok(())
    }

    fn restore_wiki_projects(&self) -> Result<()> {
        let target = self.state_root.join("wiki-projects.v1.json");
        let bytes = match std::fs::read(target) {
            Ok(bytes) if bytes.len() <= 256 * 1024 => bytes,
            _ => return Ok(()),
        };
        let entries: Vec<Value> = serde_json::from_slice(&bytes)?;
        for entry in entries.into_iter().take(1000) {
            let Some(relative) = entry["relative"]
                .as_str()
                .and_then(|path| context_index::validate_relative_path(path).ok())
            else {
                continue;
            };
            let Some(allowed) = self.allowed_roots.iter().find(|allowed| {
                sha256_hex(allowed.to_string_lossy().as_bytes())
                    == entry["boundary"].as_str().unwrap_or_default()
            }) else {
                continue;
            };
            let root = allowed.join(relative);
            let Ok(spec) = self.project_from_uri(&canonical_file_uri(&root)?, "wiki_restore")
            else {
                continue;
            };
            let id = spec.project_id.clone();
            self.state
                .lock()
                .map_err(|_| anyhow!("registry lock poisoned"))?
                .specs
                .insert(id.clone(), spec);
            // Sequential and outside search; every root is re-authorized before state is loaded.
            let _ = self.engine_for(Some(&id), None);
        }
        Ok(())
    }

    pub fn shared_wiki_search(
        &self,
        request: &context_core::ContextLookupRequest,
    ) -> Result<Value> {
        let active = self.engine_for(request.project_id.as_deref(), request.root_uri.as_deref())?;
        let selected = if request.wiki_scope == context_core::WikiSearchScope::SelectedProject {
            Some(active.project_id().to_owned())
        } else {
            None
        };
        Ok(
            json!({"schema":"agent_wiki.search.v1", "generation":self.shared_wiki.generation(),
            "results":self.shared_wiki.search_for_checkout(&request.query,selected.as_deref(),Some(active.project_id()),usize::from(request.max_results),false)?}),
        )
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
        engine.attach_wiki_hub(Arc::clone(&self.shared_wiki))?;
        self.persist_wiki_registration(&spec)?;
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

    pub fn projects_payload(&self, max_entries: u16) -> Result<Value> {
        self.discover_projects()?;
        self.reconcile_missing_at(Instant::now())?;
        let payload = self.project_payload(false)?;
        self.persist_project_catalog(&payload);
        Self::bounded_project_catalog(payload, max_entries)
    }

    /// Returns only engines that are already resident in this process.
    ///
    /// Unlike project discovery, this is an in-memory snapshot and is safe to
    /// call from a frequent metrics poll.
    pub fn active_projects_payload(&self, max_entries: u16) -> Result<Value> {
        Self::bounded_project_catalog(self.project_payload(true)?, max_entries)
    }

    /// Lists a manifest written by explicit project discovery, without walking
    /// an allowed root. When no manifest exists, return resident engines only.
    pub fn cached_projects_payload(&self, max_entries: u16) -> Result<Value> {
        if let Ok(bytes) = std::fs::read(self.state_root.join(PROJECT_CATALOG_FILE))
            && let Ok(mut payload) = serde_json::from_slice::<Value>(&bytes)
            && payload.get("schema").and_then(Value::as_str) == Some("context_projects.list.v1")
            && payload.get("projects").and_then(Value::as_array).is_some()
        {
            payload["schema"] = Value::String("context_projects.cached.v1".to_owned());
            payload["catalogue"] = Value::String("persisted_discovery".to_owned());
            return Self::bounded_project_catalog(payload, max_entries);
        }
        let mut payload = self.project_payload(true)?;
        payload["schema"] = Value::String("context_projects.cached.v1".to_owned());
        payload["catalogue"] = Value::String("resident_engines_only".to_owned());
        Self::bounded_project_catalog(payload, max_entries)
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
            .filter(|spec| !self.confirmed_missing_root(spec))
            // Python keeps this engine-only fallback out of the discovered
            // project list.  Otherwise a workspace parent appears alongside
            // the real projects found below it.
            .filter(|spec| !spec.legacy || active_only || spec.source != "repo_path")
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

    fn bounded_project_catalog(mut payload: Value, max_entries: u16) -> Result<Value> {
        if !(1..=1000).contains(&max_entries) {
            bail!("max_entries must be in 1..=1000");
        }
        let projects = payload
            .get_mut("projects")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| anyhow!("project catalogue is missing projects"))?;
        let total_count = projects.len();
        projects.truncate(usize::from(max_entries));
        let returned_count = projects.len();
        payload["count"] = json!(returned_count);
        payload["total_count"] = json!(total_count);
        payload["returned_count"] = json!(returned_count);
        payload["omitted_count"] = json!(total_count.saturating_sub(returned_count));
        payload["max_entries"] = json!(max_entries);
        payload["truncated"] = json!(returned_count < total_count);
        Ok(payload)
    }

    pub fn default_engine(&self) -> Result<Arc<ProjectEngine>> {
        self.engine_for(None, None)
    }

    fn cached_engine(&self, project_id: &str) -> Result<Option<Arc<ProjectEngine>>> {
        let spec = self
            .state
            .lock()
            .map_err(|_| anyhow!("project registry lock poisoned"))?
            .specs
            .get(project_id)
            .cloned();
        if let Some(spec) = spec {
            let root = spec
                .local_root
                .canonicalize()
                .context("project root is unavailable")?;
            if root != spec.local_root {
                bail!("project root identity changed");
            }
            if spec.registered_worktree && !self.is_registered_worktree(&root)? {
                bail!("Git worktree registration changed");
            }
        }
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
                && !(spec.project_id == self.default_project_id
                    && self.default_project_id_aliases().contains(project_id))
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
            let Some(project_id) = project_id else {
                return Ok(self.default_project_id.clone());
            };
            if project_id == self.default_project_id
                || self.default_project_id_aliases().contains(project_id)
            {
                Ok(self.default_project_id.clone())
            } else {
                Ok(project_id.to_owned())
            }
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
        let (mapped_root, mapped) = self.map_host_path(&host_root);
        let outside_allowed_roots = !self.is_allowed_root(&host_root);
        let registered_worktree = if outside_allowed_roots {
            mapped_root
                .canonicalize()
                .ok()
                .is_some_and(|root| self.is_registered_worktree(&root).unwrap_or(false))
        } else {
            false
        };
        if outside_allowed_roots && !registered_worktree {
            bail!(
                "MCP root is outside MCP_CONTEXT_ALLOWED_ROOTS and is not a registered Git worktree"
            );
        }
        if mapped_root.symlink_metadata()?.file_type().is_symlink() {
            bail!("project root must not be a symlink");
        }
        let local_root = mapped_root
            .canonicalize()
            .context("project root is not readable after mapping")?;
        if !local_root.is_dir() {
            bail!("project root must be a directory");
        }
        let registered_worktree =
            registered_worktree || self.is_registered_worktree(&local_root)?;
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
        if !registered_worktree
            && !local_boundaries
                .iter()
                .any(|allowed| local_root == *allowed || local_root.starts_with(allowed))
        {
            bail!("mapped project root escapes its configured local boundary");
        }
        if local_root == self.base_root {
            let default_spec = self
                .state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?
                .specs
                .get(&self.default_project_id)
                .cloned()
                .ok_or_else(|| anyhow!("default project specification is missing"))?;
            let mut default_spec = default_spec;
            default_spec.source = source.to_owned();
            return Ok(default_spec);
        }
        let canonical_uri = canonical_file_uri(&host_root)?;
        let project_id = derived_project_id(&host_root, &canonical_uri);
        let root_hash = sha256_hex(canonical_uri.as_bytes());
        let name = host_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("project")
            .to_owned();
        let mappings = self.git_mappings();
        let git = context_index::git::checkout_with_mappings(&local_root, &mappings).ok();
        let repository_id = Some(git.as_ref().map_or_else(
            || local_repository_id(&root_hash),
            |identity| repository_id(&self.normalize_common_dir(&identity.common_dir)),
        ));
        Ok(ProjectSpec {
            state_root: self.state_root.join("projects").join(&project_id),
            project_id,
            name,
            root_hash,
            local_root,
            source: source.to_owned(),
            mapped,
            legacy: false,
            repository_id,
            registered_worktree,
            git_head: git.as_ref().map(|identity| identity.head.clone()),
            git_branch: git.and_then(|identity| identity.branch),
        })
    }

    fn default_project_id_aliases(&self) -> HashSet<String> {
        let mut aliases = HashSet::from([self.default_project_id.clone()]);
        let Ok(default_uri) = canonical_file_uri(&self.base_root) else {
            return aliases;
        };
        aliases.insert(derived_project_id(&self.base_root, &default_uri));
        for (host_prefix, local_prefix) in &self.root_mappings {
            let Ok(mapped_prefix) = local_prefix.canonicalize() else {
                continue;
            };
            let Ok(relative) = self.base_root.strip_prefix(&mapped_prefix) else {
                continue;
            };
            let host_root = host_prefix.join(relative);
            let host_root = host_root
                .canonicalize()
                .unwrap_or_else(|_| host_root.clone());
            if let Ok(uri) = canonical_file_uri(&host_root) {
                aliases.insert(derived_project_id(&host_root, &uri));
            }
        }
        aliases
    }

    fn is_allowed_root(&self, host_root: &Path) -> bool {
        host_root == self.base_root
            || self
                .allowed_roots
                .iter()
                .any(|allowed| host_root == allowed || host_root.starts_with(allowed))
    }

    fn normalize_common_dir(&self, common_dir: &Path) -> PathBuf {
        normalize_common_dir_for_mappings(common_dir, &self.root_mappings)
    }

    fn is_registered_worktree(&self, root: &Path) -> Result<bool> {
        let mappings = self.git_mappings();
        let Ok(identity) = context_index::git::checkout_with_mappings(root, &mappings) else {
            return Ok(false);
        };
        let mut checked_common_dirs = HashSet::new();
        for repository in self.admitted_git_roots() {
            let Ok(repository_identity) =
                context_index::git::checkout_with_mappings(&repository, &mappings)
            else {
                continue;
            };
            if repository_identity.common_dir != identity.common_dir {
                continue;
            }
            if !checked_common_dirs.insert(repository_identity.common_dir.clone()) {
                continue;
            }
            if context_index::git::registered_worktrees_with_mappings(&repository, &mappings)
                .is_ok_and(|roots| roots.iter().any(|registered| registered == root))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn admitted_git_roots(&self) -> Vec<PathBuf> {
        let mut roots = self
            .state
            .lock()
            .map(|state| {
                state
                    .specs
                    .values()
                    .map(|spec| spec.local_root.clone())
                    .collect()
            })
            .unwrap_or_else(|_| vec![self.base_root.clone()]);
        roots.push(self.base_root.clone());
        roots.sort();
        roots.dedup();
        roots.truncate(DISCOVERY_MAX_PROJECTS);
        roots
    }

    fn git_mappings(&self) -> Vec<(PathBuf, PathBuf)> {
        self.root_mappings.clone()
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
            let mut inspected = 0usize;
            while let Some(entry) = entries.next() {
                inspected += 1;
                if inspected > DISCOVERY_MAX_ENTRIES {
                    break;
                }
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
                    let repository_root = spec.local_root.clone();
                    discovered.push(spec);
                    if let Ok(worktrees) = context_index::git::registered_worktrees_with_mappings(
                        &repository_root,
                        &self.git_mappings(),
                    ) {
                        for worktree in worktrees.into_iter().take(DISCOVERY_MAX_PROJECTS) {
                            if worktree == repository_root || !worktree.is_dir() {
                                continue;
                            }
                            let Some(host_path) = self.host_path_for_local(&worktree) else {
                                continue;
                            };
                            let Ok(worktree_uri) = canonical_file_uri(&host_path) else {
                                continue;
                            };
                            if let Ok(worktree_spec) =
                                self.project_from_uri(&worktree_uri, "discovered_worktree")
                            {
                                discovered.push(worktree_spec);
                            }
                            if discovered.len() >= DISCOVERY_MAX_PROJECTS {
                                break;
                            }
                        }
                    }
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
        for spec in discovered {
            state.specs.entry(spec.project_id.clone()).or_insert(spec);
        }
        drop(state);
        self.reconcile_missing_at(Instant::now())?;
        Ok(())
    }

    fn reconcile_missing_at(&self, now: Instant) -> Result<()> {
        let mut expired = Vec::new();
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?;
            let ids = state.specs.keys().cloned().collect::<Vec<_>>();
            for id in ids {
                let Some(spec) = state.specs.get(&id).cloned() else {
                    continue;
                };
                match std::fs::symlink_metadata(&spec.local_root) {
                    Ok(_) => {
                        state.missing_since.remove(&id);
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound
                            && self.confirmed_missing_root(&spec) =>
                    {
                        let since = *state.missing_since.entry(id.clone()).or_insert(now);
                        if now.saturating_duration_since(since) >= MISSING_PROJECT_GRACE
                            && state
                                .engines
                                .get(&id)
                                .is_none_or(|entry| Arc::strong_count(&entry.engine) == 1)
                            && state.construction_gates.get(&id).is_none_or(|gate| {
                                Arc::strong_count(gate) == 1 && gate.try_lock().is_ok()
                            })
                        {
                            expired.push((id, spec));
                        }
                    }
                    Err(_) => {
                        state.missing_since.remove(&id);
                    }
                }
            }
        }
        for (id, spec) in expired {
            if spec.repository_id.is_some() {
                self.migrate_repository_wiki(&spec)?;
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("project registry lock poisoned"))?;
            let still_registered = state
                .specs
                .get(&id)
                .is_some_and(|current| current.local_root == spec.local_root);
            let still_absent = self.confirmed_missing_root(&spec);
            let grace_elapsed = state.missing_since.get(&id).is_some_and(|since| {
                now.saturating_duration_since(*since) >= MISSING_PROJECT_GRACE
            });
            let engine_idle = state
                .engines
                .get(&id)
                .is_none_or(|entry| Arc::strong_count(&entry.engine) == 1);
            let gate = state.construction_gates.get(&id).cloned();
            let gate_guard = gate.as_ref().map(|gate| {
                (Arc::strong_count(gate) == 2)
                    .then(|| gate.try_lock().ok())
                    .flatten()
            });
            let gate_idle = gate.is_none() || gate_guard.as_ref().is_some_and(Option::is_some);
            if !still_registered || !still_absent || !grace_elapsed || !engine_idle || !gate_idle {
                if !still_absent {
                    state.missing_since.remove(&id);
                }
                continue;
            }
            self.shared_wiki.unregister(&id)?;
            self.remove_checkout_state(&spec)?;
            state.specs.remove(&id);
            state.engines.remove(&id);
            state.construction_gates.remove(&id);
            state.missing_since.remove(&id);
            drop(gate_guard);
            drop(state);
        }
        self.persist_project_catalog(&self.project_payload(false)?);
        Ok(())
    }

    fn confirmed_missing_root(&self, spec: &ProjectSpec) -> bool {
        if !confirmed_missing_root(spec) {
            return false;
        }
        let mapped_boundaries = self
            .allowed_roots
            .iter()
            .map(|root| self.map_host_path(root).0)
            .filter_map(|root| root.canonicalize().ok())
            .collect::<Vec<_>>();
        if mapped_boundaries.iter().any(|boundary| {
            (spec.local_root == *boundary || spec.local_root.starts_with(boundary))
                && boundary.is_dir()
        }) {
            return true;
        }
        if self.base_root == spec.local_root || spec.local_root.starts_with(&self.base_root) {
            return self.base_root.is_dir();
        }
        if spec.registered_worktree {
            return spec.local_root.parent().is_some_and(Path::is_dir);
        }
        false
    }

    fn remove_checkout_state(&self, spec: &ProjectSpec) -> Result<()> {
        let project_id = &spec.project_id;
        if project_id.is_empty()
            || !project_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!("refusing to remove state for an invalid project id");
        }
        if spec.legacy {
            if spec.state_root != self.state_root {
                bail!("refusing to remove state outside the configured legacy state root");
            }
            let generated = spec.state_root.join("rust-v2");
            if let Ok(metadata) = std::fs::symlink_metadata(&generated) {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    bail!("refusing to remove unsafe legacy checkout state");
                }
                if generated.canonicalize()? != generated {
                    bail!("refusing to remove legacy checkout state through a changed path");
                }
                std::fs::remove_dir_all(generated)?;
            }
            return Ok(());
        }
        if spec.state_root != self.state_root.join("projects").join(project_id) {
            bail!("refusing to remove state outside a registered checkout directory");
        }
        let root = spec.state_root.clone();
        let parent = self.state_root.join("projects").canonicalize();
        let Ok(parent) = parent else {
            return Ok(());
        };
        let expected = parent.join(project_id);
        if root != expected
            || std::fs::symlink_metadata(&root)
                .is_err_and(|error| error.kind() != std::io::ErrorKind::NotFound)
        {
            bail!("refusing to remove state outside the project state directory");
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&root) {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("refusing to remove an unverified checkout state path");
            }
            let canonical = root.canonicalize()?;
            if canonical != root {
                bail!("refusing to remove checkout state through a changed path");
            }
            let marker = root.join("project-owner.v1.json");
            let marker_metadata = std::fs::symlink_metadata(&marker)?;
            if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
                bail!("refusing to remove checkout state without an ownership marker");
            }
            let owner: Value = serde_json::from_slice(&std::fs::read(&marker)?)?;
            if owner["project_id"] != *project_id
                || owner["root_hash"] != spec.root_hash
                || owner["schema"] != "context_project_state.owner.v1"
                || owner["repository_id"] != spec.repository_id.clone().unwrap_or_default()
            {
                bail!("checkout state ownership marker does not match the project registry");
            }
            std::fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    fn host_path_for_local(&self, local_path: &Path) -> Option<PathBuf> {
        self.root_mappings
            .iter()
            .filter_map(|(host, local)| {
                local_path
                    .strip_prefix(local)
                    .ok()
                    .map(|relative| (host, relative, local.as_os_str().len()))
            })
            .max_by_key(|(_, _, local_len)| *local_len)
            .map(|(host, relative, _)| host.join(relative))
            .or_else(|| Some(local_path.to_owned()))
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
            "repository_id": self.repository_id,
            "root": {"uri_hash": self.root_hash, "scheme": "file", "mapped": self.mapped},
            "state": {
                "state_key": if self.legacy {"rust-v2".to_owned()} else {format!("projects/{}", self.project_id)},
                "exists": self.state_root.exists(),
                "store_exists": self.state_root.join("rust-v2/state.lmdb/data.mdb").exists(),
                "index_exists": self.state_root.join("rust-v2/index").exists(),
                "memory_exists": self.state_root.join("rust-v2/state.lmdb/data.mdb").exists(),
                "repo_boundary_enforced": true,
            },
            "git": {
                "is_repo": self.git_head.is_some(),
                "available": self.git_head.is_some(),
                "head": self.git_head,
                "branch": self.git_branch,
                "common_dir_hash": self.repository_id,
                "status_hash": "",
                "changes_hash": "",
                "dirty": false,
            },
        })
    }
}

fn normalize_common_dir_for_mappings(
    common_dir: &Path,
    mappings: &[(PathBuf, PathBuf)],
) -> PathBuf {
    mappings
        .iter()
        .filter_map(|(host, local)| {
            common_dir
                .strip_prefix(local)
                .ok()
                .map(|relative| (host, relative, local.as_os_str().len()))
        })
        .max_by_key(|(_, _, local_len)| *local_len)
        .map(|(host, relative, _)| host.join(relative))
        .unwrap_or_else(|| common_dir.to_owned())
}

fn repository_id(common_dir: &Path) -> String {
    format!(
        "repo-{}",
        &sha256_hex(common_dir.to_string_lossy().as_bytes())[..24]
    )
}

fn local_repository_id(root_hash: &str) -> String {
    format!("local-{}", &sha256_hex(root_hash.as_bytes())[..24])
}

fn confirmed_missing_root(spec: &ProjectSpec) -> bool {
    matches!(
        std::fs::symlink_metadata(&spec.local_root),
        Err(ref error) if error.kind() == std::io::ErrorKind::NotFound
    ) && spec.local_root.parent().is_some_and(Path::is_dir)
}

fn build_project_engine(
    spec: &ProjectSpec,
    wiki_state: Option<&Path>,
    git_mappings: Vec<(PathBuf, PathBuf)>,
    usage_monitor: Arc<UsageMonitor>,
    control: Option<&WorkControl>,
) -> Result<ProjectEngine> {
    if let (Some(repository_id), Some(wiki_state)) = (&spec.repository_id, wiki_state) {
        ProjectEngine::build_with_repository_wiki(
            &spec.local_root,
            &spec.state_root,
            wiki_state,
            &spec.project_id,
            repository_id,
            git_mappings,
            usage_monitor,
            control,
        )
    } else {
        match control {
            Some(control) => ProjectEngine::build_with_state_and_monitor_controlled(
                &spec.local_root,
                &spec.state_root,
                &spec.project_id,
                usage_monitor,
                Some(control),
            ),
            None => ProjectEngine::build_with_state_and_monitor(
                &spec.local_root,
                &spec.state_root,
                &spec.project_id,
                usage_monitor,
            ),
        }
    }
}

fn ensure_checkout_state_marker(spec: &ProjectSpec) -> Result<()> {
    if spec.legacy {
        return Ok(());
    }
    std::fs::create_dir_all(&spec.state_root)?;
    let marker = spec.state_root.join("project-owner.v1.json");
    let expected = json!({
        "schema":"context_project_state.owner.v1",
        "project_id":&spec.project_id,
        "root_hash":&spec.root_hash,
        "repository_id":spec.repository_id.clone().unwrap_or_default(),
    });
    match std::fs::symlink_metadata(&marker) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("project state ownership marker is unsafe");
            }
            let current: Value = serde_json::from_slice(&std::fs::read(marker)?)?;
            if current != expected {
                bail!("project state directory belongs to a different checkout");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let temporary = spec.state_root.join(".project-owner.v1.tmp");
            std::fs::write(&temporary, serde_json::to_vec(&expected)?)?;
            std::fs::rename(temporary, marker)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
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

fn derived_project_id(root: &Path, canonical_uri: &str) -> String {
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project");
    format!(
        "{}-{}",
        slug(name),
        &sha256_hex(canonical_uri.as_bytes())[..12]
    )
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
    use std::time::{Duration, Instant};

    #[test]
    fn external_checkout_requires_git_registration_and_cached_roots_are_rechecked() {
        use std::process::Command;

        let fixture = tempfile::tempdir().unwrap();
        let base = fixture.path().join("base");
        let linked = fixture.path().join("outside linked");
        let forged = fixture.path().join("forged");
        std::fs::create_dir_all(&base).unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(&base)
            .status()
            .unwrap();
        Command::new("git")
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ])
            .current_dir(&base)
            .status()
            .unwrap();
        Command::new("git")
            .args([
                "worktree",
                "add",
                "-q",
                "-b",
                "linked",
                linked.to_str().unwrap(),
            ])
            .current_dir(&base)
            .status()
            .unwrap();
        std::fs::create_dir(&forged).unwrap();
        std::fs::write(
            forged.join(".git"),
            format!("gitdir: {}\n", base.join(".git").display()),
        )
        .unwrap();

        let registry =
            super::ProjectRegistry::new(base.clone(), fixture.path().join("state"), vec![], vec![])
                .unwrap();
        let linked_engine = registry
            .engine_for(None, Some(linked.to_str().unwrap()))
            .unwrap();
        assert!(
            registry
                .engine_for(None, Some(forged.to_str().unwrap()))
                .is_err()
        );

        let project_id = linked_engine.project_id().to_owned();
        drop(linked_engine);
        Command::new("git")
            .args(["worktree", "remove", "--force", linked.to_str().unwrap()])
            .current_dir(&base)
            .status()
            .unwrap();
        assert!(registry.engine_for(Some(&project_id), None).is_err());
    }

    #[test]
    fn missing_project_cleanup_observes_grace_and_cancels_on_restore() {
        let fixture = tempfile::tempdir().unwrap();
        let workspace = fixture.path().join("workspace");
        let base = workspace.join("base");
        let child = workspace.join("child");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&child).unwrap();
        let registry = super::ProjectRegistry::new(
            base,
            fixture.path().join("state"),
            vec![workspace],
            vec![],
        )
        .unwrap();
        let spec = registry
            .project_from_uri(&super::canonical_file_uri(&child).unwrap(), "root_uri")
            .unwrap();
        let id = spec.project_id.clone();
        let index = spec.state_root.join("rust-v2/index");
        super::ensure_checkout_state_marker(&spec).unwrap();
        std::fs::create_dir_all(&index).unwrap();
        registry
            .state
            .lock()
            .unwrap()
            .specs
            .insert(id.clone(), spec);
        let now = Instant::now();
        std::fs::remove_dir_all(&child).unwrap();

        registry.reconcile_missing_at(now).unwrap();
        assert!(registry.state.lock().unwrap().specs.contains_key(&id));
        std::fs::create_dir(&child).unwrap();
        registry
            .reconcile_missing_at(now + Duration::from_secs(6 * 60))
            .unwrap();
        assert!(registry.state.lock().unwrap().specs.contains_key(&id));
        assert!(index.is_dir());

        std::fs::remove_dir_all(&child).unwrap();
        let removed_at = now + Duration::from_secs(7 * 60);
        registry.reconcile_missing_at(removed_at).unwrap();
        registry
            .reconcile_missing_at(removed_at + Duration::from_secs(5 * 60))
            .unwrap();
        assert!(!registry.state.lock().unwrap().specs.contains_key(&id));
        assert!(!index.exists());
    }

    #[test]
    fn archived_wiki_references_resolve_after_last_project_is_removed() {
        let fixture = tempfile::tempdir().unwrap();
        let workspace = fixture.path().join("workspace");
        let base = workspace.join("base");
        std::fs::create_dir_all(&base).unwrap();
        let state_root = fixture.path().join("state");
        let registry =
            super::ProjectRegistry::new(base, state_root.clone(), vec![workspace], vec![]).unwrap();
        let repository_id = "repo-archive";
        let wiki_state = state_root
            .join("repositories")
            .join(repository_id)
            .join("wiki");
        let store = StateStore::open(&wiki_state).unwrap();
        let body =
            json!({"title":"Archived","summary":"citation survives","claims":[],"sources":[]});
        let revision = store
            .put_wiki_revision("archived-page", None, &body)
            .unwrap();
        let request: ResultReferenceRequest = serde_json::from_value(json!({
            "reference":{"kind":"wiki","project_id":"deleted-checkout","repository_id":repository_id,
                "page_id":"archived-page","revision":revision}
        }))
        .unwrap();

        let resolved: Value =
            serde_json::from_slice(&registry.result_reference_resolve(&request).unwrap()).unwrap();
        assert_eq!(resolved["status"], "resolved");
        assert_eq!(resolved["source_status"], "archived");
        assert_eq!(resolved["content"]["summary"], "citation survives");
    }

    #[test]
    fn shared_wiki_rechecks_project_boundaries_and_restores_registered_projects() {
        let fixture = tempfile::tempdir().unwrap();
        let workspace = fixture.path().join("workspace");
        let a = workspace.join("alpha");
        let b = workspace.join("beta");
        let outside = fixture.path().join("outside");
        for root in [&a, &b, &outside] {
            std::fs::create_dir_all(root).unwrap();
        }
        std::fs::write(a.join("policy.md"), "auroracontract timeout is 400ms\n").unwrap();
        let state = fixture.path().join("state");
        let registry =
            super::ProjectRegistry::new(a.clone(), state.clone(), vec![workspace.clone()], vec![])
                .unwrap();
        let alpha = registry.engine_for(None, None).unwrap();
        let beta = registry
            .engine_for(None, Some(b.to_str().unwrap()))
            .unwrap();
        let index = alpha.index();
        let (content, _) = index.file_content("policy.md", 4096).unwrap();
        let hash = super::sha256_hex(content.as_bytes());
        let write=serde_json::from_value(serde_json::json!({"mode":"wiki_upsert","wiki":{
            "page_id":"aurora","title":"auroracontract","summary":"auroracontract timeout is 400ms",
            "claims":[{"text":"auroracontract timeout is 400ms","status":"verified","citations":[{"source_id":"policy"}]}],
            "sources":[{"id":"policy","kind":"markdown","path":"policy.md","sha256":hash,"fingerprint":index.snapshot_fingerprint("policy.md")}]
        }})).unwrap();
        let published: serde_json::Value =
            serde_json::from_slice(&alpha.context_memory(&write).unwrap()).unwrap();
        let request=serde_json::from_value(serde_json::json!({"mode":"wiki_search","query":"auroracontract","project_id":beta.project_id()})).unwrap();
        let search = registry.shared_wiki_search(&request).unwrap();
        assert!(
            search["results"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["project_id"] == alpha.project_id())
        );
        assert!(
            registry
                .engine_for(None, Some(outside.to_str().unwrap()))
                .is_err()
        );
        let reference =
            serde_json::from_value(serde_json::json!({"reference":published["reference"]}))
                .unwrap();
        registry
            .result_reference_resolve(&reference)
            .unwrap_or_else(|error| panic!("resolve failed: {error:#}"));
        let mismatch = serde_json::from_value(
            serde_json::json!({"reference":published["reference"],"root_uri":b}),
        )
        .unwrap();
        assert!(registry.result_reference_resolve(&mismatch).is_err());
        drop(index);
        drop(alpha);
        drop(beta);
        drop(registry);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let restarted = loop {
            match super::ProjectRegistry::new(
                a.clone(),
                state.clone(),
                vec![workspace.clone()],
                vec![],
            ) {
                Ok(registry) => break registry,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20))
                }
                Err(error) => panic!("wiki handles did not close: {error}"),
            }
        };
        restarted.engine_for(None, None).unwrap();
        restarted.restore_wiki_projects().unwrap();
        let restored = restarted.shared_wiki_search(&request).unwrap();
        assert!(
            restored["results"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["page_id"] == "aurora")
        );
    }
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
        let evicted_uri = canonical_file_uri(&repositories[2]).expect("evicted URI");
        drop(
            registry
                .engine_for(None, Some(&evicted_uri))
                .expect("wiki views must permit reloading an evicted engine"),
        );

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
    fn mapped_default_root_reuses_default_engine_and_legacy_aliases() {
        let fixture = tempfile::tempdir().expect("fixture root");
        let local_root = fixture.path().join("workspace");
        let host_root = fixture.path().join("host-workspace");
        let unrelated_host_root = host_root.join("unrelated");
        std::fs::create_dir_all(&local_root).expect("local root");
        std::fs::create_dir_all(&host_root).expect("host root");
        std::fs::create_dir_all(local_root.join("unrelated")).expect("unrelated local root");
        std::fs::create_dir_all(&unrelated_host_root).expect("unrelated host root");
        std::fs::write(local_root.join("Cargo.toml"), "[workspace]\n").expect("marker");
        let registry = ProjectRegistry::new(
            local_root.clone(),
            fixture.path().join("state"),
            vec![host_root.clone()],
            vec![(host_root.clone(), local_root.clone())],
        )
        .expect("registry");

        let default = registry.default_engine().expect("default engine");
        let explicit_uri = canonical_file_uri(&host_root).expect("mapped URI");
        let explicit = registry
            .engine_for(None, Some(&explicit_uri))
            .expect("mapped explicit engine");
        assert!(Arc::ptr_eq(&default, &explicit));
        assert_eq!(registry.cached_project_ids().expect("cached ids").len(), 1);
        let projects = registry.projects_payload(100).expect("project catalogue");
        assert_eq!(projects["count"], 1);
        assert_eq!(projects["projects"][0]["project_id"], default.project_id());
        assert_eq!(projects["projects"][0]["state"]["state_key"], "rust-v2");
        assert!(
            registry
                .engine_for(Some("unrelated-project"), Some(&explicit_uri))
                .is_err()
        );
        let unrelated_uri = canonical_file_uri(&unrelated_host_root).expect("unrelated URI");
        assert!(
            registry
                .engine_for(Some(&registry.default_project_id), Some(&unrelated_uri))
                .is_err()
        );

        let alias = derived_project_id(
            &host_root,
            &canonical_file_uri(&host_root).expect("alias URI"),
        );
        let aliased = registry
            .engine_for(Some(&alias), None)
            .expect("legacy alias engine");
        assert!(Arc::ptr_eq(&default, &aliased));

        let reverse_registry = ProjectRegistry::new(
            local_root.clone(),
            fixture.path().join("reverse-state"),
            vec![host_root.clone()],
            vec![(host_root.clone(), local_root.clone())],
        )
        .expect("reverse registry");
        let explicit_first = reverse_registry
            .engine_for(None, Some(&explicit_uri))
            .expect("explicit-first engine");
        let default_second = reverse_registry
            .default_engine()
            .expect("default-second engine");
        assert!(Arc::ptr_eq(&explicit_first, &default_second));

        let local_parent = fixture.path().join("local-parent");
        let nested_local = local_parent.join("nested");
        let host_parent = fixture.path().join("host-parent");
        let nested_host = host_parent.join("nested");
        std::fs::create_dir_all(&nested_local).expect("nested local root");
        std::fs::create_dir_all(&nested_host).expect("nested host root");
        let parent_registry = ProjectRegistry::new(
            nested_local.clone(),
            fixture.path().join("parent-state"),
            vec![host_parent.clone()],
            vec![(host_parent, local_parent)],
        )
        .expect("parent mapping registry");
        let nested_uri = canonical_file_uri(&nested_host).expect("nested mapped URI");
        assert_eq!(
            parent_registry
                .select_project(None, Some(&nested_uri))
                .expect("nested default selection"),
            parent_registry.default_project_id
        );
        let nested_alias = derived_project_id(
            &nested_host,
            &canonical_file_uri(&nested_host).expect("nested alias URI"),
        );
        assert_eq!(
            parent_registry
                .select_project(Some(&nested_alias), None)
                .expect("nested alias selection"),
            parent_registry.default_project_id
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

        let active = registry
            .active_projects_payload(100)
            .expect("active projects");
        assert_eq!(active["schema"], "context_projects.active.v1");
        assert_eq!(active["count"], 0);

        drop(registry.default_engine().expect("active default engine"));
        let active = registry
            .active_projects_payload(100)
            .expect("active projects");
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

        let cached = registry
            .cached_projects_payload(100)
            .expect("cached projects");
        assert_eq!(cached["schema"], "context_projects.cached.v1");
        assert_eq!(cached["catalogue"], "resident_engines_only");
        assert_eq!(cached["count"], 0);

        let discovered = registry.projects_payload(100).expect("explicit discovery");
        assert_eq!(discovered["count"], 1);
        let cached = registry
            .cached_projects_payload(100)
            .expect("cached projects");
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
    fn project_catalogue_cap_validates_and_reports_exact_and_truncated_results() {
        let payload = |projects: Value| {
            json!({
                "schema": "context_projects.list.v1",
                "count": projects.as_array().map_or(0, Vec::len),
                "projects": projects,
            })
        };

        for invalid in [0, 1001] {
            let error = ProjectRegistry::bounded_project_catalog(payload(json!([])), invalid)
                .expect_err("invalid catalogue cap");
            assert_eq!(error.to_string(), "max_entries must be in 1..=1000");
        }

        let one =
            ProjectRegistry::bounded_project_catalog(payload(json!([{"project_id": "alpha"}])), 1)
                .expect("one-row catalogue");
        assert_eq!(one["total_count"], 1);
        assert_eq!(one["returned_count"], 1);
        assert_eq!(one["omitted_count"], 0);
        assert_eq!(one["truncated"], false);

        let exact = ProjectRegistry::bounded_project_catalog(
            payload(json!([
                {"project_id": "alpha"},
                {"project_id": "beta"}
            ])),
            2,
        )
        .expect("exact-size catalogue");
        assert_eq!(exact["count"], 2);
        assert_eq!(exact["total_count"], 2);
        assert_eq!(exact["returned_count"], 2);
        assert_eq!(exact["truncated"], false);

        let truncated = ProjectRegistry::bounded_project_catalog(
            payload(json!([
                {"project_id": "alpha"},
                {"project_id": "beta"}
            ])),
            1,
        )
        .expect("truncated catalogue");
        assert_eq!(truncated["count"], 1);
        assert_eq!(truncated["total_count"], 2);
        assert_eq!(truncated["returned_count"], 1);
        assert_eq!(truncated["omitted_count"], 1);
        assert_eq!(truncated["max_entries"], 1);
        assert_eq!(truncated["truncated"], true);
        assert_eq!(truncated["projects"][0]["project_id"], "alpha");
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
        let projects = registry.projects_payload(100).expect("project list");
        assert_eq!(projects["count"], 1);
        assert_eq!(projects["projects"][0]["name"], "real-project");
        assert_eq!(projects["projects"][0]["source"], "discovered_git");
    }
}
