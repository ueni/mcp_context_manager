use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use context_core::{ContextPackRejectionClass, ProjectEngine, UsageMonitor};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;
use walkdir::{DirEntry, WalkDir};

const ENGINE_CAPACITY: usize = 8;
const DISCOVERY_MAX_PROJECTS: usize = 100;
const DEFAULT_DISCOVERY_MAX_DEPTH: usize = 4;
const PROJECT_CATALOG_FILE: &str = "project-catalog.v1.json";
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
    state: Arc<Mutex<RegistryState>>,
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
        };
        let mut specs = HashMap::new();
        specs.insert(project_id.clone(), default_spec);
        let usage_monitor = Arc::new(UsageMonitor::open(state_root.join("global-monitor"))?);
        Ok(Self {
            base_root,
            state_root,
            allowed_roots,
            root_mappings,
            discovery_max_depth: discovery_max_depth_from_env(),
            project_markers: project_markers_from_env(),
            default_project_id: project_id,
            usage_monitor,
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
        self.engine_for_with_builder(project_id, root_uri, move |spec| {
            ProjectEngine::build_with_state_and_monitor(
                &spec.local_root,
                &spec.state_root,
                &spec.project_id,
                usage_monitor,
            )
        })
    }

    pub fn monitor_usage(&self, action: &str, project_id: Option<&str>) -> Result<Value> {
        self.usage_monitor.action(action, project_id)
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
    let name = entry.file_name().to_string_lossy();
    !entry.file_type().is_dir()
        || (!name.starts_with('.')
            && !matches!(name.as_ref(), "build" | "dist" | "node_modules" | "target"))
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
}
