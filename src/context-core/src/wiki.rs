//! Generated, revision-addressed knowledge and the shared agent search snapshot.
use super::*;
use std::{
    io::Read,
    path::{Path, PathBuf},
};

const WIKI_MONITOR_INTERVAL: StdDuration =
    StdDuration::from_millis(if cfg!(test) { 100 } else { 2_000 });

#[derive(Clone)]
struct Project {
    owner: String,
    repository_id: String,
    branch: Option<String>,
    head: Option<String>,
    git_mappings: Vec<(PathBuf, PathBuf)>,
    store: Arc<StateStore>,
    index: Arc<RwLock<Arc<ProjectIndex>>>,
    freshness: Arc<FreshnessState>,
    refreshing: Arc<AtomicBool>,
    refresh_pending: Arc<AtomicBool>,
    _watcher: Arc<WatchGuard>,
    _refresh: Arc<RefreshCoordinator>,
}

struct Snapshot {
    index: Arc<WikiSearchIndex>,
    documents: BTreeMap<(String, String), Value>,
    projections: BTreeMap<(String, String), String>,
}

/// One generated search projection, shared only by projects admitted by the registry.
pub struct WikiHub {
    pub(crate) epoch: Arc<AtomicU64>,
    projects: RwLock<BTreeMap<String, Project>>,
    snapshot: RwLock<Arc<Snapshot>>,
    publication: Mutex<()>,
    monitoring: AtomicBool,
}

impl WikiHub {
    pub fn new() -> Result<Self> {
        Ok(Self {
            epoch: Arc::new(AtomicU64::new(0)),
            projects: RwLock::new(BTreeMap::new()),
            snapshot: RwLock::new(Arc::new(Snapshot {
                index: Arc::new(WikiSearchIndex::build(&[])?),
                documents: BTreeMap::new(),
                projections: BTreeMap::new(),
            })),
            publication: Mutex::new(()),
            monitoring: AtomicBool::new(false),
        })
    }

    pub fn generation(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub(crate) fn register(self: &Arc<Self>, engine: &ProjectEngine) -> Result<()> {
        let mut projects = self
            .projects
            .write()
            .map_err(|_| anyhow!("wiki projects lock poisoned"))?;
        if projects
            .get(engine.project_id())
            .is_some_and(|project| Arc::ptr_eq(&project.index, &engine.index))
        {
            return Ok(());
        }
        let refreshing = projects
            .get(engine.project_id())
            .map(|previous| {
                engine.freshness.generation.fetch_add(1, Ordering::AcqRel);
                Arc::clone(&previous.refreshing)
            })
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let refresh_pending = projects
            .get(engine.project_id())
            .map(|previous| Arc::clone(&previous.refresh_pending))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let git_identity =
            context_index::git::checkout_with_mappings(engine.index().root(), &engine.git_mappings)
                .ok();
        projects.insert(
            engine.project_id.clone(),
            Project {
                owner: engine.project_id.clone(),
                repository_id: engine.repository_id.clone(),
                branch: git_identity
                    .as_ref()
                    .and_then(|identity| identity.branch.clone()),
                head: git_identity.map(|identity| identity.head),
                git_mappings: engine.git_mappings.clone(),
                store: Arc::clone(&engine.wiki_store),
                index: Arc::clone(&engine.index),
                freshness: Arc::clone(&engine.freshness),
                refreshing,
                refresh_pending,
                _watcher: Arc::clone(&engine._watcher),
                _refresh: Arc::clone(&engine.refresh_coordinator),
            },
        );
        *engine
            .freshness
            .wiki_epoch
            .lock()
            .map_err(|_| anyhow!("wiki epoch lock poisoned"))? = Some(Arc::clone(&self.epoch));
        drop(projects);
        if !self.monitoring.swap(true, Ordering::AcqRel) {
            let weak = Arc::downgrade(self);
            thread::Builder::new()
                .name("wiki-source-watch".into())
                .spawn(move || {
                    let mut observed = BTreeMap::new();
                    loop {
                        let Some(hub) = weak.upgrade() else {
                            break;
                        };
                        let projects = hub
                            .projects
                            .read()
                            .map(|projects| projects.clone())
                            .unwrap_or_default();
                        for (id, project) in projects {
                            if project.freshness.activity_state() == "dormant" {
                                continue;
                            }
                            let repository = project
                                .index
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone();
                            let git = git_head_stamp(repository.root(), &project.git_mappings);
                            let stamp = (project.freshness.generation.load(Ordering::Acquire), git);
                            let deferred_work = project.refresh_pending.load(Ordering::Acquire);
                            if (observed.get(&id) != Some(&stamp) || deferred_work)
                                && !project.refreshing.load(Ordering::Acquire)
                            {
                                observed.insert(id.clone(), stamp);
                                hub.schedule_refresh(&id);
                            }
                        }
                        drop(hub);
                        thread::sleep(WIKI_MONITOR_INTERVAL);
                    }
                })?;
        }
        self.publish(&engine.project_id)?;
        self.schedule_refresh(&engine.project_id);
        Ok(())
    }

    pub fn unregister(&self, owner: &str) -> Result<()> {
        let _publication = self
            .publication
            .lock()
            .map_err(|_| anyhow!("wiki publication lock poisoned"))?;
        let removed_project = self
            .projects
            .write()
            .map_err(|_| anyhow!("wiki projects lock poisoned"))?
            .remove(owner);
        let old = self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))?
            .clone();
        let mut documents = old
            .documents
            .iter()
            .filter(|(key, _)| {
                key.0 != owner
                    && old
                        .projections
                        .get(*key)
                        .is_none_or(|source| source != owner)
            })
            .map(|(key, page)| (key.clone(), page.clone()))
            .collect::<BTreeMap<_, _>>();
        if documents == old.documents {
            return Ok(());
        }
        let projections = old
            .projections
            .iter()
            .filter(|(key, source)| {
                key.0 != owner && source.as_str() != owner && documents.contains_key(*key)
            })
            .map(|(key, source)| (key.clone(), source.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut projections = projections;
        if let Some(removed_project) = removed_project {
            let removed_keys = old
                .projections
                .iter()
                .filter(|(_, source)| source.as_str() == owner)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let projects = self
                .projects
                .read()
                .map_err(|_| anyhow!("wiki projects lock poisoned"))?
                .clone();
            for (target_owner, page_id) in removed_keys {
                if documents.contains_key(&(target_owner.clone(), page_id.clone())) {
                    continue;
                }
                let Some(target) = projects.get(&target_owner) else {
                    continue;
                };
                if target.repository_id != removed_project.repository_id {
                    continue;
                }
                let mut fallback = None;
                for (candidate_owner, candidate) in &projects {
                    if candidate_owner == &target_owner
                        || candidate.repository_id != target.repository_id
                    {
                        continue;
                    }
                    if let Some(page) = native_page_for(candidate, candidate_owner, &page_id)? {
                        fallback = Some((candidate_owner.clone(), page));
                        break;
                    }
                }
                if let Some((source_owner, page)) = fallback {
                    let key = (target_owner.clone(), page_id);
                    let mut page = page;
                    page["project_id"] = json!(target_owner);
                    documents.insert(key.clone(), page);
                    projections.insert(key, source_owner);
                }
            }
        }
        if documents == old.documents {
            if projections != old.projections {
                *self
                    .snapshot
                    .write()
                    .map_err(|_| anyhow!("wiki snapshot lock poisoned"))? = Arc::new(Snapshot {
                    index: Arc::clone(&old.index),
                    documents,
                    projections,
                });
            }
            return Ok(());
        }
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let index = Arc::new(WikiSearchIndex::build(
            &documents.values().cloned().collect::<Vec<_>>(),
        )?);
        *self
            .snapshot
            .write()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))? = Arc::new(Snapshot {
            index,
            documents,
            projections,
        });
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Rebuild on the write/background path; query readers retain the old immutable snapshot.
    pub fn publish(&self, owner: &str) -> Result<()> {
        let _publication = self
            .publication
            .lock()
            .map_err(|_| anyhow!("wiki publication lock poisoned"))?;
        let project = self
            .projects
            .read()
            .map_err(|_| anyhow!("wiki projects lock poisoned"))?
            .get(owner)
            .cloned()
            .ok_or_else(|| anyhow!("wiki project is not authorized"))?;
        project
            .freshness
            .wiki_publication_attempted
            .fetch_add(1, Ordering::Relaxed);
        let old = self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))?
            .clone();
        let mut documents = old.documents.clone();
        documents.retain(|key, _| {
            key.0 != owner
                || old
                    .projections
                    .get(key)
                    .is_some_and(|source_owner| source_owner != owner)
        });
        let mut projections = old.projections.clone();
        let previous_projections = projections
            .iter()
            .filter(|(_, source)| source.as_str() == owner)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in previous_projections {
            projections.remove(&key);
            documents.remove(&key);
        }
        for pointer in project.store.list_wiki_pages(100_001)? {
            let id = pointer["page_id"]
                .as_str()
                .ok_or_else(|| anyhow!("invalid wiki pointer"))?;
            let revision = pointer["revision"]
                .as_str()
                .ok_or_else(|| anyhow!("invalid wiki pointer"))?;
            let mut page = project
                .store
                .get_wiki_revision(id, revision)?
                .ok_or_else(|| anyhow!("missing wiki revision"))?;
            if !page.is_object() {
                bail!("wiki revision is not a structured page");
            }
            page["page_id"] = json!(id);
            page["revision"] = json!(revision);
            page["project_id"] = json!(owner);
            page["repository_id"] = json!(project.repository_id);
            page["current"] = json!(page.get("superseded_by").is_none());
            page["prompt_injection_signals"] = sanitize_text(&serde_json::to_string(&page)?).2;
            let owner_key = (owner.to_owned(), id.to_owned());
            projections.remove(&owner_key);
            documents.insert(owner_key, page.clone());
            let peer_projects = self
                .projects
                .read()
                .map_err(|_| anyhow!("wiki projects lock poisoned"))?
                .values()
                .filter(|peer| peer.owner != owner && peer.repository_id == project.repository_id)
                .map(|peer| {
                    (
                        peer.owner.clone(),
                        same_store_location(&peer.store, &project.store),
                    )
                })
                .collect::<Vec<_>>();
            for (peer_owner, shared_store) in peer_projects {
                let key = (peer_owner.clone(), id.to_owned());
                if old.documents.contains_key(&key)
                    && !old.projections.contains_key(&key)
                    && !shared_store
                {
                    continue;
                }
                let mut projected_page = page.clone();
                projected_page["project_id"] = json!(peer_owner);
                documents.insert(key.clone(), projected_page);
                if shared_store {
                    projections.remove(&key);
                } else {
                    projections.insert(key, owner.to_owned());
                }
            }
        }
        for (_, pointer) in project.store.iter_json("wiki:source_current:")? {
            let Some(source) = project.store.get_json(&source_revision_key(&pointer))? else {
                continue;
            };
            for doc in source_documents(owner, &project.repository_id, &source) {
                let id = doc["page_id"]
                    .as_str()
                    .expect("generated source page id")
                    .to_owned();
                let key = (owner.to_owned(), id);
                documents.insert(key.clone(), doc);
                projections.remove(&key);
            }
        }
        if documents.len() > 1_000_000 {
            bail!("shared wiki exceeds one million indexed chunks");
        }
        if documents == old.documents {
            project
                .freshness
                .wiki_publication_skipped
                .fetch_add(1, Ordering::Relaxed);
            if projections != old.projections {
                *self
                    .snapshot
                    .write()
                    .map_err(|_| anyhow!("wiki snapshot lock poisoned"))? = Arc::new(Snapshot {
                    index: Arc::clone(&old.index),
                    documents,
                    projections,
                });
            }
            return Ok(());
        }
        // Invalidate readers before building so a cached pack cannot race with
        // an in-progress publication. The second increment marks the committed
        // snapshot boundary for readers that waited on the old generation.
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let index = Arc::new(WikiSearchIndex::build(
            &documents.values().cloned().collect::<Vec<_>>(),
        )?);
        *self
            .snapshot
            .write()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))? = Arc::new(Snapshot {
            index,
            documents,
            projections,
        });
        self.epoch.fetch_add(1, Ordering::AcqRel);
        project
            .freshness
            .wiki_publication_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn search(
        &self,
        query: &str,
        owner: Option<&str>,
        limit: usize,
        for_pack: bool,
    ) -> Result<Vec<Value>> {
        self.search_for_checkout(query, owner, None, limit, for_pack)
    }

    pub fn search_for_checkout(
        &self,
        query: &str,
        owner: Option<&str>,
        requesting_project: Option<&str>,
        limit: usize,
        for_pack: bool,
    ) -> Result<Vec<Value>> {
        if limit == 0 || limit > 100 {
            bail!("wiki search limit must be in 1..=100");
        }
        if query.len() > 4096 {
            bail!("wiki search query exceeds 4096 bytes");
        }
        let snapshot = self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))?
            .clone();
        let projects = self
            .projects
            .read()
            .map_err(|_| anyhow!("wiki projects lock poisoned"))?;
        if owner.is_some_and(|id| !projects.contains_key(id)) {
            bail!("wiki project is not authorized");
        }
        let requesting = requesting_project
            .map(|id| {
                projects
                    .get(id)
                    .ok_or_else(|| anyhow!("requesting wiki project is not authorized"))
            })
            .transpose()?;
        let mut result = Vec::new();
        let mut wire_bytes = 0usize;
        for hit in snapshot.index.search(query, owner, (limit * 8).min(256))? {
            let Some(project) = projects.get(&hit.project_id) else {
                continue;
            };
            let Some(page) = snapshot
                .documents
                .get(&(hit.project_id.clone(), hit.page_id.clone()))
            else {
                continue;
            };
            let source_project = snapshot
                .projections
                .get(&(hit.project_id.clone(), hit.page_id.clone()))
                .and_then(|owner| projects.get(owner))
                .unwrap_or(project);
            let index = source_project
                .index
                .read()
                .map_err(|_| anyhow!("wiki repository lock poisoned"))?;
            let status = if let Some(requesting) = requesting {
                let requesting_index = requesting
                    .index
                    .read()
                    .map_err(|_| anyhow!("wiki repository lock poisoned"))?;
                if applicable_to(
                    page,
                    &requesting.owner,
                    requesting.branch.as_deref(),
                    requesting.head.as_deref(),
                ) {
                    source_status(
                        page,
                        &source_project.store,
                        &requesting_index,
                        &requesting.freshness,
                        &source_project.freshness,
                    )
                } else {
                    "inapplicable"
                }
            } else {
                source_status(
                    page,
                    &source_project.store,
                    &index,
                    &source_project.freshness,
                    &source_project.freshness,
                )
            };
            if for_pack && !matches!(status, "current" | "historical") {
                continue;
            }
            let mut summary = page["summary"].as_str().unwrap_or_default().to_owned();
            truncate_string(&mut summary, 600);
            let reference = page.get("source_reference").cloned().unwrap_or_else(|| json!({
                "kind":"wiki", "project_id":hit.project_id, "repository_id":project.repository_id, "page_id":hit.page_id, "revision":hit.revision,
            }));
            result.push(json!({"project_id":hit.project_id, "page_id":hit.page_id,
                "revision":hit.revision, "title":hit.title, "summary":summary,
                "source_kind":page.get("source_kind").and_then(Value::as_str).unwrap_or("wiki"),
                "source_status":status, "applicability":compact_applicability(page),
                "source_count":page["sources"].as_array().map_or(0,Vec::len), "sources":compact_sources(page), "reference":reference,
                "prompt_injection_signals":page["prompt_injection_signals"],
            }));
            wire_bytes += serde_json::to_vec(result.last().expect("added row"))?.len();
            if wire_bytes > 32_000 {
                result.pop();
                break;
            }
            if result.len() == limit {
                break;
            }
        }
        Ok(result)
    }

    pub fn status(&self) -> Result<Value> {
        let snapshot = self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))?;
        Ok(
            json!({"schema":"agent_wiki.status.v1", "generation":self.generation(),
            "project_count":self.projects.read().map_err(|_| anyhow!("wiki projects lock poisoned"))?.len(),
            "chunk_count":snapshot.documents.len()}),
        )
    }

    pub(crate) fn schedule_refresh(self: &Arc<Self>, owner: &str) {
        let project = self
            .projects
            .read()
            .ok()
            .and_then(|projects| projects.get(owner).cloned());
        let Some(project) = project else {
            return;
        };
        project.refresh_pending.store(true, Ordering::Release);
        if project.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let hub = Arc::clone(self);
        let owner = owner.to_owned();
        let flag = Arc::clone(&project.refreshing);
        let worker_flag = Arc::clone(&flag);
        let pending_flag = Arc::clone(&project.refresh_pending);
        if thread::Builder::new()
            .name("wiki-ingest".into())
            .spawn(move || {
                let mut error_retry_remaining = 1;
                loop {
                    pending_flag.store(false, Ordering::Release);
                    let result =
                        refresh_project_sources(&project, 1_000).and_then(|()| hub.publish(&owner));
                    if result.is_err() {
                        let _ = project.store.put_json(
                            "wiki:ingest_status",
                            &json!({"status":"failed", "reason":"source_refresh_failed"}),
                        );
                        if error_retry_remaining > 0
                            && project.freshness.activity_state() != "dormant"
                            && pending_flag.swap(false, Ordering::AcqRel)
                        {
                            error_retry_remaining -= 1;
                            continue;
                        }
                        worker_flag.store(false, Ordering::Release);
                        return;
                    }
                    if project.freshness.activity_state() == "dormant" {
                        pending_flag.store(true, Ordering::Release);
                        worker_flag.store(false, Ordering::Release);
                        return;
                    }
                    let pending_history = project
                        .store
                        .get_json("wiki:history_status")
                        .ok()
                        .flatten()
                        .and_then(|status| status["remaining_in_batch"].as_u64())
                        .unwrap_or(0)
                        > 0;
                    if pending_flag.swap(false, Ordering::AcqRel) || pending_history {
                        continue;
                    }
                    worker_flag.store(false, Ordering::Release);
                    if !pending_flag.swap(false, Ordering::AcqRel)
                        || worker_flag
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_err()
                    {
                        return;
                    }
                }
            })
            .is_err()
        {
            flag.store(false, Ordering::Release);
        }
    }

    pub fn refresh(&self, owner: &str, history_limit: usize) -> Result<Value> {
        if !(1..=10_000).contains(&history_limit) {
            bail!("history limit must be in 1..=10000");
        }
        let project = self
            .projects
            .read()
            .map_err(|_| anyhow!("wiki projects lock poisoned"))?
            .get(owner)
            .cloned()
            .ok_or_else(|| anyhow!("wiki project is not authorized"))?;
        refresh_project_sources(&project, history_limit)?;
        self.publish(owner)?;
        self.status()
    }
}

fn applicable_to(page: &Value, owner: &str, branch: Option<&str>, head: Option<&str>) -> bool {
    let projects = page["applicability"]["projects"].as_array();
    let project_matches = projects.is_none_or(|values| {
        values.is_empty() || values.iter().any(|value| value.as_str() == Some(owner))
    });
    let branches = page["applicability"]["branches"].as_array();
    let branch_matches = branches.is_none_or(|values| {
        values.is_empty()
            || branch
                .is_some_and(|branch| values.iter().any(|value| value.as_str() == Some(branch)))
    });
    let commit_matches = page["applicability"]["commit"]
        .as_str()
        .is_none_or(|commit| head == Some(commit));
    project_matches && branch_matches && commit_matches
}

fn native_page_for(project: &Project, owner: &str, page_id: &str) -> Result<Option<Value>> {
    let pointer = project
        .store
        .list_wiki_pages(100_001)?
        .into_iter()
        .find(|pointer| pointer["page_id"].as_str() == Some(page_id));
    let Some(pointer) = pointer else {
        return Ok(None);
    };
    let revision = pointer["revision"]
        .as_str()
        .ok_or_else(|| anyhow!("invalid wiki pointer"))?;
    let mut page = project
        .store
        .get_wiki_revision(page_id, revision)?
        .ok_or_else(|| anyhow!("missing wiki revision"))?;
    if !page.is_object() {
        bail!("wiki revision is not a structured page");
    }
    page["page_id"] = json!(page_id);
    page["revision"] = json!(revision);
    page["project_id"] = json!(owner);
    page["repository_id"] = json!(project.repository_id);
    page["current"] = json!(page.get("superseded_by").is_none());
    page["prompt_injection_signals"] = json!(sanitize_text(&serde_json::to_string(&page)?).2);
    Ok(Some(page))
}

fn same_store_location(left: &StateStore, right: &StateStore) -> bool {
    let left = std::fs::canonicalize(&left.paths().overlay)
        .unwrap_or_else(|_| left.paths().overlay.clone());
    let right = std::fs::canonicalize(&right.paths().overlay)
        .unwrap_or_else(|_| right.paths().overlay.clone());
    left == right
}

fn source_revision_key(pointer: &Value) -> String {
    format!(
        "wiki:source_revision:{}:{}",
        pointer["source_id"].as_str().unwrap_or_default(),
        pointer["sha256"].as_str().unwrap_or_default()
    )
}

fn source_status(
    page: &Value,
    store: &StateStore,
    index: &ProjectIndex,
    freshness: &FreshnessState,
    owner_freshness: &FreshnessState,
) -> &'static str {
    if page.get("superseded_by").is_some() {
        return "superseded";
    }
    if page.get("source_kind").is_none() {
        let id = page["page_id"].as_str().unwrap_or_default();
        if store
            .get_wiki_page(id)
            .ok()
            .flatten()
            .is_none_or(|pointer| pointer["revision"] != page["revision"])
        {
            return "superseded";
        }
    }
    if page["source_kind"] == "git" {
        return "historical";
    }
    if page["sources"].as_array().is_some_and(|sources| {
        sources.iter().any(|source| {
            source["kind"] == "markdown"
                || source["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("corpus-"))
        })
    }) && (freshness.dirty.load(Ordering::Acquire)
        || freshness.watcher_requires_verification()
        || freshness.activity_state() == "dormant"
        || owner_freshness.dirty.load(Ordering::Acquire)
        || owner_freshness.watcher_requires_verification()
        || owner_freshness.activity_state() == "dormant")
    {
        return "unverified";
    }
    if let (Some(claims), Some(sources)) = (page["claims"].as_array(), page["sources"].as_array())
        && !claims.is_empty()
        && claims.iter().all(|claim| claim["status"] == "historical")
        && !sources.is_empty()
        && sources.iter().all(|source| {
            let commit = source["commit"].as_str().unwrap_or_default();
            source["kind"] == "git"
                && store
                    .get_json(&format!("wiki:source_current:git-{commit}"))
                    .ok()
                    .flatten()
                    .is_some_and(|record| record["sha256"] == source["sha256"])
        })
    {
        return "historical";
    }
    if wiki_sources_current(page, store, index) {
        "current"
    } else {
        "stale_or_unverified"
    }
}

fn refresh_project_sources(project: &Project, history_limit: usize) -> Result<()> {
    let index = project
        .index
        .read()
        .map_err(|_| anyhow!("wiki repository lock poisoned"))?
        .clone();
    let (corpus_digest, corpus_sources) =
        context_index::governed_reference_sources(index.root(), &project.owner)?;
    if corpus_digest != index.reference_signature() {
        bail!("reference source generation changed; retry background refresh");
    }
    let mut seen = HashSet::new();
    for mut source in corpus_sources {
        let text = sanitize_text(source["text"].as_str().unwrap_or_default()).0;
        source["text"] = json!(text);
        source["sha256"] = json!(sha256_text(&text));
        source["source_id"] = json!(format!(
            "corpus-{}-{}",
            &sha256_text(&project.owner)[..12],
            &sha256_text(source["source_id"].as_str().unwrap_or_default())[..24]
        ));
        seen.insert(source["source_id"].as_str().unwrap_or_default().to_owned());
        source["corpus_digest"] = json!(corpus_digest);
        let (source, _) = sanitize_json_value(source);
        put_source(&project.store, &source)?;
    }
    for (path, text) in index.markdown_documents() {
        let id = format!(
            "md-{}-{}",
            &sha256_text(&project.owner)[..12],
            &sha256_text(&format!("{}:{path}", project.owner))[..24]
        );
        seen.insert(id.clone());
        let (text, _, _) = sanitize_text(text);
        let source = json!({"source_id":id,"kind":"markdown","title":path,"text":text,
            "sha256":sha256_text(&text),"path":path,"fingerprint":index.snapshot_fingerprint(path),"freshness":"current"});
        put_source(&project.store, &source)?;
    }
    // Deleted Markdown remains available by revision, but its current pointer is stale.
    let owner_hash = &sha256_text(&project.owner)[..12];
    let markdown_prefix = format!("md-{owner_hash}-");
    let corpus_prefix = format!("corpus-{owner_hash}-");
    for (key, mut pointer) in project.store.iter_json("wiki:source_current:")? {
        let source_id = pointer["source_id"].as_str().unwrap_or_default();
        if (source_id.starts_with(&markdown_prefix) || source_id.starts_with(&corpus_prefix))
            && !seen.contains(source_id)
        {
            pointer["freshness"] = json!("stale");
            project.store.put_json(&key, &pointer)?;
        }
    }
    if index.root().join(".git").exists() {
        let git = context_index::git::checkout_with_mappings(index.root(), &project.git_mappings)?;
        let limit = history_limit.to_string();
        let commits = run_git(
            index.root(),
            &["log", "--format=%H", "-n", &limit, "HEAD"],
            800_000,
            Some(&git.git_dir),
        )?;
        let started = Instant::now();
        let mut imported = 0usize;
        let mut processed = 0usize;
        for commit in commits.lines() {
            current_work_checkpoint()?;
            if started.elapsed() > StdDuration::from_secs(30) {
                break;
            }
            processed += 1;
            let id = format!("git-{commit}");
            if project
                .store
                .get_json(&format!("wiki:source_current:{id}"))?
                .is_some()
            {
                continue;
            }
            let body = run_git(
                index.root(),
                &[
                    "show",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-renames",
                    "--format=%H%n%s%n%b",
                    "--patch",
                    "--stat",
                    commit,
                    "--",
                    ".",
                ],
                32_768,
                Some(&git.git_dir),
            )?;
            let (text, _, _) = sanitize_text(&body);
            let title = text.lines().nth(1).unwrap_or("Historical change");
            put_source(
                &project.store,
                &json!({"source_id":id,"kind":"git","title":title,"text":text,
                "sha256":sha256_text(&text),"commit":commit,"freshness":"historical","truncated":body.len() >= 32_768}),
            )?;
            imported += 1;
        }
        project.store.put_json("wiki:history_status", &json!({"limit":history_limit,"imported_this_batch":imported,"reachable_batch":commits.lines().count(),"remaining_in_batch":commits.lines().count().saturating_sub(processed),"resumable":true}))?;
    }
    project
        .store
        .put_json("wiki:ingest_status", &json!({"status":"ready"}))?;
    Ok(())
}

fn run_git(
    root: &Path,
    arguments: &[&str],
    bound: usize,
    git_dir: Option<&Path>,
) -> Result<String> {
    let mut command = Command::new("git");
    command
        .current_dir(root)
        .args(["--no-pager", "-c", "core.quotePath=true"])
        .args(arguments)
        .args(if arguments.contains(&"--") {
            git_exclude_pathspecs()
        } else {
            Vec::new()
        })
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(git_dir) = git_dir {
        command.env("GIT_DIR", git_dir).env("GIT_WORK_TREE", root);
    }
    let mut child = command.spawn()?;
    let output = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("git output unavailable"))?;
    let reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut output = output;
        output.by_ref().take(bound as u64).read_to_end(&mut bytes)?;
        std::io::copy(&mut output, &mut std::io::sink())?;
        Ok::<_, std::io::Error>(bytes)
    });
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                let _ = reader.join();
                bail!("bounded Git history command failed");
            }
            break;
        }
        if start.elapsed() > StdDuration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            bail!("bounded Git history command timed out");
        }
        thread::sleep(StdDuration::from_millis(5));
    }
    Ok(
        String::from_utf8_lossy(&reader.join().map_err(|_| anyhow!("git reader failed"))??)
            .into_owned(),
    )
}

fn put_source(store: &StateStore, source: &Value) -> Result<()> {
    let mut pointer = json!({"source_id":source["source_id"],"sha256":source["sha256"],"freshness":source["freshness"]});
    if let Some(digest) = source.get("corpus_digest") {
        pointer["corpus_digest"] = digest.clone();
    }
    let current_key = format!(
        "wiki:source_current:{}",
        source["source_id"].as_str().unwrap_or_default()
    );
    if store.get_json(&current_key)?.as_ref() == Some(&pointer) {
        return Ok(());
    }
    let revision_key = source_revision_key(&pointer);
    let immutable = store
        .get_json(&revision_key)?
        .unwrap_or_else(|| source.clone());
    store.put_json_batch(&[(revision_key, immutable), (current_key, pointer)])
}

fn source_documents(owner: &str, repository_id: &str, source: &Value) -> Vec<Value> {
    let text = source["text"].as_str().unwrap_or_default();
    let mut result = Vec::new();
    let mut offset = 0usize;
    let mut line = 1usize;
    while offset < text.len() {
        let mut end = (offset + 1800).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        if end < text.len() {
            let part = &text[offset..end];
            if let Some(position) = part
                .rfind(char::is_whitespace)
                .filter(|position| *position >= 900)
            {
                end = offset + position + part[position..].chars().next().unwrap().len_utf8();
            }
        }
        let fragment = &text[offset..end];
        let newlines = fragment.bytes().filter(|byte| *byte == b'\n').count();
        let last_line = line + newlines - usize::from(fragment.ends_with('\n'));
        result.push(source_document(
            owner,
            repository_id,
            source,
            line,
            last_line,
            fragment,
            (offset, end),
        ));
        line += newlines;
        offset = end;
    }
    result
}

fn source_document(
    owner: &str,
    repository_id: &str,
    source: &Value,
    start: usize,
    end: usize,
    text: &str,
    bytes: (usize, usize),
) -> Value {
    let kind = source["kind"].as_str().unwrap_or_default();
    let id = source["source_id"].as_str().unwrap_or_default();
    let mut citation = json!({"id":id,"kind":kind,"sha256":source["sha256"]});
    for field in [
        "path",
        "fingerprint",
        "commit",
        "publisher",
        "version",
        "retrieved_at",
        "rights",
    ] {
        if let Some(value) = source.get(field) {
            citation[field] = value.clone();
        }
    }
    let injection = sanitize_text(&format!("{}\n{text}\n{citation}", source["title"])).2;
    json!({"page_id":format!("source-{id}-{start}-{}",bytes.0),"project_id":owner,"repository_id":repository_id,"revision":source["sha256"],"prompt_injection_signals":injection,
        "title":source["title"],"summary":text,"current":true,"source_kind":kind,
        "sources":[citation],"claims":[{"text":text,"status":if kind=="git" {"historical"} else {"verified"},"citations":[{"source_id":id}]}],
        "applicability":{"projects":[owner],"commit":source.get("commit"),"version":source.get("version")},
        "source_reference":{"kind":"wiki_source","project_id":owner,"repository_id":repository_id,"source_id":id,"sha256":source["sha256"],"start_line":start,"end_line":end,"start_byte":bytes.0,"end_byte":bytes.1,"pdf_page":source["page_starts"].as_array().map(|starts|starts.iter().filter(|line|line.as_u64().unwrap_or(u64::MAX)<=start as u64).count()),"pdf_page_end":source["page_starts"].as_array().map(|starts|starts.iter().filter(|line|line.as_u64().unwrap_or(u64::MAX)<=end as u64).count())}})
}

pub(crate) fn resolve_source(store: &StateStore, reference: &Value) -> Result<Value> {
    let id = reference["source_id"]
        .as_str()
        .ok_or_else(|| anyhow!("source_id is required"))?;
    validate_wiki_page_id(id)?;
    let hash = reference["sha256"]
        .as_str()
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| anyhow!("source hash is required"))?;
    let Some(mut source) = store.get_json(&format!("wiki:source_revision:{id}:{hash}"))? else {
        return Ok(json!({"status":"missing"}));
    };
    let text = source["text"].as_str().unwrap_or_default();
    if sha256_text(text) != hash {
        bail!("source integrity check failed");
    }
    let excerpt = if reference.get("start_byte").is_some() || reference.get("end_byte").is_some() {
        let start = usize::try_from(
            reference["start_byte"]
                .as_u64()
                .ok_or_else(|| anyhow!("source start_byte must be an unsigned offset"))?,
        )?;
        let end = usize::try_from(
            reference["end_byte"]
                .as_u64()
                .ok_or_else(|| anyhow!("source end_byte must be an unsigned offset"))?,
        )?;
        if end < start || end - start > 8000 {
            bail!("source byte range must contain at most 8000 bytes");
        }
        text.get(start..end)
            .ok_or_else(|| anyhow!("source byte range is outside UTF-8 boundaries"))?
            .to_owned()
    } else {
        let start = usize::try_from(reference["start_line"].as_u64().unwrap_or(1).max(1))?;
        let end = usize::try_from(
            reference["end_line"]
                .as_u64()
                .unwrap_or(start.saturating_add(79) as u64),
        )?;
        if end < start || end - start > 200 || start > text.lines().count() {
            bail!("source range is unavailable or exceeds 201 lines");
        }
        let mut excerpt = text
            .lines()
            .skip(start - 1)
            .take(end - start + 1)
            .collect::<Vec<_>>()
            .join("\n");
        truncate_string(&mut excerpt, 8000);
        excerpt
    };
    source
        .as_object_mut()
        .ok_or_else(|| anyhow!("invalid stored source record"))?
        .remove("text");
    let signals = sanitize_text(&format!("{source}\n{excerpt}")).2;
    Ok(
        json!({"schema":"agent_wiki.source.v1","status":"resolved","source":source,"excerpt":excerpt,"reference":reference,"prompt_injection_signals":signals}),
    )
}

/// All networking/extraction is performed by the optional companion, never this server.
pub(crate) fn admin(engine: &ProjectEngine, request: &ContextAdminRequest) -> Result<Value> {
    let existing = {
        engine
            .wiki_hub
            .read()
            .map_err(|_| anyhow!("wiki hub lock poisoned"))?
            .clone()
    };
    let hub = match existing {
        Some(hub) => hub,
        None => {
            let hub = Arc::new(WikiHub::new()?);
            engine.attach_wiki_hub(Arc::clone(&hub))?;
            hub
        }
    };
    match request.action.as_str() {
        "" | "status" => {
            let mut status = hub.status()?;
            status["project_id"] = json!(engine.project_id());
            status["source_refresh"] = engine
                .store
                .get_json("wiki:ingest_status")?
                .unwrap_or(json!({"status":"queued"}));
            status["history"] = engine
                .store
                .get_json("wiki:history_status")?
                .unwrap_or(json!({}));
            Ok(status)
        }
        "refresh" | "history_deepen" => hub.refresh(
            engine.project_id(),
            request.max_files.unwrap_or(1000) as usize,
        ),
        "ingest_submit" => {
            let input = request
                .wiki
                .as_ref()
                .ok_or_else(|| anyhow!("wiki ingestion metadata is required"))?;
            let required = |field: &Option<String>, name: &str| -> Result<String> {
                field
                    .as_ref()
                    .filter(|value| !value.trim().is_empty() && value.len() <= 2048)
                    .cloned()
                    .ok_or_else(|| anyhow!("ingestion requires bounded {name}"))
            };
            let permitted = match input.rights_status {
                Some(WikiRightsStatus::Permitted) => true,
                Some(WikiRightsStatus::MetadataOnly) => false,
                None => {
                    bail!("ingestion requires explicit rights_status: permitted or metadata_only")
                }
            };
            let url = required(&input.url, "url")?;
            if !url.starts_with("https://") || url.contains('@') || url.contains('#') {
                bail!("ingestion requires an HTTPS publisher URL without credentials or fragment");
            }
            let rights_evidence = required(&input.rights_evidence, "rights_evidence")?;
            if !rights_evidence.starts_with("https://") {
                bail!("rights evidence must use https");
            }
            let job = json!({"schema":"agent_wiki.ingest_job.v1","job_id":Uuid::new_v4().simple().to_string(),"status":if permitted{"queued"}else{"metadata_only"},"rights_status":if permitted{"permitted"}else{"metadata_only"},
                "url":url,"publisher":required(&input.publisher,"publisher")?,"title":required(&input.title,"title")?,
                "version":required(&input.version,"version")?,"license":required(&input.license,"license")?,
                "rights_evidence":rights_evidence,"created_at":now_iso()?});
            let (job, sensitivity) = sanitize_json_value(job);
            if sensitivity["redacted"] == true {
                bail!("ingestion metadata contains sensitive values");
            }
            engine.wiki_store.put_json(
                &format!(
                    "wiki:job:{}",
                    job["job_id"].as_str().expect("generated job id")
                ),
                &job,
            )?;
            Ok(job)
        }
        "ingest_claim" => {
            for (key, job) in engine.wiki_store.iter_json("wiki:job:")? {
                if job["status"] != "queued" {
                    continue;
                }
                let mut next = job.clone();
                next["status"] = json!("claimed");
                next["claimed_at"] = json!(now_iso()?);
                next["attempt"] = json!(job["attempt"].as_u64().unwrap_or(0) + 1);
                if engine
                    .store
                    .compare_and_swap_json_batch(&key, &job, &next, &[])
                    .is_ok()
                {
                    return Ok(next);
                }
            }
            Ok(json!({"status":"empty"}))
        }
        "ingest_status" | "ingest_retry" | "ingest_fail" | "ingest_import" => {
            let input = request
                .wiki
                .as_ref()
                .ok_or_else(|| anyhow!("wiki job payload is required"))?;
            let id = input
                .job_id
                .as_deref()
                .ok_or_else(|| anyhow!("job_id is required"))?;
            validate_wiki_page_id(id)?;
            let key = format!("wiki:job:{id}");
            let job = engine
                .store
                .get_json(&key)?
                .ok_or_else(|| anyhow!("ingestion job not found"))?;
            if request.action == "ingest_status" {
                return Ok(job);
            }
            if request.action == "ingest_retry" {
                if job["status"] == "claimed" {
                    let claimed = job["claimed_at"]
                        .as_str()
                        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
                        .ok_or_else(|| anyhow!("invalid claim lease"))?;
                    if OffsetDateTime::now_utc() - claimed < Duration::minutes(10) {
                        bail!("ingestion claim lease is still active");
                    }
                }

                if !matches!(job["status"].as_str(), Some("failed" | "claimed")) {
                    bail!("only failed or interrupted jobs may be retried");
                }
                let mut next = job.clone();
                next["status"] = json!("queued");
                engine
                    .store
                    .compare_and_swap_json_batch(&key, &job, &next, &[])?;
                return Ok(next);
            }
            if job["status"] != "claimed" {
                bail!("ingestion job must be claimed before completion");
            }
            if input.attempt != job["attempt"].as_u64() {
                bail!("ingestion claim attempt does not match");
            }
            if request.action == "ingest_fail" {
                let reason = input.failure.as_deref().unwrap_or("worker_failed");
                if !matches!(
                    reason,
                    "worker_failed"
                        | "download_failed"
                        | "extraction_failed"
                        | "missing_dependency"
                ) {
                    bail!("unsupported bounded worker failure");
                }
                let mut next = job.clone();
                next["status"] = json!("failed");
                next["reason"] = json!(reason);
                engine
                    .store
                    .compare_and_swap_json_batch(&key, &job, &next, &[])?;
                return Ok(next);
            }
            if job["rights_status"] != "permitted" {
                bail!("document content requires explicit permitted local-use rights");
            }
            let text = input
                .text
                .as_deref()
                .filter(|text| {
                    !text.trim().is_empty() && text.len() <= 4 * 1024 * 1024 && !text.contains('\0')
                })
                .ok_or_else(|| {
                    anyhow!("normalized PDF text must contain 1 byte to 4 MiB of UTF-8 text")
                })?;
            let hash = sha256_text(text);
            if input.sha256.as_deref() != Some(hash.as_str()) {
                bail!("normalized document hash mismatch");
            }
            let (sanitized, redactions, _) = sanitize_text(text);
            if !redactions.is_empty() || sanitized != text {
                bail!("normalized document contains sensitive data");
            }
            if input.page_starts.is_empty()
                || input.page_starts.len() > 10_000
                || input.page_starts[0] != 1
                || input.page_starts.windows(2).any(|pair| pair[0] >= pair[1])
                || input.page_starts.last().copied().unwrap_or(0) as usize > text.lines().count()
            {
                bail!("valid page start lines are required");
            }
            let source_id = format!(
                "ds-{}",
                &sha256_text(&format!("{}:{}", job["url"], job["version"]))[..24]
            );
            let source = json!({"source_id":source_id,"kind":"datasheet","title":job["title"],"text":text,"sha256":hash,
                "url":job["url"],"publisher":job["publisher"],"version":job["version"],"rights":"permitted",
                "license":job["license"],"rights_evidence":job["rights_evidence"],"retrieved_at":now_iso()?,"page_starts":input.page_starts,"freshness":"current"});
            let pointer = json!({"source_id":source_id,"sha256":hash,"freshness":"current"});
            let mut next = job.clone();
            next["status"] = json!("imported");
            next["source_id"] = json!(source_id);
            next["sha256"] = json!(hash);
            let revision_key = source_revision_key(&pointer);
            let immutable = engine.wiki_store.get_json(&revision_key)?.unwrap_or(source);
            engine.wiki_store.compare_and_swap_json_batch(
                &key,
                &job,
                &next,
                &[
                    (revision_key, immutable),
                    (format!("wiki:source_current:{source_id}"), pointer),
                ],
            )?;
            hub.publish(engine.project_id())?;
            Ok(next)
        }
        _ => bail!("unsupported wiki administrative action"),
    }
}

fn compact_sources(page: &Value) -> Value {
    Value::Array(
        page["sources"]
            .as_array()
            .into_iter()
            .flatten()
            .take(2)
            .map(|source| {
                let mut row = serde_json::Map::new();
                for key in [
                    "id",
                    "kind",
                    "sha256",
                    "path",
                    "fingerprint",
                    "commit",
                    "version",
                    "publisher",
                    "retrieved_at",
                    "rights",
                ] {
                    if let Some(value) = source.get(key).and_then(Value::as_str) {
                        let mut text = value.to_owned();
                        truncate_string(&mut text, 256);
                        row.insert(key.into(), json!(text));
                    }
                }
                Value::Object(row)
            })
            .collect(),
    )
}

fn git_head_stamp(root: &Path, mappings: &[(PathBuf, PathBuf)]) -> String {
    context_index::git::checkout_with_mappings(root, mappings)
        .map(|identity| identity.head)
        .unwrap_or_default()
}

fn compact_applicability(page: &Value) -> Value {
    let mut value = serde_json::Map::new();
    for key in ["projects", "branches"] {
        if let Some(values) = page["applicability"][key].as_array() {
            value.insert(
                key.into(),
                Value::Array(
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .take(8)
                        .map(|text| {
                            let mut text = text.to_owned();
                            truncate_string(&mut text, 128);
                            json!(text)
                        })
                        .collect(),
                ),
            );
        }
    }
    for key in ["commit", "version"] {
        if let Some(text) = page["applicability"][key].as_str() {
            let mut text = text.to_owned();
            truncate_string(&mut text, 128);
            value.insert(key.into(), json!(text));
        }
    }
    Value::Object(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn governed_datasheets_are_reused_without_acquisition_and_old_citations_survive_changes() {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("repo");
        fs::create_dir_all(root.join("reference-corpus")).unwrap();
        let text = format!(
            "{}auroracorpus timing specification 400ms\n",
            "introduction ".repeat(400)
        );
        fs::write(root.join("reference-corpus/device.txt"), &text).unwrap();
        let manifest = json!({"schema":"context_reference_manifest.v1","project_scope":"alpha","sources":[{
            "source_id":"device","canonical_url":"https://publisher.example/device.pdf","publisher":"Example","title":"Aurora datasheet",
            "version":"1","retrieved_at":"2026-10-15T00:00:00Z","media_type":"application/pdf","normalized_media_type":"text/plain; charset=utf-8",
            "normalized_path":"reference-corpus/device.txt","content_sha256":sha256_text(&text),
            "rights":{"status":"permitted","license":"local-use","evidence":"https://publisher.example/rights"},
            "freshness":{"status":"current","checked_at":"2026-10-15T00:00:00Z"}
        }]});
        fs::write(
            root.join("reference-corpus/manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        let engine = engine(&root, &fixture.path().join("state"), "alpha", &hub);
        hub.refresh("alpha", 1).unwrap();
        let hits = hub.search("auroracorpus", None, 10, true).unwrap();
        let hit = hits
            .iter()
            .find(|hit| hit["source_kind"] == "datasheet")
            .unwrap();
        assert_eq!(hit["source_status"], "current");
        assert!(
            hit["sources"][0]["id"]
                .as_str()
                .unwrap()
                .starts_with("corpus-")
        );
        let reference = hit["reference"].clone();
        assert!(
            resolve_source(&engine.wiki_store, &reference).unwrap()["excerpt"]
                .as_str()
                .unwrap()
                .contains("400ms")
        );
        fs::write(
            root.join("reference-corpus/device.txt"),
            "changed source requiring revalidation\n",
        )
        .unwrap();
        record_watcher_event(
            &engine.freshness,
            ["reference-corpus/device.txt".to_owned()],
            true,
        );
        assert!(
            hub.search("auroracorpus", None, 10, true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            resolve_source(&engine.wiki_store, &reference).unwrap()["status"],
            "resolved"
        );
    }

    fn engine(root: &Path, state: &Path, owner: &str, hub: &Arc<WikiHub>) -> ProjectEngine {
        fs::create_dir_all(root).unwrap();
        let engine = ProjectEngine::build_with_state(root, state, owner).unwrap();
        engine.attach_wiki_hub(Arc::clone(hub)).unwrap();
        engine
    }

    fn publish(engine: &ProjectEngine, expected: Option<&str>, summary: &str) -> Value {
        publish_page(engine, "quasarwiki", expected, summary)
    }

    fn publish_page(
        engine: &ProjectEngine,
        page_id: &str,
        expected: Option<&str>,
        summary: &str,
    ) -> Value {
        let index = engine.index();
        let (text, _) = index.file_content("policy.md", 4096).unwrap();
        let request = serde_json::from_value(json!({"mode":"wiki_upsert","wiki":{
            "page_id":page_id,"expected_revision":expected,"title":format!("{page_id} policy"),"summary":summary,
            "claims":[{"text":summary,"status":"verified","citations":[{"source_id":"policy"}]}],
            "sources":[{"id":"policy","kind":"markdown","path":"policy.md","sha256":sha256_text(&text),"fingerprint":index.snapshot_fingerprint("policy.md")}]
        }})).unwrap();
        serde_json::from_slice(&engine.context_memory(&request).unwrap()).unwrap()
    }

    #[test]
    fn wiki_noop_publication_and_peer_projection_ownership_are_stable() {
        let fixture = tempdir().unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        fs::create_dir_all(fixture.path().join("source")).unwrap();
        fs::create_dir_all(fixture.path().join("peer")).unwrap();
        fs::create_dir_all(fixture.path().join("alternate")).unwrap();
        fs::write(fixture.path().join("source/policy.md"), "source policy\n").unwrap();
        fs::write(fixture.path().join("peer/policy.md"), "peer policy\n").unwrap();
        fs::write(
            fixture.path().join("alternate/policy.md"),
            "alternate policy\n",
        )
        .unwrap();
        let mut source = ProjectEngine::build_with_state(
            fixture.path().join("source"),
            fixture.path().join("source-state"),
            "source",
        )
        .unwrap();
        let mut peer = ProjectEngine::build_with_state(
            fixture.path().join("peer"),
            fixture.path().join("peer-state"),
            "peer",
        )
        .unwrap();
        let mut alternate = ProjectEngine::build_with_state(
            fixture.path().join("alternate"),
            fixture.path().join("alternate-state"),
            "alternate",
        )
        .unwrap();
        source.repository_id = "same-repository".to_owned();
        peer.repository_id = "same-repository".to_owned();
        alternate.repository_id = "same-repository".to_owned();
        source.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        peer.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        alternate.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        hub.refresh("source", 10).unwrap();
        hub.refresh("peer", 10).unwrap();
        hub.refresh("alternate", 10).unwrap();

        let first = publish(&source, None, "source projection is 400ms");
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(peer_hit.iter().any(|hit| hit["project_id"] == "peer"));
        source
            .freshness
            .set_activity_age_for_test(PROJECT_DORMANT_MS);
        let dormant_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(
            dormant_hit
                .iter()
                .any(|hit| { hit["project_id"] == "peer" && hit["source_status"] == "unverified" }),
            "{dormant_hit:?}"
        );
        source.freshness.note_foreground();

        hub.publish("source").unwrap();
        let snapshot = hub
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let generation = hub.generation();
        hub.publish("source").unwrap();
        let unchanged = hub
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(hub.generation(), generation);
        assert!(Arc::ptr_eq(&snapshot.index, &unchanged.index));

        publish(&alternate, None, "alternate projection is 500ms");
        hub.unregister("alternate").unwrap();
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(
            peer_hit
                .iter()
                .any(|hit| { hit["summary"] == "source projection is 400ms" })
        );
        hub.unregister("source").unwrap();
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(!peer_hit.iter().any(|hit| hit["project_id"] == "peer"));
        source.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        hub.publish("source").unwrap();

        publish_page(&peer, "peer-only", None, "peer unrelated policy");
        hub.publish("peer").unwrap();
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(
            peer_hit
                .iter()
                .any(|hit| { hit["summary"] == "source projection is 400ms" })
        );

        let peer_native = publish(&peer, None, "peer native policy is 250ms");
        publish(
            &source,
            first["revision"].as_str(),
            "source updated projection is 300ms",
        );
        hub.publish("source").unwrap();
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(peer_hit.iter().any(|hit| {
            hit["project_id"] == "peer" && hit["summary"] == "peer native policy is 250ms"
        }));

        hub.unregister("source").unwrap();
        let peer_hit = hub
            .search_for_checkout("quasarwiki", None, Some("peer"), 10, false)
            .unwrap();
        assert!(peer_hit.iter().any(|hit| {
            hit["project_id"] == "peer" && hit["summary"] == "peer native policy is 250ms"
        }));
        assert_ne!(peer_native["revision"], Value::Null);
    }

    #[test]
    fn deferred_wiki_refresh_resumes_after_foreground_wake() {
        let fixture = tempdir().unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        let engine = engine(
            &fixture.path().join("deferred"),
            &fixture.path().join("deferred-state"),
            "deferred-owner",
            &hub,
        );
        let project = hub
            .projects
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get("deferred-owner")
            .unwrap()
            .clone();
        let startup_deadline = Instant::now() + StdDuration::from_secs(2);
        while project.refreshing.load(Ordering::Acquire) && Instant::now() < startup_deadline {
            thread::sleep(StdDuration::from_millis(5));
        }
        thread::sleep(WIKI_MONITOR_INTERVAL.saturating_mul(2));

        project
            .freshness
            .set_activity_age_for_test(PROJECT_DORMANT_MS);
        project.refresh_pending.store(true, Ordering::Release);
        project.refreshing.store(true, Ordering::Release);
        let attempts_before = project
            .freshness
            .wiki_publication_attempted
            .load(Ordering::Acquire);
        thread::sleep(WIKI_MONITOR_INTERVAL.saturating_mul(2));
        assert_eq!(
            project
                .freshness
                .wiki_publication_attempted
                .load(Ordering::Acquire),
            attempts_before
        );

        project.refreshing.store(false, Ordering::Release);
        project.freshness.note_foreground();
        let wake_deadline = Instant::now() + StdDuration::from_secs(2);
        while project
            .freshness
            .wiki_publication_attempted
            .load(Ordering::Acquire)
            == attempts_before
            && Instant::now() < wake_deadline
        {
            thread::sleep(StdDuration::from_millis(5));
        }
        assert!(
            project
                .freshness
                .wiki_publication_attempted
                .load(Ordering::Acquire)
                > attempts_before
        );
        assert!(Instant::now() < wake_deadline);
        drop(engine);
    }

    #[tokio::test]
    async fn shared_wiki_updates_remote_packs_and_preserves_compaction_references() {
        let fixture = tempdir().unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        let aroot = fixture.path().join("a");
        let broot = fixture.path().join("b");
        fs::create_dir_all(&aroot).unwrap();
        fs::create_dir_all(&broot).unwrap();
        fs::write(aroot.join("policy.md"), "Timeout policy is 400ms.\n").unwrap();
        fs::write(broot.join("policy.md"), "Timeout policy is 400ms.\n").unwrap();
        let a = engine(&aroot, &fixture.path().join("as"), "alpha", &hub);
        let b = engine(&broot, &fixture.path().join("bs"), "beta", &hub);
        hub.refresh("alpha", 1).unwrap();
        thread::sleep(StdDuration::from_millis(80));
        let first = publish(&a, None, "quasarwiki timeout is 400ms");
        let request: ContextPackRequest = serde_json::from_value(
            json!({"prompt":"quasarwiki","max_items":1,"max_source_tokens":64}),
        )
        .unwrap();
        let before: ContextPackV2 =
            serde_json::from_slice(&b.context_pack_generate(&request).await.unwrap()).unwrap();
        assert_eq!(before.wiki.len(), 1);
        assert!(before.evidence.is_empty());
        assert_eq!(before.wiki[0]["project_id"], "alpha");
        let second = publish(
            &a,
            first["revision"].as_str(),
            "quasarwiki timeout remains 400ms with bounded retry",
        );
        let after: ContextPackV2 =
            serde_json::from_slice(&b.context_pack_generate(&request).await.unwrap()).unwrap();
        assert_ne!(before.id, after.id);
        assert_eq!(after.wiki[0]["revision"], second["revision"]);
        assert!(after.wiki[0]["summary"].as_str().unwrap().len() <= 256);
        let focused:ContextPackRequest=serde_json::from_value(json!({"prompt":"quasarwiki","focus_paths":["policy.md"],"max_items":1,"max_source_tokens":64})).unwrap();
        let focused: ContextPackV2 =
            serde_json::from_slice(&a.context_pack(&focused).unwrap()).unwrap();
        assert_eq!(focused.evidence.len(), 1);
        assert!(focused.wiki.is_empty());
        let compact: ContextMemoryRequest =
            serde_json::from_value(json!({"mode":"compact","focus":"quasarwiki"})).unwrap();
        let compact: Value = serde_json::from_slice(&b.context_memory(&compact).unwrap()).unwrap();
        assert!(!compact["wiki_references"].as_array().unwrap().is_empty());
        let reference: ResultReferenceRequest =
            serde_json::from_value(json!({"reference":first["reference"]})).unwrap();
        let resolved: Value =
            serde_json::from_slice(&a.result_reference_resolve(&reference).unwrap()).unwrap();
        assert_eq!(
            resolved["content"]["summary"],
            "quasarwiki timeout is 400ms"
        );
        assert_eq!(resolved["revision_status"], "historical");
        fs::write(
            aroot.join("policy.md"),
            "Policy now requires manual verification.\n",
        )
        .unwrap();
        record_watcher_event(&a.freshness, ["policy.md".to_owned()], false);
        assert!(hub.search("quasarwiki", None, 10, true).unwrap().is_empty());
        let resolved: Value =
            serde_json::from_slice(&a.result_reference_resolve(&reference).unwrap()).unwrap();
        assert_eq!(resolved["status"], "resolved");
    }

    #[test]
    fn linked_checkout_sources_stay_isolated_and_shared_page_edits_refresh_projections() {
        fn git(root: &Path, args: &[&str]) {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let fixture = tempdir().unwrap();
        let main_root = fixture.path().join("main");
        let feature_root = fixture.path().join("feature");
        fs::create_dir_all(&main_root).unwrap();
        git(&main_root, &["init", "-b", "main"]);
        git(
            &main_root,
            &["config", "user.email", "test@example.invalid"],
        );
        git(&main_root, &["config", "user.name", "Test"]);
        fs::write(
            main_root.join("policy.md"),
            "Quasar policy applies on main.\n",
        )
        .unwrap();
        git(&main_root, &["add", "policy.md"]);
        git(&main_root, &["commit", "-m", "main policy"]);
        git(
            &main_root,
            &[
                "worktree",
                "add",
                "-b",
                "feature",
                feature_root.to_str().unwrap(),
                "main",
            ],
        );
        fs::write(
            feature_root.join("policy.md"),
            "Quasar policy applies on feature.\n",
        )
        .unwrap();
        git(&feature_root, &["add", "policy.md"]);
        git(&feature_root, &["commit", "-m", "feature policy"]);

        let hub = Arc::new(WikiHub::new().unwrap());
        let shared_wiki_path = fixture.path().join("shared-wiki");
        let shared = Arc::new(StateStore::open(&shared_wiki_path).unwrap());
        let mut main = ProjectEngine::build_with_state(
            &main_root,
            fixture.path().join("main-state"),
            "main-checkout",
        )
        .unwrap();
        let mut feature = ProjectEngine::build_with_state(
            &feature_root,
            fixture.path().join("feature-state"),
            "feature-checkout",
        )
        .unwrap();
        for engine in [&mut main, &mut feature] {
            engine.repository_id = "repo-test".to_owned();
            engine.wiki_store = Arc::new(StateStore::open(&shared_wiki_path).unwrap());
        }
        main.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        feature.attach_wiki_hub(Arc::clone(&hub)).unwrap();
        hub.refresh("main-checkout", 10).unwrap();
        hub.refresh("feature-checkout", 10).unwrap();

        let main_sources = shared
            .iter_json("wiki:source_current:md-")
            .unwrap()
            .into_iter()
            .filter(|(_, pointer)| pointer["source_id"].as_str().unwrap().starts_with("md-"))
            .map(|(_, pointer)| pointer)
            .collect::<Vec<_>>();
        assert!(main_sources.len() >= 2);
        let main_policy = main_sources
            .iter()
            .find(|pointer| {
                pointer["source_id"]
                    .as_str()
                    .unwrap()
                    .starts_with(&format!("md-{}-", &sha256_text("main-checkout")[..12]))
            })
            .unwrap();
        assert_eq!(main_policy["freshness"], "current");

        let first = publish(&main, None, "quasarwiki main-only timeout is 400ms");
        let first_revision = first["revision"].as_str().unwrap().to_owned();
        let on_feature = hub
            .search_for_checkout("quasarwiki", None, Some("feature-checkout"), 10, false)
            .unwrap();
        assert!(on_feature.iter().any(|hit| {
            hit["page_id"] == "quasarwiki" && hit["project_id"] == "feature-checkout"
        }));
        let on_feature = hub
            .search_for_checkout("quasarwiki", None, Some("feature-checkout"), 10, true)
            .unwrap();
        assert!(on_feature.is_empty());

        let index = feature.index();
        let (text, _) = index.file_content("policy.md", 4096).unwrap();
        let update = serde_json::from_value(json!({"mode":"wiki_upsert","wiki":{
            "page_id":"quasarwiki","expected_revision":first_revision,"title":"quasarwiki policy",
            "summary":"quasarwiki feature-only timeout is 250ms",
            "claims":[{"text":"quasarwiki feature-only timeout is 250ms","status":"verified","citations":[{"source_id":"policy"}]}],
            "sources":[{"id":"policy","kind":"markdown","path":"policy.md","sha256":sha256_text(&text),"fingerprint":index.snapshot_fingerprint("policy.md")}],
            "applicability":{"branches":["feature"]}
        }})).unwrap();
        let updated: Value =
            serde_json::from_slice(&feature.context_memory(&update).unwrap()).unwrap();
        let new_revision = updated["revision"].as_str().unwrap();
        hub.publish("feature-checkout").unwrap();
        let published = hub
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            published.documents[&("main-checkout".to_owned(), "quasarwiki".to_owned())]["revision"],
            new_revision,
            "projections={:?}",
            published.projections
        );

        let main_projection = hub
            .search_for_checkout("quasarwiki", None, Some("main-checkout"), 10, false)
            .unwrap();
        assert_eq!(main_projection[0]["revision"], new_revision);
        assert_eq!(main_projection[0]["source_status"], "inapplicable");
        let feature_projection = hub
            .search_for_checkout("quasarwiki", None, Some("feature-checkout"), 10, true)
            .unwrap();
        assert_eq!(feature_projection[0]["revision"], new_revision);
        assert_eq!(feature_projection[0]["source_status"], "current");
    }

    #[test]
    fn ingestion_requires_claim_hash_and_page_map_and_commits_atomically() {
        let fixture = tempdir().unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        let engine = engine(
            &fixture.path().join("repo"),
            &fixture.path().join("state"),
            "alpha",
            &hub,
        );
        let call = |action: &str, payload: Value| -> Result<Value> {
            let request =
                serde_json::from_value(json!({"mode":"wiki","action":action,"wiki":payload}))?;
            admin(&engine, &request)
        };
        let job=call("ingest_submit",json!({"url":"https://publisher.example/device.pdf","publisher":"Example","title":"Quasar device","version":"1","license":"Local use permitted","rights_status":"permitted","rights_evidence":"https://publisher.example/rights"})).unwrap();
        let claim = call("ingest_claim", json!({})).unwrap();
        assert_eq!(claim["job_id"], job["job_id"]);
        assert!(call("ingest_retry", json!({"job_id":job["job_id"]})).is_err());
        let text = "[PDF page 1]\nquasar voltage 3.3V\n[PDF page 2]\nquasar timeout 400ms\n";
        let mut payload = json!({"job_id":job["job_id"],"attempt":claim["attempt"],"text":text,"sha256":"bad","page_starts":[1,3]});
        assert!(call("ingest_import", payload.clone()).is_err());
        assert!(
            engine
                .store
                .iter_json("wiki:source_current:ds-")
                .unwrap()
                .is_empty()
        );
        payload["sha256"] = json!(sha256_text(text));
        payload["attempt"] = json!(999);
        assert!(call("ingest_import", payload.clone()).is_err());
        payload["attempt"] = claim["attempt"].clone();
        let imported = call("ingest_import", payload).unwrap();
        assert_eq!(imported["status"], "imported");
        let hits = hub.search("quasar voltage", None, 10, true).unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit["source_kind"] == "datasheet" && hit["source_status"] == "current")
        );
        let reference = &hits
            .iter()
            .find(|hit| hit["source_kind"] == "datasheet")
            .unwrap()["reference"];
        let resolved = resolve_source(&engine.wiki_store, reference).unwrap();
        assert!(
            resolved["excerpt"]
                .as_str()
                .unwrap()
                .contains("quasar voltage")
        );
    }

    #[test]
    fn git_history_is_historical_and_can_be_reindexed_without_duplicate_sources() {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("repo");
        fs::create_dir_all(&root).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Wiki test"],
            vec!["config", "user.email", "wiki@example.invalid"],
            vec!["config", "commit.gpgsign", "false"],
        ] {
            assert!(
                Command::new("git")
                    .current_dir(&root)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        fs::write(
            root.join("past.md"),
            "pastquasar decision before migration\n",
        )
        .unwrap();
        assert!(
            Command::new("git")
                .current_dir(&root)
                .args(["add", "past.md"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .current_dir(&root)
                .args(["commit", "-qm", "pastquasar original contract"])
                .status()
                .unwrap()
                .success()
        );
        let hub = Arc::new(WikiHub::new().unwrap());
        let engine = engine(&root, &fixture.path().join("state"), "alpha", &hub);
        hub.refresh("alpha", 1000).unwrap();
        hub.refresh("alpha", 1000).unwrap();
        assert_eq!(
            engine
                .store
                .iter_json("wiki:source_current:git-")
                .unwrap()
                .len(),
            1
        );
        let hits = hub.search("pastquasar", None, 10, true).unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit["source_kind"] == "git" && hit["source_status"] == "historical")
        );
    }
}
