//! Generated, revision-addressed knowledge and the shared agent search snapshot.
use super::*;
use std::{io::Read, path::Path};

#[derive(Clone)]
struct Project {
    owner: String,
    store: Arc<StateStore>,
    index: Arc<RwLock<Arc<ProjectIndex>>>,
    freshness: Arc<FreshnessState>,
    refreshing: Arc<AtomicBool>,
    _watcher: Arc<WatchGuard>,
    _refresh: Arc<RefreshCoordinator>,
}

struct Snapshot {
    index: WikiSearchIndex,
    documents: BTreeMap<(String, String), Value>,
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
                index: WikiSearchIndex::build(&[])?,
                documents: BTreeMap::new(),
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
        projects.insert(
            engine.project_id.clone(),
            Project {
                owner: engine.project_id.clone(),
                store: Arc::clone(&engine.store),
                index: Arc::clone(&engine.index),
                freshness: Arc::clone(&engine.freshness),
                refreshing,
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
                            let repository = project
                                .index
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone();
                            let git = git_head_stamp(repository.root());
                            let stamp = (project.freshness.generation.load(Ordering::Acquire), git);
                            if observed.get(&id) != Some(&stamp)
                                && !project.refreshing.load(Ordering::Acquire)
                            {
                                observed.insert(id.clone(), stamp);
                                hub.schedule_refresh(&id);
                            }
                        }
                        drop(hub);
                        thread::sleep(StdDuration::from_secs(2));
                    }
                })?;
        }
        self.publish(&engine.project_id)?;
        self.schedule_refresh(&engine.project_id);
        Ok(())
    }

    /// Rebuild on the write/background path; query readers retain the old immutable snapshot.
    pub fn publish(&self, owner: &str) -> Result<()> {
        self.epoch.fetch_add(1, Ordering::AcqRel);
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
        let old = self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))?
            .clone();
        let mut documents = old.documents.clone();
        documents.retain(|(project_id, _), _| project_id != owner);
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
            page["current"] = json!(page.get("superseded_by").is_none());
            page["prompt_injection_signals"] = sanitize_text(&serde_json::to_string(&page)?).2;
            documents.insert((owner.to_owned(), id.to_owned()), page);
        }
        for (_, pointer) in project.store.iter_json("wiki:source_current:")? {
            let Some(source) = project.store.get_json(&source_revision_key(&pointer))? else {
                continue;
            };
            for doc in source_documents(owner, &source) {
                let id = doc["page_id"]
                    .as_str()
                    .expect("generated source page id")
                    .to_owned();
                documents.insert((owner.to_owned(), id), doc);
            }
        }
        if documents.len() > 1_000_000 {
            bail!("shared wiki exceeds one million indexed chunks");
        }
        let index = WikiSearchIndex::build(&documents.values().cloned().collect::<Vec<_>>())?;
        *self
            .snapshot
            .write()
            .map_err(|_| anyhow!("wiki snapshot lock poisoned"))? =
            Arc::new(Snapshot { index, documents });
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn search(
        &self,
        query: &str,
        owner: Option<&str>,
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
            let index = project
                .index
                .read()
                .map_err(|_| anyhow!("wiki repository lock poisoned"))?;
            let status = source_status(page, &project.store, &index, &project.freshness);
            if for_pack && !matches!(status, "current" | "historical") {
                continue;
            }
            let mut summary = page["summary"].as_str().unwrap_or_default().to_owned();
            truncate_string(&mut summary, 600);
            let reference = page.get("source_reference").cloned().unwrap_or_else(|| json!({
                "kind":"wiki", "project_id":hit.project_id, "page_id":hit.page_id, "revision":hit.revision,
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
        if project.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let hub = Arc::clone(self);
        let owner = owner.to_owned();
        let flag = Arc::clone(&project.refreshing);
        if thread::Builder::new()
            .name("wiki-ingest".into())
            .spawn(move || {
                let refreshed = refresh_project_sources(&project, 1_000);
                let result = refreshed.and(hub.publish(&owner));
                if result.is_err() {
                    let _ = project.store.put_json(
                        "wiki:ingest_status",
                        &json!({"status":"failed", "reason":"source_refresh_failed"}),
                    );
                }
                project.refreshing.store(false, Ordering::Release);
                let pending = project
                    .store
                    .get_json("wiki:history_status")
                    .ok()
                    .flatten()
                    .and_then(|status| status["remaining_in_batch"].as_u64())
                    .unwrap_or(0);
                if result.is_ok() && pending > 0 {
                    hub.schedule_refresh(&owner);
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
    }) && (freshness.dirty.load(Ordering::Acquire) || freshness.watcher_requires_verification())
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
    for mut source in corpus_sources {
        let text = sanitize_text(source["text"].as_str().unwrap_or_default()).0;
        source["text"] = json!(text);
        source["sha256"] = json!(sha256_text(&text));
        source["source_id"] = json!(format!(
            "corpus-{}",
            &sha256_text(source["source_id"].as_str().unwrap_or_default())[..24]
        ));
        source["corpus_digest"] = json!(corpus_digest);
        let (source, _) = sanitize_json_value(source);
        put_source(&project.store, &source)?;
    }
    let mut seen = HashSet::new();
    for (path, text) in index.markdown_documents() {
        let id = format!("md-{}", &sha256_text(path)[..24]);
        seen.insert(id.clone());
        let (text, _, _) = sanitize_text(text);
        let source = json!({"source_id":id,"kind":"markdown","title":path,"text":text,
            "sha256":sha256_text(&text),"path":path,"fingerprint":index.snapshot_fingerprint(path),"freshness":"current"});
        put_source(&project.store, &source)?;
    }
    // Deleted Markdown remains available by revision, but its current pointer is stale.
    for (key, mut pointer) in project.store.iter_json("wiki:source_current:md-")? {
        if !seen.contains(pointer["source_id"].as_str().unwrap_or_default()) {
            pointer["freshness"] = json!("stale");
            project.store.put_json(&key, &pointer)?;
        }
    }
    if index.root().join(".git").exists() {
        let limit = history_limit.to_string();
        let commits = run_git(
            index.root(),
            &["log", "--format=%H", "-n", &limit, "HEAD"],
            800_000,
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

fn run_git(root: &Path, arguments: &[&str], bound: usize) -> Result<String> {
    let mut child = Command::new("git")
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
        .stderr(Stdio::null())
        .spawn()?;
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
    let revision_key = source_revision_key(&pointer);
    let immutable = store
        .get_json(&revision_key)?
        .unwrap_or_else(|| source.clone());
    store.put_json_batch(&[
        (revision_key, immutable),
        (
            format!(
                "wiki:source_current:{}",
                source["source_id"].as_str().unwrap_or_default()
            ),
            pointer,
        ),
    ])
}

fn source_documents(owner: &str, source: &Value) -> Vec<Value> {
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
    json!({"page_id":format!("source-{id}-{start}-{}",bytes.0),"project_id":owner,"revision":source["sha256"],"prompt_injection_signals":injection,
        "title":source["title"],"summary":text,"current":true,"source_kind":kind,
        "sources":[citation],"claims":[{"text":text,"status":if kind=="git" {"historical"} else {"verified"},"citations":[{"source_id":id}]}],
        "applicability":{"projects":[owner],"commit":source.get("commit"),"version":source.get("version")},
        "source_reference":{"kind":"wiki_source","project_id":owner,"source_id":id,"sha256":source["sha256"],"start_line":start,"end_line":end,"start_byte":bytes.0,"end_byte":bytes.1,"pdf_page":source["page_starts"].as_array().map(|starts|starts.iter().filter(|line|line.as_u64().unwrap_or(u64::MAX)<=start as u64).count()),"pdf_page_end":source["page_starts"].as_array().map(|starts|starts.iter().filter(|line|line.as_u64().unwrap_or(u64::MAX)<=end as u64).count())}})
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
            engine.store.put_json(
                &format!(
                    "wiki:job:{}",
                    job["job_id"].as_str().expect("generated job id")
                ),
                &job,
            )?;
            Ok(job)
        }
        "ingest_claim" => {
            for (key, job) in engine.store.iter_json("wiki:job:")? {
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
            let immutable = engine.store.get_json(&revision_key)?.unwrap_or(source);
            engine.store.compare_and_swap_json_batch(
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

fn git_head_stamp(root: &Path) -> String {
    let dot = root.join(".git");
    let directory = if dot.is_file() {
        let contents = fs::read_to_string(&dot).unwrap_or_default();
        let Some(path) = contents.trim().strip_prefix("gitdir:") else {
            return String::new();
        };
        root.join(path.trim())
    } else {
        dot
    };
    let head = fs::read_to_string(directory.join("HEAD")).unwrap_or_default();
    let reference = head
        .trim()
        .strip_prefix("ref: ")
        .and_then(|reference| validate_relative_path(reference).ok());
    let value = reference
        .and_then(|reference| fs::read_to_string(directory.join(reference)).ok())
        .unwrap_or_default();
    let packed = fs::metadata(directory.join("packed-refs"))
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .map(|time| format!("{time:?}"))
        .unwrap_or_default();
    sha256_text(&format!("{head}:{value}:{packed}"))
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
            resolve_source(&engine.store, &reference).unwrap()["excerpt"]
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
            resolve_source(&engine.store, &reference).unwrap()["status"],
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
        let index = engine.index();
        let (text, _) = index.file_content("policy.md", 4096).unwrap();
        let request=serde_json::from_value(json!({"mode":"wiki_upsert","wiki":{
            "page_id":"quasarwiki","expected_revision":expected,"title":"quasarwiki policy","summary":summary,
            "claims":[{"text":summary,"status":"verified","citations":[{"source_id":"policy"}]}],
            "sources":[{"id":"policy","kind":"markdown","path":"policy.md","sha256":sha256_text(&text),"fingerprint":index.snapshot_fingerprint("policy.md")}]
        }})).unwrap();
        serde_json::from_slice(&engine.context_memory(&request).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn shared_wiki_invalidates_remote_cached_packs_and_preserves_compaction_references() {
        let fixture = tempdir().unwrap();
        let hub = Arc::new(WikiHub::new().unwrap());
        let aroot = fixture.path().join("a");
        fs::create_dir_all(&aroot).unwrap();
        fs::write(aroot.join("policy.md"), "Timeout policy is 400ms.\n").unwrap();
        let a = engine(&aroot, &fixture.path().join("as"), "alpha", &hub);
        let b = engine(
            &fixture.path().join("b"),
            &fixture.path().join("bs"),
            "beta",
            &hub,
        );
        hub.refresh("alpha", 1).unwrap();
        thread::sleep(StdDuration::from_millis(80));
        let first = publish(&a, None, "quasarwiki timeout is 400ms");
        let request: ContextPackRequest = serde_json::from_value(
            json!({"prompt":"quasarwiki","max_items":1,"max_source_tokens":64}),
        )
        .unwrap();
        let before: ContextPackV2 =
            serde_json::from_slice(&b.context_pack_cached(&request).await.unwrap()).unwrap();
        assert_eq!(before.wiki.len(), 1);
        assert!(before.evidence.is_empty());
        assert_eq!(before.wiki[0]["project_id"], "alpha");
        let second = publish(
            &a,
            first["revision"].as_str(),
            "quasarwiki timeout remains 400ms with bounded retry",
        );
        let after: ContextPackV2 =
            serde_json::from_slice(&b.context_pack_cached(&request).await.unwrap()).unwrap();
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
        let resolved = resolve_source(&engine.store, reference).unwrap();
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
