//! Native repository scanning, deterministic chunking, and Tantivy retrieval.

mod corpus;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tantivy::{
    Index, IndexReader, ReloadPolicy, TantivyDocument,
    collector::TopDocs,
    doc,
    query::{AllQuery, Query, QueryParser},
    schema::{Field, STORED, STRING, Schema, TEXT, Value},
};
use tree_sitter::{Node, Parser};
use walkdir::{DirEntry, WalkDir};

pub const GENERIC_CHUNK_LINES: usize = 80;
pub const GENERIC_CHUNK_OVERLAP_LINES: usize = 8;
pub const MAX_SYMBOL_CHUNK_BYTES: usize = 32 * 1024;
pub const MIN_SYMBOL_CHUNK_BYTES: usize = 8 * 1024;
/// Maximum source file size admitted to indexing or public file resources.
///
/// The limit is checked from metadata before allocating a read buffer so a
/// minified or text-like binary file cannot make indexing unbounded.
pub const MAX_READ_BYTES: usize = 4 * 1024 * 1024;

const IGNORED_DIRECTORIES: &[&str] = &[
    ".git",
    ".mcp-context-manager",
    ".workingdir",
    ".worktrees",
    ".agent",
    ".agents",
    ".codex",
    ".openclaw",
    ".cache",
    ".venv",
    ".cmake-build",
    ".downloads",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "target",
    "venv",
    "wheelhouse",
];

/// Shared cancellation/deadline state for repository-wide blocking work.
///
/// Scans and index construction check this between files and chunks so a
/// timed-out request cannot publish a late generation after its caller left.
#[derive(Clone)]
pub struct WorkControl {
    cancelled: Arc<AtomicBool>,
    deadline: Instant,
}

impl WorkControl {
    pub fn new(deadline: Instant) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline,
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            bail!("repository work cancelled before commit")
        }
        Ok(())
    }
}

/// Returns whether a repository-relative path belongs to generated state.
/// Watchers, scanners, project discovery, and Git lineage use this policy.
pub fn is_ignored_repository_path(path: &Path) -> bool {
    path.components().any(|component| {
        component.as_os_str().to_str().is_some_and(|name| {
            IGNORED_DIRECTORIES.contains(&name) || name.ends_with('~') || name.starts_with(".#")
        })
    })
}

/// Git pathspec exclusions corresponding to the shared ignore policy.
pub fn git_exclude_pathspecs() -> Vec<String> {
    let mut pathspecs = IGNORED_DIRECTORIES
        .iter()
        .map(|directory| format!(":(exclude,glob)**/{directory}/**"))
        .collect::<Vec<_>>();
    for component in ["*~", ".#*"] {
        pathspecs.push(format!(":(exclude,glob)**/{component}"));
        pathspecs.push(format!(":(exclude,glob)**/{component}/**"));
    }
    pathspecs
}

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "build", "by", "for", "from", "how", "in", "into",
    "is", "it", "of", "on", "or", "the", "this", "to", "update", "with",
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Chunk {
    pub id: String,
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub symbol: String,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchHit {
    pub id: String,
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub symbol: String,
    pub content: String,
    pub score: f32,
    pub explicit: bool,
}

impl SearchHit {
    pub fn evidence_excerpt(&self, terms: &[String], max_chars: usize) -> (u32, u32, String) {
        let lines: Vec<&str> = self.content.lines().collect();
        if lines.is_empty() {
            return (self.start_line, self.end_line, String::new());
        }
        let match_index = lines
            .iter()
            .position(|line| {
                let lowered = line.to_ascii_lowercase();
                terms.iter().any(|term| lowered.contains(term))
            })
            .unwrap_or(0);
        let excerpt_start = match_index.saturating_sub(2);
        let excerpt_end = (match_index + 3).min(lines.len());
        let mut excerpt = lines[excerpt_start..excerpt_end].join("\n");
        truncate_utf8(&mut excerpt, max_chars);
        (
            self.start_line + excerpt_start as u32,
            self.start_line + excerpt_end.saturating_sub(1) as u32,
            excerpt,
        )
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct IndexStats {
    pub schema: &'static str,
    pub file_count: usize,
    pub chunk_count: usize,
    pub symbol_chunks: usize,
    pub python_symbol_chunks: usize,
    pub generic_chunks: usize,
    pub skipped_binary: usize,
    pub skipped_symlink: usize,
    pub corpus_source_count: usize,
    pub corpus_chunk_count: usize,
    pub corpus_metadata_only_count: usize,
    pub corpus_deduplicated_count: usize,
    pub refresh_signature: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileFingerprint {
    len: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexSnapshot {
    schema: String,
    generation: String,
    refresh_signature: String,
    chunks: Vec<Chunk>,
    fingerprints: BTreeMap<String, FileFingerprint>,
    corpus_digest: String,
    stats: SnapshotStats,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SnapshotStats {
    skipped_binary: usize,
    skipped_symlink: usize,
    corpus_source_count: usize,
    corpus_chunk_count: usize,
    corpus_metadata_only_count: usize,
    corpus_deduplicated_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SymbolRecord {
    pub path: String,
    pub name: String,
    pub kind: String,
    pub line_start: u32,
    pub line_end: u32,
    pub signature: String,
}

#[derive(Clone, Copy)]
struct Fields {
    id: Field,
    path: Field,
    path_text: Field,
    start_line: Field,
    end_line: Field,
    symbol: Field,
    body: Field,
}

pub struct ProjectIndex {
    root: PathBuf,
    index: Index,
    reader: IndexReader,
    fields: Fields,
    chunks: Arc<Vec<Chunk>>,
    chunks_by_path: Arc<HashMap<String, Vec<usize>>>,
    chunks_by_id: Arc<HashMap<String, usize>>,
    chunks_by_address: Arc<HashMap<String, usize>>,
    fingerprints: Arc<BTreeMap<String, FileFingerprint>>,
    corpus_digest: String,
    stats: IndexStats,
}

impl ProjectIndex {
    pub fn build(root: impl AsRef<Path>) -> Result<Self> {
        Self::build_for_project(root, None)
    }

    pub fn build_for_project(root: impl AsRef<Path>, project_scope: Option<&str>) -> Result<Self> {
        Self::build_for_project_controlled(root, project_scope, None)
    }

    pub fn build_for_project_controlled(
        root: impl AsRef<Path>,
        project_scope: Option<&str>,
        control: Option<&WorkControl>,
    ) -> Result<Self> {
        check_control(control)?;
        let root = root.as_ref().canonicalize().with_context(|| {
            format!(
                "repository root does not exist: {}",
                root.as_ref().display()
            )
        })?;
        if !root.is_dir() {
            bail!("repository root is not a directory: {}", root.display());
        }

        let scanned = scan_repository(&root, project_scope, control)?;
        Self::from_parts(
            root,
            scanned.chunks,
            scanned.fingerprints,
            scanned.corpus_digest,
            scanned.stats,
            control,
        )
    }

    fn from_parts(
        root: PathBuf,
        chunks: Vec<Chunk>,
        fingerprints: BTreeMap<String, FileFingerprint>,
        corpus_digest: String,
        stats: IndexStats,
        control: Option<&WorkControl>,
    ) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        let fields = Fields {
            id: schema_builder.add_text_field("id", STRING | STORED),
            path: schema_builder.add_text_field("path", STRING | STORED),
            path_text: schema_builder.add_text_field("path_text", TEXT),
            start_line: schema_builder.add_u64_field("start_line", STORED),
            end_line: schema_builder.add_u64_field("end_line", STORED),
            symbol: schema_builder.add_text_field("symbol", TEXT | STORED),
            body: schema_builder.add_text_field("body", TEXT | STORED),
        };
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer(50_000_000)?;
        for chunk in &chunks {
            check_control(control)?;
            writer.add_document(doc!(
                fields.id => chunk.id.clone(),
                fields.path => chunk.path.clone(),
                fields.path_text => chunk.path.clone(),
                fields.start_line => u64::from(chunk.start_line),
                fields.end_line => u64::from(chunk.end_line),
                fields.symbol => chunk.symbol.clone(),
                fields.body => chunk.content.clone(),
            ))?;
        }
        check_control(control)?;
        writer.commit()?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        reader.reload()?;

        let mut chunks_by_path: HashMap<String, Vec<usize>> = HashMap::new();
        let mut chunks_by_id = HashMap::new();
        let mut chunks_by_address = HashMap::new();
        for (index, chunk) in chunks.iter().enumerate() {
            check_control(control)?;
            chunks_by_path
                .entry(chunk.path.clone())
                .or_default()
                .push(index);
            chunks_by_id.insert(chunk.id.clone(), index);
            chunks_by_address.insert(immutable_candidate_address(&chunk.id), index);
        }

        check_control(control)?;
        Ok(Self {
            root,
            index,
            reader,
            fields,
            chunks: Arc::new(chunks),
            chunks_by_path: Arc::new(chunks_by_path),
            chunks_by_id: Arc::new(chunks_by_id),
            chunks_by_address: Arc::new(chunks_by_address),
            fingerprints: Arc::new(fingerprints),
            corpus_digest,
            stats,
        })
    }

    pub fn snapshot(&self) -> IndexSnapshot {
        IndexSnapshot {
            schema: "context_index.snapshot.v1".to_owned(),
            generation: self.stats.refresh_signature.clone(),
            refresh_signature: self.stats.refresh_signature.clone(),
            chunks: self.chunks.as_ref().clone(),
            fingerprints: self.fingerprints.as_ref().clone(),
            corpus_digest: self.corpus_digest.clone(),
            stats: SnapshotStats {
                skipped_binary: self.stats.skipped_binary,
                skipped_symlink: self.stats.skipped_symlink,
                corpus_source_count: self.stats.corpus_source_count,
                corpus_chunk_count: self.stats.corpus_chunk_count,
                corpus_metadata_only_count: self.stats.corpus_metadata_only_count,
                corpus_deduplicated_count: self.stats.corpus_deduplicated_count,
            },
        }
    }

    pub fn from_snapshot_controlled(
        root: impl AsRef<Path>,
        snapshot: IndexSnapshot,
        project_scope: Option<&str>,
        control: Option<&WorkControl>,
    ) -> Result<Self> {
        if snapshot.schema != "context_index.snapshot.v1"
            || snapshot.generation != snapshot.refresh_signature
        {
            bail!("unsupported persisted index snapshot")
        }
        let root = root.as_ref().canonicalize()?;
        let current = repository_signature_for_project_controlled(&root, project_scope, control)?;
        if current != snapshot.refresh_signature
            || fingerprint_signature(&snapshot.fingerprints, &snapshot.corpus_digest) != current
        {
            bail!("persisted index snapshot is stale or corrupt")
        }
        let stats = stats_from_parts(
            &snapshot.chunks,
            &snapshot.fingerprints,
            &snapshot.stats,
            current,
        );
        Self::from_parts(
            root,
            snapshot.chunks,
            snapshot.fingerprints,
            snapshot.corpus_digest,
            stats,
            control,
        )
    }

    pub fn refresh_paths_controlled(
        &self,
        paths: &[String],
        control: Option<&WorkControl>,
    ) -> Result<Self> {
        if paths.is_empty() || paths.len() > 1_024 {
            bail!("incremental refresh requires 1..=1024 changed paths")
        }
        let mut normalized = BTreeSet::new();
        for path in paths {
            let path = validate_relative_path(path)?;
            if path == "."
                || path.starts_with("reference-corpus/")
                || is_ignored_repository_path(Path::new(&path))
            {
                bail!("incremental refresh state is unsafe")
            }
            normalized.insert(path);
        }
        let mut chunks = self
            .chunks
            .iter()
            .filter(|chunk| !normalized.contains(&chunk.path))
            .cloned()
            .collect::<Vec<_>>();
        let mut fingerprints = self.fingerprints.as_ref().clone();
        for path in &normalized {
            check_control(control)?;
            fingerprints.remove(path);
            let absolute = self.root.join(path);
            match fs::symlink_metadata(&absolute) {
                Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_dir() => {
                    bail!("incremental refresh encountered an unsafe path")
                }
                Ok(_) => {
                    if let Some(text) = read_text(&absolute)? {
                        fingerprints.insert(path.clone(), fingerprint(&text));
                        chunks.extend(chunks_for_file(path, &absolute, &text)?);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        chunks.sort_by(chunk_order);
        let signature = fingerprint_signature(&fingerprints, &self.corpus_digest);
        let snapshot_stats = SnapshotStats {
            skipped_binary: self.stats.skipped_binary,
            skipped_symlink: self.stats.skipped_symlink,
            corpus_source_count: self.stats.corpus_source_count,
            corpus_chunk_count: self.stats.corpus_chunk_count,
            corpus_metadata_only_count: self.stats.corpus_metadata_only_count,
            corpus_deduplicated_count: self.stats.corpus_deduplicated_count,
        };
        let stats = stats_from_parts(&chunks, &fingerprints, &snapshot_stats, signature);
        Self::from_parts(
            self.root.clone(),
            chunks,
            fingerprints,
            self.corpus_digest.clone(),
            stats,
            control,
        )
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    pub fn all_paths(&self) -> Vec<String> {
        let mut paths = self.chunks_by_path.keys().cloned().collect::<Vec<_>>();
        paths.sort();
        paths
    }

    pub fn tree(
        &self,
        raw_path: &str,
        max_entries: usize,
        max_depth: usize,
    ) -> Result<Vec<TreeEntry>> {
        let path = validate_relative_path(raw_path)?;
        let base = self.root.join(&path);
        let metadata =
            fs::symlink_metadata(&base).with_context(|| format!("path does not exist: {path}"))?;
        if metadata.file_type().is_symlink() {
            bail!("symlink paths are not allowed: {path}");
        }
        if !metadata.is_dir() {
            bail!("tree path is not a directory: {path}");
        }
        let mut entries = Vec::new();
        for entry in WalkDir::new(&base)
            .follow_links(false)
            .min_depth(1)
            .max_depth(max_depth)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(should_visit)
        {
            let entry = entry?;
            if entry.file_type().is_symlink() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&self.root)
                .expect("tree paths stay under repository root")
                .to_string_lossy()
                .replace('\\', "/");
            entries.push(TreeEntry {
                path: relative,
                entry_type: if entry.file_type().is_dir() {
                    "dir".to_owned()
                } else {
                    "file".to_owned()
                },
                size: if entry.file_type().is_file() {
                    entry.metadata()?.len()
                } else {
                    0
                },
            });
            if entries.len() == max_entries {
                break;
            }
        }
        Ok(entries)
    }

    pub fn symbols(&self, query: &str, limit: usize) -> Vec<SymbolRecord> {
        let query = query.to_ascii_lowercase();
        let mut symbols = self
            .chunks
            .iter()
            .filter(|chunk| {
                !chunk.symbol.is_empty()
                    && (query.is_empty()
                        || chunk.symbol.to_ascii_lowercase().contains(query.as_str()))
            })
            .map(|chunk| SymbolRecord {
                path: chunk.path.clone(),
                name: chunk.symbol.clone(),
                kind: symbol_kind(chunk),
                line_start: chunk.start_line,
                line_end: chunk.end_line,
                signature: symbol_signature(chunk),
            })
            .collect::<Vec<_>>();
        symbols.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.line_start.cmp(&right.line_start))
                .then_with(|| left.name.cmp(&right.name))
        });
        symbols.dedup_by(|left, right| {
            left.path == right.path
                && left.line_start == right.line_start
                && left.name == right.name
        });
        symbols.truncate(limit);
        symbols
    }

    pub fn chunks_for_path(&self, raw_path: &str) -> Result<Vec<Chunk>> {
        let path = validate_relative_path(raw_path)?;
        Ok(self
            .chunks_by_path
            .get(&path)
            .into_iter()
            .flatten()
            .map(|index| self.chunks[*index].clone())
            .collect())
    }

    pub fn file_line_count(&self, raw_path: &str) -> Result<usize> {
        let path = validate_relative_path(raw_path)?;
        let absolute = self.root.join(&path);
        let metadata = fs::symlink_metadata(&absolute)
            .with_context(|| format!("path does not exist: {path}"))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("path is not a regular repository file: {path}");
        }
        let text = read_text(&absolute)?
            .ok_or_else(|| anyhow::anyhow!("file is not UTF-8 text: {path}"))?;
        Ok(text.lines().count())
    }

    pub fn file_content(&self, raw_path: &str, max_bytes: usize) -> Result<(String, bool)> {
        let path = validate_relative_path(raw_path)?;
        let absolute = self.root.join(&path);
        let metadata = fs::symlink_metadata(&absolute)
            .with_context(|| format!("path does not exist: {path}"))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("path is not a regular repository file: {path}");
        }
        let mut text = read_text(&absolute)?
            .ok_or_else(|| anyhow::anyhow!("file is not UTF-8 text: {path}"))?;
        let truncated = text.len() > max_bytes;
        truncate_utf8(&mut text, max_bytes);
        Ok((text, truncated))
    }

    pub fn search(
        &self,
        prompt: &str,
        explicit_paths: &[String],
        max_items: usize,
    ) -> Result<(Vec<SearchHit>, Vec<String>)> {
        self.search_scoped(prompt, explicit_paths, max_items, None)
    }

    pub fn search_scoped(
        &self,
        prompt: &str,
        explicit_paths: &[String],
        max_items: usize,
        allowed_paths: Option<&HashSet<String>>,
    ) -> Result<(Vec<SearchHit>, Vec<String>)> {
        let terms = normalize_terms(prompt, 8);
        let candidate_count = (max_items.saturating_mul(8)).clamp(64, 256);
        let mut hits = if let Some(allowed_paths) = allowed_paths {
            self.chunks
                .iter()
                .filter(|chunk| allowed_paths.contains(&chunk.path))
                .filter(|chunk| terms.is_empty() || term_match_count(chunk, &terms) > 0)
                .map(|chunk| SearchHit {
                    id: chunk.id.clone(),
                    path: chunk.path.clone(),
                    start_line: chunk.start_line,
                    end_line: chunk.end_line,
                    symbol: chunk.symbol.clone(),
                    content: chunk.content.clone(),
                    score: 0.0,
                    explicit: false,
                })
                .collect()
        } else {
            self.tantivy_search(&terms, candidate_count)?
        };
        let mut seen: HashSet<String> = hits.iter().map(|hit| hit.id.clone()).collect();

        for raw_path in explicit_paths {
            let path = validate_relative_path(raw_path)?;
            if allowed_paths.is_some_and(|allowed| !allowed.contains(&path)) {
                continue;
            }
            let Some(indices) = self.chunks_by_path.get(&path) else {
                continue;
            };
            let best = indices
                .iter()
                .map(|index| &self.chunks[*index])
                .max_by_key(|chunk| {
                    (
                        term_match_count(chunk, &terms),
                        usize::MAX - chunk.start_line as usize,
                    )
                });
            if let Some(chunk) = best
                && seen.insert(chunk.id.clone())
            {
                hits.push(SearchHit {
                    id: chunk.id.clone(),
                    path: chunk.path.clone(),
                    start_line: chunk.start_line,
                    end_line: chunk.end_line,
                    symbol: chunk.symbol.clone(),
                    content: chunk.content.clone(),
                    score: 1_000_000.0 + term_match_count(chunk, &terms) as f32,
                    explicit: true,
                });
            }
        }

        let explicit: HashSet<String> = explicit_paths
            .iter()
            .filter_map(|path| validate_relative_path(path).ok())
            .collect();
        for hit in &mut hits {
            if explicit.contains(hit.path.as_str()) {
                hit.explicit = true;
            }
            hit.score = deterministic_rank_score(hit, &terms)
                + if hit.explicit { 1_000_000.0 } else { 0.0 };
        }
        hits.sort_by(|left, right| {
            right
                .explicit
                .cmp(&left.explicit)
                .then_with(|| right.score.total_cmp(&left.score))
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.start_line.cmp(&right.start_line))
        });
        hits.truncate(candidate_count);
        Ok((hits, terms))
    }

    pub fn rerank(
        &self,
        prompt: &str,
        explicit_paths: &[String],
        candidate_ids: &[String],
        max_items: usize,
    ) -> Result<(Vec<SearchHit>, Vec<String>)> {
        let terms = normalize_terms(prompt, 8);
        let explicit = explicit_paths
            .iter()
            .map(|path| validate_relative_path(path))
            .collect::<Result<HashSet<_>>>()?;
        let mut hits = candidate_ids
            .iter()
            .filter_map(|id| self.chunks_by_id.get(id))
            .map(|index| &self.chunks[*index])
            .map(|chunk| {
                let is_explicit = explicit.contains(&chunk.path);
                SearchHit {
                    id: chunk.id.clone(),
                    path: chunk.path.clone(),
                    start_line: chunk.start_line,
                    end_line: chunk.end_line,
                    symbol: chunk.symbol.clone(),
                    content: chunk.content.clone(),
                    score: 0.0,
                    explicit: is_explicit,
                }
            })
            .collect::<Vec<_>>();
        for hit in &mut hits {
            hit.score = deterministic_rank_score(hit, &terms)
                + if hit.explicit { 1_000_000.0 } else { 0.0 };
        }
        hits.sort_by(|left, right| {
            right
                .explicit
                .cmp(&left.explicit)
                .then_with(|| right.score.total_cmp(&left.score))
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.start_line.cmp(&right.start_line))
        });
        hits.truncate((max_items.saturating_mul(8)).clamp(64, 256));
        Ok((hits, terms))
    }

    /// Rerank opaque, content-addressed candidate identities against this
    /// project's current index. `None` is a fail-closed signal that at least
    /// one shared candidate no longer exists locally.
    pub fn rerank_immutable_candidates(
        &self,
        prompt: &str,
        explicit_paths: &[String],
        candidate_addresses: &[String],
        max_items: usize,
    ) -> Result<Option<(Vec<SearchHit>, Vec<String>)>> {
        if candidate_addresses
            .iter()
            .any(|address| !self.chunks_by_address.contains_key(address))
        {
            return Ok(None);
        }
        let candidate_ids = candidate_addresses
            .iter()
            .filter_map(|address| self.chunks_by_address.get(address))
            .map(|index| self.chunks[*index].id.clone())
            .collect::<Vec<_>>();
        self.rerank(prompt, explicit_paths, &candidate_ids, max_items)
            .map(Some)
    }

    pub fn snippet(&self, raw_path: &str, start_line: u32, end_line: Option<u32>) -> Result<Chunk> {
        let path = validate_relative_path(raw_path)?;
        let absolute = self.root.join(&path);
        let metadata = fs::symlink_metadata(&absolute)
            .with_context(|| format!("path does not exist: {path}"))?;
        if metadata.file_type().is_symlink() {
            bail!("symlink paths are not allowed: {path}");
        }
        if !metadata.is_file() {
            bail!("path is not a file: {path}");
        }
        let text = read_text(&absolute)?
            .ok_or_else(|| anyhow::anyhow!("file is not UTF-8 text: {path}"))?;
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            bail!("cannot select a line range from an empty file: {path}");
        }
        let start = start_line.max(1) as usize;
        if start > lines.len() {
            bail!(
                "start_line {start} exceeds file line count {}: {path}",
                lines.len()
            );
        }
        let end = end_line
            .unwrap_or_else(|| (start as u32).saturating_add(19))
            .max(start as u32) as usize;
        let bounded_end = end.min(lines.len());
        let content = lines[start - 1..bounded_end].join("\n");
        Ok(Chunk {
            id: chunk_id(&path, start as u32, bounded_end as u32, ""),
            path,
            start_line: start as u32,
            end_line: bounded_end as u32,
            symbol: String::new(),
            content,
        })
    }

    fn tantivy_search(&self, terms: &[String], limit: usize) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let query: Box<dyn Query> = if terms.is_empty() {
            Box::new(AllQuery)
        } else {
            let parser = QueryParser::for_index(
                &self.index,
                vec![self.fields.body, self.fields.symbol, self.fields.path_text],
            );
            parser.parse_query(&terms.join(" OR "))?
        };
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit).order_by_score())?;
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, address) in top_docs {
            let document: TantivyDocument = searcher.doc(address)?;
            let text = |field: Field| {
                document
                    .get_first(field)
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_owned()
            };
            let number = |field: Field| {
                document
                    .get_first(field)
                    .and_then(|value| value.as_u64())
                    .unwrap_or_default() as u32
            };
            hits.push(SearchHit {
                id: text(self.fields.id),
                path: text(self.fields.path),
                start_line: number(self.fields.start_line),
                end_line: number(self.fields.end_line),
                symbol: text(self.fields.symbol),
                content: text(self.fields.body),
                score,
                explicit: false,
            });
        }
        Ok(hits)
    }
}

pub fn repository_signature(root: impl AsRef<Path>) -> Result<String> {
    repository_signature_for_project(root, None)
}

pub fn repository_signature_for_project(
    root: impl AsRef<Path>,
    project_scope: Option<&str>,
) -> Result<String> {
    repository_signature_for_project_controlled(root, project_scope, None)
}

pub fn repository_signature_for_project_controlled(
    root: impl AsRef<Path>,
    project_scope: Option<&str>,
    control: Option<&WorkControl>,
) -> Result<String> {
    check_control(control)?;
    let root = root.as_ref().canonicalize()?;
    let mut digest = Sha256::new();
    for entry in WalkDir::new(&root)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(should_visit)
    {
        check_control(control)?;
        let entry = entry?;
        if entry.file_type().is_symlink() || !entry.file_type().is_file() {
            continue;
        }
        let Some(text) = read_text(entry.path())? else {
            continue;
        };
        let path = entry
            .path()
            .strip_prefix(&root)
            .expect("signature paths stay under root")
            .to_string_lossy()
            .replace('\\', "/");
        update_source_signature(&mut digest, &path, &text);
    }
    let corpus = corpus::load(&root, project_scope)?;
    digest.update(b"reference-corpus\0");
    digest.update(corpus.digest.as_bytes());
    Ok(format!("files:{}", digest_hex(digest)))
}

pub fn normalize_terms(text: &str, max_terms: usize) -> Vec<String> {
    let stop_words: HashSet<&str> = STOP_WORDS.iter().copied().collect();
    let mut seen = BTreeSet::new();
    let mut terms = Vec::new();
    for term in text
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .map(str::to_ascii_lowercase)
        .filter(|term| term.len() > 1)
    {
        if stop_words.contains(term.as_str()) || !seen.insert(term.clone()) {
            continue;
        }
        terms.push(term);
        if terms.len() == max_terms {
            break;
        }
    }
    terms
}

pub fn validate_relative_path(raw: &str) -> Result<String> {
    if raw.trim().is_empty() || raw == "." {
        return Ok(".".to_owned());
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        bail!("absolute paths are not allowed: {raw}");
    }
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => clean.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("path traversal is not allowed: {raw}")
            }
        }
    }
    let normalized = clean.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        Ok(".".to_owned())
    } else {
        Ok(normalized)
    }
}

struct ScannedRepository {
    chunks: Vec<Chunk>,
    fingerprints: BTreeMap<String, FileFingerprint>,
    corpus_digest: String,
    stats: IndexStats,
}

fn scan_repository(
    root: &Path,
    project_scope: Option<&str>,
    control: Option<&WorkControl>,
) -> Result<ScannedRepository> {
    let mut chunks = Vec::new();
    let mut source_digest = Sha256::new();
    let mut fingerprints = BTreeMap::new();
    let mut file_count = 0;
    let mut python_symbol_chunks = 0;
    let mut symbol_chunks = 0;
    let mut generic_chunks = 0;
    let mut skipped_binary = 0;
    let mut skipped_symlink = 0;

    for entry in WalkDir::new(root)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(should_visit)
    {
        check_control(control)?;
        let entry = entry?;
        if entry.file_type().is_symlink() {
            skipped_symlink += 1;
            continue;
        }
        if !entry.file_type().is_file() {
            continue;
        }
        let Some(text) = read_text(entry.path())? else {
            skipped_binary += 1;
            continue;
        };
        let path = entry
            .path()
            .strip_prefix(root)
            .expect("walked paths stay under root")
            .to_string_lossy()
            .replace('\\', "/");
        update_source_signature(&mut source_digest, &path, &text);
        fingerprints.insert(path.clone(), fingerprint(&text));
        file_count += 1;
        let mut file_chunks = chunks_for_file(&path, entry.path(), &text)?;
        check_control(control)?;
        symbol_chunks += file_chunks
            .iter()
            .filter(|chunk| !chunk.symbol.is_empty())
            .count();
        if entry.path().extension().and_then(|value| value.to_str()) == Some("py") {
            python_symbol_chunks += file_chunks
                .iter()
                .filter(|chunk| !chunk.symbol.is_empty())
                .count();
        }
        generic_chunks += file_chunks
            .iter()
            .filter(|chunk| chunk.symbol.is_empty())
            .count();
        chunks.append(&mut file_chunks);
    }

    let corpus = corpus::load(root, project_scope)?;
    let corpus_chunk_count = corpus.chunks.len();
    source_digest.update(b"reference-corpus\0");
    source_digest.update(corpus.digest.as_bytes());
    chunks.extend(corpus.chunks);
    chunks.sort_by(chunk_order);
    Ok(ScannedRepository {
        chunks,
        fingerprints,
        corpus_digest: corpus.digest,
        stats: IndexStats {
            schema: "context_index.native.v1",
            file_count,
            chunk_count: symbol_chunks + generic_chunks + corpus_chunk_count,
            symbol_chunks,
            python_symbol_chunks,
            generic_chunks,
            skipped_binary,
            skipped_symlink,
            corpus_source_count: corpus.source_count,
            corpus_chunk_count,
            corpus_metadata_only_count: corpus.metadata_only_count,
            corpus_deduplicated_count: corpus.deduplicated_count,
            refresh_signature: format!("files:{}", digest_hex(source_digest)),
        },
    })
}

fn chunks_for_file(path: &str, absolute: &Path, text: &str) -> Result<Vec<Chunk>> {
    let extractor = extractor_for_path(absolute);
    if let Some(extractor) = extractor.as_deref() {
        language_chunks(path, text, extractor)
    } else {
        Ok(generic_chunks_for_range(
            path,
            &text.lines().collect::<Vec<_>>(),
            1,
            text.lines().count(),
            "",
        ))
    }
}

fn chunk_order(left: &Chunk, right: &Chunk) -> std::cmp::Ordering {
    left.path
        .cmp(&right.path)
        .then_with(|| left.start_line.cmp(&right.start_line))
        .then_with(|| left.end_line.cmp(&right.end_line))
        .then_with(|| left.symbol.cmp(&right.symbol))
}

fn fingerprint(text: &str) -> FileFingerprint {
    FileFingerprint {
        len: text.len() as u64,
        sha256: digest_hex(Sha256::new_with_prefix(text.as_bytes())),
    }
}

fn fingerprint_signature(
    fingerprints: &BTreeMap<String, FileFingerprint>,
    corpus_digest: &str,
) -> String {
    let mut digest = Sha256::new();
    for (path, fingerprint) in fingerprints {
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update(fingerprint.len.to_be_bytes());
        for pair in fingerprint.sha256.as_bytes().chunks_exact(2) {
            let byte = std::str::from_utf8(pair)
                .ok()
                .and_then(|value| u8::from_str_radix(value, 16).ok())
                .unwrap_or_default();
            digest.update([byte]);
        }
    }
    digest.update(b"reference-corpus\0");
    digest.update(corpus_digest.as_bytes());
    format!("files:{}", digest_hex(digest))
}

fn stats_from_parts(
    chunks: &[Chunk],
    fingerprints: &BTreeMap<String, FileFingerprint>,
    persisted: &SnapshotStats,
    refresh_signature: String,
) -> IndexStats {
    let repository_chunks = chunks
        .iter()
        .filter(|chunk| !chunk.path.starts_with("@corpus/"));
    let symbol_chunks = repository_chunks
        .clone()
        .filter(|chunk| !chunk.symbol.is_empty())
        .count();
    let generic_chunks = repository_chunks
        .filter(|chunk| chunk.symbol.is_empty())
        .count();
    IndexStats {
        schema: "context_index.native.v1",
        file_count: fingerprints.len(),
        chunk_count: chunks.len(),
        symbol_chunks,
        python_symbol_chunks: chunks
            .iter()
            .filter(|chunk| chunk.path.ends_with(".py") && !chunk.symbol.is_empty())
            .count(),
        generic_chunks,
        skipped_binary: persisted.skipped_binary,
        skipped_symlink: persisted.skipped_symlink,
        corpus_source_count: persisted.corpus_source_count,
        corpus_chunk_count: persisted.corpus_chunk_count,
        corpus_metadata_only_count: persisted.corpus_metadata_only_count,
        corpus_deduplicated_count: persisted.corpus_deduplicated_count,
        refresh_signature,
    }
}

fn update_source_signature(digest: &mut Sha256, path: &str, text: &str) {
    digest.update((path.len() as u64).to_be_bytes());
    digest.update(path.as_bytes());
    digest.update((text.len() as u64).to_be_bytes());
    digest.update(Sha256::digest(text.as_bytes()));
}

fn digest_hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Stable opaque identity for an immutable retrieval candidate. Project-local
/// chunk ids contain paths, so only this digest may enter a shared frontier.
pub fn immutable_candidate_address(candidate_id: &str) -> String {
    format!(
        "ca:{}",
        digest_hex(Sha256::new_with_prefix(candidate_id.as_bytes()))
    )
}

fn should_visit(entry: &DirEntry) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    if entry.file_type().is_symlink() {
        return false;
    }
    let name = entry.file_name().to_string_lossy();
    if is_ignored_repository_path(Path::new(entry.file_name())) || name == corpus::CORPUS_DIRECTORY
    {
        return false;
    }
    true
}

fn check_control(control: Option<&WorkControl>) -> Result<()> {
    if let Some(control) = control {
        control.check()?;
    }
    Ok(())
}

fn read_text(path: &Path) -> Result<Option<String>> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.len() > MAX_READ_BYTES as u64 {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.contains(&0) {
        return Ok(None);
    }
    match String::from_utf8(bytes) {
        Ok(text) => Ok(Some(text)),
        Err(_) => Ok(None),
    }
}

/// Language-specific syntax selection is deliberately isolated from chunking;
/// unsupported files always retain the generic deterministic window path.
pub trait LanguageExtractor: Send + Sync {
    fn language(&self) -> tree_sitter::Language;
    fn declaration_kinds(&self) -> &'static [&'static str];
    fn wrapper_kinds(&self) -> &'static [&'static str] {
        &[]
    }
}

macro_rules! extractor {
    ($name:ident, $language:expr, [$($kind:literal),+ $(,)?]) => {
        struct $name;

        impl LanguageExtractor for $name {
            fn language(&self) -> tree_sitter::Language {
                $language.into()
            }

            fn declaration_kinds(&self) -> &'static [&'static str] {
                &[$($kind),+]
            }
        }
    };
}

struct PythonExtractor;

impl LanguageExtractor for PythonExtractor {
    fn language(&self) -> tree_sitter::Language {
        tree_sitter_python::LANGUAGE.into()
    }

    fn declaration_kinds(&self) -> &'static [&'static str] {
        &["class_definition", "function_definition"]
    }

    fn wrapper_kinds(&self) -> &'static [&'static str] {
        &["decorated_definition"]
    }
}

extractor!(
    RustExtractor,
    tree_sitter_rust::LANGUAGE,
    [
        "const_item",
        "enum_item",
        "function_item",
        "impl_item",
        "macro_definition",
        "mod_item",
        "static_item",
        "struct_item",
        "trait_item",
        "type_item",
        "union_item",
    ]
);
extractor!(
    CExtractor,
    tree_sitter_c::LANGUAGE,
    [
        "declaration",
        "enum_specifier",
        "function_definition",
        "struct_specifier",
        "type_definition"
    ]
);
extractor!(
    CppExtractor,
    tree_sitter_cpp::LANGUAGE,
    [
        "alias_declaration",
        "class_specifier",
        "declaration",
        "enum_specifier",
        "function_definition",
        "namespace_definition",
        "struct_specifier",
        "template_declaration",
        "type_definition",
    ]
);
struct JavaScriptExtractor;

impl LanguageExtractor for JavaScriptExtractor {
    fn language(&self) -> tree_sitter::Language {
        tree_sitter_javascript::LANGUAGE.into()
    }

    fn declaration_kinds(&self) -> &'static [&'static str] {
        &[
            "class_declaration",
            "function_declaration",
            "generator_function_declaration",
            "lexical_declaration",
            "variable_declaration",
        ]
    }

    fn wrapper_kinds(&self) -> &'static [&'static str] {
        &["export_statement"]
    }
}

struct TypeScriptExtractor {
    tsx: bool,
}

impl LanguageExtractor for TypeScriptExtractor {
    fn language(&self) -> tree_sitter::Language {
        if self.tsx {
            tree_sitter_typescript::LANGUAGE_TSX.into()
        } else {
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
        }
    }

    fn declaration_kinds(&self) -> &'static [&'static str] {
        &[
            "abstract_class_declaration",
            "class_declaration",
            "enum_declaration",
            "function_declaration",
            "generator_function_declaration",
            "interface_declaration",
            "internal_module",
            "lexical_declaration",
            "type_alias_declaration",
            "variable_declaration",
        ]
    }

    fn wrapper_kinds(&self) -> &'static [&'static str] {
        &["export_statement"]
    }
}

extractor!(
    GoExtractor,
    tree_sitter_go::LANGUAGE,
    [
        "const_declaration",
        "function_declaration",
        "method_declaration",
        "type_declaration",
        "var_declaration",
    ]
);
extractor!(
    JavaExtractor,
    tree_sitter_java::LANGUAGE,
    [
        "annotation_type_declaration",
        "class_declaration",
        "enum_declaration",
        "interface_declaration",
        "record_declaration",
    ]
);

fn extractor_for_path(path: &Path) -> Option<Box<dyn LanguageExtractor>> {
    match path.extension().and_then(|value| value.to_str())? {
        "py" => Some(Box::new(PythonExtractor)),
        "rs" => Some(Box::new(RustExtractor)),
        "c" | "h" => Some(Box::new(CExtractor)),
        "cc" | "cpp" | "hpp" => Some(Box::new(CppExtractor)),
        "js" => Some(Box::new(JavaScriptExtractor)),
        "ts" => Some(Box::new(TypeScriptExtractor { tsx: false })),
        "tsx" => Some(Box::new(TypeScriptExtractor { tsx: true })),
        "go" => Some(Box::new(GoExtractor)),
        "java" => Some(Box::new(JavaExtractor)),
        _ => None,
    }
}

fn language_chunks(
    path: &str,
    text: &str,
    extractor: &dyn LanguageExtractor,
) -> Result<Vec<Chunk>> {
    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    let mut parser = Parser::new();
    let language = extractor.language();
    parser.set_language(&language)?;
    let Some(tree) = parser.parse(text, None) else {
        return Ok(generic_chunks_for_range(path, &lines, 1, lines.len(), ""));
    };
    let root = tree.root_node();
    let mut symbols = Vec::new();
    collect_language_symbols(root, text, extractor, &mut symbols);
    symbols.sort_by_key(|(start, end, symbol)| (*start, *end, symbol.clone()));
    symbols.dedup();
    if symbols.is_empty() {
        return Ok(generic_chunks_for_range(path, &lines, 1, lines.len(), ""));
    }

    let mut chunks = Vec::new();
    let mut next_uncovered = 1;
    for (start, end, symbol) in symbols {
        if start > next_uncovered {
            chunks.extend(generic_chunks_for_range(
                path,
                &lines,
                next_uncovered,
                start - 1,
                "",
            ));
        }
        chunks.extend(symbol_chunks_for_range(path, &lines, start, end, &symbol));
        next_uncovered = next_uncovered.max(end.saturating_add(1));
    }
    if next_uncovered <= lines.len() {
        chunks.extend(generic_chunks_for_range(
            path,
            &lines,
            next_uncovered,
            lines.len(),
            "",
        ));
    }
    Ok(chunks)
}

fn collect_language_symbols(
    node: Node<'_>,
    source: &str,
    extractor: &dyn LanguageExtractor,
    symbols: &mut Vec<(usize, usize, String)>,
) {
    if let Some(symbol) = language_symbol(node, source, extractor) {
        symbols.push(symbol);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_language_symbols(child, source, extractor, symbols);
    }
}

fn language_symbol(
    node: Node<'_>,
    source: &str,
    extractor: &dyn LanguageExtractor,
) -> Option<(usize, usize, String)> {
    let definition = if extractor.declaration_kinds().contains(&node.kind()) {
        node
    } else if extractor.wrapper_kinds().contains(&node.kind()) {
        let mut cursor = node.walk();
        node.named_children(&mut cursor)
            .find(|child| extractor.declaration_kinds().contains(&child.kind()))?
    } else {
        return None;
    };
    let name = symbol_name(definition, source)?;
    let start = node.start_position().row + 1;
    let end = (node.end_position().row + 1).max(start);
    Some((start, end, name))
}

fn symbol_name(node: Node<'_>, source: &str) -> Option<String> {
    if let Some(name) = node.child_by_field_name("name") {
        return name.utf8_text(source.as_bytes()).ok().map(str::to_owned);
    }
    if matches!(
        node.kind(),
        "identifier" | "field_identifier" | "type_identifier"
    ) {
        return node.utf8_text(source.as_bytes()).ok().map(str::to_owned);
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(|child| symbol_name(child, source))
}

fn generic_chunks_for_range(
    path: &str,
    lines: &[&str],
    start: usize,
    end: usize,
    symbol: &str,
) -> Vec<Chunk> {
    if start == 0 || start > end || start > lines.len() {
        return Vec::new();
    }
    let bounded_end = end.min(lines.len());
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    while chunk_start <= bounded_end {
        let chunk_end = (chunk_start + GENERIC_CHUNK_LINES - 1).min(bounded_end);
        chunks.push(make_chunk(path, lines, chunk_start, chunk_end, symbol));
        if chunk_end == bounded_end {
            break;
        }
        chunk_start = chunk_end + 1 - GENERIC_CHUNK_OVERLAP_LINES;
    }
    chunks
}

fn symbol_chunks_for_range(
    path: &str,
    lines: &[&str],
    start: usize,
    end: usize,
    symbol: &str,
) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    let bounded_end = end.min(lines.len());
    while chunk_start <= bounded_end {
        let mut chunk_end = chunk_start;
        let mut bytes = 0;
        while chunk_end <= bounded_end {
            let next = lines[chunk_end - 1].len() + 1;
            if bytes >= MIN_SYMBOL_CHUNK_BYTES && bytes + next > MAX_SYMBOL_CHUNK_BYTES {
                break;
            }
            bytes += next;
            chunk_end += 1;
        }
        let inclusive_end = chunk_end.saturating_sub(1).max(chunk_start);
        chunks.push(make_chunk(path, lines, chunk_start, inclusive_end, symbol));
        chunk_start = inclusive_end.saturating_add(1);
    }
    chunks
}

fn make_chunk(path: &str, lines: &[&str], start: usize, end: usize, symbol: &str) -> Chunk {
    let mut content = lines[start - 1..end].join("\n");
    truncate_utf8(&mut content, MAX_SYMBOL_CHUNK_BYTES);
    let content_digest = Sha256::digest(content.as_bytes());
    let content_suffix = content_digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Chunk {
        id: format!(
            "{}:{content_suffix}",
            chunk_id(path, start as u32, end as u32, symbol)
        ),
        path: path.to_owned(),
        start_line: start as u32,
        end_line: end as u32,
        symbol: symbol.to_owned(),
        content,
    }
}

fn chunk_id(path: &str, start: u32, end: u32, symbol: &str) -> String {
    format!("{path}:{start}:{end}:{symbol}")
}

fn symbol_kind(chunk: &Chunk) -> String {
    let signature = chunk.content.lines().next().unwrap_or_default().trim();
    if [
        "class ",
        "struct ",
        "interface ",
        "trait ",
        "enum ",
        "record ",
    ]
    .iter()
    .any(|prefix| signature.starts_with(prefix))
    {
        "class".to_owned()
    } else {
        "function".to_owned()
    }
}

fn symbol_signature(chunk: &Chunk) -> String {
    let line = chunk
        .content
        .lines()
        .find(|line| !line.trim_start().starts_with('@'))
        .unwrap_or_default()
        .trim();
    let line = line
        .strip_prefix("async def ")
        .or_else(|| line.strip_prefix("def "))
        .or_else(|| line.strip_prefix("fn "))
        .or_else(|| line.strip_prefix("function "))
        .unwrap_or(line);
    line.trim_end_matches([':', '{', ';']).trim().to_owned()
}

fn term_match_count(chunk: &Chunk, terms: &[String]) -> usize {
    let haystack =
        format!("{}\n{}\n{}", chunk.path, chunk.symbol, chunk.content).to_ascii_lowercase();
    terms
        .iter()
        .filter(|term| haystack.contains((*term).as_str()))
        .count()
}

fn deterministic_rank_score(hit: &SearchHit, terms: &[String]) -> f32 {
    let path = hit.path.to_ascii_lowercase();
    let symbol = hit.symbol.to_ascii_lowercase();
    let content = hit.content.to_ascii_lowercase();
    terms
        .iter()
        .map(|term| {
            let mut score = 0.0;
            if path.contains(term) {
                score += 3.0;
            }
            if symbol.contains(term) {
                score += 8.0;
            }
            if content.contains(term) {
                score += 1.0;
            }
            score
        })
        .sum()
}

fn truncate_utf8(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_reject_absolute_and_parent_traversal() {
        assert!(validate_relative_path("/etc/passwd").is_err());
        assert!(validate_relative_path("src/../../secret").is_err());
        assert_eq!(
            validate_relative_path("./src/auth.py").unwrap(),
            "src/auth.py"
        );
    }

    #[test]
    fn generic_windows_overlap_by_eight_lines() {
        let owned: Vec<String> = (1..=100).map(|line| format!("line {line}")).collect();
        let lines: Vec<&str> = owned.iter().map(String::as_str).collect();
        let chunks = generic_chunks_for_range("notes.txt", &lines, 1, 100, "");
        assert_eq!((chunks[0].start_line, chunks[0].end_line), (1, 80));
        assert_eq!((chunks[1].start_line, chunks[1].end_line), (73, 100));
    }

    #[test]
    fn snippets_clamp_partial_ranges_and_reject_ranges_without_lines() {
        let root = tempfile::tempdir().expect("temporary repository");
        fs::write(root.path().join("lines.txt"), "one\ntwo\nthree\n").expect("write line fixture");
        fs::write(root.path().join("empty.txt"), "").expect("write empty fixture");
        let index = ProjectIndex::build(root.path()).expect("build index");

        let final_line = index
            .snippet("lines.txt", 3, Some(3))
            .expect("select final line");
        assert_eq!((final_line.start_line, final_line.end_line), (3, 3));
        assert_eq!(final_line.content, "three");

        let partial = index
            .snippet("lines.txt", 2, Some(99))
            .expect("clamp partially overlapping range");
        assert_eq!((partial.start_line, partial.end_line), (2, 3));
        assert_eq!(partial.content, "two\nthree");

        let normalized = index
            .snippet("lines.txt", 0, Some(0))
            .expect("normalize coordinates below the first line");
        assert_eq!((normalized.start_line, normalized.end_line), (1, 1));

        let beyond_eof = index
            .snippet("lines.txt", 99_999, Some(100_000))
            .expect_err("reject a wholly out-of-range request");
        assert!(beyond_eof.to_string().contains("exceeds file line count 3"));

        let empty = index
            .snippet("empty.txt", 1, Some(1))
            .expect_err("reject a range from an empty file");
        assert!(empty.to_string().contains("empty file"));
    }

    #[test]
    fn scanner_chunks_unfamiliar_utf8_files_and_skips_binary_data() {
        let root = tempfile::tempdir().expect("temporary repository");
        fs::write(
            root.path().join("Cargo.lock"),
            "[[package]]\nname = \"scanner-marker\"\nversion = \"1.0.0\"\n",
        )
        .expect("write UTF-8 fixture");
        fs::write(root.path().join("artifact.bin"), b"binary\0payload")
            .expect("write binary fixture");

        let scanned = scan_repository(root.path(), None, None).expect("scan repository");
        let chunks = scanned.chunks;
        let stats = scanned.stats;

        assert!(chunks.iter().any(|chunk| chunk.path == "Cargo.lock"));
        assert!(!chunks.iter().any(|chunk| chunk.path == "artifact.bin"));
        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.skipped_binary, 1);
    }

    #[test]
    fn scanner_and_file_resources_reject_oversized_text_before_reading() {
        let root = tempfile::tempdir().expect("temporary repository");
        let large = root.path().join("generated.min.js");
        fs::write(&large, vec![b'x'; MAX_READ_BYTES + 1]).expect("write oversized fixture");

        let chunks = scan_repository(root.path(), None, None)
            .expect("scan repository")
            .chunks;
        assert!(!chunks.iter().any(|chunk| chunk.path == "generated.min.js"));

        let index = ProjectIndex::build(root.path()).expect("build index");
        assert!(index.file_content("generated.min.js", 1024).is_err());
        assert!(index.file_line_count("generated.min.js").is_err());
    }

    #[test]
    fn python_top_level_symbols_are_named() {
        let chunks = language_chunks(
            "src/auth.py",
            "class Auth:\n    def login(self):\n        return True\n\ndef issue_token():\n    return 'x'\n",
            &PythonExtractor,
        )
        .unwrap();
        assert!(chunks.iter().any(|chunk| chunk.symbol == "Auth"));
        assert!(chunks.iter().any(|chunk| chunk.symbol == "issue_token"));
    }

    #[test]
    fn supported_language_extractors_produce_searchable_symbols() {
        let root = tempfile::tempdir().expect("temporary repository");
        let fixtures = [
            ("marker.py", "def pymarker():\n    return 1\n"),
            ("marker.rs", "fn rustmarker() {}\n"),
            ("marker.c", "int cmarker(void) { return 1; }\n"),
            ("marker.cpp", "class cppmarker {};\n"),
            ("marker.js", "function jsmarker() { return 1; }\n"),
            ("marker.ts", "interface tsmarker { value: number }\n"),
            ("marker.tsx", "function tsxmarker() { return <div />; }\n"),
            ("marker.go", "package marker\nfunc gomarker() {}\n"),
            ("marker.java", "class javamarker {}\n"),
        ];
        for (path, source) in fixtures {
            fs::write(root.path().join(path), source).expect("write fixture");
        }
        let index = ProjectIndex::build(root.path()).expect("build multilingual index");
        assert!(index.stats().symbol_chunks >= fixtures.len());
        for (path, _) in fixtures {
            let query = path.split('.').next().expect("fixture stem");
            let marker = match query {
                "marker" => path
                    .split('.')
                    .nth(1)
                    .map(|extension| format!("{extension}marker"))
                    .unwrap(),
                _ => unreachable!(),
            };
            let marker = if path == "marker.cpp" {
                "cppmarker".to_owned()
            } else if path == "marker.java" {
                "javamarker".to_owned()
            } else if path == "marker.py" {
                "pymarker".to_owned()
            } else if path == "marker.rs" {
                "rustmarker".to_owned()
            } else if path == "marker.tsx" {
                "tsxmarker".to_owned()
            } else {
                marker
            };
            let (hits, _) = index.search(&marker, &[], 1).expect("search symbol");
            assert_eq!(hits.first().map(|hit| hit.path.as_str()), Some(path));
        }
    }

    #[test]
    fn linked_worktree_git_pointer_is_never_indexed_or_signed() {
        let main = tempfile::tempdir().expect("main root");
        let linked = tempfile::tempdir().expect("linked root");
        fs::write(main.path().join("src.rs"), "fn anchor() {}\n").expect("main source");
        fs::write(linked.path().join("src.rs"), "fn anchor() {}\n").expect("linked source");
        fs::create_dir(main.path().join(".git")).expect("main git directory");
        fs::write(
            linked.path().join(".git"),
            "gitdir: /private/common.git/worktrees/x\n",
        )
        .expect("linked git pointer");
        let main_index = ProjectIndex::build(main.path()).expect("main index");
        let linked_index = ProjectIndex::build(linked.path()).expect("linked index");
        assert_eq!(
            main_index.stats().refresh_signature,
            linked_index.stats().refresh_signature
        );
        assert!(!linked_index.all_paths().iter().any(|path| path == ".git"));
    }

    #[test]
    fn generated_trees_do_not_affect_index_or_signature() {
        let root = tempfile::tempdir().expect("repository root");
        fs::write(root.path().join("anchor.rs"), "fn anchor() {}\n").expect("anchor");
        let before = ProjectIndex::build(root.path()).expect("initial index");
        let before_signature = repository_signature(root.path()).expect("initial signature");

        let generated = root.path().join(".workingdir/agent-worktree/target/debug");
        fs::create_dir_all(&generated).expect("generated tree");
        fs::write(
            generated.join("churn.rs"),
            "fn generated_churn_must_not_be_indexed() {}\n",
        )
        .expect("generated churn");

        let after = ProjectIndex::build(root.path()).expect("index after churn");
        let after_signature = repository_signature(root.path()).expect("signature after churn");
        assert_eq!(before.stats().file_count, after.stats().file_count);
        assert_eq!(before_signature, after_signature);
        assert!(
            !after
                .all_paths()
                .iter()
                .any(|path| path.contains(".workingdir"))
        );
        assert!(is_ignored_repository_path(Path::new(
            ".workingdir/agent-worktree/target/debug/churn.rs"
        )));
        assert!(
            git_exclude_pathspecs()
                .iter()
                .any(|path| path == ":(exclude,glob)**/.workingdir/**")
        );
    }

    fn assert_incremental_matches_full(root: &Path, incremental: &ProjectIndex) {
        let full = ProjectIndex::build(root).expect("clean full build");
        assert_eq!(
            incremental.stats().refresh_signature,
            full.stats().refresh_signature
        );
        assert_eq!(incremental.all_paths(), full.all_paths());
        assert_eq!(incremental.chunks.as_ref(), full.chunks.as_ref());
        for prompt in ["alpha changed", "beta", "renamed", "untracked"] {
            let incremental_hits = incremental
                .search(prompt, &[], 8)
                .expect("incremental hits");
            let full_hits = full.search(prompt, &[], 8).expect("full hits");
            assert_eq!(
                incremental_hits
                    .0
                    .iter()
                    .map(|hit| (&hit.id, &hit.path))
                    .collect::<Vec<_>>(),
                full_hits
                    .0
                    .iter()
                    .map(|hit| (&hit.id, &hit.path))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn incremental_refresh_matches_full_for_change_delete_rename_and_untracked() {
        let root = tempfile::tempdir().expect("repository root");
        fs::write(root.path().join("a.rs"), "fn alpha() {}\n").expect("a");
        fs::write(root.path().join("b.rs"), "fn beta() {}\n").expect("b");
        let mut index = ProjectIndex::build(root.path()).expect("initial index");

        fs::write(root.path().join("a.rs"), "fn alpha_changed() {}\n").expect("change");
        index = index
            .refresh_paths_controlled(&["a.rs".to_owned()], None)
            .expect("change");
        assert_incremental_matches_full(root.path(), &index);

        fs::remove_file(root.path().join("b.rs")).expect("delete");
        index = index
            .refresh_paths_controlled(&["b.rs".to_owned()], None)
            .expect("delete");
        assert_incremental_matches_full(root.path(), &index);

        fs::rename(root.path().join("a.rs"), root.path().join("renamed.rs")).expect("rename");
        index = index
            .refresh_paths_controlled(&["a.rs".to_owned(), "renamed.rs".to_owned()], None)
            .expect("rename");
        assert_incremental_matches_full(root.path(), &index);

        fs::write(root.path().join("untracked.rs"), "fn untracked() {}\n").expect("untracked");
        index = index
            .refresh_paths_controlled(&["untracked.rs".to_owned()], None)
            .expect("untracked");
        assert_incremental_matches_full(root.path(), &index);

        assert!(
            index
                .refresh_paths_controlled(&["reference-corpus/manifest.json".to_owned()], None)
                .is_err()
        );
        assert!(
            index
                .refresh_paths_controlled(
                    &(0..1025).map(|i| format!("{i}.rs")).collect::<Vec<_>>(),
                    None
                )
                .is_err()
        );
    }

    #[test]
    fn persisted_snapshot_reuses_complete_generation_and_rejects_corruption() {
        let root = tempfile::tempdir().expect("repository root");
        fs::write(root.path().join("lib.rs"), "fn persisted_anchor() {}\n").expect("source");
        let index = ProjectIndex::build(root.path()).expect("index");
        let snapshot = index.snapshot();
        let restored =
            ProjectIndex::from_snapshot_controlled(root.path(), snapshot.clone(), None, None)
                .expect("valid persisted generation");
        assert_eq!(restored.chunks.as_ref(), index.chunks.as_ref());

        let mut corrupt = snapshot;
        corrupt.refresh_signature = "files:corrupt".to_owned();
        assert!(ProjectIndex::from_snapshot_controlled(root.path(), corrupt, None, None).is_err());
    }
}
