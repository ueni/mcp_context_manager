//! Isolated v2 LMDB state, durable imports, references, and telemetry.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use anyhow::{Result, anyhow, bail};
use heed::{
    Database, EnvOpenOptions,
    types::{Bytes, Str},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

pub type V2Environment = heed::Env;

pub const CODEC_VERSION: u8 = 1;
const V2_MAP_SIZE: usize = 1024 * 1024 * 1024;
// The LMDB map itself bounds the number of possible records. This conservative
// minimum encoded-record footprint keeps a single prune transaction bounded.
const MAX_DELETE_BATCH: usize = V2_MAP_SIZE / 16;

#[derive(Clone, Debug)]
pub struct StatePaths {
    pub overlay: PathBuf,
    pub lmdb: PathBuf,
    pub index: PathBuf,
    pub server_lock: PathBuf,
}

pub struct StateStore {
    paths: StatePaths,
    shared: Arc<SharedDatabase>,
}

struct SharedDatabase {
    environment: V2Environment,
    records: Database<Str, Bytes>,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<SharedDatabase>>>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceValidation {
    pub reference_id: String,
    encoded_sha256: String,
    expires_at_unix_seconds: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredJson {
    canonical_json: Vec<u8>,
}

#[derive(Debug)]
pub enum CodecError {
    Empty,
    UnsupportedVersion(u8),
    Postcard(postcard::Error),
}

pub fn encode<T: Serialize>(value: &T) -> std::result::Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_stdvec(value)?;
    let mut encoded = Vec::with_capacity(payload.len() + 1);
    encoded.push(CODEC_VERSION);
    encoded.extend(payload);
    Ok(encoded)
}

pub fn decode<T: DeserializeOwned>(value: &[u8]) -> std::result::Result<T, CodecError> {
    let Some((&version, payload)) = value.split_first() else {
        return Err(CodecError::Empty);
    };
    if version != CODEC_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    postcard::from_bytes(payload).map_err(CodecError::Postcard)
}

impl StateStore {
    /// Open the isolated Rust overlay beside the Python state. This never
    /// opens or mutates the Python LMDB environment.
    pub fn open(project_state: impl AsRef<Path>) -> Result<Self> {
        let overlay = project_state.as_ref().join("rust-v2");
        let lmdb = overlay.join("state.lmdb");
        let index = overlay.join("index");
        let server_lock = overlay.join("server.lock");
        fs::create_dir_all(&lmdb)?;
        fs::create_dir_all(&index)?;
        let key = lmdb.canonicalize()?;
        let mut opened = OPEN_DATABASES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| anyhow!("database registry lock poisoned"))?;
        opened.retain(|_, database| database.strong_count() > 0);
        let shared = if let Some(database) = opened.get(&key).and_then(Weak::upgrade) {
            database
        } else {
            let mut options = EnvOpenOptions::new();
            options.max_dbs(2).map_size(V2_MAP_SIZE);
            let environment = unsafe { options.open(&lmdb)? };
            let mut txn = environment.write_txn()?;
            let records = environment.create_database::<Str, Bytes>(&mut txn, Some("records"))?;
            txn.commit()?;
            let database = Arc::new(SharedDatabase {
                environment,
                records,
            });
            opened.insert(key, Arc::downgrade(&database));
            database
        };
        Ok(Self {
            paths: StatePaths {
                overlay,
                lmdb,
                index,
                server_lock,
            },
            shared,
        })
    }

    pub fn paths(&self) -> &StatePaths {
        &self.paths
    }

    pub fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let read_txn = self.shared.environment.read_txn()?;
        self.shared
            .records
            .get(&read_txn, key)?
            .map(decode_json_record)
            .transpose()
    }

    pub fn put_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        validate_state_key(key)?;
        let encoded = encode_json_record(value)?;
        let mut write_txn = self.shared.environment.write_txn()?;
        self.shared.records.put(&mut write_txn, key, &encoded)?;
        write_txn.commit()?;
        Ok(())
    }

    /// Inserts a generated record only when no writer has committed that key.
    pub fn put_json_if_absent(&self, key: &str, value: &serde_json::Value) -> Result<bool> {
        validate_state_key(key)?;
        let encoded = encode_json_record(value)?;
        let mut txn = self.shared.environment.write_txn()?;
        if self.shared.records.get(&txn, key)?.is_some() {
            return Ok(false);
        }
        self.shared.records.put(&mut txn, key, &encoded)?;
        txn.commit()?;
        Ok(true)
    }

    /// A generated-job transition and its imported records commit together.
    pub fn compare_and_swap_json_batch(
        &self,
        key: &str,
        expected: &serde_json::Value,
        replacement: &serde_json::Value,
        rows: &[(String, serde_json::Value)],
    ) -> Result<()> {
        validate_state_key(key)?;
        if rows.len() > 100 {
            bail!("conditional state batch exceeds 100 rows");
        }
        let expected = encode_json_record(expected)?;
        let replacement = encode_json_record(replacement)?;
        let encoded = rows
            .iter()
            .map(|(key, value)| {
                validate_state_key(key)?;
                Ok((key, encode_json_record(value)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut txn = self.shared.environment.write_txn()?;
        if self.shared.records.get(&txn, key)? != Some(expected.as_slice()) {
            bail!("generated job revision conflict");
        }
        for (key, value) in encoded {
            self.shared.records.put(&mut txn, key, &value)?;
        }
        self.shared.records.put(&mut txn, key, &replacement)?;
        txn.commit()?;
        Ok(())
    }

    pub fn put_json_if_changed(&self, key: &str, value: &serde_json::Value) -> Result<bool> {
        validate_state_key(key)?;
        let encoded = encode_json_record(value)?;
        let read_txn = self.shared.environment.read_txn()?;
        let unchanged = self.shared.records.get(&read_txn, key)? == Some(encoded.as_slice());
        drop(read_txn);
        if unchanged {
            return Ok(false);
        }
        let mut write_txn = self.shared.environment.write_txn()?;
        self.shared.records.put(&mut write_txn, key, &encoded)?;
        write_txn.commit()?;
        Ok(true)
    }

    pub fn put_json_batch(&self, rows: &[(String, serde_json::Value)]) -> Result<()> {
        if rows.len() > MAX_DELETE_BATCH {
            bail!("state write batch exceeds {MAX_DELETE_BATCH} rows");
        }
        let encoded = rows
            .iter()
            .map(|(key, value)| {
                validate_state_key(key)?;
                Ok((key, encode_json_record(value)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut write_txn = self.shared.environment.write_txn()?;
        for (key, value) in encoded {
            self.shared.records.put(&mut write_txn, key, &value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        validate_state_key(key)?;
        let mut write_txn = self.shared.environment.write_txn()?;
        let deleted = self.shared.records.delete(&mut write_txn, key)?;
        write_txn.commit()?;
        Ok(deleted)
    }

    pub fn delete_batch(&self, keys: &[String]) -> Result<usize> {
        if keys.len() > MAX_DELETE_BATCH {
            bail!("state delete batch exceeds {MAX_DELETE_BATCH} keys");
        }
        for key in keys {
            validate_state_key(key)?;
        }
        let mut write_txn = self.shared.environment.write_txn()?;
        let mut deleted = 0_usize;
        for key in keys {
            if self.shared.records.delete(&mut write_txn, key)? {
                deleted = deleted.saturating_add(1);
            }
        }
        write_txn.commit()?;
        Ok(deleted)
    }

    pub fn iter_json(&self, prefix: &str) -> Result<Vec<(String, serde_json::Value)>> {
        if !prefix.is_empty() {
            validate_state_key(prefix)?;
        }
        let read_txn = self.shared.environment.read_txn()?;
        let mut rows = Vec::new();
        if prefix.is_empty() {
            for row in self.shared.records.iter(&read_txn)? {
                let (key, value) = row?;
                rows.push((key.to_owned(), decode_json_record(value)?));
            }
        } else {
            for row in self.shared.records.prefix_iter(&read_txn, prefix)? {
                let (key, value) = row?;
                rows.push((key.to_owned(), decode_json_record(value)?));
            }
        }
        Ok(rows)
    }

    /// Returns the current immutable wiki revision for a page.
    pub fn get_wiki_page(&self, page_id: &str) -> Result<Option<serde_json::Value>> {
        validate_wiki_page_id(page_id)?;
        self.get_json(&format!("wiki:page:{page_id}"))
    }

    /// Lists current wiki revisions. Page records are small metadata pointers;
    /// their immutable bodies remain separately addressable by revision hash.
    pub fn list_wiki_pages(&self, limit: usize) -> Result<Vec<serde_json::Value>> {
        let mut pages = self
            .iter_json("wiki:page:")?
            .into_iter()
            .map(|(_, page)| page)
            .collect::<Vec<_>>();
        pages.sort_by(|left, right| {
            left.get("page_id")
                .and_then(serde_json::Value::as_str)
                .cmp(&right.get("page_id").and_then(serde_json::Value::as_str))
        });
        pages.truncate(limit);
        Ok(pages)
    }

    pub fn list_all_wiki_pages(&self) -> Result<Vec<serde_json::Value>> {
        let mut pages = self
            .iter_json("wiki:page:")?
            .into_iter()
            .map(|(_, page)| page)
            .collect::<Vec<_>>();
        pages.sort_by(|left, right| {
            left.get("page_id")
                .and_then(serde_json::Value::as_str)
                .cmp(&right.get("page_id").and_then(serde_json::Value::as_str))
        });
        Ok(pages)
    }

    /// Atomically stores a new immutable revision and moves the current pointer.
    /// The expected hash makes concurrent updates fail without losing either edit.
    pub fn put_wiki_revision(
        &self,
        page_id: &str,
        expected_revision: Option<&str>,
        page: &serde_json::Value,
    ) -> Result<String> {
        let mut revisions = self.put_wiki_revisions_batch(&[(
            page_id.to_owned(),
            expected_revision.map(str::to_owned),
            page.clone(),
        )])?;
        Ok(revisions.remove(0))
    }

    /// Atomically publishes a group of page revisions after checking every
    /// expected revision. Used by consolidation to update replacement links.
    pub fn put_wiki_revisions_batch(
        &self,
        pages: &[(String, Option<String>, serde_json::Value)],
    ) -> Result<Vec<String>> {
        if pages.is_empty() || pages.len() > 100 {
            bail!("wiki revision batch must contain 1 to 100 pages");
        }
        let mut ids = std::collections::HashSet::new();
        let mut prepared = Vec::with_capacity(pages.len());
        for (page_id, _, page) in pages {
            validate_wiki_page_id(page_id)?;
            if !ids.insert(page_id) {
                bail!("wiki revision batch contains duplicate page ids");
            }
            let body = serde_json::to_vec(page)?;
            let revision = sha256_bytes(&body);
            prepared.push((
                format!("wiki:page:{page_id}"),
                format!("wiki:revision:{page_id}:{revision}"),
                encode_json_record(page)?,
                encode_json_record(&serde_json::json!({
                    "schema": "agent_wiki.page_pointer.v1",
                    "page_id": page_id,
                    "revision": revision,
                }))?,
                revision,
            ));
        }
        let mut write_txn = self.shared.environment.write_txn()?;
        for ((page_id, expected_revision, _), (page_key, _, _, _, _)) in pages.iter().zip(&prepared)
        {
            let current = self
                .shared
                .records
                .get(&write_txn, page_key)?
                .map(decode_json_record)
                .transpose()?;
            let current_revision = current
                .as_ref()
                .and_then(|value| value.get("revision"))
                .and_then(serde_json::Value::as_str);
            if current_revision != expected_revision.as_deref() {
                bail!(
                    "wiki revision conflict for {page_id}: expected {:?}, current {:?}",
                    expected_revision,
                    current_revision
                );
            }
        }
        for (page_key, revision_key, encoded_revision, encoded_pointer, _) in &prepared {
            self.shared
                .records
                .put(&mut write_txn, revision_key, encoded_revision)?;
            self.shared
                .records
                .put(&mut write_txn, page_key, encoded_pointer)?;
        }
        write_txn.commit()?;
        Ok(prepared
            .into_iter()
            .map(|(_, _, _, _, revision)| revision)
            .collect())
    }

    /// Reads a specific immutable page revision, even after consolidation.
    pub fn get_wiki_revision(
        &self,
        page_id: &str,
        revision: &str,
    ) -> Result<Option<serde_json::Value>> {
        validate_wiki_page_id(page_id)?;
        if revision.len() != 64 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("wiki revision must be a SHA-256 hash");
        }
        let page = self.get_json(&format!("wiki:revision:{page_id}:{revision}"))?;
        if let Some(page) = &page
            && sha256_bytes(&serde_json::to_vec(page)?) != revision
        {
            bail!("wiki revision integrity check failed");
        }
        Ok(page)
    }

    /// Restartably imports the wiki namespace from an older checkout store.
    /// Each row is committed independently; the completion marker is written
    /// last, so interruption safely resumes without replacing shared edits.
    pub fn migrate_wiki_from(
        &self,
        source: &StateStore,
        source_project_id: &str,
        repository_id: &str,
    ) -> Result<()> {
        if source_project_id.is_empty()
            || !source_project_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!("invalid legacy wiki owner");
        }
        let marker = format!("wiki:migration:v1:{source_project_id}");
        if self.get_json(&marker)?.is_some() {
            return Ok(());
        }
        self.put_json_if_absent(
            &format!("wiki:owner:{source_project_id}"),
            &serde_json::json!({"repository_id":repository_id}),
        )?;
        let mut page_aliases = HashMap::new();
        for source_pointer in source.list_all_wiki_pages()? {
            let Some(page_id) = source_pointer
                .get("page_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(revision) = source_pointer
                .get("revision")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(_) = source.get_wiki_revision(page_id, revision)? else {
                continue;
            };
            let revisions = source.iter_json(&format!("wiki:revision:{page_id}:"))?;
            let destination_id = self.import_legacy_wiki_page(
                source_project_id,
                page_id,
                &source_pointer,
                &revisions,
            )?;
            page_aliases.insert(page_id.to_owned(), destination_id);
        }
        let source_rows = source.iter_json("wiki:")?;
        for (key, value) in source_rows {
            if key.starts_with("wiki:page:")
                || key.starts_with("wiki:revision:")
                || key.starts_with("wiki:alias:")
                || key.starts_with("wiki:migration:")
            {
                continue;
            }
            if let Some(old_source_id) = key
                .strip_prefix("wiki:source_current:")
                .filter(|id| id.starts_with("md-"))
            {
                let Some(old_hash) = value
                    .get("sha256")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                let source_key = format!("wiki:source_revision:{old_source_id}:{old_hash}");
                let Some(source_revision) = source.get_json(&source_key)? else {
                    continue;
                };
                let Some(path) = source_revision
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let new_source_id = format!(
                    "md-{}-{}",
                    &sha256_bytes(source_project_id.as_bytes())[..12],
                    &sha256_bytes(format!("{source_project_id}:{path}").as_bytes())[..24]
                );
                let mut pointer = value;
                pointer["source_id"] = serde_json::json!(new_source_id);
                self.put_json_if_absent(
                    &format!("wiki:source_revision:{new_source_id}:{old_hash}"),
                    &source_revision,
                )?;
                self.put_json_if_absent(&format!("wiki:source_current:{new_source_id}"), &pointer)?;
                self.put_json_if_absent(
                    &format!("wiki:source_alias:{source_project_id}:{old_source_id}"),
                    &serde_json::json!({"source_id":new_source_id}),
                )?;
                continue;
            }
            self.put_json_if_absent(&key, &value)?;
        }
        for (key, value) in source.iter_json("wiki:revision:")? {
            let remainder = key
                .strip_prefix("wiki:revision:")
                .ok_or_else(|| anyhow!("invalid legacy wiki revision key"))?;
            let Some((page_id, revision)) = remainder.rsplit_once(':') else {
                continue;
            };
            let destination_id = page_aliases
                .get(page_id)
                .map(String::as_str)
                .unwrap_or(page_id);
            self.put_json_if_absent(
                &format!("wiki:revision:{destination_id}:{revision}"),
                &value,
            )?;
        }
        self.put_json_if_absent(&marker, &serde_json::json!({"status":"complete"}))?;
        Ok(())
    }

    fn import_legacy_wiki_page(
        &self,
        owner: &str,
        page_id: &str,
        source_pointer: &serde_json::Value,
        revisions: &[(String, serde_json::Value)],
    ) -> Result<String> {
        validate_wiki_page_id(page_id)?;
        let current_revision = source_pointer["revision"]
            .as_str()
            .ok_or_else(|| anyhow!("legacy wiki pointer is invalid"))?;
        let mut txn = self.shared.environment.write_txn()?;
        let migration_page_key =
            format!("wiki:migration_page:{owner}:{page_id}:{current_revision}");
        if let Some(record) = self.shared.records.get(&txn, &migration_page_key)? {
            let record = decode_json_record(record)?;
            return record["page_id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("legacy wiki migration marker is invalid"));
        }
        let original_key = format!("wiki:page:{page_id}");
        let current = self
            .shared
            .records
            .get(&txn, &original_key)?
            .map(decode_json_record)
            .transpose()?;
        let destination_id = if current
            .as_ref()
            .is_none_or(|pointer| pointer["revision"] == current_revision)
        {
            page_id.to_owned()
        } else {
            let digest = sha256_bytes(&serde_json::to_vec(&serde_json::json!({
                "page_id": page_id,
                "revision": current_revision,
            }))?);
            format!("legacy-{}", &digest[..24])
        };
        let page_key = format!("wiki:page:{destination_id}");
        let target_current = self
            .shared
            .records
            .get(&txn, &page_key)?
            .map(decode_json_record)
            .transpose()?;
        for (key, value) in revisions {
            let suffix = key
                .strip_prefix(&format!("wiki:revision:{page_id}:"))
                .ok_or_else(|| anyhow!("invalid legacy wiki revision key"))?;
            let hash = suffix;
            if sha256_bytes(&serde_json::to_vec(value)?) != hash {
                bail!("legacy wiki revision failed its content hash");
            }
            let target_key = format!("wiki:revision:{destination_id}:{suffix}");
            if self.shared.records.get(&txn, &target_key)?.is_none() {
                self.shared
                    .records
                    .put(&mut txn, &target_key, &encode_json_record(value)?)?;
            }
        }
        if target_current.is_none() {
            let mut pointer = source_pointer.clone();
            pointer["page_id"] = serde_json::json!(destination_id);
            self.shared
                .records
                .put(&mut txn, &page_key, &encode_json_record(&pointer)?)?;
        }
        if destination_id != page_id {
            let alias_key = format!("wiki:alias:{owner}:{page_id}");
            let alias = serde_json::json!({"page_id":destination_id});
            if self.shared.records.get(&txn, &alias_key)?.is_none() {
                self.shared
                    .records
                    .put(&mut txn, &alias_key, &encode_json_record(&alias)?)?;
            }
        }
        self.shared.records.put(
            &mut txn,
            &migration_page_key,
            &encode_json_record(&serde_json::json!({"page_id":destination_id}))?,
        )?;
        txn.commit()?;
        Ok(destination_id)
    }

    pub fn create_reference(
        &self,
        producer: &str,
        project_id: &str,
        payload: &serde_json::Value,
        summary: &serde_json::Value,
        ttl_hours: i64,
    ) -> Result<serde_json::Value> {
        let identity = serde_json::to_vec(&serde_json::json!({
            "producer": producer,
            "project_id": project_id,
            "payload": payload,
        }))?;
        let reference_id = format!("ctxref-{}", &sha256_bytes(&identity)[..16]);
        if let Some(record) = self.get_json(&format!("reference:{reference_id}"))?
            && !reference_is_expired(&record, OffsetDateTime::now_utc())
            && let Some(body) = record.get("body").and_then(serde_json::Value::as_str)
            && record.get("sha256").and_then(serde_json::Value::as_str)
                == Some(sha256_bytes(body.as_bytes()).as_str())
            && let Ok(envelope) = serde_json::from_str::<serde_json::Value>(body)
            && envelope.get("payload") == Some(payload)
            && envelope
                .pointer("/metadata/producer_tool")
                .and_then(serde_json::Value::as_str)
                == Some(producer)
            && envelope
                .pointer("/metadata/project_id")
                .and_then(serde_json::Value::as_str)
                == Some(project_id)
        {
            return Ok(public_reference(&record, &envelope, ttl_hours));
        }

        let now = OffsetDateTime::now_utc();
        let created_at = now.format(&Rfc3339)?;
        let expires_at = (now + Duration::hours(ttl_hours.max(1))).format(&Rfc3339)?;
        let envelope = serde_json::json!({
            "schema": "mcp_result_reference.envelope.v1",
            "metadata": {
                "reference_id": reference_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "producer_tool": producer,
                "project_id": project_id,
            },
            "summary": summary,
            "payload": payload,
            "sensitivity": default_sensitivity(false),
        });
        let body = String::from_utf8(serde_json::to_vec(&envelope)?)?;
        let digest = sha256_bytes(body.as_bytes());
        let size_bytes = body.len();
        let record = serde_json::json!({
            "schema": "mcp_result_reference.store.v1",
            "reference_id": reference_id,
            "project_id": project_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "storage": "inline",
            "size_bytes": size_bytes,
            "sha256": digest,
            "body": body,
        });
        self.put_json(&format!("reference:{reference_id}"), &record)?;
        Ok(public_reference(&record, &envelope, ttl_hours))
    }

    pub fn list_references(&self, limit: usize) -> Result<Vec<serde_json::Value>> {
        let now = OffsetDateTime::now_utc();
        let mut references = self
            .iter_json("reference:")?
            .into_iter()
            .filter_map(|(_, record)| {
                let reference_id = record.get("reference_id")?.as_str()?;
                let status = if reference_is_expired(&record, now) {
                    "expired"
                } else {
                    "active"
                };
                Some(serde_json::json!({
                    "reference_id": reference_id,
                    "uri": reference_uri(&record, reference_id),
                    "created_at": record.get("created_at").cloned().unwrap_or_default(),
                    "expires_at": record.get("expires_at").cloned().unwrap_or_default(),
                    "sha256": record.get("sha256").cloned().unwrap_or_default(),
                    "size_bytes": record.get("size_bytes").cloned().unwrap_or_default(),
                    "status": status,
                    "repo_boundary_enforced": true,
                    "warnings": [],
                }))
            })
            .collect::<Vec<_>>();
        references.sort_by(|left, right| {
            right
                .get("created_at")
                .and_then(serde_json::Value::as_str)
                .cmp(&left.get("created_at").and_then(serde_json::Value::as_str))
        });
        references.truncate(limit);
        Ok(references)
    }

    pub fn reference_is_active(&self, reference_id: &str) -> Result<bool> {
        if !valid_reference_id(reference_id) {
            return Ok(false);
        }
        let Some(record) = self.get_json(&format!("reference:{reference_id}"))? else {
            return Ok(false);
        };
        if reference_is_expired(&record, OffsetDateTime::now_utc()) {
            return Ok(false);
        }
        let Some(body) = record.get("body").and_then(serde_json::Value::as_str) else {
            return Ok(false);
        };
        Ok(record.get("sha256").and_then(serde_json::Value::as_str)
            == Some(sha256_bytes(body.as_bytes()).as_str()))
    }

    pub fn reference_validation(&self, reference_id: &str) -> Result<Option<ReferenceValidation>> {
        if !valid_reference_id(reference_id) {
            return Ok(None);
        }
        let read_txn = self.shared.environment.read_txn()?;
        let Some(encoded) = self
            .shared
            .records
            .get(&read_txn, &format!("reference:{reference_id}"))?
        else {
            return Ok(None);
        };
        let encoded_sha256 = sha256_bytes(encoded);
        let record = decode_json_record(encoded)?;
        let Some(expires_at) = record
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        else {
            return Ok(None);
        };
        if expires_at < OffsetDateTime::now_utc() {
            return Ok(None);
        }
        let Some(body) = record.get("body").and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        if record.get("sha256").and_then(serde_json::Value::as_str)
            != Some(sha256_bytes(body.as_bytes()).as_str())
        {
            return Ok(None);
        }
        Ok(Some(ReferenceValidation {
            reference_id: reference_id.to_owned(),
            encoded_sha256,
            expires_at_unix_seconds: expires_at.unix_timestamp(),
        }))
    }

    pub fn reference_validation_is_active(&self, validation: &ReferenceValidation) -> Result<bool> {
        if validation.expires_at_unix_seconds < OffsetDateTime::now_utc().unix_timestamp() {
            return Ok(false);
        }
        let read_txn = self.shared.environment.read_txn()?;
        let Some(encoded) = self
            .shared
            .records
            .get(&read_txn, &format!("reference:{}", validation.reference_id))?
        else {
            return Ok(false);
        };
        Ok(sha256_bytes(encoded) == validation.encoded_sha256)
    }

    pub fn resolve_reference(
        &self,
        reference_id: &str,
        expected_hash: &str,
    ) -> Result<serde_json::Value> {
        if !valid_reference_id(reference_id) {
            return Ok(serde_json::json!({
                "schema": "mcp_result_reference.resolve.v1",
                "status": "invalid_reference",
            }));
        }
        let Some(record) = self.get_json(&format!("reference:{reference_id}"))? else {
            return Ok(serde_json::json!({
                "schema": "mcp_result_reference.resolve.v1",
                "status": "missing",
                "reference_id": reference_id,
            }));
        };
        if reference_is_expired(&record, OffsetDateTime::now_utc()) {
            return Ok(reference_status(
                reference_id,
                "expired",
                "record_expired",
                record.get("expires_at"),
            ));
        }
        let Some(body) = record.get("body").and_then(serde_json::Value::as_str) else {
            return Ok(reference_status(
                reference_id,
                "stale",
                "payload_unavailable",
                record.get("expires_at"),
            ));
        };
        let digest = sha256_bytes(body.as_bytes());
        if record.get("sha256").and_then(serde_json::Value::as_str) != Some(digest.as_str()) {
            let mut status = reference_status(
                reference_id,
                "stale",
                "stored_hash_mismatch",
                record.get("expires_at"),
            );
            status["actual_sha256"] = serde_json::Value::String(digest);
            return Ok(status);
        }
        if !expected_hash.is_empty() && expected_hash != digest {
            return Ok(serde_json::json!({
                "schema": "mcp_result_reference.resolve.v1",
                "status": "hash_mismatch",
                "reference_id": reference_id,
                "actual_sha256": digest,
            }));
        }
        let envelope: serde_json::Value = serde_json::from_str(body)?;
        if envelope.get("schema").and_then(serde_json::Value::as_str)
            != Some("mcp_result_reference.envelope.v1")
        {
            return Ok(reference_status(
                reference_id,
                "stale",
                "invalid_json_payload",
                record.get("expires_at"),
            ));
        }
        if envelope
            .pointer("/metadata/reference_id")
            .and_then(serde_json::Value::as_str)
            != Some(reference_id)
        {
            return Ok(serde_json::json!({
                "schema": "mcp_result_reference.resolve.v1",
                "status": "invalid_reference",
                "reference_id": reference_id,
            }));
        }
        Ok(serde_json::json!({
            "schema": "mcp_result_reference.resolve.v1",
            "status": "resolved",
            "reference_id": reference_id,
            "uri": reference_uri(&record, reference_id),
            "content": envelope.get("payload").cloned().unwrap_or_default(),
            "content_sha256": digest,
            "metadata": envelope.get("metadata").cloned().unwrap_or_else(|| serde_json::json!({})),
            "summary": envelope.get("summary").cloned().unwrap_or_else(|| serde_json::json!({})),
            "sensitivity": envelope.get("sensitivity").cloned().unwrap_or_else(|| default_sensitivity(false)),
            "repo_boundary_enforced": true,
            "warnings": [],
        }))
    }
}

fn public_reference(
    record: &serde_json::Value,
    envelope: &serde_json::Value,
    ttl_hours: i64,
) -> serde_json::Value {
    let reference_id = record
        .get("reference_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let project_id = record
        .get("project_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let producer = envelope
        .pointer("/metadata/producer_tool")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let uri = format!("repo://project/{project_id}/context/{reference_id}");
    serde_json::json!({
        "schema": "mcp_result_reference.v1",
        "reference_id": reference_id,
        "uri": uri,
        "producer_tool": producer,
        "project_id": project_id,
        "created_at": record.get("created_at").cloned().unwrap_or_default(),
        "expires_at": record.get("expires_at").cloned().unwrap_or_default(),
        "status": "active",
        "summary": envelope.get("summary").cloned().unwrap_or_default(),
        "content": {
            "mime_type": "application/json",
            "encoding": "utf-8",
            "size_bytes": record.get("size_bytes").cloned().unwrap_or_default(),
            "sha256": record.get("sha256").cloned().unwrap_or_default(),
        },
        "retention": {"ttl_hours": ttl_hours.max(1), "policy": "local_generated_state"},
        "sensitivity": envelope.get("sensitivity").cloned().unwrap_or_else(|| default_sensitivity(false)),
        "repo_boundary_enforced": true,
        "resolver": {
            "tool": "result_reference_resolve",
            "uri": uri,
            "repo_boundary_enforced": true,
        },
    })
}

fn valid_reference_id(reference_id: &str) -> bool {
    reference_id.strip_prefix("ctxref-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix.len() <= 64
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    })
}

fn reference_uri(record: &serde_json::Value, reference_id: &str) -> String {
    record
        .get("project_id")
        .and_then(serde_json::Value::as_str)
        .filter(|project_id| !project_id.is_empty())
        .map_or_else(
            || format!("repo://context/{reference_id}"),
            |project_id| format!("repo://project/{project_id}/context/{reference_id}"),
        )
}

fn reference_status(
    reference_id: &str,
    status: &str,
    reason: &str,
    expires_at: Option<&serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "schema": "mcp_result_reference.resolve.v1",
        "status": status,
        "reference_id": reference_id,
        "reason": reason,
        "expires_at": expires_at.cloned().unwrap_or_default(),
    })
}

fn default_sensitivity(payload_embedded: bool) -> serde_json::Value {
    serde_json::json!({
        "redacted": false,
        "redaction_count": 0,
        "categories": [],
        "payload_embedded": payload_embedded,
    })
}

fn sha256_bytes(bytes: &[u8]) -> String {
    digest_hex({
        let mut digest = Sha256::new();
        digest.update(bytes);
        digest
    })
}

fn encode_json_record(value: &serde_json::Value) -> Result<Vec<u8>> {
    let canonical_json = serde_json::to_vec(value)?;
    encode(&StoredJson { canonical_json }).map_err(Into::into)
}

fn decode_json_record(value: &[u8]) -> Result<serde_json::Value> {
    let stored: StoredJson =
        decode(value).map_err(|error| anyhow!("invalid v2 state codec: {error:?}"))?;
    serde_json::from_slice(&stored.canonical_json).map_err(Into::into)
}

fn validate_state_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 512 || key.contains('\0') {
        bail!("invalid state key");
    }
    Ok(())
}

fn validate_wiki_page_id(page_id: &str) -> Result<()> {
    if page_id.is_empty()
        || page_id.len() > 96
        || !page_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("wiki page id must be 1 to 96 alphanumeric, underscore, or hyphen characters");
    }
    Ok(())
}

fn reference_is_expired(value: &serde_json::Value, now: OffsetDateTime) -> bool {
    value
        .get("expires_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|expires_at| OffsetDateTime::parse(expires_at, &Rfc3339).ok())
        .is_some_and(|expires_at| expires_at < now)
}

fn digest_hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::StateStore;

    #[test]
    fn wiki_migration_preserves_shared_precedence_and_renames_collisions() {
        let fixture = tempfile::tempdir().unwrap();
        let shared = StateStore::open(fixture.path().join("shared")).unwrap();
        let legacy_a = StateStore::open(fixture.path().join("a")).unwrap();
        let legacy_b = StateStore::open(fixture.path().join("b")).unwrap();
        let first = legacy_a
            .put_wiki_revision("topic", None, &serde_json::json!({"body":"first"}))
            .unwrap();
        let conflict = legacy_b
            .put_wiki_revision("topic", None, &serde_json::json!({"body":"second"}))
            .unwrap();
        shared
            .migrate_wiki_from(&legacy_a, "project-a", "repo-a")
            .unwrap();
        shared
            .migrate_wiki_from(&legacy_b, "project-b", "repo-a")
            .unwrap();
        assert_eq!(
            shared.get_wiki_revision("topic", &first).unwrap().unwrap()["body"],
            "first"
        );
        let alias = shared
            .get_json("wiki:alias:project-b:topic")
            .unwrap()
            .unwrap();
        let renamed = alias["page_id"].as_str().unwrap();
        assert!(renamed.starts_with("legacy-"));
        assert_eq!(
            shared
                .get_wiki_revision(renamed, &conflict)
                .unwrap()
                .unwrap()["body"],
            "second"
        );
        shared
            .migrate_wiki_from(&legacy_b, "project-b", "repo-a")
            .unwrap();
        assert_eq!(
            shared
                .get_json("wiki:alias:project-b:topic")
                .unwrap()
                .unwrap()["page_id"],
            renamed
        );
    }

    #[test]
    fn wiki_migration_restart_keeps_an_edit_committed_after_partial_import() {
        let fixture = tempfile::tempdir().unwrap();
        let shared = StateStore::open(fixture.path().join("shared")).unwrap();
        let legacy = StateStore::open(fixture.path().join("legacy")).unwrap();
        let original = legacy
            .put_wiki_revision("topic", None, &serde_json::json!({"body":"legacy"}))
            .unwrap();
        let pointer = legacy.get_wiki_page("topic").unwrap().unwrap();
        let revisions = legacy.iter_json("wiki:revision:topic:").unwrap();
        shared
            .import_legacy_wiki_page("project-a", "topic", &pointer, &revisions)
            .unwrap();
        let edited = shared
            .put_wiki_revision(
                "topic",
                Some(&original),
                &serde_json::json!({"body":"shared edit"}),
            )
            .unwrap();

        shared
            .migrate_wiki_from(&legacy, "project-a", "repo-a")
            .unwrap();
        assert_eq!(
            shared.get_wiki_page("topic").unwrap().unwrap()["revision"],
            edited
        );
        assert!(
            shared
                .get_json("wiki:alias:project-a:topic")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn wiki_migration_uses_the_current_markdown_source_id() {
        let fixture = tempfile::tempdir().unwrap();
        let shared = StateStore::open(fixture.path().join("shared")).unwrap();
        let legacy = StateStore::open(fixture.path().join("legacy")).unwrap();
        let owner = "project-a";
        let path = "README.md";
        let old_id = "md-legacy";
        let text = "Shared wiki source text.";
        let hash = sha256_bytes(text.as_bytes());
        let source = serde_json::json!({
            "source_id": old_id,
            "kind": "markdown",
            "path": path,
            "text": text,
            "sha256": hash,
            "freshness": "current"
        });
        legacy
            .put_json_batch(&[
                (format!("wiki:source_revision:{old_id}:{hash}"), source),
                (
                    format!("wiki:source_current:{old_id}"),
                    serde_json::json!({"source_id":old_id,"sha256":hash,"freshness":"current"}),
                ),
            ])
            .unwrap();

        shared.migrate_wiki_from(&legacy, owner, "repo-a").unwrap();

        let expected_id = format!(
            "md-{}-{}",
            &sha256_bytes(owner.as_bytes())[..12],
            &sha256_bytes(format!("{owner}:{path}").as_bytes())[..24]
        );
        let pointer = shared
            .get_json(&format!("wiki:source_current:{expected_id}"))
            .unwrap()
            .unwrap();
        assert_eq!(pointer["source_id"], expected_id);
        assert_eq!(pointer["sha256"], hash);
        assert_eq!(
            shared
                .get_json(&format!("wiki:source_revision:{expected_id}:{hash}"))
                .unwrap()
                .unwrap()["path"],
            path
        );
        assert_eq!(
            shared
                .get_json(&format!("wiki:source_alias:{owner}:{old_id}"))
                .unwrap()
                .unwrap()["source_id"],
            expected_id
        );
    }

    #[test]
    fn simultaneous_wiki_writers_have_exactly_one_winner() {
        let root = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(StateStore::open(root.path()).unwrap());
        let revision = store
            .put_wiki_revision("page", None, &serde_json::json!({"text":"initial"}))
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let winners = std::thread::scope(|scope| {
            let handles = (0..2)
                .map(|index| {
                    let store = std::sync::Arc::clone(&store);
                    let barrier = std::sync::Arc::clone(&barrier);
                    let revision = revision.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        store
                            .put_wiki_revision(
                                "page",
                                Some(&revision),
                                &serde_json::json!({"text":index}),
                            )
                            .is_ok()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>()
        });
        assert_eq!(winners, 1);
        assert_eq!(
            store.get_wiki_revision("page", &revision).unwrap().unwrap()["text"],
            "initial"
        );
    }
    use super::*;
    use serde_json::json;

    #[test]
    fn codec_prefix_is_versioned_and_round_trips() {
        let encoded = encode(&vec!["memory", "reference"]).expect("encode");
        assert_eq!(encoded[0], CODEC_VERSION);
        let decoded: Vec<String> = decode(&encoded).expect("decode");
        assert_eq!(decoded, ["memory", "reference"]);
    }

    #[test]
    fn delete_batch_removes_rows_in_one_bounded_operation() {
        let root = tempfile::tempdir().expect("temporary state root");
        let store = StateStore::open(root.path()).expect("state store");
        store
            .put_json("entry:one", &json!({"value": 1}))
            .expect("row one");
        store
            .put_json("entry:two", &json!({"value": 2}))
            .expect("row two");

        assert_eq!(
            store
                .delete_batch(&["entry:one".to_owned(), "entry:two".to_owned()])
                .expect("delete batch"),
            2
        );
        assert!(
            store
                .iter_json("entry:")
                .expect("remaining rows")
                .is_empty()
        );
    }

    #[test]
    fn put_json_batch_persists_all_rows() {
        let root = tempfile::tempdir().expect("temporary state root");
        let store = StateStore::open(root.path()).expect("state store");
        store
            .put_json_batch(&[
                ("entry:one".to_owned(), json!({"value": 1})),
                ("entry:two".to_owned(), json!({"value": 2})),
            ])
            .expect("write batch");

        assert_eq!(store.iter_json("entry:").expect("rows").len(), 2);
    }

    #[test]
    fn wiki_revisions_are_immutable_and_compare_and_swap_is_atomic() {
        let root = tempfile::tempdir().expect("temporary state root");
        let store = StateStore::open(root.path()).expect("open v2 state");
        let first = json!({"title": "Initial", "claims": ["first"]});
        let first_revision = store
            .put_wiki_revision("architecture", None, &first)
            .expect("create first revision");

        let conflict =
            store.put_wiki_revision("architecture", None, &json!({"title": "lost update"}));
        assert!(conflict.is_err());

        let second = json!({"title": "Updated", "claims": ["second"]});
        let second_revision = store
            .put_wiki_revision("architecture", Some(&first_revision), &second)
            .expect("create second revision");
        assert_ne!(first_revision, second_revision);
        assert_eq!(
            store
                .get_wiki_page("architecture")
                .expect("current page")
                .unwrap()["revision"],
            second_revision
        );
        assert_eq!(
            store
                .get_wiki_revision("architecture", &first_revision)
                .expect("first revision")
                .unwrap(),
            first
        );
        assert_eq!(
            store
                .get_wiki_revision("architecture", &second_revision)
                .expect("second revision")
                .unwrap(),
            second
        );
    }

    #[test]
    fn active_reference_round_trips_and_detects_tampering() {
        let root = tempfile::tempdir().expect("temporary state root");
        let store = StateStore::open(root.path()).expect("open v2 state");
        let created = store
            .create_reference(
                "context_pack",
                "default",
                &json!({"paths": ["src/auth.py"]}),
                &json!({"kind": "deferred_evidence"}),
                24,
            )
            .expect("create reference");
        let reference_id = created["reference_id"].as_str().expect("reference id");
        let resolved = store
            .resolve_reference(reference_id, "")
            .expect("resolve reference");
        assert_eq!(resolved["status"], "resolved");
        assert_eq!(resolved["content"]["paths"][0], "src/auth.py");
        assert_eq!(store.list_references(10).expect("list references").len(), 1);

        let key = format!("reference:{reference_id}");
        let mut record = store
            .get_json(&key)
            .expect("stored record")
            .expect("reference exists");
        record["sha256"] = json!("tampered");
        store.put_json(&key, &record).expect("tamper fixture");
        let stale = store
            .resolve_reference(reference_id, "")
            .expect("tampered resolution");
        assert_eq!(stale["status"], "stale");
        assert_eq!(stale["reason"], "stored_hash_mismatch");
    }
}
