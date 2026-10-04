//! Isolated v2 LMDB state, durable imports, references, and telemetry.

use std::{
    fs,
    path::{Path, PathBuf},
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
// minimum encoded-record footprint keeps a single prune transaction bounded
// without rejecting any realistically representable frontier set.
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
    environment: V2Environment,
    records: Database<Str, Bytes>,
}

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
        let mut options = EnvOpenOptions::new();
        options.max_dbs(2).map_size(V2_MAP_SIZE);
        let environment = unsafe { options.open(&lmdb)? };
        let mut write_txn = environment.write_txn()?;
        let records = environment.create_database::<Str, Bytes>(&mut write_txn, Some("records"))?;
        write_txn.commit()?;
        Ok(Self {
            paths: StatePaths {
                overlay,
                lmdb,
                index,
                server_lock,
            },
            environment,
            records,
        })
    }

    pub fn paths(&self) -> &StatePaths {
        &self.paths
    }

    pub fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let read_txn = self.environment.read_txn()?;
        self.records
            .get(&read_txn, key)?
            .map(decode_json_record)
            .transpose()
    }

    pub fn put_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        validate_state_key(key)?;
        let encoded = encode_json_record(value)?;
        let mut write_txn = self.environment.write_txn()?;
        self.records.put(&mut write_txn, key, &encoded)?;
        write_txn.commit()?;
        Ok(())
    }

    pub fn put_json_if_changed(&self, key: &str, value: &serde_json::Value) -> Result<bool> {
        validate_state_key(key)?;
        let encoded = encode_json_record(value)?;
        let read_txn = self.environment.read_txn()?;
        let unchanged = self.records.get(&read_txn, key)? == Some(encoded.as_slice());
        drop(read_txn);
        if unchanged {
            return Ok(false);
        }
        let mut write_txn = self.environment.write_txn()?;
        self.records.put(&mut write_txn, key, &encoded)?;
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
        let mut write_txn = self.environment.write_txn()?;
        for (key, value) in encoded {
            self.records.put(&mut write_txn, key, &value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        validate_state_key(key)?;
        let mut write_txn = self.environment.write_txn()?;
        let deleted = self.records.delete(&mut write_txn, key)?;
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
        let mut write_txn = self.environment.write_txn()?;
        let mut deleted = 0_usize;
        for key in keys {
            if self.records.delete(&mut write_txn, key)? {
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
        let read_txn = self.environment.read_txn()?;
        let mut rows = Vec::new();
        if prefix.is_empty() {
            for row in self.records.iter(&read_txn)? {
                let (key, value) = row?;
                rows.push((key.to_owned(), decode_json_record(value)?));
            }
        } else {
            for row in self.records.prefix_iter(&read_txn, prefix)? {
                let (key, value) = row?;
                rows.push((key.to_owned(), decode_json_record(value)?));
            }
        }
        Ok(rows)
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
        let read_txn = self.environment.read_txn()?;
        let Some(encoded) = self
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
        let read_txn = self.environment.read_txn()?;
        let Some(encoded) = self
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
            .put_json("frontier:one", &json!({"value": 1}))
            .expect("row one");
        store
            .put_json("frontier:two", &json!({"value": 2}))
            .expect("row two");

        assert_eq!(
            store
                .delete_batch(&["frontier:one".to_owned(), "frontier:two".to_owned()])
                .expect("delete batch"),
            2
        );
        assert!(
            store
                .iter_json("frontier:")
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
                ("frontier:one".to_owned(), json!({"value": 1})),
                ("frontier:two".to_owned(), json!({"value": 2})),
            ])
            .expect("write batch");

        assert_eq!(store.iter_json("frontier:").expect("rows").len(), 2);
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
