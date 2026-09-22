//! Opt-in atomic publication of retained content and ordered event appends.
use crate::{
    AppendGroup, AppendGroupResult, AtomicEventStore, BoxFuture, EventLogError, Guard, NoGuard,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

/// One tenant-confined blob binding; the owner's digest is an opaque address.
#[derive(Clone, Debug)]
pub struct BlobWrite {
    pub digest: String,
    pub bytes: Vec<u8>,
}

/// The contained group supplies the tenant and the shared group receipt identity.
#[derive(Clone, Debug)]
pub struct BlobAppendGroup {
    pub group: AppendGroup,
    pub blobs: Vec<BlobWrite>,
}

impl BlobAppendGroup {
    /// # Errors
    /// Refuses invalid groups, invalid or duplicate blob keys, and empty content sets.
    pub fn fingerprint(&self) -> Result<String, EventLogError> {
        let group_fingerprint = self.group.fingerprint()?;
        if self.blobs.is_empty() {
            return Err(EventLogError::Invalid(
                "an atomic blob group must contain blobs".into(),
            ));
        }
        let mut ordered = BTreeMap::new();
        for blob in &self.blobs {
            crate::validate_field("blob digest", &blob.digest)?;
            if ordered.insert(&blob.digest, &blob.bytes).is_some() {
                return Err(EventLogError::Invalid(
                    "duplicate blob digest in atomic group".into(),
                ));
            }
        }
        let blobs = ordered.into_iter().map(|(digest, bytes)| {
            let byte_count = u64::try_from(bytes.len()).map_err(|_| EventLogError::Invalid("blob length exceeds u64".into()))?;
            Ok(json!({"digest":digest,"byte_count":byte_count,"content_sha256":format!("{:x}", Sha256::digest(bytes))}))
        }).collect::<Result<Vec<_>, EventLogError>>()?;
        crate::request_hash(
            &json!({"format":"eventlog-blob-append-group/1","group_fingerprint":group_fingerprint,"blobs":blobs}),
        )
    }
}

/// A provider must publish bindings, events, callbacks and receipt in one native transaction.
pub trait AtomicBlobEventStore: AtomicEventStore {
    fn append_group_with_blobs<'a>(
        &'a self,
        request: &'a BlobAppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        self.append_group_with_blobs_guarded(request, Arc::new(NoGuard))
    }

    /// Receipt retries run no callback and never restore content erased after success.
    fn append_group_with_blobs_guarded<'a>(
        &'a self,
        request: &'a BlobAppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandMeta, Expected, NewEvent, StreamAppend, StreamId, TenantId};
    use serde_json::json;

    fn request() -> BlobAppendGroup {
        let tenant = TenantId::new("fixture-tenant").unwrap();
        BlobAppendGroup {
            group: AppendGroup {
                tenant: tenant.clone(),
                meta: CommandMeta {
                    idempotency_key: "fixture-key".into(),
                    request_hash: "caller-hash".into(),
                    subject: "subject-1".into(),
                    actor: "actor-1".into(),
                    request_id: "request-1".into(),
                    trace_id: "trace-1".into(),
                    causation_id: None,
                    causation_depth: 0,
                    occurred_at: time::OffsetDateTime::UNIX_EPOCH,
                    claim: None,
                },
                appends: vec![StreamAppend {
                    stream: StreamId::new(tenant, "item", "one").unwrap(),
                    expected: Expected::NoStream,
                    events: vec![NewEvent::new("item.created", 1, json!({"value":1})).unwrap()],
                }],
            },
            blobs: vec![BlobWrite {
                digest: "content-key".into(),
                bytes: b"retained-content".to_vec(),
            }],
        }
    }

    #[test]
    fn atomic_blob_fingerprint_binds_actual_bytes_not_caller_hashes() {
        let mut changed = request();
        let original = changed.fingerprint().unwrap();
        changed.blobs[0].bytes = b"modified-content".to_vec();
        assert_eq!(
            changed.blobs[0].bytes.len(),
            request().blobs[0].bytes.len(),
            "equal length cannot stand in for equal content"
        );
        assert_ne!(original, changed.fingerprint().unwrap());
    }

    #[test]
    fn atomic_blob_fingerprint_freezes_legacy_and_explicit_format() {
        let request = request();
        let legacy = request.group.fingerprint().unwrap();
        assert_eq!(
            legacy,
            "a4e85afbb7dd0e722b4b14e49e965b3185a0dadbf517638289edae81ba0a84ea"
        );
        let fingerprint = request.fingerprint().unwrap();
        assert_ne!(
            fingerprint, legacy,
            "old and new calls cannot share a receipt"
        );
        let expected = json!({
            "format":"eventlog-blob-append-group/1", "group_fingerprint":legacy,
            "blobs":[{"digest":"content-key", "byte_count":16,
                "content_sha256":format!("{:x}", Sha256::digest(b"retained-content"))}]
        });
        assert_eq!(fingerprint, crate::request_hash(&expected).unwrap());
        let root = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
        let spec = std::fs::read_to_string(
            root.join("../../ess/atomic-content/domains/atomic-content.yaml"),
        )
        .unwrap();
        let declared: Vec<_> = spec
            .split("name: eventlog.atomic_content.BlobFingerprintEntry")
            .nth(1)
            .unwrap()
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- name: "))
            .collect();
        let actual: Vec<_> = expected["blobs"][0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut declared = declared;
        declared.sort_unstable();
        assert_eq!(
            declared, actual,
            "ESS fingerprint fields agree with actual preimage"
        );
    }

    #[test]
    fn atomic_blob_fingerprint_sorts_keys_and_refuses_empty_duplicate_or_invalid_input() {
        let mut first = request();
        first.blobs.push(BlobWrite {
            digest: "another-key".into(),
            bytes: Vec::new(),
        });
        let mut reversed = first.clone();
        reversed.blobs.reverse();
        assert_eq!(
            first.fingerprint().unwrap(),
            reversed.fingerprint().unwrap()
        );
        first.blobs.push(first.blobs[0].clone());
        assert!(matches!(
            first.fingerprint(),
            Err(EventLogError::Invalid(_))
        ));
        first = request();
        first.blobs.clear();
        assert!(matches!(
            first.fingerprint(),
            Err(EventLogError::Invalid(_))
        ));
        for key in ["", "invalid\nkey"] {
            first = request();
            first.blobs[0].digest = key.into();
            assert!(matches!(
                first.fingerprint(),
                Err(EventLogError::Invalid(_))
            ));
        }
    }
}
