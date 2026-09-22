//! Actual-content, callback, rollback and retry expectations shared by every provider.
use crate::meta;
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, BlobAppendGroup, BlobWrite, BoxFuture, EventLogError,
    Expected, Guard, NewEvent, ProjectionSpec, ProjectionStore, Projector, RecordedEvent,
    StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const VIEW: ProjectionSpec = ProjectionSpec {
    name: "atomic_blob_probe",
    indexed: &[],
};
const CONTENT: &[u8] = b"retained-payload-marker";

struct Probe(Arc<AtomicUsize>);
impl Projector for Probe {
    fn name(&self) -> &'static str {
        VIEW.name
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        &[VIEW]
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            let digest = event.data["digest"].as_str().unwrap();
            assert_eq!(
                store.get_blob(digest).await?,
                Some(CONTENT.to_vec()),
                "inline projection reads its tenant's tentative blob"
            );
            store
                .upsert(
                    &VIEW,
                    &event.tenant,
                    &event.stream_id,
                    &json!({"version":event.version}),
                )
                .await?;
            if event.name == "content.refused" {
                return Err(EventLogError::GuardRefused {
                    code: "projector_refused".into(),
                });
            }
            Ok(())
        })
    }
}

struct Admission {
    tenant: TenantId,
    digest: String,
    calls: Arc<AtomicUsize>,
    refuse: bool,
}
impl Guard for Admission {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                store.get_blob(&self.digest).await?,
                Some(CONTENT.to_vec()),
                "guard reads its tenant's tentative blob"
            );
            let old = store
                .get_for_update(&VIEW, &self.tenant, "admissions")
                .await?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            store
                .upsert(&VIEW, &self.tenant, "admissions", &json!(old + 1))
                .await?;
            if self.refuse {
                return Err(EventLogError::GuardRefused {
                    code: "admission_refused".into(),
                });
            }
            Ok(())
        })
    }
}

/// A stable request fixture also used by provider reopen and concurrency probes.
/// # Panics
/// Only if the crate's own synthetic fixture stops satisfying envelope validation.
#[must_use]
pub fn atomic_blob_request(key: &str, stream: &str, digest: &str) -> BlobAppendGroup {
    let tenant = TenantId::new("atomic-blob-owner").unwrap();
    BlobAppendGroup {
        group: AppendGroup {
            tenant: tenant.clone(),
            meta: meta(key, &json!({})),
            appends: vec![StreamAppend {
                stream: StreamId::new(tenant, "item", stream).unwrap(),
                expected: Expected::NoStream,
                events: vec![
                    NewEvent::new("content.created", 1, json!({"digest":digest})).unwrap(),
                ],
            }],
        },
        blobs: vec![BlobWrite {
            digest: digest.into(),
            bytes: CONTENT.to_vec(),
        }],
    }
}

/// # Panics
/// Requires actual-content identity and all-or-nothing publication, including callbacks.
pub async fn run_atomic_blobs(store: &dyn AtomicBlobEventStore) {
    let applied = Arc::new(AtomicUsize::new(0));
    store
        .register_inline(Arc::new(Probe(applied.clone())))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut request = atomic_blob_request("first", "z", "content");
    let tenant = request.group.tenant.clone();
    let foreign = TenantId::new("foreign-blob-owner").unwrap();
    store
        .put_blob(&foreign, "content", b"foreign-content")
        .await
        .unwrap();
    store
        .put_blob(&tenant, "reused", b"existing-content")
        .await
        .unwrap();
    request.blobs.extend([
        BlobWrite {
            digest: "reused".into(),
            bytes: b"existing-content".to_vec(),
        },
        BlobWrite {
            digest: "empty-content".into(),
            bytes: Vec::new(),
        },
    ]);
    request.group.appends.push(
        atomic_blob_request("unused", "a", "content")
            .group
            .appends
            .remove(0),
    );
    let mut repeated = request.group.appends[0].clone();
    repeated.expected = Expected::Exact(1);
    request.group.appends.push(repeated);
    let admission = |digest: &str, refuse| {
        Arc::new(Admission {
            tenant: tenant.clone(),
            digest: digest.into(),
            calls: calls.clone(),
            refuse,
        }) as Arc<dyn Guard>
    };
    let original = store
        .append_group_with_blobs_guarded(&request, admission("content", false))
        .await
        .unwrap();
    assert!(!original.deduplicated);
    assert_eq!(
        original
            .appends
            .iter()
            .map(|entry| (entry.first_version, entry.last_version))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 1), (2, 2)]
    );
    assert_eq!(
        store.get_blob(&tenant, "content").await.unwrap(),
        Some(CONTENT.to_vec())
    );
    assert_eq!(
        store.get_blob(&tenant, "empty-content").await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        store.get_blob(&foreign, "content").await.unwrap(),
        Some(b"foreign-content".to_vec())
    );
    let mut reversed = request.clone();
    reversed.blobs.reverse();
    let retry = store
        .append_group_with_blobs_guarded(&reversed, admission("content", true))
        .await
        .unwrap();
    assert!(retry.deduplicated);
    for (old, same) in original.appends.iter().zip(&retry.appends) {
        assert_eq!(old.events, same.events);
        assert!(same.deduplicated);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(applied.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .projection_get(&VIEW, &tenant, "admissions")
            .await
            .unwrap(),
        Some(json!(1))
    );

    let mut changed = request.clone();
    changed.blobs[0].bytes.push(1);
    assert!(matches!(
        store.append_group_with_blobs(&changed).await,
        Err(EventLogError::IdempotencyMismatch { .. })
    ));
    assert!(matches!(
        store.append_group(&request.group).await,
        Err(EventLogError::IdempotencyMismatch { .. })
    ));
    let mut legacy = atomic_blob_request("legacy", "legacy", "content");
    store.append_group(&legacy.group).await.unwrap();
    legacy.blobs[0].digest = "legacy-extra".into();
    assert!(matches!(
        store.append_group_with_blobs(&legacy).await,
        Err(EventLogError::IdempotencyMismatch { .. })
    ));
    assert_eq!(store.get_blob(&tenant, "legacy-extra").await.unwrap(), None);

    let mut collision = atomic_blob_request("collision", "collision", "aborting-fresh");
    collision.blobs.push(BlobWrite {
        digest: "content".into(),
        bytes: b"different-content".to_vec(),
    });
    assert_eq!(
        store.append_group_with_blobs(&collision).await.unwrap_err(),
        EventLogError::Invalid("blob digest already names different content".into())
    );
    assert_eq!(
        store.get_blob(&tenant, "aborting-fresh").await.unwrap(),
        None
    );
    assert_eq!(
        store.get_blob(&tenant, "content").await.unwrap(),
        Some(CONTENT.to_vec())
    );
    assert_eq!(
        store
            .stream_version(&collision.group.appends[0].stream)
            .await
            .unwrap(),
        None
    );
    collision.blobs.pop();
    assert!(
        !store
            .append_group_with_blobs(&collision)
            .await
            .unwrap()
            .deduplicated,
        "collision did not consume receipt identity"
    );

    for failure in ["guard", "projector", "later-stream"] {
        let mut rejected = atomic_blob_request(failure, failure, failure);
        if failure == "projector" {
            rejected.group.appends[0].events[0].name = "content.refused".into();
        } else if failure == "later-stream" {
            let mut stale = request.group.appends[0].clone();
            stale.expected = Expected::Exact(1);
            rejected.group.appends.push(stale);
        }
        let error = store
            .append_group_with_blobs_guarded(&rejected, admission(failure, failure == "guard"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            EventLogError::GuardRefused { .. } | EventLogError::Conflict { .. }
        ));
        assert_eq!(
            store.get_blob(&tenant, failure).await.unwrap(),
            None,
            "{failure}: losing content is not published"
        );
        assert_eq!(
            store
                .stream_version(&rejected.group.appends[0].stream)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store.projection_get(&VIEW, &tenant, failure).await.unwrap(),
            None
        );
        assert_eq!(
            store
                .projection_get(&VIEW, &tenant, "admissions")
                .await
                .unwrap(),
            Some(json!(1))
        );
        rejected.group.appends.truncate(1);
        rejected.group.appends[0].events[0].name = "content.created".into();
        assert!(
            !store
                .append_group_with_blobs(&rejected)
                .await
                .unwrap()
                .deduplicated,
            "aborted identity remains available"
        );
    }

    for invalid in [
        "empty-blobs",
        "duplicate",
        "empty-group",
        "cross-tenant",
        "bad-key",
        "bad-event",
    ] {
        let mut rejected = atomic_blob_request(invalid, invalid, invalid);
        match invalid {
            "empty-blobs" => rejected.blobs.clear(),
            "duplicate" => rejected.blobs.push(rejected.blobs[0].clone()),
            "empty-group" => rejected.group.appends.clear(),
            "cross-tenant" => {
                rejected.group.appends[0].stream =
                    StreamId::new(foreign.clone(), "item", "one").unwrap();
            }
            "bad-key" => rejected.blobs[0].digest.clear(),
            "bad-event" => rejected.group.appends[0].events[0].name.clear(),
            _ => unreachable!(),
        }
        assert!(
            matches!(
                store.append_group_with_blobs(&rejected).await,
                Err(EventLogError::Invalid(_))
            ),
            "{invalid}"
        );
        assert_eq!(store.get_blob(&tenant, invalid).await.unwrap(), None);
        let corrected = atomic_blob_request(invalid, invalid, invalid);
        assert!(
            !store
                .append_group_with_blobs(&corrected)
                .await
                .unwrap()
                .deduplicated
        );
    }

    let mut other = atomic_blob_request("first", "other", "other-content");
    other.group.tenant = foreign.clone();
    other.group.appends[0].stream = StreamId::new(foreign.clone(), "item", "other").unwrap();
    assert!(
        !store
            .append_group_with_blobs(&other)
            .await
            .unwrap()
            .deduplicated,
        "group identity and blob bindings are tenant scoped"
    );
    assert_eq!(
        store.get_blob(&tenant, "other-content").await.unwrap(),
        None
    );
    assert_eq!(
        store.get_blob(&foreign, "content").await.unwrap(),
        Some(b"foreign-content".to_vec())
    );

    let redacted = store
        .redact(&request.group.appends[0].stream, 1, "erased")
        .await
        .unwrap();
    store.delete_blob(&tenant, "content").await.unwrap();
    let before = (calls.load(Ordering::SeqCst), applied.load(Ordering::SeqCst));
    let retry = store
        .append_group_with_blobs_guarded(&request, admission("content", true))
        .await
        .unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.appends[0].events, vec![redacted]);
    assert_eq!(
        store.get_blob(&tenant, "content").await.unwrap(),
        None,
        "receipt retry must never resurrect erased bytes"
    );
    assert_eq!(
        before,
        (calls.load(Ordering::SeqCst), applied.load(Ordering::SeqCst))
    );
    assert_eq!(
        store.get_blob(&tenant, "reused").await.unwrap(),
        Some(b"existing-content".to_vec())
    );
}

/// Inspect the complete winner and absence of the loser using public read ports.
/// # Panics
/// Asserts one committed lineage and no losing blob binding or consumed receipt.
pub async fn assert_atomic_blob_competition(
    store: &dyn AtomicBlobEventStore,
    requests: &[BlobAppendGroup; 2],
    results: &[Result<eventlog_core::AppendGroupResult, EventLogError>; 2],
) {
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let winner = usize::from(results[1].is_ok());
    let loser = 1 - winner;
    let winning = &requests[winner];
    let losing = &requests[loser];
    let first = results[winner].as_ref().unwrap();
    assert!(!first.deduplicated);
    assert!(matches!(
        results[loser],
        Err(EventLogError::Conflict { .. } | EventLogError::Invalid(_))
    ));
    for blob in &winning.blobs {
        assert_eq!(
            store
                .get_blob(&winning.group.tenant, &blob.digest)
                .await
                .unwrap(),
            Some(blob.bytes.clone())
        );
    }
    assert_eq!(
        store
            .get_blob(&losing.group.tenant, &losing.blobs[0].digest)
            .await
            .unwrap(),
        None
    );
    let stream = &winning.group.appends[0].stream;
    assert_eq!(
        store.read_stream(stream, 0, 100).await.unwrap().events,
        first.appends[0].events
    );
    let retry = store.append_group_with_blobs(winning).await.unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.appends[0].events, first.appends[0].events);
    let mut corrected = losing.clone();
    corrected.blobs.truncate(1);
    corrected.group.appends[0].stream =
        StreamId::new(losing.group.tenant.clone(), "item", "corrected-loser").unwrap();
    assert!(
        !store
            .append_group_with_blobs(&corrected)
            .await
            .unwrap()
            .deduplicated
    );
}
