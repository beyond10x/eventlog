use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, BlobAppendGroup, BlobWrite, BoxFuture, EventLogError,
    Expected, Guard, NewEvent, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::sync::Arc;

fn request(tenant: &str, key: &str, stream: &str, bytes: &[u8]) -> BlobAppendGroup {
    let tenant = TenantId::new(tenant).unwrap();
    BlobAppendGroup {
        group: AppendGroup {
            tenant: tenant.clone(),
            meta: eventlog_conformance::meta(key, &json!({"constant_caller_hash": true})),
            appends: vec![StreamAppend {
                stream: StreamId::new(tenant, "item", stream).unwrap(),
                expected: Expected::NoStream,
                events: vec![
                    NewEvent::new("content.created", 1, json!({"digest": "z-held"})).unwrap(),
                ],
            }],
        },
        blobs: vec![BlobWrite {
            digest: "z-held".into(),
            bytes: bytes.to_vec(),
        }],
    }
}

struct MustNotCall;
impl Guard for MustNotCall {
    fn check<'a>(
        &'a self,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { panic!("receipt resolution must precede callbacks and binding checks") })
    }
}

async fn collision_rolls_back(store: &dyn AtomicBlobEventStore) {
    let mut attempt = request("collision-owner", "same-command", "one", b"BBBB");
    store
        .put_blob(&attempt.group.tenant, "z-held", b"AAAA")
        .await
        .unwrap();
    attempt.blobs.push(BlobWrite {
        digest: "a-staged".into(),
        bytes: b"fresh".to_vec(),
    });
    assert!(matches!(
        store.append_group_with_blobs(&attempt).await,
        Err(EventLogError::Invalid(_))
    ));
    assert_eq!(
        store
            .get_blob(&attempt.group.tenant, "a-staged")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .get_blob(&attempt.group.tenant, "z-held")
            .await
            .unwrap(),
        Some(b"AAAA".to_vec())
    );
    assert!(
        store
            .read_stream(&attempt.group.appends[0].stream, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );

    // Equal length is not equal content, and the refused attempt did not consume its receipt.
    attempt.blobs[0].bytes = b"AAAA".to_vec();
    let admitted = store.append_group_with_blobs(&attempt).await.unwrap();
    assert!(!admitted.deduplicated);
    assert_eq!(admitted.appends[0].events.len(), 1);
    assert_eq!(
        store
            .get_blob(&attempt.group.tenant, "a-staged")
            .await
            .unwrap(),
        Some(b"fresh".to_vec())
    );
}

async fn rebound_retry(writer: &dyn AtomicBlobEventStore, other: &dyn AtomicBlobEventStore) {
    let original_request = request("retry-owner", "same-command", "one", b"AAAA");
    let original = writer
        .append_group_with_blobs(&original_request)
        .await
        .unwrap();
    other
        .delete_blob(&original_request.group.tenant, "z-held")
        .await
        .unwrap();
    other
        .put_blob(&original_request.group.tenant, "z-held", b"BBBB")
        .await
        .unwrap();

    let retried = other
        .append_group_with_blobs_guarded(&original_request, Arc::new(MustNotCall))
        .await
        .unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.appends[0].events, original.appends[0].events);
    assert_eq!(
        retried.appends[0].first_version,
        original.appends[0].first_version
    );
    assert_eq!(
        retried.appends[0].last_version,
        original.appends[0].last_version
    );
    assert_eq!(
        writer
            .get_blob(&original_request.group.tenant, "z-held")
            .await
            .unwrap(),
        Some(b"BBBB".to_vec())
    );

    let mut changed = original_request.clone();
    changed.blobs[0].bytes = b"BBBB".to_vec();
    assert_eq!(
        changed.group.meta.request_hash,
        original_request.group.meta.request_hash
    );
    assert!(matches!(
        other.append_group_with_blobs(&changed).await,
        Err(EventLogError::IdempotencyMismatch { .. })
    ));

    // A fresh command checks the current binding even though an old receipt names older content.
    let fresh = request("retry-owner", "fresh-command", "two", b"AAAA");
    assert!(matches!(
        other.append_group_with_blobs(&fresh).await,
        Err(EventLogError::Invalid(_))
    ));
    assert!(
        other
            .read_stream(&fresh.group.appends[0].stream, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert_eq!(
        writer
            .get_blob(&original_request.group.tenant, "z-held")
            .await
            .unwrap(),
        Some(b"BBBB".to_vec())
    );

    // Neither the reused receipt identity nor the rebound key crosses the tenant boundary.
    let foreign = request("other-owner", "same-command", "one", b"CCCC");
    let foreign_result = other.append_group_with_blobs(&foreign).await.unwrap();
    assert!(!foreign_result.deduplicated);
    assert_ne!(
        foreign_result.appends[0].events[0].event_id,
        original.appends[0].events[0].event_id
    );
    assert_eq!(
        writer
            .get_blob(&foreign.group.tenant, "z-held")
            .await
            .unwrap(),
        Some(b"CCCC".to_vec())
    );
    assert_eq!(
        writer
            .get_blob(&original_request.group.tenant, "z-held")
            .await
            .unwrap(),
        Some(b"BBBB".to_vec())
    );
}

use eventlog_sqlite::SqliteEventStore;
#[tokio::test]
async fn sqlite_adversary_equal_length_collision_rolls_back() {
    let store = SqliteEventStore::in_memory("adversary_collision")
        .await
        .unwrap();
    collision_rolls_back(&store).await;
}
#[tokio::test]
async fn sqlite_adversary_rebound_retry_preserves_receipt_and_current_binding() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("adversary.sqlite");
    let writer = SqliteEventStore::open(path.to_str().unwrap(), "adversary_rebind")
        .await
        .unwrap();
    let other = SqliteEventStore::open(path.to_str().unwrap(), "adversary_rebind")
        .await
        .unwrap();
    rebound_retry(&writer, &other).await;
}
