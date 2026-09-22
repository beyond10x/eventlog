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

struct WaitAfterBinding(Arc<tokio::sync::Notify>);
impl Guard for WaitAfterBinding {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            assert_eq!(store.get_blob("a-staged").await?, Some(b"fresh".to_vec()));
            assert_eq!(store.get_blob("z-held").await?, Some(b"AAAA".to_vec()));
            assert_eq!(store.get_blob("foreign-only").await?, None);
            self.0.notify_one();
            std::future::pending::<()>().await;
            unreachable!()
        })
    }
}

async fn cancelled_tentative_binding(
    writer: Arc<dyn AtomicBlobEventStore>,
    observer: &dyn AtomicBlobEventStore,
) {
    let mut attempt = request("cancel-owner", "same-command", "one", b"AAAA");
    observer
        .put_blob(&attempt.group.tenant, "z-held", b"AAAA")
        .await
        .unwrap();
    let foreign = TenantId::new("foreign-owner").unwrap();
    observer
        .put_blob(&foreign, "foreign-only", b"other")
        .await
        .unwrap();
    attempt.blobs.push(BlobWrite {
        digest: "a-staged".into(),
        bytes: b"fresh".to_vec(),
    });
    let entered = Arc::new(tokio::sync::Notify::new());
    let in_task = attempt.clone();
    let signal = entered.clone();
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        task_writer
            .append_group_with_blobs_guarded(&in_task, Arc::new(WaitAfterBinding(signal)))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    // The cancelled guard never published: its binding is absent, shared content survives,
    // and the same request identity remains admissible on an independent handle.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        assert_eq!(
            observer
                .get_blob(&attempt.group.tenant, "a-staged")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            observer
                .get_blob(&attempt.group.tenant, "z-held")
                .await
                .unwrap(),
            Some(b"AAAA".to_vec())
        );
        assert_eq!(
            observer.get_blob(&foreign, "foreign-only").await.unwrap(),
            Some(b"other".to_vec())
        );
        assert!(
            observer
                .read_stream(&attempt.group.appends[0].stream, 0, 10)
                .await
                .unwrap()
                .events
                .is_empty()
        );
        let retried = observer.append_group_with_blobs(&attempt).await.unwrap();
        assert!(!retried.deduplicated);
        assert_eq!(retried.appends[0].events.len(), 1);
    })
    .await
    .unwrap();
}

use eventlog_postgres::PostgresEventStore;
async fn fixture(prefix: &str) -> PostgresEventStore {
    let url =
        std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("required synthetic PostgreSQL fixture");
    let old = PostgresEventStore::connect(&url, prefix).await.unwrap();
    old.drop_tables().await.unwrap();
    old.shutdown().await.unwrap();
    PostgresEventStore::connect(&url, prefix).await.unwrap()
}
#[tokio::test]
async fn postgres_adversary_equal_length_collision_rolls_back() {
    let store = fixture("adversary_collision").await;
    collision_rolls_back(&store).await;
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
#[tokio::test]
async fn postgres_adversary_rebound_retry_preserves_receipt_and_current_binding() {
    let writer = fixture("adversary_rebind").await;
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").unwrap();
    let other = PostgresEventStore::connect(&url, "adversary_rebind")
        .await
        .unwrap();
    rebound_retry(&writer, &other).await;
    other.shutdown().await.unwrap();
    writer.drop_tables().await.unwrap();
    writer.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread")]
async fn postgres_adversary_cancelled_guard_preserves_existing_content_and_receipt() {
    let writer = Arc::new(fixture("adversary_cancel").await);
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").unwrap();
    let observer = PostgresEventStore::connect(&url, "adversary_cancel")
        .await
        .unwrap();
    cancelled_tentative_binding(writer.clone(), &observer).await;
    observer.shutdown().await.unwrap();
    writer.drop_tables().await.unwrap();
    writer.shutdown().await.unwrap();
}
