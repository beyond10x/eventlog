use eventlog_core::{AtomicBlobEventStore, BlobWrite, EventStore};
use eventlog_file::FileEventStore;

#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_blob_content_contract() {
    let root = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(root.path()).await.unwrap();
    eventlog_conformance::run_atomic_blobs(&store).await;
}

struct Refuse;
impl eventlog_core::Guard for Refuse {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            assert!(store.get_blob("fresh").await?.is_some());
            Err(eventlog_core::EventLogError::GuardRefused {
                code: "refused".into(),
            })
        })
    }
}

struct ObstructCleanup(std::path::PathBuf);
impl eventlog_core::Guard for ObstructCleanup {
    fn check<'a>(
        &'a self,
        _: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            let path = std::fs::read_dir(self.0.join("blobs"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            std::fs::rename(&path, self.0.join("retained-staging")).unwrap();
            std::fs::create_dir(path).unwrap();
            Err(eventlog_core::EventLogError::GuardRefused {
                code: "refused".into(),
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_blob_cleanup_failure_reports_retained_unbound_artifact() {
    let root = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(root.path()).await.unwrap();
    let manifest = std::fs::read(root.path().join("manifest.json")).unwrap();
    let request = eventlog_conformance::atomic_blob_request("obstruct", "one", "fresh");
    let error = store
        .append_group_with_blobs_guarded(
            &request,
            std::sync::Arc::new(ObstructCleanup(root.path().into())),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, eventlog_core::EventLogError::Backend(ref message) if message.contains("retained unbound staging"))
    );
    assert_eq!(
        std::fs::read(root.path().join("manifest.json")).unwrap(),
        manifest
    );
    assert_eq!(
        std::fs::read(root.path().join("retained-staging")).unwrap(),
        request.blobs[0].bytes
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_blob_known_abort_cleans_only_owned_staging() {
    let root = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(root.path()).await.unwrap();
    let mut request = eventlog_conformance::atomic_blob_request("reject", "one", "fresh");
    store
        .put_blob(&request.group.tenant, "shared", b"existing")
        .await
        .unwrap();
    let before: Vec<_> = std::fs::read_dir(root.path().join("blobs"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    request.blobs.push(BlobWrite {
        digest: "shared".into(),
        bytes: b"existing".to_vec(),
    });
    assert!(
        store
            .append_group_with_blobs_guarded(&request, std::sync::Arc::new(Refuse))
            .await
            .is_err()
    );
    let after: Vec<_> = std::fs::read_dir(root.path().join("blobs"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        before, after,
        "known abort never removes a prior object and leaves no private staging"
    );
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "shared")
            .await
            .unwrap(),
        Some(b"existing".to_vec())
    );
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "fresh")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_blob_reopen_retry_preserves_erasure_and_receipt() {
    let root = tempfile::tempdir().unwrap();
    let request = eventlog_conformance::atomic_blob_request("reopen", "one", "content");
    let store = FileEventStore::open(root.path()).await.unwrap();
    let original = AtomicBlobEventStore::append_group_with_blobs(&store, &request)
        .await
        .unwrap();
    let journal = std::fs::read_to_string(root.path().join("events.jsonl")).unwrap();
    assert!(
        !journal.contains("retained-payload-marker"),
        "journal carries references and receipt hashes only"
    );
    drop(store);
    let store = FileEventStore::open(root.path()).await.unwrap();
    let retried = AtomicBlobEventStore::append_group_with_blobs(&store, &request)
        .await
        .unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.appends[0].events, original.appends[0].events);
    store
        .delete_blob(&request.group.tenant, "content")
        .await
        .unwrap();
    drop(store);
    let store = FileEventStore::open(root.path()).await.unwrap();
    assert!(
        AtomicBlobEventStore::append_group_with_blobs(&store, &request)
            .await
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        store
            .get_blob(&request.group.tenant, "content")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_blob_independent_writers_keep_only_complete_winner() {
    for mode in ["fresh", "shared", "existing", "collision"] {
        let root = tempfile::tempdir().unwrap();
        let left = FileEventStore::open(root.path()).await.unwrap();
        let right = FileEventStore::open(root.path()).await.unwrap();
        let mut requests = [
            eventlog_conformance::atomic_blob_request("left", "one", "left-content"),
            eventlog_conformance::atomic_blob_request("right", "one", "right-content"),
        ];
        if mode != "fresh" {
            for (index, request) in requests.iter_mut().enumerate() {
                request.blobs.push(BlobWrite {
                    digest: "shared-content".into(),
                    bytes: if mode == "collision" {
                        vec![u8::try_from(index).unwrap()]
                    } else {
                        b"shared".to_vec()
                    },
                });
            }
        }
        if mode == "existing" {
            left.put_blob(&requests[0].group.tenant, "shared-content", b"shared")
                .await
                .unwrap();
        }
        let (a, b) = tokio::join!(
            AtomicBlobEventStore::append_group_with_blobs(&left, &requests[0]),
            AtomicBlobEventStore::append_group_with_blobs(&right, &requests[1])
        );
        // Check physical bindings before any read's ordinary recovery cleanup.
        let binding_count = std::fs::read_dir(root.path().join("blobs"))
            .unwrap()
            .count();
        assert_eq!(
            binding_count,
            if mode == "fresh" { 1 } else { 2 },
            "known loser staging was removed, winner retained"
        );
        eventlog_conformance::assert_atomic_blob_competition(&left, &requests, &[a, b]).await;
    }
}
