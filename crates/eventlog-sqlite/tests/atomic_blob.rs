use eventlog_core::{AtomicBlobEventStore, BlobWrite, EventStore};
use eventlog_sqlite::SqliteEventStore;

#[tokio::test]
async fn sqlite_atomic_blob_content_contract() {
    let store = SqliteEventStore::in_memory("atomic_blobs").await.unwrap();
    eventlog_conformance::run_atomic_blobs(&store).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reopen_retry_preserves_erasure_and_receipt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("atomic.sqlite");
    let request = eventlog_conformance::atomic_blob_request("reopen", "one", "content");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "atomic_reopen")
        .await
        .unwrap();
    let original = store.append_group_with_blobs(&request).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    let receipt: String = raw
        .query_row(
            "SELECT request_hash || ranges FROM atomic_reopen_append_groups",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!receipt.contains("retained-payload-marker"));
    drop(raw);
    drop(store);
    let store = SqliteEventStore::open(path.to_str().unwrap(), "atomic_reopen")
        .await
        .unwrap();
    let retried = store.append_group_with_blobs(&request).await.unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.appends[0].events, original.appends[0].events);
    store
        .delete_blob(&request.group.tenant, "content")
        .await
        .unwrap();
    drop(store);
    let store = SqliteEventStore::open(path.to_str().unwrap(), "atomic_reopen")
        .await
        .unwrap();
    assert!(
        store
            .append_group_with_blobs(&request)
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
async fn sqlite_atomic_blob_independent_writers_keep_only_complete_winner() {
    for mode in ["fresh", "shared", "existing", "collision"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("atomic.sqlite");
        let left = SqliteEventStore::open(path.to_str().unwrap(), "atomic_race")
            .await
            .unwrap();
        let right = SqliteEventStore::open(path.to_str().unwrap(), "atomic_race")
            .await
            .unwrap();
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
            left.append_group_with_blobs(&requests[0]),
            right.append_group_with_blobs(&requests[1])
        );
        let raw = rusqlite::Connection::open(&path).unwrap();
        let count: i64 = raw
            .query_row("SELECT COUNT(*) FROM atomic_race_blobs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, if mode == "fresh" { 1 } else { 2 });
        eventlog_conformance::assert_atomic_blob_competition(&left, &requests, &[a, b]).await;
    }
}
