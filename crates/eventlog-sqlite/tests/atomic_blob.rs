use eventlog_core::{
    AtomicBlobEventStore, BlobAppendGroup, BlobWrite, BoxFuture, EventLogError, EventStore, Guard,
    ProjectionStore,
};
use eventlog_sqlite::SqliteEventStore;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

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

const INTEGRITY_PREFIX: &str = "atomic_integrity";
const EXISTING_DIGEST: &str = "z-existing";
const NEW_DIGEST: &str = "a-tentative";

fn integrity_request() -> BlobAppendGroup {
    let mut request =
        eventlog_conformance::atomic_blob_request("integrity", "one", EXISTING_DIGEST);
    // The provider sorts bindings: this new binding is installed before the invalid existing one.
    request.blobs.push(BlobWrite {
        digest: NEW_DIGEST.into(),
        bytes: b"tentative-content".to_vec(),
    });
    request
}

type BlobRow = (String, Vec<u8>, i64, Option<String>, i64);

#[derive(Debug, PartialEq)]
struct IntegritySnapshot {
    events: i64,
    commands: i64,
    receipts: i64,
    blobs: Vec<BlobRow>,
}

fn integrity_snapshot(path: &Path) -> IntegritySnapshot {
    let raw = rusqlite::Connection::open(path).unwrap();
    let count = |table| {
        raw.query_row(
            &format!("SELECT COUNT(*) FROM atomic_integrity_{table}"),
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let mut query = raw
        .prepare("SELECT digest,bytes,byte_count,integrity_sha256,integrity_v1 FROM atomic_integrity_blobs ORDER BY digest")
        .unwrap();
    let blobs = query
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    IntegritySnapshot {
        events: count("events"),
        commands: count("commands"),
        receipts: count("append_groups"),
        blobs,
    }
}

#[derive(Clone, Copy, Debug)]
enum Corruption {
    HashMismatch,
    MalformedHash,
    MissingHash,
    LengthMismatch,
    NegativeLength,
    Edition,
}

fn corrupt_binding(path: &Path, corruption: Corruption) {
    let raw = rusqlite::Connection::open(path).unwrap();
    // Model on-disk corruption while retaining the exact admitted schema. This is a test-only
    // connection; production connections keep their constraints and bytes are never rewritten.
    raw.execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    let assignment = match corruption {
        Corruption::HashMismatch => {
            "integrity_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
        }
        Corruption::MalformedHash => "integrity_sha256='BAD'",
        Corruption::MissingHash => "integrity_sha256=NULL",
        Corruption::LengthMismatch => "byte_count=byte_count+1",
        Corruption::NegativeLength => "byte_count=-1",
        Corruption::Edition => "integrity_v1=2",
    };
    assert_eq!(
        raw.execute(
            &format!("UPDATE atomic_integrity_blobs SET {assignment} WHERE digest=?1"),
            [EXISTING_DIGEST],
        )
        .unwrap(),
        1
    );
    raw.execute_batch("PRAGMA ignore_check_constraints = OFF")
        .unwrap();
}

struct NonReadingGuard(Arc<AtomicUsize>);

impl Guard for NonReadingGuard {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

async fn corrupt_reuse_refuses(corruption: Corruption) {
    let mut failures = Vec::new();
    for guarded in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("integrity.sqlite");
        let request = integrity_request();
        let store = SqliteEventStore::open(path.to_str().unwrap(), INTEGRITY_PREFIX)
            .await
            .unwrap();
        store
            .put_blob(
                &request.group.tenant,
                EXISTING_DIGEST,
                &request.blobs[0].bytes,
            )
            .await
            .unwrap();
        drop(store);
        corrupt_binding(&path, corruption);
        let store = SqliteEventStore::open(path.to_str().unwrap(), INTEGRITY_PREFIX)
            .await
            .unwrap();
        assert!(matches!(
            store.get_blob(&request.group.tenant, EXISTING_DIGEST).await,
            Err(EventLogError::Backend(message)) if message == "stored blob integrity metadata is invalid"
        ));
        let before = integrity_snapshot(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let result = if guarded {
            AtomicBlobEventStore::append_group_with_blobs_guarded(
                &store,
                &request,
                Arc::new(NonReadingGuard(calls.clone())),
            )
            .await
        } else {
            AtomicBlobEventStore::append_group_with_blobs(&store, &request).await
        };
        let after = integrity_snapshot(&path);
        let events = store
            .read_stream(&request.group.appends[0].stream, 0, 100)
            .await
            .unwrap()
            .events;
        let refusal = matches!(&result, Err(EventLogError::Backend(message)) if message == "stored blob integrity metadata is invalid");
        if !refusal || before != after || !events.is_empty() || calls.load(Ordering::SeqCst) != 0 {
            failures.push(format!(
                "{corruption:?} guarded={guarded}: result={result:?}, before={before:?}, after={after:?}, events={}, guard_calls={}",
                events.len(), calls.load(Ordering::SeqCst)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_hash_mismatch_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::HashMismatch).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_malformed_hash_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::MalformedHash).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_missing_hash_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::MissingHash).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_length_mismatch_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::LengthMismatch).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_negative_length_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::NegativeLength).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_reuse_refuses_unknown_edition_and_rolls_back() {
    corrupt_reuse_refuses(Corruption::Edition).await;
}

#[tokio::test]
async fn sqlite_atomic_blob_integrity_preserves_healthy_reuse_and_byte_conflicts() {
    for mode in ["fresh", "reuse", "different"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("integrity.sqlite");
        let store = SqliteEventStore::open(path.to_str().unwrap(), INTEGRITY_PREFIX)
            .await
            .unwrap();
        let request = integrity_request();
        if mode != "fresh" {
            let mut bytes = request.blobs[0].bytes.clone();
            if mode == "different" {
                bytes[0] ^= 1;
            }
            store
                .put_blob(&request.group.tenant, EXISTING_DIGEST, &bytes)
                .await
                .unwrap();
        }
        let before = integrity_snapshot(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let result = AtomicBlobEventStore::append_group_with_blobs_guarded(
            &store,
            &request,
            Arc::new(NonReadingGuard(calls.clone())),
        )
        .await;
        let after = integrity_snapshot(&path);
        if mode == "different" {
            assert!(
                matches!(result, Err(EventLogError::Invalid(message)) if message == "blob digest already names different content")
            );
            assert_eq!(before, after);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            assert!(!result.unwrap().deduplicated);
            assert_eq!(after.events, 1);
            assert_eq!(after.receipts, 1);
            assert_eq!(after.blobs.len(), 2);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            for blob in &request.blobs {
                assert_eq!(
                    store
                        .get_blob(&request.group.tenant, &blob.digest)
                        .await
                        .unwrap(),
                    Some(blob.bytes.clone())
                );
            }
        }
    }
}

#[tokio::test]
async fn sqlite_atomic_blob_receipt_retry_precedes_integrity_and_never_restores_erasure() {
    for corruption in [
        Corruption::HashMismatch,
        Corruption::LengthMismatch,
        Corruption::Edition,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("integrity.sqlite");
        let request = integrity_request();
        let store = SqliteEventStore::open(path.to_str().unwrap(), INTEGRITY_PREFIX)
            .await
            .unwrap();
        let original = AtomicBlobEventStore::append_group_with_blobs(&store, &request)
            .await
            .unwrap();
        drop(store);
        corrupt_binding(&path, corruption);
        let store = SqliteEventStore::open(path.to_str().unwrap(), INTEGRITY_PREFIX)
            .await
            .unwrap();
        assert!(
            store
                .get_blob(&request.group.tenant, EXISTING_DIGEST)
                .await
                .is_err()
        );
        for erased in [false, true] {
            if erased {
                store
                    .delete_blob(&request.group.tenant, EXISTING_DIGEST)
                    .await
                    .unwrap();
            }
            let before = integrity_snapshot(&path);
            let calls = Arc::new(AtomicUsize::new(0));
            let retry = AtomicBlobEventStore::append_group_with_blobs_guarded(
                &store,
                &request,
                Arc::new(NonReadingGuard(calls.clone())),
            )
            .await
            .unwrap();
            assert!(retry.deduplicated);
            assert_eq!(retry.appends[0].events, original.appends[0].events);
            assert_eq!(before, integrity_snapshot(&path));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            if erased {
                assert_eq!(
                    store
                        .get_blob(&request.group.tenant, EXISTING_DIGEST)
                        .await
                        .unwrap(),
                    None
                );
            }
        }
    }
}
