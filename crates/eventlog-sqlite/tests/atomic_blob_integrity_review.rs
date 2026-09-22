//! Independent SQLite atomic-reuse integrity and retained-receipt regressions.
use eventlog_core::{
    AtomicBlobEventStore, BlobAppendGroup, BlobWrite, BoxFuture, EventLogError, EventStore,
    Expected, Guard, ProjectionStore, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::{Connection, types::Value};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const PREFIX: &str = "integrity_review";

fn database(label: &str) -> PathBuf {
    let root = tempfile::Builder::new()
        .prefix(label)
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("fixture.sqlite");
    eprintln!("retained synthetic fixture: {}", path.display());
    path
}

async fn open(path: &Path) -> SqliteEventStore {
    SqliteEventStore::open(path.to_str().unwrap(), PREFIX)
        .await
        .unwrap()
}

fn request() -> BlobAppendGroup {
    let mut request = eventlog_conformance::atomic_blob_request("review", "item", "z-existing");
    request.blobs.extend([
        BlobWrite {
            digest: "a-new".into(),
            bytes: b"new-binding".to_vec(),
        },
        BlobWrite {
            digest: "m-healthy".into(),
            bytes: Vec::new(),
        },
    ]);
    request
}

async fn populate(store: &SqliteEventStore, request: &BlobAppendGroup) {
    for blob in request.blobs.iter().filter(|blob| blob.digest != "a-new") {
        store
            .put_blob(&request.group.tenant, &blob.digest, &blob.bytes)
            .await
            .unwrap();
    }
}

fn corrupt(path: &Path, tenant: &TenantId, assignment: &str) {
    let raw = Connection::open(path).unwrap();
    raw.execute_batch("PRAGMA ignore_check_constraints=ON")
        .unwrap();
    assert_eq!(raw.execute(
        &format!("UPDATE integrity_review_blobs SET {assignment} WHERE tenant_id=?1 AND digest='z-existing'"),
        [tenant.as_str()],
    ).unwrap(), 1);
}

fn snapshot(path: &Path) -> Vec<Vec<Vec<Value>>> {
    let raw = Connection::open(path).unwrap();
    ["blobs", "events", "commands", "append_groups"]
        .into_iter()
        .map(|table| {
            let mut statement = raw
                .prepare(&format!(
                    "SELECT * FROM integrity_review_{table} ORDER BY 1,2"
                ))
                .unwrap();
            let columns = statement.column_count();
            statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get(index))
                        .collect::<Result<Vec<Value>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        })
        .collect()
}

struct Observe {
    calls: Arc<AtomicUsize>,
    blobs: Vec<BlobWrite>,
    reads: bool,
    refuses: bool,
}
impl Guard for Observe {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.reads {
                for blob in &self.blobs {
                    assert_eq!(
                        store.get_blob(&blob.digest).await?,
                        Some(blob.bytes.clone())
                    );
                }
            }
            if self.refuses {
                return Err(EventLogError::GuardRefused {
                    code: "review-refusal".into(),
                });
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn corrupt_reuse_precedes_byte_conflict_and_all_guard_modes() {
    for mode in 0..3 {
        let path = database("atomic-integrity-precedence-");
        let mut request = request();
        let store = open(&path).await;
        populate(&store, &request).await;
        drop(store);
        corrupt(
            &path,
            &request.group.tenant,
            "integrity_sha256=upper(integrity_sha256)",
        );
        request.blobs[0].bytes[0] ^= 1;
        let store = open(&path).await;
        let before = snapshot(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let result = if mode == 0 {
            store.append_group_with_blobs(&request).await
        } else {
            store
                .append_group_with_blobs_guarded(
                    &request,
                    Arc::new(Observe {
                        calls: calls.clone(),
                        blobs: request.blobs.clone(),
                        reads: mode == 2,
                        refuses: false,
                    }),
                )
                .await
        };
        assert!(
            matches!(result, Err(EventLogError::Backend(message)) if message == "stored blob integrity metadata is invalid")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(snapshot(&path), before);
        assert_eq!(
            store
                .stream_version(&request.group.appends[0].stream)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .get_blob(&request.group.tenant, "a-new")
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn malformed_sqlite_storage_classes_refuse_without_partial_publication() {
    for assignment in [
        "byte_count='not-an-integer'",
        "integrity_v1=x'01'",
        "integrity_sha256=x'616263'",
        "bytes='not-a-blob'",
    ] {
        let path = database("atomic-integrity-storage-class-");
        let request = request();
        let store = open(&path).await;
        populate(&store, &request).await;
        drop(store);
        corrupt(&path, &request.group.tenant, assignment);
        let store = open(&path).await;
        assert!(matches!(
            store.get_blob(&request.group.tenant, "z-existing").await,
            Err(EventLogError::Backend(_))
        ));
        let before = snapshot(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let result = store
            .append_group_with_blobs_guarded(
                &request,
                Arc::new(Observe {
                    calls: calls.clone(),
                    blobs: request.blobs.clone(),
                    reads: true,
                    refuses: false,
                }),
            )
            .await;
        assert!(
            matches!(result, Err(EventLogError::Backend(_))),
            "{assignment}: {result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(snapshot(&path), before, "{assignment}");
    }
}

#[tokio::test]
async fn retained_receipt_wins_before_corrupt_row_decode_after_head_advance() {
    let path = database("atomic-integrity-receipt-");
    let request = request();
    let store = open(&path).await;
    let original = store.append_group_with_blobs(&request).await.unwrap();
    let mut advanced = request.clone();
    advanced.group.meta.idempotency_key = "advance".into();
    advanced.group.appends[0].expected = Expected::Exact(1);
    store.append_group_with_blobs(&advanced).await.unwrap();
    drop(store);
    corrupt(&path, &request.group.tenant, "byte_count='not-an-integer'");
    let store = open(&path).await;
    assert_eq!(
        store
            .stream_version(&request.group.appends[0].stream)
            .await
            .unwrap(),
        Some(2)
    );
    for erased in [false, true] {
        if erased {
            store
                .delete_blob(&request.group.tenant, "z-existing")
                .await
                .unwrap();
            store
                .delete_blob(&request.group.tenant, "a-new")
                .await
                .unwrap();
        }
        let before = snapshot(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = || {
            Arc::new(Observe {
                calls: calls.clone(),
                blobs: request.blobs.clone(),
                reads: true,
                refuses: true,
            })
        };
        let mut mismatch = request.clone();
        mismatch.blobs[0].bytes.push(0);
        assert!(
            matches!(store.append_group_with_blobs_guarded(&mismatch, guard()).await,
            Err(EventLogError::IdempotencyMismatch { key }) if key == "review")
        );
        let replay = store
            .append_group_with_blobs_guarded(&request, guard())
            .await
            .unwrap();
        assert!(replay.deduplicated);
        assert_eq!(replay.appends[0].events, original.appends[0].events);
        assert_eq!(
            replay.appends[0].first_version,
            original.appends[0].first_version
        );
        assert_eq!(
            replay.appends[0].last_version,
            original.appends[0].last_version
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(snapshot(&path), before);
        if erased {
            assert_eq!(
                store
                    .get_blob(&request.group.tenant, "z-existing")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                store
                    .get_blob(&request.group.tenant, "a-new")
                    .await
                    .unwrap(),
                None
            );
        }
    }
}

#[tokio::test]
async fn foreign_corruption_and_guard_refusal_preserve_healthy_reuse() {
    let path = database("atomic-integrity-tenant-");
    let request = request();
    let store = open(&path).await;
    populate(&store, &request).await;
    let foreign = TenantId::new("foreign-owner").unwrap();
    store
        .put_blob(&foreign, "z-existing", b"foreign-bytes")
        .await
        .unwrap();
    drop(store);
    corrupt(&path, &foreign, "byte_count=-1");
    let store = open(&path).await;
    let before = snapshot(&path);
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = |refuses| {
        Arc::new(Observe {
            calls: calls.clone(),
            blobs: request.blobs.clone(),
            reads: true,
            refuses,
        })
    };
    assert!(
        matches!(store.append_group_with_blobs_guarded(&request, guard(true)).await,
        Err(EventLogError::GuardRefused { code }) if code == "review-refusal")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot(&path), before);
    let committed = store
        .append_group_with_blobs_guarded(&request, guard(false))
        .await
        .unwrap();
    assert!(!committed.deduplicated);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    for blob in &request.blobs {
        assert_eq!(
            store
                .get_blob(&request.group.tenant, &blob.digest)
                .await
                .unwrap(),
            Some(blob.bytes.clone())
        );
    }
    assert!(matches!(
        store.get_blob(&foreign, "z-existing").await,
        Err(EventLogError::Backend(_))
    ));
}
