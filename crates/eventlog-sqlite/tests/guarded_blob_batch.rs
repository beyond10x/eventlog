//! `AtomicEventStore::append_group_guarded_with_blobs` on SQLite, beyond the shared exercise.
//!
//! The shared exercise (`eventlog_conformance::run_guarded_group_blobs`) covers a refused guard,
//! an admitted commit, an exact retry, a retry carrying another batch, and a retry after
//! deletion. These cases cover what it leaves out: a later member's conflict, existing and
//! repeated bindings, a retry that carries no batch, admission on a retry, a reopened file, an
//! owner whose database predates the batch record, and tampering after a verified read.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, CaptureLimits, ConsistentTenantCapture,
    EventLogError, EventStore, Expected, Guard, NoGuard, ProjectionStore, StreamAppend, StreamId,
    TenantId,
};
use eventlog_sqlite::SqliteEventStore;

const PREFIX: &str = "batch";

fn tenant() -> TenantId {
    TenantId::new("guarded-batch").unwrap()
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

fn member(id: &str, expected: Expected) -> StreamAppend {
    StreamAppend {
        stream: stream(id),
        expected,
        events: vec![event("item.changed", 1)],
    }
}

fn group(key: &str, appends: Vec<StreamAppend>) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: meta(key, &serde_json::json!({})),
        appends,
    }
}

fn blob(digest: &str, bytes: &str) -> (String, Vec<u8>) {
    (digest.to_owned(), bytes.as_bytes().to_vec())
}

async fn file_store(directory: &tempfile::TempDir) -> (SqliteEventStore, String) {
    let path = directory.path().join("batch.sqlite3");
    let path = path.to_str().unwrap().to_owned();
    (SqliteEventStore::open(&path, PREFIX).await.unwrap(), path)
}

async fn assert_unbound(store: &SqliteEventStore, digests: &[&str]) {
    for digest in digests {
        assert_eq!(
            store.get_blob(&tenant(), digest).await.unwrap(),
            None,
            "{digest}: no blob of a group that did not commit is reachable"
        );
    }
}

struct Counting(Arc<AtomicUsize>);

impl Guard for Counting {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// A guard that reads one digest of the batch it is admitting and records what it saw.
struct Peeking {
    digest: &'static str,
    saw: Arc<std::sync::Mutex<Vec<Option<Vec<u8>>>>>,
}

impl Guard for Peeking {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let seen = store.get_blob(self.digest).await?;
            self.saw.lock().unwrap().push(seen);
            Ok(())
        })
    }
}

#[tokio::test]
async fn a_later_members_conflict_leaves_no_blob_of_the_batch_reachable_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let (store, path) = file_store(&directory).await;
    store
        .append(
            &stream("held"),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta("held", &serde_json::json!({})),
        )
        .await
        .unwrap();
    store.put_blob(&tenant(), "kept", b"kept").await.unwrap();

    let refused = store
        .append_group_guarded_with_blobs(
            &group(
                "conflicted",
                vec![
                    member("fresh", Expected::Any),
                    member("held", Expected::NoStream),
                ],
            ),
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("kept", "kept"), blob("b", "beta")],
        )
        .await
        .expect_err("the second member conflicts");
    assert!(
        matches!(refused, EventLogError::Conflict { .. }),
        "the conflict is the member's own: {refused:?}"
    );
    assert_unbound(&store, &["a", "b"]).await;
    assert_eq!(store.stream_version(&stream("fresh")).await.unwrap(), None);
    drop(store);

    let reopened = SqliteEventStore::open(&path, PREFIX).await.unwrap();
    assert_unbound(&reopened, &["a", "b"]).await;
    assert_eq!(
        reopened.get_blob(&tenant(), "kept").await.unwrap(),
        Some(b"kept".to_vec()),
        "a binding that existed before the failed group is never removed as its cleanup"
    );
    assert_eq!(
        reopened.stream_version(&stream("fresh")).await.unwrap(),
        None
    );
    assert_eq!(
        reopened.stream_version(&stream("held")).await.unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn admission_runs_before_the_batch_is_bound() {
    let store = SqliteEventStore::in_memory(PREFIX).await.unwrap();
    let saw = Arc::new(std::sync::Mutex::new(Vec::new()));
    store
        .append_group_guarded_with_blobs(
            &group("peek", vec![member("one", Expected::NoStream)]),
            Arc::new(Peeking {
                digest: "a",
                saw: Arc::clone(&saw),
            }),
            &[blob("a", "alpha")],
        )
        .await
        .unwrap();
    assert_eq!(
        saw.lock().unwrap().clone(),
        vec![None],
        "the guard ran once, and ran before a byte of the batch was bound, as the port documents"
    );
    assert_eq!(
        store.get_blob(&tenant(), "a").await.unwrap(),
        Some(b"alpha".to_vec())
    );
}

#[tokio::test]
async fn an_existing_equal_binding_is_reused_and_a_different_one_refuses_the_whole_group() {
    let store = SqliteEventStore::in_memory(PREFIX).await.unwrap();
    store.put_blob(&tenant(), "a", b"alpha").await.unwrap();

    let reused = store
        .append_group_guarded_with_blobs(
            &group("reuse", vec![member("one", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("b", "beta")],
        )
        .await
        .unwrap();
    assert!(!reused.deduplicated);
    assert_eq!(
        store.get_blob(&tenant(), "b").await.unwrap(),
        Some(b"beta".to_vec())
    );

    let contradicted = store
        .append_group_guarded_with_blobs(
            &group("contradicts", vec![member("two", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("c", "gamma"), blob("a", "other")],
        )
        .await
        .expect_err("an existing digest bound to other bytes");
    assert!(
        matches!(contradicted, EventLogError::Invalid(ref message)
            if message == "blob digest already names different content"),
        "{contradicted:?}"
    );
    assert_unbound(&store, &["c"]).await;
    assert_eq!(
        store.get_blob(&tenant(), "a").await.unwrap(),
        Some(b"alpha".to_vec()),
        "the original binding is intact"
    );
    assert_eq!(store.stream_version(&stream("two")).await.unwrap(), None);
}

#[tokio::test]
async fn a_digest_repeated_inside_one_batch_binds_once_and_must_agree_with_itself() {
    let store = SqliteEventStore::in_memory(PREFIX).await.unwrap();
    store
        .append_group_guarded_with_blobs(
            &group("repeated", vec![member("one", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("a", "alpha")],
        )
        .await
        .expect("an equal repetition is one binding");
    assert_eq!(
        store.get_blob(&tenant(), "a").await.unwrap(),
        Some(b"alpha".to_vec())
    );

    let refused = store
        .append_group_guarded_with_blobs(
            &group("disagrees", vec![member("two", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("e", "first"), blob("e", "other")],
        )
        .await
        .expect_err("a digest that disagrees with itself");
    assert!(matches!(refused, EventLogError::Invalid(_)), "{refused:?}");
    assert_unbound(&store, &["e"]).await;
    assert_eq!(store.stream_version(&stream("two")).await.unwrap(), None);
}

#[tokio::test]
async fn retries_deduplicate_after_reopen_with_or_without_the_batch_and_only_a_batch_readmits() {
    let directory = tempfile::tempdir().unwrap();
    let (store, path) = file_store(&directory).await;
    let committed = group("retried", vec![member("one", Expected::NoStream)]);
    let batch = [blob("a", "alpha"), blob("b", "beta")];
    let calls = Arc::new(AtomicUsize::new(0));
    let first = store
        .append_group_guarded_with_blobs(&committed, Arc::new(Counting(calls.clone())), &batch)
        .await
        .unwrap();
    assert!(!first.deduplicated);
    drop(store);

    let reopened = SqliteEventStore::open(&path, PREFIX).await.unwrap();
    let mut reversed = batch.to_vec();
    reversed.reverse();
    let retry = reopened
        .append_group_guarded_with_blobs(&committed, Arc::new(Counting(calls.clone())), &reversed)
        .await
        .unwrap();
    assert!(retry.deduplicated, "the recorded batch survives the reopen");
    assert_eq!(retry.appends, {
        let mut original = first.appends.clone();
        for append in &mut original {
            append.deduplicated = true;
        }
        original
    });
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "a retry carrying a batch is admitted again, as the port documents"
    );

    let plain = reopened
        .append_group_guarded(&committed, Arc::new(Counting(calls.clone())))
        .await
        .unwrap();
    assert!(
        plain.deduplicated,
        "a retry carrying no batch asks nothing about blobs and is the committed group"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "and repeats no admission");
    assert_eq!(
        reopened.stream_version(&stream("one")).await.unwrap(),
        Some(1)
    );

    let changed = reopened
        .append_group_guarded_with_blobs(
            &committed,
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("b", "other")],
        )
        .await
        .expect_err("the recorded digests with other bytes");
    assert!(matches!(changed, EventLogError::Invalid(_)), "{changed:?}");
}

#[tokio::test]
async fn an_owner_created_before_the_batch_record_gains_it_inside_the_first_batch() {
    let directory = tempfile::tempdir().unwrap();
    let (store, path) = file_store(&directory).await;
    let legacy = group("legacy", vec![member("old", Expected::NoStream)]);
    store.append_group(&legacy).await.unwrap();
    drop(store);
    let raw = rusqlite::Connection::open(&path).unwrap();
    let dropped = raw
        .execute_batch(&format!("DROP TABLE IF EXISTS {PREFIX}_group_batches"))
        .is_ok();
    assert!(dropped);
    drop(raw);

    let existing = SqliteEventStore::open_existing(&path, PREFIX)
        .await
        .expect("an owner without the batch record is still an existing owner");
    let committed = group("fresh", vec![member("new", Expected::NoStream)]);
    existing
        .append_group_guarded_with_blobs(&committed, Arc::new(NoGuard), &[blob("a", "alpha")])
        .await
        .unwrap();
    assert!(
        existing
            .append_group_guarded_with_blobs(&committed, Arc::new(NoGuard), &[blob("a", "alpha")])
            .await
            .unwrap()
            .deduplicated
    );
    let asked = existing
        .append_group_guarded_with_blobs(&legacy, Arc::new(NoGuard), &[blob("a", "alpha")])
        .await
        .expect_err("a group committed without a batch never bound one");
    assert!(
        matches!(asked, EventLogError::IdempotencyMismatch { ref key } if key == "legacy"),
        "{asked:?}"
    );
    assert!(existing.append_group(&legacy).await.unwrap().deduplicated);
}

#[tokio::test]
async fn content_tampered_after_a_verified_read_is_refused_on_the_same_handle() {
    let directory = tempfile::tempdir().unwrap();
    let (store, path) = file_store(&directory).await;
    store
        .append_group_guarded_with_blobs(
            &group("bound", vec![member("one", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("grouped", "original"), blob("other", "intact")],
        )
        .await
        .unwrap();
    store
        .put_blob(&tenant(), "alone", b"original")
        .await
        .unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    let limits = CaptureLimits {
        max_events: 16,
        max_blobs: 16,
        max_projection_rows: 16,
        max_payload_bytes: 1 << 20,
    };
    for digest in ["grouped", "alone"] {
        for _ in 0..3 {
            assert_eq!(
                store.get_blob(&tenant(), digest).await.unwrap(),
                Some(b"original".to_vec())
            );
        }
    }
    store.capture_tenant(&tenant(), &[], limits).await.unwrap();

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        &format!("UPDATE {PREFIX}_blobs SET bytes=?1 WHERE digest='grouped'"),
        rusqlite::params![b"mutated!".as_slice()],
    )
    .expect("same-length byte corruption");
    raw.execute(
        &format!(
            "UPDATE {PREFIX}_blobs SET integrity_sha256='{}' WHERE digest='alone'",
            "0".repeat(64)
        ),
        [],
    )
    .expect("hash-only corruption, bytes untouched");

    for digest in ["grouped", "alone"] {
        assert!(
            matches!(
                store.get_blob(&tenant(), digest).await,
                Err(EventLogError::Backend(_))
            ),
            "{digest}: a verified read earlier on this handle does not excuse a changed row"
        );
    }
    assert!(
        store.capture_tenant(&tenant(), &[], limits).await.is_err(),
        "a capture refuses the changed rows too"
    );
    assert!(
        matches!(
            store.put_blob(&tenant(), "grouped", b"original").await,
            Err(EventLogError::Backend(_))
        ),
        "put readback validates the stored row before comparing it"
    );
    assert_eq!(
        store.get_blob(&tenant(), "other").await.unwrap(),
        Some(b"intact".to_vec()),
        "an unchanged row still reads"
    );
}

#[tokio::test]
async fn erasing_a_tenant_erases_the_batches_its_groups_recorded() {
    let directory = tempfile::tempdir().unwrap();
    let (store, path) = file_store(&directory).await;
    store
        .append_group_guarded_with_blobs(
            &group("erased", vec![member("one", Expected::NoStream)]),
            Arc::new(NoGuard),
            &[blob("a", "alpha")],
        )
        .await
        .unwrap();
    let rows = || -> i64 {
        rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                &format!("SELECT count(*) FROM {PREFIX}_group_batches WHERE tenant_id=?1"),
                [tenant().as_str()],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(rows(), 1, "the commit recorded its batch");
    store.forget_tenant(&tenant()).await.unwrap();
    assert_eq!(
        rows(),
        0,
        "an erased tenant leaves no recorded batch behind"
    );
}
