//! Second adversary pass on `EVENTLOG-BLOB-BATCH`: the guarded retry of an atomic receipt.
//!
//! `docs/design/atomic-blob-append.md` says a guarded blob-bearing retry "still runs its admission
//! first", and `append_group_guarded_with_blobs` says why: otherwise a replayed key tells its
//! caller whether bytes match. These cases hold that for both receipt kinds under one key shape.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup, BlobWrite, BoxFuture,
    EventLogError, Expected, Guard, NoGuard, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Refuse(Arc<AtomicUsize>);

impl Guard for Refuse {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(EventLogError::GuardRefused {
                code: "no-admission".into(),
            })
        })
    }
}

fn tenant() -> TenantId {
    TenantId::new("adv-two").unwrap()
}

fn group(key: &str) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: meta(key, &serde_json::json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", key).unwrap(),
            expected: Expected::NoStream,
            events: vec![event("item.created", 1)],
        }],
    }
}

fn batch(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    vec![("secret".to_owned(), bytes.to_vec())]
}

/// A refused caller replaying a key with guessed bytes learns nothing about the bytes: the guard
/// answers before the batch is compared, whichever method recorded the receipt.
#[tokio::test]
async fn a_refused_guarded_retry_of_an_atomic_receipt_is_refused_by_the_guard_not_by_the_bytes() {
    let store = SqliteEventStore::in_memory("adv_two").await.unwrap();

    // The guarded receipt: guard first, then the batch.
    store
        .append_group_guarded_with_blobs(&group("guarded"), Arc::new(NoGuard), &batch(b"real"))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let guarded = store
        .append_group_guarded_with_blobs(
            &group("guarded"),
            Arc::new(Refuse(calls.clone())),
            &batch(b"guess"),
        )
        .await
        .expect_err("refused");
    assert!(
        matches!(guarded, EventLogError::GuardRefused { .. }),
        "guarded receipt: {guarded:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The atomic receipt, same shape of retry.
    store
        .append_group_with_blobs(&BlobAppendGroup {
            group: group("atomic"),
            blobs: vec![BlobWrite {
                digest: "secret".into(),
                bytes: b"real".to_vec(),
            }],
        })
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let atomic = store
        .append_group_guarded_with_blobs(
            &group("atomic"),
            Arc::new(Refuse(calls.clone())),
            &batch(b"guess"),
        )
        .await
        .expect_err("refused");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "admission runs before anything is said about the batch; got {atomic:?}"
    );
    assert!(
        matches!(atomic, EventLogError::GuardRefused { .. }),
        "atomic receipt: {atomic:?}"
    );
}
