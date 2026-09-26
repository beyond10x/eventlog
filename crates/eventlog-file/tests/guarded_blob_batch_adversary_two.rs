//! Second adversary pass on `EVENTLOG-BLOB-BATCH`: the File guarded retry of an atomic receipt.
//!
//! `docs/design/atomic-blob-append.md` says a guarded blob-bearing retry "still runs its admission
//! first", and `FileEventStore::group` says why: otherwise a caller could replay a key and read
//! back whether bytes match, with the guard never called.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup, BlobWrite, BoxFuture,
    EventLogError, Expected, Guard, NoGuard, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
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
#[tokio::test(flavor = "multi_thread")]
async fn file_refused_guarded_retry_of_an_atomic_receipt_is_refused_by_the_guard_not_by_the_bytes()
{
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();

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

    AtomicBlobEventStore::append_group_with_blobs(
        &store,
        &BlobAppendGroup {
            group: group("atomic"),
            blobs: vec![BlobWrite {
                digest: "secret".into(),
                bytes: b"real".to_vec(),
            }],
        },
    )
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

/// An atomic receipt that reached the manifest survives a reopen as the same request for the
/// guarded method, and a torn frame after it (a crash mid-append of the next write, no intent)
/// is neither folded into a receipt nor silently dropped: the reopen refuses.
#[tokio::test(flavor = "multi_thread")]
async fn file_atomic_receipt_survives_reopen_and_a_torn_tail_after_it_refuses() {
    struct Count(Arc<AtomicUsize>);
    impl Guard for Count {
        fn check<'a>(
            &'a self,
            _store: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }
    let directory = tempfile::tempdir().unwrap();
    {
        let store = FileEventStore::open(directory.path()).await.unwrap();
        AtomicBlobEventStore::append_group_with_blobs(
            &store,
            &BlobAppendGroup {
                group: group("atomic"),
                blobs: vec![
                    BlobWrite {
                        digest: "b".into(),
                        bytes: b"two".to_vec(),
                    },
                    BlobWrite {
                        digest: "a".into(),
                        bytes: b"one".to_vec(),
                    },
                ],
            },
        )
        .await
        .unwrap();
    }
    let reopened = FileEventStore::open_existing(directory.path())
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let retry = reopened
        .append_group_guarded_with_blobs(
            &group("atomic"),
            Arc::new(Count(calls.clone())),
            &[
                ("a".to_owned(), b"one".to_vec()),
                ("b".to_owned(), b"two".to_vec()),
                ("a".to_owned(), b"one".to_vec()),
            ],
        )
        .await
        .expect("the reopened atomic receipt is this request");
    assert!(retry.deduplicated);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(reopened);

    let events = directory.path().join("events.jsonl");
    let mut bytes = std::fs::read(&events).unwrap();
    let frame = bytes.clone();
    let last = frame[..frame.len() - 1]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    bytes.extend_from_slice(&frame[last..last + (frame.len() - last) / 2]);
    std::fs::write(&events, &bytes).unwrap();
    // Either the opener refuses the unexplained suffix, or the committed receipt before it still
    // answers the retry as deduplicated. A fresh commit would mean the receipt was lost.
    if let Ok(store) = FileEventStore::open_existing(directory.path()).await {
        let again = store
            .append_group_guarded_with_blobs(
                &group("atomic"),
                Arc::new(NoGuard),
                &[
                    ("a".to_owned(), b"one".to_vec()),
                    ("b".to_owned(), b"two".to_vec()),
                ],
            )
            .await;
        assert!(
            again.as_ref().is_err() || again.as_ref().is_ok_and(|r| r.deduplicated),
            "{again:?}"
        );
    }
}
