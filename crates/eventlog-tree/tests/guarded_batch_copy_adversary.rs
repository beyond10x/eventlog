//! Adversary case for `EVENTLOG-BLOB-BATCH`: SQLite now implements
//! `AtomicEventStore::append_group_guarded_with_blobs`, so a caller holding a trait object can
//! retry through the SQLite copy of a tree the exact call it made against the tree.
//!
//! `eventlog_tree::copy` promises "a retry of a copied command writes nothing" and replays every
//! blob-bearing group through `AtomicBlobEventStore::append_group_with_blobs`, whose receipt
//! fingerprint covers the batch. The SQLite guarded method fingerprints the group alone and looks
//! up `<prefix>_group_batches`, which the copy never writes. The same retry that deduplicates on
//! the tree is therefore told its key belongs to another request on the copy.

#[macro_use]
mod each_layout;

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, Guard, NoGuard,
    ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use eventlog_tree::{TreeEventStore, TreeLayout, copy};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn tenant() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

fn group() -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", "left").unwrap(),
            expected: Expected::NoStream,
            events: vec![event("item.received", 1)],
        }],
        meta: meta("boundary-1", &json!({ "boundary": 1 })),
    }
}

fn batch() -> Vec<(String, Vec<u8>)> {
    vec![
        ("blob-1".to_owned(), b"one".to_vec()),
        ("blob-2".to_owned(), b"two".to_vec()),
    ]
}

async fn a_guarded_batch_retry_that_deduplicates_on_the_tree_deduplicates_on_its_sqlite_copy(
    layout: TreeLayout,
) {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    tree.stream_identity(&tenant()).await.unwrap();
    let first = tree
        .append_group_guarded_with_blobs(&group(), Arc::new(NoGuard), &batch())
        .await
        .unwrap();
    assert!(!first.deduplicated);

    let on_tree = tree
        .append_group_guarded_with_blobs(&group(), Arc::new(NoGuard), &batch())
        .await
        .expect("control: the tree answers its own retry");
    assert!(on_tree.deduplicated, "control: the tree deduplicates");

    let target = SqliteEventStore::in_memory("copy_retry").await.unwrap();
    copy(&tree, &target).await.unwrap();

    let on_copy = target
        .append_group_guarded_with_blobs(&group(), Arc::new(NoGuard), &batch())
        .await;
    match on_copy {
        Ok(result) => assert!(
            result.deduplicated,
            "the copy appended a copied command a second time"
        ),
        Err(EventLogError::IdempotencyMismatch { key }) => panic!(
            "the copy refuses the tree's own retry of {key} as another request's key: the copied \
             receipt carries the blob-group fingerprint and no `_group_batches` row, so the only \
             way forward for the caller is a new key, which appends the group twice"
        ),
        Err(other) => panic!("unexpected refusal of a copied command's retry: {other:?}"),
    }
}

/// Refuses every admission and counts how often it was asked.
struct Refuses(AtomicUsize);

impl Guard for Refuses {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(EventLogError::GuardRefused {
                code: "retry_refused".into(),
            })
        })
    }
}

/// `AtomicEventStore::append_group_guarded_with_blobs` admits a retry carrying a batch again, so
/// a guard that refuses now refuses the retry, on the handle that committed it and on a reopened
/// one. A retry carrying no batch still repeats no admission.
async fn a_guarded_batch_retry_on_the_tree_is_admitted_again(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let plain = AppendGroup {
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant(), "item", "plain").unwrap(),
            expected: Expected::NoStream,
            events: vec![event("item.received", 1)],
        }],
        meta: meta("plain-1", &json!({ "plain": 1 })),
        ..group()
    };
    tree.append_group_guarded(&plain, Arc::new(NoGuard))
        .await
        .unwrap();
    let first = tree
        .append_group_guarded_with_blobs(&group(), Arc::new(NoGuard), &batch())
        .await
        .unwrap();
    assert!(!first.deduplicated);

    for (handle, store) in [
        ("the committing handle", tree),
        (
            "a reopened handle",
            TreeEventStore::open_in(directory.path(), layout)
                .await
                .unwrap(),
        ),
    ] {
        let guard = Arc::new(Refuses(AtomicUsize::new(0)));
        let retry = store
            .append_group_guarded_with_blobs(&group(), guard.clone(), &batch())
            .await;
        match retry {
            Err(EventLogError::GuardRefused { code }) => assert_eq!(code, "retry_refused"),
            Ok(result) => panic!(
                "{handle} answered a batch-bearing retry from its receipt without asking the \
                 guard (deduplicated: {})",
                result.deduplicated
            ),
            Err(other) => panic!("{handle}: not the guard's refusal: {other:?}"),
        }
        assert_eq!(
            guard.0.load(Ordering::SeqCst),
            1,
            "{handle}: admission ran once"
        );

        let guard = Arc::new(Refuses(AtomicUsize::new(0)));
        let without_batch = store
            .append_group_guarded(&plain, guard.clone())
            .await
            .expect("a retry carrying no batch repeats no admission");
        assert!(without_batch.deduplicated);
        assert_eq!(guard.0.load(Ordering::SeqCst), 0, "{handle}: no admission");

        let admitted = store
            .append_group_guarded_with_blobs(&group(), Arc::new(NoGuard), &batch())
            .await
            .expect("an admitted retry is answered from its receipt");
        assert!(admitted.deduplicated, "{handle}: the retry wrote twice");
        assert_eq!(
            admitted.appends[0].events, first.appends[0].events,
            "{handle}: another answer"
        );
    }
}

each_layout!(
    a_guarded_batch_retry_that_deduplicates_on_the_tree_deduplicates_on_its_sqlite_copy,
    a_guarded_batch_retry_on_the_tree_is_admitted_again,
);
