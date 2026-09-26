//! Adversary case for `EVENTLOG-BLOB-BATCH`: SQLite now implements
//! `AtomicEventStore::append_group_guarded_with_blobs`, so a caller holding a trait object can
//! retry through the SQLite copy of a tree the exact call it made against the tree.
//!
//! `eventlog_tree::copy` promises "a retry of a copied command writes nothing" and replays every
//! blob-bearing group through `AtomicBlobEventStore::append_group_with_blobs`, whose receipt
//! fingerprint covers the batch. The SQLite guarded method fingerprints the group alone and looks
//! up `<prefix>_group_batches`, which the copy never writes. The same retry that deduplicates on
//! the tree is therefore told its key belongs to another request on the copy.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, NoGuard, StreamAppend,
    StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use eventlog_tree::{TreeEventStore, copy};
use serde_json::json;
use std::sync::Arc;

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

#[tokio::test(flavor = "multi_thread")]
async fn a_guarded_batch_retry_that_deduplicates_on_the_tree_deduplicates_on_its_sqlite_copy() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path()).await.unwrap();
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
