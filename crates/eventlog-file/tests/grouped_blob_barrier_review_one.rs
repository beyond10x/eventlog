//! Independent-review cases for `story:grouped-blob-writes-take-one-durability-barrier`.
//!
//! Added by review pass 1 of unit 11. Each case drives the implementation from a contract the
//! unit itself wrote — the doc comment on `FileEventStore::append_group_with_blobs` and the
//! "Where the barrier is for a grouped write" section of `docs/design/file-provider.md`.
//! Nothing here edits an implementation file or an existing case.

use std::fs;

use eventlog_core::{
    AppendGroup, EventLogError, EventStore, Expected, StreamAppend, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::json;

fn tenant() -> TenantId {
    TenantId::new("grouped-blob-review").expect("valid tenant")
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).expect("valid stream")
}

/// One group of `count` members under idempotency key `key`.
fn group(key: &str, count: usize, expected: Expected) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: (0..count)
            .map(|index| StreamAppend {
                stream: stream(&format!("s{index}")),
                expected,
                events: vec![eventlog_conformance::event("item.changed", 1)],
            })
            .collect(),
    }
}

fn blobs(first: usize, count: usize) -> Vec<(String, Vec<u8>)> {
    (first..first + count)
        .map(|index| {
            (
                format!("d{index:04}"),
                format!("bytes-{index}").into_bytes(),
            )
        })
        .collect()
}

/// `append_group_with_blobs` returning `Ok` means every digest of the batch is bound.
///
/// The unit's own contract, twice stated. The method's doc comment: "A deduplicated retry binds
/// nothing, because the original commit already bound it." `docs/design/file-provider.md`: "A
/// deduplicated group retry binds nothing, because the original commit already bound it."
///
/// Both sentences assume the committed frame already carries the batch the retry is holding.
/// Nothing enforces that assumption: the operation's identity is `AppendGroup::fingerprint`, which
/// hashes the tenant, the members and the command meta, and this unit added blobs to what the
/// operation commits without adding them to what identifies it. A second call under a key the
/// store has already committed therefore takes the deduplicating return before `bind_blobs` runs
/// at all, and reports success over a batch it never wrote.
///
/// Either outcome is defensible and this case accepts both: refuse with `IdempotencyMismatch`
/// because the batch is part of the request, or bind the batch. What is not defensible is `Ok`
/// with the batch unbound, because a caller has no way to tell that from the batch being durable.
#[tokio::test]
async fn ok_from_a_grouped_blob_write_means_the_batch_is_bound() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("import-1", 2, Expected::Any), &blobs(0, 1))
        .await
        .expect("the first commit under this key");

    let second = store
        .append_group_with_blobs(&group("import-1", 2, Expected::Any), &blobs(1, 1))
        .await;

    match second {
        Err(EventLogError::IdempotencyMismatch { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other:?}"),
        Ok(result) => {
            assert_eq!(
                store
                    .get_blob(&tenant(), "d0001")
                    .await
                    .expect("readable store"),
                Some(b"bytes-1".to_vec()),
                "a grouped blob write that returns Ok (deduplicated={}) bound every digest of \
                 its batch; the group fingerprint does not cover the batch, so a committed key \
                 reports success over blobs no frame binds",
                result.deduplicated,
            );
        }
    }
}

/// A group refused after its objects are written leaves no object file behind.
///
/// `bind_blobs` writes and synchronizes every object before the first member append runs, so a
/// member that refuses — an ordinary optimistic-concurrency refusal, `Expected::Exact` against a
/// head that moved — refuses after the objects are on disk. That window did not exist at the base
/// commit: `put_blob` wrote its object in a transaction with nothing after it, and `append_group`
/// wrote no objects at all.
///
/// `docs/design/file-provider.md` names exactly two disposers for objects no committed frame
/// references, "the next committed transaction and the complete opener", and the unit's own crash
/// case asserts the invariant in its strong form — "unreferenced objects from an interrupted
/// batch are disposed of". This case asserts the same invariant for the refusal path, which is
/// the path a live process actually takes.
#[tokio::test]
async fn a_group_refused_after_its_blobs_are_written_leaves_no_object_behind() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    let refused = store
        .append_group_with_blobs(&group("conflict", 3, Expected::Exact(7)), &blobs(0, 4))
        .await
        .expect_err("a member expecting version 7 of a stream that does not exist refuses");
    assert!(
        matches!(refused, EventLogError::Conflict { .. }),
        "the refusal is the ordinary concurrency refusal, not something earlier: {refused:?}"
    );

    let objects = fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count);
    assert_eq!(
        objects, 0,
        "a refused batch leaves no object file no committed frame names"
    );
}

/// The same refusal, asked one step later: whatever the refusal leaves is gone once the store has
/// done anything else at all.
///
/// Separated from the case above because the two carry very different weight. If this one is
/// green the residue is bounded by the next operation on the handle; if it is red the residue is
/// durable, and a batch of 256 objects is 256 files nothing will ever remove.
#[tokio::test]
async fn whatever_a_refused_group_leaves_is_gone_after_the_next_operation() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("conflict", 3, Expected::Exact(7)), &blobs(0, 4))
        .await
        .expect_err("a member expecting version 7 of a stream that does not exist refuses");

    store
        .append_group_with_blobs(&group("after", 1, Expected::Any), &blobs(100, 1))
        .await
        .expect("a group that does not refuse");

    let objects = fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count);
    assert_eq!(
        objects, 1,
        "only the object the committed frame binds survives the next committed transaction"
    );
}
