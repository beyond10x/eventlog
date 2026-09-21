//! Independent-review cases, pass 2, for `story:grouped-blob-writes-take-one-durability-barrier`.
//!
//! Added by review pass 2 of unit 11, against correction round 1. Each case drives the
//! implementation from a contract the unit itself wrote — the doc comment on
//! `Transaction::verify_bound`, the doc comment on `FileEventStore::append_group_with_blobs`, and
//! the "Where the barrier is for a grouped write" and "Who disposes of an object no frame
//! references" sections of `docs/design/file-provider.md`.
//!
//! Nothing here edits an implementation file and nothing here changes an existing case.

use std::fs;

use eventlog_core::{
    AppendGroup, EventLogError, EventStore, Expected, StreamAppend, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::json;

fn tenant() -> TenantId {
    TenantId::new("grouped-blob-review-two").expect("valid tenant")
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).expect("valid stream")
}

fn group(key: &str, count: usize) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: (0..count)
            .map(|index| StreamAppend {
                stream: stream(&format!("s{index}")),
                expected: Expected::Any,
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

/// A retry of a grouped blob write is still idempotent after the batch's blob has been deleted.
///
/// This is the cost of the correction, charged on a path the correction did not consider.
/// `Transaction::verify_bound` now answers `IdempotencyMismatch` for "a digest the history does
/// not carry at all", on the reasoning its own doc gives: "a digest the history does not carry at
/// all means this is not the request that committed". `EventStore::delete_blob` makes that
/// reasoning false. The digest is unbound because it was deleted, not because a different request
/// is retrying — the group's frame, its members and its idempotency key are all exactly where the
/// first call left them, and `AppendGroup::fingerprint` still matches.
///
/// What the refusal costs is not a wasted call. `IdempotencyMismatch` tells a caller "your key
/// names a different request", and the only recovery from it is a new key; a new key over the
/// same members appends every one of them a second time. So a blob deletion — an erasure, a
/// retention sweep, an operator's `delete_blob` — turns the retry that idempotency exists to
/// serve into a duplicate commit, for a group that is committed and durable and that the store
/// can still identify.
///
/// Three outcomes are defensible here and this case accepts any of them: deduplicate as before,
/// or refuse with something that is not `IdempotencyMismatch` and so does not invite a new key.
/// What is not defensible is telling a caller its committed key belongs to another request.
#[tokio::test]
async fn a_retry_is_still_idempotent_after_the_batchs_blob_has_been_deleted() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    let first = store
        .append_group_with_blobs(&group("erased", 2), &blobs(0, 1))
        .await
        .expect("the first commit under this key");
    assert!(!first.deduplicated, "the first commit is not a retry");

    // An ordinary erasure of the blob. The group, its members and its key are untouched.
    store
        .delete_blob(&tenant(), "d0000")
        .await
        .expect("the blob is deleted");
    assert_eq!(
        store.get_blob(&tenant(), "d0000").await.expect("readable"),
        None,
        "the fixture only means something once the blob really is gone"
    );

    let retry = store
        .append_group_with_blobs(&group("erased", 2), &blobs(0, 1))
        .await;

    match retry {
        Ok(result) => assert!(
            result.deduplicated,
            "the same key, the same members and the same batch is the same request"
        ),
        Err(EventLogError::IdempotencyMismatch { ref key }) => panic!(
            "the committed key {key} is reported as naming a different request after the blob \
             it bound was deleted; the group is still committed under exactly this key, and the \
             only recovery from IdempotencyMismatch is a new key, which appends every member of \
             the group a second time"
        ),
        Err(other) => panic!("unexpected refusal: {other:?}"),
    }
}

/// A refusal after the batch is on disk disposes of what the batch wrote and nothing else.
///
/// The correction chose object identity over a state-driven sweep, and `Transaction::written`'s
/// own comment says why: "A transaction that refuses has already applied its own `Op::Blob`s to
/// `state`, so `clean_blobs` would read those objects as live; this is the list that says
/// otherwise." The other half of that choice is the half nothing asserts — the list must also not
/// name an object the *committed* history binds. A batch that re-binds a digest the store already
/// carries takes the `continue` arm of `bind_blobs` and creates no object for it, so the
/// already-committed object must survive the refusal. This is the control for the disposal
/// finding: a disposer that removed one object too many would be a data-loss defect that the
/// existing "leaves no object behind" cases, which count zero, cannot see.
#[tokio::test]
async fn a_refused_group_does_not_dispose_of_a_blob_the_history_already_binds() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .put_blob(&tenant(), "d0000", b"bytes-0")
        .await
        .expect("one blob committed on its own path");

    // One member expecting version 7 of a stream that does not exist: an ordinary concurrency
    // refusal, taken after `bind_blobs` has written and synchronized the fresh object.
    let mut refusing = group("mixed", 1);
    refusing.appends[0].expected = Expected::Exact(7);
    let refused = store
        .append_group_with_blobs(&refusing, &blobs(0, 2))
        .await
        .expect_err("the member refuses after the batch is on disk");
    assert!(
        matches!(refused, EventLogError::Conflict { .. }),
        "the refusal is the ordinary concurrency refusal: {refused:?}"
    );

    assert_eq!(
        store.get_blob(&tenant(), "d0000").await.expect("readable"),
        Some(b"bytes-0".to_vec()),
        "the blob the committed history binds survives a refusal that re-bound it"
    );
    assert_eq!(
        store.get_blob(&tenant(), "d0001").await.expect("readable"),
        None,
        "and the digest the refused batch introduced binds nothing"
    );
    assert_eq!(
        fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count),
        1,
        "exactly the committed object remains: the refusal disposed of its own and no other"
    );
}

/// The file provider's half of the port-level control: a committed key over a batch the history
/// does not carry is refused, and the call publishes nothing.
///
/// This is the behaviour `docs/design/file-provider.md` states — "A batch naming bytes the history
/// does not carry is not the request that committed and refuses with `IdempotencyMismatch`" — and
/// it is the control for `crates/eventlog-sqlite/tests/grouped_blob_barrier_review_two.rs`, where
/// the same sequence against the port's default implementation returns `Ok` with
/// `deduplicated: true` after publishing the batch. Expected green here; it is in this file so
/// that the two providers' answers to one call sequence can be read side by side.
#[tokio::test]
async fn a_committed_key_over_an_unbound_batch_is_refused_and_publishes_nothing() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("port-control", 2), &blobs(0, 1))
        .await
        .expect("the first commit under this key");

    let retry = store
        .append_group_with_blobs(&group("port-control", 2), &blobs(0, 2))
        .await
        .expect_err("a batch carrying a digest the commit never bound is not that request");
    assert!(
        matches!(retry, EventLogError::IdempotencyMismatch { ref key } if key == "port-control"),
        "the refusal names the key: {retry:?}"
    );
    assert_eq!(
        store.get_blob(&tenant(), "d0001").await.expect("readable"),
        None,
        "and the refused retry published no byte of the batch it carried"
    );
}
