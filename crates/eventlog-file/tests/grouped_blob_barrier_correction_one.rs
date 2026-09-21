//! Correction-round cases for `story:grouped-blob-writes-take-one-durability-barrier`.
//!
//! Two things are held here that nothing held before.
//!
//! The **guarded** form of the grouped blob write, which is the form a migration importing many
//! boundaries at once has to use: the unguarded `append_group_with_blobs` commits under `NoGuard`,
//! and an importer that commits under `NoGuard` publishes anchors its own design forbids. The
//! guarded form must publish nothing at all when the guard refuses — the ordering that makes that
//! true is `guard.check` before `bind_blobs`, and these cases are what holds it there.
//!
//! And the **identity of a grouped blob write**: `AppendGroup::fingerprint` hashes the tenant, the
//! members and the command meta and never the batch, so a second call under a committed key takes
//! the deduplicating return. `Ok` from that return is a claim that the batch is durable, and these
//! cases say what it costs to make the claim true — a genuine retry still deduplicates, and a
//! retry holding a batch the committed history does not carry is refused rather than answered
//! with a success it cannot support.

use std::{fs, sync::Arc};

use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, Guard,
    ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::json;

fn tenant() -> TenantId {
    TenantId::new("grouped-blob-correction").expect("valid tenant")
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

/// A guard that refuses every command it is asked about.
struct Refuses;

impl Guard for Refuses {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async {
            Err(EventLogError::GuardRefused {
                code: "correction_refused".into(),
            })
        })
    }
}

/// A guarded blob-bearing group whose guard refuses binds no blob, appends no frame, and leaves
/// no object file behind.
///
/// This is the case that pins `guard.check` above `bind_blobs`. Swap those two statements and the
/// store keeps every byte of a batch the guard rejected: content-addressed, bound by a committed
/// `Op::Blob`, readable through the ordinary port, and referenced by no group. No timing assertion
/// and no barrier counter can see that — the barrier count is one either way.
#[tokio::test]
async fn a_guarded_group_whose_guard_refuses_binds_no_blob_and_appends_no_frame() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    let refused = store
        .append_group_guarded_with_blobs(&group("refused", 2), Arc::new(Refuses), &blobs(0, 4))
        .await
        .expect_err("the guard refuses");
    assert!(
        matches!(refused, EventLogError::GuardRefused { ref code } if code == "correction_refused"),
        "the refusal is the guard's own, not something the batch raised first: {refused:?}"
    );

    for (digest, _) in blobs(0, 4) {
        assert_eq!(
            store.get_blob(&tenant(), &digest).await.expect("readable"),
            None,
            "{digest}: a refused guard binds none of the group's blobs"
        );
    }
    assert_eq!(
        store.stream_version(&stream("s0")).await.expect("readable"),
        None,
        "a refused guard appends no member of the group"
    );
    assert_eq!(
        fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count),
        0,
        "a refused guard leaves no object file no committed frame names"
    );
}

/// The guarded form, admitted, commits exactly what the unguarded form commits.
///
/// Without this the case above is satisfiable by a guarded form that refuses everything. Two
/// stores, the same digests and the same members, one through each form; the observation is the
/// provider's own complete blob content, so a blob bound under a different key or left out fails
/// it. The guarded commit is then retried and must deduplicate, which is how this says the group
/// really was committed and not merely written.
///
/// **WITHDRAWN IN CORRECTION ROUND 2: the retry below used a refusing guard**, on the assumption
/// that a committed key deduplicates before admission runs again. Review pass 2 showed what that
/// assumption buys an attacker — a caller replaying a key it once committed learned whether a
/// digest was bound and whether bytes matched, with the guard never called — and the coordinator
/// decided admission runs first for any retry that carries a batch. So the retry here uses an
/// admitting guard, and the refusing one now proves the opposite property in
/// `a_retry_carrying_a_batch_runs_admission_before_it_says_anything_about_it`. The port's
/// "retries return the original result without repeating admission" is unchanged for a retry
/// that carries no batch, which is every caller of `append_group_guarded`.
#[tokio::test]
async fn the_guarded_form_commits_what_the_unguarded_form_commits() {
    let unguarded_root = tempfile::tempdir().expect("temp root");
    let unguarded = FileEventStore::open(unguarded_root.path())
        .await
        .expect("opened");
    unguarded
        .append_group_with_blobs(&group("same", 3), &blobs(0, 4))
        .await
        .expect("the unguarded form commits");

    let guarded_root = tempfile::tempdir().expect("temp root");
    let guarded = FileEventStore::open(guarded_root.path())
        .await
        .expect("opened");
    let committed = guarded
        .append_group_guarded_with_blobs(
            &group("same", 3),
            Arc::new(eventlog_core::NoGuard),
            &blobs(0, 4),
        )
        .await
        .expect("an admitted guard commits");
    assert!(!committed.deduplicated, "the first commit is not a retry");
    assert_eq!(committed.appends.len(), 3, "every member is appended");

    for (digest, bytes) in blobs(0, 4) {
        assert_eq!(
            guarded
                .get_blob(&tenant(), &digest)
                .await
                .expect("readable"),
            unguarded
                .get_blob(&tenant(), &digest)
                .await
                .expect("readable"),
            "{digest}: both forms bind the same bytes"
        );
        assert_eq!(
            guarded
                .get_blob(&tenant(), &digest)
                .await
                .expect("readable"),
            Some(bytes),
            "{digest}: and the bytes are the ones handed in"
        );
    }
    assert_eq!(
        fs::read_dir(guarded_root.path().join("blobs")).map_or(0, Iterator::count),
        fs::read_dir(unguarded_root.path().join("blobs")).map_or(0, Iterator::count),
        "neither form leaves an object the other does not"
    );

    let retry = guarded
        .append_group_guarded_with_blobs(
            &group("same", 3),
            Arc::new(eventlog_core::NoGuard),
            &blobs(0, 4),
        )
        .await
        .expect("an admitted retry of a committed key deduplicates");
    assert!(
        retry.deduplicated,
        "the guarded commit really committed: its key is taken"
    );
}

/// A retry carrying a batch runs admission before it says anything about that batch.
///
/// Pass 2's F5, which it could not write a red case for because the case above asserted the
/// opposite. The batch is part of what a retry is asking about, so a caller must not learn
/// whether the store binds a digest — or whether its bytes match — by replaying a key it once
/// committed with the guard switched off. Both answers the dedup path can give are checked here:
/// the refusing guard gets its own refusal, not `IdempotencyMismatch` and not `Ok`.
///
/// The exemption is deliberate and is asserted too: a retry carrying **no** batch is the ordinary
/// group retry, it asks nothing about blobs, and it keeps the port's "retries return the original
/// result without repeating admission or projections" — which
/// `eventlog-conformance/src/atomic_group.rs` asserts by counting guard calls across a retry.
#[tokio::test]
async fn a_retry_carrying_a_batch_runs_admission_before_it_says_anything_about_it() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("probe", 2), &blobs(0, 2))
        .await
        .expect("the first commit");

    // The batch the commit carried: a refusing guard must still refuse it, rather than the store
    // answering `deduplicated` before admission ran.
    let refused = store
        .append_group_guarded_with_blobs(&group("probe", 2), Arc::new(Refuses), &blobs(0, 2))
        .await
        .expect_err("admission runs on a retry that carries a batch");
    assert!(
        matches!(refused, EventLogError::GuardRefused { ref code } if code == "correction_refused"),
        "the refusal is the guard's own, reached before anything is said about the batch: \
         {refused:?}"
    );

    // A batch the commit did not carry: the same refusal, so the refused caller cannot tell the
    // two apart and learns nothing about what is bound.
    let probing = store
        .append_group_guarded_with_blobs(&group("probe", 2), Arc::new(Refuses), &blobs(7, 1))
        .await
        .expect_err("admission runs before the batch is compared at all");
    assert!(
        matches!(probing, EventLogError::GuardRefused { ref code } if code == "correction_refused"),
        "a refused caller cannot distinguish a bound batch from an unbound one: {probing:?}"
    );

    // And the exemption: no batch, no admission, the original result.
    let plain = store
        .append_group_guarded(&group("probe", 2), Arc::new(Refuses))
        .await
        .expect("a retry carrying no batch does not repeat admission");
    assert!(plain.deduplicated, "it is the original result");
}

/// A guard that refuses when it can already see a digest of the batch it is admitting.
struct RefusesWhatItCanAlreadySee(&'static str);

impl Guard for RefusesWhatItCanAlreadySee {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if store.get_blob(self.0).await?.is_some() {
                return Err(EventLogError::GuardRefused {
                    code: "saw_its_own_batch".into(),
                });
            }
            Ok(())
        })
    }
}

/// Admission does not observe the batch of the command it is admitting.
///
/// The pair to the refusal case, and the one that survives the refusal path being tidied up: once
/// a refused transaction disposes of the objects it wrote, binding the batch before admission
/// leaves the same store behind as binding it after, and no assertion about the outcome of a
/// refusal can tell the two orders apart. This can. A guard reads a digest of its own batch
/// through the transaction it is running in; with admission first that digest is unbound, and the
/// group commits. Bind first and the guard is shown content no frame has published and refuses —
/// which is what a real admission guard would do, because content it can see is content it must
/// treat as committed.
#[tokio::test]
async fn admission_does_not_see_the_batch_of_the_command_it_is_admitting() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    let committed = store
        .append_group_guarded_with_blobs(
            &group("unseen", 2),
            Arc::new(RefusesWhatItCanAlreadySee("d0000")),
            &blobs(0, 3),
        )
        .await
        .expect("admission runs before the batch is written, so it sees nothing of it");
    assert!(!committed.deduplicated, "the first commit is not a retry");

    for (digest, bytes) in blobs(0, 3) {
        assert_eq!(
            store.get_blob(&tenant(), &digest).await.expect("readable"),
            Some(bytes),
            "{digest}: and the batch it admitted is bound afterwards"
        );
    }

    // The same guard over a batch whose digest really is committed refuses, which is how this
    // says the guard's read reaches committed content at all rather than always answering None.
    let refused = store
        .append_group_guarded_with_blobs(
            &group("seen", 2),
            Arc::new(RefusesWhatItCanAlreadySee("d0000")),
            &blobs(9, 1),
        )
        .await
        .expect_err("a digest the history carries is visible to admission");
    assert!(
        matches!(refused, EventLogError::GuardRefused { ref code } if code == "saw_its_own_batch"),
        "the guard's read does see committed blobs: {refused:?}"
    );
}

/// A genuine retry of a grouped blob write still deduplicates.
///
/// The correction for the identity defect must not be paid for by refusing the retry that the
/// whole idempotency mechanism exists to serve. Same key, same members, same batch: the second
/// call answers `deduplicated` and binds nothing new, because the first call bound all of it.
#[tokio::test]
async fn a_genuine_retry_of_a_grouped_blob_write_still_deduplicates() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    let first = store
        .append_group_with_blobs(&group("retry", 2), &blobs(0, 3))
        .await
        .expect("the first commit");
    assert!(!first.deduplicated, "the first commit is not a retry");

    let second = store
        .append_group_with_blobs(&group("retry", 2), &blobs(0, 3))
        .await
        .expect("the same request twice is the same request");
    assert!(
        second.deduplicated,
        "an identical retry deduplicates rather than refusing"
    );
    let ranges = |result: &eventlog_core::AppendGroupResult| {
        result
            .appends
            .iter()
            .map(|append| (append.first_version, append.last_version))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ranges(&second),
        ranges(&first),
        "a retry answers with the original version ranges"
    );
    assert!(
        second.appends.iter().all(|append| append.deduplicated),
        "and says of every member that it is the original, not a second append"
    );
    assert_eq!(
        fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count),
        3,
        "a deduplicated retry writes no second copy of an object it already bound"
    );
    for (digest, bytes) in blobs(0, 3) {
        assert_eq!(
            store.get_blob(&tenant(), &digest).await.expect("readable"),
            Some(bytes),
            "{digest}: and everything the first call bound is still bound"
        );
    }
}

/// A retry under a committed key whose batch contradicts the committed content is refused as a
/// content contradiction, not as a fresh binding.
///
/// The two refusals a deduplicating call can owe are different and must stay distinguishable: a
/// digest the history does not carry at all means this is not the request that committed
/// (`IdempotencyMismatch`, the same answer a changed member list gets), while a digest the history
/// carries under different bytes is the contradiction `bind_blobs` already refuses on the fresh
/// path (`Invalid`, "already names different content"). Neither may be answered with `Ok`.
#[tokio::test]
async fn a_retrys_batch_is_held_to_the_content_the_commit_actually_bound() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("held", 2), &blobs(0, 1))
        .await
        .expect("the first commit");

    let contradicted = store
        .append_group_with_blobs(
            &group("held", 2),
            &[("d0000".to_owned(), b"different".to_vec())],
        )
        .await
        .expect_err("a bound digest under different bytes is a contradiction");
    assert!(
        matches!(contradicted, EventLogError::Invalid(ref message)
            if message.contains("already names different content")),
        "the dedup path refuses conflicting content exactly as the fresh path does: \
         {contradicted:?}"
    );

    let unbound = store
        .append_group_with_blobs(&group("held", 2), &blobs(9, 1))
        .await
        .expect_err("a digest no committed frame binds is not this request");
    assert!(
        matches!(unbound, EventLogError::IdempotencyMismatch { ref key } if key == "held"),
        "a batch the commit does not carry is the same refusal a changed member list gets: \
         {unbound:?}"
    );

    assert_eq!(
        store.get_blob(&tenant(), "d0000").await.expect("readable"),
        Some(b"bytes-0".to_vec()),
        "the committed binding is untouched by either refusal"
    );
    assert_eq!(
        fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count),
        1,
        "and neither refusal left an object behind"
    );
}

/// A retry is the same request whatever order it names its batch in, and whatever it repeats.
///
/// Added in correction round 2, holding the claim the digest-set comparison makes. The fresh path
/// collapses a repeated digest — `bind_blobs` binds it once — so a retry that repeats one must not
/// be a different request either, and neither must a retry that lists the same digests in another
/// order. Without the normalization the comparison is on the caller's incidental sequencing, and
/// an importer that rebuilds its batch from a map gets `IdempotencyMismatch` on a retry, whose
/// only recovery is a new key that appends every member a second time.
#[tokio::test]
async fn a_retrys_batch_is_a_set_not_a_sequence() {
    let root = tempfile::tempdir().expect("temp root");
    let store = FileEventStore::open(root.path()).await.expect("opened");

    store
        .append_group_with_blobs(&group("set", 2), &blobs(0, 3))
        .await
        .expect("the first commit");

    let mut reversed = blobs(0, 3);
    reversed.reverse();
    assert!(
        store
            .append_group_with_blobs(&group("set", 2), &reversed)
            .await
            .expect("the same digests in another order is the same request")
            .deduplicated,
        "a batch is a set of digests, not the order the caller happened to hand them in"
    );

    let mut repeated = blobs(0, 3);
    repeated.push(repeated[1].clone());
    assert!(
        store
            .append_group_with_blobs(&group("set", 2), &repeated)
            .await
            .expect("a repeated digest is the same request")
            .deduplicated,
        "the fresh path binds a repeated digest once, so a retry may repeat one too"
    );

    assert_eq!(
        fs::read_dir(root.path().join("blobs")).map_or(0, Iterator::count),
        3,
        "and neither retry wrote a second copy of anything"
    );
}
