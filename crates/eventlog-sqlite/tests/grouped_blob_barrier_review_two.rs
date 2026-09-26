//! Independent-review cases, pass 2, for `story:grouped-blob-writes-take-one-durability-barrier`.
//!
//! Unit 11 put `append_group_guarded_with_blobs` on the `AtomicEventStore` **port** rather than on
//! one provider, and gave it a default implementation. These cases drive that default from the
//! two documents the same unit wrote about it.
//!
//! `docs/design/file-provider.md`, section "The guarded form", in the order the paragraph puts its
//! sentences:
//!
//! > `append_group_guarded_with_blobs` is the same commit under an admission guard, and it is on
//! > the `AtomicEventStore` port rather than on this provider because the caller it exists for —
//! > a migration importing many boundaries at once — holds a trait object. **Admission runs
//! > before a byte of the batch is written, so a refused guard publishes neither the group nor a
//! > blob.** Providers that have not implemented the single-barrier form inherit a default that
//! > writes each blob on its own path first: **correct**, and as slow as it is today.
//!
//! The guarantee is stated for the method, and the default is then called correct. The default
//! writes every blob before admission runs. Nothing in this repository compares the two, because
//! the unit's own suite is in `crates/eventlog-file` and `crates/eventlog-conformance/` is
//! byte-untouched — so the port gained a method that no cross-provider exercise covers, against
//! `AGENTS.md` invariant 2 ("Every provider implements the same applicable conformance
//! contracts").
//!
//! `SqliteEventStore` is one of the two providers that inherit the default. Nothing here edits an
//! implementation file and nothing here changes an existing case.

use std::sync::Arc;

use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, Guard, NoGuard,
    ProjectionStore, StreamAppend, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;

fn tenant() -> TenantId {
    TenantId::new("grouped-blob-port").expect("valid tenant")
}

fn group(key: &str, count: usize) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &serde_json::json!({})),
        appends: (0..count)
            .map(|index| StreamAppend {
                stream: StreamId::new(tenant(), "item", format!("s{index}")).expect("stream"),
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
                code: "review_two_refused".into(),
            })
        })
    }
}

/// A refused guard publishes no blob of the batch, through the port, on every provider.
///
/// The port's whole reason for existing is the caller that holds a trait object: it cannot name
/// the provider it is talking to, so it can only rely on what the port promises. The promise as
/// written is "a refused guard publishes neither the group nor a blob", and the paragraph that
/// makes it goes on to call the default correct. `SqliteEventStore` takes the default.
///
/// This is the whole point of an admission guard on this method. A guard refuses a command
/// because the content must not be published; a default that writes every blob first and asks
/// afterwards has published all of it, and content-addressed storage has no way to take it back —
/// `get_blob` serves the bytes to anyone who can name the digest, and the digests are the ones the
/// refused caller chose. The port's own doc comment calls what the default leaves "orphan blobs,
/// which are non-authority and bind nothing": on this provider a blob row *is* the binding, there
/// is no reference count over it and nothing sweeps it, so the leftovers are not orphans, they are
/// published content.
///
/// **AMENDED BY CORRECTION ROUND 2, and this is the one place either of pass 2's files was
/// changed.** The finding was upheld and the coordinator's fix is *fail closed*: the default now
/// writes nothing, commits nothing and refuses with `eventlog_core::UNAVAILABLE`. The question
/// this case asks is unchanged and its answer is now stronger — nothing of the batch is published
/// — but the refusal a caller sees is no longer the guard's, because the guard is never reached
/// on a provider that does not implement the method. Asserting `GuardRefused` here would have
/// been asserting the behaviour the finding condemned.
///
/// **AMENDED AGAIN BY `EVENTLOG-BLOB-BATCH`.** SQLite now implements the method, inside the one
/// `BEGIN IMMEDIATE` the group already takes, with admission before the batch is bound. The
/// refusal is therefore the guard's own again, and the question this case asks — is any blob of a
/// refused batch published — is asserted exactly as before. What changed is which contract
/// applies, the same switch `eventlog_conformance::run_guarded_group_blobs` makes by probing.
#[tokio::test]
async fn a_refused_guard_publishes_no_blob_of_the_batch() {
    let store = SqliteEventStore::in_memory("port_refused")
        .await
        .expect("opened");

    let refused = store
        .append_group_guarded_with_blobs(&group("refused", 2), Arc::new(Refuses), &blobs(0, 4))
        .await
        .expect_err("the guard refuses");
    assert!(
        matches!(refused, EventLogError::GuardRefused { ref code } if code == "review_two_refused"),
        "the refusal is the guard's own, reached before any byte of the batch: {refused:?}"
    );

    for (digest, _) in blobs(0, 4) {
        assert_eq!(
            store.get_blob(&tenant(), &digest).await.expect("readable"),
            None,
            "{digest}: a refused guard publishes no blob of the batch it refused"
        );
    }
    for index in 0..2 {
        assert_eq!(
            store
                .stream_version(&StreamId::new(tenant(), "item", format!("s{index}")).expect("id"))
                .await
                .expect("readable"),
            None,
            "s{index}: and appends no member of the group"
        );
    }
}

/// `deduplicated: true` means nothing new was written.
///
/// The second half of the same defect, and the one a caller cannot detect. Unit 11's correction
/// round made `Ok` from a grouped blob write mean the batch is durable *and checked* it — on the
/// file provider a committed key carrying a digest the history does not bind is refused with
/// `IdempotencyMismatch` and publishes nothing, which
/// `crates/eventlog-file/tests/grouped_blob_barrier_review_two.rs` asserts as the control for this
/// case. The default has no `verify_bound` step: it writes the batch first, and the dedup branch
/// underneath it then answers `deduplicated: true` — a result that says the original commit
/// already did all of this, returned over content the original commit never saw and this call has
/// just published.
///
/// Either outcome is defensible and this case accepts both: refuse the retry as the file provider
/// does, or answer `deduplicated: false`. What is not defensible is the combination asserted here
/// — a deduplicating success over a digest the deduplicated commit never bound.
///
/// **AMENDED BY CORRECTION ROUND 2.** Upheld, and the coordinator's fix removes the sequence
/// rather than correcting its answer: with the default failing closed there is no commit under
/// this key for a retry to deduplicate against, so the defect is gone by construction. What is
/// asserted now is that construction — the first call refuses, and after it the store carries no
/// blob of the batch and no member of the group, so no later call can be answered
/// `deduplicated: true` over content nothing committed. The original sequence is kept, both calls
/// and all, so the case still fails if a provider ever starts committing here without
/// implementing the guarantee.
///
/// **AMENDED AGAIN BY `EVENTLOG-BLOB-BATCH`.** SQLite now commits here, so the original question
/// is live again and is asserted in its original form: the first call commits `d0000` and both
/// members, and the retry under the same key carrying `d0001` — a batch that commit never bound —
/// is either refused as the file provider refuses it, publishing nothing, or answered
/// `deduplicated: false`. SQLite records the sorted digests each commit bound, so it refuses.
#[tokio::test]
async fn a_deduplicated_return_is_not_given_over_a_batch_the_commit_never_bound() {
    let store = SqliteEventStore::in_memory("port_dedup")
        .await
        .expect("opened");

    let first = store
        .append_group_guarded_with_blobs(&group("dedup", 2), Arc::new(NoGuard), &blobs(0, 1))
        .await
        .expect("the first call commits the group and its batch");
    assert!(!first.deduplicated, "the first commit is not a retry");
    assert_eq!(
        store.get_blob(&tenant(), "d0000").await.expect("readable"),
        Some(b"bytes-0".to_vec()),
        "the first commit bound its batch"
    );

    let retry = store
        .append_group_guarded_with_blobs(&group("dedup", 2), Arc::new(NoGuard), &blobs(1, 1))
        .await;

    match retry {
        Err(EventLogError::IdempotencyMismatch { .. }) => {
            assert_eq!(
                store.get_blob(&tenant(), "d0001").await.expect("readable"),
                None,
                "d0001: a refused retry publishes no byte of the batch it carried"
            );
        }
        Err(other) => panic!("unexpected refusal: {other:?}"),
        Ok(result) => {
            assert_eq!(
                (
                    result.deduplicated,
                    store.get_blob(&tenant(), "d0001").await.expect("readable")
                ),
                (false, Some(b"bytes-1".to_vec())),
                "a deduplicating return says the original commit already bound this batch; \
                 d0001 was published by this call and by no commit before it"
            );
        }
    }

    for index in 0..2 {
        assert_eq!(
            store
                .stream_version(&StreamId::new(tenant(), "item", format!("s{index}")).expect("id"))
                .await
                .expect("readable"),
            Some(1),
            "s{index}: the first commit appended each member once, and the retry appended nothing"
        );
    }
}
