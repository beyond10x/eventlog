//! Independent conformance checks for `story:file-eventlog-verifies-once-per-open`.
//!
//! Each case asserts a promise the unit's own documents make, against the code the same unit
//! wrote. The promises under test are quoted in each case.
//!
//! Every case runs on both on-disk layouts (`layouts::VARIANTS`): `eventlog-file/1`, and
//! `eventlog-file/2` with the damaged frame in the active segment and in a sealed one
//! (story:incremental-history-digest-for-a-resumed-handle).

mod layouts;

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_file::FileEventStore;
use layouts::{VARIANTS, assert_damaged_where, damage_in_place, history, rewrite_history};
use serde_json::json;
use std::fs;

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

/// `docs/design/file-provider.md:16` — "Unknown physical fields, wrong sequence/previous digest,
/// missing committed bytes and damaged committed frames refuse without altering the history."
///
/// The store's `## Outcome` says the same envelope is preserved by this change.
#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_committed_frame_refuses_on_an_open_handle() {
    for variant in VARIANTS {
        let directory = tempfile::tempdir().unwrap();
        let store = variant.open(directory.path()).await;
        store
            .append(
                &stream("a"),
                Expected::NoStream,
                &[event("item.created", 1234)],
                &meta("only-frame", &json!({})),
            )
            .await
            .unwrap();
        variant.settle(&store).await;
        let manifest = fs::read(directory.path().join("manifest.json")).unwrap();
        let damaged = damage_in_place(directory.path(), "1234", "5678");
        assert_damaged_where(variant, directory.path(), &damaged);
        assert_eq!(
            fs::read(directory.path().join("manifest.json")).unwrap(),
            manifest,
            "{variant:?}: the commit authority is untouched; only the committed frame is damaged"
        );
        let read = store.read_stream(&stream("a"), 0, 100).await;
        let served = read
            .as_ref()
            .ok()
            .and_then(|page| page.events.first().map(|e| e.data.clone()));
        assert!(
            read.is_err(),
            "{variant:?}: a damaged committed frame must refuse; the handle served {served:?} \
             while the journal holds the damaged frame"
        );
    }
}

/// The second half of the same sentence: "...refuse **without altering the history**".
///
/// Either the operation refuses, or — if it is accepted — the store it wrote into must still
/// open. A handle that commits a frame on top of a prefix it can no longer validate leaves a
/// store no opener will accept.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate() {
    for variant in VARIANTS {
        let directory = tempfile::tempdir().unwrap();
        let store = variant.open(directory.path()).await;
        store
            .append(
                &stream("a"),
                Expected::NoStream,
                &[event("item.created", 1234)],
                &meta("first-frame", &json!({})),
            )
            .await
            .unwrap();
        variant.settle(&store).await;
        let damaged = damage_in_place(directory.path(), "1234", "5678");
        assert_damaged_where(variant, directory.path(), &damaged);
        let appended = store
            .append(
                &stream("b"),
                Expected::NoStream,
                &[event("item.created", 1)],
                &meta("after-damage", &json!({})),
            )
            .await;
        drop(store);
        let reopened = FileEventStore::open(directory.path()).await;
        assert!(
            appended.is_err() || reopened.is_ok(),
            "{variant:?}: a damaged committed frame must refuse without altering the history; \
             the append was accepted ({:?}) and the store it wrote into no longer opens ({:?})",
            appended.map(|result| result.last_version),
            reopened.err(),
        );
    }
}

/// The story's `## Outcome`: "a chain that does not extend the observed manifest is still
/// refused", and `docs/design/file-provider.md:33` — "An open handle checks that every subsequent
/// history extends the exact head it observed."
///
/// Here the committed prefix the handle observed is replaced, byte for byte, by content that is
/// not the history it verified, while a frame committed after it still names the observed digest.
/// Recomputing the chain from zero — which is what "extends the exact head it observed" means —
/// does not reach the observed digest, so this history does not extend the observed head.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewritten_prefix_does_not_extend_the_head_an_open_handle_observed() {
    for variant in VARIANTS {
        let directory = tempfile::tempdir().unwrap();
        let first = variant.open(directory.path()).await;
        first
            .append(
                &stream("a"),
                Expected::NoStream,
                &[event("item.created", 1)],
                &meta("observed-frame", &json!({})),
            )
            .await
            .unwrap();
        variant.settle(&first).await;
        let observed_length = history(directory.path()).len();

        // Another handle commits one more frame, which chains from the head `first` observed.
        let second = FileEventStore::open(directory.path()).await.unwrap();
        second
            .append(
                &stream("b"),
                Expected::NoStream,
                &[event("item.created", 2)],
                &meta("fresh-frame", &json!({})),
            )
            .await
            .unwrap();
        drop(second);

        // The observed prefix is replaced by something that is not the history `first` verified.
        // The committed byte length of every file and `manifest.json` are left exactly as they
        // were.
        let written = rewrite_history(directory.path(), |bytes| {
            bytes.splice(
                ..observed_length - 1,
                b"x".repeat(observed_length - 1).into_iter(),
            );
        });
        assert!(!written.is_empty(), "{variant:?}: the prefix was rewritten");

        let read = first.read_stream(&stream("a"), 0, 100).await;
        assert!(
            read.is_err(),
            "{variant:?}: a history whose observed prefix is not the history this handle \
             verified does not extend the head it observed and must be refused; the handle \
             served {:?} instead",
            read.map(|page| page.events.len())
        );
    }
}
