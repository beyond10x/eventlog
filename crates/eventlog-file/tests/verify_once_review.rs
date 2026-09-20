//! Independent conformance checks for `story:file-eventlog-verifies-once-per-open`.
//!
//! Each case asserts a promise the unit's own documents make, against the code the same unit
//! wrote. The promises under test are quoted in each case.

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_file::FileEventStore;
use serde_json::json;
use std::{fs, path::Path};

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

/// Replace one same-length run of bytes inside the committed journal, leaving `manifest.json`
/// byte-identical and `events.jsonl` exactly as long as the manifest says it is. This is the
/// damaged-committed-frame state the journal's digest chain exists to detect.
fn damage_in_place(root: &Path, from: &str, to: &str) {
    assert_eq!(
        from.len(),
        to.len(),
        "the damage must not change the length"
    );
    let path = root.join("events.jsonl");
    let before = fs::read(&path).unwrap();
    let text = String::from_utf8(before.clone()).unwrap();
    assert!(
        text.contains(from),
        "fixture byte run {from} is in the frame"
    );
    let after = text.replacen(from, to, 1).into_bytes();
    assert_eq!(after.len(), before.len(), "same committed byte length");
    fs::write(&path, after).unwrap();
}

/// `docs/design/file-provider.md:16` — "Unknown physical fields, wrong sequence/previous digest,
/// missing committed bytes and damaged committed frames refuse without altering the history."
///
/// The store's `## Outcome` says the same envelope is preserved by this change.
#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_committed_frame_refuses_on_an_open_handle() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 1234)],
            &meta("only-frame", &json!({})),
        )
        .await
        .unwrap();
    let manifest = fs::read(directory.path().join("manifest.json")).unwrap();
    damage_in_place(directory.path(), "1234", "5678");
    assert_eq!(
        fs::read(directory.path().join("manifest.json")).unwrap(),
        manifest,
        "the commit authority is untouched; only the committed frame is damaged"
    );
    let read = store.read_stream(&stream("a"), 0, 100).await;
    let served = read
        .as_ref()
        .ok()
        .and_then(|page| page.events.first().map(|e| e.data.clone()));
    assert!(
        read.is_err(),
        "a damaged committed frame must refuse; the handle served {served:?} while events.jsonl \
         holds the damaged frame"
    );
}

/// The second half of the same sentence: "...refuse **without altering the history**".
///
/// Either the operation refuses, or — if it is accepted — the store it wrote into must still
/// open. A handle that commits a frame on top of a prefix it can no longer validate leaves a
/// store no opener will accept.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_handle_does_not_commit_onto_a_history_it_can_no_longer_validate() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    store
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 1234)],
            &meta("first-frame", &json!({})),
        )
        .await
        .unwrap();
    damage_in_place(directory.path(), "1234", "5678");
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
        "a damaged committed frame must refuse without altering the history; the append was \
         accepted ({:?}) and the store it wrote into no longer opens ({:?})",
        appended.map(|result| result.last_version),
        reopened.err(),
    );
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
    let directory = tempfile::tempdir().unwrap();
    let first = FileEventStore::open(directory.path()).await.unwrap();
    first
        .append(
            &stream("a"),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta("observed-frame", &json!({})),
        )
        .await
        .unwrap();
    let observed_length = fs::read(directory.path().join("events.jsonl"))
        .unwrap()
        .len();

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
    // The committed byte length and `manifest.json` are left exactly as they were.
    let path = directory.path().join("events.jsonl");
    let mut bytes = fs::read(&path).unwrap();
    let replacement = b"x".repeat(observed_length - 1);
    bytes.splice(..observed_length - 1, replacement);
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        fs::metadata(&path).unwrap().len(),
        "the committed byte length is unchanged"
    );
    fs::write(&path, bytes).unwrap();

    let read = first.read_stream(&stream("a"), 0, 100).await;
    assert!(
        read.is_err(),
        "a history whose observed prefix is not the history this handle verified does not extend \
         the head it observed and must be refused; the handle served {:?} instead",
        read.map(|page| page.events.len())
    );
}
