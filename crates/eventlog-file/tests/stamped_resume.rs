//! A resumed handle that trusts its file stamp still refuses a history damaged after the stamp was
//! taken.
//!
//! `tests/verify_once_review.rs` damages the journal microseconds after writing it, so every one
//! of its resumes is inside the untrusted window and takes the hashing path. These cases wait the
//! window out first, so the handle holds a trusted stamp when the damage lands, and the stamp is
//! the only thing that can catch it.

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_file::FileEventStore;
use serde_json::json;
use std::{fs, path::Path, time::Duration};

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("durable-owner").unwrap(), "item", id).unwrap()
}

fn damage_in_place(root: &Path, from: &str, to: &str) {
    assert_eq!(
        from.len(),
        to.len(),
        "the damage must not change the length"
    );
    let path = root.join("events.jsonl");
    let text = fs::read_to_string(&path).unwrap();
    assert!(
        text.contains(from),
        "fixture byte run {from} is in the frame"
    );
    fs::write(&path, text.replacen(from, to, 1)).unwrap();
}

/// Longer than the store's untrusted window, so the next verification stamps the file.
const PAST_THE_WINDOW: Duration = Duration::from_millis(2_200);

async fn trusted_handle(root: &Path) -> FileEventStore {
    {
        let writer = FileEventStore::open(root).await.unwrap();
        writer
            .append(
                &stream("a"),
                Expected::NoStream,
                &[event("item.created", 1234)],
                &meta("first-frame", &json!({})),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(PAST_THE_WINDOW).await;
    // This open verifies a file last changed before the window, so it keeps a trusted stamp.
    let store = FileEventStore::open(root).await.unwrap();
    store.read_stream(&stream("a"), 0, 10).await.unwrap();
    store
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_handle_does_not_commit_onto_a_frame_damaged_in_place() {
    let directory = tempfile::tempdir().unwrap();
    let store = trusted_handle(directory.path()).await;
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
        "a trusted stamp let an append land on a damaged frame ({:?}); the store no longer opens \
         ({:?})",
        appended.map(|result| result.last_version),
        reopened.err(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_handle_does_not_serve_a_frame_damaged_in_place() {
    let directory = tempfile::tempdir().unwrap();
    let store = trusted_handle(directory.path()).await;
    damage_in_place(directory.path(), "1234", "5678");
    let read = store.read_stream(&stream("a"), 0, 10).await;
    assert!(
        read.as_ref().map_or(true, |slice| slice.events[0].data
            == json!({ "value": 1234 })),
        "a trusted stamp served damaged bytes: {read:?}"
    );
    if let Ok(slice) = read {
        assert_ne!(
            slice.events[0].data,
            json!({ "value": 5678 }),
            "the damaged value was served"
        );
    }
}
