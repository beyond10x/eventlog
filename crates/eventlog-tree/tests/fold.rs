//! An unchanged tree reopens from the state `.cache/` kept for it, and any change to its files is
//! replayed and verified instead.
//!
//! A state is kept only once every file is 2 s old, so each case waits that long.

#[macro_use]
mod each_layout;

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{TreeEventStore, TreeLayout, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", id).unwrap()
}

async fn append(root: &Path, layout: TreeLayout, id: &str, key: &str) {
    let store = TreeEventStore::open_in(root, layout).await.unwrap();
    store
        .append(
            &stream(id),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta(key, &json!({})),
        )
        .await
        .unwrap();
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(2_100)).await;
}

fn event_file(root: &Path) -> PathBuf {
    let mut stack = vec![root.join("tenants")];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.components().any(|part| part.as_os_str() == "streams") {
                return path;
            }
        }
    }
    panic!("no event file under {}", root.display());
}

async fn an_unchanged_store_reopens_from_its_kept_state_with_the_same_answers(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;

    let first = TreeEventStore::open_in(root, layout).await.unwrap();
    assert!(
        first.replayed().await,
        "the first open after the writes replays"
    );
    let replayed = first.read_stream(&stream("x"), 0, 10).await.unwrap();
    drop(first);

    let second = TreeEventStore::open_in(root, layout).await.unwrap();
    assert!(
        !second.replayed().await,
        "an unchanged store is read back from .cache/"
    );
    let kept = second.read_stream(&stream("x"), 0, 10).await.unwrap();
    assert_eq!(kept.events.len(), 2);
    for (left, right) in replayed.events.iter().zip(&kept.events) {
        assert_eq!(left.event_id, right.event_id);
        assert_eq!(left.version, right.version);
        assert_eq!(left.data, right.data);
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.parents, right.parents);
    }
    assert_eq!(
        fs::read(root.join(".cache/.gitignore")).unwrap(),
        b"*\n",
        "a cache is never committed"
    );
    assert!(
        verify(root, None).is_empty(),
        "verify does not read .cache/"
    );
}

async fn a_file_damaged_in_place_after_the_state_was_kept_is_replayed_and_refused(
    layout: TreeLayout,
) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;
    drop(TreeEventStore::open_in(root, layout).await.unwrap());
    assert!(
        !TreeEventStore::open_in(root, layout)
            .await
            .unwrap()
            .replayed()
            .await
    );

    // Same length, different bytes: only `ctime` tells this apart from the stamped file.
    let path = event_file(root);
    let mut bytes = fs::read(&path).unwrap();
    let at = bytes
        .windows(5)
        .position(|window| window == b"item.")
        .expect("the event name is in its file");
    bytes[at..at + 4].copy_from_slice(b"ITEM");
    fs::write(&path, &bytes).unwrap();

    let refused = TreeEventStore::open_in(root, layout).await;
    assert!(
        matches!(&refused, Err(eventlog_core::EventLogError::Backend(message)) if message.contains("corrupt")),
        "a changed file is verified, not served from the kept state: {:?}",
        refused.err()
    );
}

async fn a_file_another_writer_added_is_replayed_rather_than_served_from_the_old_state(
    layout: TreeLayout,
) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;
    drop(TreeEventStore::open_in(root, layout).await.unwrap());

    append(root, layout, "y", "two").await;
    let reopened = TreeEventStore::open_in(root, layout).await.unwrap();
    assert!(reopened.replayed().await);
    assert_eq!(
        reopened
            .read_stream(&stream("y"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        2
    );
}

async fn a_kept_state_for_other_projectors_is_not_used(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;
    drop(TreeEventStore::open_in(root, layout).await.unwrap());

    let with_projector = TreeEventStore::open_with_inline(
        root,
        vec![std::sync::Arc::new(eventlog_conformance::SpareProjector)],
    )
    .await
    .unwrap();
    assert!(
        with_projector.replayed().await,
        "a state folded without a projector cannot stand for one folded with it"
    );
}

each_layout!(
    current_thread:
    an_unchanged_store_reopens_from_its_kept_state_with_the_same_answers,
    a_file_damaged_in_place_after_the_state_was_kept_is_replayed_and_refused,
    a_file_another_writer_added_is_replayed_rather_than_served_from_the_old_state,
    a_kept_state_for_other_projectors_is_not_used,
);
