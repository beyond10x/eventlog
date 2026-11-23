//! An unchanged tree reopens from the state `.cache/` kept for it, and any change to its files is
//! replayed and verified instead.
//!
//! A state is kept only once every file is 2 s old, so each case waits that long.

#[macro_use]
mod each_layout;

use eventlog_conformance::{event, meta};
use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, NewEvent, ProjectionSpec, ProjectionStore,
    Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_tree::{TreeEventStore, TreeLayout, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Every file under `.cache/` whose bytes contain `marker`.
fn cached_copies(root: &Path, marker: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.join(".cache")];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if fs::read(&path)
                .unwrap()
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
            {
                found.push(path);
            }
        }
    }
    found
}

fn kept_states(root: &Path) -> usize {
    fs::read_dir(root.join(".cache").join("state")).map_or(0, Iterator::count)
}

const MARKER: &str = "zq-removed-body-marker";

/// A store whose one event carries [`MARKER`], opened after it settled, so that the open kept a
/// state holding the marker.
async fn kept_with_marker(root: &Path, layout: TreeLayout) -> TreeEventStore {
    TreeEventStore::open_in(root, layout)
        .await
        .unwrap()
        .append(
            &stream("x"),
            Expected::NoStream,
            &[NewEvent::new("item.received", 1, json!({ "value": MARKER })).unwrap()],
            &meta("marked", &json!({})),
        )
        .await
        .unwrap();
    settle().await;
    let store = TreeEventStore::open_in(root, layout).await.unwrap();
    assert!(store.replayed().await);
    assert!(
        !cached_copies(root, MARKER).is_empty(),
        "the settled replay kept no state to remove, so this case proves nothing"
    );
    store
}

async fn an_erasure_leaves_no_kept_state_holding_the_tenants_events(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = kept_with_marker(root, layout).await;
    store.forget_tenant(stream("x").tenant()).await.unwrap();
    assert_eq!(
        cached_copies(root, MARKER),
        Vec::<PathBuf>::new(),
        "a kept state still holds the erased tenant's events"
    );
}

async fn a_redaction_leaves_no_kept_state_holding_the_body(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = kept_with_marker(root, layout).await;
    store.redact(&stream("x"), 1, "erased").await.unwrap();
    assert_eq!(
        cached_copies(root, MARKER),
        Vec::<PathBuf>::new(),
        "a kept state still holds the redacted body"
    );
}

async fn a_replay_keeps_no_state_while_another_writer_holds_the_lock(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;

    // A writer holding the lock may be removing content right now; a state replayed from the
    // files before that removal must not be put back after it.
    let lock = fs::OpenOptions::new()
        .write(true)
        .open(root.join(".lock"))
        .unwrap();
    lock.lock().unwrap();
    let store = TreeEventStore::open_in(root, layout).await.unwrap();
    assert!(store.replayed().await);
    assert_eq!(
        kept_states(root),
        0,
        "a state was kept while another writer held the lock"
    );
    drop(store);
    drop(lock);

    drop(TreeEventStore::open_in(root, layout).await.unwrap());
    assert_eq!(
        kept_states(root),
        2,
        "a free lock keeps the image and its state"
    );
}

static CONFIGURED_ROWS: ProjectionSpec = ProjectionSpec {
    name: "configured_rows",
    indexed: &[],
};

/// A projector whose rows depend on its configuration, `label`, under one name and one
/// executable. It vouches for its output with a fingerprint only when `fingerprinted`.
struct Configured {
    label: &'static str,
    fingerprinted: bool,
}

impl Projector for Configured {
    fn name(&self) -> &'static str {
        "configured"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&CONFIGURED_ROWS)
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(
                    &CONFIGURED_ROWS,
                    &event.tenant,
                    "row",
                    &json!({ "label": self.label }),
                )
                .await
        })
    }
    fn fingerprint(&self) -> Option<String> {
        self.fingerprinted.then(|| self.label.to_owned())
    }
}

/// Open the store at `root` with a [`Configured`] projector, and return whether it replayed and
/// the label its row holds.
async fn open_configured(
    root: &Path,
    layout: TreeLayout,
    label: &'static str,
    fingerprinted: bool,
) -> (bool, Option<serde_json::Value>) {
    let store = TreeEventStore::open_in_with_inline(
        root,
        layout,
        vec![Arc::new(Configured {
            label,
            fingerprinted,
        })],
    )
    .await
    .unwrap();
    let row = store
        .projection_get(&CONFIGURED_ROWS, stream("x").tenant(), "row")
        .await
        .unwrap();
    (store.replayed().await, row.map(|row| row["label"].clone()))
}

async fn a_projector_reconfigured_under_one_name_is_folded_again(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;
    let (replayed, label) = open_configured(root, layout, "first", false).await;
    assert!(replayed);
    assert_eq!(label, Some(json!("first")));

    let (replayed, label) = open_configured(root, layout, "second", false).await;
    assert_eq!(
        label,
        Some(json!("second")),
        "a warm open served the rows of the projector's earlier configuration"
    );
    assert!(
        replayed,
        "a projector with no fingerprint was not folded again"
    );
}

async fn a_fingerprinted_projector_reopens_warm_until_its_fingerprint_changes(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    append(root, layout, "x", "one").await;
    settle().await;
    assert_eq!(
        open_configured(root, layout, "first", true).await,
        (true, Some(json!("first")))
    );
    assert_eq!(
        open_configured(root, layout, "first", true).await,
        (false, Some(json!("first"))),
        "an unchanged fingerprint did not reopen from the kept state"
    );
    assert_eq!(
        open_configured(root, layout, "second", true).await,
        (true, Some(json!("second"))),
        "a changed fingerprint reopened from the state folded under the old one"
    );
}

each_layout!(
    current_thread:
    an_unchanged_store_reopens_from_its_kept_state_with_the_same_answers,
    a_file_damaged_in_place_after_the_state_was_kept_is_replayed_and_refused,
    a_file_another_writer_added_is_replayed_rather_than_served_from_the_old_state,
    a_kept_state_for_other_projectors_is_not_used,
    an_erasure_leaves_no_kept_state_holding_the_tenants_events,
    a_redaction_leaves_no_kept_state_holding_the_body,
    a_replay_keeps_no_state_while_another_writer_holds_the_lock,
    a_projector_reconfigured_under_one_name_is_folded_again,
    a_fingerprinted_projector_reopens_warm_until_its_fingerprint_changes,
);
