//! Adversary cases for `story:file-eventlog-rechecks-its-prefix-by-comparison`, pass 1.
//!
//! A module of `journal` so that the buffer a view keeps (`Content::retained`) is visible: the
//! story's acceptance is about how many copies of the committed bytes one handle holds, and the
//! number of views holding the writer's buffer is the only place that number can be read.

use super::Journal;
use crate::FileEventStore;
use eventlog_core::{
    CaptureLimits, ConsistentTenantCapture as _, EventStore as _, Expected, StreamId, TenantId,
};
use serde_json::json;
use std::{fs, path::Path, sync::Arc};

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

async fn append(store: &FileEventStore, stream: &StreamId, key: &str) {
    store
        .append(
            stream,
            Expected::Any,
            &[eventlog_conformance::event("item.changed", 1)],
            &eventlog_conformance::meta(key, &json!({})),
        )
        .await
        .unwrap();
}

/// How many views hold the writer's buffer. Between operations the writer's view and the
/// capture's view are the only holders, so with both present 2 is one copy and 1 is two copies.
async fn writer_buffer_holders(store: &FileEventStore) -> usize {
    let runtime = store.runtime.lock().await;
    assert!(
        runtime.captured.is_some(),
        "the fixture needs a capture view"
    );
    let verified = runtime
        .verified
        .as_ref()
        .expect("the fixture needs a writer view");
    Arc::strong_count(&verified.content.retained)
}

fn committed_length(root: &Path) -> u64 {
    fs::metadata(root.join("events.jsonl")).unwrap().len()
}

/// The acceptance: "The verified bytes are held once per handle, shared between `Verified` and
/// `capture::Observed` rather than copied."
///
/// A handle captures; another writer on the same root commits a frame; this handle's next capture
/// takes that frame in and moves the observed head past the writer's view, so the writer's next
/// transaction rereads the whole history into a buffer of its own. From then on the handle holds
/// the committed history twice — the writer's buffer and the one the capture's view still holds —
/// through every append, for as long as no further capture runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_that_rereads_everything_keeps_the_committed_bytes_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("adversary-once-owner").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    append(&store, &stream, "own-0").await;
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    assert_eq!(
        writer_buffer_holders(&store).await,
        2,
        "the control: a capture shares the writer's buffer"
    );

    {
        let other = FileEventStore::open(root).await.unwrap();
        append(&other, &stream, "other-0").await;
    }
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    for index in 1..=5 {
        append(&store, &stream, &format!("own-{index}")).await;
    }
    assert_eq!(
        writer_buffer_holders(&store).await,
        2,
        "five appends after a complete reread, the writer keeps its own copy of the committed \
         bytes beside the one the capture's view holds"
    );
}

/// A capture after this handle's own appends does not resume onto the capture's view today: a
/// write through the handle moves `runtime.observed`, `capture` drops a view whose manifest is not
/// the observed head, and the capture rereads, rechains, re-encodes and refolds the whole history
/// — five frames here (the identity, `own-0` and `own-1..=3`), and no prefix byte compared.
///
/// Pinned as it is, not as it should be: `story:file-capture-after-a-write-resumes` makes this
/// capture resume, and when it lands both assertions flip — it compares exactly the bytes its own
/// view verified (not the longer buffer the writer extended in place) and chains only the three
/// frames appended since.
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_after_own_appends_compares_only_the_bytes_its_view_verified() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("adversary-bound-owner").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    append(&store, &stream, "own-0").await;
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    let observed = committed_length(root);
    for index in 1..=3 {
        append(&store, &stream, &format!("own-{index}")).await;
    }
    let before = crate::cost::of(root);
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    let spent = crate::cost::of(root) - before;
    assert_eq!(
        spent.prefix_bytes_compared, 0,
        "a capture after this handle's own write now resumes: if story \
         file-capture-after-a-write-resumes has landed, flip this to the {observed} bytes its own \
         view verified: {spent:?}"
    );
    assert_eq!(
        spent.frames_chained, 5,
        "a capture after this handle's own write no longer rechains the whole history: if story \
         file-capture-after-a-write-resumes has landed, flip this to the 3 frames appended \
         since: {spent:?}"
    );
}

/// `website/docs/operations.md`: "budget about the committed size of `events.jsonl` per open
/// handle". The writer's buffer after a complete open is the vector the read grew, so what a
/// handle allocates for its committed bytes is its capacity, not the committed length.
#[test]
fn a_complete_open_keeps_about_the_committed_size() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut journal = Journal::open(root.path()).unwrap();
        for index in 0..7 {
            journal
                .append(json!({ "index": index, "pad": "x".repeat(690_000) }))
                .unwrap();
        }
    }
    let journal = Journal::open(root.path()).unwrap();
    let length = journal.content.length;
    let capacity = journal.content.retained.bytes().capacity();
    assert!(
        capacity <= length + length / 8,
        "a complete open keeps {capacity} bytes for a committed prefix of {length}"
    );
}
