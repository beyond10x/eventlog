//! The segmented layout, `eventlog-file/2` (story:incremental-history-digest-for-a-resumed-handle,
//! decision-blocker:on-disk-layout-changes option A).
//!
//! The decision: a store keeps `eventlog-file/1` unless its owner opts in; every release keeps
//! reading `eventlog-file/1`; no migration and no refusal of the old layout. And the story: a
//! sealed segment whose stamp a handle trusts is not re-read, and is trusted exactly as an
//! `eventlog-file/1` handle trusts its one file — so damage to it after the stamp was taken is
//! still refused.

mod layouts;

use eventlog_conformance::{event, meta};
use eventlog_core::{EventLogError, EventStore, Expected, StreamId, TenantId};
use eventlog_file::{FileEventStore, FileLayout};
use layouts::{SEGMENTED, damage_in_place, history, journal_files};
use serde_json::{Value, json};
use std::{fs, path::Path, time::Duration};

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("durable-owner").unwrap(), "item", id).unwrap()
}

fn manifest_of(root: &Path) -> Value {
    serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap()
}

async fn append(store: &FileEventStore, id: &str, value: i64) {
    store
        .append(
            &stream(id),
            Expected::NoStream,
            &[event("item.created", value)],
            &meta(&format!("frame-{id}"), &json!({})),
        )
        .await
        .unwrap();
}

/// No owner opted in: the manifest is `eventlog-file/1` with exactly the fields it has always
/// had, and nothing is written under `segments/`.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_keeps_the_single_layout_unless_its_owner_opts_in() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    for id in ["a", "b", "c"] {
        append(&store, id, 1).await;
    }
    let manifest = manifest_of(directory.path());
    assert_eq!(manifest["format"], "eventlog-file/1");
    let mut fields: Vec<_> = manifest.as_object().unwrap().keys().cloned().collect();
    fields.sort();
    assert_eq!(
        fields,
        ["digest", "epoch", "format", "length", "sequence", "store"]
    );
    assert!(!directory.path().join("segments").exists());

    // Opening it with the opt-in does not migrate it.
    drop(store);
    let reopened = FileEventStore::open_with_layout(directory.path(), SEGMENTED)
        .await
        .unwrap();
    append(&reopened, "d", 1).await;
    assert_eq!(manifest_of(directory.path())["format"], "eventlog-file/1");
    assert!(!directory.path().join("segments").exists());
}

/// An owner that opts in gets `eventlog-file/2`; the seal size is the store's, so a handle opened
/// without the opt-in keeps sealing; and the committed history is the concatenation of the files.
#[tokio::test(flavor = "multi_thread")]
async fn an_opted_in_store_seals_its_segments_for_every_writer() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open_with_layout(directory.path(), SEGMENTED)
        .await
        .unwrap();
    append(&store, "a", 1).await;
    append(&store, "b", 2).await;
    drop(store);
    let plain = FileEventStore::open(directory.path()).await.unwrap();
    append(&plain, "c", 3).await;
    let manifest = manifest_of(directory.path());
    assert_eq!(manifest["format"], "eventlog-file/2");
    assert_eq!(manifest["segments"]["seal_at"], 1);
    let sealed: Vec<u64> = manifest["segments"]["sealed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|length| length.as_u64().unwrap())
        .collect();
    assert_eq!(sealed.len(), 2, "every frame but the last is sealed");
    let files = journal_files(directory.path());
    assert_eq!(files.len(), 3);
    for (file, length) in files.iter().zip(&sealed) {
        assert_eq!(fs::metadata(file).unwrap().len(), *length);
    }
    assert_eq!(
        history(directory.path()).len() as u64,
        manifest["length"].as_u64().unwrap()
    );
    // Another handle, and a fresh open, read every segment.
    let fresh = FileEventStore::open(directory.path()).await.unwrap();
    for (id, value) in [("a", 1), ("b", 2), ("c", 3)] {
        let page = fresh.read_stream(&stream(id), 0, 10).await.unwrap();
        assert_eq!(page.events[0].data, json!({ "value": value }), "{id}");
        let page = plain.read_stream(&stream(id), 0, 10).await.unwrap();
        assert_eq!(page.events[0].data, json!({ "value": value }), "{id}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_segmented_layout_that_seals_at_zero_bytes_is_refused_and_creates_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let refused =
        FileEventStore::open_with_layout(&root, FileLayout::Segmented { seal_at: 0 }).await;
    assert!(matches!(refused, Err(EventLogError::Invalid(_))));
    assert!(!root.exists());
}

/// A sealed segment missing, cut short, or joined by a file the manifest does not name is not
/// the committed history: the complete opener refuses and leaves the evidence where it was.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_short_or_unexplained_segment_is_refused_without_altering_the_history() {
    for damage in ["missing", "short", "unexplained", "unexplained-name"] {
        let directory = tempfile::tempdir().unwrap();
        let store = FileEventStore::open_with_layout(directory.path(), SEGMENTED)
            .await
            .unwrap();
        for id in ["a", "b", "c"] {
            append(&store, id, 1).await;
        }
        drop(store);
        let first = directory.path().join("segments/0000000000.jsonl");
        match damage {
            "missing" => fs::remove_file(&first).unwrap(),
            "short" => {
                let bytes = fs::read(&first).unwrap();
                fs::write(&first, &bytes[..bytes.len() - 1]).unwrap();
            }
            "unexplained" => {
                fs::write(directory.path().join("segments/0000000009.jsonl"), b"").unwrap();
            }
            "unexplained-name" => {
                fs::write(directory.path().join("segments/stray"), b"").unwrap();
            }
            _ => unreachable!(),
        }
        let manifest_before = fs::read(directory.path().join("manifest.json")).unwrap();
        let files = journal_files(directory.path());
        let bytes: Vec<Vec<u8>> = files
            .iter()
            .map(|path| fs::read(path).unwrap_or_default())
            .collect();
        assert!(
            FileEventStore::open(directory.path()).await.is_err(),
            "{damage}: the store opened"
        );
        assert_eq!(
            fs::read(directory.path().join("manifest.json")).unwrap(),
            manifest_before,
            "{damage}"
        );
        assert_eq!(journal_files(directory.path()), files, "{damage}");
        for (path, before) in files.iter().zip(&bytes) {
            assert_eq!(&fs::read(path).unwrap_or_default(), before, "{damage}");
        }
    }
}

/// Longer than the store's untrusted window, so the next verification stamps the files.
const PAST_THE_WINDOW: Duration = Duration::from_millis(2_200);

/// A handle whose sealed segments carry trusted stamps, and whose active segment it has just
/// written itself, so it holds no trusted stamp for that one.
async fn trusted_sealed_handle(root: &Path) -> FileEventStore {
    {
        let writer = FileEventStore::open_with_layout(root, SEGMENTED)
            .await
            .unwrap();
        append(&writer, "a", 1234).await;
        append(&writer, "b", 1).await;
    }
    tokio::time::sleep(PAST_THE_WINDOW).await;
    // This open verifies files last changed before the window, so it keeps their stamps.
    let store = FileEventStore::open(root).await.unwrap();
    append(&store, "c", 1).await;
    store.read_stream(&stream("a"), 0, 10).await.unwrap();
    store
}

/// The stamp of a sealed segment vouches for its bytes only while the file holds still: a write
/// moves its `ctime`, and the damaged frame is refused rather than served from the kept fold.
#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_sealed_segment_damaged_in_place_is_not_served() {
    let directory = tempfile::tempdir().unwrap();
    let store = trusted_sealed_handle(directory.path()).await;
    let damaged = damage_in_place(directory.path(), "1234", "5678");
    assert!(damaged.starts_with(directory.path().join("segments")));
    let read = store.read_stream(&stream("a"), 0, 10).await;
    assert!(
        read.is_err(),
        "a trusted sealed segment served a frame damaged after its stamp was taken: {read:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_sealed_segment_damaged_in_place_is_not_appended_onto() {
    let directory = tempfile::tempdir().unwrap();
    let store = trusted_sealed_handle(directory.path()).await;
    damage_in_place(directory.path(), "1234", "5678");
    let appended = store
        .append(
            &stream("d"),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta("after-damage", &json!({})),
        )
        .await;
    drop(store);
    let reopened = FileEventStore::open(directory.path()).await;
    assert!(
        appended.is_err() || reopened.is_ok(),
        "a trusted sealed segment let an append land after a damaged frame ({:?}); the store no \
         longer opens ({:?})",
        appended.map(|result| result.last_version),
        reopened.err(),
    );
    assert!(appended.is_err(), "the append was not refused");
}

/// The control for both cases above: undamaged, the same handle serves and appends, and the store
/// it leaves opens.
#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_sealed_segment_left_alone_serves_and_appends() {
    let directory = tempfile::tempdir().unwrap();
    let store = trusted_sealed_handle(directory.path()).await;
    let page = store.read_stream(&stream("a"), 0, 10).await.unwrap();
    assert_eq!(page.events[0].data, json!({ "value": 1234 }));
    append(&store, "d", 1).await;
    drop(store);
    let reopened = FileEventStore::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.stream_version(&stream("d")).await.unwrap(),
        Some(1)
    );
}
