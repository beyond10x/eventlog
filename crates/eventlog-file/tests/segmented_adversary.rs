//! Adversary cases for the segmented layout, `eventlog-file/2`
//! (story:incremental-history-digest-for-a-resumed-handle).
//!
//! Each case drives the public provider against a state the unit's own guards or documents claim
//! to handle: a segment name already taken when an append seals, a segment file no manifest names
//! while a handle is live, and sealed segments replaced under a handle that trusts their stamps.

use eventlog_conformance::{INSPECTION_LIMITS, event, meta};
use eventlog_core::{
    CaptureLimits, ConsistentTenantCapture, EventStore, Expected, InspectHistory, StreamId,
    TenantId,
};
use eventlog_file::{FileEventStore, FileHistoryInspector, FileLayout, FileTenantCapture};
use serde_json::json;
use std::{fs, path::Path, time::Duration};

const SEAL_EVERY_FRAME: FileLayout = FileLayout::Segmented { seal_at: 1 };
const PAST_THE_WINDOW: Duration = Duration::from_millis(2_200);

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 256,
        max_blobs: 256,
        max_projection_rows: 256,
        max_payload_bytes: 1 << 20,
    }
}

async fn append(store: &FileEventStore, id: &str, value: i64) -> Result<(), String> {
    store
        .append(
            &stream(id),
            Expected::NoStream,
            &[event("item.created", value)],
            &meta(&format!("frame-{id}"), &json!({})),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("{error:?}"))
}

/// `seal_active` refuses a sealed name that is already taken, because "a rename would silently
/// replace it". It refuses after `append.json` names the seal, so the next operation runs
/// `recover_append`, which reads any entry at that name as the active segment this append moved
/// and renames it over `events.jsonl` before verifying anything about it. The committed active
/// segment is destroyed by the recovery of a seal that never happened, and the error that follows
/// says "no history was repaired".
#[tokio::test(flavor = "multi_thread")]
async fn a_sealing_append_refused_by_a_taken_segment_name_keeps_the_active_segment() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open_with_layout(root, SEAL_EVERY_FRAME)
        .await
        .unwrap();
    append(&store, "a", 1).await.unwrap();
    let active = fs::read(root.join("events.jsonl")).unwrap();
    assert!(active.ends_with(b"\n"), "the active segment holds a frame");

    // A file at the name the next seal will take, which no writer made.
    fs::create_dir(root.join("segments")).unwrap();
    fs::write(root.join("segments/0000000000.jsonl"), b"not a segment\n").unwrap();

    let appended = append(&store, "b", 2).await;
    assert!(
        appended.is_err(),
        "precondition: the seal refuses a taken name"
    );
    // Any later operation recovers the refused seal's intent.
    let _ = store.read_stream(&stream("a"), 0, 10).await;
    drop(store);
    let _ = FileEventStore::open(root).await;

    assert_eq!(
        fs::read(root.join("events.jsonl")).unwrap(),
        active,
        "recovery of a refused seal replaced the committed active segment with {:?}",
        String::from_utf8_lossy(&fs::read(root.join("events.jsonl")).unwrap()),
    );
}

/// The complete opener, `open_strict` and `inspect` refuse a `segments/` entry the manifest does
/// not name. A resumed writer does not ask, so a live handle keeps committing onto a store that,
/// the moment it drops, no opener accepts.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_writer_does_not_append_onto_a_store_with_an_unexplained_segment() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open_with_layout(root, SEAL_EVERY_FRAME)
        .await
        .unwrap();
    append(&store, "a", 1).await.unwrap();
    append(&store, "b", 2).await.unwrap();
    assert!(root.join("segments/0000000000.jsonl").exists());
    fs::write(root.join("segments/stray"), b"").unwrap();

    let appended = append(&store, "c", 3).await;
    drop(store);
    let reopened = FileEventStore::open(root).await;
    assert!(
        appended.is_err() || reopened.is_ok(),
        "a resumed writer committed ({appended:?}) onto a store the complete opener refuses ({:?})",
        reopened.err(),
    );
}

/// The reader's half of the same gap: a capture handle that resumes serves a store which a fresh
/// strict open of the same directory refuses as damaged.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_capture_does_not_serve_a_store_its_strict_open_refuses() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open_with_layout(root, SEAL_EVERY_FRAME)
        .await
        .unwrap();
    store.stream_identity(&tenant()).await.unwrap();
    append(&store, "a", 1).await.unwrap();
    append(&store, "b", 2).await.unwrap();
    drop(store);
    let capture = FileTenantCapture::open(root).await.unwrap();
    capture
        .capture_tenant(&tenant(), &[], limits())
        .await
        .unwrap();
    fs::write(root.join("segments/stray"), b"").unwrap();

    let resumed = capture.capture_tenant(&tenant(), &[], limits()).await;
    let fresh = FileTenantCapture::open(root).await;
    assert!(
        resumed.is_err() || fresh.is_ok(),
        "a resumed capture served {} events from a store a strict open refuses ({:?})",
        resumed.map_or(0, |captured| captured.events.len()),
        fresh.err(),
    );
}

/// Control for the strict readers: an unexplained segment refuses the inspector and a fresh
/// capture open, as the design says.
#[tokio::test(flavor = "multi_thread")]
async fn an_unexplained_segment_refuses_the_inspector_and_a_fresh_capture() {
    for name in ["0000000005.jsonl", "stray", "0000000001.jsonl.tmp"] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let store = FileEventStore::open_with_layout(root, SEAL_EVERY_FRAME)
            .await
            .unwrap();
        append(&store, "a", 1).await.unwrap();
        append(&store, "b", 2).await.unwrap();
        drop(store);
        fs::write(root.join("segments").join(name), b"").unwrap();
        assert!(
            FileHistoryInspector::new(root)
                .inspect_history(&tenant(), INSPECTION_LIMITS)
                .await
                .is_err(),
            "{name}: inspected"
        );
        assert!(
            FileTenantCapture::open(root).await.is_err(),
            "{name}: captured"
        );
    }
}

/// A handle whose two sealed segments carry trusted stamps.
async fn trusted_sealed_handle(root: &Path) -> FileEventStore {
    {
        let writer = FileEventStore::open_with_layout(root, SEAL_EVERY_FRAME)
            .await
            .unwrap();
        append(&writer, "a", 1234).await.unwrap();
        append(&writer, "b", 1).await.unwrap();
        append(&writer, "c", 1).await.unwrap();
    }
    tokio::time::sleep(PAST_THE_WINDOW).await;
    let store = FileEventStore::open(root).await.unwrap();
    store.read_stream(&stream("a"), 0, 10).await.unwrap();
    store
}

/// The stamp rule's trust: a sealed segment replaced by another inode of the same size carrying
/// the same `mtime` is not served, because `ctime` and the inode moved.
#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_sealed_segment_replaced_at_its_size_and_mtime_is_not_served() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = trusted_sealed_handle(root).await;
    let first = root.join("segments/0000000000.jsonl");
    let original = fs::read(&first).unwrap();
    let modified = fs::metadata(&first).unwrap().modified().unwrap();
    let text = String::from_utf8(original.clone()).unwrap();
    assert!(text.contains("1234"));
    let forged = text.replacen("1234", "5678", 1);
    assert_eq!(forged.len(), original.len());
    let staged = root.join("forged.tmp");
    fs::write(&staged, forged.as_bytes()).unwrap();
    fs::File::options()
        .write(true)
        .open(&staged)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    fs::rename(&staged, &first).unwrap();
    assert_eq!(fs::metadata(&first).unwrap().modified().unwrap(), modified);

    let read = store.read_stream(&stream("a"), 0, 10).await;
    assert!(
        read.as_ref()
            .map_or(true, |page| page.events[0].data == json!({"value": 1234})),
        "a forged sealed segment was served: {read:?}"
    );
    assert!(read.is_err(), "the forged segment was not refused");
}

/// A sealed segment replaced by a symbolic link to an identical copy, or by a directory, is not a
/// segment any writer sealed: a trusted handle and the complete opener refuse it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_segment_replaced_by_a_link_or_a_directory_is_refused() {
    for replacement in ["link", "directory"] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let store = trusted_sealed_handle(root).await;
        let first = root.join("segments/0000000000.jsonl");
        let copy = root.join("copy.jsonl");
        fs::copy(&first, &copy).unwrap();
        fs::remove_file(&first).unwrap();
        match replacement {
            "link" => std::os::unix::fs::symlink(&copy, &first).unwrap(),
            "directory" => fs::create_dir(&first).unwrap(),
            _ => unreachable!(),
        }
        assert!(
            store.read_stream(&stream("a"), 0, 10).await.is_err(),
            "{replacement}: the trusted handle served it"
        );
        assert!(
            append(&store, "d", 1).await.is_err(),
            "{replacement}: appended"
        );
        drop(store);
        assert!(
            FileEventStore::open(root).await.is_err(),
            "{replacement}: opened"
        );
    }
}
