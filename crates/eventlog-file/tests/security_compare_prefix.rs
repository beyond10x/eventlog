//! Security review of `story:file-eventlog-rechecks-its-prefix-by-comparison`.
//!
//! The unit replaced the in-window SHA-256 re-hash of a resumed handle's committed prefix with a
//! byte-for-byte comparison that reads the prefix in 64 KiB pieces (`same_prefix` in
//! `src/journal.rs`). Its acceptance requires that every refusal the re-hash gave still holds, for
//! the writer's resume and for the strict resume alike. Every refusal case the suite already has
//! damages a store smaller than one piece, so the comparison's later pieces and its last byte are
//! not measured by any of them. These cases damage a committed prefix several pieces long — at the
//! piece boundary, past it, and at its last byte — and require each resumed path to refuse as the
//! complete opener does: the writer's resume, a capture through the same handle (whose view shares
//! the writer's buffer), and a read-only capture handle.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventLogError,
    EventStore, Expected, StreamId, TenantId,
};
use eventlog_file::{FileEventStore, FileTenantCapture};
use serde_json::json;
use std::{fs, path::Path};

/// The size of one read of the comparison in `same_prefix`.
const PIECE: usize = 64 * 1024;

/// The tenant whose history fills the prefix; never captured, so no capture cap applies to it.
fn bulk() -> TenantId {
    TenantId::new("bulk-owner").unwrap()
}

/// The tenant a capture observes: two events, one in the first frame and one in the last.
fn small() -> TenantId {
    TenantId::new("small-owner").unwrap()
}

fn seen() -> StreamId {
    StreamId::new(small(), "item", "seen").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

/// The two files that are the history: the committed bytes and the commit point.
fn history(root: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        fs::read(root.join("events.jsonl")).unwrap(),
        fs::read(root.join("manifest.json")).unwrap(),
    )
}

/// A store whose committed prefix is at least three pieces long. The small tenant's first event is
/// in the first piece and its last event, `7654321`, is in the last frame, past every piece.
async fn large(root: &Path) -> FileEventStore {
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&small()).await.unwrap();
    store
        .append(
            &seen(),
            Expected::NoStream,
            &[event("item.created", 4242)],
            &meta("small-first", &json!({})),
        )
        .await
        .unwrap();
    let many = StreamId::new(bulk(), "item", "many").unwrap();
    let mut version = 0_u64;
    let mut batch = 0_i64;
    while fs::metadata(root.join("events.jsonl")).unwrap().len() < (3 * PIECE) as u64 {
        let events: Vec<_> = (0..400)
            .map(|index| event("item.changed", 1_000_000 + batch * 400 + index))
            .collect();
        store
            .append(
                &many,
                if version == 0 {
                    Expected::NoStream
                } else {
                    Expected::Exact(version)
                },
                &events,
                &meta(&format!("bulk-{batch}"), &json!({})),
            )
            .await
            .unwrap();
        version += 400;
        batch += 1;
    }
    store
        .append(
            &seen(),
            Expected::Exact(1),
            &[event("item.changed", 7_654_321)],
            &meta("small-last", &json!({})),
        )
        .await
        .unwrap();
    let length = usize::try_from(fs::metadata(root.join("events.jsonl")).unwrap().len()).unwrap();
    assert!(
        length > 3 * PIECE,
        "the fixture prefix spans several pieces: {length} bytes"
    );
    store
}

/// Change the committed byte at `at` to a different byte, keeping the file's exact length and
/// leaving `manifest.json` alone: the in-place damage only the prefix re-check can see.
fn damage_at(root: &Path, at: usize) {
    let path = root.join("events.jsonl");
    let mut bytes = fs::read(&path).unwrap();
    let length = bytes.len();
    assert!(at < length, "offset {at} is inside the committed prefix");
    bytes[at] = match bytes[at] {
        b'0'..=b'8' | b'a'..=b'y' | b'A'..=b'Y' => bytes[at] + 1,
        b'9' => b'0',
        b'z' => b'a',
        b'Z' => b'A',
        b'\n' => b' ',
        _ => b'x',
    };
    fs::write(&path, &bytes).unwrap();
    assert_eq!(
        usize::try_from(fs::metadata(&path).unwrap().len()).unwrap(),
        length,
        "the damage keeps the committed length"
    );
}

/// Where the small tenant's last value is written: in the last frame, past every piece.
fn last_value_offset(root: &Path) -> usize {
    let bytes = fs::read(root.join("events.jsonl")).unwrap();
    let at = bytes
        .windows(b"7654321".len())
        .rposition(|window| window == b"7654321")
        .expect("the last value is committed");
    assert!(at > 3 * PIECE, "the last value lies past every piece: {at}");
    at
}

fn integrity(result: &Result<impl std::fmt::Debug, EventLogError>) -> bool {
    matches!(result, Err(EventLogError::Backend(message)) if message.contains("integrity"))
}

/// The writer's resume, inside the window, against damage at `offset(root)`: the next read and
/// the next append both refuse, the append alters nothing, and a fresh complete open refuses the
/// same bytes (so the damage is real, and the refusal is the one the complete opener gives).
async fn writer_refuses(label: &str, offset: impl Fn(&Path) -> usize) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = large(root).await;
    // A read re-establishes the verified view after the handle's own append, so the damage below
    // is met by the resumed comparison and not by a complete reopen.
    store.read_stream(&seen(), 0, 10).await.unwrap();
    damage_at(root, offset(root));
    let damaged = history(root);

    let read = store.read_stream(&seen(), 0, 10).await;
    assert!(
        integrity(&read),
        "{label}: the writer's resume served a prefix damaged in place: {read:?}"
    );
    let appended = store
        .append(
            &seen(),
            Expected::Exact(2),
            &[event("item.changed", 1)],
            &meta(&format!("after-damage-{label}"), &json!({})),
        )
        .await;
    assert!(
        integrity(&appended),
        "{label}: the writer's resume appended onto a prefix damaged in place: {appended:?}"
    );
    assert_eq!(
        history(root),
        damaged,
        "{label}: a refused append altered the history"
    );
    drop(store);
    assert!(
        integrity(&FileEventStore::open(root).await.map(|_| ())),
        "{label}: the fixture damage is not one the complete opener refuses"
    );
}

/// Damage in the second piece of the comparison, at its very first byte.
#[tokio::test(flavor = "multi_thread")]
async fn the_writer_refuses_damage_at_the_first_byte_of_the_second_piece() {
    writer_refuses("first byte of the second piece", |_| PIECE).await;
}

/// Damage at the last byte the first piece reads.
#[tokio::test(flavor = "multi_thread")]
async fn the_writer_refuses_damage_at_the_last_byte_of_the_first_piece() {
    writer_refuses("last byte of the first piece", |_| PIECE - 1).await;
}

/// Damage in a value past every piece, in the frame the handle appended last.
#[tokio::test(flavor = "multi_thread")]
async fn the_writer_refuses_damage_in_the_last_frame_past_every_piece() {
    writer_refuses("last frame", last_value_offset).await;
}

/// Damage at the last committed byte: the final frame's line ending.
#[tokio::test(flavor = "multi_thread")]
async fn the_writer_refuses_damage_at_the_last_committed_byte() {
    writer_refuses("last committed byte", |root| {
        usize::try_from(fs::metadata(root.join("events.jsonl")).unwrap().len()).unwrap() - 1
    })
    .await;
}

/// A capture through the writing handle reuses a view whose bytes share the writer's buffer
/// (`retain_once`), after the handle's own appends extended that buffer in place. Damage past the
/// first piece must refuse that capture, the next read and the next append, and alter nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_sharing_the_writers_bytes_refuses_damage_past_the_first_piece() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = large(root).await;
    store.capture_tenant(&small(), &[], limits()).await.unwrap();
    store
        .append(
            &seen(),
            Expected::Exact(2),
            &[event("item.changed", 3)],
            &meta("own-after-capture", &json!({})),
        )
        .await
        .unwrap();
    store.read_stream(&seen(), 0, 10).await.unwrap();
    // The writer moved the head, so this capture rereads strictly and then shares the writer's
    // buffer; the capture after the damage resumes from that shared view.
    store.capture_tenant(&small(), &[], limits()).await.unwrap();
    damage_at(root, last_value_offset(root));
    let damaged = history(root);

    for round in 0..2 {
        assert_eq!(
            store.capture_tenant(&small(), &[], limits()).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Journal
            }),
            "round {round}: a capture served a view of a prefix damaged in place"
        );
        assert_eq!(
            history(root),
            damaged,
            "round {round}: a capture altered the history"
        );
    }
    let read = store.read_stream(&seen(), 0, 10).await;
    assert!(
        integrity(&read),
        "the writer's resume served a prefix damaged in place after a capture: {read:?}"
    );
    let appended = store
        .append(
            &seen(),
            Expected::Exact(3),
            &[event("item.changed", 4)],
            &meta("after-damage-capture", &json!({})),
        )
        .await;
    assert!(
        integrity(&appended),
        "the writer's resume appended onto a prefix damaged in place after a capture: \
         {appended:?}"
    );
    assert_eq!(
        history(root),
        damaged,
        "a refused append altered the history"
    );
}

/// A read-only capture handle keeps a view of its own; its resume must refuse damage past the first
/// piece exactly as `tests/consistent_capture.rs` requires for a one-piece store.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_capture_refuses_damage_past_the_first_piece() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    drop(large(root).await);
    let handle = FileTenantCapture::open(root).await.unwrap();
    handle
        .capture_tenant(&small(), &[], limits())
        .await
        .unwrap();
    damage_at(root, last_value_offset(root));
    let damaged = history(root);
    for round in 0..2 {
        assert_eq!(
            handle.capture_tenant(&small(), &[], limits()).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Journal
            }),
            "round {round}: a read-only capture served a view of a prefix damaged in place"
        );
        assert_eq!(
            history(root),
            damaged,
            "round {round}: a capture altered the history"
        );
    }
}
