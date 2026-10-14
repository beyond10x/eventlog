//! `story:file-capture-after-a-write-resumes`: a capture taken after a write through the same
//! handle resumes from the handle's verified view.
//!
//! A module of the crate rather than a file under `tests/` because the acceptance is stated in the
//! cost module's counters (`frames_chained`, `frames_reencoded`, `frames_folded`), and that module
//! is test-only scaffolding that an integration test cannot reach.

use crate::{FileEventStore, FileTenantCapture};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture as _, EventStore as _,
    Expected, StreamId, TenantId,
};
use serde_json::json;
use std::{fs, path::Path};

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

async fn append(store: &FileEventStore, stream: &StreamId, key: &str, marker: i64) {
    store
        .append(
            stream,
            Expected::Any,
            &[eventlog_conformance::event("item.changed", marker)],
            &eventlog_conformance::meta(key, &json!({})),
        )
        .await
        .unwrap();
}

/// What a handle that never saw this store observes: the reference a resumed capture must equal.
async fn fresh_observation(root: &Path, tenant: &TenantId) -> eventlog_core::TenantCapture {
    FileTenantCapture::open(root)
        .await
        .unwrap()
        .capture_tenant(tenant, &[], limits())
        .await
        .unwrap()
}

/// The acceptance, with a capture view already in hand: the handle captures, writes three frames
/// through itself, and captures again. Every frame of that history was chained and folded by this
/// handle — the first ones by the open and the first capture, the last three by the writes — so
/// the second capture has none left to chain, re-encode or fold.
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_after_own_writes_charges_no_frame_the_handle_already_verified() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("capwrite-viewed").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    append(&store, &stream, "own-0", 0).await;
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    for index in 1..=3 {
        append(&store, &stream, &format!("own-{index}"), index).await;
    }

    let before = crate::cost::of(root);
    let captured = store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    let spent = crate::cost::of(root) - before;
    assert_eq!(
        (
            spent.frames_chained,
            spent.frames_reencoded,
            spent.frames_folded
        ),
        (0, 0, 0),
        "a capture after this handle's own writes re-verified history it already verified: \
         {spent:?}"
    );
    assert!(
        spent.prefix_bytes_compared > 0,
        "the resumed capture still compares the committed prefix with the bytes it verified: \
         {spent:?}"
    );
    assert_eq!(captured.events.len(), 4);
    assert_eq!(captured, fresh_observation(root, &tenant).await);
}

/// The same with no capture view at all: the first capture after an open and some writes. The
/// open chained and folded the history and every write folded its own frame, so this capture too
/// has nothing left to re-verify. The deferred observation shares the path and is held to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_capture_after_own_writes_charges_no_frame_the_handle_already_verified() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("capwrite-first").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    for index in 0..=3 {
        append(&store, &stream, &format!("own-{index}"), index).await;
    }

    let before = crate::cost::of(root);
    let deferred = store
        .capture_tenant_deferred(&tenant, &[], limits())
        .await
        .unwrap();
    let spent = crate::cost::of(root) - before;
    assert_eq!(
        (
            spent.frames_chained,
            spent.frames_reencoded,
            spent.frames_folded
        ),
        (0, 0, 0),
        "a first capture after this handle's own writes re-verified history it already \
         verified: {spent:?}"
    );
    assert_eq!(deferred.events.len(), 4);
    assert_eq!(
        deferred.events,
        fresh_observation(root, &tenant).await.events
    );
}

/// No trust is added: a committed frame damaged in place after the handle's own write — same
/// length, `manifest.json` untouched — is refused by the next capture exactly as a capture that
/// reread everything refuses it, with and without a capture view in hand, and the refusal leaves
/// the bytes where they are.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_damaged_after_own_writes_is_still_refused_by_the_capture() {
    for viewed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("capwrite-damaged").unwrap();
        let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant).await.unwrap();
        append(&store, &stream, "own-0", 4242).await;
        if viewed {
            store.capture_tenant(&tenant, &[], limits()).await.unwrap();
        }
        append(&store, &stream, "own-1", 1).await;

        let events = root.join("events.jsonl");
        let text = fs::read_to_string(&events).unwrap();
        assert!(text.contains("4242"), "the fixture frame is committed");
        let damaged = text.replacen("4242", "4243", 1);
        assert_eq!(damaged.len(), text.len(), "the damage keeps the length");
        fs::write(&events, &damaged).unwrap();

        for round in 0..2 {
            assert_eq!(
                store.capture_tenant(&tenant, &[], limits()).await,
                Err(CaptureError::Corrupt {
                    material: CaptureMaterial::Journal
                }),
                "viewed {viewed}, round {round}: a capture after an own write served damaged \
                 committed history"
            );
            assert_eq!(
                fs::read_to_string(&events).unwrap(),
                damaged,
                "viewed {viewed}, round {round}: the refusal altered the history"
            );
        }
    }
}
