//! Adversary cases for `story:file-eventlog-rechecks-its-prefix-by-comparison`, pass 2.
//!
//! A module of `journal` so that the buffer a view keeps (`Content::retained`) is visible: the
//! acceptance is that one handle holds its verified bytes once, and which buffer each view points
//! at is the only place that can be read.

use crate::FileEventStore;
use eventlog_core::{
    CaptureLimits, ConsistentTenantCapture as _, EventStore as _, Expected, NewEvent, StreamId,
    TenantId,
};
use serde_json::json;
use std::{
    future::Future as _,
    sync::Arc,
    task::{Context, Poll, Waker},
};

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 64,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 65_536,
    }
}

/// The acceptance: "The verified bytes are held once per handle, shared between `Verified` and
/// `capture::Observed` rather than copied."
///
/// A capture that read the whole file strictly leaves its view in a buffer of its own, and
/// `retain_once` (`capture.rs`) moves that view onto the writer's buffer — on a second blocking
/// task that it awaits after the capture's own work, with the writer's view and the new capture
/// view both already in the runtime. A caller that drops the capture there (a
/// `tokio::time::timeout` or a `select!` around `capture_tenant`) leaves both views in place and
/// neither one moved: the handle keeps its committed bytes twice until its next transaction or
/// capture. The transaction path has no such point: it adopts inside `enter`'s blocking work and
/// drops a view it cannot share synchronously.
///
/// The case polls the capture by hand and drops it at the first `Pending` after `retain_once` has
/// cloned the writer's view for its blocking task: the one moment the writer's buffer has a holder
/// that is neither a view nor this case. Eight megabytes of history make that task's comparison
/// long enough that the poll which spawned it cannot also see it finish.
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_dropped_while_it_retains_once_leaves_one_buffer() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("adversary2-dropped-owner").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let bulk = StreamId::new(
        TenantId::new("adversary2-bulk-owner").unwrap(),
        "item",
        "many",
    )
    .unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.changed", 1)],
            &eventlog_conformance::meta("own-0", &json!({})),
        )
        .await
        .unwrap();
    for index in 0..8 {
        let padded =
            NewEvent::new("item.changed", 1, json!({ "pad": "x".repeat(1 << 20) })).unwrap();
        store
            .append(
                &bulk,
                Expected::Any,
                &[padded],
                &eventlog_conformance::meta(&format!("bulk-{index}"), &json!({})),
            )
            .await
            .unwrap();
    }
    let writer = {
        let runtime = store.runtime.lock().await;
        assert!(
            runtime.captured.is_none(),
            "the fixture needs a capture that reads the whole file"
        );
        runtime
            .verified
            .as_ref()
            .expect("the fixture needs the writer's view")
            .content
            .clone()
    };
    // The writer's view and this case's clone.
    let holders = Arc::strong_count(&writer.retained);

    let mut capture = Box::pin(store.capture_tenant(&tenant, &[], limits()));
    let mut context = Context::from_waker(Waker::noop());
    let mut dropped = false;
    loop {
        match capture.as_mut().poll(&mut context) {
            Poll::Ready(outcome) => {
                outcome.unwrap();
                break;
            }
            Poll::Pending if Arc::strong_count(&writer.retained) > holders => {
                drop(capture);
                dropped = true;
                break;
            }
            Poll::Pending => tokio::task::yield_now().await,
        }
    }
    assert!(
        dropped,
        "the capture finished in the poll that began retaining once: nothing to drop at"
    );

    let runtime = store.runtime.lock().await;
    let verified = runtime.verified.as_ref().map(|view| &view.content);
    let captured = crate::capture::reader_bytes(runtime.captured.as_ref());
    // A capture dropped before it stores its view leaves only the writer's: one buffer. Two
    // views must share one; the case asks that and no more.
    assert!(
        verified.is_some(),
        "the case needs the writer's view after the drop"
    );
    if let (Some(verified), Some(captured)) = (verified, captured.as_ref()) {
        assert!(
            verified.shares_bytes_with(captured),
            "a capture dropped while it retained once left the writer's view ({} bytes) and the \
             capture's view ({} bytes) in two buffers",
            verified.length,
            captured.length
        );
    }
}
