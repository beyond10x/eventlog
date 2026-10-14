//! Adversary cases for `story:file-capture-after-a-write-resumes`.
//!
//! The change lets a `FileEventStore` capture whose own view is stale start from the writer's
//! verified view. Every case here asks one question: does a capture through the writing handle
//! ever serve something a fresh strict observation of the same files would not?

use std::{
    fs,
    future::Future,
    path::Path,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use eventlog_conformance::{TALLY, Tally};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventLogError,
    EventStore, Expected, Projector, StreamId, TenantCapture, TenantId,
};
use eventlog_file::{FileEventStore, FileTenantCapture};
use serde_json::json;

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 4096,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 22,
    }
}

async fn append(store: &FileEventStore, stream: &StreamId, key: &str, value: i64) {
    store
        .append(
            stream,
            Expected::Any,
            &[eventlog_conformance::event("item.changed", value)],
            &eventlog_conformance::meta(key, &json!({})),
        )
        .await
        .unwrap();
}

/// What a handle that never saw this store answers: the reference every capture is held to.
async fn fresh(root: &Path, tenant: &TenantId) -> Result<TenantCapture, CaptureError> {
    match FileTenantCapture::open(root).await {
        Ok(handle) => handle.capture_tenant(tenant, &[TALLY], limits()).await,
        Err(refusal) => Err(refusal),
    }
}

/// A small deterministic generator, so a failing interleaving reproduces from its seed.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

/// Two handles on one root, both with the inline tally, interleaving appends, blob bindings and
/// deletions, refused writes, reads, and eager and deferred captures through the writing handle.
/// Every capture through `a` is compared, whole, with a fresh strict observation.
#[tokio::test(flavor = "multi_thread")]
async fn interleaved_writes_and_captures_match_a_fresh_observation() {
    for seed in [7_u64, 11, 4242, 90_001] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("capwrite-adv").unwrap();
        let a = FileEventStore::open(root).await.unwrap();
        let b = FileEventStore::open(root).await.unwrap();
        a.register_inline(Arc::new(Tally) as Arc<dyn Projector>)
            .await
            .unwrap();
        b.register_inline(Arc::new(Tally) as Arc<dyn Projector>)
            .await
            .unwrap();
        a.stream_identity(&tenant).await.unwrap();
        let streams: Vec<StreamId> = (0..3)
            .map(|index| StreamId::new(tenant.clone(), "item", format!("s{index}")).unwrap())
            .collect();
        let mut random = Lcg(seed);
        let mut digests: Vec<String> = Vec::new();
        for step in 0..60_u64 {
            let stream = &streams[usize::try_from(random.next(3)).unwrap()];
            let key = format!("k{seed}-{step}");
            let value = i64::try_from(step).unwrap();
            match random.next(9) {
                0 | 1 => append(&a, stream, &key, value).await,
                2 => append(&b, stream, &key, value).await,
                3 => {
                    let digest = format!("d{seed}-{step}");
                    let writer = if random.next(2) == 0 { &a } else { &b };
                    writer
                        .put_blob(&tenant, &digest, format!("bytes-{step}").as_bytes())
                        .await
                        .unwrap();
                    digests.push(digest);
                }
                4 => {
                    if let Some(digest) = digests.pop() {
                        a.delete_blob(&tenant, &digest).await.unwrap();
                    }
                }
                5 => {
                    // A refusal that records nothing keeps the writer's view and moves the head.
                    let refused = a
                        .append(
                            stream,
                            Expected::Exact(10_000),
                            &[eventlog_conformance::event("item.changed", value)],
                            &eventlog_conformance::meta(&key, &json!({})),
                        )
                        .await;
                    assert!(refused.is_err(), "seed {seed} step {step}: {refused:?}");
                }
                6 => {
                    a.read_stream(stream, 0, 100).await.unwrap();
                }
                7 => {
                    let deferred = a
                        .capture_tenant_deferred(&tenant, &[TALLY], limits())
                        .await
                        .unwrap();
                    let reference = fresh(root, &tenant).await.unwrap();
                    assert_eq!(
                        deferred.events, reference.events,
                        "seed {seed} step {step}: deferred events differ from a fresh observation"
                    );
                    assert_eq!(deferred.projections, reference.projections);
                    let bytes: Vec<(String, Vec<u8>)> = deferred
                        .blobs
                        .iter()
                        .map(|blob| (blob.digest.clone(), blob.bytes().unwrap()))
                        .collect();
                    let expected: Vec<(String, Vec<u8>)> = reference
                        .blobs
                        .iter()
                        .map(|blob| (blob.digest.clone(), blob.bytes.clone()))
                        .collect();
                    assert_eq!(bytes, expected, "seed {seed} step {step}: deferred blobs");
                }
                _ => {
                    let captured = a.capture_tenant(&tenant, &[TALLY], limits()).await;
                    assert_eq!(
                        captured,
                        fresh(root, &tenant).await,
                        "seed {seed} step {step}: a capture through the writing handle differs \
                         from a fresh strict observation"
                    );
                }
            }
        }
        let captured = a.capture_tenant(&tenant, &[TALLY], limits()).await;
        assert_eq!(captured, fresh(root, &tenant).await, "seed {seed}: final");
    }
}

/// The committed file truncated by one byte after the handle's own write: the capture refuses as
/// a fresh strict open refuses, with and without a capture view in hand.
#[tokio::test(flavor = "multi_thread")]
async fn a_history_truncated_after_own_writes_is_refused_like_a_fresh_open() {
    for viewed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("capwrite-truncated").unwrap();
        let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant).await.unwrap();
        append(&store, &stream, "own-0", 0).await;
        if viewed {
            store.capture_tenant(&tenant, &[], limits()).await.unwrap();
        }
        append(&store, &stream, "own-1", 1).await;
        let events = root.join("events.jsonl");
        let length = fs::metadata(&events).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&events)
            .unwrap()
            .set_len(length - 1)
            .unwrap();
        let reference = fresh(root, &tenant).await;
        assert!(reference.is_err(), "the control: a fresh open refuses");
        assert_eq!(
            store.capture_tenant(&tenant, &[], limits()).await,
            reference,
            "viewed {viewed}: a capture after an own write answered a truncated history \
             differently from a fresh open"
        );
    }
}

/// The path this change newly reaches: a capture with no view of its own after an open over a
/// file old enough to be stamped. The capture starts from the writer's stamped view; damage in
/// place moves `ctime`, so the stamp must not vouch and the capture must refuse.
#[tokio::test(flavor = "multi_thread")]
async fn damage_after_a_stamped_open_is_refused_by_a_capture_starting_from_the_writer() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("capwrite-stamped").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    {
        let writer = FileEventStore::open(root).await.unwrap();
        writer.stream_identity(&tenant).await.unwrap();
        append(&writer, &stream, "old-0", 5151).await;
        append(&writer, &stream, "old-1", 1).await;
    }
    // Past the untrusted window, so the next open stamps the bytes it reads.
    tokio::time::sleep(Duration::from_millis(2_300)).await;
    let store = FileEventStore::open(root).await.unwrap();
    store.read_stream(&stream, 0, 10).await.unwrap();
    let events = root.join("events.jsonl");
    let text = fs::read_to_string(&events).unwrap();
    let damaged = text.replacen("5151", "5152", 1);
    assert_eq!(damaged.len(), text.len());
    fs::write(&events, &damaged).unwrap();
    for round in 0..2 {
        assert_eq!(
            store.capture_tenant(&tenant, &[], limits()).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Journal
            }),
            "round {round}: a capture starting from a stamped writer view served damaged history"
        );
    }
}

/// Another handle redacts the tenant's history after this handle wrote: the capture answers the
/// redaction exactly as a fresh observation does.
#[tokio::test(flavor = "multi_thread")]
async fn a_redaction_through_another_handle_after_own_writes_matches_a_fresh_observation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("capwrite-redacted").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    append(&store, &stream, "own-0", 0).await;
    store.capture_tenant(&tenant, &[], limits()).await.unwrap();
    append(&store, &stream, "own-1", 1).await;
    {
        let other = FileEventStore::open(root).await.unwrap();
        other.redact(&stream, 1, "privacy").await.unwrap();
    }
    let reference = match FileTenantCapture::open(root).await {
        Ok(handle) => handle.capture_tenant(&tenant, &[], limits()).await,
        Err(refusal) => Err(refusal),
    };
    assert_eq!(reference, Err(CaptureError::RedactedHistory));
    assert_eq!(
        store.capture_tenant(&tenant, &[], limits()).await,
        reference
    );
}

/// Another handle erases a different tenant after this handle wrote. The epoch moved, so the
/// capture refuses with the per-handle divergence guard and never serves the pre-erasure view it
/// started from — the documented answer, not the fresh one.
#[tokio::test(flavor = "multi_thread")]
async fn an_erasure_through_another_handle_after_own_writes_is_not_served_from_the_writer() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let tenant = TenantId::new("capwrite-kept").unwrap();
    let erased = TenantId::new("capwrite-erased").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    let store = FileEventStore::open(root).await.unwrap();
    store.stream_identity(&tenant).await.unwrap();
    store.stream_identity(&erased).await.unwrap();
    append(&store, &stream, "own-0", 0).await;
    append(
        &store,
        &StreamId::new(erased.clone(), "item", "gone").unwrap(),
        "gone",
        9,
    )
    .await;
    {
        let other = FileEventStore::open(root).await.unwrap();
        other.forget_tenant(&erased).await.unwrap();
    }
    for target in [&tenant, &erased] {
        let outcome = store.capture_tenant(target, &[], limits()).await;
        assert!(
            matches!(
                outcome,
                Err(CaptureError::Store(EventLogError::Backend(ref reason)))
                    if reason.contains("diverged")
            ) || outcome == Err(CaptureError::TenantIdentityMissing),
            "{target:?}: a capture after an erasure through another handle served {outcome:?}"
        );
    }
}

/// An append whose future is dropped after its commit began: the blocking work still commits,
/// the handle's runtime never hears of it, and the next capture must see the frame exactly as a
/// fresh observation does — neither the writer's pre-commit view nor a missing frame.
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_after_a_cancelled_own_write_matches_a_fresh_observation() {
    for viewed in [false, true] {
        for polls in 1..6 {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            let tenant = TenantId::new("capwrite-cancelled").unwrap();
            let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
            let store = FileEventStore::open(root).await.unwrap();
            store
                .register_inline(Arc::new(Tally) as Arc<dyn Projector>)
                .await
                .unwrap();
            store.stream_identity(&tenant).await.unwrap();
            append(&store, &stream, "own-0", 0).await;
            if viewed {
                store
                    .capture_tenant(&tenant, &[TALLY], limits())
                    .await
                    .unwrap();
            }
            append(&store, &stream, "own-1", 1).await;
            {
                let meta = eventlog_conformance::meta("cancelled", &json!({}));
                let events = [eventlog_conformance::event("item.changed", 2)];
                let mut write = Box::pin(store.append(&stream, Expected::Any, &events, &meta));
                let mut context = Context::from_waker(Waker::noop());
                for _ in 0..polls {
                    if let Poll::Ready(outcome) = write.as_mut().poll(&mut context) {
                        outcome.unwrap();
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            // Let any commit the dropped future started finish, so the reference below and the
            // capture observe the same files.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let captured = store.capture_tenant(&tenant, &[TALLY], limits()).await;
            assert_eq!(
                captured,
                fresh(root, &tenant).await,
                "viewed {viewed}, dropped after {polls} polls: the capture differs from a fresh \
                 observation"
            );
        }
    }
}
