//! Adversary pass on the removal of the resumed path's snapshot clearing: public-API sequences
//! that would let a resumed handle serve a snapshot of a retired generation, or save one under it.
use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, Snapshot, StreamId, TenantId};
use eventlog_file::FileEventStore;
use serde_json::json;

fn tenant() -> TenantId {
    TenantId::new("snapclear-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}
fn snapshot(version: u64, marker: &str) -> Snapshot {
    Snapshot {
        version,
        state_schema_version: 1,
        state: json!({ "marker": marker }),
        recorded_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
async fn create(store: &FileEventStore, id: &str, key: &str) {
    store
        .append(
            &stream(id),
            Expected::NoStream,
            &[event("item.created", 1)],
            &meta(key, &json!({})),
        )
        .await
        .unwrap();
}

/// Two handles that each hold a cached view mint the generation of one stream at the same time.
/// Were the second to mint its own id, its frame would replace the first handle's generation
/// inside one epoch, which is exactly the cause the removed clearing defended against.
#[tokio::test(flavor = "multi_thread")]
async fn two_handles_minting_one_streams_generation_at_once_agree() {
    let directory = tempfile::tempdir().unwrap();
    let left = FileEventStore::open(directory.path()).await.unwrap();
    let right = FileEventStore::open(directory.path()).await.unwrap();
    for round in 0..32 {
        let id = format!("s{round}");
        create(&left, &id, &format!("create-{round}")).await;
        // Both handles resume (cached views) and race to mint.
        let target = stream(&id);
        let (a, b) = tokio::join!(
            left.snapshot_generation(&target),
            right.snapshot_generation(&target)
        );
        let (a, b) = (a.unwrap().unwrap(), b.unwrap().unwrap());
        assert_eq!(a, b, "round {round}: one epoch minted two generations");
        assert!(
            left.save_snapshot_checked(&stream(&id), &snapshot(1, "left"), &a)
                .await
                .unwrap()
        );
        assert_eq!(
            right.load_snapshot(&stream(&id)).await.unwrap(),
            Some(snapshot(1, "left"))
        );
    }
}

/// A generation obtained before the tenant was forgotten is never accepted for the recreated
/// stream, nor is the snapshot saved under it served, by a fresh or a resumed handle.
#[tokio::test(flavor = "multi_thread")]
async fn a_generation_from_before_forget_is_refused_after_the_stream_is_recreated() {
    let directory = tempfile::tempdir().unwrap();
    let first = FileEventStore::open(directory.path()).await.unwrap();
    create(&first, "a", "before").await;
    let retired = first
        .snapshot_generation(&stream("a"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        first
            .save_snapshot_checked(&stream("a"), &snapshot(1, "before-forget"), &retired)
            .await
            .unwrap()
    );
    first.forget_tenant(&tenant()).await.unwrap();
    create(&first, "a", "after").await;

    let resumed = FileEventStore::open(directory.path()).await.unwrap();
    let writer = FileEventStore::open(directory.path()).await.unwrap();
    create(&writer, "b", "other").await;
    // The resumed handle folds the writer's frame onto its cached view.
    assert_eq!(resumed.load_snapshot(&stream("a")).await.unwrap(), None);
    assert!(
        !resumed
            .save_snapshot_checked(&stream("a"), &snapshot(1, "stale"), &retired)
            .await
            .unwrap()
    );
    let current = resumed
        .snapshot_generation(&stream("a"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(current, retired);
    create(&writer, "c", "again").await;
    assert_eq!(resumed.load_snapshot(&stream("a")).await.unwrap(), None);
}

/// A handle that observed the epoch before another handle's redaction never serves the cache it
/// saved itself: the redacting commit removed it, and the old view is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_handle_of_the_previous_epoch_never_serves_its_own_snapshot_after_redaction() {
    let directory = tempfile::tempdir().unwrap();
    let stale = FileEventStore::open(directory.path()).await.unwrap();
    create(&stale, "a", "first").await;
    let generation = stale
        .snapshot_generation(&stream("a"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        stale
            .save_snapshot_checked(&stream("a"), &snapshot(1, "pre-redaction"), &generation)
            .await
            .unwrap()
    );
    let redactor = FileEventStore::open(directory.path()).await.unwrap();
    redactor.redact(&stream("a"), 1, "erased").await.unwrap();
    assert!(
        !matches!(stale.load_snapshot(&stream("a")).await, Ok(Some(_))),
        "a retired generation's snapshot was served"
    );
    assert!(
        !matches!(
            stale
                .save_snapshot_checked(&stream("a"), &snapshot(1, "late"), &generation)
                .await,
            Ok(true)
        ),
        "a save under a retired generation was accepted"
    );
    let fresh = FileEventStore::open(directory.path()).await.unwrap();
    assert_eq!(fresh.load_snapshot(&stream("a")).await.unwrap(), None);
}
