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

/// Review L25, the assertion of the shared exercise `run_snapshot_revision_is_monotonic`: within
/// one generation a delayed save of an older fold keeps the newer cache, and a cache of an earlier
/// generation, planted back after the redacting commit disposed of it, is replaced at any revision.
#[tokio::test(flavor = "multi_thread")]
async fn a_delayed_older_save_never_moves_the_cached_revision_back() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let events: Vec<_> = (1..=4).map(|value| event("item.counted", value)).collect();
    store
        .append(
            &stream("m"),
            Expected::NoStream,
            &events,
            &meta("monotonic", &json!({})),
        )
        .await
        .unwrap();
    let observed = store
        .snapshot_generation(&stream("m"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .save_snapshot_checked(&stream("m"), &snapshot(3, "three"), &observed)
            .await
            .unwrap()
    );
    assert!(
        store
            .save_snapshot_checked(&stream("m"), &snapshot(1, "one"), &observed)
            .await
            .unwrap(),
        "an older fold of current history is not a stale observation"
    );
    assert_eq!(
        store.load_snapshot(&stream("m")).await.unwrap(),
        Some(snapshot(3, "three")),
        "the replay point does not move back within a generation"
    );
    assert!(
        store
            .save_snapshot_checked(&stream("m"), &snapshot(4, "four"), &observed)
            .await
            .unwrap()
    );
    assert_eq!(
        store.load_snapshot(&stream("m")).await.unwrap(),
        Some(snapshot(4, "four"))
    );

    let cache = directory.path().join(".cache");
    let files: Vec<_> = std::fs::read_dir(&cache)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    let (path, bytes) = (files[0].clone(), std::fs::read(&files[0]).unwrap());
    store
        .append(
            &stream("m"),
            Expected::Exact(4),
            &[event("item.counted", 5)],
            &meta("monotonic-5", &json!({})),
        )
        .await
        .unwrap();
    store.redact(&stream("m"), 5, "erased").await.unwrap();
    assert_eq!(store.load_snapshot(&stream("m")).await.unwrap(), None);
    std::fs::write(&path, &bytes).unwrap();
    let renewed = store
        .snapshot_generation(&stream("m"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(renewed, observed);
    assert!(
        store
            .save_snapshot_checked(&stream("m"), &snapshot(1, "renewed"), &renewed)
            .await
            .unwrap()
    );
    assert_eq!(
        store.load_snapshot(&stream("m")).await.unwrap(),
        Some(snapshot(1, "renewed")),
        "a new generation replaces the previous generation's cache"
    );
}
