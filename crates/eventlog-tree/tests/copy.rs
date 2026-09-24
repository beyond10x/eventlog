//! `copy` linearizes a tree store into a linear SQL store (design § 9.6): groups in § 6.3
//! position order, every stream gapless, and each event's tree `{version, digest, parents}` kept
//! in an origin map beside the event rather than in its body.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup, BlobWrite,
    BranchableEventStore, EventLogError, EventStore, Expected, HeadSetDigest, NoGuard,
    RecordedEvent, StreamAppend, StreamId, TenantId, redaction_tombstone,
};
use eventlog_sqlite::{OriginMap, SqliteEventStore};
use eventlog_tree::{CopyReport, TreeEventStore, copy};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

fn stream(owner: &str, id: &str) -> StreamId {
    StreamId::new(tenant(owner), "item", id).unwrap()
}

/// Every history file under `from` added to `into`, as Git leaves a merge of two branches that
/// added different files. The writer lock and the fold cache are not history and are not merged.
fn merge_into(from: &Path, into: &Path) {
    for entry in walk(from) {
        let relative = entry.strip_prefix(from).unwrap();
        if relative == Path::new(".lock") || relative.starts_with(".cache") {
            continue;
        }
        let target = into.join(relative);
        if let Ok(existing) = fs::read(&target) {
            assert_eq!(
                existing,
                fs::read(&entry).unwrap(),
                "{} conflicts",
                relative.display()
            );
            continue;
        }
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(&entry, &target).unwrap();
    }
}

fn walk(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

async fn feed(store: &dyn EventStore, owner: &str) -> Vec<RecordedEvent> {
    store
        .read_feed(&tenant(owner), 0, 1000)
        .await
        .unwrap()
        .events
}

/// The copied events with the tree's `{version, digest, parents}` put back from the origin map,
/// so they can be compared with the tree's own. Panics when an event has no origin or its origin
/// names another version: an origin map that does not cover the copy is the defect.
fn with_origins(events: &[RecordedEvent], origins: &OriginMap) -> Vec<RecordedEvent> {
    events
        .iter()
        .map(|event| {
            let key = (
                event.stream_type.clone(),
                event.stream_id.clone(),
                event.version,
            );
            let origin = origins
                .get(&key)
                .unwrap_or_else(|| panic!("the copy recorded no origin for {key:?}"));
            assert_eq!(
                origin.version, event.version,
                "the copy renumbered {key:?} away from the tree's version"
            );
            RecordedEvent {
                digest: Some(origin.digest.clone()),
                parents: origin.parents.clone(),
                ..event.clone()
            }
        })
        .collect()
}

/// The tree records no redaction instant, so a redaction replays at the instant its replay ran,
/// in the tree's own engine and in the copy alike. Whether an event is redacted is what compares.
fn instant_free(events: Vec<RecordedEvent>) -> Vec<RecordedEvent> {
    events
        .into_iter()
        .map(|event| RecordedEvent {
            redacted_at: event.redacted_at.map(|_| time::OffsetDateTime::UNIX_EPOCH),
            ..event
        })
        .collect()
}

fn blob_group() -> AppendGroup {
    AppendGroup {
        tenant: tenant("tenant-a"),
        appends: vec![
            StreamAppend {
                stream: stream("tenant-a", "left"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 1)],
            },
            StreamAppend {
                stream: stream("tenant-a", "right"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 2), event("item.indexed", 3)],
            },
        ],
        meta: meta("group-1", &json!({ "group": 1 })),
    }
}

fn blobs() -> Vec<(String, Vec<u8>)> {
    vec![("blob-1".to_owned(), b"bytes".to_vec())]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tree_copied_into_sqlite_holds_every_event_blob_and_receipt_modulo_the_origin_map() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path()).await.unwrap();
    for owner in ["tenant-a", "tenant-b"] {
        tree.stream_identity(&tenant(owner)).await.unwrap();
    }
    let body = json!({ "command": "one" });
    let appended = tree
        .append(
            &stream("tenant-a", "x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("key-1", &body),
        )
        .await
        .unwrap();
    let grouped = tree
        .append_group_guarded_with_blobs(&blob_group(), Arc::new(NoGuard), &blobs())
        .await
        .unwrap();
    tree.put_blob(&tenant("tenant-a"), "orphan-1", b"alone")
        .await
        .unwrap();
    tree.append(
        &stream("tenant-b", "y"),
        Expected::NoStream,
        &[event("item.received", 7)],
        &meta("key-b", &json!({})),
    )
    .await
    .unwrap();
    tree.redact(&stream("tenant-a", "x"), 1, "erased")
        .await
        .unwrap();

    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    let report = copy(&tree, &target).await.unwrap();
    assert_eq!(
        report,
        CopyReport {
            tenants: 2,
            groups: 3,
            events: 6,
            blobs: 2,
            redactions: 1,
        },
        "the copy did not report what it wrote"
    );

    for owner in ["tenant-a", "tenant-b"] {
        assert_eq!(
            target.stream_identity(&tenant(owner)).await.unwrap(),
            tree.stream_identity(&tenant(owner)).await.unwrap(),
            "{owner}'s stream identity was not copied"
        );
        let origins = target.origins(&tenant(owner)).await.unwrap();
        let copied = feed(&target, owner).await;
        assert_eq!(
            origins.len(),
            copied.len(),
            "{owner}: the origin map does not cover exactly the copied events"
        );
        assert_eq!(
            instant_free(with_origins(&copied, &origins)),
            instant_free(feed(&tree, owner).await),
            "{owner}: a copied event differs from the tree's beyond its origin"
        );
        assert!(
            copied
                .iter()
                .all(|event| event.digest.is_none() && event.parents.is_empty()),
            "{owner}: the linear store claims a chain it does not keep"
        );
    }

    // The redacted body is gone from the copy; its origin, kept beside the row, is not.
    let origins = target.origins(&tenant("tenant-a")).await.unwrap();
    let redacted = &target
        .read_stream(&stream("tenant-a", "x"), 0, 10)
        .await
        .unwrap()
        .events[0];
    assert!(redacted.is_redacted(), "the redaction was not copied");
    assert_eq!(redacted.data, redaction_tombstone("erased"));
    assert_eq!(
        origins[&("item".to_owned(), "x".to_owned(), 1)].digest,
        appended.events[0].digest.clone().unwrap(),
        "redaction erased the origin of the event it redacted"
    );

    for (digest, bytes) in [("blob-1", &b"bytes"[..]), ("orphan-1", &b"alone"[..])] {
        assert_eq!(
            target.get_blob(&tenant("tenant-a"), digest).await.unwrap(),
            Some(bytes.to_vec()),
            "blob {digest} was not copied"
        );
    }

    // Receipts: the copy answers each command as the tree does, and a retry writes nothing.
    let receipt = target
        .recorded_command(&stream("tenant-a", "x"), "key-1", &appended_hash(&body))
        .await
        .unwrap()
        .expect("the append's receipt was not copied");
    let tree_receipt = tree
        .recorded_command(&stream("tenant-a", "x"), "key-1", &appended_hash(&body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        instant_free(with_origins(&receipt.events, &origins)),
        instant_free(tree_receipt.events),
        "the append's receipt differs from the tree's beyond its origin"
    );
    assert_eq!(
        (receipt.first_version, receipt.last_version),
        (appended.first_version, appended.last_version)
    );
    // A linear store takes a blob-bearing group through its blob entry point, as the replay does.
    let retried = target
        .append_group_with_blobs(&BlobAppendGroup {
            group: blob_group(),
            blobs: blobs()
                .into_iter()
                .map(|(digest, bytes)| BlobWrite { digest, bytes })
                .collect(),
        })
        .await
        .unwrap();
    assert!(
        retried.deduplicated,
        "the group's receipt was not copied: a retry wrote it again"
    );
    for (copied, written) in retried.appends.iter().zip(&grouped.appends) {
        assert_eq!(
            with_origins(&copied.events, &origins),
            written.events,
            "the group's receipt differs from the tree's beyond its origin"
        );
    }
}

fn appended_hash(body: &serde_json::Value) -> String {
    eventlog_core::request_hash(body).unwrap()
}

/// Build a stream that two checkouts forked and one of them merged, as two directories that hold
/// the same files but were merged in opposite directions.
async fn forked_and_merged() -> (tempfile::TempDir, tempfile::TempDir, Vec<String>) {
    let ours = tempfile::tempdir().unwrap();
    let theirs = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(ours.path()).await.unwrap();
        store
            .append(
                &stream("tenant-a", "x"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("base", &json!({})),
            )
            .await
            .unwrap();
    }
    merge_into(ours.path(), theirs.path());
    // Theirs writes first, so by § 6.3's tie-break its side sorts first on both checkouts.
    let mut written = Vec::new();
    for (directory, key) in [(theirs.path(), "theirs"), (ours.path(), "ours")] {
        let store = TreeEventStore::open(directory).await.unwrap();
        let result = store
            .append(
                &stream("tenant-a", "x"),
                Expected::Exact(1),
                &[event("item.indexed", 2)],
                &meta(key, &json!({ "side": key })),
            )
            .await
            .unwrap();
        written.push(result.events[0].event_id.clone());
        store
            .append(
                &stream("tenant-a", key),
                Expected::NoStream,
                &[event("item.received", 3)],
                &meta(&format!("{key}-own"), &json!({})),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    merge_into(theirs.path(), ours.path());
    {
        let store = TreeEventStore::open(ours.path()).await.unwrap();
        let heads = store.heads(&stream("tenant-a", "x")).await.unwrap();
        assert_eq!(heads.len(), 2, "the fixture did not fork the stream");
        let merged = store
            .append(
                &stream("tenant-a", "x"),
                Expected::Merge(HeadSetDigest::of(&heads)),
                &[event("item.merged", 4)],
                &meta("merge", &json!({})),
            )
            .await
            .unwrap();
        written.push(merged.events[0].event_id.clone());
    }
    merge_into(ours.path(), theirs.path());
    (ours, theirs, written)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forked_then_merged_tree_linearizes_in_position_order_the_same_way_on_every_checkout() {
    let (ours, theirs, written) = forked_and_merged().await;
    let mut linearized = Vec::new();
    for directory in [ours.path(), theirs.path()] {
        let tree = TreeEventStore::open(directory).await.unwrap();
        let target = SqliteEventStore::in_memory("copy").await.unwrap();
        copy(&tree, &target).await.unwrap();
        let origins = target.origins(&tenant("tenant-a")).await.unwrap();
        let copied = feed(&target, "tenant-a").await;
        assert_eq!(
            with_origins(&copied, &origins),
            feed(&tree, "tenant-a").await,
            "the copy of a merged tree differs from the tree beyond its origin"
        );
        linearized.push((copied, origins));
    }
    assert_eq!(
        linearized[0], linearized[1],
        "two checkouts of one history linearized differently"
    );

    let (copied, origins) = &linearized[0];
    let positions: Vec<u64> = copied.iter().map(|event| event.global_seq).collect();
    assert_eq!(
        positions,
        (1..=positions.len() as u64).collect::<Vec<_>>(),
        "the copy's positions are not 1..n"
    );
    let x: Vec<&RecordedEvent> = copied
        .iter()
        .filter(|event| event.stream_id == "x")
        .collect();
    assert_eq!(
        x.iter().map(|event| event.version).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "the merged stream is not gapless in the copy"
    );
    assert_eq!(
        x[1..]
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>(),
        written,
        "the fork's sides were not linearized in § 6.3 order: earliest recorded side first"
    );
    let merge = &origins[&("item".to_owned(), "x".to_owned(), 4)];
    let mut sides = vec![
        origins[&("item".to_owned(), "x".to_owned(), 2)]
            .digest
            .clone(),
        origins[&("item".to_owned(), "x".to_owned(), 3)]
            .digest
            .clone(),
    ];
    sides.sort();
    let mut parents = merge.parents.clone();
    parents.sort();
    assert_eq!(
        parents, sides,
        "the merge event's origin does not name both heads it joined"
    );
    for stream_id in ["ours", "theirs"] {
        assert_eq!(
            copied
                .iter()
                .filter(|event| event.stream_id == stream_id)
                .map(|event| event.version)
                .collect::<Vec<_>>(),
            vec![1],
            "stream {stream_id} is not gapless in the copy"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_refuses_a_target_that_already_holds_history_and_writes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path()).await.unwrap();
    for owner in ["tenant-a", "tenant-b"] {
        tree.append(
            &stream(owner, "x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("tree", &json!({})),
        )
        .await
        .unwrap();
    }
    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    target
        .append(
            &stream("tenant-b", "z"),
            Expected::NoStream,
            &[event("item.received", 9)],
            &meta("already", &json!({})),
        )
        .await
        .unwrap();

    let refused = copy(&tree, &target)
        .await
        .expect_err("a copy merged a tree into a store that already held history");
    assert!(
        matches!(&refused, EventLogError::Invalid(reason) if reason.contains("tenant-b")),
        "the refusal does not name the tenant that already has history: {refused:?}"
    );
    assert!(
        feed(&target, "tenant-a").await.is_empty(),
        "a refused copy wrote the tenant before the one it refused"
    );
    assert_eq!(feed(&target, "tenant-b").await.len(), 1);
    assert!(
        target
            .origins(&tenant("tenant-a"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn origins_survive_reopening_the_sqlite_file_and_go_with_an_erased_tenant() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path().join("tree"))
        .await
        .unwrap();
    tree.append(
        &stream("tenant-a", "x"),
        Expected::NoStream,
        &[event("item.received", 1), event("item.indexed", 2)],
        &meta("key-1", &json!({})),
    )
    .await
    .unwrap();
    let path = directory.path().join("copy.sqlite3");
    let path = path.to_str().unwrap();
    let written = {
        let target = SqliteEventStore::open(path, "copy").await.unwrap();
        copy(&tree, &target).await.unwrap();
        target.origins(&tenant("tenant-a")).await.unwrap()
    };
    assert_eq!(written.len(), 2, "the copy recorded no origin per event");

    let target = SqliteEventStore::open(path, "copy").await.unwrap();
    assert_eq!(
        target.origins(&tenant("tenant-a")).await.unwrap(),
        written,
        "the origin map did not survive reopening the database"
    );
    target.forget_tenant(&tenant("tenant-a")).await.unwrap();
    assert!(
        target
            .origins(&tenant("tenant-a"))
            .await
            .unwrap()
            .is_empty(),
        "an erased tenant's origins outlived it"
    );
}

/// A tree whose two tenants each hold one event and one blob.
async fn two_tenant_tree(directory: &Path) -> TreeEventStore {
    let tree = TreeEventStore::open(directory).await.unwrap();
    for owner in ["tenant-a", "tenant-b"] {
        tree.append(
            &stream(owner, "x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("tree", &json!({})),
        )
        .await
        .unwrap();
    }
    for (owner, digest) in [("tenant-a", "blob-a"), ("tenant-b", "blob-b")] {
        tree.put_blob(&tenant(owner), digest, b"tree bytes")
            .await
            .unwrap();
    }
    tree
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_refuses_a_target_that_binds_a_tree_blob_to_other_bytes_and_writes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let tree = two_tenant_tree(directory.path()).await;
    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    target
        .put_blob(&tenant("tenant-b"), "blob-b", b"other bytes")
        .await
        .unwrap();

    let refused = copy(&tree, &target)
        .await
        .expect_err("a copy overwrote or merged a blob the target binds to other bytes");
    assert!(
        matches!(&refused, EventLogError::Invalid(reason) if reason.contains("blob-b")),
        "the refusal does not name the conflicting blob: {refused:?}"
    );
    assert!(
        feed(&target, "tenant-a").await.is_empty(),
        "a refused copy wrote the tenant before the one it refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_refuses_a_target_with_queued_restored_identities_and_writes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let tree = two_tenant_tree(directory.path()).await;
    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    target
        .restore_events(vec![eventlog_sqlite::RestoredEvent {
            event_id: "someone-else".to_owned(),
            recorded_at: time::OffsetDateTime::UNIX_EPOCH,
            origin: None,
        }])
        .unwrap();

    let refused = copy(&tree, &target)
        .await
        .expect_err("a copy ran on a target another replay had queued identities on");
    assert!(
        matches!(&refused, EventLogError::Invalid(reason) if reason.contains("restored")),
        "the refusal does not say why: {refused:?}"
    );
    for (owner, digest) in [("tenant-a", "blob-a"), ("tenant-b", "blob-b")] {
        assert!(
            target
                .get_blob(&tenant(owner), digest)
                .await
                .unwrap()
                .is_none(),
            "a refused copy wrote {owner}'s blob"
        );
    }
    assert_eq!(target.restored_pending().unwrap(), 1);
}
