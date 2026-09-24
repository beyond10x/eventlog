//! Adversarial cases for `copy` (design § 9.6): what the author's suite does not say about a
//! refused copy, blobs shared between groups, file-backed targets and erasure.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, NoGuard, StreamAppend,
    StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use eventlog_tree::{TreeEventStore, copy};
use serde_json::json;
use std::sync::Arc;

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

fn stream(owner: &str, id: &str) -> StreamId {
    StreamId::new(tenant(owner), "item", id).unwrap()
}

async fn feed_len(store: &dyn EventStore, owner: &str) -> usize {
    store
        .read_feed(&tenant(owner), 0, 1000)
        .await
        .unwrap()
        .events
        .len()
}

/// AGENTS.md invariant 7, "refusals change nothing": a copy that refuses must leave the target as
/// it found it. A target that already minted a stream identity for a later tenant holds no events
/// for it, so it passes the pre-check, and the copy refuses only after the earlier tenant is in.
#[tokio::test(flavor = "multi_thread")]
async fn a_copy_refused_for_a_minted_identity_leaves_no_earlier_tenant_in_the_target() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path()).await.unwrap();
    for owner in ["tenant-a", "tenant-b"] {
        tree.stream_identity(&tenant(owner)).await.unwrap();
        tree.append(
            &stream(owner, "x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta(&format!("{owner}-key"), &json!({})),
        )
        .await
        .unwrap();
    }
    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    // A read of the identity, before any event, mints one of the target's own.
    target.stream_identity(&tenant("tenant-b")).await.unwrap();

    let refused = copy(&tree, &target)
        .await
        .expect_err("the copy accepted a target whose identity for tenant-b differs");
    assert!(
        matches!(refused, EventLogError::Invalid(_)),
        "unexpected refusal: {refused:?}"
    );
    assert_eq!(
        feed_len(&target, "tenant-a").await,
        0,
        "a refused copy left tenant-a's events in the target"
    );
    assert!(
        target
            .origins(&tenant("tenant-a"))
            .await
            .unwrap()
            .is_empty(),
        "a refused copy left tenant-a's origins in the target"
    );
}

fn shared_blob_group(key: &str, id: &str) -> AppendGroup {
    AppendGroup {
        tenant: tenant("tenant-a"),
        appends: vec![StreamAppend {
            stream: stream("tenant-a", id),
            expected: Expected::NoStream,
            events: vec![event("item.received", 1)],
        }],
        meta: meta(key, &json!({ "key": key })),
    }
}

/// Two groups bind one blob; the target is a file database, whose blob reads re-check integrity
/// (unlike the in-memory target every author case uses), and it is reopened before being read.
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_bound_by_two_groups_copies_into_a_file_database_and_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path().join("tree"))
        .await
        .unwrap();
    let blobs = vec![("shared".to_owned(), b"shared bytes".to_vec())];
    for (key, id) in [("g1", "one"), ("g2", "two")] {
        tree.append_group_guarded_with_blobs(
            &shared_blob_group(key, id),
            Arc::new(NoGuard),
            &blobs,
        )
        .await
        .unwrap();
    }
    tree.redact(&stream("tenant-a", "one"), 1, "gone")
        .await
        .unwrap();
    let path = directory.path().join("copy.sqlite3");
    let path = path.to_str().unwrap();
    let report = {
        let target = SqliteEventStore::open(path, "copy").await.unwrap();
        copy(&tree, &target).await.unwrap()
    };
    assert_eq!((report.groups, report.events, report.redactions), (2, 2, 1));

    let target = SqliteEventStore::open(path, "copy").await.unwrap();
    assert_eq!(
        target
            .get_blob(&tenant("tenant-a"), "shared")
            .await
            .unwrap(),
        Some(b"shared bytes".to_vec()),
        "the shared blob did not survive the copy and a reopen"
    );
    let origins = target.origins(&tenant("tenant-a")).await.unwrap();
    assert_eq!(origins.len(), 2, "an event lost its origin: {origins:?}");
    let redacted = &target
        .read_stream(&stream("tenant-a", "one"), 0, 10)
        .await
        .unwrap()
        .events[0];
    assert!(
        redacted.is_redacted(),
        "the redaction did not reach the file target"
    );
}

/// Erasing one copied tenant erases its origins and no other tenant's.
#[tokio::test(flavor = "multi_thread")]
async fn erasing_one_copied_tenant_keeps_every_other_tenants_origins() {
    let directory = tempfile::tempdir().unwrap();
    let tree = TreeEventStore::open(directory.path()).await.unwrap();
    for owner in ["tenant-a", "tenant-b"] {
        tree.append(
            &stream(owner, "x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("same-key", &json!({})),
        )
        .await
        .unwrap();
    }
    let target = SqliteEventStore::in_memory("copy").await.unwrap();
    copy(&tree, &target).await.unwrap();
    let before = target.origins(&tenant("tenant-b")).await.unwrap();
    assert_eq!(before.len(), 2);
    target.forget_tenant(&tenant("tenant-a")).await.unwrap();
    assert!(
        target
            .origins(&tenant("tenant-a"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        target.origins(&tenant("tenant-b")).await.unwrap(),
        before,
        "erasing tenant-a took tenant-b's origins"
    );
}
