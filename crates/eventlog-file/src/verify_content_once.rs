//! `story:file-and-postgres-verify-each-content-once` on the File provider.
//!
//! A module of the crate so that `cost::of(root).blobs_hashed` — charged by the comparison in
//! `crate::verified` — and the handle's own memory of verified content are both visible.

use crate::FileEventStore;
use eventlog_core::{
    AppendGroup, AtomicEventStore as _, BoxFuture, EventLogError, EventStore as _, Expected, Guard,
    NoGuard, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::{fs, path::Path, sync::Arc};

const READS: usize = 20;

fn tenant() -> TenantId {
    TenantId::new("verify-once").unwrap()
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

fn content() -> Vec<u8> {
    (0..4096u32).map(|index| (index % 251) as u8).collect()
}

fn group(key: &str, id: &str) -> AppendGroup {
    AppendGroup {
        tenant: tenant(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: vec![StreamAppend {
            stream: stream(id),
            expected: Expected::Any,
            events: vec![eventlog_conformance::event("item.changed", 1)],
        }],
    }
}

/// A guard that reads one digest `READS` times, as a batch guard reads its batch once per member.
struct ReadsRepeatedly;

impl Guard for ReadsRepeatedly {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            for _ in 0..READS {
                assert_eq!(store.get_blob("d").await?, Some(content()));
            }
            Ok(())
        })
    }
}

/// The only object file under `root`.
fn object(root: &Path) -> std::path::PathBuf {
    let mut objects: Vec<_> = fs::read_dir(root.join("blobs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(objects.len(), 1, "the fixture binds one object");
    objects.remove(0)
}

/// Seed `d` through one handle, close it, and open a fresh one that has not read `d`.
async fn fresh_handle(root: &Path) -> FileEventStore {
    let writer = FileEventStore::open(root).await.unwrap();
    writer.put_blob(&tenant(), "d", &content()).await.unwrap();
    drop(writer);
    FileEventStore::open(root).await.unwrap()
}

/// The acceptance: a guard that reads one blob N times inside one guarded append pays one
/// SHA-256 over that content, not N.
#[tokio::test(flavor = "multi_thread")]
async fn one_guarded_append_reading_one_blob_n_times_hashes_it_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = fresh_handle(root).await;
    let before = crate::cost::of(root);
    store
        .append_group_guarded(&group("reads", "one"), Arc::new(ReadsRepeatedly))
        .await
        .unwrap();
    let after = crate::cost::of(root) - before;
    assert_eq!(
        after.blobs_hashed, 1,
        "{READS} reads of one unchanged blob inside one guarded append: {after:?}"
    );
}

/// Content this handle bound itself was hashed while binding, so no read hashes it again.
#[tokio::test(flavor = "multi_thread")]
async fn content_this_handle_wrote_is_not_hashed_again_by_any_read() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open(root).await.unwrap();
    store.put_blob(&tenant(), "d", &content()).await.unwrap();
    let before = crate::cost::of(root);
    store
        .append_group_guarded(&group("reads", "one"), Arc::new(ReadsRepeatedly))
        .await
        .unwrap();
    for _ in 0..READS {
        assert_eq!(
            store.get_blob(&tenant(), "d").await.unwrap(),
            Some(content())
        );
    }
    let after = crate::cost::of(root) - before;
    assert_eq!(after.blobs_hashed, 0, "{after:?}");
}

/// An object changed on disk after a verified read is refused on the next read through the same
/// handle, by `EventStore::get_blob` and by `ProjectionStore::get_blob` alike, every time.
#[tokio::test(flavor = "multi_thread")]
async fn an_object_changed_after_a_verified_read_is_refused_on_the_same_handle() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = fresh_handle(root).await;
    assert_eq!(
        store.get_blob(&tenant(), "d").await.unwrap(),
        Some(content())
    );
    store
        .append_group_guarded(&group("verified", "one"), Arc::new(ReadsRepeatedly))
        .await
        .unwrap();

    let path = object(root);
    let mut changed = content();
    changed[4095] ^= 1;
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&path, permissions).unwrap();
    fs::write(&path, &changed).unwrap();

    for _ in 0..3 {
        assert!(
            matches!(
                store.get_blob(&tenant(), "d").await,
                Err(EventLogError::Backend(_))
            ),
            "a same-length change after a verified read is refused"
        );
        let refused = store
            .append_group_guarded(&group("tampered", "two"), Arc::new(ReadsRepeatedly))
            .await
            .expect_err("the guard's read is refused");
        assert!(matches!(refused, EventLogError::Backend(_)), "{refused:?}");
    }
}

/// Remembered content is dropped when this handle deletes a blob, erases a tenant, or returns an
/// error from a write that bound blobs.
#[tokio::test(flavor = "multi_thread")]
async fn deletion_erasure_and_a_failed_binding_drop_remembered_content() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = FileEventStore::open(root).await.unwrap();
    store.put_blob(&tenant(), "d", &content()).await.unwrap();
    assert!(store.content.held() > 0, "the put is remembered");

    store
        .put_blob(&tenant(), "d", b"other bytes")
        .await
        .expect_err("a different binding");
    assert_eq!(store.content.held(), 0, "refused put");

    store.get_blob(&tenant(), "d").await.unwrap();
    assert!(store.content.held() > 0, "the read is remembered");
    store
        .append(
            &stream("held"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.created", 1)],
            &eventlog_conformance::meta("held", &json!({})),
        )
        .await
        .unwrap();
    let mut conflicting = group("conflicts", "held");
    conflicting.appends[0].expected = Expected::NoStream;
    store
        .append_group_guarded_with_blobs(
            &conflicting,
            Arc::new(NoGuard),
            &[("new".to_owned(), b"never committed".to_vec())],
        )
        .await
        .expect_err("the member conflicts after the batch was bound");
    assert_eq!(store.content.held(), 0, "guarded group rolled back");

    store.get_blob(&tenant(), "d").await.unwrap();
    eventlog_core::AtomicBlobEventStore::append_group_with_blobs_guarded(
        &store,
        &eventlog_core::BlobAppendGroup {
            group: conflicting,
            blobs: vec![eventlog_core::BlobWrite {
                digest: "new".into(),
                bytes: b"never committed".to_vec(),
            }],
        },
        Arc::new(NoGuard),
    )
    .await
    .expect_err("the member conflicts after the batch was bound");
    assert_eq!(store.content.held(), 0, "atomic blob group rolled back");

    store.put_blob(&tenant(), "e", b"other").await.unwrap();
    store.delete_blob(&tenant(), "e").await.unwrap();
    assert_eq!(store.content.held(), 0, "deletion");

    store.get_blob(&tenant(), "d").await.unwrap();
    assert!(store.content.held() > 0);
    store.forget_tenant(&tenant()).await.unwrap();
    assert_eq!(store.content.held(), 0, "erasure");
}
