//! `story:file-and-postgres-verify-each-content-once` on PostgreSQL.
//!
//! A module of the crate so that the handle's own memory of verified content — and the count of
//! full checks it could not answer — is visible. Every case needs `EVENTLOG_TEST_POSTGRES_URL`
//! and refuses to run without it: a skipped backend has not been proved.

use crate::PostgresEventStore;
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore as _, AtomicEventStore as _, BlobAppendGroup, BlobWrite,
    BoxFuture, EventLogError, EventStore as _, Expected, Guard, NoGuard, ProjectionStore,
    StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::sync::Arc;

const READS: usize = 20;

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
}

/// A prefix no other case or run uses: lowercase letters only, as `validate_prefix` requires.
fn prefix(case: &str) -> String {
    let mut nanos = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .unsigned_abs();
    let mut suffix = String::new();
    for _ in 0..12 {
        suffix.push(char::from(b'a' + (nanos % 26) as u8));
        nanos /= 26;
    }
    format!("vco_{case}_{suffix}")
}

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

/// Seed `d` through one handle, close it, and connect a fresh one that has not read `d`.
async fn fresh_handle(prefix: &str) -> PostgresEventStore {
    let writer = PostgresEventStore::connect(&url(), prefix).await.unwrap();
    writer.drop_tables().await.unwrap();
    let writer = PostgresEventStore::connect(&url(), prefix).await.unwrap();
    writer.put_blob(&tenant(), "d", &content()).await.unwrap();
    writer.shutdown().await.unwrap();
    PostgresEventStore::connect(&url(), prefix).await.unwrap()
}

/// The acceptance: a guard that reads one blob N times inside one guarded append pays one
/// SHA-256 over that content, not N.
#[tokio::test]
async fn one_guarded_append_reading_one_blob_n_times_hashes_it_once() {
    let prefix = prefix("once");
    let store = fresh_handle(&prefix).await;
    let before = store.verified.hashed();
    store
        .append_group_guarded(&group("reads", "one"), Arc::new(ReadsRepeatedly))
        .await
        .unwrap();
    assert_eq!(
        store.verified.hashed() - before,
        1,
        "{READS} reads of one unchanged row inside one guarded append"
    );
    store.drop_tables().await.unwrap();
}

/// Content this handle bound itself was hashed while binding, so no read hashes it again.
#[tokio::test]
async fn content_this_handle_wrote_is_not_hashed_again_by_any_read() {
    let prefix = prefix("wrote");
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.drop_tables().await.unwrap();
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.put_blob(&tenant(), "d", &content()).await.unwrap();
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
    assert_eq!(store.verified.hashed(), 0, "put readback and every read");
    store.drop_tables().await.unwrap();
}

/// A row changed after a verified read is refused on the next read through the same handle, by
/// `EventStore::get_blob` and by `ProjectionStore::get_blob` alike, every time.
#[tokio::test]
async fn a_row_changed_after_a_verified_read_is_refused_on_the_same_handle() {
    let prefix = prefix("tamper");
    let store = fresh_handle(&prefix).await;
    assert_eq!(
        store.get_blob(&tenant(), "d").await.unwrap(),
        Some(content())
    );
    store
        .append_group_guarded(&group("verified", "one"), Arc::new(ReadsRepeatedly))
        .await
        .unwrap();

    let (sql, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut changed = content();
    changed[4095] ^= 1;
    sql.execute(
        &format!("UPDATE {prefix}_blobs SET bytes=$1 WHERE tenant_id=$2 AND digest='d'"),
        &[&changed, &tenant().as_str()],
    )
    .await
    .unwrap();

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
    store.drop_tables().await.unwrap();
}

/// Remembered content is dropped when this handle deletes a blob, erases a tenant, or returns an
/// error from a write that bound blobs.
#[tokio::test]
async fn deletion_erasure_and_a_failed_binding_drop_remembered_content() {
    let prefix = prefix("clear");
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.drop_tables().await.unwrap();
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.put_blob(&tenant(), "d", &content()).await.unwrap();
    assert!(store.verified.held() > 0, "the put is remembered");

    store
        .put_blob(&tenant(), "d", b"other bytes")
        .await
        .expect_err("a different binding");
    assert_eq!(store.verified.held(), 0, "refused put");

    store.get_blob(&tenant(), "d").await.unwrap();
    assert!(store.verified.held() > 0, "the read is remembered");
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
        .append_group_with_blobs_guarded(
            &BlobAppendGroup {
                group: conflicting,
                blobs: vec![BlobWrite {
                    digest: "new".into(),
                    bytes: b"never committed".to_vec(),
                }],
            },
            Arc::new(NoGuard),
        )
        .await
        .expect_err("the member conflicts after the batch was bound");
    assert_eq!(store.verified.held(), 0, "atomic blob group rolled back");

    store.put_blob(&tenant(), "e", b"other").await.unwrap();
    store.delete_blob(&tenant(), "e").await.unwrap();
    assert_eq!(store.verified.held(), 0, "deletion");

    store.get_blob(&tenant(), "d").await.unwrap();
    assert!(store.verified.held() > 0);
    store.forget_tenant(&tenant()).await.unwrap();
    assert_eq!(store.verified.held(), 0, "erasure");
    store.drop_tables().await.unwrap();
}
