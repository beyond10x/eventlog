//! A grouped append's deadline includes its wait for inline registration (review L65).
//!
//! A module of the crate so that a case can hold the registration mutex the way an inline
//! rebuild does. Every case needs `EVENTLOG_TEST_POSTGRES_URL` and refuses to run without it: a
//! skipped backend has not been proved.

use crate::{PoolOptions, PostgresEventStore};
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore as _, AtomicEventStore as _, BlobAppendGroup, BlobWrite,
    EventLogError, Expected, NoGuard, StreamAppend, StreamId, TenantId,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_millis(400);
const HELD: Duration = Duration::from_millis(1_200);

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
    format!("rd_{case}_{suffix}")
}

fn group(key: &str) -> AppendGroup {
    let tenant = TenantId::new("deadline").unwrap();
    AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant, "item", key).unwrap(),
            expected: Expected::NoStream,
            events: vec![eventlog_conformance::event("item.received", 1)],
        }],
    }
}

async fn store(case: &str) -> Arc<PostgresEventStore> {
    let options = PoolOptions {
        transaction_timeout: TIMEOUT,
        ..PoolOptions::default()
    };
    Arc::new(
        PostgresEventStore::connect_local(&url(), &prefix(case), options)
            .await
            .unwrap(),
    )
}

/// Hold registration for longer than the whole budget, as a long inline rebuild does, and
/// return the outcome of `append` and how long it took.
async fn behind_held_registration<T: Send + 'static>(
    store: &Arc<PostgresEventStore>,
    append: impl std::future::Future<Output = Result<T, EventLogError>> + Send + 'static,
) -> (Result<T, EventLogError>, Duration) {
    let registration = store.registration.lock().await;
    let started = Instant::now();
    let writer = tokio::spawn(async move {
        let outcome = append.await;
        (outcome, started.elapsed())
    });
    tokio::time::sleep(HELD).await;
    drop(registration);
    writer.await.unwrap()
}

fn assert_deadline_includes_wait<T: std::fmt::Debug>(
    outcome: Result<T, EventLogError>,
    elapsed: Duration,
) {
    assert!(
        matches!(outcome, Err(EventLogError::Deadline { .. })),
        "the group must spend its one budget on the registration wait, got {outcome:?}"
    );
    assert!(
        elapsed < HELD,
        "the deadline fired after the registration wait ended: {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grouped_append_spends_its_deadline_on_the_registration_wait() {
    let store = store("group").await;
    let writer = Arc::clone(&store);
    let (outcome, elapsed) = behind_held_registration(&store, async move {
        writer
            .append_group_guarded(&group("one"), Arc::new(NoGuard))
            .await
    })
    .await;
    assert_deadline_includes_wait(outcome, elapsed);
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grouped_blob_append_spends_its_deadline_on_the_registration_wait() {
    let store = store("blobgroup").await;
    let writer = Arc::clone(&store);
    let (outcome, elapsed) = behind_held_registration(&store, async move {
        let request = BlobAppendGroup {
            group: group("two"),
            blobs: vec![BlobWrite {
                digest: "deadline-blob".to_owned(),
                bytes: vec![1, 2, 3],
            }],
        };
        writer
            .append_group_with_blobs_guarded(&request, Arc::new(NoGuard))
            .await
    })
    .await;
    assert_deadline_includes_wait(outcome, elapsed);
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
