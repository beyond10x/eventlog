//! A command receipt and its events are one observation (review L51).
//!
//! A module of the crate so that a case can hold `recorded_command` between its receipt and its
//! event reads. Every case needs `EVENTLOG_TEST_POSTGRES_URL` and refuses to run without it: a
//! skipped backend has not been proved.

use crate::PostgresEventStore;
use eventlog_core::{EventLogError, EventStore as _, Expected, StreamId, TenantId};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Receipt reads of one store prefix to hold before their events are read: (prefix, reached,
/// resume). Keyed by prefix so concurrent cases in this process never pause each other.
type ReceiptPause = (String, Arc<Notify>, Arc<Notify>);
static RECEIPT_PAUSES: Mutex<Vec<ReceiptPause>> = Mutex::new(Vec::new());

fn pause_after_receipt(prefix: &str) -> (Arc<Notify>, Arc<Notify>) {
    let reached = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    RECEIPT_PAUSES.lock().unwrap().push((
        prefix.to_owned(),
        Arc::clone(&reached),
        Arc::clone(&resume),
    ));
    (reached, resume)
}

/// Called by `recorded_command` between its receipt read and its event read.
pub(crate) async fn after_receipt(prefix: &str) {
    let pause = {
        let mut pauses = RECEIPT_PAUSES.lock().unwrap();
        pauses
            .iter()
            .position(|(paused, _, _)| paused == prefix)
            .map(|index| pauses.remove(index))
    };
    if let Some((_, reached, resume)) = pause {
        reached.notify_one();
        resume.notified().await;
    }
}

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
    format!("rs_{case}_{suffix}")
}

fn stream() -> StreamId {
    StreamId::new(TenantId::new("receipt").unwrap(), "item", "one").unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_receipt_and_its_events_come_from_one_snapshot_across_tenant_erasure() {
    let prefix = prefix("erased");
    let store = Arc::new(PostgresEventStore::connect(&url(), &prefix).await.unwrap());
    let meta = eventlog_conformance::meta("command-a", &json!({}));
    store
        .append(
            &stream(),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &meta,
        )
        .await
        .unwrap();

    let (reached, resume) = pause_after_receipt(&prefix);
    let reader = {
        let store = Arc::clone(&store);
        let hash = meta.request_hash.clone();
        tokio::spawn(async move { store.recorded_command(&stream(), "command-a", &hash).await })
    };
    reached.notified().await;
    // The erasure commits after the receipt was read and before its events are.
    store.forget_tenant(stream().tenant()).await.unwrap();
    resume.notify_one();

    let recorded = reader
        .await
        .unwrap()
        .unwrap()
        .expect("the receipt read before the erasure");
    assert_eq!(
        (
            recorded.first_version,
            recorded.last_version,
            recorded.events.len()
        ),
        (1, 1, 1),
        "the receipt's interval and its events must be one observation"
    );
    assert_eq!(
        store
            .recorded_command(&stream(), "command-a", &meta.request_hash)
            .await
            .unwrap(),
        None,
        "a later read sees the erasure"
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_receipt_whose_interval_lacks_an_event_is_refused() {
    let prefix = prefix("short");
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    let meta = eventlog_conformance::meta("command-a", &json!({}));
    store
        .append(
            &stream(),
            Expected::NoStream,
            &[
                eventlog_conformance::event("item.received", 1),
                eventlog_conformance::event("item.received", 2),
            ],
            &meta,
        )
        .await
        .unwrap();
    let (sql, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Not a path the kit offers: the fixture removes a row to stand for a damaged interval.
    sql.batch_execute(&format!("DELETE FROM {prefix}_events WHERE version = 2"))
        .await
        .unwrap();

    match store
        .recorded_command(&stream(), "command-a", &meta.request_hash)
        .await
    {
        Err(EventLogError::Backend(reason)) => {
            assert_eq!(reason, "recorded command interval is incomplete");
        }
        other => panic!("expected the incomplete interval to be refused, got {other:?}"),
    }
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
