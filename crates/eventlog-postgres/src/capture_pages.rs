//! A capture page is sized from the remaining payload allowance (review L77).
//!
//! A module of the crate so that a case can see how many rows each capture page fetched. Every
//! case needs `EVENTLOG_TEST_POSTGRES_URL` and refuses to run without it: a skipped backend has
//! not been proved.

use crate::PostgresEventStore;
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureResource, ConsistentTenantCapture as _, EventStore as _,
    Expected, NewEvent, StreamId, TenantId,
};
use serde_json::json;
use std::sync::Mutex;

/// Rows of each capture page fetched, by store prefix, in order.
static PAGES: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Called by the capture reads with the rows one page fetched.
pub(crate) fn record_page(prefix: &str, rows: usize) {
    PAGES.lock().unwrap().push((prefix.to_owned(), rows));
}

fn pages_of(prefix: &str) -> Vec<usize> {
    PAGES
        .lock()
        .unwrap()
        .iter()
        .filter(|(paged, _)| paged == prefix)
        .map(|(_, rows)| *rows)
        .collect()
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
    format!("cp_{case}_{suffix}")
}

/// Bodies of 8,011 compact bytes: `{"text":"` and `"}` around 8,000 filler characters.
const BODY: u64 = 8_011;
const EVENTS: usize = 20;

/// A store whose one tenant holds `EVENTS` events of `BODY` bytes each.
async fn large_history(case: &str) -> (PostgresEventStore, String, TenantId) {
    let prefix = prefix(case);
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    let tenant = TenantId::new("capture-pages").unwrap();
    store.stream_identity(&tenant).await.unwrap();
    let body = json!({"text": "x".repeat(8_000)});
    assert_eq!(serde_json::to_vec(&body).unwrap().len() as u64, BODY);
    let events: Vec<NewEvent> = (0..EVENTS)
        .map(|_| NewEvent::new("item.noted", 1, body.clone()).unwrap())
        .collect();
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").unwrap(),
            Expected::NoStream,
            &events,
            &eventlog_conformance::meta("large", &json!({})),
        )
        .await
        .unwrap();
    (store, prefix, tenant)
}

fn limits(max_payload_bytes: u64) -> CaptureLimits {
    CaptureLimits {
        max_events: 1_000,
        max_blobs: 1_000,
        max_projection_rows: 1_000,
        max_payload_bytes,
    }
}

fn assert_payload_refused<T: std::fmt::Debug>(outcome: Result<T, CaptureError>) {
    match outcome {
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::PayloadBytes,
            ..
        }) => {}
        other => panic!("expected the payload cap to be crossed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_body_over_the_allowance_is_refused_after_one_fetched_row() {
    let (store, prefix, tenant) = large_history("first").await;
    assert_payload_refused(store.capture_tenant(&tenant, &[], limits(4_000)).await);
    assert_eq!(
        pages_of(&prefix),
        vec![1],
        "the refused capture fetched more rows than it could admit"
    );
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_page_holds_what_the_remaining_allowance_could_admit() {
    let (store, prefix, tenant) = large_history("grow").await;
    // Room for three and a half bodies: one row, then two at the largest size seen, then one.
    assert_payload_refused(
        store
            .capture_tenant(&tenant, &[], limits(3 * BODY + BODY / 2))
            .await,
    );
    assert_eq!(pages_of(&prefix), vec![1, 2, 1]);
    let whole = store
        .capture_tenant(&tenant, &[], limits(EVENTS as u64 * BODY))
        .await
        .expect("an allowance that holds every body");
    assert_eq!(whole.events.len(), EVENTS);
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
