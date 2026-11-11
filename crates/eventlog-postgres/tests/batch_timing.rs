//! The consumer's measured cases on PostgreSQL: 10,000 single-row reads through `read_many`, and
//! append groups of 1,000, 5,000 and 20,000 events.
//!
//! The bounds are the story's, for a release build (`cargo test --release -p eventlog-postgres
//! --test batch_timing -- --nocapture`): the reads under one second, and every group committed
//! inside the default store-operation deadline. One test, so the cases never share the fixture
//! with each other.
//!
//! The 20,000 events go to 4,000 streams, five each. A group takes one transaction-scoped
//! advisory lock per distinct stream, and each lock is a slot in PostgreSQL's shared lock table:
//! with the default `max_connections = 100` and `max_locks_per_transaction = 64` that table holds
//! about 7,000, so a group over 20,000 distinct streams fails with SQLSTATE 53200 however fast
//! it is.

use std::time::{Duration, Instant};

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventStore, Expected, Read, ReadResult, StreamAppend, StreamId,
    TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
}

fn prefix() -> String {
    let run: String = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
        .bytes()
        .rev()
        .take(12)
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("btm_{run}")
}

fn stream(tenant: &TenantId, n: usize) -> StreamId {
    StreamId::new(tenant.clone(), "instance", format!("i-{n}")).unwrap()
}

/// `streams` new streams with `each` events apiece, in one group.
fn group(tenant: &TenantId, streams: usize, each: usize) -> AppendGroup {
    AppendGroup {
        tenant: tenant.clone(),
        appends: (0..streams)
            .map(|n| StreamAppend {
                stream: stream(tenant, n),
                expected: Expected::NoStream,
                events: (0..each)
                    .map(|k| event("instance.changed", i64::try_from(n * each + k).unwrap()))
                    .collect(),
            })
            .collect(),
        meta: meta(
            &format!("group-{streams}-{each}"),
            &serde_json::json!({ "streams": streams, "each": each }),
        ),
    }
}

#[tokio::test]
async fn ten_thousand_reads_and_a_twenty_thousand_event_group_fit_their_budgets() {
    let store = PostgresEventStore::connect(&url(), &prefix())
        .await
        .unwrap();
    let deadline = PoolOptions::default().transaction_timeout;
    let mut refused = Vec::new();
    let mut reads = Vec::new();
    for (streams, each) in [(1_000, 1), (5_000, 1), (4_000, 5)] {
        let tenant = TenantId::new(format!("tenant-{streams}-{each}")).unwrap();
        let group = group(&tenant, streams, each);
        let started = Instant::now();
        let outcome = store.append_group(&group).await;
        let elapsed = started.elapsed();
        eprintln!(
            "timing: append_group of {} events on {streams} streams: {elapsed:?} -> {:?}",
            streams * each,
            outcome.as_ref().map(|result| result.appends.len())
        );
        if outcome.is_err() || elapsed >= deadline {
            refused.push(format!(
                "group of {} events: {elapsed:?} {:?}",
                streams * each,
                outcome.err()
            ));
        }
        // Each stream read at its last version: `read_stream(stream, v - 1, 1)`.
        reads.extend((0..streams).map(|n| Read::Stream {
            stream: stream(&tenant, n),
            after_version: u64::try_from(each - 1).unwrap(),
            limit: 1,
        }));
    }

    assert_eq!(reads.len(), 10_000);
    let started = Instant::now();
    let answered = store.read_many(&reads).await.unwrap();
    let elapsed = started.elapsed();
    eprintln!("timing: read_many of 10,000 single-row reads: {elapsed:?}");
    let rows: usize = answered
        .iter()
        .map(|answer| match answer {
            ReadResult::Stream(slice) => slice.events.len(),
            ReadResult::Blob(_) => 0,
        })
        .sum();
    if elapsed >= Duration::from_secs(1) {
        refused.push(format!("10,000 reads: {elapsed:?}"));
    }
    if rows != 10_000 {
        refused.push(format!("10,000 reads returned {rows} rows"));
    }
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
    assert!(refused.is_empty(), "over budget: {refused:?}");
}
