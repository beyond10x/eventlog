//! Independent re-measurement of the cost this unit's `## Outcome` states, pass 2.
//!
//! Inert unless driven: both cases return immediately without their environment variable, exactly
//! as `durability::process_writer` does, so an ordinary suite run neither builds a fixture nor
//! pays for one.
//!
//! ```console
//! EL_MEASURE_BUILD=<dir> cargo test --release -p eventlog-file --test measure_review_two -- --exact build_fixture --nocapture
//! EL_MEASURE_RUN=<dir>   cargo test --release -p eventlog-file --test measure_review_two -- --exact read_transactions --nocapture
//! ```

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_file::FileEventStore;
use serde_json::json;
use std::{fs, time::Instant};

const FRAMES: usize = 1_000;
const BLOBS: usize = 1_000;
const BLOB_BYTES: usize = 10 * 1024;
const TRANSACTIONS: usize = 200;

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

/// The fixture the story's `## Acceptance` names: about 2,000 frames, 1,000 blobs of 10 KB.
#[tokio::test(flavor = "multi_thread")]
async fn build_fixture() {
    let Ok(root) = std::env::var("EL_MEASURE_BUILD") else {
        return;
    };
    let store = FileEventStore::open(&root).await.unwrap();
    for index in 0..FRAMES {
        store
            .append(
                &stream("a"),
                Expected::Exact(u64::try_from(index).unwrap()),
                &[event("item.recorded", i64::try_from(index).unwrap())],
                &meta(&format!("seed-{index}"), &json!({})),
            )
            .await
            .unwrap();
    }
    let bytes = vec![b'x'; BLOB_BYTES];
    for index in 0..BLOBS {
        store
            .put_blob(&tenant(), &format!("digest-{index}"), &bytes)
            .await
            .unwrap();
    }
    let events = fs::metadata(std::path::Path::new(&root).join("events.jsonl"))
        .unwrap()
        .len();
    println!(
        "FIXTURE events.jsonl={events} frames={} blobs={BLOBS}",
        FRAMES + BLOBS
    );
}

/// `open` once, then 200 read-only transactions, wall time for each.
#[tokio::test(flavor = "multi_thread")]
async fn read_transactions() {
    let Ok(root) = std::env::var("EL_MEASURE_RUN") else {
        return;
    };
    let committed = fs::metadata(std::path::Path::new(&root).join("events.jsonl"))
        .unwrap()
        .len();
    let opening = Instant::now();
    let store = FileEventStore::open(&root).await.unwrap();
    let open_ms = opening.elapsed().as_secs_f64() * 1000.0;
    let reading = Instant::now();
    for _ in 0..TRANSACTIONS {
        assert_eq!(
            store.stream_version(&stream("a")).await.unwrap(),
            Some(u64::try_from(FRAMES).unwrap())
        );
    }
    let read_ms = reading.elapsed().as_secs_f64() * 1000.0;
    println!(
        "MEASURE committed_bytes={committed} open_ms={open_ms:.1} \
         read_{TRANSACTIONS}_ms={read_ms:.1} per_transaction_ms={:.3}",
        read_ms / f64::from(u32::try_from(TRANSACTIONS).unwrap())
    );
}
