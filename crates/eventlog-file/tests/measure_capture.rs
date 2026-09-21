//! The workload a wall clock attaches to when the question is what a capture costs.
//!
//! In the shape [`eventlog_file`]'s own `measure` case already uses: an ordinary case that does
//! nothing until the environment names a workload, so the suite gains no ignored lane and one
//! process does one thing. `EVENTLOG_FILE_CAPTURE_MEASURE` is `eager` (the value every caller
//! receives by value) or `deferred` (the value whose blob bytes are read when they are asked
//! for); `EVENTLOG_FILE_CAPTURE_MEASURE_BLOBS` is how many blobs the authority binds and
//! `EVENTLOG_FILE_CAPTURE_MEASURE_BYTES` how large each one is. The defaults are the ESS copy's
//! own shape — 7,815 blobs and 157.0 MB — because that is the authority the acceptance names.
//!
//! The fixture is written through `append_group_with_blobs` in chunks rather than through
//! `put_blob` per blob: what is being measured is the read, and one barrier per blob would spend
//! the whole run writing.

use std::{sync::Arc, time::Instant};

use eventlog_core::{
    AppendGroup, CaptureLimits, ConsistentTenantCapture, EventStore, Expected, StreamAppend,
    StreamId, TenantId,
};
use eventlog_file::{FileEventStore, FileTenantCapture};

fn tenant() -> TenantId {
    TenantId::new("capture-measure").unwrap()
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 1 << 20,
        max_blobs: 1 << 20,
        max_projection_rows: 1 << 20,
        max_payload_bytes: 1 << 40,
    }
}

fn setting(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

#[tokio::test(flavor = "multi_thread")]
async fn measure_capture() {
    let Ok(workload) = std::env::var("EVENTLOG_FILE_CAPTURE_MEASURE") else {
        return;
    };
    let count = setting("EVENTLOG_FILE_CAPTURE_MEASURE_BLOBS", 7_815);
    let size = setting("EVENTLOG_FILE_CAPTURE_MEASURE_BYTES", 20_090);
    let chunk = setting("EVENTLOG_FILE_CAPTURE_MEASURE_CHUNK", 256);

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let written = Instant::now();
    {
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        let mut bound = 0usize;
        while bound < count {
            let batch: Vec<(String, Vec<u8>)> = (bound..(bound + chunk).min(count))
                .map(|index| {
                    let mut bytes = vec![0u8; size];
                    bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
                    (format!("d{index:06}"), bytes)
                })
                .collect();
            let group = AppendGroup {
                tenant: tenant(),
                meta: eventlog_conformance::meta(
                    &format!("measure-{bound}"),
                    &serde_json::json!({}),
                ),
                appends: vec![StreamAppend {
                    stream: StreamId::new(tenant(), "item", format!("s{bound}")).unwrap(),
                    expected: Expected::Any,
                    events: vec![eventlog_conformance::event("item.changed", 1)],
                }],
            };
            store.append_group_with_blobs(&group, &batch).await.unwrap();
            bound += batch.len();
        }
    }
    let fixture = written.elapsed();

    let handle = Arc::new(FileTenantCapture::open(root).await.unwrap());
    let taken = Instant::now();
    let blobs = match workload.as_str() {
        "eager" => {
            let value = handle
                .capture_tenant(&tenant(), &[], limits())
                .await
                .unwrap();
            value.blobs.len()
        }
        "deferred" => {
            let value = handle
                .capture_tenant_deferred(&tenant(), &[], limits())
                .await
                .unwrap();
            value.blobs.len()
        }
        other => panic!("unknown workload {other}"),
    };
    let capture = taken.elapsed();
    assert_eq!(blobs, count, "the measured capture named every bound blob");
    println!(
        "CAPTURE workload={workload} blobs={count} bytes={} fixture={fixture:?} capture={capture:?}",
        count * size
    );
}
