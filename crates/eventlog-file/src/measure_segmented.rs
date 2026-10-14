//! The before/after measurement story:incremental-history-digest-for-a-resumed-handle asks for:
//! 200 read transactions and the blobs they hash, on the story's fixture (1,000 frames and 1,000
//! blobs of 10 KiB), on both layouts.
//!
//! Inert unless driven, as `tests/measure_review_two.rs` is:
//!
//! ```console
//! EL_MEASURE_SEGMENTED=<dir> cargo test --release -p eventlog-file --lib \
//!   measure_segmented::measure_segmented -- --exact --nocapture
//! ```
//!
//! Each layout gets its own store under `<dir>`, built once and reused by later runs. Two rows per
//! layout: `aged`, a handle that opened a store unchanged for longer than the untrusted window
//! (the stamp path), and `own-write`, the same handle after one append of its own, which puts
//! every following resume inside the window — the case the story measures.

use super::*;
use eventlog_conformance::{event, meta};
use std::time::{Duration, Instant};

const FRAMES: u64 = 1_000;
const BLOBS: usize = 1_000;
const BLOB_BYTES: usize = 10 * 1024;
const TRANSACTIONS: u32 = 200;

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("durable-owner").unwrap(), "item", id).unwrap()
}

async fn build(root: &Path, layout: FileLayout) {
    let store = FileEventStore::open_with_layout(root, layout)
        .await
        .unwrap();
    for index in 0..FRAMES {
        store
            .append(
                &stream("a"),
                Expected::Exact(index),
                &[event("item.recorded", i64::try_from(index).unwrap())],
                &meta(&format!("seed-{index}"), &serde_json::json!({})),
            )
            .await
            .unwrap();
    }
    let bytes = vec![b'x'; BLOB_BYTES];
    for index in 0..BLOBS {
        store
            .put_blob(stream("a").tenant(), &format!("digest-{index}"), &bytes)
            .await
            .unwrap();
    }
}

async fn reads(store: &FileEventStore, root: &Path, row: &str, layout: FileLayout) {
    let before = crate::cost::of(root);
    let started = Instant::now();
    for _ in 0..TRANSACTIONS {
        assert_eq!(
            store.stream_version(&stream("a")).await.unwrap(),
            Some(FRAMES)
        );
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let cost = crate::cost::of(root) - before;
    println!(
        "MEASURE layout={layout:?} row={row} read_{TRANSACTIONS}_ms={elapsed:.1} \
         per_transaction_ms={:.4} blobs_hashed={} prefix_bytes_compared={} \
         journal_bytes_hashed={} resumes_trusted={}",
        elapsed / f64::from(TRANSACTIONS),
        cost.blobs_hashed,
        cost.prefix_bytes_compared,
        cost.journal_bytes_hashed,
        cost.resumes_trusted,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn measure_segmented() {
    let Ok(base) = std::env::var("EL_MEASURE_SEGMENTED") else {
        return;
    };
    for (name, layout) in [
        ("single", FileLayout::Single),
        ("segmented", FileLayout::segmented()),
    ] {
        let root = Path::new(&base).join(name);
        if !root.join("manifest.json").exists() {
            build(&root, layout).await;
        }
        let manifest: Value =
            serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
        let own = manifest["sequence"].as_u64().unwrap() - (FRAMES + BLOBS as u64);
        // Past the window, so the opener stamps every file it verifies.
        tokio::time::sleep(Duration::from_millis(2_200)).await;
        let store = FileEventStore::open(&root).await.unwrap();
        println!(
            "FIXTURE layout={layout:?} committed_bytes={} sealed_segments={}",
            manifest["length"],
            manifest["segments"]["sealed"]
                .as_array()
                .map_or(0, Vec::len)
        );
        reads(&store, &root, "aged", layout).await;
        store
            .append(
                &stream("own"),
                Expected::Any,
                &[event("item.recorded", 0)],
                &meta(&format!("own-{own}"), &serde_json::json!({})),
            )
            .await
            .unwrap();
        // Each run's own write is on stream `own`, so stream `a` keeps its version.
        reads(&store, &root, "own-write", layout).await;
    }
}
