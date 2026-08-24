//! Every store call happens inside a running tokio runtime — the exact shape that panicked in
//! the consuming modules at startup when the sync/async bridge was still the caller's job.
//!
//! The bridge lives inside the crate now, so the whole surface must complete here without a
//! panic, on both runtime flavours a consumer might run.

use std::sync::Arc;

use eventlog_core::EventStore;
use eventlog_sqlite::SqliteEventStore;
use serde_json::json;

async fn fresh() -> SqliteEventStore {
    SqliteEventStore::in_memory("corpus")
        .await
        .expect("a store")
}

/// Every method of the store surface, from inside the runtime.
///
/// The shared exercises reach every method but one; `recorded_command` is otherwise only reached
/// through the repository, so it is called directly here.
async fn exercise_every_method() {
    let store = fresh().await;
    eventlog_conformance::run(&store).await;
    eventlog_conformance::run_claims(&store).await;

    let body = json!({ "command": "create" });
    let tenant = eventlog_core::TenantId::new("tenant-a").expect("valid tenant");
    let stream = eventlog_core::StreamId::new(tenant, "item", "item-1").expect("valid stream");
    let recorded = store
        .recorded_command(
            &stream,
            "key-1",
            &eventlog_core::request_hash(&body).expect("hashable"),
        )
        .await
        .expect("readable");
    assert!(
        recorded.is_some_and(|result| result.deduplicated),
        "the exercise's first command is on record"
    );

    let store: Arc<dyn EventStore> = Arc::new(fresh().await);
    eventlog_conformance::run_projections(&store).await;
    let store: Arc<dyn EventStore> = Arc::new(fresh().await);
    eventlog_conformance::run_inline_projections(&store).await;
    let store: Arc<dyn EventStore> = Arc::new(fresh().await);
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn every_store_method_completes_on_a_current_thread_runtime() {
    exercise_every_method().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_store_method_completes_on_a_multi_thread_runtime() {
    exercise_every_method().await;
}
