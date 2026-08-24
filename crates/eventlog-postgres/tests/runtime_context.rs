//! Every store call happens inside a running tokio runtime — the exact shape that panicked in
//! the consuming modules at startup ("Cannot start a runtime from within a runtime").
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to exercise every store method against a real database.
//! Without it, the constructor still runs inside the runtime and must come back with an error
//! value rather than panic the worker: the sync driver's panic fired before any packet was sent,
//! so no database is needed to reproduce the bug class this pins down.

use std::sync::Arc;

use eventlog_core::EventStore;
use eventlog_postgres::PostgresEventStore;
use serde_json::json;
use tokio_postgres::NoTls;

/// The watermark couples feed visibility across every connection in the instance
/// (`pg_snapshot_xmin` is cluster-wide), so a test holding a transaction open while another
/// asserts on its feed is a race by design. One at a time, deterministically.
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn url() -> Option<String> {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// A store on its own prefix with a clean slate, tally table included.
async fn fresh(url: &str, prefix: &str) -> PostgresEventStore {
    let store = PostgresEventStore::connect(url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    store.drop_tables().await.expect("a clean slate");
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_p_tally"))
        .await
        .expect("a clean slate");
    PostgresEventStore::connect(url, prefix)
        .await
        .expect("a reachable PostgreSQL")
}

/// Every method of the store surface, from inside the runtime.
///
/// The shared exercises reach every method but one; `recorded_command` is otherwise only reached
/// through the repository, so it is called directly here. `tag` keeps the two runtime flavours,
/// which run concurrently in this binary, off each other's tables.
async fn exercise_every_method(tag: &str) {
    let Some(url) = url() else {
        // The sync driver panicked right here — before any packet — so even with no database
        // the constructor proves the class: an error value, not a panic.
        let refused = PostgresEventStore::connect("postgres://127.0.0.1:1/none", "runtime").await;
        assert!(
            refused.is_err(),
            "an unreachable database is an error, not a panic"
        );
        eprintln!("partly skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };

    let store = fresh(&url, &format!("rt_{tag}")).await;
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

    let store: Arc<dyn EventStore> = Arc::new(fresh(&url, &format!("rt_{tag}_p")).await);
    eventlog_conformance::run_projections(&store).await;
    let store: Arc<dyn EventStore> = Arc::new(fresh(&url, &format!("rt_{tag}_i")).await);
    eventlog_conformance::run_inline_projections(&store).await;
    let store: Arc<dyn EventStore> = Arc::new(fresh(&url, &format!("rt_{tag}_g")).await);
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn every_store_method_completes_on_a_current_thread_runtime() {
    let _exclusive = EXCLUSIVE.lock().await;
    exercise_every_method("current").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_store_method_completes_on_a_multi_thread_runtime() {
    let _exclusive = EXCLUSIVE.lock().await;
    exercise_every_method("multi").await;
}
