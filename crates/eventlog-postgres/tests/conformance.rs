//! The shared exercise against a real PostgreSQL, plus the one property SQLite cannot show.
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run these. Without it they report themselves as not run
//! rather than passing quietly, because a backend nobody exercised is not a backend anybody proved.

use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use tokio_postgres::NoTls;

/// The watermark couples feed visibility across every connection in the instance
/// (`pg_snapshot_xmin` is cluster-wide), so a test holding a transaction open while another
/// asserts on its feed is a race by design. One at a time, deterministically.
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The database to exercise against, when one was given.
///
/// An empty value counts as unset. `Ok("")` from the environment would otherwise send the whole
/// suite at an unusable connection string and report a connection failure as a test failure.
fn url() -> Option<String> {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// A bare client for test setup, its connection task spawned like the store spawns its own.
async fn client(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn store(prefix: &str) -> Option<PostgresEventStore> {
    let url = url()?;
    let store = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    store.drop_tables().await.expect("a clean slate");
    let store = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    Some(store)
}

#[tokio::test]
async fn the_shared_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("conformance").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn a_reader_never_skips_an_event_that_committed_late() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let prefix = "watermark";
    let setup = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a reachable PostgreSQL");
    setup.drop_tables().await.expect("a clean slate");
    let store = PostgresEventStore::connect(&url, prefix)
        .await
        .expect("a store");
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    let fast = StreamId::new(tenant.clone(), "item", "fast").expect("valid stream");

    // A second connection takes position 1 and holds its transaction open.
    let mut slow = client(&url).await;
    let held = slow.transaction().await.expect("an open transaction");
    held.execute(
        "INSERT INTO watermark_events (
             tenant_id, stream_type, stream_id, version, event_id, event_name,
             event_schema_version, occurred_at, recorded_at, subject, actor, request_id,
             trace_id, causation_depth, data)
         VALUES ('tenant-a', 'item', 'slow', 1, gen_random_uuid(), 'item.received', 1,
                 now(), now(), 'person-1', 'service-1', 'request-slow', 'trace-slow', 0,
                 '{\"value\": 1}'::jsonb)",
        &[],
    )
    .await
    .expect("the slow writer takes its position");

    // The second writer starts later, takes position 2, and commits first.
    store
        .append(
            &fast,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)],
            &eventlog_conformance::meta("fast", &serde_json::json!({"n": 2})),
        )
        .await
        .expect("the fast writer commits");

    // A reader that trusted the sequence alone would take position 2 now and never come back for
    // position 1. It sees nothing instead, and waits.
    let blocked = store.read_feed(&tenant, 0, 10).await.expect("readable");
    assert!(
        blocked.events.is_empty(),
        "a reader must not move past a position that is still in flight"
    );

    held.commit().await.expect("the slow writer commits second");

    let page = store.read_feed(&tenant, 0, 10).await.expect("readable");
    let seen: Vec<&str> = page
        .events
        .iter()
        .map(|event| event.stream_id.as_str())
        .collect();
    assert_eq!(
        seen,
        vec!["slow", "fast"],
        "both writers are delivered, in position order, once both have committed"
    );
}

#[tokio::test]
async fn the_projection_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("projections").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["projections_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_projections(&store).await;
}

#[tokio::test]
async fn the_inline_projection_exercise_passes_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("inline").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["inline_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_inline_projections(&store).await;
}

async fn drop_projection_tables(store: &PostgresEventStore, tables: &[&str]) {
    let Some(url) = url() else { return };
    let client = client(&url).await;
    for table in tables {
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await
            .expect("a clean slate");
    }
    let _ = store;
}

#[tokio::test]
async fn the_paging_rule_holds_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("paging").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["paging_p_tally"]).await;
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn the_claim_rule_holds_on_postgresql() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(store) = store("claims").await else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run_claims(&store).await;
}

/// The same refusal, where it matters most: a hosted deployment points the log at the database the
/// module already had.
#[tokio::test]
async fn a_table_of_ours_that_somebody_else_made_is_refused_by_name() {
    let _exclusive = EXCLUSIVE.lock().await;
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let client = client(&url).await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS previous_events;
             CREATE TABLE previous_events (
                 tenant_id TEXT NOT NULL,
                 sequence BIGINT NOT NULL,
                 body JSONB NOT NULL
             );",
        )
        .await
        .expect("the previous store's table");

    let Err(refused) = PostgresEventStore::connect(&url, "previous").await else {
        panic!("a table this kit did not create must not be silently adopted");
    };
    let message = refused.to_string();
    assert!(
        message.contains("previous_events"),
        "the refusal names the table: {message}"
    );
    client
        .batch_execute("DROP TABLE IF EXISTS previous_events")
        .await
        .expect("cleaned up");
}
