//! The shared exercise against a real PostgreSQL, plus the one property SQLite cannot show.
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run these. Without it they report themselves as not run
//! rather than passing quietly, because a backend nobody exercised is not a backend anybody proved.

use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use postgres::{Client, NoTls};

fn url() -> Option<String> {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").ok()
}

fn store(prefix: &str) -> Option<PostgresEventStore> {
    let url = url()?;
    let store = PostgresEventStore::connect(&url, prefix).expect("a reachable PostgreSQL");
    store.drop_tables().expect("a clean slate");
    let store = PostgresEventStore::connect(&url, prefix).expect("a reachable PostgreSQL");
    Some(store)
}

#[test]
fn the_shared_exercise_passes_on_postgresql() {
    let Some(store) = store("conformance") else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    eventlog_conformance::run(&store);
}

#[test]
fn a_reader_never_skips_an_event_that_committed_late() {
    let Some(url) = url() else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    let prefix = "watermark";
    let setup = PostgresEventStore::connect(&url, prefix).expect("a reachable PostgreSQL");
    setup.drop_tables().expect("a clean slate");
    let store = PostgresEventStore::connect(&url, prefix).expect("a store");
    let tenant = TenantId::new("tenant-a").expect("valid tenant");
    let fast = StreamId::new(tenant.clone(), "item", "fast").expect("valid stream");

    // A second connection takes position 1 and holds its transaction open.
    let mut slow = Client::connect(&url, NoTls).expect("a second connection");
    let mut held = slow.transaction().expect("an open transaction");
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
    .expect("the slow writer takes its position");

    // The second writer starts later, takes position 2, and commits first.
    store
        .append(
            &fast,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 2)],
            &eventlog_conformance::meta("fast", &serde_json::json!({"n": 2})),
        )
        .expect("the fast writer commits");

    // A reader that trusted the sequence alone would take position 2 now and never come back for
    // position 1. It sees nothing instead, and waits.
    let blocked = store.read_feed(&tenant, 0, 10).expect("readable");
    assert!(
        blocked.events.is_empty(),
        "a reader must not move past a position that is still in flight"
    );

    held.commit().expect("the slow writer commits second");

    let page = store.read_feed(&tenant, 0, 10).expect("readable");
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

#[test]
fn the_projection_exercise_passes_on_postgresql() {
    let Some(store) = store("projections") else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["projections_p_tally"]);
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_projections(&store);
}

#[test]
fn the_inline_projection_exercise_passes_on_postgresql() {
    let Some(store) = store("inline") else {
        eprintln!("skipped: EVENTLOG_TEST_POSTGRES_URL is not set");
        return;
    };
    drop_projection_tables(&store, &["inline_p_tally"]);
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(store);
    eventlog_conformance::run_inline_projections(&store);
}

fn drop_projection_tables(store: &PostgresEventStore, tables: &[&str]) {
    let Some(url) = url() else { return };
    let mut client = Client::connect(&url, NoTls).expect("a connection");
    for table in tables {
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .expect("a clean slate");
    }
    let _ = store;
}
