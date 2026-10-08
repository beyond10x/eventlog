//! The comparative capacity lane's connection budget counts the workload's own backends.
//!
//! The lane's server also answers clients that are not the workload: CI's service health check
//! runs `pg_isready` every five seconds, and each probe is a short-lived client backend. A sample
//! that lands on one counted nine against a budget of eight. These cases hold the budget to the
//! tagged workload in both directions: nine tagged backends still fail it, and an untagged
//! bystander next to eight tagged ones does not.
//!
//! The lane also compares `pg_stat_statements` deallocations before and after each
//! configuration, so a collection for the lane resets the statement table first: a table filled
//! by earlier work would otherwise evict an entry inside whichever configuration crossed
//! `pg_stat_statements.max`. A reset the server refuses leaves the opening snapshot unavailable.
//!
//! Set `EVENTLOG_TEST_POSTGRES_URL` to run the database cases; without it they report themselves
//! as not run.

#[path = "../examples/observation/metrics_collector.rs"]
mod metrics_collector;
#[path = "../examples/observation/workload_budget.rs"]
mod workload_budget;

use std::{path::PathBuf, time::Duration};

use eventlog_postgres::PostgresEventStore;
use serde_json::json;
use tokio_postgres::{Client, NoTls};

fn url() -> Option<String> {
    let value = std::env::var("EVENTLOG_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    assert!(
        value.is_some() || std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
        "required PostgreSQL proof cannot skip an absent database URL"
    );
    value
}

async fn client(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One short collection while the given clients are held open.
async fn observe(url: &str, application: &str, name: &str) -> serde_json::Value {
    collect(url.parse().unwrap(), application, name, false).await
}

/// One short collection, resetting `pg_stat_statements` first when asked.
async fn collect(
    postgres: tokio_postgres::Config,
    application: &str,
    name: &str,
    reset_statements: bool,
) -> serde_json::Value {
    let directory = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("capacity-budget-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let stop = directory.join("stop");
    let collector = tokio::spawn(metrics_collector::collect(
        metrics_collector::MetricsConfig {
            postgres,
            cgroup: directory.join("no-cgroup"),
            output: directory.join("metrics.jsonl"),
            stop_file: stop.clone(),
            maximum_duration: Duration::from_secs(10),
            interval: Duration::from_millis(20),
            query_timeout: Duration::from_secs(2),
            attribute_connections_above: 8,
            workload_application_name: application.to_owned(),
            reset_statements,
        },
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::write(&stop, b"complete\n").unwrap();
    let summary = collector.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&directory);
    summary
}

#[test]
fn the_budget_reads_the_workload_maximum_and_nothing_else() {
    let check = |metrics| workload_budget::within_connection_budget(&metrics);
    assert!(check(
        json!({"workload_connections_max": 8, "connections_max_excluding_observer": 8})
    ));
    assert!(
        check(json!({"workload_connections_max": 8, "connections_max_excluding_observer": 9})),
        "a bystander backend is not the workload"
    );
    assert!(
        !check(json!({"workload_connections_max": 9, "connections_max_excluding_observer": 9})),
        "nine workload connections exceed the budget of eight"
    );
    assert!(
        !check(json!({"workload_connections_max": 0, "connections_max_excluding_observer": 8})),
        "a workload never observed under its tag proves nothing about its budget"
    );
    assert!(
        !check(json!({"connections_max_excluding_observer": 8})),
        "an absent workload maximum is not within budget"
    );
}

#[test]
fn a_tagged_url_carries_the_application_name_in_either_form() {
    let tagged = |url: &str| -> String {
        workload_budget::tagged_url(url, workload_budget::WORKLOAD_APPLICATION)
    };
    for url in [
        "postgresql://postgres@127.0.0.1:5432/postgres",
        "postgresql://postgres@127.0.0.1:5432/postgres?connect_timeout=5",
        "host=127.0.0.1 port=5432 user=postgres dbname=postgres",
    ] {
        let config: tokio_postgres::Config = tagged(url).parse().unwrap();
        assert_eq!(
            config.get_application_name(),
            Some(workload_budget::WORKLOAD_APPLICATION),
            "{url}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nine_tagged_backends_fail_the_budget_and_a_bystander_does_not() {
    let Some(url) = url() else {
        eprintln!("skipped: the capacity connection budget requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let application = format!("eventlog-budget-{}", std::process::id());
    let tagged = workload_budget::tagged_url(&url, &application);
    let mut workload = Vec::new();
    for _ in 0..9 {
        workload.push(client(&tagged).await);
    }
    let bystander = client(&url).await;

    let over = observe(&url, &application, "over").await;
    assert_eq!(over["postgres_failures"], 0, "{over}");
    assert_eq!(over["workload_connections_max"], 9, "{over}");
    assert!(
        over["connections_max_excluding_observer"]
            .as_u64()
            .is_some_and(|value| value >= 10),
        "the all-backend count still sees the bystander: {over}"
    );
    assert!(
        !over["over_threshold_samples"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "an over-threshold sample stays attributed: {over}"
    );
    assert!(
        !workload_budget::within_connection_budget(&over),
        "nine workload backends must fail the budget"
    );
    assert!(
        !over.to_string().contains(&application),
        "no application name is retained in the receipt"
    );

    drop(workload.pop());
    let within = observe(&url, &application, "within").await;
    assert_eq!(within["postgres_failures"], 0, "{within}");
    assert_eq!(within["workload_connections_max"], 8, "{within}");
    assert!(
        within["connections_max_excluding_observer"]
            .as_u64()
            .is_some_and(|value| value >= 9),
        "the bystander is still counted among all backends: {within}"
    );
    assert!(
        workload_budget::within_connection_budget(&within),
        "eight workload backends beside a bystander are within budget"
    );
    drop(bystander);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pooled_adapter_keeps_the_tag_on_its_connections() {
    let Some(url) = url() else {
        eprintln!("skipped: the capacity connection tag requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let application = format!("eventlog-budget-pool-{}", std::process::id());
    let store = PostgresEventStore::connect(
        &workload_budget::tagged_url(&url, &application),
        "capacity_budget_tag",
    )
    .await
    .expect("a reachable PostgreSQL");
    let observer = client(&url).await;
    let tagged: i64 = observer
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            &[&application],
        )
        .await
        .unwrap()
        .get(0);
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
    assert!(
        tagged >= 1,
        "the pool's connections carry the URL's application_name"
    );
}

/// Whether the connected database has `pg_stat_statements` installed.
async fn statements_installed(client: &Client) -> bool {
    client
        .query_one(
            "SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_stat_statements')",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_that_resets_statements_starts_from_an_empty_table() {
    let Some(url) = url() else {
        eprintln!("skipped: the statement reset requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let admin = client(&url).await;
    if !statements_installed(&admin).await {
        eprintln!("skipped: the statement reset requires pg_stat_statements");
        return;
    }
    let reset_before: serde_json::Value = admin
        .query_one(
            "SELECT to_jsonb(stats_reset) FROM pg_stat_statements_info",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let application = format!("eventlog-budget-reset-{}", std::process::id());
    let summary = collect(url.parse().unwrap(), &application, "reset", true).await;
    let before = &summary["statements_before"];
    assert_eq!(before["status"], "observed", "{summary}");
    assert_eq!(
        before["value"]["deallocations"], 0,
        "an emptied table has evicted nothing: {summary}"
    );
    assert_ne!(
        before["value"]["stats_reset"], reset_before,
        "the opening snapshot follows a fresh reset: {summary}"
    );
    assert_eq!(
        before["value"]["stats_reset"], summary["statements_after"]["value"]["stats_reset"],
        "nothing resets the table again inside the collection: {summary}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_statement_reset_leaves_the_opening_snapshot_unavailable() {
    let Some(url) = url() else {
        eprintln!("skipped: the statement reset requires EVENTLOG_TEST_POSTGRES_URL");
        return;
    };
    let admin = client(&url).await;
    if !statements_installed(&admin).await {
        eprintln!("skipped: the statement reset requires pg_stat_statements");
        return;
    }
    // A login role without the reset privilege, which PostgreSQL grants to superusers only.
    admin
        .batch_execute(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles \
               WHERE rolname = 'eventlog_test_statement_reader') \
             THEN CREATE ROLE eventlog_test_statement_reader LOGIN; END IF; END $$",
        )
        .await
        .unwrap();
    let mut reader: tokio_postgres::Config = url.parse().unwrap();
    reader.user("eventlog_test_statement_reader");

    let application = format!("eventlog-budget-refused-{}", std::process::id());
    let summary = collect(reader, &application, "refused", true).await;
    assert_eq!(
        summary["statements_before"],
        json!({"status": "unavailable", "error": "statement_reset_failed"}),
        "a refused reset is never an observed snapshot: {summary}"
    );
}
