//! The prepared find of one pooled connection, against a real server.
//!
//! These tests read the state of a pooled session (`pg_prepared_statements`, the session's
//! settings) and of its statement cache, which an external test binary cannot reach. Each starts
//! with the skip guard of the pool tests: an absent `EVENTLOG_TEST_POSTGRES_URL` skips, and fails
//! when `EVENTLOG_REQUIRE_POSTGRES` is set.
use super::{
    PoolOptions, PostgresConfig, PostgresEventStore, backend,
    pool::{AuthorizedConnection, PostgresConnectionAuthority, PostgresTransportAssurance},
    projection_indexes::{connect, drop_owner},
    projection_table, schema,
    statement_cache::{CAPACITY, Counts},
};
use eventlog_core::{BoxFuture, EventLogError, EventStore, ProjectionSpec, TenantId};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_postgres::{Client, NoTls};

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "find_rows",
    indexed: &["kind"],
};

const SECOND: ProjectionSpec = ProjectionSpec {
    name: "find_rows_second",
    indexed: &["kind"],
};

const FIELDS: [&str; 8] = ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"];

fn url() -> Option<String> {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    assert!(
        url.is_some() || std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
        "required PostgreSQL proof cannot skip an absent database URL"
    );
    if url.is_none() {
        eprintln!("skipped: prepared finds need EVENTLOG_TEST_POSTGRES_URL");
    }
    url
}

fn with_application(url: &str, application: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={application}")
}

/// A fresh owner: its tables for `specs`, every row of the one table `ROWS` seeded.
async fn owner(url: &str, prefix: &str, specs: &[ProjectionSpec]) -> Client {
    let mut sql = connect(url).await;
    let refs: Vec<&ProjectionSpec> = specs.iter().collect();
    drop_owner(&sql, prefix, &refs).await;
    schema::migrate(&mut sql, prefix, specs).await.unwrap();
    sql
}

async fn seed_items(sql: &Client, prefix: &str, spec: &ProjectionSpec, rows: u32) {
    let table = projection_table(prefix, spec.name);
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
         SELECT 'tenant', 'key-' || lpad(g::text, 4, '0'),
                jsonb_build_object('kind', 'item', 'n', g), 'item'
         FROM generate_series(1, {rows}) AS g"
    ))
    .await
    .unwrap();
}

async fn one_connection(url: &str, prefix: &str, application: &str) -> PostgresEventStore {
    PostgresEventStore::connect_local(
        &with_application(url, application),
        prefix,
        PoolOptions {
            max_connections: 1,
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap()
}

fn tenant() -> TenantId {
    TenantId::new("tenant").unwrap()
}

async fn find(
    store: &PostgresEventStore,
    spec: &ProjectionSpec,
    field: &str,
    value: &str,
) -> Result<Vec<Value>, EventLogError> {
    store
        .projection_find(spec, &tenant(), field, value, 10)
        .await
}

async fn items(store: &PostgresEventStore) -> Vec<Value> {
    let found = find(store, &ROWS, "kind", "item").await.unwrap();
    assert_eq!(found.len(), 10, "the find returns the ten items");
    found
}

/// The one pooled session as the server and the cache see it.
#[derive(Debug)]
struct Session {
    prepared: i64,
    counts: Counts,
    refused: bool,
    prepared_texts: usize,
}

async fn session(store: &PostgresEventStore) -> Session {
    let mut lease = store.pool.acquire().await.unwrap();
    let prepared: i64 = lease
        .query_typed("SELECT count(*) FROM pg_prepared_statements", &[])
        .await
        .unwrap()[0]
        .get(0);
    let cache = lease.statements();
    let seen = Session {
        prepared,
        counts: cache.counts(),
        refused: cache.is_refused(),
        prepared_texts: cache.prepared_texts(),
    };
    lease.settled();
    seen
}

/// The server process of the one pooled session.
async fn backend_pid(store: &PostgresEventStore) -> i32 {
    let mut lease = store.pool.acquire().await.unwrap();
    let pid: i32 = lease
        .query_typed("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()[0]
        .get(0);
    lease.settled();
    pid
}

async fn setting(store: &PostgresEventStore, name: &str) -> String {
    let mut lease = store.pool.acquire().await.unwrap();
    let value: String = lease
        .query_typed(
            "SELECT current_setting($1)",
            &[(&name, tokio_postgres::types::Type::TEXT)],
        )
        .await
        .unwrap()[0]
        .get(0);
    lease.settled();
    value
}

async fn finish(store: PostgresEventStore, sql: &Client, prefix: &str, specs: &[ProjectionSpec]) {
    store.shutdown().await.unwrap();
    let refs: Vec<&ProjectionSpec> = specs.iter().collect();
    drop_owner(sql, prefix, &refs).await;
}

async fn backends(observer: &Client, application: &str) -> Vec<i32> {
    observer
        .query(
            "SELECT pid FROM pg_stat_activity WHERE application_name = $1 ORDER BY pid",
            &[&application],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// Polls until none of `pids` is a backend of `application` any more.
async fn await_gone(observer: &Client, application: &str, pids: &[i32]) {
    for _ in 0..100 {
        let alive = backends(observer, application).await;
        if pids.iter().all(|pid| !alive.contains(pid)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("backends {pids:?} of {application} are still there");
}

#[tokio::test]
async fn a_find_is_prepared_on_its_second_use_and_reused() {
    let Some(url) = url() else { return };
    let prefix = "find_second_use";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 40).await;
    let store = one_connection(&url, prefix, "find_second_use").await;

    items(&store).await;
    let first = session(&store).await;
    assert_eq!(
        first.prepared, 0,
        "the first find is not prepared: {first:?}"
    );
    assert_eq!(first.counts, Counts::default());

    items(&store).await;
    let second = session(&store).await;
    assert_eq!(second.prepared, 1, "the second find prepares: {second:?}");
    assert_eq!(second.counts.prepared, 1);

    for _ in 0..18 {
        items(&store).await;
    }
    let after = session(&store).await;
    assert_eq!(
        after.prepared, 1,
        "twenty finds keep one statement: {after:?}"
    );
    assert_eq!(
        after.counts,
        Counts {
            prepare_attempts: 1,
            prepared: 1,
            evicted: 0,
            refused: 0
        }
    );
    finish(store, &sql, prefix, &[ROWS]).await;
}

/// The skewed fixture: the large tenant holds 25,000 rows and the value `rare` only in its last
/// ten, 63 small tenants hold 75 rows each. A custom plan reads the value's digest index, a
/// generic plan walks the tenant's primary key.
#[tokio::test]
async fn a_prepared_find_never_switches_to_a_generic_plan() {
    let Some(url) = url() else { return };
    let prefix = "find_custom_plan";
    let sql = owner(&url, prefix, &[ROWS]).await;
    let table = projection_table(prefix, ROWS.name);
    let kind = "CASE WHEN g > 24990 THEN 'rare' WHEN g % 2 = 0 THEN 'open' ELSE 'closed' END";
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
         SELECT 'big', 'key-' || lpad(g::text, 6, '0'),
                jsonb_build_object('kind', {kind}, 'n', g), {kind}
         FROM generate_series(1, 25000) AS g"
    ))
    .await
    .unwrap();
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
         SELECT 'small-' || lpad(t::text, 2, '0'), 'key-' || lpad(g::text, 6, '0'),
                jsonb_build_object('kind', 'open', 'n', g), 'open'
         FROM generate_series(1, 63) AS t, generate_series(1, 75) AS g"
    ))
    .await
    .unwrap();
    sql.batch_execute(&format!("VACUUM (ANALYZE) {table}"))
        .await
        .unwrap();
    let store = one_connection(&url, prefix, "find_custom_plan").await;
    let big = TenantId::new("big").unwrap();
    for _ in 0..30 {
        let found = store
            .projection_find(&ROWS, &big, "kind", "rare", 10)
            .await
            .unwrap();
        assert_eq!(found.len(), 10);
    }
    assert_eq!(
        setting(&store, "plan_cache_mode").await,
        "force_custom_plan"
    );
    let mut lease = store.pool.acquire().await.unwrap();
    let version: i32 = lease
        .query_typed("SELECT current_setting('server_version_num')::int", &[])
        .await
        .unwrap()[0]
        .get(0);
    if version >= 140_000 {
        let plans = lease
            .query_typed(
                "SELECT generic_plans, custom_plans FROM pg_prepared_statements",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(plans.len(), 1, "one prepared find");
        assert_eq!(
            (plans[0].get::<_, i64>(0), plans[0].get::<_, i64>(1)),
            (0, 29),
            "finds 2 to 30 are the prepared executions, every one planned for its value"
        );
    }
    lease.settled();
    drop(lease);
    finish(store, &sql, prefix, &[ROWS]).await;
}

#[tokio::test]
async fn a_find_survives_ddl_that_changes_the_cached_result_type() {
    let Some(url) = url() else { return };
    let prefix = "find_result_type";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 20).await;
    let store = one_connection(&url, prefix, "find_result_type").await;
    let before = items(&store).await;
    assert_eq!(items(&store).await, before);
    assert_eq!(session(&store).await.prepared, 1);

    sql.batch_execute(&format!(
        "ALTER TABLE {} ALTER COLUMN body TYPE json USING body::json",
        projection_table(prefix, ROWS.name)
    ))
    .await
    .unwrap();
    assert_eq!(
        items(&store).await,
        before,
        "the find returns the same rows after the column changed type"
    );
    let stale = session(&store).await;
    assert_eq!(stale.counts.evicted, 1, "{stale:?}");
    assert!(!stale.refused, "a stale statement refuses nothing");
    assert_eq!(stale.prepared_texts, 0);
    assert_eq!(
        stale.prepared, 0,
        "the stale statement is closed: {stale:?}"
    );
    assert_eq!(store.pool_churn().retired_unsettled_lease, 0);

    items(&store).await;
    assert_eq!(session(&store).await.prepared, 0, "first use again");
    items(&store).await;
    let again = session(&store).await;
    assert_eq!(again.prepared, 1, "prepared again at its second use");
    assert_eq!(again.counts.evicted, 1, "one stale statement in all");
    finish(store, &sql, prefix, &[ROWS]).await;
}

#[tokio::test]
async fn a_find_after_a_dropped_digest_column_fails_once_and_the_same_way_as_unprepared() {
    let Some(url) = url() else { return };
    let prefix = "find_dropped_digest";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 20).await;
    let store = one_connection(&url, prefix, "find_dropped_digest").await;
    items(&store).await;
    items(&store).await;
    assert_eq!(session(&store).await.prepared, 1);
    // Opened before the column goes: opening a store admits the table's shape.
    let fresh = one_connection(&url, prefix, "find_dropped_digest_fresh").await;

    sql.batch_execute(&format!(
        "ALTER TABLE {} DROP COLUMN idx_0_digest",
        projection_table(prefix, ROWS.name)
    ))
    .await
    .unwrap();
    let prepared_error = find(&store, &ROWS, "kind", "item").await.unwrap_err();
    assert!(
        matches!(&prepared_error, EventLogError::Backend(text) if text.contains("SQLSTATE 42703")),
        "{prepared_error:?}"
    );
    assert_eq!(
        store.pool_churn().retired_unsettled_lease,
        1,
        "the failed find retires its connection, and the cache with it"
    );

    let unprepared_error = find(&fresh, &ROWS, "kind", "item").await.unwrap_err();
    assert_eq!(
        format!("{prepared_error:?}"),
        format!("{unprepared_error:?}"),
        "the prepared path reports the error the unprepared path reports"
    );
    let replacement = session(&store).await;
    assert_eq!(
        replacement.counts,
        Counts::default(),
        "the replacement connection starts empty"
    );
    fresh.shutdown().await.unwrap();
    finish(store, &sql, prefix, &[ROWS]).await;
}

#[tokio::test]
async fn a_connection_retired_by_a_deadline_takes_its_statements_with_it() {
    let Some(url) = url() else { return };
    let prefix = "find_retired";
    let application = "find_retired";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 20).await;
    let store = PostgresEventStore::connect_local(
        &with_application(&url, application),
        prefix,
        PoolOptions {
            max_connections: 1,
            lock_timeout: Duration::from_millis(300),
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap();
    items(&store).await;
    items(&store).await;
    assert_eq!(session(&store).await.prepared, 1);
    let held = [backend_pid(&store).await];

    sql.batch_execute(&format!(
        "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
        projection_table(prefix, ROWS.name)
    ))
    .await
    .unwrap();
    let blocked = find(&store, &ROWS, "kind", "item").await;
    assert!(
        matches!(blocked, Err(EventLogError::Deadline { .. })),
        "the prepared find waits for the lock past its bound: {blocked:?}"
    );
    sql.batch_execute("ROLLBACK").await.unwrap();
    await_gone(&sql, application, &held).await;

    items(&store).await;
    let replacement = session(&store).await;
    assert_eq!(replacement.prepared, 0, "{replacement:?}");
    assert_eq!(replacement.counts, Counts::default());
    items(&store).await;
    let warmed = session(&store).await;
    assert_eq!(warmed.prepared, 1, "{warmed:?}");
    assert_eq!(
        warmed.counts,
        Counts {
            prepare_attempts: 1,
            prepared: 1,
            evicted: 0,
            refused: 0
        },
        "no stale or refused statement on the replacement"
    );
    finish(store, &sql, prefix, &[ROWS]).await;
}

#[tokio::test]
async fn two_projections_with_the_same_layout_keep_their_own_statements() {
    let Some(url) = url() else { return };
    let specs = [ROWS, SECOND];
    let prefix = "find_two_tables";
    let other = "find_two_tables_other";
    let sql = owner(&url, prefix, &specs).await;
    let sql_other = owner(&url, other, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 12).await;
    seed_items(&sql, prefix, &SECOND, 3).await;
    seed_items(&sql_other, other, &ROWS, 5).await;
    let store = one_connection(&url, prefix, "find_two_tables").await;
    let second_store = one_connection(&url, other, "find_two_tables_other").await;
    for _ in 0..4 {
        assert_eq!(find(&store, &ROWS, "kind", "item").await.unwrap().len(), 10);
        assert_eq!(
            find(&store, &SECOND, "kind", "item").await.unwrap().len(),
            3
        );
        assert_eq!(
            find(&second_store, &ROWS, "kind", "item")
                .await
                .unwrap()
                .len(),
            5
        );
    }
    let held = session(&store).await;
    assert_eq!(held.prepared, 2, "one statement per table: {held:?}");
    assert_eq!(held.prepared_texts, 2);
    assert_eq!(session(&second_store).await.prepared, 1);
    second_store.shutdown().await.unwrap();
    drop_owner(&sql_other, other, &[&ROWS]).await;
    finish(store, &sql, prefix, &specs).await;
}

#[tokio::test]
async fn a_session_that_refuses_named_statements_still_finds() {
    let Some(url) = url() else { return };
    let prefix = "find_refusing";
    let application = "find_refusing";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 20).await;

    // (a) The server answers the prepare with 42P05, a name already present on a multiplexed
    // session: the find answers, the connection is refused, and no later find tries to prepare.
    let store = one_connection(&url, prefix, application).await;
    let expected = items(&store).await;
    store.pool.fail_next_prepare("42P05");
    assert_eq!(
        items(&store).await,
        expected,
        "the refused prepare still answers"
    );
    for _ in 0..4 {
        assert_eq!(items(&store).await, expected);
    }
    let refused = session(&store).await;
    assert!(refused.refused, "{refused:?}");
    assert_eq!(refused.prepared, 0);
    assert_eq!(
        refused.counts,
        Counts {
            prepare_attempts: 1,
            prepared: 0,
            evicted: 0,
            refused: 1
        },
        "one attempt, then none"
    );
    store.shutdown().await.unwrap();

    // (b) A prepare that waits for a lock past its bound is a deadline, returned as it is. It
    // refuses nothing: the replacement connection prepares at its second use.
    let store = PostgresEventStore::connect_local(
        &with_application(&url, application),
        prefix,
        PoolOptions {
            max_connections: 1,
            lock_timeout: Duration::from_millis(300),
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap();
    items(&store).await;
    sql.batch_execute(&format!(
        "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
        projection_table(prefix, ROWS.name)
    ))
    .await
    .unwrap();
    let blocked = find(&store, &ROWS, "kind", "item").await;
    assert!(
        matches!(blocked, Err(EventLogError::Deadline { .. })),
        "{blocked:?}"
    );
    sql.batch_execute("ROLLBACK").await.unwrap();
    for _ in 0..3 {
        assert_eq!(items(&store).await, expected);
    }
    let replacement = session(&store).await;
    assert!(!replacement.refused, "{replacement:?}");
    assert_eq!(replacement.prepared, 1);
    store.shutdown().await.unwrap();

    // (c) The session loses its statements, as under a real `DEALLOCATE ALL`: the bind of the
    // cached statement fails with 26000, the find answers, and nothing is prepared again.
    let store = one_connection(&url, prefix, application).await;
    items(&store).await;
    items(&store).await;
    assert_eq!(session(&store).await.prepared, 1);
    let mut lease = store.pool.acquire().await.unwrap();
    lease.batch_execute("DEALLOCATE ALL").await.unwrap();
    lease.settled();
    drop(lease);
    assert_eq!(
        items(&store).await,
        expected,
        "the find answers after the loss"
    );
    for _ in 0..3 {
        assert_eq!(items(&store).await, expected);
    }
    let lost = session(&store).await;
    assert!(lost.refused, "{lost:?}");
    assert_eq!(lost.prepared, 0, "nothing is prepared again");
    assert_eq!(lost.counts.prepare_attempts, 1);
    assert_eq!(lost.counts.refused, 1);
    finish(store, &sql, prefix, &[ROWS]).await;
}

struct Plain {
    url: String,
}

impl PostgresConnectionAuthority for Plain {
    fn assurance(&self) -> PostgresTransportAssurance {
        PostgresTransportAssurance::Isolated
    }

    fn connect(
        &self,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<AuthorizedConnection, EventLogError>> {
        Box::pin(async move {
            let (client, driver) = tokio_postgres::connect(&self.url, NoTls)
                .await
                .map_err(backend)?;
            Ok(AuthorizedConnection::new(client, driver))
        })
    }
}

#[tokio::test]
async fn a_caller_owned_connection_gets_the_same_session_settings() {
    let Some(url) = url() else { return };
    let prefix = "find_caller_owned";
    let sql = owner(&url, prefix, &[ROWS]).await;
    seed_items(&sql, prefix, &ROWS, 20).await;
    let config =
        PostgresConfig::caller_owned("public", prefix, Arc::new(Plain { url: url.clone() }))
            .unwrap();
    let store = PostgresEventStore::local(
        config,
        PoolOptions {
            max_connections: 1,
            ..PoolOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        setting(&store, "plan_cache_mode").await,
        "force_custom_plan"
    );
    items(&store).await;
    items(&store).await;
    assert_eq!(
        session(&store).await.prepared,
        1,
        "a caller-owned session prepares too"
    );
    finish(store, &sql, prefix, &[ROWS]).await;
}

#[tokio::test]
async fn a_store_with_more_than_64_find_texts_keeps_finding() {
    let Some(url) = url() else { return };
    let prefix = "find_cap";
    let specs: Vec<ProjectionSpec> = (0..9)
        .map(|n| ProjectionSpec {
            name: Box::leak(format!("cap_{n}").into_boxed_str()),
            indexed: &FIELDS,
        })
        .collect();
    let sql = owner(&url, prefix, &specs).await;
    for spec in &specs {
        sql.batch_execute(&format!(
            "INSERT INTO {} (tenant_id, row_key, body, idx_0, idx_1, idx_2, idx_3, idx_4, idx_5,
                             idx_6, idx_7)
             VALUES ('tenant', 'key', jsonb_build_object('projection', '{}'),
                     'v', 'v', 'v', 'v', 'v', 'v', 'v', 'v')",
            projection_table(prefix, spec.name),
            spec.name
        ))
        .await
        .unwrap();
    }
    let store = one_connection(&url, prefix, "find_cap").await;
    let texts = specs.len() * FIELDS.len();
    assert!(texts > CAPACITY);
    for pass in 0..4 {
        for spec in &specs {
            for field in FIELDS {
                assert_eq!(
                    find(&store, spec, field, "v").await.unwrap(),
                    [json!({ "projection": spec.name })],
                    "{} {field}, pass {pass}",
                    spec.name
                );
            }
        }
    }
    let held = session(&store).await;
    assert_eq!(held.prepared, i64::try_from(CAPACITY).unwrap(), "{held:?}");
    assert_eq!(held.prepared_texts, CAPACITY);
    assert_eq!(
        held.counts,
        Counts {
            prepare_attempts: CAPACITY,
            prepared: CAPACITY,
            evicted: 0,
            refused: 0
        },
        "the cap neither evicts nor refuses"
    );
    finish(store, &sql, prefix, &specs).await;
}
