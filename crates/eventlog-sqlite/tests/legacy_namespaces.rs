//! Owner namespaces stay confined when one SQLite file hosts several owners: legacy erasure,
//! physical projection table names, rebuild staging and the projection index.
use eventlog_core::{
    BoxFuture, CaptureLimits, ConsistentTenantCapture, EventLogError, EventStore, Expected,
    InlineProjectionAdmin, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId,
    TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use std::sync::Arc;

async fn seed(path: &str, prefix: &str, tenant: &TenantId) -> SqliteEventStore {
    let store = SqliteEventStore::open(path, prefix).await.expect("owner");
    store
        .register_inline(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("projection");
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("one", &serde_json::json!({})),
        )
        .await
        .expect("source state");
    store
}

#[tokio::test]
async fn legacy_erasure_matches_literal_underscores_and_preserves_other_owners() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("owners.sqlite");
    let path = database.to_str().expect("path");
    let tenant = TenantId::new("shared-tenant").expect("tenant");
    drop(seed(path, "owner_a", &tenant).await);
    let other = seed(path, "ownerxa", &tenant).await;
    let sql = rusqlite::Connection::open(&database).expect("source fixture");
    sql.execute_batch("DROP TABLE owner_a_projection_registry; DROP TABLE owner_a_scope_counters")
        .expect("old owner roster");
    let reopened = SqliteEventStore::open(path, "owner_a")
        .await
        .expect("legacy owner");
    reopened.forget_tenant(&tenant).await.expect("erasure");
    assert!(
        reopened
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
            .await
            .expect("erased projection")
            .is_none()
    );
    assert!(
        other
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
            .await
            .expect("other owner projection")
            .is_some()
    );
    let counts: (i64, i64) = sql
        .query_row(
            "SELECT (SELECT COUNT(*) FROM owner_a_events), (SELECT COUNT(*) FROM ownerxa_events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("retained source rows");
    assert_eq!(counts, (0, 1));
}

#[tokio::test]
async fn ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("nested.sqlite");
    let path = database.to_str().expect("path");
    let tenant = TenantId::new("shared-tenant").expect("tenant");
    drop(seed(path, "owner", &tenant).await);
    let other = seed(path, "owner_p_nested", &tenant).await;
    let sql = rusqlite::Connection::open(&database).expect("source fixture");
    sql.execute_batch("DROP TABLE owner_projection_registry; DROP TABLE owner_scope_counters")
        .expect("old owner roster");
    let reopened = SqliteEventStore::open(path, "owner")
        .await
        .expect("legacy owner");
    assert!(matches!(
        reopened.forget_tenant(&tenant).await,
        Err(EventLogError::Invalid(message)) if message.contains("ambiguous legacy projection")
    ));
    for store in [&reopened, &other] {
        assert!(
            store
                .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
                .await
                .expect("rolled back projection")
                .is_some()
        );
    }
    let counts: (i64, i64, i64) = sql
        .query_row(
            "SELECT (SELECT COUNT(*) FROM owner_events), (SELECT COUNT(*) FROM owner_commands), (SELECT COUNT(*) FROM owner_p_nested_events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("rolled back source and receipts");
    assert_eq!(counts, (1, 1, 1));
}

/// One projection under a chosen name, writing one row per event.
struct Named {
    name: &'static str,
    specs: &'static [ProjectionSpec],
}

const P_B: &[ProjectionSpec] = &[ProjectionSpec {
    name: "p_b",
    indexed: &[],
}];
const B: &[ProjectionSpec] = &[ProjectionSpec {
    name: "b",
    indexed: &[],
}];
const X_EVENTS: &[ProjectionSpec] = &[ProjectionSpec {
    name: "x_events",
    indexed: &[],
}];
const TALLY_ONLY: &[ProjectionSpec] = &[ProjectionSpec {
    name: "tally",
    indexed: &[],
}];

impl Projector for Named {
    fn name(&self) -> &'static str {
        self.name
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        self.specs
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(
                    &self.specs[0],
                    &event.tenant,
                    &event.stream_id,
                    &serde_json::json!({ "owner": self.name }),
                )
                .await
        })
    }
}

fn registered(path: &std::path::Path, prefix: &str) -> Vec<String> {
    let sql = rusqlite::Connection::open(path).expect("fixture connection");
    let mut statement = sql
        .prepare(&format!(
            "SELECT projection_name FROM {prefix}_projection_registry ORDER BY projection_name"
        ))
        .expect("registry");
    statement
        .query_map([], |row| row.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("names")
}

fn refusal(result: Result<(), EventLogError>) -> String {
    match result {
        Err(EventLogError::Invalid(message)) => message,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn two_owners_cannot_share_one_physical_projection_table() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("shared-name.sqlite");
    let path = database.to_str().expect("path");
    let short = SqliteEventStore::open(path, "a").await.expect("owner a");
    short
        .register_inline(Arc::new(Named {
            name: "pb",
            specs: P_B,
        }))
        .await
        .expect("owner a's projection p_b is table a_p_p_b while no owner a_p exists");
    let long = SqliteEventStore::open(path, "a_p")
        .await
        .expect("owner a_p");
    let message = refusal(
        long.register_inline(Arc::new(Named {
            name: "b",
            specs: B,
        }))
        .await,
    );
    assert!(
        message.contains("a_p_p_b") && message.contains("owner a's projection p_b"),
        "the refusal names the table and the owner that has it: {message}"
    );
    assert_eq!(
        registered(&database, "a_p"),
        Vec::<String>::new(),
        "the refused registration rolled back"
    );

    // The same collision in the other order: the longer owner claims first.
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("shared-name-reversed.sqlite");
    let path = database.to_str().expect("path");
    let short = SqliteEventStore::open(path, "a").await.expect("owner a");
    let long = SqliteEventStore::open(path, "a_p")
        .await
        .expect("owner a_p");
    long.register_inline(Arc::new(Named {
        name: "b",
        specs: B,
    }))
    .await
    .expect("owner a_p's projection b is table a_p_p_b");
    let message = refusal(
        short
            .register_inline(Arc::new(Named {
                name: "pb",
                specs: P_B,
            }))
            .await,
    );
    assert!(
        message.contains("a_p_p_b") && message.contains("owner a_p's projection b"),
        "{message}"
    );
    assert_eq!(registered(&database, "a"), Vec::<String>::new());
}

#[tokio::test]
async fn a_collision_made_before_the_roster_existed_is_refused_at_the_next_registration() {
    for (first, second, holder) in [
        ("a", "a_p", "owner a's projection p_b"),
        ("a_p", "a", "owner a_p's projection b"),
    ] {
        let directory = tempfile::tempdir().expect("assigned TMPDIR");
        let database = directory.path().join("before-roster.sqlite");
        let path = database.to_str().expect("path");
        let projector = |prefix: &str| -> Arc<dyn Projector> {
            if prefix == "a" {
                Arc::new(Named {
                    name: "pb",
                    specs: P_B,
                })
            } else {
                Arc::new(Named {
                    name: "b",
                    specs: B,
                })
            }
        };
        let earlier = SqliteEventStore::open(path, first)
            .await
            .expect("first owner");
        earlier
            .register_inline(projector(first))
            .await
            .expect("first registration");
        // A file written by a kit without the roster: the table exists, nobody claimed it.
        rusqlite::Connection::open(&database)
            .expect("fixture connection")
            .execute_batch("DROP TABLE eventlog_table_owners")
            .expect("no roster yet");
        let later = SqliteEventStore::open(path, second)
            .await
            .expect("second owner");
        let message = refusal(later.register_inline(projector(second)).await);
        assert!(
            message.contains("a_p_p_b") && message.contains(holder),
            "{first} then {second}: {message}"
        );
    }
}

#[tokio::test]
async fn a_projection_table_cannot_take_another_owners_table_name() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("core-name.sqlite");
    let path = database.to_str().expect("path");
    drop(
        SqliteEventStore::open(path, "a_p_x")
            .await
            .expect("owner a_p_x"),
    );
    let short = SqliteEventStore::open(path, "a").await.expect("owner a");
    let message = refusal(
        short
            .register_inline(Arc::new(Named {
                name: "xevents",
                specs: X_EVENTS,
            }))
            .await,
    );
    assert!(
        message.contains("a_p_x_events") && message.contains("namespace of owner a_p_x"),
        "{message}"
    );

    // A nested owner whose tables cannot meet this name does not block it.
    drop(
        SqliteEventStore::open(path, "a_p")
            .await
            .expect("owner a_p"),
    );
    short
        .register_inline(Arc::new(Named {
            name: "tally",
            specs: TALLY_ONLY,
        }))
        .await
        .expect("a_p_tally is no table owner a_p can have");
}

/// An owner named like the rebuild's staging prefix had its live and staging tables under one
/// unqualified name; SQLite resolved the publication to the staging table and the rebuild reported
/// success while the live view stayed as it was.
#[tokio::test]
async fn a_rebuild_publishes_into_the_live_view_when_the_owner_is_named_like_its_staging() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("staging-name.sqlite");
    let path = database.to_str().expect("path");
    let tenant = TenantId::new("staging-tenant").expect("tenant");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("stream");

    let store = SqliteEventStore::open(path, "eventlog_rebuild")
        .await
        .expect("owner");
    store
        .create_projections(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("catch-up tables");
    for key in ["one", "two"] {
        store
            .append(
                &stream,
                Expected::Any,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta(key, &serde_json::json!({})),
            )
            .await
            .expect("append");
    }
    let applied = store
        .rebuild_projection(Arc::new(eventlog_conformance::Tally), &tenant)
        .await
        .expect("rebuild");
    assert_eq!(applied, 2);
    let row = store
        .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
        .await
        .expect("readable")
        .expect("the rebuilt row is in the live table");
    assert_eq!(row["count"], 2);

    // The inline administration rebuild stages the same way.
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("staging-name-inline.sqlite");
    let path = database.to_str().expect("path");
    let store = SqliteEventStore::open(path, "eventlog_rebuild")
        .await
        .expect("owner");
    store
        .register_inline(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("inline tables");
    for key in ["one", "two"] {
        store
            .append(
                &stream,
                Expected::Any,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta(key, &serde_json::json!({})),
            )
            .await
            .expect("append");
    }
    rusqlite::Connection::open(&database)
        .expect("fixture connection")
        .execute_batch("UPDATE eventlog_rebuild_p_tally SET body = json_set(body, '$.count', 99)")
        .expect("drifted live row");
    store
        .rebuild_inline_projection("tally", &tenant)
        .await
        .expect("inline rebuild");
    let row = store
        .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
        .await
        .expect("readable")
        .expect("row");
    assert_eq!(row["count"], 2, "the rebuild replaced the drifted live row");
}

fn index_columns(sql: &rusqlite::Connection, index: &str) -> Vec<String> {
    let mut statement = sql
        .prepare(&format!("PRAGMA index_info({index})"))
        .expect("index info");
    statement
        .query_map([], |row| row.get(2))
        .expect("columns")
        .collect::<Result<_, _>>()
        .expect("names")
}

/// The VM steps of the query `projection_find` runs, on the review's fixture: 1,001 rows of which
/// the last 10 match.
fn find_steps(sql: &rusqlite::Connection, table: &str) -> (usize, i32) {
    let tx = sql.unchecked_transaction().expect("fixture transaction");
    for index in 0..1_001 {
        let kind = if index + 10 >= 1_001 { "item" } else { "other" };
        tx.execute(
            &format!("INSERT INTO {table}(tenant_id,row_key,body,idx_0) VALUES ('owner',?1,?2,?3)"),
            rusqlite::params![
                format!("key-{index:05}"),
                serde_json::json!({ "kind": kind }).to_string(),
                kind
            ],
        )
        .expect("fixture row");
    }
    tx.commit().expect("fixture");
    let mut statement = sql
        .prepare(&format!(
            "SELECT body FROM {table}
             WHERE tenant_id = ?1 AND idx_0 = ?2
             ORDER BY row_key LIMIT ?3"
        ))
        .expect("find query");
    let mut rows = statement
        .query(rusqlite::params!["owner", "item", 10])
        .expect("query");
    let mut returned = 0;
    while rows.next().expect("row").is_some() {
        returned += 1;
    }
    drop(rows);
    (
        returned,
        statement.get_status(rusqlite::StatementStatus::VmStep),
    )
}

#[tokio::test]
async fn the_projection_index_serves_find_in_row_key_order_and_old_indexes_are_upgraded() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("index.sqlite");
    let path = database.to_str().expect("path");
    let store = SqliteEventStore::open(path, "owner").await.expect("owner");
    store
        .create_projections(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("tables");
    let sql = rusqlite::Connection::open(&database).expect("fixture connection");
    assert_eq!(
        index_columns(&sql, "owner_p_tally_idx_0"),
        ["tenant_id", "idx_0", "row_key"]
    );
    let (returned, steps) = find_steps(&sql, "owner_p_tally");
    eprintln!("projection_find fixture: returned={returned} vm_steps={steps}");
    assert_eq!(returned, 10);
    assert!(
        steps < 500,
        "the index ends the ordered scan at the limit; {steps} VM steps visited every row"
    );

    // A file made before row_key joined the index: capture still admits it read-only, and the
    // next registration upgrades it in place.
    sql.execute_batch(
        "DROP INDEX owner_p_tally_idx_0;
         CREATE INDEX owner_p_tally_idx_0 ON owner_p_tally (tenant_id, idx_0);",
    )
    .expect("an index of the earlier shape");
    let tenant = TenantId::new("owner").expect("tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .capture_tenant(
            &tenant,
            &[eventlog_conformance::TALLY],
            CaptureLimits {
                max_events: 8,
                max_blobs: 8,
                max_projection_rows: 2_000,
                max_payload_bytes: 1 << 20,
            },
        )
        .await
        .expect("an index of the earlier shape is still the declared shape");
    assert_eq!(
        index_columns(&sql, "owner_p_tally_idx_0"),
        ["tenant_id", "idx_0"],
        "a capture changes nothing"
    );
    let reopened = SqliteEventStore::open(path, "owner").await.expect("owner");
    reopened
        .create_projections(Arc::new(eventlog_conformance::Tally))
        .await
        .expect("registration");
    assert_eq!(
        index_columns(&sql, "owner_p_tally_idx_0"),
        ["tenant_id", "idx_0", "row_key"],
        "registration upgraded the index"
    );
}
