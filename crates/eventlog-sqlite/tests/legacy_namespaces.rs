//! Legacy erasure must remain confined when one SQLite file hosts several owners.
use eventlog_core::{EventLogError, EventStore, Expected, StreamId, TenantId};
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
