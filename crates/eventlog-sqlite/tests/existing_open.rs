use eventlog_core::{EventStore, TenantId};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::Connection;

fn owner_tables(path: &str, prefix: &str) -> Vec<String> {
    let connection = Connection::open(path).expect("existing database");
    let mut query = connection
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?1 ORDER BY name")
        .expect("schema query");
    query
        .query_map([format!("{prefix}_%")], |row| row.get(0))
        .expect("schema rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("schema names")
}

#[tokio::test]
async fn existing_open_refuses_absent_or_unowned_database_without_creating_tables() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("absent.sqlite3");
    let name = path.to_str().expect("utf-8 path");
    assert!(
        SqliteEventStore::open_existing(name, "owner")
            .await
            .is_err()
    );
    assert!(!path.exists(), "missing database must remain missing");

    let other = SqliteEventStore::open(name, "other")
        .await
        .expect("different owner provisioned");
    drop(other);
    let before = owner_tables(name, "owner");
    assert!(
        SqliteEventStore::open_existing(name, "owner")
            .await
            .is_err()
    );
    assert_eq!(
        owner_tables(name, "owner"),
        before,
        "missing owner tables were minted"
    );
}

#[tokio::test]
async fn existing_open_reopens_current_schema_but_never_replaces_a_missing_table() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let name = path.to_str().expect("utf-8 path");
    drop(
        SqliteEventStore::open(name, "owner")
            .await
            .expect("provisioned"),
    );
    let store = SqliteEventStore::open_existing(name, "owner")
        .await
        .expect("existing owner reopens");
    let tenant = TenantId::new("existing-open").expect("tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("existing owner serves reads");
    drop(store);

    Connection::open(name)
        .expect("raw connection")
        .execute_batch("DROP TABLE owner_identity")
        .expect("remove identity table");
    let before = owner_tables(name, "owner");
    assert!(
        SqliteEventStore::open_existing(name, "owner")
            .await
            .is_err()
    );
    assert_eq!(
        owner_tables(name, "owner"),
        before,
        "missing table was recreated"
    );
}
