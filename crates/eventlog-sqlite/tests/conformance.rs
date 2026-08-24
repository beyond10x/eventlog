use eventlog_sqlite::SqliteEventStore;

#[tokio::test]
async fn the_shared_exercise_passes_in_memory() {
    let store = SqliteEventStore::in_memory("corpus")
        .await
        .expect("an in-memory store");
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn the_shared_exercise_passes_on_a_file() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let store = SqliteEventStore::open(path.to_str().expect("utf-8 path"), "corpus")
        .await
        .expect("a file-backed store");
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn two_owners_share_a_database_without_sharing_a_table() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let path = path.to_str().expect("utf-8 path");
    let corpus = SqliteEventStore::open(path, "corpus")
        .await
        .expect("a store");
    let planner = SqliteEventStore::open(path, "planner")
        .await
        .expect("a store");
    eventlog_conformance::run(&corpus).await;
    eventlog_conformance::run(&planner).await;
}

#[tokio::test]
async fn the_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_projections(&store).await;
}

#[tokio::test]
async fn the_inline_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_inline_projections(&store).await;
}

#[tokio::test]
async fn the_paging_rule_holds_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn the_claim_rule_holds_in_memory() {
    let store = SqliteEventStore::in_memory("corpus")
        .await
        .expect("a store");
    eventlog_conformance::run_claims(&store).await;
}

/// The failure a running deployment found and no test could have.
///
/// Planner and colab both had a `<prefix>_events` table from the store they used before the log.
/// `CREATE TABLE IF NOT EXISTS` skipped it, and the first sign was a missing column inside a wall
/// of DDL, naming neither the database nor the collision. Every test starts empty, so nothing here
/// had ever opened a database that was not.
#[tokio::test]
async fn a_table_of_ours_that_somebody_else_made_is_refused_by_name() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("previous-store.sqlite3");

    // Exactly the shape planner's old store left behind.
    let connection = rusqlite::Connection::open(&path).expect("a database");
    connection
        .execute_batch(
            "CREATE TABLE planner_events (
                 tenant_id TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 body TEXT NOT NULL
             );",
        )
        .expect("the previous store's table");
    drop(connection);

    let Err(refused) = SqliteEventStore::open(path.to_str().expect("utf-8 path"), "planner").await
    else {
        panic!("a table this kit did not create must not be silently adopted");
    };
    let message = refused.to_string();
    assert!(
        message.contains("planner_events"),
        "the refusal names the table: {message}"
    );
    assert!(
        message.contains("previous store"),
        "and says what it probably is, so somebody knows what to do: {message}"
    );

    // A database this kit did make opens again without complaint.
    let fresh = directory.path().join("ours.sqlite3");
    let fresh = fresh.to_str().expect("utf-8 path");
    SqliteEventStore::open(fresh, "planner")
        .await
        .expect("a new database");
    SqliteEventStore::open(fresh, "planner")
        .await
        .expect("reopening one of ours is not a collision");
}
