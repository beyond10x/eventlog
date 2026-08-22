use eventlog_sqlite::SqliteEventStore;

#[test]
fn the_shared_exercise_passes_in_memory() {
    let store = SqliteEventStore::in_memory("corpus").expect("an in-memory store");
    eventlog_conformance::run(&store);
}

#[test]
fn the_shared_exercise_passes_on_a_file() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let store = SqliteEventStore::open(path.to_str().expect("utf-8 path"), "corpus")
        .expect("a file-backed store");
    eventlog_conformance::run(&store);
}

#[test]
fn two_owners_share_a_database_without_sharing_a_table() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let path = path.to_str().expect("utf-8 path");
    let corpus = SqliteEventStore::open(path, "corpus").expect("a store");
    let planner = SqliteEventStore::open(path, "planner").expect("a store");
    eventlog_conformance::run(&corpus);
    eventlog_conformance::run(&planner);
}

#[test]
fn the_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> =
        std::sync::Arc::new(SqliteEventStore::in_memory("corpus").expect("a store"));
    eventlog_conformance::run_projections(&store);
}

#[test]
fn the_inline_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> =
        std::sync::Arc::new(SqliteEventStore::in_memory("corpus").expect("a store"));
    eventlog_conformance::run_inline_projections(&store);
}

#[test]
fn the_paging_rule_holds_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> =
        std::sync::Arc::new(SqliteEventStore::in_memory("corpus").expect("a store"));
    eventlog_conformance::run_paging(&store);
}

#[test]
fn the_claim_rule_holds_in_memory() {
    let store = SqliteEventStore::in_memory("corpus").expect("a store");
    eventlog_conformance::run_claims(&store);
}
