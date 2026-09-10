use eventlog_core::EventStore;
use eventlog_file::FileEventStore;
use std::sync::Arc;

macro_rules! contract {
    ($name:ident, $run:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            let directory = tempfile::tempdir().unwrap();
            let store: Arc<dyn EventStore> =
                Arc::new(FileEventStore::open(directory.path()).await.unwrap());
            eventlog_conformance::$run(&store).await;
        }
    };
}
contract!(file_projections_contract, run_projections);
contract!(file_inline_contract, run_inline_projections);
contract!(file_paging_contract, run_paging);
contract!(file_rebuild_contract, run_rebuild_isolation);
contract!(file_public_inputs_contract, run_public_input_validation);

#[tokio::test(flavor = "multi_thread")]
async fn file_storage_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run(&store).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn file_groups_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn file_claims_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run_claims(&store).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn file_snapshots_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store: Arc<dyn EventStore> =
        Arc::new(FileEventStore::open(directory.path()).await.unwrap());
    eventlog_conformance::run_snapshot_generations(store).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn file_scopes_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run_scope_atomicity(&store, &store.admission_permit()).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn file_callback_failure_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run_inline_failure_atomicity(&store, &store.admission_permit()).await;
}
