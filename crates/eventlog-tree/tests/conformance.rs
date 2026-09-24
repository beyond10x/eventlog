//! The shared exercises a linear provider passes, run over the tree store. The claims exercise is
//! not run: this store declares claims unsupported in its capabilities.

use eventlog_core::EventStore;
use eventlog_tree::TreeEventStore;
use std::sync::Arc;

macro_rules! contract {
    ($name:ident, $run:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            let directory = tempfile::tempdir().unwrap();
            let store: Arc<dyn EventStore> =
                Arc::new(TreeEventStore::open(directory.path()).await.unwrap());
            eventlog_conformance::$run(&store).await;
        }
    };
}
contract!(tree_projections_contract, run_projections);
contract!(tree_inline_contract, run_inline_projections);
contract!(tree_paging_contract, run_paging);
contract!(tree_rebuild_contract, run_rebuild_isolation);
contract!(tree_public_inputs_contract, run_public_input_validation);

#[tokio::test(flavor = "multi_thread")]
async fn tree_storage_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run(&store).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_groups_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_guarded_group_blobs_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    assert!(
        eventlog_conformance::run_guarded_group_blobs(&store).await,
        "the tree store implements the guarded blob-bearing group through its engine's blob entry point"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_snapshots_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store: Arc<dyn EventStore> =
        Arc::new(TreeEventStore::open(directory.path()).await.unwrap());
    eventlog_conformance::run_snapshot_generations(store).await;
}
