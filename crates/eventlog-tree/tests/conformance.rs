//! The shared exercises a linear provider passes, run over the tree store in each layout a new
//! store can be created in. The claims exercise is not run: this store declares claims unsupported
//! in its capabilities.

#[macro_use]
mod each_layout;

use eventlog_core::EventStore;
use eventlog_tree::{TreeEventStore, TreeLayout};
use std::sync::Arc;

macro_rules! contract {
    ($name:ident, $run:ident) => {
        async fn $name(layout: TreeLayout) {
            let directory = tempfile::tempdir().unwrap();
            let store: Arc<dyn EventStore> = Arc::new(
                TreeEventStore::open_in(directory.path(), layout)
                    .await
                    .unwrap(),
            );
            eventlog_conformance::$run(&store).await;
        }
    };
}
contract!(tree_projections_contract, run_projections);
contract!(tree_inline_contract, run_inline_projections);
contract!(tree_paging_contract, run_paging);
contract!(tree_rebuild_contract, run_rebuild_isolation);
contract!(tree_public_inputs_contract, run_public_input_validation);

async fn tree_storage_contract(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    eventlog_conformance::run(&store).await;
}

async fn tree_groups_contract(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
}

async fn tree_guarded_group_blobs_contract(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    assert!(
        eventlog_conformance::run_guarded_group_blobs(&store).await,
        "the tree store implements the guarded blob-bearing group through its engine's blob entry point"
    );
}

/// One test runs both layouts, one after the other: the exercise pauses a serialization through a
/// process-wide registry keyed by a fixed id, so two runs of it at once in one process collide.
#[tokio::test(flavor = "multi_thread")]
async fn tree_snapshots_contract() {
    for layout in [TreeLayout::Nested, TreeLayout::Hashed] {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn EventStore> = Arc::new(
            TreeEventStore::open_in(directory.path(), layout)
                .await
                .unwrap(),
        );
        eventlog_conformance::run_snapshot_generations(store).await;
    }
}

each_layout!(
    tree_projections_contract,
    tree_inline_contract,
    tree_paging_contract,
    tree_rebuild_contract,
    tree_public_inputs_contract,
    tree_storage_contract,
    tree_groups_contract,
    tree_guarded_group_blobs_contract,
);
