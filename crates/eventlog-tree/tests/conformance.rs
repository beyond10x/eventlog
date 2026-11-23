//! The shared exercises a linear provider passes, run over the tree store in each layout a new
//! store can be created in. The claims exercise is not run: this store declares claims unsupported
//! in its capabilities.

#[macro_use]
mod each_layout;

use eventlog_conformance::{ADMIN_LEDGER, AdminProjector, SpareProjector};
use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, InlineProjectionAdmin, NewEvent,
    ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_tree::{TreeEventStore, TreeLayout};
use serde_json::json;
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

/// A projection under a name nothing admitted, as a misspelling makes one.
struct Misspelled;

impl Projector for Misspelled {
    fn name(&self) -> &'static str {
        "admin_projector"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        const LEDGR: ProjectionSpec = ProjectionSpec {
            name: "admin_ledgr",
            indexed: &["kind"],
        };
        std::slice::from_ref(&LEDGR)
    }
    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// An admitted projection's name declared with another shape.
struct Reshaped;

impl Projector for Reshaped {
    fn name(&self) -> &'static str {
        "admin_projector"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        const LEDGER: ProjectionSpec = ProjectionSpec {
            name: ADMIN_LEDGER.name,
            indexed: &[],
        };
        std::slice::from_ref(&LEDGER)
    }
    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// `InlineProjectionAdmin::attach_inline_existing` attaches code to projections admitted already;
/// it never admits one. The tree keeps no projection structure in its files, so what is admitted
/// is what `create_projections` admitted on this handle, and an attachment of anything else is
/// refused as SQLite refuses one its tables do not hold, instead of reporting success.
async fn an_inline_projection_nothing_admitted_is_refused_attachment(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let refused = |result: Result<(), EventLogError>, what: &str| match result {
        Err(EventLogError::Invalid(reason)) => assert_eq!(
            reason, "projection admission does not match durable structure",
            "{what}"
        ),
        other => panic!("{what}: attached without an admission: {other:?}"),
    };
    refused(
        store.attach_inline_existing(Arc::new(SpareProjector)).await,
        "a projector nothing admitted",
    );
    assert!(!store.is_inline(SpareProjector.name()).await);

    let projector = Arc::new(AdminProjector::default());
    store.create_projections(projector.clone()).await.unwrap();
    refused(
        store.attach_inline_existing(Arc::new(Misspelled)).await,
        "a misspelled projection",
    );
    refused(
        store.attach_inline_existing(Arc::new(Reshaped)).await,
        "an admitted name declared with another shape",
    );
    assert!(!store.is_inline(projector.name()).await);

    // What was admitted still attaches, before and after the store has served.
    store
        .create_projections(Arc::new(SpareProjector))
        .await
        .unwrap();
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("an admitted projector attaches");
    let tenant = TenantId::new("admin-owner").unwrap();
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").unwrap(),
            Expected::NoStream,
            &[NewEvent::new("item.recorded", 1, json!({ "key": "one" })).unwrap()],
            &eventlog_conformance::meta("admin-one", &json!({})),
        )
        .await
        .unwrap();
    store
        .attach_inline_existing(Arc::new(SpareProjector))
        .await
        .expect("an admitted projector attaches after serving");
    assert!(store.is_inline(SpareProjector.name()).await);
    assert!(
        store
            .projection_get(&ADMIN_LEDGER, &tenant, "one")
            .await
            .unwrap()
            .is_some(),
        "the attached projector folded the history"
    );
}

each_layout!(
    an_inline_projection_nothing_admitted_is_refused_attachment,
    tree_projections_contract,
    tree_inline_contract,
    tree_paging_contract,
    tree_rebuild_contract,
    tree_public_inputs_contract,
    tree_storage_contract,
    tree_groups_contract,
    tree_guarded_group_blobs_contract,
);
