use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use eventlog_core::{EventStore, Expected, InlineProjectionAdmin, Projector, StreamId, TenantId};
use eventlog_file::FileEventStore;
use serde_json::json;

fn inventory(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("readable directory") {
            let entry = entry.expect("readable entry");
            let path = entry.path();
            let relative = path.strip_prefix(root).expect("below root").to_owned();
            if entry.file_type().expect("entry kind").is_dir() {
                found.insert(relative, None);
                pending.push(path);
            } else {
                found.insert(relative, Some(fs::read(path).expect("readable file")));
            }
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn file_inline_admin_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(
        FileEventStore::open(directory.path())
            .await
            .expect("opened store"),
    );
    let store: Arc<dyn EventStore> = concrete.clone();
    let admin: &dyn InlineProjectionAdmin = concrete.as_ref();
    eventlog_conformance::run_inline_admin(&store, admin).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn attach_and_attached_rebuild_refuse_each_pending_intent_without_changing_a_byte() {
    for intent in ["append.json", "privacy.json"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = FileEventStore::open(directory.path())
            .await
            .expect("opened");
        let projector = Arc::new(eventlog_conformance::AdminProjector::default());
        store
            .create_projections(projector.clone())
            .await
            .expect("admitted");

        fs::write(directory.path().join(intent), b"{\"pending\":true}").expect("intent");
        let before = inventory(directory.path());
        assert!(
            store
                .attach_inline_existing(projector.clone())
                .await
                .is_err()
        );
        assert!(!store.is_inline(projector.name()).await);
        assert_eq!(
            inventory(directory.path()),
            before,
            "attach changed {intent}"
        );

        fs::remove_file(directory.path().join(intent)).expect("remove fixture");
        store
            .attach_inline_existing(projector.clone())
            .await
            .expect("attached");
        fs::write(directory.path().join(intent), b"{\"pending\":true}").expect("intent");
        let before = inventory(directory.path());
        let tenant = TenantId::new("intent-owner").expect("tenant");
        assert!(
            store
                .rebuild_inline_projection(projector.name(), &tenant)
                .await
                .is_err()
        );
        assert_eq!(
            inventory(directory.path()),
            before,
            "rebuild changed {intent}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dirty_markers_survive_reopen_and_attach_until_the_selected_rebuild_publishes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let tenant = TenantId::new("dirty-owner").expect("tenant");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("stream");
    {
        let store = FileEventStore::open(directory.path())
            .await
            .expect("opened");
        let projector = Arc::new(eventlog_conformance::AdminProjector::default());
        store
            .create_projections(projector.clone())
            .await
            .expect("admitted");
        store
            .attach_inline_existing(projector)
            .await
            .expect("attached");
        store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.recorded", 1)],
                &eventlog_conformance::meta("dirty-append", &json!({})),
            )
            .await
            .expect("appended");
        store.redact(&stream, 1, "privacy").await.expect("redacted");
    }

    let store = FileEventStore::open(directory.path())
        .await
        .expect("reopened");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    let before = inventory(directory.path());
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("structurally attached");
    assert_eq!(
        inventory(directory.path()),
        before,
        "attach changed dirty authority"
    );
    assert!(
        store
            .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
            .await
            .is_err(),
        "dirty projection was presented as clean"
    );

    let rebuilt = store
        .rebuild_inline_projection(projector.name(), &tenant)
        .await
        .expect("generic redaction-aware rebuild");
    assert_eq!(rebuilt.applied, 1);
    assert!(
        store
            .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
            .await
            .expect("clean after rebuild")
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_holds_registration_coordination_and_cancelled_waiter_leaves_no_deadlock() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let writer = FileEventStore::open(directory.path())
        .await
        .expect("writer");
    let coordinated = Arc::new(eventlog_conformance::CoordinatedProjector::default());
    writer
        .create_projections(coordinated.clone())
        .await
        .expect("coordinated shape");
    writer
        .create_projections(Arc::new(eventlog_conformance::SpareProjector))
        .await
        .expect("spare shape");
    let tenant = TenantId::new("registration-owner").expect("tenant");
    writer
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("registration", &json!({})),
        )
        .await
        .expect("history");
    drop(writer);

    let store = Arc::new(FileEventStore::open(directory.path()).await.expect("admin"));
    store
        .attach_inline_existing(coordinated.clone())
        .await
        .expect("attached");
    coordinated.arm();
    let rebuilding = {
        let store = Arc::clone(&store);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            store
                .rebuild_inline_projection("coordinated_admin", &tenant)
                .await
        })
    };
    for _ in 0..1_000 {
        if coordinated.entered() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(coordinated.entered(), "replay hold was not reached");
    let waiting = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .attach_inline_existing(Arc::new(eventlog_conformance::SpareProjector))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !waiting.is_finished(),
        "attachment crossed replay coordination"
    );
    waiting.abort();
    assert!(waiting.await.expect_err("cancelled waiter").is_cancelled());
    coordinated.release();
    rebuilding
        .await
        .expect("rebuild task")
        .expect("rebuild completed");
    store
        .attach_inline_existing(Arc::new(eventlog_conformance::SpareProjector))
        .await
        .expect("later attachment proceeds");
}
