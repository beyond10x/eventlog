use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, InlineProjectionAdmin, NewEvent,
    ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use serde_json::json;

fn durable_structure(path: &str, prefix: &str) -> Vec<(String, String, Option<String>)> {
    let connection = rusqlite::Connection::open(path).expect("inspection connection");
    let mut statement = connection
        .prepare(
            "SELECT type,name,sql FROM sqlite_master WHERE substr(name,1,length(?1))=?1 \
             ORDER BY type,name",
        )
        .expect("catalog query");
    statement
        .query_map([prefix], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("catalog rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("catalog values")
}

struct PausedAdminProjector {
    generation: AtomicU64,
    seen: AtomicU64,
    pause_after: AtomicU64,
    fail_after: AtomicU64,
    armed: AtomicBool,
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl PausedAdminProjector {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(1),
            seen: AtomicU64::new(0),
            pause_after: AtomicU64::new(u64::MAX),
            fail_after: AtomicU64::new(u64::MAX),
            armed: AtomicBool::new(false),
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        }
    }

    fn reset(&self) {
        self.seen.store(0, Ordering::Release);
    }
}

impl Projector for PausedAdminProjector {
    fn name(&self) -> &'static str {
        "paused_inline_admin"
    }

    fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
        &[
            eventlog_conformance::ADMIN_LEDGER,
            eventlog_conformance::ADMIN_SIDECAR,
        ]
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let generation = self.generation.load(Ordering::Acquire);
            for specification in self.projections() {
                store
                    .upsert(
                        specification,
                        &event.tenant,
                        &event.stream_id,
                        &json!({"generation": generation, "position": event.global_seq}),
                    )
                    .await?;
            }
            let seen = self.seen.fetch_add(1, Ordering::AcqRel) + 1;
            if seen == self.fail_after.load(Ordering::Acquire) {
                return Err(EventLogError::Invalid("injected paused failure".into()));
            }
            if self.armed.load(Ordering::Acquire)
                && seen == self.pause_after.load(Ordering::Acquire)
            {
                self.reached.wait();
                self.resume.wait();
            }
            Ok(())
        })
    }
}

fn generations(connection: &rusqlite::Connection, table: &str, tenant: &TenantId) -> Vec<u64> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT body FROM {table} WHERE tenant_id=?1 ORDER BY row_key"
        ))
        .expect("row query");
    statement
        .query_map([tenant.as_str()], |row| row.get::<_, String>(0))
        .expect("row set")
        .map(|body| {
            serde_json::from_str::<serde_json::Value>(&body.expect("body"))
                .expect("json body")["generation"]
                .as_u64()
                .expect("generation")
        })
        .collect()
}

fn stored_cursor(
    connection: &rusqlite::Connection,
    prefix: &str,
    projector: &str,
    tenant: &TenantId,
) -> u64 {
    use rusqlite::OptionalExtension;
    connection
        .query_row(
            &format!(
                "SELECT global_seq FROM {prefix}_projection_cursors \
                 WHERE projection=?1 AND tenant_id=?2"
            ),
            [projector, tenant.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .expect("cursor query")
        .map_or(0, |value| u64::try_from(value).expect("nonnegative cursor"))
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_inline_admin_contract() {
    let concrete = Arc::new(
        SqliteEventStore::in_memory("inline_admin")
            .await
            .expect("opened store"),
    );
    let store: Arc<dyn EventStore> = concrete.clone();
    let admin: &dyn InlineProjectionAdmin = concrete.as_ref();
    eventlog_conformance::run_inline_admin(&store, admin).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn structural_attach_changes_neither_catalog_nor_registry_and_drift_installs_nothing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("admin.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_structure";
    let store = SqliteEventStore::open(path, prefix).await.expect("opened");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted");
    let before = durable_structure(path, prefix);
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    assert!(store.is_inline(projector.name()).await);
    assert_eq!(durable_structure(path, prefix), before);

    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("drift.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let store = SqliteEventStore::open(path, prefix).await.expect("opened");
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted");
    rusqlite::Connection::open(path)
        .expect("fixture connection")
        .execute_batch(&format!("DROP INDEX {prefix}_p_admin_ledger_idx_0"))
        .expect("drift fixture");
    let before = durable_structure(path, prefix);
    assert!(
        store
            .attach_inline_existing(projector.clone())
            .await
            .is_err()
    );
    assert!(!store.is_inline(projector.name()).await);
    assert_eq!(durable_structure(path, prefix), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_active_blob_aborts_the_complete_admin_fold() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt-blob.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_corrupt_blob";
    let store = SqliteEventStore::open(path, prefix).await.expect("opened");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    let tenant = TenantId::new("corrupt-blob-owner").expect("tenant");
    store
        .put_blob(&tenant, "active-blob", b"complete active bytes")
        .await
        .expect("blob");
    let event = NewEvent::new(
        "item.recorded",
        1,
        json!({"key":"one", "value":1, "blob":"active-blob"}),
    )
    .expect("event");
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[event],
            &eventlog_conformance::meta("corrupt-blob", &json!({})),
        )
        .await
        .expect("history");
    projector.generation.store(2, Ordering::Release);
    let connection = rusqlite::Connection::open(path).expect("fixture connection");
    assert_eq!(
        connection
            .execute(
                &format!(
                    "UPDATE {prefix}_blobs SET bytes=?1,byte_count=?2 \
                     WHERE tenant_id=?3 AND digest=?4"
                ),
                rusqlite::params![
                    b"tampered".as_slice(),
                    8_i64,
                    tenant.as_str(),
                    "active-blob"
                ],
            )
            .expect("corrupt stored bytes"),
        1
    );
    drop(connection);
    assert!(matches!(
        store
            .rebuild_inline_projection(projector.name(), &tenant)
            .await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
            .await
            .expect("old row")
            .expect("old row remains")["generation"],
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_admin_rebuild_keeps_rows_cursor_registration_and_other_tenant_atomic() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("paused.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_paused";
    let store = Arc::new(SqliteEventStore::open(path, prefix).await.expect("opened"));
    let projector = Arc::new(PausedAdminProjector::new());
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    let owner = TenantId::new("paused-owner").expect("owner");
    let neighbour = TenantId::new("paused-neighbour").expect("neighbour");
    let first = store
        .append(
            &StreamId::new(owner.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("paused-one", &json!({})),
        )
        .await
        .expect("first event");
    projector.reset();
    store
        .rebuild_inline_projection(projector.name(), &owner)
        .await
        .expect("initial cursor");
    store
        .append(
            &StreamId::new(neighbour.clone(), "item", "other").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 9)],
            &eventlog_conformance::meta("paused-other", &json!({})),
        )
        .await
        .expect("other tenant");
    let second = store
        .append(
            &StreamId::new(owner.clone(), "item", "two").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 2)],
            &eventlog_conformance::meta("paused-two", &json!({})),
        )
        .await
        .expect("second event");

    let connection = rusqlite::Connection::open(path).expect("fixture connection");
    assert_eq!(
        connection
            .execute(
                &format!(
                    "INSERT INTO {prefix}_p_admin_ledger \
                     (tenant_id,row_key,body,idx_0) VALUES (?1,'damaged',?2,'damaged')"
                ),
                rusqlite::params![owner.as_str(), json!({"generation":0}).to_string()],
            )
            .expect("extra active row"),
        1
    );
    drop(connection);

    projector.generation.store(2, Ordering::Release);
    projector.reset();
    projector.pause_after.store(2, Ordering::Release);
    projector.armed.store(true, Ordering::Release);
    let rebuilding = {
        let store = Arc::clone(&store);
        let owner = owner.clone();
        tokio::spawn(async move {
            store
                .rebuild_inline_projection("paused_inline_admin", &owner)
                .await
        })
    };
    let reached = Arc::clone(&projector.reached);
    tokio::task::spawn_blocking(move || reached.wait())
        .await
        .expect("replay reached final callback");

    let connection = rusqlite::Connection::open(path).expect("inspection connection");
    for name in ["admin_ledger", "admin_sidecar"] {
        let expected = if name == "admin_ledger" {
            vec![0, 1, 1]
        } else {
            vec![1, 1]
        };
        assert_eq!(
            generations(&connection, &format!("{prefix}_p_{name}"), &owner),
            expected,
            "{name}: active rows changed before publication"
        );
        assert_eq!(
            generations(&connection, &format!("{prefix}_p_{name}"), &neighbour),
            vec![1],
            "{name}: unrelated tenant changed"
        );
    }
    assert_eq!(
        stored_cursor(&connection, prefix, projector.name(), &owner),
        first.events[0].global_seq
    );
    assert!(store.is_inline(projector.name()).await);
    drop(connection);

    let resume = Arc::clone(&projector.resume);
    tokio::task::spawn_blocking(move || resume.wait())
        .await
        .expect("replay resumed");
    let result = rebuilding
        .await
        .expect("rebuild task")
        .expect("rebuild result");
    assert_eq!(result.position, second.events[0].global_seq);
    let connection = rusqlite::Connection::open(path).expect("inspection connection");
    for name in ["admin_ledger", "admin_sidecar"] {
        assert_eq!(
            generations(&connection, &format!("{prefix}_p_{name}"), &owner),
            vec![2, 2]
        );
        assert_eq!(
            generations(&connection, &format!("{prefix}_p_{name}"), &neighbour),
            vec![1]
        );
    }
    assert_eq!(
        stored_cursor(&connection, prefix, projector.name(), &owner),
        second.events[0].global_seq
    );
    assert!(store.is_inline(projector.name()).await);
    drop(connection);

    projector.generation.store(3, Ordering::Release);
    projector.reset();
    projector.armed.store(false, Ordering::Release);
    projector.fail_after.store(2, Ordering::Release);
    assert!(matches!(
        store
            .rebuild_inline_projection(projector.name(), &owner)
            .await,
        Err(EventLogError::Invalid(_))
    ));
    let connection = rusqlite::Connection::open(path).expect("inspection connection");
    for name in ["admin_ledger", "admin_sidecar"] {
        assert_eq!(
            generations(&connection, &format!("{prefix}_p_{name}"), &owner),
            vec![2, 2],
            "{name}: callback failure changed active rows"
        );
    }
    assert_eq!(
        stored_cursor(&connection, prefix, projector.name(), &owner),
        second.events[0].global_seq,
        "callback failure changed the cursor"
    );
    assert!(store.is_inline(projector.name()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_waiter_continues_as_one_worker_after_replay_releases_registration() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("coordination.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_coordination";
    let writer = SqliteEventStore::open(path, prefix).await.expect("writer");
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

    let store = Arc::new(SqliteEventStore::open(path, prefix).await.expect("admin"));
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
    assert!(waiting.await.expect_err("cancelled caller").is_cancelled());
    coordinated.release();
    rebuilding
        .await
        .expect("rebuild task")
        .expect("rebuild completed");
    for _ in 0..1_000 {
        if store.is_inline("missing_admin_projector").await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        store.is_inline("missing_admin_projector").await,
        "owned blocking worker did not finish its indivisible attachment"
    );
    store
        .rebuild_inline_projection("missing_admin_projector", &tenant)
        .await
        .expect("later operation proceeds without deadlock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_rebuild_caller_leaves_one_continuing_worker_and_whole_publication() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("cancelled-rebuild.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_cancelled_rebuild";
    let writer = SqliteEventStore::open(path, prefix).await.expect("writer");
    let coordinated = Arc::new(eventlog_conformance::CoordinatedProjector::default());
    writer
        .create_projections(coordinated.clone())
        .await
        .expect("shape");
    let tenant = TenantId::new("cancelled-worker-owner").expect("tenant");
    writer
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("cancelled-rebuild", &json!({})),
        )
        .await
        .expect("history");
    drop(writer);

    let store = Arc::new(SqliteEventStore::open(path, prefix).await.expect("admin"));
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
    rebuilding.abort();
    coordinated.release();
    assert!(
        rebuilding
            .await
            .expect_err("cancelled caller")
            .is_cancelled()
    );

    assert!(
        store
            .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
            .await
            .expect("read after continuing worker")
            .is_some(),
        "continuing worker did not publish its complete fold"
    );
    store
        .rebuild_inline_projection("coordinated_admin", &tenant)
        .await
        .expect("later rebuild proceeds");
}
