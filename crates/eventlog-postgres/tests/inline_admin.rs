use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use eventlog_core::{
    BoxFuture, CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture,
    EventLogError, EventStore, Expected, InlineProjectionAdmin, ProjectionStore, Projector,
    RecordedEvent, StreamId, TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresConfig, PostgresEventStore};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
}

fn prefix() -> String {
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("inline_admin_{suffix}")
}

async fn sql() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .expect("fixture session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn stored_cursor(client: &tokio_postgres::Client, prefix: &str, tenant: &TenantId) -> i64 {
    client
        .query_one(
            &format!(
                "SELECT global_seq FROM {prefix}_projection_cursors \
                 WHERE projection='admin_projector' AND tenant_id=$1"
            ),
            &[&tenant.as_str()],
        )
        .await
        .expect("published cursor")
        .get(0)
}

async fn publication_key(client: &tokio_postgres::Client, prefix: &str) -> i64 {
    let identity = serde_json::to_string(&[prefix, "publication"]).expect("coordinates");
    client
        .query_one(
            "SELECT hashtextextended(current_database() || ':' || current_schema() || $1,0)",
            &[&identity],
        )
        .await
        .expect("publication coordinate")
        .get(0)
}

async fn recovered(store: &PostgresEventStore) {
    for _ in 0..1_000 {
        if store.pool_status().checked_out == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("pool occupancy did not recover: {:?}", store.pool_status());
}

struct SnapshotProjector {
    generation: AtomicU64,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl SnapshotProjector {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(1),
            armed: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

impl Projector for SnapshotProjector {
    fn name(&self) -> &'static str {
        "snapshot_admin"
    }

    fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
        std::slice::from_ref(&eventlog_conformance::ADMIN_LEDGER)
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if event.version == 1 && self.armed.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if event.version == 2 && store.get_blob("changing").await?.as_deref() != Some(b"before")
            {
                return Err(EventLogError::Invalid(
                    "replay crossed the active blob snapshot".into(),
                ));
            }
            store
                .upsert(
                    &eventlog_conformance::ADMIN_LEDGER,
                    &event.tenant,
                    &event.stream_id,
                    &serde_json::json!({
                        "kind": event.name,
                        "generation": self.generation.load(Ordering::Acquire),
                    }),
                )
                .await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_inline_admin_contract() {
    let concrete = Arc::new(
        PostgresEventStore::connect(&url(), &prefix())
            .await
            .expect("connected"),
    );
    let store: Arc<dyn EventStore> = concrete.clone();
    let admin: &dyn InlineProjectionAdmin = concrete.as_ref();
    eventlog_conformance::run_inline_admin(&store, admin).await;
    concrete.drop_tables().await.expect("dropped");
    concrete.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_stored_event_cannot_replace_postgres_rows_or_cursor() {
    for mutation in ["data='[1,2,3]'::jsonb", "global_seq=0", "global_seq=-1"] {
        assert_corrupt_stored_event_cannot_replace_postgres_rows_or_cursor(mutation).await;
    }
}

async fn assert_corrupt_stored_event_cannot_replace_postgres_rows_or_cursor(mutation: &str) {
    let prefix = prefix();
    let owner = TenantId::new("corrupt-owner").expect("owner");
    let neighbour = TenantId::new("unrelated-owner").expect("neighbour");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    let writer = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("connected");
    writer
        .create_projections(projector.clone())
        .await
        .expect("admitted tables");
    writer
        .stream_identity(&owner)
        .await
        .expect("owner identity");
    writer
        .append(
            &StreamId::new(owner.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[
                eventlog_conformance::event("item.recorded", 1),
                eventlog_conformance::event("item.recorded", 2),
            ],
            &eventlog_conformance::meta("owner", &serde_json::json!({})),
        )
        .await
        .expect("owner history");
    writer
        .append(
            &StreamId::new(neighbour.clone(), "item", "other").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 3)],
            &eventlog_conformance::meta("neighbour", &serde_json::json!({})),
        )
        .await
        .expect("unrelated history");
    writer.shutdown().await.expect("writer closed");

    let store = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("reopened");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    for tenant in [&owner, &neighbour] {
        store
            .rebuild_inline_projection(projector.name(), tenant)
            .await
            .expect("first publication");
    }
    let client = sql().await;
    let owner_cursor = stored_cursor(&client, &prefix, &owner).await;
    let neighbour_cursor = stored_cursor(&client, &prefix, &neighbour).await;
    let mut before = Vec::new();
    for tenant in [&owner, &neighbour] {
        for specification in [
            &eventlog_conformance::ADMIN_LEDGER,
            &eventlog_conformance::ADMIN_SIDECAR,
        ] {
            before.push(
                store
                    .projection_get(
                        specification,
                        tenant,
                        if tenant == &owner { "one" } else { "other" },
                    )
                    .await
                    .expect("published row")
                    .expect("row exists"),
            );
        }
    }
    assert_eq!(
        client
            .execute(
                &format!(
                    "UPDATE {prefix}_events SET {mutation} \
                     WHERE tenant_id=$1 AND version=2"
                ),
                &[&owner.as_str()],
            )
            .await
            .expect("tampered last event"),
        1
    );
    assert!(matches!(
        store
            .capture_tenant(
                &owner,
                &[],
                CaptureLimits {
                    max_events: 4096,
                    max_blobs: 256,
                    max_projection_rows: 4096,
                    max_payload_bytes: 1 << 22,
                },
            )
            .await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Event
        })
    ));
    let refused = store
        .rebuild_inline_projection(projector.name(), &owner)
        .await;
    assert!(
        refused.is_err(),
        "rebuild admitted corrupt history: {refused:?}"
    );
    assert_eq!(stored_cursor(&client, &prefix, &owner).await, owner_cursor);
    assert_eq!(
        stored_cursor(&client, &prefix, &neighbour).await,
        neighbour_cursor
    );
    let mut after = Vec::new();
    for tenant in [&owner, &neighbour] {
        for specification in [
            &eventlog_conformance::ADMIN_LEDGER,
            &eventlog_conformance::ADMIN_SIDECAR,
        ] {
            after.push(
                store
                    .projection_get(
                        specification,
                        tenant,
                        if tenant == &owner { "one" } else { "other" },
                    )
                    .await
                    .expect("published row")
                    .expect("row exists"),
            );
        }
    }
    assert_eq!(after, before, "failed rebuild changed active rows");
    store.drop_tables().await.expect("dropped fixture tables");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_application_role_attaches_existing_shapes_without_ddl() {
    use rustls::pki_types::pem::PemObject;

    let schema = prefix();
    let (admin, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .expect("admin fixture connection");
    let driver = tokio::spawn(async move { connection.await.expect("admin driver") });
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='eventlog_test_application') \
             THEN CREATE ROLE eventlog_test_application LOGIN CONNECTION LIMIT 8; END IF; END $$; \
             ALTER ROLE eventlog_test_application CONNECTION LIMIT 8;"
        ))
        .await
        .expect("isolated owner schema");

    let cert = rustls::pki_types::CertificateDer::from_pem_file(
        std::env::var("EVENTLOG_TEST_POSTGRES_CA").expect("assigned CA"),
    )
    .expect("test CA");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trusted CA");
    let migration =
        PostgresConfig::verified(&url(), &schema, "kit", roots.clone()).expect("migration config");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    PostgresEventStore::migrate(migration, PoolOptions::default(), projector.projections())
        .await
        .expect("migration admitted shapes");
    admin
        .batch_execute(&format!(
            "GRANT USAGE ON SCHEMA {schema} TO eventlog_test_application; \
             GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {schema} TO eventlog_test_application; \
             GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO eventlog_test_application"
        ))
        .await
        .expect("DML grants");

    let application = PostgresConfig::verified(
        &std::env::var("EVENTLOG_TEST_HOSTED_POSTGRES_URL").expect("assigned application URL"),
        &schema,
        "kit",
        roots,
    )
    .expect("application config");
    let store = PostgresEventStore::open(application, PoolOptions::default(), 16, 2, 8)
        .await
        .expect("restricted store");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("direct non-DDL attachment");
    assert!(store.is_inline(projector.name()).await);
    store.shutdown().await.expect("closed");
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("fixture cleanup");
    drop(admin);
    driver.await.expect("driver task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_reads_a_committed_event_the_feed_watermark_withholds() {
    let prefix = prefix();
    let store = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("connected");
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    store
        .create_projections(projector.clone())
        .await
        .expect("admitted");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");

    let (older, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .expect("older transaction connection");
    let driver = tokio::spawn(async move { connection.await.expect("older driver") });
    older.batch_execute("BEGIN").await.expect("begin older");
    older
        .query_one("SELECT pg_current_xact_id()", &[])
        .await
        .expect("allocate older xid");

    let tenant = TenantId::new("beyond-watermark").expect("tenant");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("stream");
    let appended = store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("beyond-watermark", &serde_json::json!({})),
        )
        .await
        .expect("newer committed append");
    assert!(
        store
            .read_feed(&tenant, 0, 10)
            .await
            .expect("watermarked feed")
            .events
            .is_empty(),
        "fixture event was not held behind the unrelated xmin"
    );
    let rebuilt = store
        .rebuild_inline_projection(projector.name(), &tenant)
        .await
        .expect("complete-history rebuild");
    assert_eq!(rebuilt.applied, 1);
    assert_eq!(rebuilt.position, appended.events[0].global_seq);
    assert!(
        store
            .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
            .await
            .expect("projection read")
            .is_some()
    );

    older.batch_execute("ROLLBACK").await.expect("end older");
    drop(older);
    driver.await.expect("driver task");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_registration_waiter_cannot_cross_replay_and_capacity_recovers() {
    let prefix = prefix();
    let writer = PostgresEventStore::connect(&url(), &prefix)
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
            &eventlog_conformance::meta("registration", &serde_json::json!({})),
        )
        .await
        .expect("history");
    writer.shutdown().await.expect("writer closed");

    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("admin"),
    );
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
    for _ in 0..15_000 {
        if coordinated.entered() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if !coordinated.entered() {
        coordinated.release();
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
    let crossed = waiting.is_finished();
    waiting.abort();
    coordinated.release();
    let cancelled = waiting.await;
    assert!(!crossed, "attachment crossed replay coordination");
    assert!(cancelled.expect_err("cancelled waiter").is_cancelled());
    rebuilding
        .await
        .expect("rebuild task")
        .expect("rebuild completed");
    for _ in 0..1_000 {
        if store.pool_status().checked_out == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(store.pool_status().checked_out, 0);
    store
        .attach_inline_existing(Arc::new(eventlog_conformance::SpareProjector))
        .await
        .expect("later attachment proceeds");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_while_waiting_for_publication_retires_the_session_and_writer_recovers() {
    let prefix = prefix();
    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("store"),
    );
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    store
        .create_projections(projector.clone())
        .await
        .expect("shape");
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    let tenant = TenantId::new("lock-wait-owner").expect("tenant");
    let holder = sql().await;
    let key = publication_key(&holder, &prefix).await;
    holder
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("held publication lock");
    let rebuilding = {
        let store = Arc::clone(&store);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            store
                .rebuild_inline_projection("admin_projector", &tenant)
                .await
        })
    };
    for _ in 0..1_000 {
        if store.pool_status().checked_out == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(store.pool_status().checked_out, 1);
    assert!(
        !rebuilding.is_finished(),
        "rebuild crossed held publication lock"
    );
    rebuilding.abort();
    assert!(
        rebuilding
            .await
            .expect_err("cancelled rebuild")
            .is_cancelled()
    );
    holder
        .execute("SELECT pg_advisory_unlock($1)", &[&key])
        .await
        .expect("released publication lock");
    recovered(&store).await;
    store
        .rebuild_inline_projection(projector.name(), &tenant)
        .await
        .expect("later rebuild proceeds");
    store
        .append(
            &StreamId::new(tenant, "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("after-lock-cancel", &serde_json::json!({})),
        )
        .await
        .expect("later writer proceeds");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_during_replay_retires_the_session_and_later_rebuild_proceeds() {
    let prefix = prefix();
    let writer = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("writer");
    let coordinated = Arc::new(eventlog_conformance::CoordinatedProjector::default());
    writer
        .create_projections(coordinated.clone())
        .await
        .expect("shape");
    let tenant = TenantId::new("replay-cancel-owner").expect("tenant");
    writer
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
            Expected::NoStream,
            &[eventlog_conformance::event("item.recorded", 1)],
            &eventlog_conformance::meta("before-replay-cancel", &serde_json::json!({})),
        )
        .await
        .expect("history");
    writer.shutdown().await.expect("writer closed");
    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("admin"),
    );
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
            .expect_err("cancelled rebuild")
            .is_cancelled()
    );
    recovered(&store).await;
    store
        .rebuild_inline_projection("coordinated_admin", &tenant)
        .await
        .expect("later rebuild proceeds");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_holds_publishers_and_keeps_one_active_blob_snapshot() {
    let prefix = prefix();
    let writer = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("writer");
    let projector = Arc::new(SnapshotProjector::new());
    writer
        .create_projections(projector.clone())
        .await
        .expect("shape");
    let tenant = TenantId::new("snapshot-order-owner").expect("tenant");
    let stream = StreamId::new(tenant.clone(), "item", "history").expect("stream");
    writer
        .put_blob(&tenant, "changing", b"before")
        .await
        .expect("initial blob");
    let history = writer
        .append(
            &stream,
            Expected::NoStream,
            &[
                eventlog_conformance::event("item.recorded", 1),
                eventlog_conformance::event("item.recorded", 2),
            ],
            &eventlog_conformance::meta("snapshot-history", &serde_json::json!({})),
        )
        .await
        .expect("history");
    writer.shutdown().await.expect("writer closed");

    let store = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("admin"),
    );
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached");
    store
        .rebuild_inline_projection(projector.name(), &tenant)
        .await
        .expect("initial rows");
    projector.generation.store(2, Ordering::Release);
    projector.armed.store(true, Ordering::Release);
    let rebuilding = {
        let store = Arc::clone(&store);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            store
                .rebuild_inline_projection("snapshot_admin", &tenant)
                .await
        })
    };
    projector.entered.notified().await;
    for key in ["history"] {
        assert_eq!(
            store
                .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, key)
                .await
                .expect("old row")
                .expect("old row exists")["generation"],
            1
        );
    }

    let publisher = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("publisher"),
    );
    publisher
        .delete_blob(&tenant, "changing")
        .await
        .expect("delete newer binding");
    publisher
        .put_blob(&tenant, "changing", b"after")
        .await
        .expect("replace newer binding");
    let appending = {
        let publisher = Arc::clone(&publisher);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            publisher
                .append(
                    &StreamId::new(tenant, "item", "later").expect("stream"),
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.recorded", 3)],
                    &eventlog_conformance::meta("later", &serde_json::json!({})),
                )
                .await
        })
    };
    let erasing = {
        let publisher = Arc::clone(&publisher);
        let stream = stream.clone();
        tokio::spawn(async move { publisher.redact(&stream, 1, "privacy").await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!appending.is_finished(), "append crossed publication lock");
    assert!(!erasing.is_finished(), "erasure crossed publication lock");
    projector.release.notify_one();
    let rebuilt = rebuilding
        .await
        .expect("rebuild task")
        .expect("consistent snapshot rebuild");
    assert_eq!(rebuilt.applied, 2);
    assert_eq!(rebuilt.position, history.events[1].global_seq);
    appending
        .await
        .expect("append task")
        .expect("append proceeds");
    erasing
        .await
        .expect("erasure task")
        .expect("erasure proceeds");
    publisher.shutdown().await.expect("publisher closed");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_and_unlock_response_loss_are_unknown_and_retire_before_capacity_returns() {
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        sync::atomic::{AtomicBool, Ordering},
    };

    for lose_unlock in [false, true] {
        let prefix = prefix();
        let setup = PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("setup");
        let projector = Arc::new(eventlog_conformance::AdminProjector::default());
        setup
            .create_projections(projector.clone())
            .await
            .expect("shape");
        let tenant = TenantId::new(if lose_unlock {
            "unlock-response-owner"
        } else {
            "commit-response-owner"
        })
        .expect("tenant");
        setup
            .append(
                &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.recorded", 1)],
                &eventlog_conformance::meta("response-loss", &serde_json::json!({})),
            )
            .await
            .expect("history");
        setup.shutdown().await.expect("setup closed");

        let parsed: tokio_postgres::Config = url().parse().expect("config");
        let upstream_port = parsed.get_ports()[0];
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("response-loss proxy");
        let proxy_port = listener.local_addr().expect("address").port();
        let armed = Arc::new(AtomicBool::new(false));
        let intercepted = Arc::new(AtomicBool::new(false));
        let proxy_arm = Arc::clone(&armed);
        let proxy_intercepted = Arc::clone(&intercepted);
        let proxy = std::thread::spawn(move || {
            let (mut downstream, _) = listener.accept().expect("one test client");
            let mut upstream =
                TcpStream::connect(("127.0.0.1", upstream_port)).expect("test upstream");
            downstream.set_nodelay(true).expect("no coalescing");
            upstream.set_nodelay(true).expect("no coalescing");
            let mut inbound = downstream.try_clone().expect("downstream reader");
            let mut outbound = upstream.try_clone().expect("upstream writer");
            let forward = std::thread::spawn(move || {
                let _ = std::io::copy(&mut inbound, &mut outbound);
                let _ = outbound.shutdown(Shutdown::Both);
            });
            let mut committed = false;
            loop {
                let mut header = [0_u8; 5];
                if upstream.read_exact(&mut header).is_err() {
                    break;
                }
                let length = u32::from_be_bytes(header[1..].try_into().expect("length"));
                assert!((4..=16 * 1024 * 1024).contains(&length));
                let mut body = vec![0; usize::try_from(length - 4).expect("frame")];
                if upstream.read_exact(&mut body).is_err() {
                    break;
                }
                let armed = proxy_arm.load(Ordering::Acquire);
                let commit = header[0] == b'C' && body == b"COMMIT\0";
                let unlock = header[0] == b'C' && body == b"SELECT 1\0";
                if armed && ((!lose_unlock && commit) || (lose_unlock && committed && unlock)) {
                    proxy_intercepted.store(true, Ordering::Release);
                    break;
                }
                if armed && lose_unlock && commit {
                    committed = true;
                }
                if downstream
                    .write_all(&header)
                    .and_then(|()| downstream.write_all(&body))
                    .is_err()
                {
                    break;
                }
            }
            let _ = upstream.shutdown(Shutdown::Both);
            let _ = downstream.shutdown(Shutdown::Both);
            forward.join().expect("forwarder stopped");
        });
        let proxy_url = format!("host=127.0.0.1 port={proxy_port} user=postgres dbname=postgres");
        let connected = PostgresEventStore::connect(&proxy_url, &prefix)
            .await
            .expect("proxy store");
        projector.generation.store(2, Ordering::Release);
        connected
            .attach_inline_existing(projector.clone())
            .await
            .expect("attached");
        armed.store(true, Ordering::Release);
        assert_eq!(
            connected
                .rebuild_inline_projection(projector.name(), &tenant)
                .await,
            Err(eventlog_core::EventLogError::UnknownCommit)
        );
        assert!(intercepted.load(Ordering::Acquire));
        connected.shutdown().await.expect("quarantine drained");
        proxy.join().expect("proxy terminated");

        let reopened = PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("reopened");
        assert_eq!(
            reopened
                .projection_get(&eventlog_conformance::ADMIN_LEDGER, &tenant, "one")
                .await
                .expect("durable row")
                .expect("published row")["generation"],
            2
        );
        reopened.drop_tables().await.expect("dropped");
        reopened.shutdown().await.expect("closed");
    }
}
