//! Provider cases for PostgreSQL's consistent tenant capture.
//!
//! What is provable here and nowhere else: that the exclusive publication lock is taken before the
//! snapshot exists, that the feed's watermark is not applied to a complete observation, and that
//! cancellation anywhere between acquiring the lock and reading retires the session instead of
//! recycling one that still holds it.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use eventlog_conformance::{CAPTURE_LEDGER, CAPTURE_SIDECAR, CaptureLedger};
use eventlog_core::{
    BoxFuture, CaptureError, CaptureLimits, CaptureMaterial, CatchUpRunner,
    ConsistentTenantCapture, EventLogError, EventStore, Expected, Guard, NewEvent,
    ProjectionCaptureRefusal, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId,
    TenantId,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};
use serde_json::json;
use tokio::sync::Notify;
use tokio_postgres::NoTls;

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
}

fn prefix(label: &str) -> String {
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    format!("cap_{label}_{suffix}")
}

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 256,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 22,
    }
}

/// A raw session in the same schema the store uses, for the competing writers these cases need.
async fn sql() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&url(), NoTls)
        .await
        .expect("fixture session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO public")
        .await
        .expect("same schema as the store");
    client
}

/// The owner's publication coordinate, derived by the database exactly as the provider derives it.
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

fn appended(key: &str, value: i64) -> NewEvent {
    NewEvent::new("item.received", 1, json!({ "key": key, "value": value }))
        .expect("fixture event is valid")
}

async fn recovered(store: &PostgresEventStore) {
    for _ in 0..400 {
        if store.pool_status().checked_out == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("pool occupancy never recovered: {:?}", store.pool_status());
}

#[tokio::test]
async fn postgres_consistent_capture_contract() {
    let prefix = prefix("contract");
    let concrete = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("connected"),
    );
    let port: Arc<dyn EventStore> = concrete.clone();
    eventlog_conformance::run_consistent_capture(&port, concrete.as_ref()).await;
    concrete.drop_tables().await.expect("dropped");
    concrete.shutdown().await.expect("closed");
}

const ASSIGNMENT: ProjectionSpec = ProjectionSpec {
    name: "capture_assignment",
    indexed: &[],
};

struct Assignment;

impl Projector for Assignment {
    fn name(&self) -> &'static str {
        "capture_assignment"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&ASSIGNMENT)
    }
    fn apply<'a>(
        &'a self,
        _: &'a RecordedEvent,
        _: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Takes its transaction's XID early, then waits, so a later publisher commits a lower position.
struct AssignOlderXid {
    tenant: TenantId,
    assigned: Arc<Notify>,
    resume: Arc<Notify>,
}

impl Guard for AssignOlderXid {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(&ASSIGNMENT, &self.tenant, "guard", &json!({ "held": 1 }))
                .await?;
            self.assigned.notify_one();
            self.resume.notified().await;
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_returns_committed_history_the_feed_watermark_withholds() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let prefix = prefix("xmin");
        let older = Arc::new(
            PostgresEventStore::connect(&url(), &prefix)
                .await
                .expect("older publisher"),
        );
        older
            .create_projections(Arc::new(Assignment))
            .await
            .expect("guard view");
        let newer = PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("independent publisher");
        let tenant = TenantId::new("pg-capture-xmin").expect("valid tenant");
        newer
            .stream_identity(&tenant)
            .await
            .expect("provisioned identity");

        let assigned = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let guard = Arc::new(AssignOlderXid {
            tenant: tenant.clone(),
            assigned: assigned.clone(),
            resume: resume.clone(),
        });
        let writer = older.clone();
        let writer_tenant = tenant.clone();
        let first = tokio::spawn(async move {
            writer
                .append_guarded(
                    &StreamId::new(writer_tenant, "item", "older").expect("valid stream"),
                    Expected::NoStream,
                    &[appended("older", 1)],
                    &eventlog_conformance::meta("older", &json!({})),
                    guard,
                )
                .await
        });
        assigned.notified().await;
        // An ordinary transaction in another owner takes an XID between the two append XIDs and
        // holds the cluster's xmin. It never touches this owner's tables or publication lock.
        let mut outsider = sql().await;
        let unrelated = outsider.transaction().await.expect("unrelated transaction");
        let middle: String = unrelated
            .query_one("SELECT pg_current_xact_id()::text", &[])
            .await
            .expect("middle XID")
            .get(0);
        let lower = newer
            .append(
                &StreamId::new(tenant.clone(), "item", "newer").expect("valid stream"),
                Expected::NoStream,
                &[appended("newer", 2)],
                &eventlog_conformance::meta("newer", &json!({})),
            )
            .await
            .expect("lower position committed");
        resume.notify_one();
        let higher = first
            .await
            .expect("older task")
            .expect("higher position committed");
        assert!(lower.events[0].global_seq < higher.events[0].global_seq);

        // The unrelated transaction is still open, so its xmin still covers the newer XID.
        let captured = newer
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation");
        let feed = newer
            .read_feed(&tenant, 0, 10)
            .await
            .expect("ordinary feed");
        unrelated.commit().await.expect("release unrelated xmin");

        eprintln!(
            "unrelated_xid={middle}; captured={}; feed={}",
            captured.events.len(),
            feed.events.len()
        );
        assert_eq!(
            captured
                .events
                .iter()
                .map(|event| event.global_seq)
                .collect::<Vec<_>>(),
            vec![lower.events[0].global_seq, higher.events[0].global_seq],
            "both committed events, in ascending position, whatever order their XIDs are in"
        );
        assert!(
            feed.events.len() < captured.events.len(),
            "the fixture only means something while the watermark is withholding committed history"
        );
        older.drop_tables().await.expect("dropped");
        older.shutdown().await.expect("closed");
        newer.shutdown().await.expect("closed");
    })
    .await
    .expect("bounded ordering fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_observes_an_erasure_that_completed_during_its_lock_wait() {
    let prefix = prefix("erasure");
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url(),
            &prefix,
            PoolOptions {
                lock_timeout: Duration::from_secs(20),
                transaction_timeout: Duration::from_secs(40),
                ..PoolOptions::default()
            },
        )
        .await
        .expect("connected"),
    );
    let name = "pg-capture-erased";
    let tenant = TenantId::new(name).expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .append(
            &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
            Expected::NoStream,
            &[appended("one", 1)],
            &eventlog_conformance::meta("one", &json!({})),
        )
        .await
        .expect("appended");
    store
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("the control: this tenant is observable before the erasure");

    let holder = sql().await;
    let key = publication_key(&holder, &prefix).await;
    holder
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("held the publication gate");
    let capturing = tokio::spawn({
        let store = store.clone();
        let tenant = tenant.clone();
        async move { store.capture_tenant(&tenant, &[], limits()).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !capturing.is_finished(),
        "capture must queue on the gate before it has any snapshot"
    );
    // A compliant publisher holding the gate completes a full erasure during that wait.
    for table in [
        "events",
        "append_groups",
        "commands",
        "claims",
        "identity",
        "snapshots",
        "snapshot_generations",
        "projection_cursors",
        "blobs",
    ] {
        holder
            .execute(
                &format!("DELETE FROM {prefix}_{table} WHERE tenant_id = $1"),
                &[&name],
            )
            .await
            .expect("erased");
    }
    holder
        .execute("SELECT pg_advisory_unlock($1)", &[&key])
        .await
        .expect("released the gate");
    let outcome = tokio::time::timeout(Duration::from_secs(30), capturing)
        .await
        .expect("capture finished")
        .expect("capture task");
    assert_eq!(
        outcome.expect_err("the erased tenant is gone"),
        CaptureError::TenantIdentityMissing,
        "a snapshot fixed before the wait would still be holding the erased tenant"
    );
    recovered(&store).await;
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_and_deadline_retire_the_lease_and_recover_capacity() {
    let prefix = prefix("cancel");
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url(),
            &prefix,
            PoolOptions {
                max_connections: 2,
                lock_timeout: Duration::from_millis(400),
                acquisition_timeout: Duration::from_millis(800),
                transaction_timeout: Duration::from_secs(10),
                ..PoolOptions::default()
            },
        )
        .await
        .expect("connected"),
    );
    let tenant = TenantId::new("pg-capture-cancel").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("valid stream");
    for index in 0..200_i64 {
        store
            .append(
                &stream,
                Expected::Any,
                &[appended(&format!("key-{index}"), index)],
                &eventlog_conformance::meta(&format!("fill-{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }

    let holder = sql().await;
    let key = publication_key(&holder, &prefix).await;
    holder
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("held the publication gate");

    // A deadline while acquiring the gate keeps the typed result and retires the lease.
    let refusal = store
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect_err("the gate is held elsewhere");
    assert!(
        matches!(refusal, CaptureError::Store(EventLogError::Deadline { .. })),
        "{refusal:?}"
    );
    recovered(&store).await;

    // Cancellation while awaiting the gate cannot recycle a session that may hold it.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(120),
            store.capture_tenant(&tenant, &[], limits())
        )
        .await
        .is_err()
    );
    recovered(&store).await;
    holder
        .execute("SELECT pg_advisory_unlock($1)", &[&key])
        .await
        .expect("released the gate");

    // Cancellation while holding the gate and reading, at widening offsets.
    let mut completed = 0_usize;
    let mut cancelled = 0_usize;
    for millis in [1_u64, 3, 8, 20, 60, 150] {
        match tokio::time::timeout(
            Duration::from_millis(millis),
            store.capture_tenant(&tenant, &[], limits()),
        )
        .await
        {
            Ok(result) => {
                result.expect("a capture that finished is complete");
                completed += 1;
            }
            Err(_) => cancelled += 1,
        }
        recovered(&store).await;
        // A subsequent writer proceeds, which it could not if the gate were still held.
        store
            .append(
                &stream,
                Expected::Any,
                &[appended("after-cancellation", 1)],
                &eventlog_conformance::meta(&format!("after-{millis}"), &json!({})),
            )
            .await
            .expect("a later writer proceeds");
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("a later capture acquires the same gate");
    }
    eprintln!("capture cancellation offsets: {completed} completed, {cancelled} cancelled");
    assert!(cancelled > 0, "no offset actually cancelled a capture");

    // Captures above completed on this pool's own sessions, and a session-level advisory lock is
    // re-entrant: a connection that was recycled still holding the gate would take it again
    // without waiting, and every later capture here would look fine. A pool that shares no
    // session with this one cannot be fooled that way, so it is what decides whether the release
    // actually happened before the lease was handed back.
    let observer = PostgresEventStore::connect_local(
        &url(),
        &prefix,
        PoolOptions {
            lock_timeout: Duration::from_millis(400),
            transaction_timeout: Duration::from_secs(10),
            ..PoolOptions::default()
        },
    )
    .await
    .expect("independent observer");
    observer
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("a completed capture left the publication gate held on a recycled session");
    observer.shutdown().await.expect("closed");
    let status = store.pool_status();
    assert_eq!((status.checked_out, status.waiting), (0, 0), "{status:?}");
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

const PAUSED: ProjectionSpec = ProjectionSpec {
    name: "paused_capture_rebuild",
    indexed: &[],
};

struct PausingCopy {
    generation: Arc<AtomicU64>,
    pause: Arc<AtomicBool>,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

impl Projector for PausingCopy {
    fn name(&self) -> &'static str {
        "paused_capture_rebuild"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&PAUSED)
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(
                    &PAUSED,
                    &event.tenant,
                    &event.stream_id,
                    &json!({ "generation": self.generation.load(Ordering::SeqCst) }),
                )
                .await?;
            if self.pause.swap(false, Ordering::SeqCst) {
                self.reached.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_paused_before_replacement_never_exposes_mixed_rows() {
    let prefix = prefix("rebuild");
    let store = Arc::new(
        PostgresEventStore::connect_local(
            &url(),
            &prefix,
            PoolOptions {
                lock_timeout: Duration::from_secs(20),
                transaction_timeout: Duration::from_secs(40),
                ..PoolOptions::default()
            },
        )
        .await
        .expect("connected"),
    );
    let tenant = TenantId::new("pg-capture-rebuild").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    for id in ["alpha", "beta", "gamma"] {
        store
            .append(
                &StreamId::new(tenant.clone(), "item", id).expect("valid stream"),
                Expected::NoStream,
                &[appended(id, 1)],
                &eventlog_conformance::meta(id, &json!({})),
            )
            .await
            .expect("appended");
    }
    let projector = Arc::new(PausingCopy {
        generation: Arc::new(AtomicU64::new(1)),
        pause: Arc::new(AtomicBool::new(false)),
        reached: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    });
    let port: Arc<dyn EventStore> = store.clone();
    let runner = CatchUpRunner::new(Arc::clone(&port), projector.clone())
        .await
        .expect("declared projection");
    eventlog_conformance::drain_at_least(&runner, &tenant, 3).await;
    let first = store
        .capture_tenant(&tenant, &[PAUSED], limits())
        .await
        .expect("complete observation");
    assert_eq!(first.projections[0].rows.len(), 3);

    projector.generation.store(2, Ordering::SeqCst);
    projector.pause.store(true, Ordering::SeqCst);
    let rebuild = tokio::spawn({
        let store = store.clone();
        let projector = projector.clone();
        let tenant = tenant.clone();
        async move { store.rebuild_projection(projector, &tenant).await }
    });
    projector.reached.notified().await;
    let capturing = tokio::spawn({
        let store = store.clone();
        let tenant = tenant.clone();
        async move { store.capture_tenant(&tenant, &[PAUSED], limits()).await }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !capturing.is_finished(),
        "a rebuild holds the gate through its replacement, so no capture can straddle it"
    );
    projector.resume.notify_one();
    rebuild
        .await
        .expect("rebuild task")
        .expect("rebuilt from current history");
    let after = tokio::time::timeout(Duration::from_secs(30), capturing)
        .await
        .expect("capture finished")
        .expect("capture task")
        .expect("complete observation");
    assert_eq!(after.projections[0].rows.len(), 3);
    assert!(
        after.projections[0]
            .rows
            .iter()
            .all(|(_, body)| body["generation"] == json!(2)),
        "the whole new materialization, never a mixture of the two"
    );
    recovered(&store).await;
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_and_physical_shape_drift_refuse_capture() {
    let prefix = prefix("shape");
    let concrete = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("connected"),
    );
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("pg-capture-shape").expect("valid tenant");
    concrete
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    port.create_projections(Arc::new(CaptureLedger))
        .await
        .expect("declared projections");
    concrete
        .capture_tenant(&tenant, &[CAPTURE_LEDGER, CAPTURE_SIDECAR], limits())
        .await
        .expect("the control: both are admitted before anything changes under them");

    let sql = sql().await;
    sql.batch_execute(&format!(
        "ALTER TABLE {prefix}_p_capture_sidecar ADD COLUMN extra text"
    ))
    .await
    .expect("added a column nobody declared");
    sql.batch_execute(&format!(
        "CREATE INDEX {prefix}_p_capture_ledger_extra ON {prefix}_p_capture_ledger (row_key)"
    ))
    .await
    .expect("added an index nobody declared");
    for (specification, name) in [
        (CAPTURE_SIDECAR, "capture_sidecar"),
        (CAPTURE_LEDGER, "capture_ledger"),
    ] {
        assert_eq!(
            concrete
                .capture_tenant(&tenant, &[specification], limits())
                .await
                .expect_err("the actual schema is not the admitted shape"),
            CaptureError::ProjectionUnavailable {
                projection: name.to_owned(),
                reason: ProjectionCaptureRefusal::PhysicalShapeMismatch
            },
            "a name in the registry alone is not shape admission"
        );
    }

    sql.execute(
        &format!(
            "UPDATE {prefix}_projection_registry SET indexed_fields='[\"other\"]'::jsonb
             WHERE projection_name=$1"
        ),
        &[&"capture_ledger"],
    )
    .await
    .expect("drifted the declaration");
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
            .await
            .expect_err("the declaration no longer matches the request"),
        CaptureError::ProjectionUnavailable {
            projection: "capture_ledger".to_owned(),
            reason: ProjectionCaptureRefusal::DeclarationMismatch
        }
    );
    concrete.drop_tables().await.expect("dropped");
    sql.batch_execute(&format!(
        "DROP TABLE IF EXISTS {prefix}_p_capture_ledger; DROP TABLE IF EXISTS {prefix}_p_capture_sidecar"
    ))
    .await
    .expect("dropped projections");
    concrete.shutdown().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_identity_is_preserved_exactly_or_refused_as_corruption() {
    let prefix = prefix("identity");
    let store = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("connected");
    let name = "pg-capture-identity";
    let tenant = TenantId::new(name).expect("valid tenant");
    let minted = store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation")
            .stream_identity,
        minted
    );

    let sql = sql().await;
    let statement = format!("UPDATE {prefix}_identity SET stream_identity=$1 WHERE tenant_id=$2");
    // A legacy value with no UUID syntax and surrounding whitespace is somebody's real identity.
    sql.execute(&statement, &[&"  Legacy Identity/v0  ", &name])
        .await
        .expect("replaced stored identity");
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("legacy identities are valid")
            .stream_identity,
        "  Legacy Identity/v0  ",
        "returned byte for byte: no trim, no normalization, no UUID rule"
    );
    sql.execute(&statement, &[&"", &name])
        .await
        .expect("replaced stored identity");
    assert_eq!(
        store.capture_tenant(&tenant, &[], limits()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Identity
        }),
        "an empty stored identity is corruption, not permission to mint a replacement"
    );
    store.drop_tables().await.expect("dropped");
    store.shutdown().await.expect("closed");
}
