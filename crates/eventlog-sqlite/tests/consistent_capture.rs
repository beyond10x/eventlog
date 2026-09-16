//! Provider cases for SQLite's consistent tenant capture.
//!
//! What is provable here and nowhere else: that the capture transaction orders against writers on
//! separately opened handles, that a rebuild paused before its replacement cannot leak a mixed
//! view, and that a stored identity or a projection table nobody here wrote is refused rather than
//! transcribed.

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use eventlog_conformance::{CAPTURE_LEDGER, CAPTURE_SIDECAR, CaptureLedger};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, CaptureError, CaptureLimits, CaptureMaterial,
    CaptureResource, CatchUpRunner, ConsistentTenantCapture, EventLogError, EventStore, Expected,
    NewEvent, ProjectionCaptureRefusal, ProjectionSpec, ProjectionStore, Projector, RecordedEvent,
    StreamAppend, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const PREFIX: &str = "cap";

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 256,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 22,
    }
}

fn database(directory: &tempfile::TempDir) -> String {
    directory
        .path()
        .join("store.db")
        .to_str()
        .expect("utf-8 path")
        .to_owned()
}

async fn opened(path: &str) -> SqliteEventStore {
    SqliteEventStore::open(path, PREFIX)
        .await
        .expect("opened store")
}

fn appended(key: &str, value: i64) -> NewEvent {
    NewEvent::new("item.received", 1, json!({ "key": key, "value": value }))
        .expect("fixture event is valid")
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_consistent_capture_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(opened(&database(&directory)).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    eventlog_conformance::run_consistent_capture(&port, concrete.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_orders_against_append_and_erasure_across_separate_handles() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let writer = Arc::new(opened(&path).await);
    let reader = opened(&path).await;
    let tenant = TenantId::new("sqlite-capture-owner").expect("valid tenant");
    writer
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");

    // A group commits both of its events or neither, so an odd count is a torn observation.
    let group = |key: &str| AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta(key, &json!({})),
        appends: ["alpha", "beta"]
            .into_iter()
            .map(|id| StreamAppend {
                stream: StreamId::new(tenant.clone(), "item", id).expect("valid stream"),
                expected: Expected::Any,
                events: vec![appended(id, 1)],
            })
            .collect(),
    };
    for round in 0..2 {
        writer
            .append_group(&group(&format!("g{round}")))
            .await
            .expect("committed group");
        let captured = reader
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation");
        assert_eq!(
            captured.events.len(),
            (round + 1) * 2,
            "a capture on another handle sees exactly what committed before it"
        );
    }

    // Under contention every successful capture is still a whole number of committed groups.
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut writers = Vec::new();
    for round in 2..10 {
        let writer = writer.clone();
        let group = group(&format!("g{round}"));
        let barrier = barrier.clone();
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            writer.append_group(&group).await
        }));
    }
    barrier.wait().await;
    let mut observed = 0_usize;
    let mut refused = 0_usize;
    for _ in 0..24 {
        match reader.capture_tenant(&tenant, &[], limits()).await {
            Ok(captured) => {
                observed += 1;
                assert_eq!(
                    captured.events.len() % 2,
                    0,
                    "capture observed half of an atomic group"
                );
                for id in ["alpha", "beta"] {
                    let versions: Vec<u64> = captured
                        .events
                        .iter()
                        .filter(|event| event.stream_id == id)
                        .map(|event| event.version)
                        .collect();
                    assert!(
                        versions
                            .iter()
                            .enumerate()
                            .all(|(index, version)| { u64::try_from(index + 1) == Ok(*version) }),
                        "a stream's versions are gapless from one: {versions:?}"
                    );
                }
            }
            Err(CaptureError::Store(EventLogError::Backend(_))) => refused += 1,
            Err(other) => panic!("unexpected capture refusal: {other:?}"),
        }
    }
    for task in writers {
        task.await
            .expect("writer finished")
            .expect("committed group");
    }
    eprintln!("contended captures: {observed} observed, {refused} refused by the writer lock");
    assert!(observed + refused == 24 && observed > 0);

    // An erasure that has not committed yet is still an earlier writer, and a capture is not
    // entitled to answer ahead of it. A deferred reader would fix its snapshot here and hand back
    // a complete observation of a tenant that is being removed — which is why the capture takes
    // the write transaction rather than a read one. WAL snapshot isolation alone does not decide
    // this: it would keep the stale answer perfectly consistent, and perfectly wrong.
    let mut erasing = Connection::open(&path).expect("third connection");
    let pending = erasing
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .expect("writer transaction");
    for table in ["identity", "events"] {
        pending
            .execute(
                &format!("DELETE FROM {PREFIX}_{table} WHERE tenant_id=?1"),
                params!["sqlite-capture-owner"],
            )
            .expect("erased rows");
    }
    let during = reader.capture_tenant(&tenant, &[], limits()).await;
    assert!(
        matches!(during, Err(CaptureError::Store(EventLogError::Backend(_)))),
        "capture answered from a snapshot fixed ahead of an uncommitted erasure: {during:?}"
    );
    pending.rollback().expect("released the writer");

    // Erasure on one handle is observed by the other, as a missing identity rather than a husk.
    writer
        .forget_tenant(&tenant)
        .await
        .expect("complete erasure");
    assert_eq!(
        reader
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect_err("the tenant is gone"),
        CaptureError::TenantIdentityMissing
    );
}

const PAUSED: ProjectionSpec = ProjectionSpec {
    name: "paused_rebuild",
    indexed: &[],
};

/// A projector that stops once, inside the rebuild transaction, before any replacement happens.
struct PausingCopy {
    generation: Arc<AtomicU64>,
    pause: Arc<AtomicBool>,
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl Projector for PausingCopy {
    fn name(&self) -> &'static str {
        "paused_rebuild"
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
                // The driver polls this on the blocking thread that holds BEGIN IMMEDIATE.
                self.reached.wait();
                self.resume.wait();
            }
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_paused_before_replacement_never_exposes_mixed_rows() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let writer = Arc::new(opened(&path).await);
    let reader = opened(&path).await;
    let tenant = TenantId::new("sqlite-capture-rebuild").expect("valid tenant");
    writer
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    for id in ["alpha", "beta", "gamma"] {
        writer
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
        reached: Arc::new(Barrier::new(2)),
        resume: Arc::new(Barrier::new(2)),
    });
    let port: Arc<dyn EventStore> = writer.clone();
    let runner = CatchUpRunner::new(Arc::clone(&port), projector.clone())
        .await
        .expect("declared projection");
    eventlog_conformance::drain_at_least(&runner, &tenant, 3).await;
    let first = reader
        .capture_tenant(&tenant, &[PAUSED], limits())
        .await
        .expect("complete observation");
    assert_eq!(first.projections[0].rows.len(), 3);
    assert!(
        first.projections[0]
            .rows
            .iter()
            .all(|(_, body)| body["generation"] == json!(1))
    );

    projector.generation.store(2, Ordering::SeqCst);
    projector.pause.store(true, Ordering::SeqCst);
    let rebuilding = writer.clone();
    let rebuild_tenant = tenant.clone();
    let rebuild_projector = projector.clone();
    let rebuild = tokio::spawn(async move {
        rebuilding
            .rebuild_projection(rebuild_projector, &rebuild_tenant)
            .await
    });
    let waiting = projector.reached.clone();
    tokio::task::spawn_blocking(move || waiting.wait())
        .await
        .expect("rebuild reached the pause");

    // Mid-rebuild, with the shadow rows written and the replacement not yet made.
    let during = reader.capture_tenant(&tenant, &[PAUSED], limits()).await;
    match &during {
        Ok(captured) => {
            let generations: Vec<&Value> = captured.projections[0]
                .rows
                .iter()
                .map(|(_, body)| &body["generation"])
                .collect();
            eprintln!("capture during paused rebuild: observed {generations:?}");
            assert!(
                generations.iter().all(|value| **value == json!(1)),
                "a capture saw a partially replaced materialization"
            );
        }
        Err(CaptureError::Store(EventLogError::Backend(_))) => {
            eprintln!("capture during paused rebuild: refused by the writer transaction");
        }
        Err(other) => panic!("unexpected capture refusal: {other:?}"),
    }

    let resuming = projector.resume.clone();
    tokio::task::spawn_blocking(move || resuming.wait())
        .await
        .expect("rebuild resumed");
    rebuild
        .await
        .expect("rebuild task finished")
        .expect("rebuilt");
    let after = reader
        .capture_tenant(&tenant, &[PAUSED], limits())
        .await
        .expect("complete observation");
    assert_eq!(after.projections[0].rows.len(), 3);
    assert!(
        after.projections[0]
            .rows
            .iter()
            .all(|(_, body)| body["generation"] == json!(2)),
        "after the replacement commits, the whole new materialization is visible"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_identity_is_preserved_exactly_or_refused_as_corruption() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let store = opened(&path).await;
    let tenant = TenantId::new("sqlite-capture-identity").expect("valid tenant");
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

    let sql = Connection::open(&path).expect("second connection");
    let set = |value: &str| {
        sql.execute(
            &format!("UPDATE {PREFIX}_identity SET stream_identity={value} WHERE tenant_id=?1"),
            params!["sqlite-capture-identity"],
        )
        .expect("replaced stored identity");
    };
    // What SQLite actually put in the column, measured rather than assumed: a fixture that does
    // not store what its SQL says would leave the case below testing nothing.
    let stored = || -> (String, Vec<u8>) {
        sql.query_row(
            &format!(
                "SELECT typeof(stream_identity), CAST(stream_identity AS BLOB)
                 FROM {PREFIX}_identity WHERE tenant_id=?1"
            ),
            params!["sqlite-capture-identity"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stored identity")
    };

    // A legacy value with no UUID syntax and surrounding whitespace is somebody's real identity.
    set("'  Legacy Identity/v0  '");
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("legacy identities are valid")
            .stream_identity,
        "  Legacy Identity/v0  ",
        "returned byte for byte: no trim, no normalization, no UUID rule"
    );

    // A numeric literal is not a numeric identity here. The column has TEXT affinity, so SQLite
    // converts the value to text before storing it, and what comes back is the nonempty string
    // "7" — a valid legacy identity, not corruption. Nothing may reject it to look stricter.
    set("7");
    assert_eq!(
        stored(),
        ("text".to_owned(), b"7".to_vec()),
        "TEXT affinity converted the numeric literal before storing it"
    );
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("a numeric-looking identity is still an identity")
            .stream_identity,
        "7",
        "exact preservation, with no grammar that reads meaning into the bytes"
    );

    // The same byte, stored under a class no writer here produces, is a different fact.
    for (value, class, what) in [
        ("''", "text", "empty"),
        ("CAST(x'ff' AS TEXT)", "text", "undecodable"),
        ("x'37'", "blob", "a storage class that is not text"),
    ] {
        set(value);
        assert_eq!(
            stored().0,
            class,
            "{what}: the fixture only means something while SQLite stores what it says"
        );
        assert_eq!(
            store.capture_tenant(&tenant, &[], limits()).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Identity
            }),
            "{what} stored identity is corruption, not permission to mint a replacement"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_and_physical_shape_drift_refuse_capture() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let concrete = Arc::new(opened(&path).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("sqlite-capture-shape").expect("valid tenant");
    concrete
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    port.create_projections(Arc::new(CaptureLedger))
        .await
        .expect("declared projections");
    // The control: both are admitted before anything is changed under them.
    concrete
        .capture_tenant(&tenant, &[CAPTURE_LEDGER, CAPTURE_SIDECAR], limits())
        .await
        .expect("complete observation");

    let sql = Connection::open(&path).expect("second connection");
    // A name in the registry is a declaration, not a table.
    sql.execute_batch(&format!(
        "DROP TABLE {PREFIX}_p_capture_sidecar;
         CREATE TABLE {PREFIX}_p_capture_sidecar (
             tenant_id TEXT NOT NULL,
             row_key TEXT NOT NULL COLLATE NOCASE,
             body TEXT NOT NULL,
             PRIMARY KEY (tenant_id, row_key));"
    ))
    .expect("replaced the stored table");
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_SIDECAR], limits())
            .await
            .expect_err("a foreign table is not this projection"),
        CaptureError::ProjectionUnavailable {
            projection: "capture_sidecar".to_owned(),
            reason: ProjectionCaptureRefusal::PhysicalShapeMismatch
        }
    );

    sql.execute_batch(&format!(
        "CREATE INDEX {PREFIX}_p_capture_ledger_extra ON {PREFIX}_p_capture_ledger(row_key)"
    ))
    .expect("added an index nobody declared");
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
            .await
            .expect_err("an undeclared index is a different shape"),
        CaptureError::ProjectionUnavailable {
            projection: "capture_ledger".to_owned(),
            reason: ProjectionCaptureRefusal::PhysicalShapeMismatch
        }
    );

    sql.execute(
        &format!(
            "UPDATE {PREFIX}_projection_registry SET indexed_fields='[\"other\"]'
             WHERE projection_name=?1"
        ),
        params!["capture_ledger"],
    )
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
}

#[tokio::test(flavor = "multi_thread")]
async fn admitted_projection_keys_include_an_embedded_nul() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let concrete = Arc::new(opened(&database(&directory)).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("sqlite-capture-keys").expect("valid tenant");
    concrete
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let runner = CatchUpRunner::new(Arc::clone(&port), Arc::new(CaptureLedger))
        .await
        .expect("declared projections");
    let keys = ["", "\u{0}", "a\u{0}b", " ", "é"];
    for (index, key) in keys.iter().enumerate() {
        let index = i64::try_from(index).expect("small index");
        port.append(
            &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
            Expected::Any,
            &[appended(key, index)],
            &eventlog_conformance::meta(&format!("key-{index}"), &json!({})),
        )
        .await
        .expect("appended");
    }
    eventlog_conformance::drain_at_least(&runner, &tenant, keys.len() as u64).await;
    let captured = concrete
        .capture_tenant(&tenant, &[CAPTURE_SIDECAR], limits())
        .await
        .expect("complete observation");
    let mut expected: Vec<&str> = keys.to_vec();
    expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(
        captured.projections[0]
            .rows
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        expected,
        "exact decoded bytes, in bytewise order, including the NUL this provider admits"
    );
}

/// Both paged reads decide their resume point from a storage class, not only from a value.
///
/// `read_rows` and `read_blobs` resume with `column > ?`, binding the previous page's last
/// coordinate as text. SQLite orders storage classes before values, so a BLOB coordinate is
/// greater than every text one *and* greater than any text bound as the resume point: the row is
/// re-selected on every page, `after` never advances past it, and what the caller is handed is a
/// cap its content never crossed. Neither column can hold such a value through this kit —
/// `ProjectionStore::upsert` takes `&str` and `put_blob` derives its digest — so both are
/// corruption. The class is both of them, not the one that was reported.
///
/// The caps keep the probe bounded: if the reading is right, the loop ends at a cap rather than
/// running forever. The timeout is only there so a wrong reading is reported instead of hanging.
#[tokio::test(flavor = "multi_thread")]
async fn a_paged_coordinate_outside_text_is_corruption_in_both_reads() {
    async fn admitted(store: &SqliteEventStore, tenant: &TenantId) -> Vec<String> {
        store
            .capture_tenant(tenant, &[CAPTURE_SIDECAR], limits())
            .await
            .expect("complete observation")
            .projections[0]
            .rows
            .iter()
            .map(|(key, _)| key.clone())
            .collect()
    }

    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let concrete = Arc::new(opened(&path).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("sqlite-capture-classes").expect("valid tenant");
    concrete
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let runner = CatchUpRunner::new(Arc::clone(&port), Arc::new(CaptureLedger))
        .await
        .expect("declared projections");
    // Every key this provider admits, so the refusals below cannot be a key grammar in disguise.
    let keys = ["", "\u{0}", "a\u{0}b", " ", "é"];
    for (index, key) in keys.iter().enumerate() {
        let index = i64::try_from(index).expect("small index");
        port.append(
            &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
            Expected::Any,
            &[appended(key, index)],
            &eventlog_conformance::meta(&format!("key-{index}"), &json!({})),
        )
        .await
        .expect("appended");
    }
    eventlog_conformance::drain_at_least(&runner, &tenant, keys.len() as u64).await;
    concrete
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    let mut expected: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
    expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(
        admitted(&concrete, &tenant).await,
        expected,
        "the control: every admitted key round-trips exactly, before anything foreign is stored"
    );

    let sql = Connection::open(&path).expect("second connection");
    sql.execute(
        &format!(
            "INSERT INTO {PREFIX}_p_capture_sidecar (tenant_id,row_key,body)
             VALUES (?1, x'7a', '{{\"foreign\":true}}')"
        ),
        params![tenant.as_str()],
    )
    .expect("stored a row key this kit could not write");
    // What SQLite actually put in the column, measured rather than assumed: TEXT affinity converts
    // a numeric literal before storing it, and leaves a BLOB alone.
    let class: String = sql
        .query_row(
            &format!(
                "SELECT typeof(row_key) FROM {PREFIX}_p_capture_sidecar
                 WHERE tenant_id=?1 AND CAST(row_key AS BLOB)=x'7a'"
            ),
            params![tenant.as_str()],
            |row| row.get(0),
        )
        .expect("stored row key");
    assert_eq!(
        class, "blob",
        "the fixture only means something while the column holds a class TEXT affinity keeps"
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(60),
            concrete.capture_tenant(&tenant, &[CAPTURE_SIDECAR], limits()),
        )
        .await
        .expect("the capture terminated"),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Projection
        }),
        "a stored row key no writer here produced is projection corruption, refused before it can \
         become a text resume bound"
    );

    let removed = sql
        .execute(
            &format!("DELETE FROM {PREFIX}_p_capture_sidecar WHERE typeof(row_key)<>'text'"),
            [],
        )
        .expect("removed the foreign row");
    assert_eq!(removed, 1, "exactly one row was foreign");
    assert_eq!(
        admitted(&concrete, &tenant).await,
        expected,
        "removing that one row restores the whole observation: nothing else was refused"
    );

    // The same fault on the other paged coordinate. Every other column is copied from a row this
    // kit wrote, so the digest's storage class is the only difference between the two bindings.
    let copied = sql
        .execute(
            &format!(
                "INSERT INTO {PREFIX}_blobs
                     (tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1)
                 SELECT tenant_id,CAST(digest AS BLOB),bytes,byte_count,recorded_at,
                        integrity_sha256,integrity_v1
                 FROM {PREFIX}_blobs WHERE tenant_id=?1 AND typeof(digest)='text'"
            ),
            params![tenant.as_str()],
        )
        .expect("stored a digest this kit could not write");
    assert_eq!(
        copied, 1,
        "one foreign binding, differing only in storage class"
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(60),
            concrete.capture_tenant(&tenant, &[], limits()),
        )
        .await
        .expect("the capture terminated"),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a stored digest no writer here produced is blob corruption, refused before it can become \
         a text resume bound"
    );
}

/// A stored length proves nothing about a payload until it is proven against the bytes.
///
/// `preflight` decides the payload cap from the blob table inside the capture transaction, before
/// any blob is decoded. The design asks for that preflight *and* for "a malformed length is
/// corruption" *and* for "every reported limit must actually have been exceeded", so the length it
/// sums has to be the length the bytes actually have. The observation stays native and bounded:
/// SQLite computes the lengths inside the same transaction and no blob is loaded, let alone
/// allocated, to find one out.
#[tokio::test(flavor = "multi_thread")]
async fn a_stored_blob_length_is_proven_against_the_bytes_before_any_payload_cap() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let store = opened(&path).await;
    let tenant = TenantId::new("sqlite-capture-lengths").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    // The control: eleven bytes, and every cap here is far above them.
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation")
            .blobs[0]
            .bytes,
        b"bound-bytes".to_vec()
    );

    let sql = Connection::open(&path).expect("second connection");
    let set = |count: i64| {
        assert_eq!(
            sql.execute(
                &format!("UPDATE {PREFIX}_blobs SET byte_count=?2 WHERE tenant_id=?1"),
                params![tenant.as_str(), count],
            )
            .expect("replaced the stored length"),
            1,
            "the fixture only means something while exactly one stored length is a lie"
        );
    };
    // A tight cap the eleven real bytes do cross, so an understatement that fits it is still the
    // value the old preflight would have proven the refusal from.
    let tight = CaptureLimits {
        max_events: 16,
        max_blobs: 16,
        max_projection_rows: 16,
        max_payload_bytes: 4,
    };
    for (count, caps, what) in [
        (
            1_099_511_627_776_i64,
            limits(),
            "an overstated length, under a cap the content never reaches",
        ),
        (
            5,
            tight,
            "an understated length, under a cap it alone crosses",
        ),
        (-1, limits(), "a length no writer here could have stored"),
    ] {
        set(count);
        assert_eq!(
            store.capture_tenant(&tenant, &[], caps).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Blob
            }),
            "{what}: a malformed stored length is corruption, not a crossed payload cap"
        );
    }

    // Restored, the same reader still refuses a cap the content does cross, and names the resource
    // and the caller's own limit. Validating a length is not an excuse to stop counting.
    set(11);
    assert_eq!(
        store.capture_tenant(&tenant, &[], tight).await,
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::PayloadBytes,
            limit: 4
        }),
        "eleven stored bytes cross a four-byte cap, and that limit was actually exceeded"
    );
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], limits())
            .await
            .expect("complete observation")
            .blobs[0]
            .bytes,
        b"bound-bytes".to_vec(),
        "and the restored store is the one the control read"
    );
}
