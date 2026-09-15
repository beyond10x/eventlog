use eventlog_sqlite::SqliteEventStore;

#[tokio::test]
async fn the_shared_exercise_passes_in_memory() {
    let store = SqliteEventStore::in_memory("corpus")
        .await
        .expect("an in-memory store");
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn the_shared_exercise_passes_on_a_file() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let store = SqliteEventStore::open(path.to_str().expect("utf-8 path"), "corpus")
        .await
        .expect("a file-backed store");
    eventlog_conformance::run(&store).await;
}

#[tokio::test]
async fn two_owners_share_a_database_without_sharing_a_table() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("eventlog.sqlite3");
    let path = path.to_str().expect("utf-8 path");
    let corpus = SqliteEventStore::open(path, "corpus")
        .await
        .expect("a store");
    let planner = SqliteEventStore::open(path, "planner")
        .await
        .expect("a store");
    eventlog_conformance::run(&corpus).await;
    eventlog_conformance::run(&planner).await;
}

#[tokio::test]
async fn the_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_projections(&store).await;
}

#[tokio::test]
async fn the_inline_projection_exercise_passes_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_inline_projections(&store).await;
}

#[tokio::test]
async fn the_paging_rule_holds_in_memory() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("corpus")
            .await
            .expect("a store"),
    );
    eventlog_conformance::run_paging(&store).await;
}

#[tokio::test]
async fn the_claim_rule_holds_in_memory() {
    let store = SqliteEventStore::in_memory("corpus")
        .await
        .expect("a store");
    eventlog_conformance::run_claims(&store).await;
}

/// The failure a running deployment found and no test could have.
///
/// Planner and colab both had a `<prefix>_events` table from the store they used before the log.
/// `CREATE TABLE IF NOT EXISTS` skipped it, and the first sign was a missing column inside a wall
/// of DDL, naming neither the database nor the collision. Every test starts empty, so nothing here
/// had ever opened a database that was not.
#[tokio::test]
async fn a_table_of_ours_that_somebody_else_made_is_refused_by_name() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("previous-store.sqlite3");

    // Exactly the shape planner's old store left behind.
    let connection = rusqlite::Connection::open(&path).expect("a database");
    connection
        .execute_batch(
            "CREATE TABLE planner_events (
                 tenant_id TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 body TEXT NOT NULL
             );",
        )
        .expect("the previous store's table");
    drop(connection);

    let Err(refused) = SqliteEventStore::open(path.to_str().expect("utf-8 path"), "planner").await
    else {
        panic!("a table this kit did not create must not be silently adopted");
    };
    let message = refused.to_string();
    assert!(
        message.contains("planner_events"),
        "the refusal names the table: {message}"
    );
    assert!(
        message.contains("previous store"),
        "and says what it probably is, so somebody knows what to do: {message}"
    );

    // A database this kit did make opens again without complaint.
    let fresh = directory.path().join("ours.sqlite3");
    let fresh = fresh.to_str().expect("utf-8 path");
    SqliteEventStore::open(fresh, "planner")
        .await
        .expect("a new database");
    SqliteEventStore::open(fresh, "planner")
        .await
        .expect("reopening one of ours is not a collision");
}

#[tokio::test]
async fn rebuild_preserves_other_tenants_and_previous_view_on_failure() {
    let store: std::sync::Arc<dyn eventlog_core::EventStore> = std::sync::Arc::new(
        SqliteEventStore::in_memory("rebuild_isolation")
            .await
            .expect("store"),
    );
    eventlog_conformance::run_rebuild_isolation(&store).await;
}

#[tokio::test]
async fn scope_reservations_are_atomic_and_confined_in_memory_and_file() {
    let memory = SqliteEventStore::in_memory("scope_atomic")
        .await
        .expect("memory");
    eventlog_conformance::run_scope_atomicity(&memory, &memory.admission_permit()).await;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("scopes.sqlite");
    let file = SqliteEventStore::open(path.to_str().expect("path"), "scope_atomic")
        .await
        .expect("file");
    eventlog_conformance::run_scope_atomicity(&file, &file.admission_permit()).await;
}

#[tokio::test]
async fn inline_failure_preserves_all_atomic_state_and_callback_authority() {
    let memory = SqliteEventStore::in_memory("inline_atomic")
        .await
        .expect("memory");
    eventlog_conformance::run_inline_failure_atomicity(&memory, &memory.admission_permit()).await;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("atomic.sqlite");
    let file = SqliteEventStore::open(path.to_str().expect("path"), "inline_atomic")
        .await
        .expect("file");
    eventlog_conformance::run_inline_failure_atomicity(&file, &file.admission_permit()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_differing_blob_writers_have_one_winner() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("blob-race.sqlite3");
    let path = path.to_str().expect("path");
    let first = SqliteEventStore::open(path, "blob_race")
        .await
        .expect("first connection");
    let second = SqliteEventStore::open(path, "blob_race")
        .await
        .expect("second connection");
    eventlog_conformance::run_blob_binding_race(&first, &second).await;
}

#[tokio::test]
async fn sqlite_blob_integrity_covers_all_read_boundaries_and_binding_controls() {
    use eventlog_core::{EventLogError, EventStore, TenantId};

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("blob-integrity.sqlite3");
    let path = path.to_str().expect("path");
    let tenant = TenantId::new("blob-integrity").expect("tenant");
    let store = SqliteEventStore::open(path, "blob_integrity")
        .await
        .expect("store");
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .expect("binding");
    drop(store);

    let sql = rusqlite::Connection::open(path).expect("raw connection");
    sql.execute(
        "UPDATE blob_integrity_blobs SET bytes=?1 WHERE tenant_id=?2 AND digest=?3",
        rusqlite::params![b"mutated".as_slice(), tenant.as_str(), "opaque"],
    )
    .expect("same-length corruption");
    drop(sql);

    let reopened = SqliteEventStore::open(path, "blob_integrity")
        .await
        .expect("reopened store");
    assert!(
        matches!(
            reopened.get_blob(&tenant, "opaque").await,
            Err(EventLogError::Backend(_))
        ),
        "same-length corruption must be refused after reopening"
    );

    assert!(matches!(
        reopened.put_blob(&tenant, "opaque", b"original").await,
        Err(EventLogError::Backend(_))
    ));
    let raw = rusqlite::Connection::open(path).expect("raw connection");
    for statement in [
        "UPDATE blob_integrity_blobs SET bytes=X'6f726967696e616c', byte_count=7, integrity_sha256='BAD' WHERE tenant_id='blob-integrity' AND digest='opaque'",
        "UPDATE blob_integrity_blobs SET integrity_sha256='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' WHERE tenant_id='blob-integrity' AND digest='opaque'",
        "UPDATE blob_integrity_blobs SET integrity_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE tenant_id='blob-integrity' AND digest='opaque'",
    ] {
        raw.execute_batch(statement).expect("causal hash mutation");
        assert!(matches!(
            reopened.get_blob(&tenant, "opaque").await,
            Err(EventLogError::Backend(_))
        ));
    }
    raw.execute_batch("UPDATE blob_integrity_blobs SET integrity_sha256='0682c5f2076f099c34cfdd15a9e063849ed437a49677e6fcc5b4198c76575be5', byte_count=8 WHERE tenant_id='blob-integrity' AND digest='opaque'")
        .expect("restore exact metadata");
    assert_eq!(
        reopened
            .get_blob(&tenant, "opaque")
            .await
            .expect("valid read"),
        Some(b"original".to_vec())
    );
}

#[tokio::test]
async fn sqlite_callback_blob_corruption_poison_rolls_back_every_owner() {
    use eventlog_core::{
        AdmissionScope, AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected,
        Reservation, StreamAppend, StreamId, TenantId,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("blob-callback.sqlite3");
    let path_text = path.to_str().expect("path");
    let tenant = TenantId::new("blob-callback").expect("tenant");
    let store = SqliteEventStore::open(path_text, "blob_callback")
        .await
        .expect("store");
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let projector = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_inline",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    store
        .register_inline(projector.clone())
        .await
        .expect("inline projector");
    let first = StreamId::new(tenant.clone(), "item", "present").unwrap();
    store
        .append(
            &first,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("present", &serde_json::json!({})),
        )
        .await
        .expect("present callback read");
    let before = store
        .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
        .await
        .unwrap();

    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE blob_callback_blobs SET bytes=?1 WHERE tenant_id=?2 AND digest='opaque'",
            rusqlite::params![b"mutated".as_slice(), tenant.as_str()],
        )
        .unwrap();
    for (mode_value, id) in [(1, "propagated"), (2, "caught")] {
        mode.store(mode_value, Ordering::Release);
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        assert!(matches!(
            store
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta(id, &serde_json::json!({}))
                )
                .await,
            Err(EventLogError::Backend(_))
        ));
        assert!(
            store
                .read_stream(&stream, 0, 10)
                .await
                .unwrap()
                .events
                .is_empty()
        );
        assert!(
            store
                .projection_get(
                    &eventlog_conformance::BLOB_PROBE,
                    &tenant,
                    &format!("item/{id}")
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    let guarded = StreamId::new(tenant.clone(), "item", "guarded").unwrap();
    let guarded_check = Arc::new(eventlog_conformance::BlobReadingGuard {
        digest: "opaque".into(),
        catch_failure: true,
        reservation: Some((
            store.admission_permit(),
            vec![Reservation {
                scope: AdmissionScope::Tenant {
                    tenant: tenant.clone(),
                    key: "blob-single".into(),
                },
                delta: 1,
                ceiling: 1,
            }],
        )),
    });
    assert!(matches!(
        store
            .append_guarded(
                &guarded,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("guarded", &serde_json::json!({})),
                guarded_check.clone()
            )
            .await,
        Err(EventLogError::Backend(_))
    ));
    assert!(
        store
            .read_stream(&guarded, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );

    let group_stream = StreamId::new(tenant.clone(), "item", "grouped").unwrap();
    let group = AppendGroup {
        tenant: tenant.clone(),
        appends: vec![StreamAppend {
            stream: group_stream.clone(),
            expected: Expected::NoStream,
            events: vec![eventlog_conformance::event("item.received", 1)],
        }],
        meta: eventlog_conformance::meta("grouped", &serde_json::json!({})),
    };
    let group_check = Arc::new(eventlog_conformance::BlobReadingGuard {
        digest: "opaque".into(),
        catch_failure: true,
        reservation: Some((
            store.admission_permit(),
            vec![Reservation {
                scope: AdmissionScope::Tenant {
                    tenant: tenant.clone(),
                    key: "blob-group".into(),
                },
                delta: 1,
                ceiling: 1,
            }],
        )),
    });
    assert!(matches!(
        store
            .append_group_guarded(&group, group_check.clone())
            .await,
        Err(EventLogError::Backend(_))
    ));
    assert!(
        store
            .read_stream(&group_stream, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    let catch_up = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_catchup",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    store.create_projections(catch_up.clone()).await.unwrap();
    assert!(matches!(
        store.run_catch_up(catch_up.clone(), &tenant, 10).await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
            .await
            .unwrap(),
        before,
        "failed catch-up preserves the active view and cursor"
    );
    let rebuild = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "blob_integrity_rebuild",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    assert!(matches!(
        store.rebuild_projection(rebuild.clone(), &tenant).await,
        Err(EventLogError::Backend(_))
    ));
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::BLOB_PROBE, &tenant, "item/present")
            .await
            .unwrap(),
        before,
        "failed shadow rebuild preserves the active view"
    );

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("UPDATE blob_callback_blobs SET bytes=X'6f726967696e616c', byte_count=8, integrity_sha256='0682c5f2076f099c34cfdd15a9e063849ed437a49677e6fcc5b4198c76575be5' WHERE tenant_id='blob-callback' AND digest='opaque'")
        .unwrap();
    mode.store(0, Ordering::Release);
    assert!(
        !store
            .append_guarded(
                &guarded,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("guarded", &serde_json::json!({})),
                guarded_check,
            )
            .await
            .expect("failed callback rolled back command receipt and reservation")
            .deduplicated
    );
    let group_result = store
        .append_group_guarded(&group, group_check)
        .await
        .expect("failed callback rolled back group receipt and reservation");
    assert!(!group_result.deduplicated);
    assert!(store.append_group(&group).await.unwrap().deduplicated);
    assert_eq!(
        store
            .run_catch_up(catch_up, &tenant, 10)
            .await
            .unwrap()
            .applied,
        3,
        "successful retry proves the failed pass did not advance its cursor"
    );
    assert_eq!(store.rebuild_projection(rebuild, &tenant).await.unwrap(), 3);
}

#[tokio::test]
async fn sqlite_blob_migration_is_exact_explicit_atomic_and_fenced() {
    use eventlog_core::{EventLogError, EventStore, LegacyBlobMigration, TenantId};

    fn downgrade(path: &std::path::Path, prefix: &str) {
        let sql = rusqlite::Connection::open(path).unwrap();
        sql.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             ALTER TABLE {prefix}_blobs RENAME TO {prefix}_blobs_current;
             CREATE TABLE {prefix}_blobs (
                 tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id,digest));
             INSERT INTO {prefix}_blobs(tenant_id,digest,bytes,byte_count,recorded_at)
                 SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {prefix}_blobs_current;
             DROP TABLE {prefix}_blobs_current;
             COMMIT;"
        ))
        .unwrap();
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("migration.sqlite3");
    let tenant = TenantId::new("legacy-tenant").unwrap();
    let store = SqliteEventStore::open(path.to_str().unwrap(), "legacy")
        .await
        .unwrap();
    store
        .put_blob(&tenant, "opaque", b"legacy bytes")
        .await
        .unwrap();
    drop(store);
    downgrade(&path, "legacy");

    assert!(matches!(
        SqliteEventStore::open(path.to_str().unwrap(), "legacy").await,
        Err(EventLogError::Invalid(_))
    ));
    let raw = rusqlite::Connection::open(&path).unwrap();
    let before: (Vec<u8>, i64, String) = raw
        .query_row(
            "SELECT bytes,byte_count,recorded_at FROM legacy_blobs WHERE tenant_id='legacy-tenant' AND digest='opaque'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let columns: i64 = raw
        .query_row(
            "SELECT count(*) FROM pragma_table_info('legacy_blobs')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        columns, 5,
        "default refusal leaves predecessor shape intact"
    );
    drop(raw);

    let (migrated, report) = SqliteEventStore::open_with_blob_migration(
        path.to_str().unwrap(),
        "legacy",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .unwrap();
    assert!(report.upgraded);
    assert_eq!(report.trusted_legacy_rows, 1);
    assert_eq!(
        migrated.get_blob(&tenant, "opaque").await.unwrap(),
        Some(before.0.clone())
    );
    drop(migrated);
    let raw = rusqlite::Connection::open(&path).unwrap();
    let after: (Vec<u8>, i64, String) = raw
        .query_row(
            "SELECT bytes,byte_count,recorded_at FROM legacy_blobs WHERE tenant_id='legacy-tenant' AND digest='opaque'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        after, before,
        "migration preserves predecessor payload metadata"
    );
    assert!(
        raw.execute(
            "INSERT INTO legacy_blobs VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params!["old-reader", "opaque", b"bytes".as_slice(), 5, before.2],
        )
        .is_err()
    );
    drop(raw);
    let (_, retry) = SqliteEventStore::open_with_blob_migration(
        path.to_str().unwrap(),
        "legacy",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .unwrap();
    assert!(!retry.upgraded);
    assert_eq!(retry.trusted_legacy_rows, 0);

    let malformed_path = directory.path().join("malformed.sqlite3");
    let malformed = SqliteEventStore::open(malformed_path.to_str().unwrap(), "malformed")
        .await
        .unwrap();
    malformed
        .put_blob(&tenant, "opaque", b"bytes")
        .await
        .unwrap();
    drop(malformed);
    downgrade(&malformed_path, "malformed");
    rusqlite::Connection::open(&malformed_path)
        .unwrap()
        .execute("UPDATE malformed_blobs SET byte_count=99", [])
        .unwrap();
    assert!(matches!(
        SqliteEventStore::open_with_blob_migration(
            malformed_path.to_str().unwrap(),
            "malformed",
            LegacyBlobMigration::TrustObservedBytes
        )
        .await,
        Err(EventLogError::Backend(_))
    ));
    let raw = rusqlite::Connection::open(&malformed_path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM pragma_table_info('malformed_blobs')",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        5,
        "failed migration rolls back its additive columns"
    );
}

#[tokio::test]
async fn review_blob_empty_content_and_conflict_rollback_reuse() {
    use eventlog_core::{EventLogError, EventStore, TenantId};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("review.sqlite3");
    let first = SqliteEventStore::open(path.to_str().unwrap(), "review")
        .await
        .unwrap();
    let second = SqliteEventStore::open(path.to_str().unwrap(), "review")
        .await
        .unwrap();
    let tenant = TenantId::new("review").unwrap();
    first.put_blob(&tenant, "opaque", b"").await.unwrap();
    second.put_blob(&tenant, "opaque", b"").await.unwrap();
    for _ in 0..32 {
        assert!(matches!(
            first.put_blob(&tenant, "opaque", b"nonempty").await,
            Err(EventLogError::Invalid(_))
        ));
        assert_eq!(
            second.get_blob(&tenant, "opaque").await.unwrap(),
            Some(Vec::new())
        );
    }
    first.delete_blob(&tenant, "opaque").await.unwrap();
    second
        .put_blob(&tenant, "opaque", b"rebound")
        .await
        .unwrap();
    assert!(matches!(
        first.put_blob(&tenant, "opaque", b"").await,
        Err(EventLogError::Invalid(_))
    ));
    assert_eq!(
        first.get_blob(&tenant, "opaque").await.unwrap().as_deref(),
        Some(&b"rebound"[..])
    );
}
