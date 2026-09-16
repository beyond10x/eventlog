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
async fn review_caught_malformed_blob_metadata_poisons_sqlite_inline_append() {
    use eventlog_core::{EventLogError, EventStore, Expected, StreamId, TenantId};
    use std::sync::{Arc, atomic::AtomicU8};

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("malformed-callback.sqlite3");
    let tenant = TenantId::new("malformed-callback").expect("tenant");
    let store = SqliteEventStore::open(path.to_str().expect("path"), "malformed_callback")
        .await
        .expect("store");
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .expect("binding");

    let mode = Arc::new(AtomicU8::new(2));
    let projector = Arc::new(eventlog_conformance::BlobReadingProjector {
        driver_name: "malformed_callback_inline",
        digest: "opaque".into(),
        mode: Arc::clone(&mode),
    });
    store
        .register_inline(projector)
        .await
        .expect("inline projector");

    let raw = rusqlite::Connection::open(&path).expect("raw connection");
    raw.execute(
        "UPDATE malformed_callback_blobs SET byte_count='not-an-integer' \
         WHERE tenant_id=?1 AND digest='opaque'",
        rusqlite::params![tenant.as_str()],
    )
    .expect("malformed stored count");
    drop(raw);

    let stream = StreamId::new(tenant.clone(), "item", "caught-malformed").expect("stream");
    let result = store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("caught-malformed", &serde_json::json!({})),
        )
        .await;
    assert!(
        matches!(result, Err(EventLogError::Backend(_))),
        "a caught decode error from malformed stored metadata must poison the append: {result:?}"
    );
    assert!(
        store
            .read_stream(&stream, 0, 10)
            .await
            .expect("read stream")
            .events
            .is_empty(),
        "the poisoned append must not retain its event"
    );
    assert!(
        store
            .projection_get(
                &eventlog_conformance::BLOB_PROBE,
                &tenant,
                "item/caught-malformed",
            )
            .await
            .expect("read projection")
            .is_none(),
        "the poisoned append must not retain its view update"
    );
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
async fn review_sqlite_admission_requires_an_enforced_integrity_check() {
    use eventlog_core::EventLogError;

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("foreign-integrity-check.sqlite3");
    let raw = rusqlite::Connection::open(&path).expect("raw connection");
    raw.execute_batch(
        "CREATE TABLE foreign_check_blobs (
             tenant_id TEXT NOT NULL,
             digest TEXT NOT NULL,
             bytes BLOB NOT NULL,
             byte_count INTEGER NOT NULL,
             recorded_at TEXT NOT NULL,
             integrity_sha256 TEXT,
             integrity_v1 INTEGER NOT NULL DEFAULT 1,
             CONSTRAINT \"CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)\" CHECK (1),
             PRIMARY KEY (tenant_id, digest)
         );",
    )
    .expect("current-like foreign table without the integrity constraint");
    drop(raw);

    let admitted = SqliteEventStore::open(path.to_str().expect("path"), "foreign_check").await;
    assert!(
        matches!(admitted, Err(EventLogError::Invalid(_))),
        "SQLite admission must verify an enforced integrity check, not matching text in a constraint name"
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

#[tokio::test]
async fn review2_sqlite_admission_refuses_hidden_collation_and_conflict_clauses() {
    use eventlog_core::EventLogError;

    // Every table below has the exact admitted columns, declared types, nullability, defaults,
    // primary key, single pk index and the exact integrity CHECK. Each also carries one clause
    // that PRAGMA table_xinfo, index_list and the CHECK tokenizer cannot observe.
    let mut admitted_clauses = Vec::new();
    for (label, ddl) in [
        (
            "nocase-digest",
            "CREATE TABLE hidden_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL COLLATE NOCASE,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest)
             );",
        ),
        (
            "pk-on-conflict-replace",
            "CREATE TABLE hidden_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest) ON CONFLICT REPLACE
             );",
        ),
        (
            "foreign-key-cascade",
            "CREATE TABLE hidden_parent (id TEXT PRIMARY KEY);
             CREATE TABLE hidden_blobs (
                 tenant_id TEXT NOT NULL REFERENCES hidden_parent(id) ON DELETE CASCADE,
                 digest TEXT NOT NULL,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest)
             );",
        ),
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join(format!("{label}.sqlite3"));
        rusqlite::Connection::open(&path)
            .expect("raw connection")
            .execute_batch(ddl)
            .expect("foreign current-like blob table");
        let admitted = SqliteEventStore::open(path.to_str().expect("path"), "hidden").await;
        let refused = matches!(&admitted, Err(EventLogError::Invalid(_)));
        eprintln!("{label}: refused={refused} admitted={}", admitted.is_ok());
        if !refused {
            admitted_clauses.push(label);
        }
    }
    assert!(
        admitted_clauses.is_empty(),
        "similar foreign blob tables must be refused before they are served; admitted clauses: {admitted_clauses:?}"
    );
}

#[tokio::test]
async fn review2_sqlite_nocase_digest_table_must_not_serve_another_identity() {
    use eventlog_core::{EventStore, TenantId};

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("nocase.sqlite3");
    rusqlite::Connection::open(&path)
        .expect("raw connection")
        .execute_batch(
            "CREATE TABLE nocase_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL COLLATE NOCASE,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest)
             );",
        )
        .expect("foreign table");
    // Refusal at open is the required behaviour; this case then has nothing left to prove.
    let Ok(store) = SqliteEventStore::open(path.to_str().expect("path"), "nocase").await else {
        return;
    };
    let tenant = TenantId::new("nocase").expect("tenant");
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .expect("first binding");
    assert_eq!(
        store.get_blob(&tenant, "OPAQUE").await.expect("read"),
        None,
        "a distinct opaque digest spelling must not return another identity's bytes"
    );
    assert!(
        store
            .put_blob(&tenant, "OPAQUE", b"different")
            .await
            .is_ok(),
        "a never-bound opaque digest spelling must be bindable"
    );
}

#[tokio::test]
async fn review2_sqlite_similar_legacy_predecessors_are_refused_before_any_persistent_change() {
    use eventlog_core::{EventStore, LegacyBlobMigration, TenantId};

    fn downgrade_with(path: &std::path::Path, prefix: &str, extra_clause: &str) {
        let sql = rusqlite::Connection::open(path).unwrap();
        sql.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             ALTER TABLE {prefix}_blobs RENAME TO {prefix}_blobs_current;
             CREATE TABLE {prefix}_blobs (
                 tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id,digest){extra_clause});
             INSERT INTO {prefix}_blobs(tenant_id,digest,bytes,byte_count,recorded_at)
                 SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {prefix}_blobs_current;
             DROP TABLE {prefix}_blobs_current;
             COMMIT;"
        ))
        .unwrap();
    }
    fn column_count(path: &std::path::Path, table: &str) -> i64 {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                &format!("SELECT count(*) FROM pragma_table_info('{table}')"),
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("legacy-tenant").unwrap();

    // (a) A five-column predecessor that also carries an extra CHECK: the migration must refuse
    //     and leave no additive column behind.
    let extra = directory.path().join("extra-check.sqlite3");
    let store = SqliteEventStore::open(extra.to_str().unwrap(), "legacy")
        .await
        .unwrap();
    store.put_blob(&tenant, "opaque", b"bytes").await.unwrap();
    drop(store);
    downgrade_with(&extra, "legacy", ", CHECK (byte_count >= 0)");
    let result = SqliteEventStore::open_with_blob_migration(
        extra.to_str().unwrap(),
        "legacy",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await;
    assert!(
        result.is_err(),
        "a predecessor with an extra CHECK is not the exact supported predecessor"
    );
    drop(result);
    assert_eq!(
        column_count(&extra, "legacy_blobs"),
        5,
        "refusal must roll back additive columns"
    );

    // (b) A predecessor row whose recorded_at cannot be parsed: refuse, roll back.
    let clock = directory.path().join("bad-clock.sqlite3");
    let store = SqliteEventStore::open(clock.to_str().unwrap(), "legacy")
        .await
        .unwrap();
    store.put_blob(&tenant, "opaque", b"bytes").await.unwrap();
    drop(store);
    downgrade_with(&clock, "legacy", "");
    rusqlite::Connection::open(&clock)
        .unwrap()
        .execute_batch("UPDATE legacy_blobs SET recorded_at='yesterday'")
        .unwrap();
    let result = SqliteEventStore::open_with_blob_migration(
        clock.to_str().unwrap(),
        "legacy",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await;
    assert!(
        result.is_err(),
        "a malformed predecessor row aborts the complete migration"
    );
    drop(result);
    assert_eq!(column_count(&clock, "legacy_blobs"), 5);

    // (c) A fresh current table refuses the old five-column INSERT shape outright.
    let fresh = directory.path().join("fresh.sqlite3");
    let store = SqliteEventStore::open(fresh.to_str().unwrap(), "fresh")
        .await
        .unwrap();
    drop(store);
    let raw = rusqlite::Connection::open(&fresh).unwrap();
    assert!(
        raw.execute(
            "INSERT INTO fresh_blobs (tenant_id,digest,bytes,byte_count,recorded_at) VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params!["old-writer", "opaque", b"bytes".as_slice(), 5, "2026-01-01T00:00:00Z"],
        )
        .is_err(),
        "an old five-column writer must be refused by the fresh edition too"
    );
}

#[tokio::test]
async fn review2_sqlite_hidden_clauses_change_binding_semantics_once_admitted() {
    use eventlog_core::{EventLogError, EventStore, TenantId};

    let directory = tempfile::tempdir().expect("directory");
    let tenant = TenantId::new("hidden").expect("tenant");

    // (a) PRIMARY KEY ... ON CONFLICT REPLACE: what does a conflicting put do?
    let replace = directory.path().join("replace.sqlite3");
    rusqlite::Connection::open(&replace)
        .unwrap()
        .execute_batch(
            "CREATE TABLE hidden_blobs (
                 tenant_id TEXT NOT NULL, digest TEXT NOT NULL, bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL, recorded_at TEXT NOT NULL, integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest) ON CONFLICT REPLACE);",
        )
        .unwrap();
    if let Ok(store) = SqliteEventStore::open(replace.to_str().unwrap(), "hidden").await {
        store
            .put_blob(&tenant, "opaque", b"original")
            .await
            .unwrap();
        let second = store.put_blob(&tenant, "opaque", b"replaced").await;
        let stored = store.get_blob(&tenant, "opaque").await.unwrap();
        eprintln!("on-conflict-replace: second put={second:?} stored={stored:?}");
        assert!(
            matches!(second, Err(EventLogError::Invalid(_))),
            "immutable binding must refuse different content"
        );
        assert_eq!(
            stored,
            Some(b"original".to_vec()),
            "immutable binding must keep the first bytes"
        );
    }

    // (b) REFERENCES ... ON DELETE CASCADE: a foreign delete path removes an admitted binding.
    let cascade = directory.path().join("cascade.sqlite3");
    rusqlite::Connection::open(&cascade)
        .unwrap()
        .execute_batch(
            "CREATE TABLE hidden_parent (id TEXT PRIMARY KEY);
             INSERT INTO hidden_parent VALUES ('hidden');
             CREATE TABLE hidden_blobs (
                 tenant_id TEXT NOT NULL REFERENCES hidden_parent(id) ON DELETE CASCADE,
                 digest TEXT NOT NULL, bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL, recorded_at TEXT NOT NULL, integrity_sha256 TEXT,
                 integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
                 PRIMARY KEY (tenant_id, digest));",
        )
        .unwrap();
    if let Ok(store) = SqliteEventStore::open(cascade.to_str().unwrap(), "hidden").await {
        store
            .put_blob(&tenant, "opaque", b"original")
            .await
            .unwrap();
        rusqlite::Connection::open(&cascade)
            .unwrap()
            .execute_batch("PRAGMA foreign_keys=ON; DELETE FROM hidden_parent;")
            .unwrap();
        let stored = store.get_blob(&tenant, "opaque").await.unwrap();
        eprintln!("cascade: stored after foreign parent delete={stored:?}");
        assert_eq!(
            stored,
            Some(b"original".to_vec()),
            "an admitted binding must not be deletable through a foreign cascade path"
        );
    }
}

#[tokio::test]
async fn review2_sqlite_legacy_admission_refuses_hidden_clauses_before_additive_migration() {
    use eventlog_core::{EventStore, LegacyBlobMigration, TenantId};

    fn downgrade_to_predecessor(path: &std::path::Path, prefix: &str, digest_column: &str) {
        rusqlite::Connection::open(path)
            .unwrap()
            .execute_batch(&format!(
                "BEGIN IMMEDIATE;
                 ALTER TABLE {prefix}_blobs RENAME TO {prefix}_blobs_current;
                 CREATE TABLE {prefix}_blobs (
                     tenant_id TEXT NOT NULL,{digest_column},bytes BLOB NOT NULL,
                     byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,
                     PRIMARY KEY (tenant_id,digest));
                 INSERT INTO {prefix}_blobs(tenant_id,digest,bytes,byte_count,recorded_at)
                     SELECT tenant_id,digest,bytes,byte_count,recorded_at
                     FROM {prefix}_blobs_current;
                 DROP TABLE {prefix}_blobs_current;
                 COMMIT;"
            ))
            .unwrap();
    }
    fn column_count(path: &std::path::Path, table: &str) -> i64 {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                &format!("SELECT count(*) FROM pragma_table_info('{table}')"),
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
    fn stored_sql(path: &std::path::Path, table: &str) -> String {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                rusqlite::params![table],
                |row| row.get(0),
            )
            .unwrap()
    }

    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("legacy-tenant").unwrap();

    // A clause no column, index or check pragma reports survives the whole legacy path: the
    // predecessor is recognised, the additive columns are committed onto it, and the result is
    // then admitted as current. The refusal has to happen before the first ALTER.
    let hidden = directory.path().join("hidden-collation.sqlite3");
    let store = SqliteEventStore::open(hidden.to_str().unwrap(), "legacy")
        .await
        .unwrap();
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .unwrap();
    drop(store);
    downgrade_to_predecessor(&hidden, "legacy", "digest TEXT NOT NULL COLLATE NOCASE");
    assert!(
        SqliteEventStore::open_with_blob_migration(
            hidden.to_str().unwrap(),
            "legacy",
            LegacyBlobMigration::TrustObservedBytes,
        )
        .await
        .is_err(),
        "a predecessor carrying an unrecognised clause must not be migrated"
    );
    assert_eq!(
        column_count(&hidden, "legacy_blobs"),
        5,
        "the refusal precedes the additive columns"
    );
    assert!(
        stored_sql(&hidden, "legacy_blobs").contains("COLLATE"),
        "the refused predecessor is left exactly as it was found"
    );

    // Control: the same fixture without that clause is the exact supported predecessor, migrates,
    // and is admitted again on reopen.
    let supported = directory.path().join("supported.sqlite3");
    let store = SqliteEventStore::open(supported.to_str().unwrap(), "legacy")
        .await
        .unwrap();
    store
        .put_blob(&tenant, "opaque", b"original")
        .await
        .unwrap();
    drop(store);
    downgrade_to_predecessor(&supported, "legacy", "digest TEXT NOT NULL");
    let (migrated, report) = SqliteEventStore::open_with_blob_migration(
        supported.to_str().unwrap(),
        "legacy",
        LegacyBlobMigration::TrustObservedBytes,
    )
    .await
    .expect("the exact supported predecessor still migrates");
    assert!(report.upgraded);
    assert_eq!(report.trusted_legacy_rows, 1);
    assert_eq!(
        migrated.get_blob(&tenant, "opaque").await.unwrap(),
        Some(b"original".to_vec())
    );
    drop(migrated);
    assert_eq!(column_count(&supported, "legacy_blobs"), 7);
    SqliteEventStore::open(supported.to_str().unwrap(), "legacy")
        .await
        .expect("the migrated table is admitted on reopen");
}

#[tokio::test]
async fn review2_sqlite_admission_refuses_unrecognized_blob_table_semantics() {
    use eventlog_core::EventLogError;

    // Every body below declares the admitted columns, types, nullability, defaults, primary key,
    // single key index and exact integrity check. Each carries one further clause, and each clause
    // is a member of the same class: physical behaviour the stored table declares and no pragma
    // this admission reads reports back.
    let current = "tenant_id TEXT NOT NULL,
             digest TEXT NOT NULL,
             bytes BLOB NOT NULL,
             byte_count INTEGER NOT NULL,
             recorded_at TEXT NOT NULL,
             integrity_sha256 TEXT,
             integrity_v1 INTEGER NOT NULL DEFAULT 1
                 CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
             PRIMARY KEY (tenant_id, digest)";
    let key = "PRIMARY KEY (tenant_id, digest)";

    let mut admitted = Vec::new();
    for (label, body) in [
        ("exact-current", current.to_owned()),
        (
            "collate-on-a-value-column",
            current.replace(
                "recorded_at TEXT NOT NULL",
                "recorded_at TEXT NOT NULL COLLATE NOCASE",
            ),
        ),
        (
            "column-conflict-clause",
            current.replace(
                "byte_count INTEGER NOT NULL",
                "byte_count INTEGER NOT NULL ON CONFLICT ROLLBACK",
            ),
        ),
        (
            "second-check",
            current.replace(key, &format!("CHECK (byte_count >= 0), {key}")),
        ),
        (
            "extra-unique",
            current.replace(key, &format!("{key}, UNIQUE (digest)")),
        ),
        (
            "generated-column",
            current.replace(
                "recorded_at TEXT NOT NULL",
                "recorded_at TEXT NOT NULL, shadow TEXT GENERATED ALWAYS AS (digest) VIRTUAL",
            ),
        ),
        (
            "table-foreign-key",
            current.replace(
                key,
                &format!(
                    "{key}, FOREIGN KEY (tenant_id) REFERENCES hidden_parent(id) ON DELETE CASCADE"
                ),
            ),
        ),
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("shape.sqlite3");
        rusqlite::Connection::open(&path)
            .expect("raw connection")
            .execute_batch(&format!(
                "CREATE TABLE hidden_parent (id TEXT PRIMARY KEY);
                 CREATE TABLE hidden_blobs ({body});"
            ))
            .unwrap_or_else(|error| panic!("{label} is not valid SQLite: {error}"));
        let opened = SqliteEventStore::open(path.to_str().expect("path"), "hidden").await;
        let refused = matches!(&opened, Err(EventLogError::Invalid(_)));
        eprintln!("{label}: refused={refused} admitted={}", opened.is_ok());
        if label == "exact-current" {
            assert!(opened.is_ok(), "the exact current body stays admitted");
        } else if !refused {
            admitted.push(label);
        }
    }
    assert!(
        admitted.is_empty(),
        "a clause this admission does not recognise must be refused, not served; admitted: {admitted:?}"
    );
}
