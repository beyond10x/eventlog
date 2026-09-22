#![cfg(target_os = "linux")]
use eventlog_conformance::{INSPECTION_LIMITS, prepare_inspection_history, run_history_inspection};
use eventlog_core::{InspectHistory, InspectionError, TenantId};
use eventlog_sqlite::{SqliteEventStore, SqliteHistoryInspector};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

// Independent success fixtures serialize; native concurrency has its own cases.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let value = if fs::symlink_metadata(&path).unwrap().is_symlink() {
                fs::read_link(&path)
                    .unwrap()
                    .to_string_lossy()
                    .as_bytes()
                    .to_vec()
            } else {
                fs::read(&path).unwrap()
            };
            (path, value)
        })
        .collect()
}

// Explicit test-fixture preparation, never an inspector action. The schema and
// data are emitted by the unchanged published writer; only the fixture's journal
// mode is selected before its source bytes are frozen for the observation.
async fn fixture(path: &Path, rollback: bool) -> Vec<eventlog_core::RecordedEvent> {
    let store = SqliteEventStore::open(path.to_str().unwrap(), "inspection")
        .await
        .unwrap();
    let expected = prepare_inspection_history(&store).await;
    drop(store);
    if rollback {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=DELETE")
            .unwrap();
    }
    expected
}

#[tokio::test]
async fn sqlite_inspection_history_preserves_source() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inspection.sqlite3");
    let expected = fixture(&path, true).await;
    let before = bytes(directory.path());
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    run_history_inspection(&inspector, &expected).await;
    assert_eq!(bytes(directory.path()), before);
    drop(inspector);
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn sqlite_inspection_missing_sources_never_create() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.sqlite3");
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    assert_eq!(
        inspector.inspect_history(&tenant, INSPECTION_LIMITS).await,
        Err(InspectionError::MissingSource)
    );
    drop(inspector);
    assert!(bytes(directory.path()).is_empty());
    fixture(&path, true).await;
    let before = bytes(directory.path());
    assert_eq!(
        SqliteHistoryInspector::new(&path, "absent")
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::MissingSource)
    );
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn sqlite_inspection_wal_and_journal_refusals_preserve_source() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inspection.sqlite3");
    fixture(&path, false).await;
    assert_eq!(&fs::read(&path).unwrap()[18..20], &[2, 2]);
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let before = bytes(directory.path());
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    assert_eq!(
        inspector.inspect_history(&tenant, INSPECTION_LIMITS).await,
        Err(InspectionError::RecoveryRequired)
    );
    drop(inspector);
    assert_eq!(bytes(directory.path()), before);
    // Independently exercise each sidecar while the frozen main file is rollback mode.
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("PRAGMA journal_mode=DELETE")
        .unwrap();
    drop(connection);
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        fs::write(&sidecar, b"pending source").unwrap();
        let before = bytes(directory.path());
        let inspector = SqliteHistoryInspector::new(&path, "inspection");
        assert_eq!(
            inspector.inspect_history(&tenant, INSPECTION_LIMITS).await,
            Err(InspectionError::RecoveryRequired)
        );
        drop(inspector);
        assert_eq!(bytes(directory.path()), before);
        fs::remove_file(&sidecar).unwrap();
        std::os::unix::fs::symlink("missing-target", &sidecar).unwrap();
        let before = bytes(directory.path());
        assert_eq!(
            SqliteHistoryInspector::new(&path, "inspection")
                .inspect_history(&tenant, INSPECTION_LIMITS)
                .await,
            Err(InspectionError::RecoveryRequired)
        );
        assert_eq!(bytes(directory.path()), before);
        fs::remove_file(sidecar).unwrap();
    }
}

#[tokio::test]
async fn sqlite_inspection_native_writer_refuses_without_changes() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inspection.sqlite3");
    fixture(&path, true).await;
    let before = bytes(directory.path());
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        SqliteHistoryInspector::new(&path, "inspection")
            .inspect_history(
                &TenantId::new("inspection-tenant").unwrap(),
                INSPECTION_LIMITS
            )
            .await,
        Err(InspectionError::SourceBusy)
    );
    writer.execute_batch("ROLLBACK").unwrap();
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn sqlite_inspection_identity_corruption_schema_and_uri() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inspection ?#% ü.sqlite3");
    let expected = fixture(&path, true).await;
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let identity = " retained-identity ";
    {
        let fixture_writer = rusqlite::Connection::open(&path).unwrap();
        fixture_writer
            .execute(
                "INSERT INTO inspection_identity VALUES (?1,?2)",
                [tenant.as_str(), identity],
            )
            .unwrap();
    }
    let before = bytes(directory.path());
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    let result = inspector
        .inspect_history(&tenant, INSPECTION_LIMITS)
        .await
        .unwrap();
    assert_eq!(result.stream_identity.as_deref(), Some(identity));
    assert_eq!(result.events, expected);
    assert_eq!(bytes(directory.path()), before);
    for (mutation, refusal) in [
        (
            "UPDATE inspection_events SET data='not-json' WHERE global_seq=1",
            InspectionError::CorruptSource,
        ),
        (
            "UPDATE inspection_events SET redacted_at='1970-01-01T00:00:00Z' WHERE global_seq=1",
            InspectionError::RedactedHistory,
        ),
        (
            "ALTER TABLE inspection_events ADD COLUMN unrecognized TEXT",
            InspectionError::UnsupportedSource,
        ),
    ] {
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(mutation)
            .unwrap();
        let before = bytes(directory.path());
        assert_eq!(
            inspector.inspect_history(&tenant, INSPECTION_LIMITS).await,
            Err(refusal)
        );
        assert_eq!(bytes(directory.path()), before);
    }
    drop(inspector);
    let symlink = directory.path().join("alias.sqlite3");
    std::os::unix::fs::symlink(&path, &symlink).unwrap();
    let before = bytes(directory.path());
    assert_eq!(
        SqliteHistoryInspector::new(&symlink, "inspection")
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::UnsupportedSource)
    );
    assert_eq!(bytes(directory.path()), before);
}
