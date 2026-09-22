#![cfg(target_os = "linux")]
use eventlog_conformance::{INSPECTION_LIMITS, prepare_inspection_history};
use eventlog_core::{InspectHistory, InspectionError, InspectionLimits, TenantId};
use eventlog_sqlite::{SqliteEventStore, SqliteHistoryInspector};
use rusqlite::Connection;
use std::{collections::BTreeMap, fs, path::Path, process::Command, time::Duration};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn fixture(path: &Path) {
    let store = SqliteEventStore::open(path.to_str().unwrap(), "inspection")
        .await
        .unwrap();
    prepare_inspection_history(&store).await;
    drop(store);
    Connection::open(path)
        .unwrap()
        .execute_batch("PRAGMA journal_mode=DELETE")
        .unwrap();
}

fn inventory(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read(path).unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn adversary_sqlite_inadmissible_recorded_envelope_is_corruption() {
    let _test = TEST_LOCK.lock().await;
    let mut failures = Vec::new();
    for (field, mutation) in [
        (
            "event id",
            "UPDATE inspection_events SET event_id='' WHERE global_seq=1",
        ),
        (
            "event name",
            "UPDATE inspection_events SET event_name='' WHERE global_seq=1",
        ),
        (
            "actor",
            "UPDATE inspection_events SET actor='' WHERE global_seq=1",
        ),
        (
            "body",
            "UPDATE inspection_events SET data='[]' WHERE global_seq=1",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.sqlite3");
        fixture(&path).await;
        let inspector = SqliteHistoryInspector::new(&path, "inspection");
        let tenant = TenantId::new("inspection-tenant").unwrap();
        assert_eq!(
            inspector
                .inspect_history(&tenant, INSPECTION_LIMITS)
                .await
                .unwrap()
                .events
                .len(),
            3
        );
        Connection::open(&path)
            .unwrap()
            .execute_batch(mutation)
            .unwrap();
        let before = inventory(directory.path());
        let observed = inspector.inspect_history(&tenant, INSPECTION_LIMITS).await;
        drop(inspector);
        assert_eq!(inventory(directory.path()), before);
        if observed != Err(InspectionError::CorruptSource) {
            failures.push(format!("{field}: accepted={}", observed.is_ok()));
        }
    }
    assert!(
        failures.is_empty(),
        "invalid recorded envelopes were not refused: {failures:?}"
    );
}

#[tokio::test]
async fn adversary_sqlite_empty_identity_and_exact_source_cap() {
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.sqlite3");
    fixture(&path).await;
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let size = fs::metadata(&path).unwrap().len();
    let limits = InspectionLimits {
        source_bytes: size,
        ..INSPECTION_LIMITS
    };
    let before = inventory(directory.path());
    assert_eq!(
        inspector
            .inspect_history(&tenant, limits)
            .await
            .unwrap()
            .events
            .len(),
        3
    );
    assert_eq!(
        inspector
            .inspect_history(
                &tenant,
                InspectionLimits {
                    source_bytes: size - 1,
                    ..limits
                }
            )
            .await,
        Err(InspectionError::LimitExceeded)
    );
    let empty = inspector
        .inspect_history(
            &TenantId::new("inspection-absent").unwrap(),
            InspectionLimits {
                events: 0,
                envelope_bytes: 0,
                ..limits
            },
        )
        .await
        .unwrap();
    assert_eq!(empty.stream_identity, None);
    assert!(empty.events.is_empty());
    assert_eq!(inventory(directory.path()), before);
    Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO inspection_identity VALUES (?1, '')",
            [tenant.as_str()],
        )
        .unwrap();
    let before = inventory(directory.path());
    assert_eq!(
        inspector.inspect_history(&tenant, INSPECTION_LIMITS).await,
        Err(InspectionError::CorruptSource)
    );
    drop(inspector);
    assert_eq!(inventory(directory.path()), before);
}

#[tokio::test]
async fn adversary_sqlite_success_preserves_preexisting_reader_lock() {
    const CHILD: &str = "EVENTLOG_ADVERSARY_INSPECTION_READER";
    if let Some(path) = std::env::var_os(CHILD) {
        let writer = Connection::open(path).unwrap();
        writer.busy_timeout(Duration::ZERO).unwrap();
        assert_eq!(
            writer
                .execute_batch("BEGIN EXCLUSIVE")
                .unwrap_err()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );
        return;
    }
    let _test = TEST_LOCK.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.sqlite3");
    fixture(&path).await;
    // Reading/closing another raw descriptor while SQLite holds a POSIX lock
    // would itself release that lock. Inventory outside the held-lock interval.
    let before = inventory(directory.path());
    let reader = Connection::open(&path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT * FROM inspection_events")
        .unwrap();
    let challenge = || {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "adversary_sqlite_success_preserves_preexisting_reader_lock",
                "--nocapture",
            ])
            .env(CHILD, &path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    };
    challenge();
    let inspector = SqliteHistoryInspector::new(&path, "inspection");
    assert_eq!(
        inspector
            .inspect_history(
                &TenantId::new("inspection-tenant").unwrap(),
                INSPECTION_LIMITS
            )
            .await
            .unwrap()
            .events
            .len(),
        3
    );
    drop(inspector);
    challenge();
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(inventory(directory.path()), before);
    Connection::open(&path)
        .unwrap()
        .execute_batch("BEGIN EXCLUSIVE; ROLLBACK")
        .unwrap();
}

#[tokio::test]
async fn adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock() {
    const MODE: &str = "EVENTLOG_ADVERSARY_DESCRIPTOR_MODE";
    const SOURCE: &str = "EVENTLOG_ADVERSARY_DESCRIPTOR_SOURCE";
    let name = "adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock";
    let mode = std::env::var(MODE).unwrap_or_default();
    if mode == "writer" {
        let writer = Connection::open(std::env::var_os(SOURCE).unwrap()).unwrap();
        writer.busy_timeout(Duration::ZERO).unwrap();
        assert_eq!(
            writer
                .execute_batch("BEGIN IMMEDIATE")
                .unwrap_err()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );
        return;
    }
    if mode.is_empty() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(MODE, "registry")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    assert_eq!(mode, "registry");
    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("inspection-tenant").unwrap();
    for index in 0..64 {
        let path = directory.path().join(format!("source-{index}.sqlite3"));
        fixture(&path).await;
        let before = fs::read(&path).unwrap();
        assert_eq!(
            SqliteHistoryInspector::new(&path, "inspection")
                .inspect_history(&tenant, INSPECTION_LIMITS)
                .await
                .unwrap()
                .events
                .len(),
            3
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    let alias = directory.path().join("alias.sqlite3");
    fs::hard_link(directory.path().join("source-0.sqlite3"), &alias).unwrap();
    assert_eq!(
        SqliteHistoryInspector::new(&alias, "inspection")
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await
            .unwrap()
            .events
            .len(),
        3
    );
    let path = directory.path().join("source-65.sqlite3");
    fixture(&path).await;
    let before = inventory(directory.path());
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let challenge = || {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(MODE, "writer")
            .env(SOURCE, &path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    };
    challenge();
    let descriptors = fs::read_dir("/proc/self/fd").unwrap().count();
    assert_eq!(
        SqliteHistoryInspector::new(&path, "inspection")
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::SourceBusy)
    );
    assert_eq!(fs::read_dir("/proc/self/fd").unwrap().count(), descriptors);
    challenge();
    writer.execute_batch("ROLLBACK").unwrap();
    assert_eq!(inventory(directory.path()), before);
}
