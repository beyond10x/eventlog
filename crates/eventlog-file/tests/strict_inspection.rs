use eventlog_conformance::{INSPECTION_LIMITS, prepare_inspection_history, run_history_inspection};
use eventlog_core::{
    EventStore, Expected, InspectHistory, InspectionError, NewEvent, StreamId, TenantId,
};
use eventlog_file::{FileEventStore, FileHistoryInspector};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

fn bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).unwrap();
        if metadata.is_dir() {
            result.insert(path.clone(), Vec::new());
            result.extend(bytes(&path));
        } else if metadata.is_symlink() {
            result.insert(
                path.clone(),
                fs::read_link(path)
                    .unwrap()
                    .to_string_lossy()
                    .as_bytes()
                    .to_vec(),
            );
        } else {
            result.insert(path.clone(), fs::read(path).unwrap());
        }
    }
    result
}

#[tokio::test]
async fn file_inspection_body_fields_and_zero_schema_are_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let stream = StreamId::new(tenant.clone(), "inspection", "body-fields").unwrap();
    let body = serde_json::json!({"unknown_envelope_field": {"actor": "", "data": []}});
    let event = NewEvent::new("Inspected", 0, body.clone()).unwrap();
    let expected = store
        .append(
            &stream,
            Expected::NoStream,
            &[event],
            &eventlog_conformance::meta("inspection-body-fields", &body),
        )
        .await
        .unwrap();
    drop(store);
    let before = bytes(directory.path());
    let observed = FileHistoryInspector::new(directory.path())
        .inspect_history(&tenant, INSPECTION_LIMITS)
        .await
        .unwrap();
    assert_eq!(observed.events, expected.events);
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn file_inspection_history_preserves_source() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let expected = prepare_inspection_history(&store).await;
    store
        .put_blob(
            &TenantId::new("inspection-tenant").unwrap(),
            "retained",
            b"retained",
        )
        .await
        .unwrap();
    drop(store);
    fs::write(directory.path().join("privacy.next"), b"unselected staging").unwrap();
    let before = bytes(directory.path());
    let inspector = FileHistoryInspector::new(directory.path());
    run_history_inspection(&inspector, &expected).await;
    assert_eq!(bytes(directory.path()), before);
    drop(inspector);
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn file_inspection_missing_sources_never_create() {
    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let missing = directory.path().join("absent");
    assert_eq!(
        FileHistoryInspector::new(&missing)
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::MissingSource)
    );
    assert!(!missing.exists());
    for name in ["writer.lock", "manifest.json", "events.jsonl"] {
        let fixture = tempfile::tempdir().unwrap();
        drop(FileEventStore::open(fixture.path()).await.unwrap());
        fs::remove_file(fixture.path().join(name)).unwrap();
        let before = bytes(fixture.path());
        assert_eq!(
            FileHistoryInspector::new(fixture.path())
                .inspect_history(&tenant, INSPECTION_LIMITS)
                .await,
            Err(InspectionError::MissingSource)
        );
        assert_eq!(bytes(fixture.path()), before);
    }
}

#[tokio::test]
async fn file_inspection_recovery_and_corruption_preserve_source() {
    let tenant = TenantId::new("inspection-tenant").unwrap();
    for name in ["append.json", "privacy.json"] {
        let directory = tempfile::tempdir().unwrap();
        drop(FileEventStore::open(directory.path()).await.unwrap());
        fs::write(directory.path().join(name), b"pending").unwrap();
        let before = bytes(directory.path());
        assert_eq!(
            FileHistoryInspector::new(directory.path())
                .inspect_history(&tenant, INSPECTION_LIMITS)
                .await,
            Err(InspectionError::RecoveryRequired)
        );
        assert_eq!(bytes(directory.path()), before);
        #[cfg(unix)]
        {
            fs::remove_file(directory.path().join(name)).unwrap();
            std::os::unix::fs::symlink("absent-target", directory.path().join(name)).unwrap();
            let before = bytes(directory.path());
            assert_eq!(
                FileHistoryInspector::new(directory.path())
                    .inspect_history(&tenant, INSPECTION_LIMITS)
                    .await,
                Err(InspectionError::RecoveryRequired)
            );
            assert_eq!(bytes(directory.path()), before);
        }
    }
    let directory = tempfile::tempdir().unwrap();
    drop(FileEventStore::open(directory.path()).await.unwrap());
    fs::write(directory.path().join("events.jsonl"), b"uncommitted suffix").unwrap();
    let before = bytes(directory.path());
    assert_eq!(
        FileHistoryInspector::new(directory.path())
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::CorruptSource)
    );
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn file_inspection_native_lock_is_nonblocking() {
    let directory = tempfile::tempdir().unwrap();
    drop(FileEventStore::open(directory.path()).await.unwrap());
    let lock = fs::File::open(directory.path().join("writer.lock")).unwrap();
    lock.lock().unwrap();
    let before = bytes(directory.path());
    assert_eq!(
        FileHistoryInspector::new(directory.path())
            .inspect_history(
                &TenantId::new("inspection-tenant").unwrap(),
                INSPECTION_LIMITS
            )
            .await,
        Err(InspectionError::SourceBusy)
    );
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test]
async fn file_inspection_identity_redaction_and_unknown_format() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let events = prepare_inspection_history(&store).await;
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let identity = store.stream_identity(&tenant).await.unwrap();
    let before = bytes(directory.path());
    let inspection = FileHistoryInspector::new(directory.path())
        .inspect_history(&tenant, INSPECTION_LIMITS)
        .await
        .unwrap();
    assert_eq!(
        inspection.stream_identity.as_deref(),
        Some(identity.as_str())
    );
    assert_eq!(bytes(directory.path()), before);
    store
        .redact(&events[0].stream().unwrap(), 1, "fixture-redaction")
        .await
        .unwrap();
    drop(store);
    let before = bytes(directory.path());
    assert_eq!(
        FileHistoryInspector::new(directory.path())
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::RedactedHistory)
    );
    assert_eq!(bytes(directory.path()), before);
    let path = directory.path().join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest["format"] = "eventlog-file/unsupported".into();
    fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let before = bytes(directory.path());
    assert_eq!(
        FileHistoryInspector::new(directory.path())
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await,
        Err(InspectionError::UnsupportedSource)
    );
    assert_eq!(bytes(directory.path()), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_inspection_concurrent_append_is_one_observation() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let inspector = FileHistoryInspector::new(directory.path());
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let stream = eventlog_core::StreamId::new(tenant.clone(), "inspection", "atomic").unwrap();
    let writer = async {
        for index in 0..8 {
            let event = eventlog_conformance::event("Inspected", index);
            let meta = eventlog_conformance::meta(
                &format!("concurrent-{index}"),
                &serde_json::json!({"index":index}),
            );
            store
                .append(
                    &stream,
                    eventlog_core::Expected::Any,
                    &[event.clone(), event],
                    &meta,
                )
                .await
                .unwrap();
        }
    };
    let reader = async {
        for _ in 0..16 {
            match inspector.inspect_history(&tenant, INSPECTION_LIMITS).await {
                Ok(observation) => {
                    assert_eq!(
                        observation.events.len() % 2,
                        0,
                        "a committed pair cannot be split"
                    );
                    for (index, event) in observation.events.iter().enumerate() {
                        assert_eq!(event.version, u64::try_from(index).unwrap() + 1);
                    }
                }
                Err(InspectionError::SourceBusy) => {}
                other => panic!("unexpected observation: {other:?}"),
            }
        }
    };
    tokio::join!(writer, reader);
    assert_eq!(
        inspector
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await
            .unwrap()
            .events
            .len(),
        16
    );
}
