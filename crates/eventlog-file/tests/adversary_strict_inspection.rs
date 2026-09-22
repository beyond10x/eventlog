use eventlog_conformance::{INSPECTION_LIMITS, prepare_inspection_history};
use eventlog_core::{EventStore, InspectHistory, InspectionError, InspectionLimits, TenantId};
use eventlog_file::{FileEventStore, FileHistoryInspector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, fs, path::Path};

#[derive(Serialize, Deserialize)]
struct Frame {
    format: String,
    store: String,
    epoch: u64,
    sequence: u64,
    previous: String,
    transaction: Value,
    digest: String,
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

// Mutate only a synthetic fixture's semantic contents, rebuilding its valid physical
// frame checksums so the refusal must come from the claimed history contract.
fn mutate(root: &Path, mut change: impl FnMut(&mut Value)) {
    let original = fs::read(root.join("events.jsonl")).unwrap();
    let mut result = Vec::new();
    let mut previous = "0".repeat(64);
    for line in original.split_inclusive(|byte| *byte == b'\n') {
        let mut frame: Frame = serde_json::from_slice(line).unwrap();
        for operation in frame.transaction.as_array_mut().unwrap() {
            change(operation);
        }
        frame.previous.clone_from(&previous);
        frame.digest.clear();
        frame.digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&frame).unwrap()));
        previous.clone_from(&frame.digest);
        result.extend(serde_json::to_vec(&frame).unwrap());
        result.push(b'\n');
    }
    let manifest_path = root.join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["length"] = Value::from(result.len());
    manifest["digest"] = Value::from(previous);
    fs::write(root.join("events.jsonl"), result).unwrap();
    fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}

#[tokio::test]
async fn adversary_file_duplicate_event_identity_is_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let expected = prepare_inspection_history(&store).await;
    drop(store);
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let inspector = FileHistoryInspector::new(directory.path());
    assert_eq!(
        inspector
            .inspect_history(&tenant, INSPECTION_LIMITS)
            .await
            .unwrap()
            .events,
        expected
    );
    let mut first: Option<String> = None;
    let mut changed = 0;
    mutate(directory.path(), |operation| {
        if operation["operation"] == "event" && operation["event"]["tenant"] == tenant.as_str() {
            if let Some(id) = &first {
                operation["event"]["event_id"] = Value::String(id.clone());
                changed += 1;
            } else {
                first = Some(operation["event"]["event_id"].as_str().unwrap().to_owned());
            }
        }
    });
    assert_eq!(changed, 2);
    let before = inventory(directory.path());
    let observed = inspector.inspect_history(&tenant, INSPECTION_LIMITS).await;
    assert_eq!(inventory(directory.path()), before);
    assert_eq!(observed, Err(InspectionError::CorruptSource));
}

#[tokio::test]
async fn adversary_file_unknown_recorded_envelope_field_is_not_discarded() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    prepare_inspection_history(&store).await;
    drop(store);
    let mut changed = 0;
    mutate(directory.path(), |operation| {
        if operation["operation"] == "event" {
            operation["event"]["unsupported_authority_field"] = Value::Bool(true);
            changed += 1;
        }
    });
    assert_eq!(changed, 4);
    let before = inventory(directory.path());
    let observed = FileHistoryInspector::new(directory.path())
        .inspect_history(
            &TenantId::new("inspection-tenant").unwrap(),
            INSPECTION_LIMITS,
        )
        .await;
    assert_eq!(inventory(directory.path()), before);
    assert_eq!(observed, Err(InspectionError::CorruptSource));
}

#[tokio::test]
async fn adversary_file_empty_identity_refuses_and_zero_result_caps_allow_absence() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let tenant = TenantId::new("inspection-tenant").unwrap();
    store.stream_identity(&tenant).await.unwrap();
    drop(store);
    let inspector = FileHistoryInspector::new(directory.path());
    let zero = InspectionLimits {
        events: 0,
        envelope_bytes: 0,
        ..INSPECTION_LIMITS
    };
    let absent = inspector
        .inspect_history(&TenantId::new("inspection-absent").unwrap(), zero)
        .await
        .unwrap();
    assert!(absent.events.is_empty());
    assert_eq!(absent.stream_identity, None);
    let mut changed = 0;
    mutate(directory.path(), |operation| {
        if operation["operation"] == "identity" {
            operation["id"] = Value::String(String::new());
            changed += 1;
        }
    });
    assert_eq!(changed, 1);
    let before = inventory(directory.path());
    assert_eq!(
        inspector.inspect_history(&tenant, zero).await,
        Err(InspectionError::CorruptSource)
    );
    drop(inspector);
    assert_eq!(inventory(directory.path()), before);
}

#[tokio::test]
async fn adversary_file_inadmissible_recorded_envelope_is_corruption() {
    let mut failures = Vec::new();
    for (field, invalid) in [
        ("event_id", Value::String(String::new())),
        ("name", Value::String(String::new())),
        ("actor", Value::String(String::new())),
        ("data", serde_json::json!([])),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(directory.path()).await.unwrap();
        prepare_inspection_history(&store).await;
        drop(store);
        let mut changed = 0;
        mutate(directory.path(), |operation| {
            if operation["operation"] == "event" && operation["event"]["global_seq"] == 1 {
                operation["event"][field] = invalid.clone();
                changed += 1;
            }
        });
        assert_eq!(changed, 1);
        let before = inventory(directory.path());
        let observed = FileHistoryInspector::new(directory.path())
            .inspect_history(
                &TenantId::new("inspection-tenant").unwrap(),
                INSPECTION_LIMITS,
            )
            .await;
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
