use std::{collections::BTreeMap, fs, path::Path};

use eventlog_core::{EventStore, TenantId};
use eventlog_file::FileEventStore;

fn files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root)
        .expect("existing root")
        .map(|entry| {
            let entry = entry.expect("directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            (name, fs::read(entry.path()).expect("file bytes"))
        })
        .collect()
}

#[tokio::test]
async fn existing_open_refuses_missing_and_partial_authority_without_creation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let absent = directory.path().join("absent");
    assert!(FileEventStore::open_existing(&absent).await.is_err());
    assert!(!absent.exists(), "missing root must remain missing");

    let bare = directory.path().join("bare");
    fs::create_dir(&bare).expect("bare directory");
    assert!(FileEventStore::open_existing(&bare).await.is_err());
    assert!(files(&bare).is_empty(), "bare directory gained authority");

    let partial = directory.path().join("partial");
    drop(FileEventStore::open(&partial).await.expect("provisioned"));
    fs::remove_file(partial.join("manifest.json")).expect("remove commit authority");
    let before = files(&partial);
    assert!(FileEventStore::open_existing(&partial).await.is_err());
    assert_eq!(files(&partial), before, "missing manifest was replaced");
}

#[tokio::test]
async fn existing_open_reopens_but_a_removed_root_is_never_reprovisioned() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("eventlog");
    drop(
        FileEventStore::open(&root)
            .await
            .expect("explicit creation"),
    );
    let existing = FileEventStore::open_existing(&root)
        .await
        .expect("existing authority reopens");
    let tenant = TenantId::new("existing-open").expect("tenant");
    existing
        .stream_identity(&tenant)
        .await
        .expect("existing store serves reads");

    fs::remove_dir_all(&root).expect("remove selected authority");
    assert!(existing.stream_identity(&tenant).await.is_err());
    assert!(
        !root.exists(),
        "an existing handle recreated its removed root"
    );
}
