//! Which layout a store's event files are written in.
//!
//! A store created without asking is written as every release before this one wrote it, and a
//! store such a release wrote reads unchanged and keeps its layout. Only an owner who creates a
//! store with [`TreeLayout::Hashed`] gets `eventlog-tree/3`, whose event paths have a length no
//! stream type or id can grow (`docs/design/tree-layout-path-length-v0.1.md`).

use eventlog_conformance::{event, meta};
use eventlog_core::{EventLogError, EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{MigrationMode, TreeEventStore, TreeLayout, migrate, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn tenant() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}

/// A store Eventlog 0.8.4 wrote: stream `item/x` with three events, the second redacted, and
/// stream `item/y/z` with one, committed with the raw blob `blob-1`.
fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tree-0.8.4")
}

/// Every history file under `root`, relative to it, with its bytes. The writer lock and the
/// kept state are not history.
fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            if relative == Path::new(".lock") || relative == Path::new(".cache") {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else {
                found.push((relative, fs::read(&path).unwrap()));
            }
        }
    }
    found.sort();
    found
}

fn copy_of_fixture() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    for (relative, bytes) in files(&fixture()) {
        let target = directory.path().join(relative);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, bytes).unwrap();
    }
    directory
}

fn format(root: &Path) -> String {
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("store.json")).unwrap()).unwrap();
    value["format"].as_str().unwrap().to_owned()
}

fn event_files(root: &Path) -> Vec<PathBuf> {
    files(root)
        .into_iter()
        .map(|(path, _)| path)
        .filter(|path| path.components().any(|part| part.as_os_str() == "streams"))
        .collect()
}

async fn assert_serves_the_fixture(store: &TreeEventStore) {
    let x = store.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert_eq!(
        x.iter().map(|event| event.version).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        x.iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec!["item.received", "item.indexed", "item.closed"]
    );
    assert!(x[1].is_redacted(), "the redaction 0.8.4 wrote was lost");
    assert_eq!(x[2].parents, vec![x[1].digest.clone().unwrap()]);
    assert_eq!(store.stream_version(&stream("y/z")).await.unwrap(), Some(1));
    assert_eq!(
        store.get_blob(&tenant(), "blob-1").await.unwrap(),
        Some(b"bytes".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_written_by_0_8_4_reads_unchanged_and_keeps_writing_its_layout() {
    let directory = copy_of_fixture();
    let root = directory.path();
    let before = files(root);

    let store = TreeEventStore::open(root).await.unwrap();
    assert_eq!(store.layout().unwrap(), TreeLayout::Nested);
    assert_serves_the_fixture(&store).await;
    drop(store);
    assert_eq!(files(root), before, "opening rewrote a file");
    assert_eq!(verify(root, None), Vec::new());

    // Asking for the hashed layout does not move a store that already has one.
    let store = TreeEventStore::open_in(root, TreeLayout::Hashed)
        .await
        .unwrap();
    assert_eq!(store.layout().unwrap(), TreeLayout::Nested);
    assert_serves_the_fixture(&store).await;
    let appended = store
        .append(
            &stream("x"),
            Expected::Exact(3),
            &[event("item.reopened", 5)],
            &meta("three", &json!({})),
        )
        .await
        .unwrap();
    drop(store);
    assert_eq!(format(root), "eventlog-tree/2");
    let digest = appended.events[0].digest.clone().unwrap();
    assert!(
        root.join(format!("tenants/tenant-a/streams/item/x/{digest}.json"))
            .is_file(),
        "a 0.8.4 store wrote an event outside its nested stream directory"
    );
    assert_eq!(verify(root, Some(&fixture())), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_store_is_nested_unless_its_owner_opts_in_to_the_hashed_layout() {
    for (layout, tag) in [
        (TreeLayout::Nested, "eventlog-tree/2"),
        (TreeLayout::Hashed, "eventlog-tree/3"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let created = if layout == TreeLayout::Nested {
            TreeEventStore::open(root).await.unwrap()
        } else {
            TreeEventStore::open_in(root, layout).await.unwrap()
        };
        let written = created
            .append(
                &stream("y/z"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("one", &json!({})),
            )
            .await
            .unwrap();
        drop(created);
        assert_eq!(format(root), tag);
        let digest = written.events[0].digest.clone().unwrap();
        let file = format!("{digest}.json");
        let events = event_files(root);
        assert_eq!(events.len(), 1);
        let parts: Vec<String> = events[0]
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        if layout == TreeLayout::Nested {
            assert_eq!(
                parts,
                [
                    "tenants",
                    "tenant-a",
                    "streams",
                    "item",
                    "y%2Fz",
                    file.as_str()
                ]
            );
        } else {
            let [_, _, _, fan_out, name, last] = parts.as_slice() else {
                panic!("a hashed event path of another depth: {parts:?}");
            };
            assert_eq!(
                (name.len(), fan_out.as_str(), last.as_str()),
                (32, &name[..2], file.as_str())
            );
            assert!(name.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }

        // The layout is the store's, not the caller's: a plain open keeps it.
        let reopened = TreeEventStore::open(root).await.unwrap();
        assert_eq!(reopened.layout().unwrap(), layout);
        assert_eq!(
            reopened.stream_version(&stream("y/z")).await.unwrap(),
            Some(1)
        );
        drop(reopened);
        assert_eq!(verify(root, None), Vec::new());
        let report = migrate(root, MigrationMode::DryRun).unwrap();
        assert!(!report.pending(), "{report:?}");
        assert_eq!(report.to_format, tag);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hashed_store_refuses_an_event_file_at_its_nested_path_by_name() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = TreeEventStore::open_in(root, TreeLayout::Hashed)
        .await
        .unwrap();
    store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
    drop(store);
    let hashed = event_files(root).remove(0);
    let nested = Path::new("tenants/tenant-a/streams/item/x").join(hashed.file_name().unwrap());
    fs::create_dir_all(root.join(nested.parent().unwrap())).unwrap();
    fs::rename(root.join(&hashed), root.join(&nested)).unwrap();
    let _ = fs::remove_dir_all(root.join(".cache"));

    let Err(error) = TreeEventStore::open(root).await else {
        panic!("a hashed store served an event at a nested path");
    };
    assert!(
        matches!(&error, EventLogError::Backend(message)
            if message.contains("an event file outside its stream's directory")),
        "{error}"
    );
    assert!(
        verify(root, None)
            .iter()
            .any(|finding| finding.rule == "V4")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_directory_of_nested_files_without_store_json_is_never_declared_hashed() {
    let directory = copy_of_fixture();
    let root = directory.path();
    fs::remove_file(root.join("store.json")).unwrap();
    let store = TreeEventStore::open_in(root, TreeLayout::Hashed)
        .await
        .unwrap();
    assert_eq!(store.layout().unwrap(), TreeLayout::Nested);
    assert_serves_the_fixture(&store).await;
    drop(store);
    assert_eq!(format(root), "eventlog-tree/2");
}
