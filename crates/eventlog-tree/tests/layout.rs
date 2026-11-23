//! Which layout a store's event files are written in.
//!
//! A store created without asking is written as every release before this one wrote it, and a
//! store such a release wrote reads unchanged and keeps its layout. Only an owner who creates a
//! store with [`TreeLayout::Hashed`] gets `eventlog-tree/3`, whose event paths have a length no
//! stream type or id can grow (`docs/design/tree-layout-path-length-v0.1.md`).

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, NoGuard, StreamAppend,
    StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use eventlog_tree::{MigrationMode, TreeEventStore, TreeLayout, copy, migrate, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// A tenant id of `MAX_FIELD_LEN` (512) bytes, every other byte one a file name escapes: its
/// escaped segment, 1,024 bytes, is past every file system's 255-byte name limit.
fn longest_tenant() -> TenantId {
    let mut name = String::from("t");
    while name.len() < eventlog_core::MAX_FIELD_LEN {
        name.push(if name.len() % 2 == 0 { 't' } else { '/' });
    }
    TenantId::new(name).unwrap()
}

/// A tenant directory the store names for `tenant`: one segment under `tenants/`, and never
/// longer than 255 bytes.
fn tenant_directories(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(root.join("tenants"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Story `tree-accepts-a-512-byte-tenant`: a Tree store accepts every tenant id the core accepts.
/// A tenant of the longest accepted length writes every kind of file, reopens cold, verifies,
/// migrates, copies and is erased, in both layouts; a tenant whose escaped segment fits a file
/// name keeps the directory name every release wrote for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_of_the_longest_accepted_length_is_stored_and_reopens() {
    for layout in [TreeLayout::Nested, TreeLayout::Hashed] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let long = longest_tenant();
        // 85 bytes that escape to 255, the longest name the file system takes.
        let fits = TenantId::new("/".repeat(85)).unwrap();
        let on = |tenant: &TenantId, id: &str| StreamId::new(tenant.clone(), "item", id).unwrap();
        let store = TreeEventStore::open_in(root, layout).await.unwrap();
        let identity = store
            .stream_identity(&long)
            .await
            .expect("a 512-byte tenant is named");
        let appended = store
            .append(
                &on(&long, "x"),
                Expected::NoStream,
                &[event("item.received", 1), event("item.indexed", 2)],
                &meta("one", &json!({})),
            )
            .await
            .expect("a 512-byte tenant's event is written");
        let group = AppendGroup {
            tenant: long.clone(),
            appends: vec![StreamAppend {
                stream: on(&long, "y"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 3)],
            }],
            meta: meta("group", &json!({})),
        };
        store
            .append_group_guarded_with_blobs(
                &group,
                Arc::new(NoGuard),
                &[("blob-1".to_owned(), b"bytes".to_vec())],
            )
            .await
            .expect("a 512-byte tenant's group and blob are written");
        store
            .append(
                &on(&fits, "x"),
                Expected::NoStream,
                &[event("item.received", 4)],
                &meta("fits", &json!({})),
            )
            .await
            .unwrap();
        drop(store);
        let names = tenant_directories(root);
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(
            names.iter().all(|name| name.len() <= 255),
            "a tenant directory name past the file-name limit: {names:?}"
        );
        assert!(
            names.contains(&"%2F".repeat(85)),
            "a tenant whose segment fits lost the directory name 0.10.0 wrote: {names:?}"
        );

        let _ = fs::remove_dir_all(root.join(".cache"));
        let reopened = TreeEventStore::open_in(root, layout).await.unwrap();
        assert!(reopened.replayed().await, "control: the reopen replayed");
        let read = reopened
            .read_stream(&on(&long, "x"), 0, 10)
            .await
            .unwrap()
            .events;
        let ids =
            |events: &[eventlog_core::RecordedEvent]| -> Vec<(String, u64, serde_json::Value)> {
                events
                    .iter()
                    .map(|event| (event.event_id.clone(), event.version, event.data.clone()))
                    .collect()
            };
        assert_eq!(
            ids(&read),
            ids(&appended.events),
            "the 512-byte tenant's events reread"
        );
        assert_eq!(read[0].tenant, long);
        assert_eq!(
            reopened.stream_version(&on(&long, "y")).await.unwrap(),
            Some(1)
        );
        assert_eq!(
            reopened.get_blob(&long, "blob-1").await.unwrap(),
            Some(b"bytes".to_vec())
        );
        assert_eq!(reopened.stream_identity(&long).await.unwrap(), identity);
        assert_eq!(
            reopened.stream_version(&on(&fits, "x")).await.unwrap(),
            Some(1)
        );
        assert_eq!(verify(root, None), Vec::new());
        let report = migrate(root, MigrationMode::DryRun).unwrap();
        assert!(!report.pending(), "{report:?}");

        let target = SqliteEventStore::in_memory("copy").await.unwrap();
        let copied = copy(&reopened, &target).await.unwrap();
        assert_eq!((copied.tenants, copied.events), (2, 4));
        assert_eq!(
            target
                .read_stream(&on(&long, "x"), 0, 10)
                .await
                .unwrap()
                .events
                .len(),
            2
        );

        reopened.forget_tenant(&long).await.unwrap();
        assert_eq!(tenant_directories(root), vec!["%2F".repeat(85)]);
        drop(reopened);
        let erased = TreeEventStore::open_in(root, layout).await.unwrap();
        assert_eq!(erased.stream_version(&on(&long, "x")).await.unwrap(), None);
    }
}
