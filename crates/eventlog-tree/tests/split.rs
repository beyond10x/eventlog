//! `eventlog-tree/2`: a blob's long text is stored once by its SHA-256, every existing
//! `eventlog-tree/1` store reads as it did, and the migration between them changes nothing a
//! reader is served (`docs/design/tree-text-blobs-v0.1.md`).

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{MigrationMode, TreeEventStore, migrate, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn tenant() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

/// A JSON blob holding `body` twice, as a record and its revision would, and a short field.
fn document(id: &str, body: &str) -> Vec<u8> {
    serde_json::to_vec(&json!(["a.record/1", {
        "body": body,
        "document": { "body": body, "id": id },
        "id": id,
    }]))
    .unwrap()
}

fn body(seed: &str) -> String {
    format!(
        "# {seed}\n\n{}",
        "A sentence of the story's outcome.\n".repeat(100)
    )
}

/// A tree whose `store.json` says `eventlog-tree/1`, as every store written before 0.6.0 does.
fn v1_store(root: &Path) {
    fs::create_dir_all(root).unwrap();
    fs::write(
        root.join("store.json"),
        br#"{"format":"eventlog-tree/1","identity":"store-1"}"#,
    )
    .unwrap();
}

async fn write(root: &Path, blobs: &[(&str, Vec<u8>)]) {
    let store = TreeEventStore::open(root).await.unwrap();
    for (digest, bytes) in blobs {
        store.put_blob(&tenant(), digest, bytes).await.unwrap();
    }
    store
        .append(
            &StreamId::new(tenant(), "item", "x").unwrap(),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
}

async fn served(root: &Path, digest: &str) -> Option<Vec<u8>> {
    let _ = fs::remove_dir_all(root.join(".cache"));
    let store = TreeEventStore::open(root).await.unwrap();
    store.get_blob(&tenant(), digest).await.unwrap()
}

fn files_under(root: &Path, kind: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("tenants")];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.components().any(|part| part.as_os_str() == kind) {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut all = Vec::new();
    for kind in ["blobs", "split-blobs", "texts", "streams", "groups"] {
        for path in files_under(root, kind) {
            let bytes = fs::read(&path).unwrap();
            all.push((path, bytes));
        }
    }
    all.push((
        root.join("store.json"),
        fs::read(root.join("store.json")).unwrap(),
    ));
    all
}

fn copy(from: &Path, into: &Path) {
    fs::create_dir_all(into).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = into.join(path.file_name().unwrap());
        if path.is_dir() {
            copy(&path, &target);
        } else {
            fs::copy(&path, &target).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_store_keeps_a_body_two_blobs_share_as_one_text_and_serves_the_exact_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let shared = body("shared");
    let one = document("one", &shared);
    let two = document("two", &shared);
    write(
        root,
        &[("sha256:one", one.clone()), ("sha256:two", two.clone())],
    )
    .await;

    let store_json: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("store.json")).unwrap()).unwrap();
    assert_eq!(store_json["format"], "eventlog-tree/2");
    assert!(
        files_under(root, "blobs").is_empty(),
        "a splittable blob was kept raw"
    );
    assert_eq!(files_under(root, "split-blobs").len(), 2);
    let texts = files_under(root, "texts");
    assert_eq!(
        texts.len(),
        1,
        "the body both blobs hold was stored more than once"
    );
    assert_eq!(
        fs::read(&texts[0]).unwrap(),
        serde_json::to_string(&shared)
            .unwrap()
            .trim_matches('"')
            .as_bytes()
    );
    assert_eq!(served(root, "sha256:one").await, Some(one));
    assert_eq!(served(root, "sha256:two").await, Some(two));
    assert_eq!(verify(root, None), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_v1_store_reads_and_writes_raw_blobs_as_before() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    v1_store(root);
    let bytes = document("one", &body("one"));
    write(root, &[("sha256:one", bytes.clone())]).await;
    assert_eq!(
        fs::read(root.join("store.json")).unwrap(),
        br#"{"format":"eventlog-tree/1","identity":"store-1"}"#
    );
    assert_eq!(files_under(root, "blobs").len(), 1);
    assert_eq!(fs::read(&files_under(root, "blobs")[0]).unwrap(), bytes);
    assert!(files_under(root, "split-blobs").is_empty() && files_under(root, "texts").is_empty());
    assert_eq!(served(root, "sha256:one").await, Some(bytes));
    assert_eq!(verify(root, None), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_writes_nothing_and_an_apply_serves_every_blob_byte_for_byte() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let base = directory.path().join("base");
    v1_store(&root);
    let shared = body("shared");
    let one = document("one", &shared);
    let two = document("two", &shared);
    let small = br#"{"small":true}"#.to_vec();
    write(
        &root,
        &[
            ("sha256:one", one.clone()),
            ("sha256:two", two.clone()),
            ("sha256:small", small.clone()),
        ],
    )
    .await;
    copy(&root, &base);
    let before = snapshot(&root);

    let planned = migrate(&root, MigrationMode::DryRun).unwrap();
    assert_eq!(snapshot(&root), before, "a dry run wrote");
    assert!(planned.pending());
    assert_eq!(
        (planned.blobs, planned.blobs_split, planned.texts_added),
        (3, 2, 1)
    );
    assert_eq!(planned.verified_blobs, None);

    let applied = migrate(&root, MigrationMode::Apply).unwrap();
    assert_eq!(applied.verified_blobs, Some(3));
    assert_eq!(
        (applied.bytes_after, applied.largest_after.clone()),
        (planned.bytes_after, planned.largest_after.clone()),
        "the dry run did not predict what the apply wrote"
    );
    assert!(applied.bytes_after < applied.bytes_before);
    assert_eq!(
        files_under(&root, "blobs").len(),
        1,
        "only the small blob stays raw"
    );
    let after = snapshot(&root);
    for (path, bytes) in &before {
        let history = path
            .components()
            .any(|part| part.as_os_str() == "streams" || part.as_os_str() == "groups");
        if history {
            assert!(
                after.contains(&(path.clone(), bytes.clone())),
                "an event or group file changed: {}",
                path.display()
            );
        }
    }
    assert_eq!(served(&root, "sha256:one").await, Some(one));
    assert_eq!(served(&root, "sha256:two").await, Some(two));
    assert_eq!(served(&root, "sha256:small").await, Some(small));
    assert_eq!(verify(&root, None), Vec::new());
    assert_eq!(
        verify(&root, Some(&base)),
        Vec::new(),
        "the migration read as a history rewrite"
    );

    let again = migrate(&root, MigrationMode::DryRun).unwrap();
    assert!(
        !again.pending(),
        "a migrated store still had work: {again:?}"
    );
    let settled = snapshot(&root);
    migrate(&root, MigrationMode::Apply).unwrap();
    assert_eq!(snapshot(&root), settled, "a second apply changed the store");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_raw_blob_merged_into_a_v2_store_reads_and_is_split_by_the_next_migration() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    write(root, &[]).await;
    let bytes = document("late", &body("late"));
    // What a branch that had not migrated adds: a raw file at the first layout's path.
    let shard = root.join("tenants/tenant-a/blobs/sh");
    fs::create_dir_all(&shard).unwrap();
    fs::write(shard.join("sha256%3Alate"), &bytes).unwrap();
    assert_eq!(served(root, "sha256:late").await, Some(bytes.clone()));
    assert_eq!(migrate(root, MigrationMode::Apply).unwrap().blobs_split, 1);
    assert_eq!(files_under(root, "blobs"), [] as [std::path::PathBuf; 0]);
    assert_eq!(served(root, "sha256:late").await, Some(bytes));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_whose_two_forms_disagree_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    write(root, &[("sha256:one", document("one", &body("one")))]).await;
    let shard = root.join("tenants/tenant-a/blobs/sh");
    fs::create_dir_all(&shard).unwrap();
    fs::write(shard.join("sha256%3Aone"), document("one", &body("other"))).unwrap();
    let refused = TreeEventStore::open(root)
        .await
        .err()
        .expect("the store opened");
    assert!(
        refused.to_string().contains("two forms disagree"),
        "{refused}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_text_is_refused_on_open_and_by_verify() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    write(root, &[("sha256:one", document("one", &body("one")))]).await;
    let text = &files_under(root, "texts")[0];
    let mut bytes = fs::read(text).unwrap();
    bytes[0] ^= 1;
    fs::write(text, bytes).unwrap();
    let _ = fs::remove_dir_all(root.join(".cache"));
    let refused = TreeEventStore::open(root)
        .await
        .err()
        .expect("the store opened");
    assert!(
        refused
            .to_string()
            .contains("does not expand to its digest"),
        "{refused}"
    );
    let rules: Vec<_> = verify(root, None)
        .into_iter()
        .map(|finding| finding.rule)
        .collect();
    assert_eq!(rules, vec!["V4"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_v1_store_holding_split_files_and_an_unknown_format_are_refused_by_name() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    write(root, &[("sha256:one", document("one", &body("one")))]).await;
    fs::write(
        root.join("store.json"),
        br#"{"format":"eventlog-tree/1","identity":"x"}"#,
    )
    .unwrap();
    let refused = TreeEventStore::open(root)
        .await
        .err()
        .expect("the store opened");
    assert!(
        refused
            .to_string()
            .contains("eventlog-tree/1 store holding split blobs"),
        "{refused}"
    );
    let rules: Vec<_> = verify(root, None)
        .into_iter()
        .map(|finding| finding.rule)
        .collect();
    assert!(rules.contains(&"V1") && rules.contains(&"V4"), "{rules:?}");

    fs::write(
        root.join("store.json"),
        br#"{"format":"eventlog-tree/9","identity":"x"}"#,
    )
    .unwrap();
    let refused = TreeEventStore::open(root)
        .await
        .err()
        .expect("the store opened");
    assert!(
        refused.to_string().contains("not an eventlog-tree store"),
        "{refused}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_blob_removes_the_texts_only_it_named() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let shared = body("shared");
    let own = body("own");
    let one = serde_json::to_vec(&json!({ "a": shared, "b": own })).unwrap();
    let two = document("two", &shared);
    write(root, &[("sha256:one", one), ("sha256:two", two.clone())]).await;
    assert_eq!(files_under(root, "texts").len(), 2);
    let store = TreeEventStore::open(root).await.unwrap();
    store.delete_blob(&tenant(), "sha256:one").await.unwrap();
    drop(store);
    let texts = files_under(root, "texts");
    assert_eq!(texts.len(), 1, "the deleted blob's own text survived it");
    assert!(!fs::read(&texts[0]).unwrap().starts_with(b"# own"));
    assert_eq!(served(root, "sha256:one").await, None);
    assert_eq!(served(root, "sha256:two").await, Some(two));
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_a_blob_the_head_serves_with_other_bytes_than_its_base() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let base = directory.path().join("base");
    v1_store(&root);
    write(&root, &[("sha256:one", document("one", &body("one")))]).await;
    copy(&root, &base);
    migrate(&root, MigrationMode::Apply).unwrap();
    let manifest = &files_under(&root, "split-blobs")[0];
    fs::remove_file(manifest).unwrap();
    let findings = verify(&root, Some(&base));
    assert!(
        findings.iter().any(|finding| finding.rule == "V2"
            && finding
                .detail
                .contains("no longer served with the same bytes")),
        "{findings:?}"
    );
}
