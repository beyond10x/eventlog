//! Each rule of the offline check refuses the defect it names, and a valid tree has no finding.

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{TreeEventStore, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", id).unwrap()
}

async fn written(root: &Path) {
    let store = TreeEventStore::open(root).await.unwrap();
    store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
}

fn find(root: &Path, part: &str) -> PathBuf {
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .components()
                .any(|component| component.as_os_str() == part)
            {
                return path;
            }
        }
    }
    panic!("no file under {part}");
}

fn copy(from: &Path, into: &Path) {
    for entry in fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = into.join(path.file_name().unwrap());
        if path.is_dir() {
            fs::create_dir_all(&target).unwrap();
            copy(&path, &target);
        } else {
            fs::copy(&path, &target).unwrap();
        }
    }
}

fn rules(findings: &[eventlog_tree::Finding]) -> Vec<&'static str> {
    findings.iter().map(|finding| finding.rule).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tree_the_store_wrote_has_no_finding() {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path()).await;
    assert_eq!(verify(directory.path(), None), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_outside_the_layout_is_v1() {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path()).await;
    fs::write(directory.path().join("manifest.json"), b"{}").unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_no_group_commits_is_v3() {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path()).await;
    fs::remove_file(find(directory.path(), "groups")).unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V3", "V3"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edited_event_is_v4() {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path()).await;
    let file = find(directory.path(), "streams");
    let edited = fs::read_to_string(&file)
        .unwrap()
        .replace("item.received", "item.forged")
        .replace("item.indexed", "item.forged");
    fs::write(&file, edited).unwrap();
    assert!(rules(&verify(directory.path(), None)).contains(&"V4"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reformatted_file_is_v5() {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path()).await;
    let file = directory.path().join("store.json");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
    fs::write(&file, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V5"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_or_rewritten_file_against_the_base_is_v2_but_a_redaction_is_not() {
    let base = tempfile::tempdir().unwrap();
    written(base.path()).await;

    let redacted = tempfile::tempdir().unwrap();
    copy(base.path(), redacted.path());
    TreeEventStore::open(redacted.path())
        .await
        .unwrap()
        .redact(&stream("x"), 1, "erased")
        .await
        .unwrap();
    assert_eq!(
        verify(redacted.path(), Some(base.path())),
        Vec::new(),
        "a redaction was reported as a change"
    );

    let deleted = tempfile::tempdir().unwrap();
    copy(base.path(), deleted.path());
    fs::remove_file(find(deleted.path(), "groups")).unwrap();
    assert!(rules(&verify(deleted.path(), Some(base.path()))).contains(&"V2"));

    let rewritten = tempfile::tempdir().unwrap();
    copy(base.path(), rewritten.path());
    let file = find(rewritten.path(), "groups");
    let text = fs::read_to_string(&file)
        .unwrap()
        .replace("\"one\"", "\"two\"");
    fs::write(&file, text).unwrap();
    assert!(rules(&verify(rewritten.path(), Some(base.path()))).contains(&"V2"));
}
