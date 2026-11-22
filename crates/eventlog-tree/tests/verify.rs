//! Each rule of the offline check refuses the defect it names, and a valid tree has no finding.

#[macro_use]
mod each_layout;

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{TreeEventStore, TreeLayout, verify};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", id).unwrap()
}

async fn written(root: &Path, layout: TreeLayout) {
    let store = TreeEventStore::open_in(root, layout).await.unwrap();
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

async fn a_tree_the_store_wrote_has_no_finding(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path(), layout).await;
    assert_eq!(verify(directory.path(), None), Vec::new());
}

async fn a_file_outside_the_layout_is_v1(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path(), layout).await;
    fs::write(directory.path().join("manifest.json"), b"{}").unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V1"]);
}

async fn an_event_no_group_commits_is_v3(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path(), layout).await;
    fs::remove_file(find(directory.path(), "groups")).unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V3", "V3"]);
}

async fn an_edited_event_is_v4(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path(), layout).await;
    let file = find(directory.path(), "streams");
    let edited = fs::read_to_string(&file)
        .unwrap()
        .replace("item.received", "item.forged")
        .replace("item.indexed", "item.forged");
    fs::write(&file, edited).unwrap();
    assert!(rules(&verify(directory.path(), None)).contains(&"V4"));
}

async fn a_reformatted_file_is_v5(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    written(directory.path(), layout).await;
    let file = directory.path().join("store.json");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
    fs::write(&file, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    assert_eq!(rules(&verify(directory.path(), None)), vec!["V5"]);
}

async fn a_deleted_or_rewritten_file_against_the_base_is_v2_but_a_redaction_is_not(
    layout: TreeLayout,
) {
    let base = tempfile::tempdir().unwrap();
    written(base.path(), layout).await;

    let redacted = tempfile::tempdir().unwrap();
    copy(base.path(), redacted.path());
    TreeEventStore::open_in(redacted.path(), layout)
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

/// The findings of rule `incomplete` whose detail contains `reason`.
fn incomplete_because(findings: &[eventlog_tree::Finding], reason: &str) -> bool {
    findings
        .iter()
        .any(|finding| finding.rule == "incomplete" && finding.detail.contains(reason))
}

async fn a_base_file_that_cannot_be_read_makes_the_comparison_incomplete(layout: TreeLayout) {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir().unwrap();
    written(base.path(), layout).await;
    let tree = tempfile::tempdir().unwrap();
    copy(base.path(), tree.path());

    // Permission bits do not hold for root; each case runs only where they do.
    let group = find(base.path(), "groups");
    fs::set_permissions(&group, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&group).is_err() {
        let findings = verify(tree.path(), Some(base.path()));
        assert!(
            incomplete_because(&findings, "a file of the base that cannot be read"),
            "{findings:?}"
        );
    }
    fs::set_permissions(&group, fs::Permissions::from_mode(0o644)).unwrap();

    let streams = base.path().join("tenants").join("tenant-a").join("streams");
    fs::set_permissions(&streams, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read_dir(&streams).is_err() {
        let findings = verify(tree.path(), Some(base.path()));
        assert!(
            incomplete_because(&findings, "a directory that cannot be listed"),
            "{findings:?}"
        );
    }
    fs::set_permissions(&streams, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(verify(tree.path(), Some(base.path())), Vec::new());
}

async fn a_base_that_names_no_store_or_another_store_is_incomplete(layout: TreeLayout) {
    let base = tempfile::tempdir().unwrap();
    written(base.path(), layout).await;
    let tree = tempfile::tempdir().unwrap();
    copy(base.path(), tree.path());

    let empty = tempfile::tempdir().unwrap();
    let findings = verify(tree.path(), Some(empty.path()));
    assert!(
        incomplete_because(&findings, "the base names no tree store"),
        "an empty base passed: {findings:?}"
    );

    let directory_in_place = tempfile::tempdir().unwrap();
    copy(base.path(), directory_in_place.path());
    let store_json = directory_in_place.path().join("store.json");
    fs::remove_file(&store_json).unwrap();
    fs::create_dir(&store_json).unwrap();
    let findings = verify(tree.path(), Some(directory_in_place.path()));
    assert!(
        incomplete_because(&findings, "the base names no tree store"),
        "a base whose store.json is a directory passed: {findings:?}"
    );

    let other = tempfile::tempdir().unwrap();
    written(other.path(), layout).await;
    let findings = verify(tree.path(), Some(other.path()));
    assert!(
        incomplete_because(&findings, "the base is another store"),
        "{findings:?}"
    );
}

each_layout!(
    a_base_file_that_cannot_be_read_makes_the_comparison_incomplete,
    a_base_that_names_no_store_or_another_store_is_incomplete,
    a_tree_the_store_wrote_has_no_finding,
    a_file_outside_the_layout_is_v1,
    an_event_no_group_commits_is_v3,
    an_edited_event_is_v4,
    a_reformatted_file_is_v5,
    a_deleted_or_rewritten_file_against_the_base_is_v2_but_a_redaction_is_not,
);
