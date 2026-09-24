//! What only a store merged outside itself has to get right: history that survives reopening
//! unchanged, writes interrupted before their commit point, forks made by merging two copies, and
//! files edited behind the store's back.

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BranchableEventStore, EventLogError, EventStore, Expected,
    HeadSetDigest, StreamAppend, StreamId, TenantId,
};
use eventlog_tree::TreeEventStore;
use serde_json::json;
use std::fs;
use std::path::Path;

fn stream(id: &str) -> StreamId {
    StreamId::new(TenantId::new("tenant-a").unwrap(), "item", id).unwrap()
}

/// Copy every file under `from` into `into`, as a merge of two branches that added different
/// files does. A file both sides hold must be identical, as Git would leave it.
fn merge_into(from: &Path, into: &Path) {
    for entry in walk(from) {
        let relative = entry.strip_prefix(from).unwrap();
        if relative == Path::new(".lock") {
            continue;
        }
        let target = into.join(relative);
        if let Ok(existing) = fs::read(&target) {
            assert_eq!(
                existing,
                fs::read(&entry).unwrap(),
                "{} conflicts",
                relative.display()
            );
            continue;
        }
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(&entry, &target).unwrap();
    }
}

fn walk(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn copy_tree(from: &Path, into: &Path) {
    fs::create_dir_all(into).unwrap();
    merge_into(from, into);
}

#[tokio::test(flavor = "multi_thread")]
async fn history_reopens_with_the_same_ids_versions_instants_and_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let body = json!({ "command": "one" });
    let first = {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store.stream_identity(stream("x").tenant()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1), event("item.indexed", 2)],
                &meta("key-1", &body),
            )
            .await
            .unwrap()
    };
    let identity_before = {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store.stream_identity(stream("x").tenant()).await.unwrap()
    };
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    assert_eq!(
        store.stream_identity(stream("x").tenant()).await.unwrap(),
        identity_before,
        "a reopened tenant got a new stream identity"
    );
    let read = store.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert_eq!(read, first.events, "reopening changed an event");
    let retry = store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("key-1", &body),
        )
        .await
        .unwrap();
    assert!(
        retry.deduplicated,
        "a retry after reopening wrote a second time"
    );
    assert_eq!(retry.events, first.events);
    assert_eq!(read[1].parents, vec![read[0].digest.clone().unwrap()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_and_its_blobs_reopen_whole() {
    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("tenant-a").unwrap();
    let group = AppendGroup {
        tenant: tenant.clone(),
        appends: vec![
            StreamAppend {
                stream: stream("left"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 1)],
            },
            StreamAppend {
                stream: stream("right"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 2)],
            },
        ],
        meta: meta("group-1", &json!({ "group": 1 })),
    };
    let blobs = vec![("blob-1".to_owned(), b"bytes".to_vec())];
    let written = {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store
            .append_group_guarded_with_blobs(
                &group,
                std::sync::Arc::new(eventlog_core::NoGuard),
                &blobs,
            )
            .await
            .unwrap()
    };
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    assert_eq!(
        store.get_blob(&tenant, "blob-1").await.unwrap(),
        Some(b"bytes".to_vec())
    );
    for (index, id) in ["left", "right"].into_iter().enumerate() {
        assert_eq!(
            store.read_stream(&stream(id), 0, 10).await.unwrap().events,
            written.appends[index].events
        );
    }
    let retry = store
        .append_group_guarded_with_blobs(
            &group,
            std::sync::Arc::new(eventlog_core::NoGuard),
            &blobs,
        )
        .await
        .unwrap();
    assert!(
        retry.deduplicated,
        "a group retried after reopening wrote twice"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_without_its_group_file_is_never_served_and_repair_removes_it() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("key-1", &json!({})),
            )
            .await
            .unwrap();
    }
    // Remove the commit point: the event file is now what a crash before it leaves.
    let groups: Vec<_> = walk(directory.path())
        .into_iter()
        .filter(|path| path.components().any(|part| part.as_os_str() == "groups"))
        .collect();
    assert_eq!(groups.len(), 1);
    fs::remove_file(&groups[0]).unwrap();

    let store = TreeEventStore::open(directory.path()).await.unwrap();
    assert_eq!(
        store.stream_version(&stream("x")).await.unwrap(),
        None,
        "an uncommitted event was served"
    );
    assert_eq!(store.torn().unwrap().len(), 1);
    assert_eq!(store.repair().await.unwrap().len(), 1);
    assert!(store.torn().unwrap().is_empty(), "repair left a torn file");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_branches_that_wrote_different_streams_merge_into_one_history() {
    let base = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(base.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("base", &json!({})),
            )
            .await
            .unwrap();
    }
    let ours = tempfile::tempdir().unwrap();
    let theirs = tempfile::tempdir().unwrap();
    copy_tree(base.path(), ours.path());
    copy_tree(base.path(), theirs.path());
    {
        let store = TreeEventStore::open(ours.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::Exact(1),
                &[event("item.indexed", 2)],
                &meta("ours", &json!({})),
            )
            .await
            .unwrap();
    }
    {
        let store = TreeEventStore::open(theirs.path()).await.unwrap();
        store
            .append(
                &stream("y"),
                Expected::NoStream,
                &[event("item.received", 3)],
                &meta("theirs", &json!({})),
            )
            .await
            .unwrap();
    }
    merge_into(theirs.path(), ours.path());
    let store = TreeEventStore::open(ours.path()).await.unwrap();
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(2));
    assert_eq!(store.stream_version(&stream("y")).await.unwrap(), Some(1));
    assert_eq!(
        store.heads(&stream("x")).await.unwrap().len(),
        1,
        "a linear stream forked"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_branches_that_wrote_one_stream_fork_it_and_only_a_merge_over_both_heads_joins_it() {
    let base = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(base.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("base", &json!({})),
            )
            .await
            .unwrap();
    }
    let ours = tempfile::tempdir().unwrap();
    let theirs = tempfile::tempdir().unwrap();
    copy_tree(base.path(), ours.path());
    copy_tree(base.path(), theirs.path());
    for (directory, key, value) in [(ours.path(), "ours", 2), (theirs.path(), "theirs", 3)] {
        let store = TreeEventStore::open(directory).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::Exact(1),
                &[event("item.indexed", value)],
                &meta(key, &json!({ "side": key })),
            )
            .await
            .unwrap();
    }
    merge_into(theirs.path(), ours.path());

    let store = TreeEventStore::open(ours.path()).await.unwrap();
    let heads = store.heads(&stream("x")).await.unwrap();
    assert_eq!(
        heads.len(),
        2,
        "the merged stream did not report both heads"
    );
    let events = store.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert_eq!(events.len(), 3, "a side of the fork was not served");
    assert_eq!(
        events.iter().map(|event| event.version).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "a forked stream's versions are not its replay order"
    );

    let refused = store
        .append(
            &stream("x"),
            Expected::Exact(3),
            &[event("item.indexed", 9)],
            &meta("after", &json!({})),
        )
        .await
        .expect_err("an ordinary append extended a forked stream");
    assert!(matches!(refused, EventLogError::Forked { ref heads, .. } if heads.len() == 2));

    let partial = store
        .append(
            &stream("x"),
            Expected::Merge(HeadSetDigest::of(&heads[..1])),
            &[event("item.merged", 9)],
            &meta("partial", &json!({})),
        )
        .await
        .expect_err("a merge over one head of two was accepted");
    assert!(matches!(partial, EventLogError::Forked { .. }));

    let merged = store
        .append(
            &stream("x"),
            Expected::Merge(HeadSetDigest::of(&heads)),
            &[event("item.merged", 9)],
            &meta("merge", &json!({})),
        )
        .await
        .unwrap();
    let mut parents = merged.events[0].parents.clone();
    parents.sort();
    let mut expected = heads.clone();
    expected.sort();
    assert_eq!(
        parents, expected,
        "the merge event does not follow both heads"
    );
    assert_eq!(store.heads(&stream("x")).await.unwrap().len(), 1);

    drop(store);
    let reopened = TreeEventStore::open(ours.path()).await.unwrap();
    assert_eq!(
        reopened.heads(&stream("x")).await.unwrap().len(),
        1,
        "the join did not survive reopening"
    );
    assert_eq!(
        reopened.stream_version(&stream("x")).await.unwrap(),
        Some(4)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_merge_expectation_on_a_linear_stream_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
    let heads = store.heads(&stream("x")).await.unwrap();
    let error = store
        .append(
            &stream("x"),
            Expected::Merge(HeadSetDigest::of(&heads)),
            &[event("item.merged", 2)],
            &meta("two", &json!({})),
        )
        .await
        .expect_err("a merge over a single head was accepted");
    assert!(matches!(error, EventLogError::Invalid(_)));
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_file_edited_behind_the_store_is_refused_on_open() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1)],
                &meta("one", &json!({})),
            )
            .await
            .unwrap();
    }
    let file = walk(directory.path())
        .into_iter()
        .find(|path| path.components().any(|part| part.as_os_str() == "streams"))
        .unwrap();
    let edited = fs::read_to_string(&file)
        .unwrap()
        .replace("\"value\":1", "\"value\":2");
    fs::write(&file, edited).unwrap();
    let Err(error) = TreeEventStore::open(directory.path()).await else {
        panic!("a store with an edited event opened");
    };
    assert!(matches!(error, EventLogError::Backend(ref message) if message.contains("corrupt")));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_handle_sees_the_first_handles_commit_before_it_writes() {
    let directory = tempfile::tempdir().unwrap();
    let first = TreeEventStore::open(directory.path()).await.unwrap();
    let second = TreeEventStore::open(directory.path()).await.unwrap();
    first
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("first", &json!({})),
        )
        .await
        .unwrap();
    let error = second
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 2)],
            &meta("second", &json!({})),
        )
        .await
        .expect_err("a second writer appended over history it had not read");
    assert!(matches!(
        error,
        EventLogError::Conflict {
            expected: 0,
            actual: 1
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redacted_body_stays_redacted_after_reopening_and_the_chain_holds() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store
            .append(
                &stream("x"),
                Expected::NoStream,
                &[event("item.received", 1), event("item.indexed", 2)],
                &meta("one", &json!({})),
            )
            .await
            .unwrap();
        store.redact(&stream("x"), 1, "erased").await.unwrap();
    }
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    let events = store.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert!(
        events[0].is_redacted(),
        "the redaction did not survive reopening"
    );
    assert_eq!(events[0].data, eventlog_core::redaction_tombstone("erased"));
    assert_eq!(events[1].parents, vec![events[0].digest.clone().unwrap()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_members_of_one_group_on_one_stream_chain_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("tenant-a").unwrap();
    let group = AppendGroup {
        tenant: tenant.clone(),
        appends: vec![
            StreamAppend {
                stream: stream("x"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 1)],
            },
            StreamAppend {
                stream: stream("x"),
                expected: Expected::Exact(1),
                events: vec![event("item.indexed", 2)],
            },
        ],
        meta: meta("one-stream-group", &json!({})),
    };
    let written = {
        let store = TreeEventStore::open(directory.path()).await.unwrap();
        store.append_group(&group).await.unwrap()
    };
    assert_eq!(
        written.appends[1].events[0].parents,
        vec![written.appends[0].events[0].digest.clone().unwrap()],
        "the second member does not follow the first"
    );
    let store = TreeEventStore::open(directory.path()).await.unwrap();
    assert_eq!(store.heads(&stream("x")).await.unwrap().len(), 1);
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(2));
}
