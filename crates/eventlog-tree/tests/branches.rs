//! What only a store merged outside itself has to get right: history that survives reopening
//! unchanged, writes interrupted before their commit point, forks made by merging two copies, and
//! files edited behind the store's back.

#[macro_use]
mod each_layout;

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, BranchableEventStore, EventLogError, EventStore,
    Expected, Guard, HeadSetDigest, ProjectionStore, Read, ReadResult, StreamAppend, StreamId,
    TenantId,
};
use eventlog_tree::{TreeEventStore, TreeLayout};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

async fn history_reopens_with_the_same_ids_versions_instants_and_receipts(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let body = json!({ "command": "one" });
    let first = {
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
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
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
        store.stream_identity(stream("x").tenant()).await.unwrap()
    };
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
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

async fn a_group_and_its_blobs_reopen_whole(layout: TreeLayout) {
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
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
        store
            .append_group_guarded_with_blobs(
                &group,
                std::sync::Arc::new(eventlog_core::NoGuard),
                &blobs,
            )
            .await
            .unwrap()
    };
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
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

async fn an_event_without_its_group_file_is_never_served_and_repair_removes_it(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
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

    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    assert_eq!(
        store.stream_version(&stream("x")).await.unwrap(),
        None,
        "an uncommitted event was served"
    );
    assert_eq!(store.torn().unwrap().len(), 1);
    assert_eq!(store.repair().await.unwrap().len(), 1);
    assert!(store.torn().unwrap().is_empty(), "repair left a torn file");
}

async fn two_branches_that_wrote_different_streams_merge_into_one_history(layout: TreeLayout) {
    let base = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open_in(base.path(), layout).await.unwrap();
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
        let store = TreeEventStore::open_in(ours.path(), layout).await.unwrap();
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
        let store = TreeEventStore::open_in(theirs.path(), layout)
            .await
            .unwrap();
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
    let store = TreeEventStore::open_in(ours.path(), layout).await.unwrap();
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(2));
    assert_eq!(store.stream_version(&stream("y")).await.unwrap(), Some(1));
    assert_eq!(
        store.heads(&stream("x")).await.unwrap().len(),
        1,
        "a linear stream forked"
    );
}

async fn two_branches_that_wrote_one_stream_fork_it_and_only_a_merge_over_both_heads_joins_it(
    layout: TreeLayout,
) {
    let base = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open_in(base.path(), layout).await.unwrap();
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
        let store = TreeEventStore::open_in(directory, layout).await.unwrap();
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

    let store = TreeEventStore::open_in(ours.path(), layout).await.unwrap();
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
    let reopened = TreeEventStore::open_in(ours.path(), layout).await.unwrap();
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

async fn a_merge_expectation_on_a_linear_stream_is_refused(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
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

async fn an_event_file_edited_behind_the_store_is_refused_on_open(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open_in(directory.path(), layout)
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
    }
    let file = walk(directory.path())
        .into_iter()
        .find(|path| path.components().any(|part| part.as_os_str() == "streams"))
        .unwrap();
    let edited = fs::read_to_string(&file)
        .unwrap()
        .replace("\"value\":1", "\"value\":2");
    fs::write(&file, edited).unwrap();
    let Err(error) = TreeEventStore::open_in(directory.path(), layout).await else {
        panic!("a store with an edited event opened");
    };
    assert!(matches!(error, EventLogError::Backend(ref message) if message.contains("corrupt")));
}

async fn a_second_handle_sees_the_first_handles_commit_before_it_writes(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let first = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let second = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
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

async fn a_redacted_body_stays_redacted_after_reopening_and_the_chain_holds(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
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
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let events = store.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert!(
        events[0].is_redacted(),
        "the redaction did not survive reopening"
    );
    assert_eq!(events[0].data, eventlog_core::redaction_tombstone("erased"));
    assert_eq!(events[1].parents, vec![events[0].digest.clone().unwrap()]);
}

async fn two_members_of_one_group_on_one_stream_chain_and_reopen(layout: TreeLayout) {
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
        let store = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
        store.append_group(&group).await.unwrap()
    };
    assert_eq!(
        written.appends[1].events[0].parents,
        vec![written.appends[0].events[0].digest.clone().unwrap()],
        "the second member does not follow the first"
    );
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    assert_eq!(store.heads(&stream("x")).await.unwrap().len(), 1);
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(2));
}

async fn a_redaction_or_erasure_through_one_handle_is_not_served_by_another(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::new("tenant-a").unwrap();
    let first = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let second = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    first
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1), event("item.indexed", 2)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
    let served = first.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert_eq!(served[0].data, json!({ "value": 1 }));

    second.redact(&stream("x"), 1, "erased").await.unwrap();
    let tombstone = eventlog_core::redaction_tombstone("erased");
    let events = first.read_stream(&stream("x"), 0, 10).await.unwrap().events;
    assert_eq!(
        events[0].data, tombstone,
        "the first handle served a body the second handle redacted"
    );
    let feed = first.read_feed(&tenant, 0, 10).await.unwrap().events;
    assert_eq!(
        feed[0].data, tombstone,
        "the first handle's feed served a body the second handle redacted"
    );

    second.forget_tenant(&tenant).await.unwrap();
    assert_eq!(
        first.read_stream(&stream("x"), 0, 10).await.unwrap().events,
        Vec::new(),
        "the first handle served a stream of a tenant the second handle erased"
    );
    assert_eq!(
        first.read_feed(&tenant, 0, 10).await.unwrap().events,
        Vec::new(),
        "the first handle's feed served a tenant the second handle erased"
    );
    assert_eq!(first.stream_version(&stream("x")).await.unwrap(), None);
}

async fn a_batch_read_sees_a_group_committed_beside_it_whole_or_not_at_all(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let group = AppendGroup {
        tenant: TenantId::new("tenant-a").unwrap(),
        appends: vec![
            StreamAppend {
                stream: stream("a"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 1)],
            },
            StreamAppend {
                stream: stream("b"),
                expected: Expected::NoStream,
                events: vec![event("item.received", 2)],
            },
        ],
        meta: meta("both", &json!({})),
    };
    let reads = [
        Read::Stream {
            stream: stream("a"),
            after_version: 0,
            limit: 10,
        },
        Read::Stream {
            stream: stream("b"),
            after_version: 0,
            limit: 10,
        },
    ];
    // The batch starts first, so the group commits while it is between its two reads.
    let (batch, committed) = tokio::join!(store.read_many(&reads), store.append_group(&group));
    committed.unwrap();
    let seen: Vec<usize> = batch
        .unwrap()
        .iter()
        .map(|result| match result {
            ReadResult::Stream(slice) => slice.events.len(),
            ReadResult::Blob(_) => unreachable!("only streams were read"),
        })
        .collect();
    assert!(
        seen == [0, 0] || seen == [1, 1],
        "one batch saw half of an atomic group: {seen:?} events in its two streams"
    );
}

async fn an_erasure_does_not_count_another_writers_group_as_loaded(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let erased = TenantId::new("tenant-a").unwrap();
    let other = StreamId::new(TenantId::new("tenant-b").unwrap(), "item", "y").unwrap();
    let first = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    first
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("erased", &json!({})),
        )
        .await
        .unwrap();
    let second = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    second
        .append(
            &other,
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("second", &json!({})),
        )
        .await
        .unwrap();

    first.forget_tenant(&erased).await.unwrap();
    let error = first
        .append(
            &other,
            Expected::NoStream,
            &[event("item.received", 2)],
            &meta("first", &json!({})),
        )
        .await
        .expect_err("an append after an erasure was checked against a stale head");
    assert!(
        matches!(
            error,
            EventLogError::Conflict {
                expected: 0,
                actual: 1
            }
        ),
        "{error:?}"
    );
    assert_eq!(first.heads(&other).await.unwrap().len(), 1);
}

/// A guard that tells the test it is running inside the engine's write, then waits there until
/// the test lets it go.
struct HeldGuard {
    entered: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Guard for HeldGuard {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.entered.lock().unwrap().send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        })
    }
}

async fn an_append_its_caller_abandoned_in_the_engine_is_not_served_or_deduplicated(
    layout: TreeLayout,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap(),
    );
    let (entered, entered_seen) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let guard = Arc::new(HeldGuard {
        entered: std::sync::Mutex::new(entered),
        release: std::sync::Mutex::new(released),
    });
    let appending = tokio::spawn({
        let store = Arc::clone(&store);
        async move {
            store
                .append_guarded(
                    &stream("x"),
                    Expected::NoStream,
                    &[event("item.received", 1)],
                    &meta("abandoned", &json!({})),
                    guard,
                )
                .await
        }
    });
    tokio::task::spawn_blocking(move || entered_seen.recv_timeout(Duration::from_secs(20)))
        .await
        .unwrap()
        .expect("the append never reached its guard");
    // The caller stops awaiting while the engine's worker is inside the write; the worker then
    // finishes it with no one left to commit it to the files.
    appending.abort();
    assert!(appending.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        store.read_stream(&stream("x"), 0, 10).await.unwrap().events,
        Vec::new(),
        "the handle served an append that never reached the files"
    );
    let retried = store
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("abandoned", &json!({})),
        )
        .await
        .unwrap();
    assert!(
        !retried.deduplicated,
        "a retry was answered from a receipt that never reached the files"
    );
    drop(store);
    let reopened = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    assert_eq!(
        reopened.stream_version(&stream("x")).await.unwrap(),
        Some(1),
        "the retried append did not reach the files"
    );
}

/// A store under `into` whose stream `x` two branches forked, returning its two heads.
async fn forked(into: &Path, layout: TreeLayout) -> Vec<String> {
    let theirs = tempfile::tempdir().unwrap();
    TreeEventStore::open_in(into, layout)
        .await
        .unwrap()
        .append(
            &stream("x"),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("base", &json!({})),
        )
        .await
        .unwrap();
    copy_tree(into, theirs.path());
    for (directory, key, value) in [(into, "ours", 2), (theirs.path(), "theirs", 3)] {
        TreeEventStore::open_in(directory, layout)
            .await
            .unwrap()
            .append(
                &stream("x"),
                Expected::Exact(1),
                &[event("item.indexed", value)],
                &meta(key, &json!({ "side": key })),
            )
            .await
            .unwrap();
    }
    merge_into(theirs.path(), into);
    let heads = TreeEventStore::open_in(into, layout)
        .await
        .unwrap()
        .heads(&stream("x"))
        .await
        .unwrap();
    assert_eq!(heads.len(), 2);
    heads
}

async fn a_merge_retried_after_it_committed_returns_its_receipt(layout: TreeLayout) {
    let directory = tempfile::tempdir().unwrap();
    let heads = forked(directory.path(), layout).await;
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    let merge = |digest| {
        let store = &store;
        async move {
            store
                .append(
                    &stream("x"),
                    Expected::Merge(digest),
                    &[event("item.merged", 9)],
                    &meta("merge", &json!({})),
                )
                .await
        }
    };
    let merged = merge(HeadSetDigest::of(&heads)).await.unwrap();
    assert!(!merged.deduplicated);

    let retried = merge(HeadSetDigest::of(&heads))
        .await
        .expect("a retry of a committed merge was refused");
    assert!(
        retried.deduplicated,
        "a retry of a committed merge wrote again"
    );
    assert_eq!(
        (retried.first_version, retried.last_version),
        (merged.first_version, merged.last_version)
    );
    assert_eq!(retried.events[0].event_id, merged.events[0].event_id);
    assert_eq!(retried.events[0].digest, merged.events[0].digest);

    // The head set is part of the merge request: the same key over other heads is another one.
    let other = merge(HeadSetDigest::of(&heads[..1]))
        .await
        .expect_err("a merge over other heads was answered from this merge's receipt");
    assert!(
        matches!(other, EventLogError::IdempotencyMismatch { ref key } if key == "merge"),
        "{other:?}"
    );
    assert_eq!(store.stream_version(&stream("x")).await.unwrap(), Some(4));
}

/// Two handles on one checkout contend for its writer lock on a runtime with one thread. The
/// handle that takes the lock first is suspended awaiting its engine while holding it, so the
/// other must wait for the lock without blocking the thread that handle needs to resume.
#[test]
fn two_handles_contending_for_the_writer_lock_on_one_thread_both_commit() {
    for layout in [TreeLayout::Nested, TreeLayout::Hashed] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_owned();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let versions = runtime.block_on(async {
                let first = TreeEventStore::open_in(&root, layout).await.unwrap();
                let second = TreeEventStore::open_in(&root, layout).await.unwrap();
                let (x, y) = (stream("x"), stream("y"));
                let (one_events, two_events) =
                    ([event("item.received", 1)], [event("item.received", 2)]);
                let (one_meta, two_meta) = (meta("first", &json!({})), meta("second", &json!({})));
                let (one, two) = tokio::join!(
                    first.append(&x, Expected::NoStream, &one_events, &one_meta),
                    second.append(&y, Expected::NoStream, &two_events, &two_meta),
                );
                (one.unwrap().last_version, two.unwrap().last_version)
            });
            let _ = done.send(versions);
        });
        match finished.recv_timeout(Duration::from_secs(20)) {
            Ok(versions) => assert_eq!(versions, (1, 1), "{layout:?}"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
                "{layout:?}: the single runtime thread blocked on the writer lock its holder needs \
                 that thread to release"
            ),
            Err(error) => panic!("{layout:?}: the runtime thread failed: {error}"),
        }
    }
}

/// What makes an open handle rebuild its engine from the files.
#[derive(Clone, Copy, Debug)]
enum Rebuild {
    AnotherHandleAppends,
    /// Another handle appends, and a third open replays the files in tenant order and keeps its
    /// state for exactly these files, which the rebuild must not load.
    AnotherReplayKeptItsState,
    MergeAddsHistory,
    AnotherHandleRedacts,
    AnotherHandleErases,
}

fn on(tenant: &str, id: &str) -> StreamId {
    StreamId::new(TenantId::new(tenant).unwrap(), "item", id).unwrap()
}

async fn append_one(
    store: &TreeEventStore,
    tenant: &str,
    id: &str,
) -> eventlog_core::RecordedEvent {
    store
        .append(
            &on(tenant, id),
            Expected::Any,
            &[event("item.received", 1)],
            &meta(&format!("{tenant}-{id}"), &json!({})),
        )
        .await
        .unwrap()
        .events
        .remove(0)
}

/// Every event of `tenant` after `cursor`, through every page, and the cursor a reader saves.
async fn feed_after(
    store: &TreeEventStore,
    tenant: &str,
    mut cursor: u64,
) -> (Vec<eventlog_core::RecordedEvent>, u64) {
    let tenant = TenantId::new(tenant).unwrap();
    let mut events = Vec::new();
    loop {
        let page = store.read_feed(&tenant, cursor, 2).await.unwrap();
        events.extend(page.events);
        cursor = page.next_position;
        if !page.has_more {
            return (events, cursor);
        }
    }
}

/// Positions are valid while the store is open, so a reader's saved feed cursor must not pass an
/// event it has not seen when the handle rebuilds its engine. Tenant `tenant-z` writes first and
/// `tenant-a` second, so a replay in tenant order would move `tenant-a`'s events, and the one new
/// to the handle, below the cursor `tenant-a`'s reader saved.
async fn a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
    layout: TreeLayout,
    rebuild: Rebuild,
) {
    let directory = tempfile::tempdir().unwrap();
    let branch = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    for (tenant, id) in [
        ("tenant-z", "one"),
        ("tenant-z", "two"),
        ("tenant-a", "one"),
        ("tenant-a", "two"),
    ] {
        append_one(&store, tenant, id).await;
    }
    let (seen, cursor) = feed_after(&store, "tenant-a", 0).await;
    let (z_seen, _) = feed_after(&store, "tenant-z", 0).await;
    let served: Vec<(String, u64)> = z_seen
        .iter()
        .chain(&seen)
        .map(|event| (event.event_id.clone(), event.global_seq))
        .collect();
    let high_water = served.iter().map(|(_, at)| *at).max().unwrap();

    let unseen = if matches!(rebuild, Rebuild::MergeAddsHistory) {
        copy_tree(directory.path(), branch.path());
        let other = TreeEventStore::open_in(branch.path(), layout)
            .await
            .unwrap();
        let unseen = append_one(&other, "tenant-a", "three").await;
        for path in walk(&branch.path().join("tenants")) {
            let target = directory
                .path()
                .join(path.strip_prefix(branch.path()).unwrap());
            if !target.exists() {
                fs::create_dir_all(target.parent().unwrap()).unwrap();
                fs::copy(&path, &target).unwrap();
            }
        }
        unseen
    } else {
        let other = TreeEventStore::open_in(directory.path(), layout)
            .await
            .unwrap();
        let unseen = append_one(&other, "tenant-a", "three").await;
        match rebuild {
            Rebuild::AnotherHandleRedacts => {
                other
                    .redact(&on("tenant-z", "one"), 1, "erased")
                    .await
                    .unwrap();
            }
            Rebuild::AnotherHandleErases => {
                other
                    .forget_tenant(&TenantId::new("tenant-z").unwrap())
                    .await
                    .unwrap();
            }
            Rebuild::AnotherReplayKeptItsState => {
                // A state is kept only once every file is older than the stamp window.
                tokio::time::sleep(Duration::from_millis(2_100)).await;
                let replaying = TreeEventStore::open_in(directory.path(), layout)
                    .await
                    .unwrap();
                assert!(replaying.replayed().await);
                drop(replaying);
                let warm = TreeEventStore::open_in(directory.path(), layout)
                    .await
                    .unwrap();
                assert!(
                    !warm.replayed().await,
                    "control: the third open kept its state for these files"
                );
            }
            _ => {}
        }
        unseen
    };
    // A write rebuilds after another writer's commit; a read rebuilds after a removal.
    let own = match rebuild {
        Rebuild::AnotherHandleAppends
        | Rebuild::AnotherReplayKeptItsState
        | Rebuild::MergeAddsHistory => Some(append_one(&store, "tenant-z", "three").await),
        Rebuild::AnotherHandleRedacts | Rebuild::AnotherHandleErases => None,
    };

    let (after, _) = feed_after(&store, "tenant-a", cursor).await;
    let ids: Vec<&str> = after.iter().map(|event| event.event_id.as_str()).collect();
    assert_eq!(
        ids,
        [unseen.event_id.as_str()],
        "{rebuild:?}: the reader at cursor {cursor} after {:?} was served {after:?}",
        seen.iter()
            .map(|event| event.global_seq)
            .collect::<Vec<_>>()
    );
    assert!(
        after[0].global_seq > high_water,
        "{rebuild:?}: history new to the handle placed at {} below its high water {high_water}",
        after[0].global_seq
    );
    let (a_now, _) = feed_after(&store, "tenant-a", 0).await;
    let (z_now, _) = feed_after(&store, "tenant-z", 0).await;
    let now: BTreeMap<String, u64> = a_now
        .iter()
        .chain(&z_now)
        .map(|event| (event.event_id.clone(), event.global_seq))
        .collect();
    for (event_id, at) in &served {
        match (rebuild, now.get(event_id)) {
            (Rebuild::AnotherHandleErases, None) => {}
            (_, Some(now)) => assert_eq!(now, at, "{rebuild:?}: a served event moved"),
            (_, None) => panic!("{rebuild:?}: a served event is gone"),
        }
    }
    if let Some(own) = own {
        assert!(
            own.global_seq > high_water,
            "{rebuild:?}: the write that rebuilt landed at {} below {high_water}",
            own.global_seq
        );
    }
    let next = append_one(&store, "tenant-a", "four").await;
    assert!(
        next.global_seq > after[0].global_seq,
        "{rebuild:?}: an append after the rebuild landed at {} below {}",
        next.global_seq,
        after[0].global_seq
    );
}

/// An erasure leaves a gap where the erased events stood. When the erased tenant held the highest
/// position, an event written to that tenant afterwards still lands above every position the
/// handle assigned, so the cursor its reader saved before the erasure passes nothing.
async fn an_erased_tenants_saved_cursor_passes_no_event_written_after_the_erasure(
    layout: TreeLayout,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    append_one(&store, "tenant-a", "one").await;
    append_one(&store, "tenant-z", "one").await;
    append_one(&store, "tenant-z", "two").await;
    let (_, cursor) = feed_after(&store, "tenant-z", 0).await;
    let other = TreeEventStore::open_in(directory.path(), layout)
        .await
        .unwrap();
    other
        .forget_tenant(&TenantId::new("tenant-z").unwrap())
        .await
        .unwrap();
    assert_eq!(feed_after(&store, "tenant-z", 0).await.0, Vec::new());
    let written = append_one(&store, "tenant-z", "three").await;
    let (after, _) = feed_after(&store, "tenant-z", cursor).await;
    assert_eq!(
        after
            .iter()
            .map(|event| &event.event_id)
            .collect::<Vec<_>>(),
        [&written.event_id],
        "an event written after the erasure took position {} at or below the saved cursor \
         {cursor}",
        written.global_seq
    );
}

async fn a_saved_feed_cursor_skips_nothing_after_another_handle_appends(layout: TreeLayout) {
    a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
        layout,
        Rebuild::AnotherHandleAppends,
    )
    .await;
}

async fn a_saved_feed_cursor_skips_nothing_when_another_replay_kept_its_state(layout: TreeLayout) {
    a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
        layout,
        Rebuild::AnotherReplayKeptItsState,
    )
    .await;
}

async fn a_saved_feed_cursor_skips_nothing_after_a_merge(layout: TreeLayout) {
    a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
        layout,
        Rebuild::MergeAddsHistory,
    )
    .await;
}

async fn a_saved_feed_cursor_skips_nothing_after_a_redaction(layout: TreeLayout) {
    a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
        layout,
        Rebuild::AnotherHandleRedacts,
    )
    .await;
}

async fn a_saved_feed_cursor_skips_nothing_after_a_tenant_erasure(layout: TreeLayout) {
    a_saved_feed_cursor_skips_nothing_when_the_open_handle_rebuilds(
        layout,
        Rebuild::AnotherHandleErases,
    )
    .await;
}

each_layout!(
    a_saved_feed_cursor_skips_nothing_after_another_handle_appends,
    a_saved_feed_cursor_skips_nothing_when_another_replay_kept_its_state,
    a_saved_feed_cursor_skips_nothing_after_a_merge,
    a_saved_feed_cursor_skips_nothing_after_a_redaction,
    a_saved_feed_cursor_skips_nothing_after_a_tenant_erasure,
    an_erased_tenants_saved_cursor_passes_no_event_written_after_the_erasure,
    history_reopens_with_the_same_ids_versions_instants_and_receipts,
    a_group_and_its_blobs_reopen_whole,
    an_event_without_its_group_file_is_never_served_and_repair_removes_it,
    two_branches_that_wrote_different_streams_merge_into_one_history,
    two_branches_that_wrote_one_stream_fork_it_and_only_a_merge_over_both_heads_joins_it,
    a_merge_expectation_on_a_linear_stream_is_refused,
    an_event_file_edited_behind_the_store_is_refused_on_open,
    a_second_handle_sees_the_first_handles_commit_before_it_writes,
    a_redacted_body_stays_redacted_after_reopening_and_the_chain_holds,
    two_members_of_one_group_on_one_stream_chain_and_reopen,
    a_redaction_or_erasure_through_one_handle_is_not_served_by_another,
    an_erasure_does_not_count_another_writers_group_as_loaded,
    a_batch_read_sees_a_group_committed_beside_it_whole_or_not_at_all,
    an_append_its_caller_abandoned_in_the_engine_is_not_served_or_deduplicated,
    a_merge_retried_after_it_committed_returns_its_receipt,
);
