use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, EventLogError, EventStore, Expected, StreamAppend, StreamId,
    TenantId,
};
use eventlog_file::FileEventStore;
use serde_json::json;
use std::{fs, path::Path, process::Command};

fn tenant() -> TenantId {
    TenantId::new("durable-owner").unwrap()
}
fn stream(id: &str) -> StreamId {
    StreamId::new(tenant(), "item", id).unwrap()
}
fn group(key: &str, reverse: bool) -> AppendGroup {
    let mut ids = ["a", "z"];
    if reverse {
        ids.reverse();
    }
    AppendGroup {
        tenant: tenant(),
        meta: meta(key, &json!({})),
        appends: ids
            .into_iter()
            .map(|id| StreamAppend {
                stream: stream(id),
                expected: Expected::Any,
                events: vec![event("item.created", 1)],
            })
            .collect(),
    }
}
fn bytes_below(root: &Path) -> Vec<u8> {
    let mut all = Vec::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            all.extend(bytes_below(&path));
        } else {
            all.extend(fs::read(path).unwrap());
        }
    }
    all
}
#[tokio::test(flavor = "multi_thread")]
async fn process_writer() {
    let Ok(root) = std::env::var("EVENTLOG_FILE_WRITER_ROOT") else {
        return;
    };
    let id = std::env::var("EVENTLOG_FILE_WRITER_ID").unwrap();
    let store = FileEventStore::open(&root).await.unwrap();
    for index in 0..8 {
        store
            .append_group(&group(&format!("{id}-{index}"), index % 2 == 1))
            .await
            .unwrap();
        let retry = store
            .append_group(&group("same-command", false))
            .await
            .unwrap();
        assert_eq!(retry.appends.len(), 2);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn independent_processes_serialize_groups_and_duplicate_keys() {
    let directory = tempfile::tempdir().unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| {
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process_writer", "--nocapture"])
                .env("EVENTLOG_FILE_WRITER_ROOT", directory.path())
                .env("EVENTLOG_FILE_WRITER_ID", index.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let outputs: Vec<_> = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap())
        .collect();
    for output in outputs {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let store = FileEventStore::open(directory.path()).await.unwrap();
    for id in ["a", "z"] {
        assert_eq!(store.stream_version(&stream(id)).await.unwrap(), Some(33));
    }
    let events = store.read_feed(&tenant(), 0, 1000).await.unwrap().events;
    assert_eq!(events.len(), 66);
    for pair in events.as_chunks::<2>().0 {
        assert_ne!(pair[0].stream_id, pair[1].stream_id);
        assert_eq!(
            pair[0].request_id, pair[1].request_id,
            "groups never interleave"
        );
        assert_eq!(pair[0].global_seq + 1, pair[1].global_seq);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn reopen_preserves_receipts_and_cache_deletion_preserves_authority() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let request = group("first", false);
    let original = store.append_group(&request).await.unwrap();
    let generation = store
        .snapshot_generation(&stream("a"))
        .await
        .unwrap()
        .unwrap();
    let snapshot = eventlog_core::Snapshot {
        version: 1,
        state_schema_version: 1,
        state: json!({"count":1}),
        recorded_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    assert!(
        store
            .save_snapshot_checked(&stream("a"), &snapshot, &generation)
            .await
            .unwrap()
    );
    drop(store);
    fs::remove_dir_all(directory.path().join(".cache")).unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let retry = store.append_group(&request).await.unwrap();
    assert!(retry.deduplicated);
    assert_eq!(original.appends[0].events, retry.appends[0].events);
    assert!(store.load_snapshot(&stream("a")).await.unwrap().is_none());
    let mut changed = request;
    changed.appends[0].events[0].data = json!({"changed":true});
    assert!(matches!(
        store.append_group(&changed).await,
        Err(EventLogError::IdempotencyMismatch { .. })
    ));
}
#[tokio::test(flavor = "multi_thread")]
async fn privacy_removes_active_bytes_and_never_reuses_feed_positions() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let secret = "payload-marker-that-must-disappear";
    let request =
        [eventlog_core::NewEvent::new("item.created", 1, json!({"marker":secret})).unwrap()];
    let original = store
        .append(
            &stream("private"),
            Expected::NoStream,
            &request,
            &meta("private", &json!({})),
        )
        .await
        .unwrap();
    store
        .put_blob(&tenant(), "blob-ref", b"blob-marker-that-must-disappear")
        .await
        .unwrap();
    let generation = store
        .snapshot_generation(&stream("private"))
        .await
        .unwrap()
        .unwrap();
    let snapshot = eventlog_core::Snapshot {
        version: 1,
        state_schema_version: 1,
        state: json!({"marker":secret}),
        recorded_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    store
        .save_snapshot_checked(&stream("private"), &snapshot, &generation)
        .await
        .unwrap();
    let redacted = store.redact(&stream("private"), 1, "erased").await.unwrap();
    assert_eq!(redacted.event_id, original.events[0].event_id);
    assert!(!String::from_utf8_lossy(&bytes_below(directory.path())).contains(secret));
    store.forget_tenant(&tenant()).await.unwrap();
    assert!(
        !String::from_utf8_lossy(&bytes_below(directory.path()))
            .contains("blob-marker-that-must-disappear")
    );
    drop(store);
    let store = FileEventStore::open(directory.path()).await.unwrap();
    assert!(
        store
            .read_feed(&tenant(), 0, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(
        store
            .get_blob(&tenant(), "blob-ref")
            .await
            .unwrap()
            .is_none()
    );
    let fresh = store
        .append(
            &stream("private"),
            Expected::NoStream,
            &[event("item.created", 2)],
            &meta("private", &json!({})),
        )
        .await
        .unwrap();
    assert!(!fresh.deduplicated);
    assert!(fresh.events[0].global_seq > original.events[0].global_seq);
}
#[tokio::test(flavor = "multi_thread")]
async fn blob_damage_and_missing_manifest_refuse_reopen() {
    for damage in ["blob", "manifest"] {
        let directory = tempfile::tempdir().unwrap();
        let store = FileEventStore::open(directory.path()).await.unwrap();
        store
            .put_blob(&tenant(), "digest", b"original")
            .await
            .unwrap();
        drop(store);
        if damage == "blob" {
            let path = fs::read_dir(directory.path().join("blobs"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            fs::write(path, b"tampered").unwrap();
        } else {
            fs::remove_file(directory.path().join("manifest.json")).unwrap();
        }
        assert!(
            FileEventStore::open(directory.path()).await.is_err(),
            "{damage}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicting_groups_commit_exactly_one_complete_request() {
    let directory = tempfile::tempdir().unwrap();
    let left = FileEventStore::open(directory.path()).await.unwrap();
    let right = FileEventStore::open(directory.path()).await.unwrap();
    let mut a = group("left", false);
    let mut b = group("right", true);
    for request in [&mut a, &mut b] {
        for entry in &mut request.appends {
            entry.expected = Expected::NoStream;
        }
    }
    let (a, b) = tokio::join!(left.append_group(&a), right.append_group(&b));
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(matches!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(EventLogError::Conflict { .. })
    ));
    assert_eq!(
        left.read_feed(&tenant(), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        2
    );
}

const COPY: eventlog_core::ProjectionSpec = eventlog_core::ProjectionSpec {
    name: "body_copy",
    indexed: &[],
};
struct CopyBody;
impl eventlog_core::Projector for CopyBody {
    fn name(&self) -> &'static str {
        "copy_body"
    }
    fn projections(&self) -> &'static [eventlog_core::ProjectionSpec] {
        &[COPY]
    }
    fn apply<'a>(
        &'a self,
        event: &'a eventlog_core::RecordedEvent,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            store
                .upsert(&COPY, &event.tenant, &event.stream_id, &event.data)
                .await
        })
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn redaction_fences_projection_reads_and_new_writes_until_complete_rebuild() {
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let store = FileEventStore::open(directory.path()).await.unwrap();
    let projector = Arc::new(CopyBody);
    store.register_inline(projector.clone()).await.unwrap();
    let request = group("before-redaction", false);
    let first = store.append_group(&request).await.unwrap();
    store.redact(&stream("a"), 1, "erased").await.unwrap();
    assert!(store.append_group(&request).await.unwrap().deduplicated);
    assert!(store.projection_get(&COPY, &tenant(), "a").await.is_err());
    assert!(
        store
            .append_group(&group("after-redaction", false))
            .await
            .is_err()
    );
    assert_eq!(store.stream_version(&stream("z")).await.unwrap(), Some(1));
    assert_eq!(
        store
            .rebuild_projection(projector, &tenant())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .projection_get(&COPY, &tenant(), "a")
            .await
            .unwrap()
            .unwrap(),
        json!({"redacted":true,"reason":"erased"})
    );
    assert!(
        !store
            .append_group(&group("after-redaction", false))
            .await
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        store
            .read_stream(&stream("a"), 0, 100)
            .await
            .unwrap()
            .events[0]
            .event_id,
        first.appends[0].events[0].event_id
    );
}
