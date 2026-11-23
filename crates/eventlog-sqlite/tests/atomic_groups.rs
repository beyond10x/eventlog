use eventlog_sqlite::SqliteEventStore;
#[tokio::test]
async fn ordered_groups_commit_and_rollback_as_one_unit() {
    let store = SqliteEventStore::in_memory("groups").await.unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
}

#[tokio::test]
async fn guarded_group_blobs_contract() {
    let store = SqliteEventStore::in_memory("guarded_group_blobs")
        .await
        .unwrap();
    assert!(
        eventlog_conformance::run_guarded_group_blobs(&store).await,
        "SQLite implements the guarded blob-bearing group in its one BEGIN IMMEDIATE; a refusal \
         here means the override was lost, and the exercise's other branch would have passed it"
    );
}

#[tokio::test]
async fn guarded_group_blobs_contract_on_a_database_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("guarded.sqlite3");
    let store = SqliteEventStore::open(path.to_str().unwrap(), "guarded_group_blobs")
        .await
        .unwrap();
    assert!(
        eventlog_conformance::run_guarded_group_blobs(&store).await,
        "a database file takes the same contract as memory: its reads hash, memory's do not"
    );
}

#[tokio::test]
async fn blob_group_retry_identity() {
    let store = SqliteEventStore::in_memory("retry_identity").await.unwrap();
    eventlog_conformance::run_blob_group_retry_identity(&store).await;
}

#[tokio::test]
async fn group_retry_survives_database_reopen() {
    use eventlog_core::{
        AppendGroup, AtomicEventStore, Expected, StreamAppend, StreamId, TenantId,
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("groups.sqlite");
    let path = path.to_str().unwrap();
    let tenant = TenantId::new("tenant").unwrap();
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta("reopen", &serde_json::json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant, "item", "one").unwrap(),
            expected: Expected::NoStream,
            events: vec![eventlog_conformance::event("item.created", 1)],
        }],
    };
    let store = SqliteEventStore::open(path, "groups").await.unwrap();
    let original = store.append_group(&group).await.unwrap();
    drop(store);
    let store = SqliteEventStore::open(path, "groups").await.unwrap();
    let retried = store.append_group(&group).await.unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.appends[0].events, original.appends[0].events);
}

// Opposite request orders must not become opposite lock orders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_groups_preserve_order_without_partial_commits() {
    use eventlog_core::{
        AppendGroup, AtomicEventStore, EventStore, Expected, StreamAppend, StreamId, TenantId,
    };
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("race.sqlite");
    let store = Arc::new(
        SqliteEventStore::open(path.to_str().unwrap(), "group_race")
            .await
            .unwrap(),
    );
    let tenant = TenantId::new("race-tenant").unwrap();
    let mut tasks = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    for index in 0..16 {
        let path = path.clone();
        let barrier = barrier.clone();
        let tenant = tenant.clone();
        tasks.push(tokio::spawn(async move {
            let store = SqliteEventStore::open(path.to_str().unwrap(), "group_race")
                .await
                .unwrap();
            let identity = index / 2;
            let ids = if identity % 2 == 0 {
                ["a", "z"]
            } else {
                ["z", "a"]
            };
            let group = AppendGroup {
                tenant: tenant.clone(),
                meta: eventlog_conformance::meta(
                    &format!("race-{identity}"),
                    &serde_json::json!({}),
                ),
                appends: ids
                    .iter()
                    .map(|id| StreamAppend {
                        stream: StreamId::new(tenant.clone(), "item", *id).unwrap(),
                        expected: Expected::Any,
                        events: vec![eventlog_conformance::event("item.changed", 1)],
                    })
                    .collect(),
            };
            barrier.wait().await;
            let first = store.append_group(&group).await.unwrap();
            let retry = store.append_group(&group).await.unwrap();
            assert!(retry.deduplicated);
            assert_eq!(first.appends[0].events, retry.appends[0].events);
            assert_eq!(first.appends[1].events, retry.appends[1].events);
            first
        }));
    }
    let mut duplicates = 0;
    for task in tasks {
        let result = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap();
        duplicates += usize::from(result.deduplicated);
        assert_eq!(result.appends.len(), 2);
        assert!(result.appends[0].events[0].global_seq < result.appends[1].events[0].global_seq);
    }
    assert_eq!(
        duplicates, 8,
        "one durable result for each simultaneous identity pair"
    );
    for id in ["a", "z"] {
        let stream = StreamId::new(tenant.clone(), "item", id).unwrap();
        let events = store.read_stream(&stream, 0, 100).await.unwrap().events;
        assert_eq!(events.len(), 8);
        assert_eq!(events.last().unwrap().version, 8);
    }
}

/// One observation: a batch read never shows one member of a group another handle committed
/// without the other, because the batch holds one read transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_many_never_shows_half_a_group_committed_by_another_handle() {
    use eventlog_core::{
        AppendGroup, AtomicEventStore, EventStore, Expected, Read, ReadResult, StreamAppend,
        StreamId, TenantId,
    };
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot.sqlite");
    let path = path.to_str().unwrap();
    let writer_store = Arc::new(SqliteEventStore::open(path, "snapshot").await.unwrap());
    let reader = SqliteEventStore::open(path, "snapshot").await.unwrap();
    let tenant = TenantId::new("snapshot-tenant").unwrap();
    let left = StreamId::new(tenant.clone(), "item", "left").unwrap();
    let right = StreamId::new(tenant.clone(), "item", "right").unwrap();
    writer_store
        .put_blob(&tenant, "fixture", b"content")
        .await
        .unwrap();
    let rounds = 80;
    let writer = {
        let (store, tenant, left, right) = (
            Arc::clone(&writer_store),
            tenant.clone(),
            left.clone(),
            right.clone(),
        );
        tokio::spawn(async move {
            for n in 0..rounds {
                let entry = |stream: &StreamId| StreamAppend {
                    stream: stream.clone(),
                    expected: Expected::Exact(n),
                    events: vec![eventlog_conformance::event("item.changed", 1)],
                };
                store
                    .append_group(&AppendGroup {
                        tenant: tenant.clone(),
                        appends: vec![entry(&left), entry(&right)],
                        meta: eventlog_conformance::meta(
                            &format!("pair-{n}"),
                            &serde_json::json!({ "n": n }),
                        ),
                    })
                    .await
                    .unwrap();
            }
        })
    };
    let reads = [
        Read::Stream {
            stream: left.clone(),
            after_version: 0,
            limit: 1_000,
        },
        Read::Blob {
            tenant: tenant.clone(),
            digest: "fixture".into(),
        },
        Read::Stream {
            stream: right.clone(),
            after_version: 0,
            limit: 1_000,
        },
    ];
    let mut observed = 0;
    while !writer.is_finished() {
        let batch = reader.read_many(&reads).await.unwrap();
        let lengths: Vec<usize> = batch
            .iter()
            .filter_map(|answer| match answer {
                ReadResult::Stream(slice) => Some(slice.events.len()),
                ReadResult::Blob(bytes) => {
                    assert_eq!(bytes.as_deref(), Some(&b"content"[..]));
                    None
                }
            })
            .collect();
        assert_eq!(lengths[0], lengths[1], "half a group visible: {lengths:?}");
        observed += 1;
    }
    writer.await.unwrap();
    assert!(observed > 0);
    let last = reader.read_many(&reads).await.unwrap();
    assert_eq!(
        last[0],
        ReadResult::Stream(reader.read_stream(&left, 0, 1_000).await.unwrap())
    );
}
