use eventlog_sqlite::SqliteEventStore;
#[tokio::test]
async fn ordered_groups_commit_and_rollback_as_one_unit() {
    let store = SqliteEventStore::in_memory("groups").await.unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
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
