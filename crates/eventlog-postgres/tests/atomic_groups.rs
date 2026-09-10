use eventlog_postgres::PostgresEventStore;
#[tokio::test]
async fn ordered_groups_commit_and_rollback_as_one_unit() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    let store = PostgresEventStore::connect(&url, &format!("groups_{suffix}"))
        .await
        .unwrap();
    eventlog_conformance::run_atomic_groups(&store).await;
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

// Opposite request orders must not become opposite lock orders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_groups_preserve_order_without_partial_commits() {
    use eventlog_core::{
        AppendGroup, AtomicEventStore, EventStore, Expected, StreamAppend, StreamId, TenantId,
    };
    use std::sync::Arc;
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let initial = PostgresEventStore::connect(&url, "group_race")
        .await
        .unwrap();
    initial.drop_tables().await.unwrap();
    initial.shutdown().await.unwrap();
    let store = Arc::new(
        PostgresEventStore::connect(&url, "group_race")
            .await
            .unwrap(),
    );
    let tenant = TenantId::new("race-tenant").unwrap();
    let mut tasks = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    for index in 0..16 {
        let store = store.clone();
        let barrier = barrier.clone();
        let tenant = tenant.clone();
        tasks.push(tokio::spawn(async move {
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
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
