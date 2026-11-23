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

#[tokio::test]
async fn guarded_group_blobs_contract() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    let store = PostgresEventStore::connect(&url, &format!("gblobs_{suffix}"))
        .await
        .unwrap();
    assert!(
        eventlog_conformance::run_guarded_group_blobs(&store).await,
        "this provider implements the guarded blob-bearing group in one transaction; losing it \
         would hand entity-runtime's adapter back to one put_blob transaction per blob"
    );
    eventlog_conformance::run_blob_group_retry_identity(&store).await;
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

/// Beyond the shared exercise: a retry carrying no batch is the ordinary group retry and repeats
/// no admission; a group committed without a batch never bound one; a later member's conflict
/// binds no blob of the batch; the recorded digests with other bytes refuse; and erasure takes
/// the batch record with the receipt.
#[tokio::test]
async fn guarded_group_blobs_retries_and_member_conflicts() {
    use eventlog_core::{
        AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, Guard,
        NoGuard, ProjectionStore, StreamAppend, StreamId, TenantId,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Counting(Arc<AtomicUsize>);
    impl Guard for Counting {
        fn check<'a>(
            &'a self,
            _store: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    let store = PostgresEventStore::connect(&url, &format!("gretry_{suffix}"))
        .await
        .unwrap();
    let tenant = TenantId::new("guarded-retry").unwrap();
    let stream = |id: &str| StreamId::new(tenant.clone(), "item", id).unwrap();
    let group = |key: &str, members: &[(&str, Expected)]| AppendGroup {
        tenant: tenant.clone(),
        meta: eventlog_conformance::meta(key, &serde_json::json!({})),
        appends: members
            .iter()
            .map(|(id, expected)| StreamAppend {
                stream: stream(id),
                expected: *expected,
                events: vec![eventlog_conformance::event("item.changed", 1)],
            })
            .collect(),
    };
    let blob = |digest: &str, bytes: &str| (digest.to_owned(), bytes.as_bytes().to_vec());

    let committed = group("retried", &[("one", Expected::NoStream)]);
    let batch = [blob("a", "alpha"), blob("b", "beta"), blob("a", "alpha")];
    let calls = Arc::new(AtomicUsize::new(0));
    let first = store
        .append_group_guarded_with_blobs(&committed, Arc::new(Counting(calls.clone())), &batch)
        .await
        .unwrap();
    assert!(!first.deduplicated);
    let plain = store
        .append_group_guarded(&committed, Arc::new(Counting(calls.clone())))
        .await
        .unwrap();
    assert!(
        plain.deduplicated,
        "a retry carrying no batch asks nothing about blobs and is the committed group"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "and repeats no admission");
    let changed = store
        .append_group_guarded_with_blobs(
            &committed,
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("b", "other")],
        )
        .await
        .expect_err("the recorded digests with other bytes");
    assert_eq!(
        changed,
        EventLogError::Invalid("blob digest already names different content".into())
    );
    let repeated = store
        .append_group_guarded_with_blobs(
            &committed,
            Arc::new(NoGuard),
            &[blob("a", "alpha"), blob("a", "other")],
        )
        .await
        .expect_err("one digest with two contents in one batch");
    assert_eq!(
        repeated,
        EventLogError::Invalid("blob digest already names different content".into())
    );

    let legacy = group("legacy", &[("old", Expected::NoStream)]);
    store.append_group(&legacy).await.unwrap();
    let asked = store
        .append_group_guarded_with_blobs(&legacy, Arc::new(NoGuard), &[blob("a", "alpha")])
        .await
        .expect_err("a group committed without a batch never bound one");
    assert_eq!(
        asked,
        EventLogError::IdempotencyMismatch {
            key: "legacy".into()
        }
    );

    // The second member conflicts after admission and after the batch was bound in the same
    // transaction: nothing of the group, and no blob of the batch, is published.
    let conflicting = group(
        "conflicts",
        &[("fresh", Expected::NoStream), ("one", Expected::NoStream)],
    );
    let refused = store
        .append_group_guarded_with_blobs(&conflicting, Arc::new(NoGuard), &[blob("c", "gamma")])
        .await
        .expect_err("the second member's stream already exists");
    assert_eq!(
        refused,
        EventLogError::Conflict {
            expected: 0,
            actual: 1
        }
    );
    assert_eq!(store.get_blob(&tenant, "c").await.unwrap(), None);
    assert_eq!(store.stream_version(&stream("fresh")).await.unwrap(), None);

    // Erasure removes the tenant's batch records with its receipts: the same key and batch
    // afterwards is a fresh request.
    store.forget_tenant(&tenant).await.unwrap();
    let again = store
        .append_group_guarded_with_blobs(&committed, Arc::new(NoGuard), &batch)
        .await
        .unwrap();
    assert!(!again.deduplicated, "the erased tenant's receipt is gone");
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
