use eventlog_conformance::transaction_group;
use eventlog_core::{
    EventLogError, EventStore, Expected, StreamId, TenantId, TransactionalEventStore,
};
use eventlog_postgres::{PoolOptions, PostgresEventStore};
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned disposable PostgreSQL URL")
}
fn tenant() -> TenantId {
    TenantId::new("session-test").unwrap()
}

#[tokio::test]
async fn native_session_contract_commits_and_rolls_back_complete_work() {
    let store = PostgresEventStore::connect(&url(), "session_contract")
        .await
        .unwrap();
    eventlog_conformance::run_transaction_sessions(&store).await;
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn staged_work_stays_invisible_and_absent_identity_locks_serialize_contenders() {
    let store = Arc::new(
        PostgresEventStore::connect(&url(), "session_visibility")
            .await
            .unwrap(),
    );
    let observer = Arc::new(
        PostgresEventStore::connect(&url(), "session_visibility")
            .await
            .unwrap(),
    );
    let scoped = tenant();
    let (staged, ready) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let worker = store.clone();
    let resume = release.clone();
    let writer = tokio::spawn(async move {
        worker
            .with_transaction(&tenant(), move |session| {
                Box::pin(async move {
                    let tenant = session.tenant().clone();
                    session.lock_identity("logical", "absent").await?;
                    session
                        .lock_stream(&StreamId::new(tenant.clone(), "item", "absent").unwrap())
                        .await?;
                    session
                        .append_group(&transaction_group(
                            &tenant,
                            "visible-after-commit",
                            &[("one", Expected::NoStream, 1)],
                        ))
                        .await?;
                    staged.send(()).unwrap();
                    resume.notified().await;
                    Ok(())
                })
            })
            .await
    });
    ready.await.unwrap();
    let one = StreamId::new(scoped.clone(), "item", "one").unwrap();
    assert_eq!(observer.stream_version(&one).await.unwrap(), None);
    let waiting = store.clone();
    let (locked, mut acquired) = oneshot::channel();
    let contender = tokio::spawn(async move {
        waiting
            .with_transaction(&tenant(), move |session| {
                Box::pin(async move {
                    session.lock_identity("logical", "absent").await?;
                    locked.send(()).unwrap();
                    Ok(())
                })
            })
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut acquired)
            .await
            .is_err(),
        "contender acquired a held absent identity"
    );
    let ordinary_store = observer.clone();
    let (completed, mut ordinary_done) = oneshot::channel();
    let ordinary = tokio::spawn(async move {
        let result = ordinary_store
            .append(
                &StreamId::new(tenant(), "item", "absent").unwrap(),
                Expected::NoStream,
                &[eventlog_conformance::event("item.changed", 1)],
                &eventlog_conformance::meta("ordinary", &serde_json::json!({})),
            )
            .await;
        completed.send(()).unwrap();
        result
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut ordinary_done)
            .await
            .is_err(),
        "ordinary append escaped a native stream lock"
    );
    release.notify_one();
    writer.await.unwrap().unwrap();
    acquired.await.unwrap();
    contender.await.unwrap().unwrap();
    ordinary_done.await.unwrap();
    ordinary.await.unwrap().unwrap();
    assert_eq!(observer.stream_version(&one).await.unwrap(), Some(1));
    store.shutdown().await.unwrap();
    observer.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_outer_deadline_never_publishes_staged_groups() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let options = PoolOptions {
        transaction_timeout: std::time::Duration::from_millis(150),
        ..PoolOptions::default()
    };
    let store = PostgresEventStore::connect_local(&url(), "session_deadline", options)
        .await
        .unwrap();
    let staged = Arc::new(AtomicBool::new(false));
    let reached = staged.clone();
    let result: Result<(), _> = store
        .with_transaction(&tenant(), move |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                session
                    .append_group(&transaction_group(
                        &tenant,
                        "deadline",
                        &[("one", Expected::NoStream, 1)],
                    ))
                    .await?;
                reached.store(true, Ordering::Release);
                std::future::pending::<()>().await;
                Ok(())
            })
        })
        .await;
    assert!(
        staged.load(Ordering::Acquire),
        "deadline must occur after a complete group was staged"
    );
    assert!(matches!(result, Err(EventLogError::UnknownCommit)));
    store.shutdown().await.unwrap();
    let observer = PostgresEventStore::connect(&url(), "session_deadline")
        .await
        .unwrap();
    assert_eq!(
        observer
            .stream_version(&StreamId::new(tenant(), "item", "one").unwrap())
            .await
            .unwrap(),
        None
    );
    let replay = transaction_group(&tenant(), "deadline", &[("two", Expected::NoStream, 2)]);
    assert!(
        !eventlog_core::AtomicEventStore::append_group(&observer, &replay)
            .await
            .unwrap()
            .deduplicated
    );
    observer.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_cancelled_group_caught_by_the_callback_poisoned_the_outer_transaction() {
    use eventlog_core::{BoxFuture, ProjectionSpec, ProjectionStore, Projector, RecordedEvent};
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Pause(Arc<AtomicBool>);
    impl Projector for Pause {
        fn name(&self) -> &'static str {
            "pause_second"
        }
        fn projections(&self) -> &'static [ProjectionSpec] {
            &[]
        }
        fn apply<'a>(
            &'a self,
            event: &'a RecordedEvent,
            store: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                if event.stream_id == "two" {
                    assert!(
                        store
                            .get(&eventlog_conformance::DOCUMENTS, &event.tenant, "one")
                            .await?
                            .is_some(),
                        "the first group entry must really be staged"
                    );
                    self.0.store(true, Ordering::Release);
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    }
    let store = PostgresEventStore::connect(&url(), "session_cancel_group")
        .await
        .unwrap();
    let reached = Arc::new(AtomicBool::new(false));
    store
        .register_inline(Arc::new(eventlog_conformance::DocumentProjector))
        .await
        .unwrap();
    store
        .register_inline(Arc::new(Pause(reached.clone())))
        .await
        .unwrap();
    let result = store
        .with_transaction(&tenant(), |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                session.reserve_sequence("ids", 1).await?;
                let group = transaction_group(
                    &tenant,
                    "cancelled",
                    &[
                        ("one", Expected::NoStream, 1),
                        ("two", Expected::NoStream, 2),
                    ],
                );
                assert!(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        session.append_group(&group)
                    )
                    .await
                    .is_err()
                );
                Ok(())
            })
        })
        .await;
    assert!(
        reached.load(Ordering::Acquire),
        "timeout must happen after a real prefix was staged"
    );
    assert!(
        matches!(result, Err(EventLogError::Invalid(detail)) if detail.contains("unfinished session operation")),
        "caught cancellation cannot be converted to callback success"
    );
    assert_eq!(
        store
            .stream_version(&StreamId::new(tenant(), "item", "one").unwrap())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::DOCUMENTS, &tenant(), "one")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .with_transaction(&tenant(), |session| Box::pin(async move {
                session.reserve_sequence("ids", 1).await
            }))
            .await
            .unwrap(),
        0
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_cancelled_projection_write_cannot_escape_the_session_marker() {
    use eventlog_core::AtomicEventStore;
    use serde_json::json;
    let store = PostgresEventStore::connect(&url(), "session_cancel_view")
        .await
        .unwrap();
    store
        .register_inline(Arc::new(eventlog_conformance::DocumentProjector))
        .await
        .unwrap();
    store
        .append_group(&transaction_group(
            &tenant(),
            "seed",
            &[("held", Expected::NoStream, 1)],
        ))
        .await
        .unwrap();
    let (mut observer, driver) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(driver);
    let (ready, locked) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let blocker = tokio::spawn(async move {
        let held = observer.transaction().await.unwrap();
        held.query_one(
            "SELECT body FROM session_cancel_view_p_documents WHERE row_key='held' FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
        ready.send(()).unwrap();
        resume.await.unwrap();
        held.rollback().await.unwrap();
        drop(observer);
        driver.await.unwrap().unwrap();
    });
    locked.await.unwrap();
    let result = store
        .with_transaction(&tenant(), move |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                session
                    .append_group(&transaction_group(
                        &tenant,
                        "prefix",
                        &[("one", Expected::NoStream, 1)],
                    ))
                    .await?;
                let changed = json!({"value":99});
                let result = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    session.projections().upsert(
                        &eventlog_conformance::DOCUMENTS,
                        &tenant,
                        "held",
                        &changed,
                    ),
                )
                .await;
                assert!(result.is_err(), "assigned row lock must stall the write");
                release.send(()).unwrap();
                Ok(())
            })
        })
        .await;
    blocker.await.unwrap();
    assert!(
        matches!(result, Err(EventLogError::Invalid(detail)) if detail.contains("unfinished session operation")),
        "projection future cancellation lost the session marker"
    );
    assert_eq!(
        store
            .projection_get(&eventlog_conformance::DOCUMENTS, &tenant(), "held")
            .await
            .unwrap(),
        Some(json!({"value":1}))
    );
    assert_eq!(
        store
            .stream_version(&StreamId::new(tenant(), "item", "one").unwrap())
            .await
            .unwrap(),
        None
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_generation_capture_fences_redaction_until_commit_and_refuses_foreign_reads() {
    use eventlog_core::AtomicEventStore;
    let store = Arc::new(
        PostgresEventStore::connect(&url(), "session_generation")
            .await
            .unwrap(),
    );
    let stream = StreamId::new(tenant(), "item", "one").unwrap();
    store
        .append_group(&transaction_group(
            &tenant(),
            "seed",
            &[("one", Expected::NoStream, 1)],
        ))
        .await
        .unwrap();
    let generation = store.snapshot_generation(&stream).await.unwrap();
    let (ready, captured) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let resume = release.clone();
    let worker = store.clone();
    let reader = tokio::spawn(async move {
        worker
            .with_transaction(&tenant(), move |session| {
                Box::pin(async move {
                    let stream = StreamId::new(session.tenant().clone(), "item", "one").unwrap();
                    session.lock_stream(&stream).await?;
                    let held = session.snapshot_generation(&stream).await?;
                    assert_eq!(session.read_stream(&stream, 0, 10).await?.events.len(), 1);
                    ready.send(held).unwrap();
                    resume.notified().await;
                    Ok(())
                })
            })
            .await
    });
    assert_eq!(captured.await.unwrap(), generation);
    let redactor = store.clone();
    let redacted_stream = stream.clone();
    let (finished, mut redacted) = oneshot::channel();
    let erasure = tokio::spawn(async move {
        let result = redactor.redact(&redacted_stream, 1, "privacy").await;
        finished.send(()).unwrap();
        result
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut redacted)
            .await
            .is_err(),
        "redaction must wait for the captured generation"
    );
    release.notify_one();
    reader.await.unwrap().unwrap();
    redacted.await.unwrap();
    erasure.await.unwrap().unwrap();
    assert_ne!(
        store.snapshot_generation(&stream).await.unwrap(),
        generation
    );
    let result = store
        .with_transaction(&tenant(), |session| {
            Box::pin(async move {
                session
                    .read_stream(
                        &StreamId::new(TenantId::new("foreign").unwrap(), "item", "one").unwrap(),
                        0,
                        10,
                    )
                    .await
            })
        })
        .await;
    assert!(
        matches!(result, Err(EventLogError::Invalid(detail)) if detail.contains("cross tenant"))
    );
    store.shutdown().await.unwrap();
}
