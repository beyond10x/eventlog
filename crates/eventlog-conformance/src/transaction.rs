//! Applicable contract for native caller-owned transactions.
use eventlog_core::{
    AppendGroup, EventLogError, Expected, ProjectionQuery, StreamAppend, StreamId, TenantId,
    TransactionalEventStore,
};
use serde_json::json;
use std::sync::Arc;

/// Build an opaque fixture group whose actual content binds its durable retry identity.
/// # Panics
/// Panics if fixture identifiers become invalid.
pub fn transaction_group(
    tenant: &TenantId,
    key: &str,
    entries: &[(&str, Expected, i64)],
) -> AppendGroup {
    AppendGroup {
        tenant: tenant.clone(),
        meta: crate::meta(key, &json!({"key":key})),
        appends: entries
            .iter()
            .map(|(id, expected, by)| StreamAppend {
                stream: StreamId::new(tenant.clone(), "item", *id).unwrap(),
                expected: *expected,
                events: vec![crate::event("item.changed", *by)],
            })
            .collect(),
    }
}

/// Prove own reads, group savepoint rollback, durable retries, outer rollback and sequence scope.
/// # Panics
/// Panics if a provider violates the native session contract.
pub async fn run_transaction_sessions<S: TransactionalEventStore>(store: &S) {
    store
        .register_inline(Arc::new(crate::DocumentProjector))
        .await
        .unwrap();
    let tenant = TenantId::new("session-tenant").unwrap();
    store
        .with_transaction(&tenant, |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                let one = StreamId::new(tenant.clone(), "item", "one").unwrap();
                session.lock_identity("logical", "absent").await?;
                session.lock_stream(&one).await?;
                assert!(session.snapshot_generation(&one).await?.is_some());
                assert!(session.read_stream(&one, 0, 10).await?.events.is_empty());
                assert_eq!(session.reserve_sequence("ids", 3).await?, 0);
                let failed = transaction_group(
                    &tenant,
                    "first-group",
                    &[
                        ("one", Expected::NoStream, 1),
                        ("two", Expected::Exact(99), 2),
                    ],
                );
                assert!(matches!(
                    session.append_group(&failed).await,
                    Err(EventLogError::Conflict { .. })
                ));
                assert_eq!(
                    session.stream_version(&one).await?,
                    None,
                    "caught group failure must remove its first append"
                );
                let accepted = transaction_group(
                    &tenant,
                    "first-group",
                    &[
                        ("one", Expected::NoStream, 1),
                        ("two", Expected::NoStream, 2),
                    ],
                );
                let first = session.append_group(&accepted).await?;
                let again = session.append_group(&accepted).await?;
                assert!(
                    again.deduplicated,
                    "own staged receipt must resolve without another write"
                );
                assert_eq!(first.appends[0].events, again.appends[0].events);
                assert_eq!(
                    session.read_stream(&one, 0, 1).await?.events,
                    first.appends[0].events
                );
                assert_eq!(session.list_streams("item", None, 10).await?.len(), 2);
                let query = ProjectionQuery {
                    matching: json!({"value":2}),
                    prefix: None,
                    after: None,
                    limit: 10,
                };
                let page = session
                    .projections()
                    .query_documents(&crate::DOCUMENTS, &tenant, &query)
                    .await?;
                assert_eq!(page.rows.len(), 1);
                assert_eq!(page.rows[0].0, "two");
                assert_eq!(session.reserve_sequence("ids", 2).await?, 3);
                Ok(())
            })
        })
        .await
        .unwrap();
    let one = StreamId::new(tenant.clone(), "item", "one").unwrap();
    assert_eq!(store.stream_version(&one).await.unwrap(), Some(1));
    let aborted: Result<(), _> = store
        .with_transaction(&tenant, |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                assert_eq!(session.reserve_sequence("ids", 1).await?, 5);
                session
                    .append_group(&transaction_group(
                        &tenant,
                        "discard",
                        &[("discarded", Expected::NoStream, 4)],
                    ))
                    .await?;
                Err(EventLogError::GuardRefused {
                    code: "outer_refused".into(),
                })
            })
        })
        .await;
    assert!(
        matches!(aborted, Err(EventLogError::GuardRefused { code }) if code == "outer_refused")
    );
    let discarded = StreamId::new(tenant.clone(), "item", "discarded").unwrap();
    assert_eq!(store.stream_version(&discarded).await.unwrap(), None);
    assert_eq!(
        store
            .projection_get(&crate::DOCUMENTS, &tenant, "discarded")
            .await
            .unwrap(),
        None
    );
    store
        .with_transaction(&tenant, |session| {
            Box::pin(async move {
                let tenant = session.tenant().clone();
                assert_eq!(
                    session.reserve_sequence("ids", 1).await?,
                    5,
                    "outer refusal must roll back sequence reservations"
                );
                let replacement = transaction_group(
                    &tenant,
                    "discard",
                    &[("replacement", Expected::NoStream, 8)],
                );
                assert!(
                    !session.append_group(&replacement).await?.deduplicated,
                    "outer refusal must roll back durable group identity too"
                );
                assert_eq!(
                    session
                        .reserve_sequence("maximum", i64::MAX.cast_unsigned())
                        .await?,
                    0
                );
                assert!(matches!(
                    session.reserve_sequence("maximum", 1).await,
                    Err(EventLogError::Invalid(_))
                ));
                assert_eq!(session.reserve_sequence("after-refusal", 1).await?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
    let other = TenantId::new("session-tenant-extra").unwrap();
    for tenant in [&other, &tenant] {
        let prior = store
            .with_transaction(tenant, |session| {
                Box::pin(async move { session.reserve_sequence("isolated", 1).await })
            })
            .await
            .unwrap();
        assert_eq!(prior, 0);
    }
    store.forget_tenant(&tenant).await.unwrap();
    assert_eq!(
        store
            .with_transaction(&tenant, |session| Box::pin(async move {
                session.reserve_sequence("ids", 1).await
            }))
            .await
            .unwrap(),
        0,
        "erasure removes q sequence coordinates"
    );
    assert_eq!(
        store
            .with_transaction(&other, |session| Box::pin(async move {
                session.reserve_sequence("isolated", 1).await
            }))
            .await
            .unwrap(),
        1,
        "erasure cannot affect a longer neighboring tenant coordinate"
    );
}
