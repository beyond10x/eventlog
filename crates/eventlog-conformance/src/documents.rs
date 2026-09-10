//! Applicable contract for providers with native document queries.
use std::sync::Arc;

use eventlog_core::{
    BoxFuture, EventLogError, EventStore, Expected, ProjectionQuery, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
};
use serde_json::{Value, json};

/// The generic JSON document fixture, retaining an ordinary scalar index too.
pub const DOCUMENTS: ProjectionSpec = ProjectionSpec {
    name: "documents",
    indexed: &["kind"],
};

/// A fold that checks its own transaction writes before returning.
pub struct DocumentProjector;
impl Projector for DocumentProjector {
    fn name(&self) -> &'static str {
        "documents"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        &[DOCUMENTS]
    }
    fn document_projections(&self) -> &'static [&'static str] {
        &["documents"]
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let stream = event.stream()?;
            let key = event
                .data
                .get("_key")
                .and_then(Value::as_str)
                .unwrap_or(stream.stream_id());
            store
                .upsert(&DOCUMENTS, stream.tenant(), key, &event.data)
                .await?;
            let query = ProjectionQuery {
                matching: event.data.clone(),
                prefix: None,
                after: None,
                limit: 100,
            };
            let rows = store
                .query_documents(&DOCUMENTS, stream.tenant(), &query)
                .await?;
            assert!(
                rows.rows
                    .iter()
                    .any(|(found, body)| found == key && body == &event.data),
                "projection must query its own uncommitted write"
            );
            let foreign = TenantId::new("foreign-document-tenant").unwrap();
            assert!(
                matches!(
                    store.query_documents(&DOCUMENTS, &foreign, &query).await,
                    Err(EventLogError::Invalid(_))
                ),
                "transaction query cannot cross tenant"
            );
            if event.data.get("refuse") == Some(&json!(true)) {
                return Err(EventLogError::Invalid(
                    "document fixture refuses after write".into(),
                ));
            }
            Ok(())
        })
    }
}

async fn append(
    store: &dyn EventStore,
    tenant: &TenantId,
    key: &str,
    mut body: Value,
) -> Result<(), EventLogError> {
    let stream = StreamId::new(
        tenant.clone(),
        "document",
        eventlog_core::request_hash(&json!(key))?,
    )
    .unwrap();
    body["_key"] = json!(key);
    let mut event = crate::event("document.updated", 1);
    event.data = body.clone();
    store
        .append(
            &stream,
            Expected::NoStream,
            &[event],
            &crate::meta(stream.stream_id(), &body),
        )
        .await?;
    Ok(())
}

/// Exercise containment, keyset boundaries, scalar compatibility and transactional rollback.
/// # Panics
/// Panics if a provider violates the document query contract.
pub async fn run_document_queries(store: &dyn EventStore) {
    store
        .register_inline(Arc::new(DocumentProjector))
        .await
        .unwrap();
    let tenant = TenantId::new("document-query").unwrap();
    for (key, body) in [
        (
            "p%_/a",
            json!({"kind":"selected", "nested":{"count":100,"flag":true}, "tags":[1,2,3], "nullable":null, "large":9_007_199_254_740_993_u64}),
        ),
        (
            "p%_/Z",
            json!({"kind":"selected", "nested":{"count":100.0,"flag":false}, "tags":[3,2], "large":9_007_199_254_740_992_u64}),
        ),
        (
            "p%_/ä",
            json!({"kind":"selected", "nested":{"count":100}, "nullable":false}),
        ),
        ("pXY/a", json!({"kind":"selected", "nested":{"count":100}})),
        ("p%_/", json!({"kind":"selected", "nested":{"count":100}})),
        ("p%_/b", json!({"kind":"other", "nested":{"count":99}})),
    ] {
        append(store, &tenant, key, body).await.unwrap();
    }
    append(
        store,
        &TenantId::new("other-document-query").unwrap(),
        "p%_/A",
        json!({"kind":"selected", "nested":{"count":100}}),
    )
    .await
    .unwrap();
    let mut query = ProjectionQuery {
        matching: json!({"kind":"selected", "nested":{"count":100.0}}),
        prefix: Some("p%_/".into()),
        after: None,
        limit: 2,
    };
    let first = store
        .projection_query(&DOCUMENTS, &tenant, &query)
        .await
        .unwrap();
    assert_eq!(
        first
            .rows
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        ["p%_/Z", "p%_/a"]
    );
    assert_eq!(first.next_cursor.as_deref(), Some("p%_/a"));
    query.after = first.next_cursor;
    let last = store
        .projection_query(&DOCUMENTS, &tenant, &query)
        .await
        .unwrap();
    assert_eq!(
        last.rows
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        ["p%_/ä"]
    );
    assert_eq!(last.next_cursor, None);
    query.after = Some("before-prefix".into());
    query.limit = 0;
    let bounded = store
        .projection_query(&DOCUMENTS, &tenant, &query)
        .await
        .unwrap();
    assert_eq!(bounded.rows.len(), 1);
    assert_eq!(bounded.next_cursor.as_deref(), Some("p%_/Z"));
    query.after = None;
    query.limit = usize::MAX;
    for matching in [
        json!({"nullable":null}),
        json!({"large":9_007_199_254_740_993_u64}),
        json!({"tags":[1,1]}),
        json!({"nested":{"flag":true}}),
    ] {
        query.matching = matching;
        let page = store
            .projection_query(&DOCUMENTS, &tenant, &query)
            .await
            .unwrap();
        assert_eq!(
            page.rows
                .iter()
                .map(|(k, _)| k.as_str())
                .collect::<Vec<_>>(),
            ["p%_/a"]
        );
        assert_eq!(page.next_cursor, None);
    }
    assert_eq!(
        store
            .projection_find(&DOCUMENTS, &tenant, "kind", "other", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        append(store, &tenant, "rolled-back", json!({"refuse":true}))
            .await
            .is_err()
    );
    query.prefix = None;
    query.matching = json!({"refuse":true});
    assert!(
        store
            .projection_query(&DOCUMENTS, &tenant, &query)
            .await
            .unwrap()
            .rows
            .is_empty(),
        "refusal must roll back the queried projection write"
    );
    let refused = StreamId::new(
        tenant,
        "document",
        eventlog_core::request_hash(&json!("rolled-back")).unwrap(),
    )
    .unwrap();
    assert!(
        store
            .read_stream(&refused, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty(),
        "refusal must roll back the event too"
    );
}

/// Providers without native document indexes must refuse rather than silently scan.
/// # Panics
/// Panics when an unsupported provider accepts the declared capability.
pub async fn run_document_queries_unavailable(store: &dyn EventStore) {
    assert!(matches!(
        store.register_inline(Arc::new(DocumentProjector)).await,
        Err(EventLogError::Invalid(_))
    ));
    let tenant = TenantId::new("document-query").unwrap();
    let query = ProjectionQuery {
        matching: json!({}),
        prefix: None,
        after: None,
        limit: 10,
    };
    assert!(matches!(
        store.projection_query(&DOCUMENTS, &tenant, &query).await,
        Err(EventLogError::Invalid(_))
    ));
}
