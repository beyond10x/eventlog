use std::sync::Arc;

use eventlog_core::{EventStore, Expected, ProjectionQuery, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use serde_json::json;

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned disposable PostgreSQL URL")
}

#[tokio::test]
async fn document_queries_preserve_containment_paging_and_transaction_scope() {
    let store = PostgresEventStore::connect(&url(), "document_contract")
        .await
        .unwrap();
    eventlog_conformance::run_document_queries(&store).await;
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn inline_documents_remain_visible_while_an_unrelated_transaction_withholds_feed() {
    let store = PostgresEventStore::connect(&url(), "document_watermark")
        .await
        .unwrap();
    store
        .register_inline(Arc::new(eventlog_conformance::DocumentProjector))
        .await
        .unwrap();
    let (mut client, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let transaction = client.transaction().await.unwrap();
    transaction
        .simple_query("SELECT pg_current_xact_id()")
        .await
        .unwrap();
    let tenant = TenantId::new("document-watermark").unwrap();
    let stream = StreamId::new(tenant.clone(), "document", "one").unwrap();
    let mut event = eventlog_conformance::event("document.updated", 1);
    event.data = json!({"kind":"ready"});
    store
        .append(
            &stream,
            Expected::NoStream,
            &[event],
            &eventlog_conformance::meta("one", &json!({})),
        )
        .await
        .unwrap();
    assert!(
        store
            .read_feed(&tenant, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty(),
        "the fixture must actually withhold the feed"
    );
    let query = ProjectionQuery {
        matching: json!({"kind":"ready"}),
        prefix: None,
        after: None,
        limit: 10,
    };
    let page = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.projection_query(&eventlog_conformance::DOCUMENTS, &tenant, &query),
    )
    .await
    .expect("committed inline queries must not await the feed")
    .unwrap();
    assert_eq!(page.rows, vec![("one".into(), json!({"kind":"ready"}))]);
    transaction.rollback().await.unwrap();
    drop(client);
    driver.await.unwrap().unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn transaction_queries_refuse_a_lagging_projection() {
    use eventlog_core::{BoxFuture, EventLogError, Guard, ProjectionStore};
    struct ReadCatchUp(TenantId);
    impl Guard for ReadCatchUp {
        fn check<'a>(
            &'a self,
            store: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                let query = ProjectionQuery {
                    matching: json!({}),
                    prefix: None,
                    after: None,
                    limit: 10,
                };
                store
                    .query_documents(&eventlog_conformance::DOCUMENTS, &self.0, &query)
                    .await?;
                Ok(())
            })
        }
    }
    let store = PostgresEventStore::connect(&url(), "document_catchup")
        .await
        .unwrap();
    store
        .create_projections(Arc::new(eventlog_conformance::DocumentProjector))
        .await
        .unwrap();
    let tenant = TenantId::new("document-catchup").unwrap();
    let stream = StreamId::new(tenant.clone(), "document", "one").unwrap();
    let result = store
        .append_guarded(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("document.updated", 1)],
            &eventlog_conformance::meta("one", &json!({})),
            Arc::new(ReadCatchUp(tenant)),
        )
        .await;
    assert!(
        matches!(result, Err(EventLogError::Invalid(_))),
        "a guard cannot accept a lagging document view"
    );
    assert!(
        store
            .read_stream(&stream, 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    store.shutdown().await.unwrap();
}
