#[tokio::test]
async fn document_queries_require_a_native_index_capability() {
    let store = eventlog_sqlite::SqliteEventStore::in_memory("document_refusal")
        .await
        .unwrap();
    eventlog_conformance::run_document_queries_unavailable(&store).await;
}
