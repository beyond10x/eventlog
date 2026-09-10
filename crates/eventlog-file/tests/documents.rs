#[tokio::test]
async fn document_queries_require_a_native_index_capability() {
    let directory = tempfile::tempdir().unwrap();
    let store = eventlog_file::FileEventStore::open(directory.path())
        .await
        .unwrap();
    eventlog_conformance::run_document_queries_unavailable(&store).await;
}
