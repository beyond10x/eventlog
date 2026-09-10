#[tokio::test]
async fn committed_inventory_preserves_scope_paging_and_erasure() {
    let directory = tempfile::tempdir().unwrap();
    let store = eventlog_file::FileEventStore::open(directory.path())
        .await
        .unwrap();
    eventlog_conformance::run_stream_inventory(&store).await;
}
