#[tokio::test]
async fn committed_inventory_preserves_scope_paging_and_erasure() {
    let store = eventlog_sqlite::SqliteEventStore::in_memory("inventory")
        .await
        .unwrap();
    eventlog_conformance::run_stream_inventory(&store).await;
}
