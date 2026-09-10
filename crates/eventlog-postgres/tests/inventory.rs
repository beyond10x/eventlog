use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use serde_json::json;

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned disposable PostgreSQL URL")
}

#[tokio::test]
async fn committed_inventory_preserves_scope_paging_and_erasure() {
    let store = PostgresEventStore::connect(&url(), "inventory_contract")
        .await
        .unwrap();
    eventlog_conformance::run_stream_inventory(&store).await;
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn committed_inventory_is_visible_while_an_unrelated_transaction_withholds_the_feed() {
    let store = PostgresEventStore::connect(&url(), "inventory_watermark")
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
    let tenant = TenantId::new("inventory-watermark").unwrap();
    let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
    store
        .append(
            &stream,
            Expected::NoStream,
            &[eventlog_conformance::event("item.created", 1)],
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
        "the assigned older transaction must actually withhold the feed"
    );
    let inventory = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.list_streams(&tenant, "item", None, 10),
    )
    .await
    .expect("inventory must not wait for the feed")
    .unwrap();
    assert_eq!(
        inventory,
        vec![stream],
        "a committed stream must be immediately enumerable"
    );
    transaction.rollback().await.unwrap();
    drop(client);
    driver.await.unwrap().unwrap();
    store.drop_tables().await.unwrap();
    store.shutdown().await.unwrap();
}
