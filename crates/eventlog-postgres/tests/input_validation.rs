//! Public input boundaries run the same atomicity checks on the real PostgreSQL backend.
use eventlog_core::EventStore;
use eventlog_postgres::PostgresEventStore;
use std::sync::Arc;

#[tokio::test]
async fn public_input_validation_is_atomic_on_postgresql() {
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    let prefix = format!("input_{suffix}");
    let adapter = Arc::new(
        PostgresEventStore::connect(&url, &prefix)
            .await
            .expect("fixture"),
    );
    let store: Arc<dyn EventStore> = adapter.clone();
    eventlog_conformance::run_public_input_validation(&store).await;
    adapter.shutdown().await.expect("shutdown");
}
