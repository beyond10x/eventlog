//! Adversary cases for `story:file-and-postgres-verify-each-content-once` on PostgreSQL.

use super::{content, prefix, tenant, url};
use crate::PostgresEventStore;
use eventlog_core::EventStore as _;
use std::time::Duration;

/// `verified_content.rs` promises that "a deletion or erasure through the handle leaves none of
/// its bytes here". `forget_tenant` forgets before its transaction commits; a read of the tenant
/// through the same handle while the erasure is in flight re-remembers the content, and the
/// erasure then commits with the erased tenant's bytes still held by the handle.
#[tokio::test]
async fn a_read_during_an_erasure_through_the_same_handle_leaves_erased_bytes_remembered() {
    let prefix = prefix("erase_race");
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.drop_tables().await.unwrap();
    let store = PostgresEventStore::connect(&url(), &prefix).await.unwrap();
    store.put_blob(&tenant(), "d", &content()).await.unwrap();

    let (sql, connection) = tokio_postgres::connect(&url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Hold the erasure at its blob DELETE; plain SELECTs are not blocked by SHARE.
    sql.batch_execute(&format!("BEGIN; LOCK TABLE {prefix}_blobs IN SHARE MODE"))
        .await
        .unwrap();

    let erased_tenant = tenant();
    let erase = store.forget_tenant(&erased_tenant);
    let racer = async {
        let mut waited = false;
        for _ in 0..100 {
            let waiting: i64 = sql
                .query_one(
                    "SELECT pg_stat_clear_snapshot(), count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'DELETE FROM %blobs%'",
                    &[],
                )
                .await
                .unwrap()
                .get(1);
            if waiting > 0 {
                waited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(waited, "the erasure reached its blob DELETE and waits");
        assert_eq!(
            store.get_blob(&tenant(), "d").await.unwrap(),
            Some(content()),
            "the uncommitted erasure is not visible yet"
        );
        sql.batch_execute("COMMIT").await.unwrap();
    };
    let (erased, ()) = tokio::join!(erase, racer);
    erased.unwrap();
    assert_eq!(
        store.get_blob(&tenant(), "d").await.unwrap(),
        None,
        "the tenant is erased"
    );
    assert_eq!(
        (store.verified.held(), store.verified.entries()),
        (0, 0),
        "an erasure through this handle leaves none of the erased bytes in its memory"
    );
    store.drop_tables().await.unwrap();
}
