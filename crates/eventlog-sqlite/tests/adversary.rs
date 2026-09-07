//! Old-file erasure compatibility at the new projection registry boundary.
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_sqlite::SqliteEventStore;
use std::sync::Arc;

#[tokio::test]
async fn legacy_projection_rows_are_erased_without_reregistering_retired_projectors() {
    let directory = tempfile::tempdir().expect("assigned TMPDIR");
    let database = directory.path().join("legacy.sqlite");
    let path = database.to_str().expect("path");
    let tenant = TenantId::new("retired-projection-tenant").expect("tenant");
    {
        let original = SqliteEventStore::open(path, "legacy_erasure")
            .await
            .expect("source file");
        original
            .register_inline(Arc::new(eventlog_conformance::Tally))
            .await
            .expect("historical projector");
        original
            .append(
                &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("one", &serde_json::json!({})),
            )
            .await
            .expect("historical state");
    }
    {
        let sql = rusqlite::Connection::open(&database).expect("old physical schema fixture");
        // Only the two newly added tables are removed; old event/view bytes are retained.
        sql.execute_batch("DROP TABLE IF EXISTS legacy_erasure_scope_counters; DROP TABLE IF EXISTS legacy_erasure_projection_registry").expect("exact old table roster");
    }
    let reopened = SqliteEventStore::open(path, "legacy_erasure")
        .await
        .expect("supported old-file reopen");
    assert!(
        reopened
            .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
            .await
            .expect("old reader")
            .is_some()
    );
    reopened
        .forget_tenant(&tenant)
        .await
        .expect("tenant erasure");
    let retained = reopened
        .projection_get(&eventlog_conformance::TALLY, &tenant, "item/one")
        .await
        .expect("retired projection remains readable");
    eprintln!("retained_projection_after_successful_erasure={retained:?}");
    assert!(
        retained.is_none(),
        "tenant erasure left old projection content because the newly created registry was empty"
    );
}
