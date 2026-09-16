use std::sync::Arc;

use eventlog_conformance::{CAPTURE_LEDGER, CaptureLedger};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventStore, Expected,
    NewEvent, ProjectionCaptureRefusal, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::{Connection, params};
use serde_json::json;

const PREFIX: &str = "revtwo";

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 8,
        max_blobs: 8,
        max_projection_rows: 8,
        max_payload_bytes: 8_192,
    }
}

/// A foreign index is physical-shape drift even when its quoted name cannot be interpolated into
/// a `PRAGMA index_info(...)` argument. The stored identifier must not turn a typed shape refusal
/// into an operational SQL error before capture compares it with the expected index name.
#[tokio::test(flavor = "multi_thread")]
async fn a_quoted_foreign_index_name_is_physical_shape_mismatch() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("store.db");
    let path = path.to_str().expect("UTF-8 path");
    let store = Arc::new(
        SqliteEventStore::open(path, PREFIX)
            .await
            .expect("opened store"),
    );
    let port: Arc<dyn EventStore> = store.clone();
    port.create_projections(Arc::new(CaptureLedger))
        .await
        .expect("created projection tables");
    let tenant = TenantId::new("capture-review-two-index").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");

    let sql = Connection::open(path).expect("foreign schema connection");
    sql.execute_batch(&format!(
        "DROP INDEX {PREFIX}_p_capture_ledger_idx_0;
         CREATE INDEX \"foreign-index\" ON {PREFIX}_p_capture_ledger(tenant_id,idx_0);"
    ))
    .expect("replaced the declared index with foreign shape");

    assert_eq!(
        store
            .capture_tenant(&tenant, &[CAPTURE_LEDGER], limits())
            .await
            .expect_err("a foreign index is not the declared projection shape"),
        CaptureError::ProjectionUnavailable {
            projection: "capture_ledger".to_owned(),
            reason: ProjectionCaptureRefusal::PhysicalShapeMismatch,
        }
    );
}

/// SQLite does not consider a BLOB coordinate equal to the text parameter for the same bytes. A
/// capture must detect that malformed owned event rather than silently return an empty history.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_text_event_tenant_coordinate_is_corruption_not_absence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("store.db");
    let path = path.to_str().expect("UTF-8 path");
    let store = SqliteEventStore::open(path, PREFIX)
        .await
        .expect("opened store");
    let tenant = TenantId::new("capture-review-two-owner").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("valid stream");
    store
        .append(
            &stream,
            Expected::NoStream,
            &[NewEvent::new("item.received", 1, json!({ "value": 1 })).expect("valid event")],
            &eventlog_conformance::meta("review-two-event", &json!({})),
        )
        .await
        .expect("appended event");

    let sql = Connection::open(path).expect("foreign storage connection");
    assert_eq!(
        sql.execute(
            &format!(
                "UPDATE {PREFIX}_events SET tenant_id=CAST(tenant_id AS BLOB)
                 WHERE tenant_id=?1"
            ),
            params![tenant.as_str()],
        )
        .expect("changed only the coordinate storage class"),
        1
    );
    let stored: (String, Vec<u8>) = sql
        .query_row(
            &format!("SELECT typeof(tenant_id),CAST(tenant_id AS BLOB) FROM {PREFIX}_events"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stored event coordinate");
    assert_eq!(stored.0, "blob", "the fixture must evade TEXT equality");
    assert_eq!(stored.1, tenant.as_str().as_bytes());

    assert_eq!(
        store.capture_tenant(&tenant, &[], limits()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Event,
        }),
        "a malformed row with this tenant's exact bytes must not disappear from complete history"
    );
}
