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

/// The full-scan and VM steps of the malformed-coordinate probe a capture runs on one table.
fn malformed_probe_steps(sql: &Connection, table: &str, tenant: &TenantId) -> (bool, i32, i32) {
    let mut statement = sql
        .prepare(&format!(
            "SELECT EXISTS(SELECT 1 FROM {table}
             WHERE typeof(tenant_id)<>'text' AND CAST(tenant_id AS BLOB)=?1)"
        ))
        .expect("probe query");
    let found: bool = statement
        .query_row(params![tenant.as_str().as_bytes()], |row| row.get(0))
        .expect("probe");
    (
        found,
        statement.get_status(rusqlite::StatementStatus::FullscanStep),
        statement.get_status(rusqlite::StatementStatus::VmStep),
    )
}

/// One tenant's capture checked every tenant's identity row for a malformed coordinate: with
/// 8,192 healthy bindings the probe took 8,191 full-scan steps. A partial index holding only
/// malformed coordinates answers it with none, and still finds a malformed row.
#[tokio::test(flavor = "multi_thread")]
async fn the_malformed_coordinate_probe_does_not_scan_other_tenants_identities() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("store.db");
    let path = path.to_str().expect("UTF-8 path");
    let store = SqliteEventStore::open(path, PREFIX)
        .await
        .expect("opened store");
    let tenant = TenantId::new("capture-review-two-probe").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let sql = Connection::open(path).expect("fixture connection");
    let tx = sql.unchecked_transaction().expect("fixture transaction");
    for index in 0..8_191 {
        tx.execute(
            &format!("INSERT INTO {PREFIX}_identity(tenant_id,stream_identity) VALUES (?1,?2)"),
            params![format!("other-{index:05}"), format!("identity-{index:05}")],
        )
        .expect("healthy binding");
    }
    tx.commit().expect("fixture");

    let table = format!("{PREFIX}_identity");
    let (found, fullscan, steps) = malformed_probe_steps(&sql, &table, &tenant);
    eprintln!("identity probe over 8,192 bindings: fullscan_steps={fullscan} vm_steps={steps}");
    assert!(!found);
    assert_eq!(fullscan, 0, "the probe reads only malformed coordinates");
    store
        .capture_tenant(&tenant, &[], limits())
        .await
        .expect("a healthy tenant captures");

    let events = format!("{PREFIX}_events");
    let (_, fullscan, _) = malformed_probe_steps(&sql, &events, &tenant);
    assert_eq!(fullscan, 0, "the events probe is indexed the same way");

    sql.execute(
        &format!("UPDATE {table} SET tenant_id=CAST(tenant_id AS BLOB) WHERE tenant_id=?1"),
        params![tenant.as_str()],
    )
    .expect("a malformed coordinate");
    assert!(malformed_probe_steps(&sql, &table, &tenant).0);
    assert_eq!(
        store.capture_tenant(&tenant, &[], limits()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Identity,
        }),
        "the index keeps finding a malformed identity row"
    );
}
