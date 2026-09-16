//! Source-examination cases for SQLite's consistent tenant capture.
//!
//! Added by the first source examination of `feat: capture complete tenant state consistently
//! across providers`. Both cases drive one clause of
//! `docs/design/consistent-tenant-capture.md`: "Every reported limit must actually have been
//! exceeded", against the two places the new capture path decides a cap from stored material it
//! has not yet validated. Nothing here edits an implementation file or an existing case.

use std::{sync::Arc, time::Duration};

use eventlog_conformance::{CAPTURE_SIDECAR, CaptureLedger};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, CatchUpRunner, ConsistentTenantCapture,
    EventStore, Expected, NewEvent, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::{Connection, params};
use serde_json::json;

/// A prefix of this file's own, distinct from the admitted `consistent_capture` case's `cap`.
///
/// The provider's contract is `crates/eventlog-sqlite/src/lib.rs:3008`: non-empty, at most 32
/// bytes, and every byte lowercase ASCII or an underscore. A digit is refused, which is what the
/// first execution of these cases reported. Isolation between runs comes from the temporary
/// database file, so the prefix itself is fixed.
const PREFIX: &str = "revone";

/// Caps far above anything these fixtures hold, so a crossed cap is never the honest answer.
fn bounded() -> CaptureLimits {
    CaptureLimits {
        max_events: 256,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 1 << 22,
    }
}

fn database(directory: &tempfile::TempDir) -> String {
    directory
        .path()
        .join("store.db")
        .to_str()
        .expect("utf-8 path")
        .to_owned()
}

async fn opened(path: &str) -> SqliteEventStore {
    SqliteEventStore::open(path, PREFIX)
        .await
        .expect("opened store")
}

fn appended(key: &str, value: i64) -> NewEvent {
    NewEvent::new("item.received", 1, json!({ "key": key, "value": value }))
        .expect("fixture event is valid")
}

/// A stored `row_key` that is not text must not be answered with a crossed row cap.
///
/// `read_rows` resumes with `row_key > ?2`, binding the previous page's last key as text. SQLite
/// orders storage classes before values, so a `BLOB` key is greater than every text key and than
/// any text bound as the resume point. The examination's reading is that such a row is re-selected
/// on every page, and that what the caller is handed is `LimitExceeded` for a row cap the tenant's
/// four rows do not cross. The design's answer for stored material no writer here produced is
/// corruption, and its rule for a limit is that the content must actually have crossed it.
///
/// The cap keeps the probe bounded: if the reading is right, the loop ends at the cap rather than
/// running forever. The timeout is only there so a wrong reading is reported instead of hanging.
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_row_key_storage_class_is_not_a_crossed_row_cap() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let concrete = Arc::new(opened(&path).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("sqlite-review-one-rowkey").expect("valid tenant");
    port.stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let runner = CatchUpRunner::new(Arc::clone(&port), Arc::new(CaptureLedger))
        .await
        .expect("declared capture projections");
    for index in 0..3_i64 {
        port.append(
            &StreamId::new(tenant.clone(), "item", "one").expect("valid stream"),
            Expected::Any,
            &[appended(&format!("k{index}"), index)],
            &eventlog_conformance::meta(&format!("row-{index}"), &json!({})),
        )
        .await
        .expect("appended");
    }
    eventlog_conformance::drain_at_least(&runner, &tenant, 3).await;
    // The control: three rows, well inside every cap, before anything foreign is stored.
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_SIDECAR], bounded())
            .await
            .expect("complete observation")
            .projections[0]
            .rows
            .len(),
        3
    );

    let sql = Connection::open(&path).expect("second connection");
    sql.execute(
        &format!(
            "INSERT INTO {PREFIX}_p_capture_sidecar (tenant_id,row_key,body)
             VALUES (?1, x'61', '{{\"foreign\":true}}')"
        ),
        params![tenant.as_str()],
    )
    .expect("stored a row this kit did not write");
    // What SQLite actually put in the column, measured rather than assumed.
    let class: String = sql
        .query_row(
            &format!(
                "SELECT typeof(row_key) FROM {PREFIX}_p_capture_sidecar
                 WHERE tenant_id=?1 AND CAST(row_key AS BLOB)=x'61'"
            ),
            params![tenant.as_str()],
            |row| row.get(0),
        )
        .expect("stored row key");
    assert_eq!(
        class, "blob",
        "the fixture only means something while the column holds a storage class TEXT affinity \
         does not convert"
    );
    let stored: i64 = sql
        .query_row(
            &format!("SELECT COUNT(*) FROM {PREFIX}_p_capture_sidecar WHERE tenant_id=?1"),
            params![tenant.as_str()],
            |row| row.get(0),
        )
        .expect("stored row count");
    assert_eq!(stored, 4, "four rows, against a cap of 64");

    let observed = tokio::time::timeout(
        Duration::from_secs(60),
        concrete.capture_tenant(&tenant, &[CAPTURE_SIDECAR], bounded()),
    )
    .await
    .expect("the capture terminated");
    assert!(
        !matches!(observed, Err(CaptureError::LimitExceeded { .. })),
        "four stored rows cannot cross a cap of 64; a reported limit must actually have been \
         exceeded: {observed:?}"
    );
}

/// An overstated stored blob length is corruption, not a crossed payload cap.
///
/// `preflight` proves the payload cap from `SUM(byte_count)` before any blob is decoded, so the
/// column decides the refusal before `validate_stored_blob` ever compares it against the bytes.
/// The design calls a malformed length corruption, and requires that a reported limit is one the
/// content actually crossed. The tenant's one blob is eleven bytes.
#[tokio::test(flavor = "multi_thread")]
async fn an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = database(&directory);
    let concrete = Arc::new(opened(&path).await);
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("sqlite-review-one-length").expect("valid tenant");
    port.stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    port.put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    // The control: the real content is eleven bytes and fits every cap here.
    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[], bounded())
            .await
            .expect("complete observation")
            .blobs[0]
            .bytes,
        b"bound-bytes".to_vec()
    );

    let sql = Connection::open(&path).expect("second connection");
    let overstated = 1_099_511_627_776_i64;
    let changed = sql
        .execute(
            &format!("UPDATE {PREFIX}_blobs SET byte_count=?2 WHERE tenant_id=?1"),
            params![tenant.as_str(), overstated],
        )
        .expect("overstated the stored length");
    assert_eq!(
        changed, 1,
        "the fixture only means something while one stored length is a lie"
    );

    assert_eq!(
        concrete.capture_tenant(&tenant, &[], bounded()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a malformed stored length is corruption; the eleven bytes this tenant holds cross no cap"
    );
}
