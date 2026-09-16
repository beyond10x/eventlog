//! Source-examination cases for PostgreSQL's consistent tenant capture.
//!
//! Added by the first source examination of `feat: capture complete tenant state consistently
//! across providers`. The first case drives "Every reported limit must actually have been
//! exceeded" against the preflight that decides the payload cap from a stored column it has not
//! validated. The second pins the exactly-one-constraint branch of physical shape admission, which
//! is the branch a catalog that reports more constraints than a primary key would fall out of.
//! Nothing here edits an implementation file or an existing case.

use std::sync::Arc;

use eventlog_conformance::{CAPTURE_SIDECAR, CaptureLedger};
use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventStore,
    ProjectionCaptureRefusal, TenantId,
};
use eventlog_postgres::PostgresEventStore;
use tokio_postgres::NoTls;

fn url() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL")
}

/// A prefix isolated per run, built the way the admitted `consistent_capture` cases build theirs.
///
/// The provider's contract is `crates/eventlog-postgres/src/lib.rs:1712`: non-empty, at most 32
/// bytes, every byte lowercase ASCII or an underscore, and neither `eventlog_expected` nor
/// `eventlog_rebuild`. The nanosecond stamp is mapped digit by digit onto letters for exactly that
/// reason — a digit is refused, which is what the first execution of these cases reported. The
/// assertion is here so a fixture that outgrows the contract says so itself.
fn prefix(label: &str) -> String {
    let suffix: String = time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .to_string()
        .bytes()
        .map(|digit| char::from(b'a' + digit - b'0'))
        .collect();
    let prefix = format!("rev_{label}_{suffix}");
    assert!(
        !prefix.is_empty()
            && prefix.len() <= 32
            && prefix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
        "the fixture's own prefix must satisfy the provider's contract: {prefix}"
    );
    prefix
}

/// Caps far above anything these fixtures hold, so a crossed cap is never the honest answer.
fn bounded() -> CaptureLimits {
    CaptureLimits {
        max_events: 256,
        max_blobs: 64,
        max_projection_rows: 64,
        max_payload_bytes: 1 << 22,
    }
}

/// A raw session in the same schema the store uses.
async fn sql() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&url(), NoTls)
        .await
        .expect("fixture session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO public")
        .await
        .expect("same schema as the store");
    client
}

/// An overstated stored blob length is corruption, not a crossed payload cap.
///
/// `preflight` proves the payload cap from `SUM(byte_count)` inside the snapshot, before any blob
/// is decoded, so the column decides the refusal before `validate_stored_blob` ever compares it
/// against the bytes. The design calls a malformed length corruption, and requires that a reported
/// limit is one the content actually crossed. The tenant's one blob is eleven bytes.
#[tokio::test]
async fn an_overstated_stored_blob_length_is_corruption_not_a_crossed_payload_cap() {
    let prefix = prefix("length");
    let store = PostgresEventStore::connect(&url(), &prefix)
        .await
        .expect("connected");
    let tenant = TenantId::new("postgres-review-one-length").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    // The control: the real content is eleven bytes and fits every cap here.
    assert_eq!(
        store
            .capture_tenant(&tenant, &[], bounded())
            .await
            .expect("complete observation")
            .blobs[0]
            .bytes,
        b"bound-bytes".to_vec()
    );

    let fixture = sql().await;
    let overstated = 1_099_511_627_776_i64;
    let changed = fixture
        .execute(
            &format!("UPDATE {prefix}_blobs SET byte_count=$2 WHERE tenant_id=$1"),
            &[&tenant.as_str(), &overstated],
        )
        .await
        .expect("overstated the stored length");
    assert_eq!(
        changed, 1,
        "the fixture only means something while one stored length is a lie"
    );

    assert_eq!(
        store.capture_tenant(&tenant, &[], bounded()).await,
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a malformed stored length is corruption; the eleven bytes this tenant holds cross no cap"
    );
}

/// A catalog that reports a constraint this kit did not create is a different table.
///
/// Physical shape admission requires the projection table to carry exactly one constraint, and for
/// it to be the primary key. This case stores one more and requires the refusal.
#[tokio::test]
async fn a_constraint_this_kit_did_not_create_refuses_physical_shape() {
    let prefix = prefix("shape");
    let concrete = Arc::new(
        PostgresEventStore::connect(&url(), &prefix)
            .await
            .expect("connected"),
    );
    let port: Arc<dyn EventStore> = concrete.clone();
    let tenant = TenantId::new("postgres-review-one-shape").expect("valid tenant");
    port.stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    port.create_projections(Arc::new(CaptureLedger))
        .await
        .expect("declared capture projections");
    // The control: the table this kit created is admitted before anything is added to it.
    concrete
        .capture_tenant(&tenant, &[CAPTURE_SIDECAR], bounded())
        .await
        .expect("complete observation");

    let fixture = sql().await;
    fixture
        .batch_execute(&format!(
            "ALTER TABLE {prefix}_p_capture_sidecar
             ADD CONSTRAINT review_one_extra CHECK (row_key <> '')"
        ))
        .await
        .expect("added a constraint nobody declared");

    assert_eq!(
        concrete
            .capture_tenant(&tenant, &[CAPTURE_SIDECAR], bounded())
            .await
            .expect_err("a table carrying a rule this kit did not write is a different table"),
        CaptureError::ProjectionUnavailable {
            projection: CAPTURE_SIDECAR.name.to_owned(),
            reason: ProjectionCaptureRefusal::PhysicalShapeMismatch
        }
    );
}
