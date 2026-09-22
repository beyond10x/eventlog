//! Independent review, first source pass, of inline projection administration at 43ceaa09.
//!
//! The design's "Rebuild admission and snapshot" section requires the rebuild to "Validate the
//! same stored event/envelope/numeric invariants as native capture, without filtering malformed
//! rows." Native capture enforces `validate_captured_order`/`validate_captured_event` in every
//! provider (`eventlog-sqlite/src/capture.rs:132`). The new inline rebuild reads events with
//! `read_event` and applies them with no such check, so a durable event that capture refuses as
//! corrupt is admitted and published by rebuild.

use std::sync::Arc;

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, ConsistentTenantCapture, EventStore, Expected,
    InlineProjectionAdmin, Projector, StreamId, TenantId,
};
use eventlog_sqlite::SqliteEventStore;
use rusqlite::OptionalExtension;

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_events: 4096,
        max_blobs: 256,
        max_projection_rows: 4096,
        max_payload_bytes: 1 << 22,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_admits_a_stored_event_that_capture_refuses_as_corrupt() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("review.sqlite");
    let path = path.to_str().expect("utf-8 path");
    let prefix = "inline_admin_review_one";
    let tenant = TenantId::new("review-owner").expect("tenant");

    // A store with one admitted projector and one valid committed event.
    {
        let store = SqliteEventStore::open(path, prefix).await.expect("opened");
        store
            .create_projections(Arc::new(eventlog_conformance::AdminProjector::default()))
            .await
            .expect("admitted tables");
        store
            .append(
                &StreamId::new(tenant.clone(), "item", "one").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.recorded", 1)],
                &eventlog_conformance::meta("review", &serde_json::json!({})),
            )
            .await
            .expect("valid history");
    }

    // Tamper the committed payload into a non-object, exactly the durable corruption
    // `validate_captured_event` names (`!event.data.is_object()`), through a separate connection.
    // Nothing here touches production source.
    {
        let connection = rusqlite::Connection::open(path).expect("inspection connection");
        let changed = connection
            .execute(
                &format!("UPDATE {prefix}_events SET data = '[1,2,3]' WHERE tenant_id = ?1"),
                rusqlite::params![tenant.as_str()],
            )
            .expect("tamper one stored payload");
        assert_eq!(changed, 1, "exactly one committed event was tampered");
    }

    let store = Arc::new(
        SqliteEventStore::open(path, prefix)
            .await
            .expect("reopened"),
    );
    let projector = Arc::new(eventlog_conformance::AdminProjector::default());
    store
        .attach_inline_existing(projector.clone())
        .await
        .expect("attached admitted projector");

    // Native capture refuses the tampered history.
    let captured =
        ConsistentTenantCapture::capture_tenant(store.as_ref(), &tenant, &[], limits()).await;
    assert!(
        captured.is_err(),
        "capture must refuse a non-object stored payload, but returned Ok"
    );

    // The design requires the rebuild to validate the same invariants and not filter malformed
    // rows. It must therefore also refuse this tenant's history rather than publish rows derived
    // from it.
    let rebuilt = store
        .rebuild_inline_projection(projector.name(), &tenant)
        .await;
    assert!(
        rebuilt.is_err(),
        "rebuild admitted an event that capture refuses as corrupt: {rebuilt:?}"
    );
}

fn published(
    connection: &rusqlite::Connection,
    prefix: &str,
    tenant: &TenantId,
) -> (Vec<(String, String, String)>, Option<i64>) {
    let mut rows = Vec::new();
    for name in ["admin_ledger", "admin_sidecar"] {
        let mut statement = connection
            .prepare(&format!(
                "SELECT row_key,body FROM {prefix}_p_{name} WHERE tenant_id=?1 ORDER BY row_key"
            ))
            .expect("active projection query");
        rows.extend(
            statement
                .query_map([tenant.as_str()], |row| {
                    Ok((name.to_owned(), row.get(0)?, row.get(1)?))
                })
                .expect("active projection rows")
                .map(|row| row.expect("active projection value")),
        );
    }
    let cursor = connection
        .query_row(
            &format!(
                "SELECT global_seq FROM {prefix}_projection_cursors \
                 WHERE projection='admin_projector' AND tenant_id=?1"
            ),
            [tenant.as_str()],
            |row| row.get(0),
        )
        .optional()
        .expect("cursor query");
    (rows, cursor)
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_envelope_or_stream_order_cannot_replace_rows_or_cursor() {
    for corruption in [
        "non_object_data",
        "stream_version_gap",
        "zero_global_seq",
        "negative_global_seq",
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("corrupt-history.sqlite");
        let path = path.to_str().expect("utf-8 path");
        let prefix = "inline_admin_corrupt_history";
        let owner = TenantId::new("corrupt-owner").expect("owner");
        let neighbour = TenantId::new("unrelated-owner").expect("neighbour");
        let store = SqliteEventStore::open(path, prefix).await.expect("opened");
        let projector = Arc::new(eventlog_conformance::AdminProjector::default());
        store
            .create_projections(projector.clone())
            .await
            .expect("admitted tables");
        store.stream_identity(&owner).await.expect("owner identity");
        store
            .append(
                &StreamId::new(owner.clone(), "item", "one").expect("stream"),
                Expected::NoStream,
                &[
                    eventlog_conformance::event("item.recorded", 1),
                    eventlog_conformance::event("item.recorded", 2),
                ],
                &eventlog_conformance::meta("owner", &serde_json::json!({})),
            )
            .await
            .expect("owner history");
        store
            .append(
                &StreamId::new(neighbour.clone(), "item", "other").expect("stream"),
                Expected::NoStream,
                &[eventlog_conformance::event("item.recorded", 3)],
                &eventlog_conformance::meta("neighbour", &serde_json::json!({})),
            )
            .await
            .expect("unrelated history");
        drop(store);
        let store = SqliteEventStore::open(path, prefix)
            .await
            .expect("reopened");
        store
            .attach_inline_existing(projector.clone())
            .await
            .expect("attached");
        store
            .rebuild_inline_projection(projector.name(), &owner)
            .await
            .expect("first owner publication");
        store
            .rebuild_inline_projection(projector.name(), &neighbour)
            .await
            .expect("unrelated publication");

        let connection = rusqlite::Connection::open(path).expect("inspection connection");
        let owner_before = published(&connection, prefix, &owner);
        let neighbour_before = published(&connection, prefix, &neighbour);
        assert!(!owner_before.0.is_empty(), "fixture has published rows");
        assert!(owner_before.1.is_some(), "fixture has a cursor");
        let mutation = match corruption {
            "non_object_data" => "data='[1,2,3]'",
            "stream_version_gap" => "version=3",
            "zero_global_seq" => "global_seq=0",
            "negative_global_seq" => "global_seq=-1",
            _ => unreachable!(),
        };
        assert_eq!(
            connection
                .execute(
                    &format!(
                        "UPDATE {prefix}_events SET {mutation} WHERE tenant_id=?1 AND version=2"
                    ),
                    [owner.as_str()],
                )
                .expect("tampered last event"),
            1,
            "one stored event was changed"
        );
        drop(connection);

        assert!(
            matches!(
                store.capture_tenant(&owner, &[], limits()).await,
                Err(CaptureError::Corrupt {
                    material: CaptureMaterial::Event
                })
            ),
            "{corruption}: capture must refuse the complete corrupt history"
        );
        let refused = store
            .rebuild_inline_projection(projector.name(), &owner)
            .await;
        assert!(
            refused.is_err(),
            "{corruption}: rebuild admitted corrupt history: {refused:?}"
        );
        let connection = rusqlite::Connection::open(path).expect("inspection connection");
        assert_eq!(
            published(&connection, prefix, &owner),
            owner_before,
            "{corruption}: owner publication changed"
        );
        assert_eq!(
            published(&connection, prefix, &neighbour),
            neighbour_before,
            "{corruption}: unrelated publication changed"
        );
    }
}
