//! What a consistent tenant capture promises, wherever it is implemented.
//!
//! These assertions are the definition; File, SQLite and PostgreSQL are three implementations that
//! agree with them. Everything here goes through the real store port and the real capture
//! capability — no simulated read loop, because a simulated loop would prove the simulation.

use std::sync::Arc;

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureRequestRefusal, CaptureResource, CatchUpRunner,
    ConsistentTenantCapture, EventStore, Expected, NewEvent, ProjectionCaptureRefusal,
    ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId, TenantCapture, TenantId,
};
use serde_json::{Value, json};

use crate::{drain_at_least, meta};

/// The per-key read model these cases capture. Its indexed field makes declaration drift visible.
pub const CAPTURE_LEDGER: ProjectionSpec = ProjectionSpec {
    name: "capture_ledger",
    indexed: &["kind"],
};

/// A second table under the same projector, so request order has something to preserve.
pub const CAPTURE_SIDECAR: ProjectionSpec = ProjectionSpec {
    name: "capture_sidecar",
    indexed: &[],
};

/// A projector whose row key is whatever the event body says, so every admitted key is reachable.
pub struct CaptureLedger;

impl Projector for CaptureLedger {
    fn name(&self) -> &'static str {
        "capture_ledger"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        &[CAPTURE_LEDGER, CAPTURE_SIDECAR]
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), eventlog_core::EventLogError>> {
        Box::pin(async move {
            let key = event
                .data
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or(event.stream_id.as_str())
                .to_owned();
            store
                .upsert(
                    &CAPTURE_LEDGER,
                    &event.tenant,
                    &key,
                    &json!({ "kind": event.name, "version": event.version }),
                )
                .await?;
            // A verbatim copy of the body, which is what makes the redaction case decisive.
            store
                .upsert(&CAPTURE_SIDECAR, &event.tenant, &key, &event.data)
                .await
        })
    }
}

fn keyed(name: &str, key: &str, value: i64) -> NewEvent {
    NewEvent::new(name, 1, json!({ "key": key, "value": value })).expect("fixture event is valid")
}

fn generous() -> CaptureLimits {
    CaptureLimits {
        max_events: 1024,
        max_blobs: 1024,
        max_projection_rows: 1024,
        max_payload_bytes: 1 << 20,
    }
}

fn compact(value: &Value) -> u64 {
    serde_json::to_vec(value)
        .expect("stored body encodes")
        .len() as u64
}

/// Run every capture assertion against one backend.
///
/// # Panics
/// Panics with the failing assertion, naming the promise the provider broke.
pub async fn run_consistent_capture(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
) {
    let runner = CatchUpRunner::new(Arc::clone(store), Arc::new(CaptureLedger))
        .await
        .expect("declared capture projections")
        .with_batch(200);
    identity_decides_before_history(store, capture).await;
    let owner = complete_content_in_order(store, capture, &runner).await;
    every_cap_is_exact(store, capture, &owner).await;
    invalid_requests_and_unavailable_projections(capture, &owner).await;
    a_lagging_projection_is_returned_as_it_is(store, capture, &runner).await;
    redaction_withholds_every_value(store, capture, &runner).await;
}

/// Identity precedence, in the order the design fixes it.
async fn identity_decides_before_history(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
) {
    let missing = TenantId::new("capture-missing").expect("valid tenant");
    assert_eq!(
        capture
            .capture_tenant(&missing, &[], generous())
            .await
            .expect_err("a tenant nobody provisioned has no capture"),
        CaptureError::TenantIdentityMissing
    );

    let provisioned = TenantId::new("capture-provisioned").expect("valid tenant");
    let first = store
        .stream_identity(&provisioned)
        .await
        .expect("provisioned identity");
    let value = capture
        .capture_tenant(&provisioned, &[], generous())
        .await
        .expect("an empty provisioned tenant is a valid capture");
    assert_eq!(value.tenant, provisioned);
    assert_eq!(value.stream_identity, first);
    assert!(value.events.is_empty());
    assert!(value.blobs.is_empty());
    assert!(
        value.projections.is_empty(),
        "an empty projection request asserts nothing about unrequested tables"
    );

    store
        .forget_tenant(&provisioned)
        .await
        .expect("complete erasure");
    assert_eq!(
        capture
            .capture_tenant(&provisioned, &[], generous())
            .await
            .expect_err("erasure removes the identity with the content"),
        CaptureError::TenantIdentityMissing
    );
    let second = store
        .stream_identity(&provisioned)
        .await
        .expect("re-provisioned identity");
    assert_ne!(
        second, first,
        "re-provisioning after erasure is a different stream, and says so"
    );
    assert_eq!(
        capture
            .capture_tenant(&provisioned, &[], generous())
            .await
            .expect("valid again")
            .stream_identity,
        second
    );

    // Ordinary append does not provision an identity, so this history is reachable.
    let unprovisioned = TenantId::new("capture-unprovisioned").expect("valid tenant");
    let stream = StreamId::new(unprovisioned.clone(), "item", "one").expect("valid stream");
    store
        .append(
            &stream,
            Expected::NoStream,
            &[keyed("item.received", "one", 1)],
            &meta("unprovisioned", &json!({})),
        )
        .await
        .expect("appended without provisioning");
    assert_eq!(
        capture
            .capture_tenant(&unprovisioned, &[], generous())
            .await
            .expect_err("history without an identity is not a capture"),
        CaptureError::TenantIdentityMissing
    );
    store
        .redact(&stream, 1, "erased")
        .await
        .expect("redacted event");
    assert_eq!(
        capture
            .capture_tenant(&unprovisioned, &[], generous())
            .await
            .expect_err("an absent identity is decided first"),
        CaptureError::TenantIdentityMissing,
        "redacted history does not take precedence over an identity that is not there"
    );
    store
        .stream_identity(&unprovisioned)
        .await
        .expect("provisioned after the fact");
    assert_eq!(
        capture
            .capture_tenant(&unprovisioned, &[], generous())
            .await
            .expect_err("now the history condition decides"),
        CaptureError::RedactedHistory,
        "provisioning an identity changes which refusal applies; it never produces a value"
    );
}

/// Complete content, in the promised order, with nothing from anybody else in it.
async fn complete_content_in_order(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
    runner: &CatchUpRunner,
) -> TenantId {
    let owner = TenantId::new("capture-owner").expect("valid tenant");
    let neighbour = TenantId::new("capture-neighbour").expect("valid tenant");
    for tenant in [&owner, &neighbour] {
        store
            .stream_identity(tenant)
            .await
            .expect("provisioned identity");
    }
    // An empty key, a whitespace key and a Unicode key: every string an existing writer admits.
    for (index, (key, id)) in [("", "alpha"), (" ", "beta"), ("é", "alpha")]
        .into_iter()
        .enumerate()
    {
        let stream = StreamId::new(owner.clone(), "item", id).expect("valid stream");
        let index = i64::try_from(index).expect("small index");
        store
            .append(
                &stream,
                Expected::Any,
                &[keyed("item.received", key, index)],
                &meta(&format!("owner-{index}"), &json!({ "index": index })),
            )
            .await
            .expect("appended");
    }
    store
        .append(
            &StreamId::new(neighbour.clone(), "item", "alpha").expect("valid stream"),
            Expected::Any,
            &[keyed("item.received", "", 9)],
            &meta("neighbour-0", &json!({})),
        )
        .await
        .expect("appended");
    drain_at_least(runner, &owner, 3).await;
    drain_at_least(runner, &neighbour, 1).await;

    store
        .put_blob(&owner, "zeta", b"zeta-bytes")
        .await
        .expect("orphan binding");
    store
        .put_blob(&owner, "alpha", b"alpha")
        .await
        .expect("bound content");
    store
        .put_blob(&owner, "Gone", b"gone")
        .await
        .expect("bound content");
    store
        .delete_blob(&owner, "Gone")
        .await
        .expect("deleted binding");
    store
        .put_blob(&neighbour, "zeta", b"neighbour-bytes")
        .await
        .expect("another tenant's binding");

    let value = capture
        .capture_tenant(&owner, &[CAPTURE_SIDECAR, CAPTURE_LEDGER], generous())
        .await
        .expect("complete observation");
    assert_eq!(value.events.len(), 3);
    assert!(
        value
            .events
            .windows(2)
            .all(|pair| pair[0].global_seq < pair[1].global_seq),
        "every event once, in ascending feed position"
    );
    assert!(value.events.iter().all(|event| event.tenant == owner));
    assert_eq!(
        value
            .blobs
            .iter()
            .map(|blob| blob.digest.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"],
        "bytewise digest order; a deleted reference is absent, not an empty binding"
    );
    assert_eq!(
        value.blobs[1].bytes,
        b"zeta-bytes".to_vec(),
        "a binding no event names is still this tenant's content"
    );
    assert_eq!(value.projections.len(), 2);
    assert_eq!(
        value.projections[0].specification, CAPTURE_SIDECAR,
        "projection results keep the request's order"
    );
    assert_eq!(value.projections[1].specification, CAPTURE_LEDGER);
    for projected in &value.projections {
        assert_eq!(
            projected
                .rows
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            ["", " ", "é"],
            "every admitted key round-trips, in bytewise order, with no grammar imposed on it"
        );
    }

    let other = capture
        .capture_tenant(&neighbour, &[CAPTURE_LEDGER], generous())
        .await
        .expect("complete observation");
    assert_eq!(other.events.len(), 1);
    assert_eq!(
        other
            .blobs
            .iter()
            .map(|blob| (blob.digest.as_str(), blob.bytes.as_slice()))
            .collect::<Vec<_>>(),
        [("zeta", b"neighbour-bytes".as_slice())],
        "one opaque digest bound in two tenants is two bindings"
    );
    assert_eq!(other.projections[0].rows.len(), 1);

    let bare = capture
        .capture_tenant(&owner, &[], generous())
        .await
        .expect("an empty projection request is valid");
    assert_eq!(bare.events, value.events);
    assert!(bare.projections.is_empty());
    owner
}

/// Every cap, to the exact unit, including zero and the aggregated payload.
async fn every_cap_is_exact(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
    owner: &TenantId,
) {
    let request = [CAPTURE_LEDGER, CAPTURE_SIDECAR];
    let full: TenantCapture = capture
        .capture_tenant(owner, &request, generous())
        .await
        .expect("complete observation");
    let events = full.events.len() as u64;
    let blobs = full.blobs.len() as u64;
    let rows: u64 = full
        .projections
        .iter()
        .map(|projected| projected.rows.len() as u64)
        .sum();
    // Blob bytes, event bodies and row bodies. Not coordinates, not envelope fields, not
    // declarations, and not whatever a container costs to hold them.
    let payload: u64 = full
        .blobs
        .iter()
        .map(|blob| blob.bytes.len() as u64)
        .sum::<u64>()
        + full
            .events
            .iter()
            .map(|event| compact(&event.data))
            .sum::<u64>()
        + full
            .projections
            .iter()
            .flat_map(|projected| projected.rows.iter())
            .map(|(_, body)| compact(body))
            .sum::<u64>();
    let exact = CaptureLimits {
        max_events: events,
        max_blobs: blobs,
        max_projection_rows: rows,
        max_payload_bytes: payload,
    };
    assert_eq!(
        capture
            .capture_tenant(owner, &request, exact)
            .await
            .expect("an exact fit is a fit"),
        full
    );
    // No remembered quota and no partial cursor: the same request answers the same way.
    assert_eq!(
        capture
            .capture_tenant(owner, &request, exact)
            .await
            .expect("repeatable"),
        full
    );

    for (resource, limits) in [
        (
            CaptureResource::Events,
            CaptureLimits {
                max_events: events - 1,
                ..exact
            },
        ),
        (
            CaptureResource::Blobs,
            CaptureLimits {
                max_blobs: blobs - 1,
                ..exact
            },
        ),
        (
            CaptureResource::ProjectionRows,
            CaptureLimits {
                max_projection_rows: rows - 1,
                ..exact
            },
        ),
        (
            CaptureResource::PayloadBytes,
            CaptureLimits {
                max_payload_bytes: payload - 1,
                ..exact
            },
        ),
    ] {
        let limit = match resource {
            CaptureResource::Events => limits.max_events,
            CaptureResource::Blobs => limits.max_blobs,
            CaptureResource::ProjectionRows => limits.max_projection_rows,
            CaptureResource::PayloadBytes => limits.max_payload_bytes,
        };
        assert_eq!(
            capture
                .capture_tenant(owner, &request, limits)
                .await
                .expect_err("one unit short of a cap is over it"),
            CaptureError::LimitExceeded { resource, limit },
            "the reported resource is the one the content actually crossed"
        );
    }

    let zero = CaptureLimits {
        max_events: 0,
        max_blobs: 0,
        max_projection_rows: 0,
        max_payload_bytes: 0,
    };
    let refusal = capture
        .capture_tenant(owner, &[], zero)
        .await
        .expect_err("zero is a cap, not an unset default");
    assert!(
        matches!(refusal, CaptureError::LimitExceeded { limit: 0, .. }),
        "{refusal:?}"
    );
    // The same zero caps are satisfiable where there is nothing to hold.
    let bare = TenantId::new("capture-zero").expect("valid tenant");
    store
        .stream_identity(&bare)
        .await
        .expect("provisioned identity");
    let empty = capture
        .capture_tenant(&bare, &[], zero)
        .await
        .expect("nothing fits in nothing");
    assert!(empty.events.is_empty() && empty.blobs.is_empty());
}

/// Invalid requests, and the projections a store will not serve.
async fn invalid_requests_and_unavailable_projections(
    capture: &dyn ConsistentTenantCapture,
    owner: &TenantId,
) {
    const REPEATED_FIELD: ProjectionSpec = ProjectionSpec {
        name: "capture_ledger",
        indexed: &["kind", "kind"],
    };
    const INVALID: ProjectionSpec = ProjectionSpec {
        name: "Capture_Ledger",
        indexed: &[],
    };
    const UNDECLARED: ProjectionSpec = ProjectionSpec {
        name: "capture_unknown",
        indexed: &[],
    };
    const DRIFTED: ProjectionSpec = ProjectionSpec {
        name: "capture_ledger",
        indexed: &[],
    };

    for (request, reason) in [
        (
            vec![CAPTURE_LEDGER, CAPTURE_LEDGER],
            CaptureRequestRefusal::DuplicateProjection,
        ),
        (
            vec![REPEATED_FIELD],
            CaptureRequestRefusal::DuplicateIndexedField,
        ),
        (
            vec![INVALID],
            CaptureRequestRefusal::InvalidProjectionSpecification,
        ),
    ] {
        assert_eq!(
            capture
                .capture_tenant(owner, &request, generous())
                .await
                .expect_err("a duplicate request is invalid, never merged"),
            CaptureError::InvalidRequest { reason }
        );
    }

    for (specification, reason) in [
        (UNDECLARED, ProjectionCaptureRefusal::Undeclared),
        (DRIFTED, ProjectionCaptureRefusal::DeclarationMismatch),
    ] {
        assert_eq!(
            capture
                .capture_tenant(owner, &[specification], generous())
                .await
                .expect_err("this store cannot serve that projection"),
            CaptureError::ProjectionUnavailable {
                projection: specification.name.to_owned(),
                reason
            }
        );
    }
}

/// A projection behind its history is returned as it is, with no claim that it is current.
async fn a_lagging_projection_is_returned_as_it_is(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
    runner: &CatchUpRunner,
) {
    let lagging = TenantId::new("capture-lagging").expect("valid tenant");
    store
        .stream_identity(&lagging)
        .await
        .expect("provisioned identity");
    let stream = StreamId::new(lagging.clone(), "item", "one").expect("valid stream");
    for index in 0..2_i64 {
        store
            .append(
                &stream,
                Expected::Any,
                &[keyed("item.received", &format!("key-{index}"), index)],
                &meta(&format!("lag-{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }
    drain_at_least(runner, &lagging, 2).await;
    for index in 2..4_i64 {
        store
            .append(
                &stream,
                Expected::Any,
                &[keyed("item.received", &format!("key-{index}"), index)],
                &meta(&format!("lag-{index}"), &json!({})),
            )
            .await
            .expect("appended");
    }

    let value = capture
        .capture_tenant(&lagging, &[CAPTURE_LEDGER], generous())
        .await
        .expect("complete observation");
    assert_eq!(value.events.len(), 4, "history is complete at the snapshot");
    assert_eq!(
        value.projections[0]
            .rows
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["key-0", "key-1"],
        "the exact rows at the snapshot, faithfully: no projector runs and no catch-up is triggered"
    );
    // No currentness bit exists to claim otherwise; the consumer checks its own invariants.
    assert!(value.projections[0].rows.len() < value.events.len());
}

/// Redacted history withholds every value, with or without a projection, before and after rebuild.
async fn redaction_withholds_every_value(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
    runner: &CatchUpRunner,
) {
    let tenant = TenantId::new("capture-redacted").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    let stream = StreamId::new(tenant.clone(), "item", "one").expect("valid stream");
    let marker = "body-copied-into-a-materialized-row";
    store
        .append(
            &stream,
            Expected::NoStream,
            &[NewEvent::new(
                "item.received",
                1,
                json!({ "key": "one", "marker": marker }),
            )
            .expect("fixture event is valid")],
            &meta("redacted", &json!({})),
        )
        .await
        .expect("appended");
    store
        .put_blob(&tenant, "bound", b"bound-bytes")
        .await
        .expect("bound content");
    drain_at_least(runner, &tenant, 1).await;
    let before = capture
        .capture_tenant(&tenant, &[CAPTURE_SIDECAR], generous())
        .await
        .expect("complete observation");
    assert!(
        before.projections[0]
            .rows
            .iter()
            .any(|(_, body)| body.to_string().contains(marker)),
        "the fixture only means something while the row holds a copy of the body"
    );

    store
        .redact(&stream, 1, "erased")
        .await
        .expect("redacted event");
    for request in [Vec::new(), vec![CAPTURE_SIDECAR]] {
        assert_eq!(
            capture
                .capture_tenant(&tenant, &request, generous())
                .await
                .expect_err("no complete observation of this tenant exists"),
            CaptureError::RedactedHistory,
            "the refusal does not depend on having asked for a projection"
        );
    }
    store
        .rebuild_projection(Arc::new(CaptureLedger), &tenant)
        .await
        .expect("rebuilt from current history");
    for request in [Vec::new(), vec![CAPTURE_SIDECAR]] {
        assert_eq!(
            capture
                .capture_tenant(&tenant, &request, generous())
                .await
                .expect_err("rebuilding a view does not restore erased history"),
            CaptureError::RedactedHistory
        );
    }
}
