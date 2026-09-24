//! Source-examination cases for the shared capture contract.
//!
//! Added by the first source examination of `feat: capture complete tenant state consistently
//! across providers`. Every assertion here is taken from `docs/design/consistent-tenant-capture.md`
//! and driven against the public helpers, rather than from the implementation that produced them.
//! Nothing in this file touches an implementation file or an existing case.

use eventlog_core::{
    CaptureBudget, CaptureError, CaptureLimits, CaptureMaterial, CaptureRequestRefusal,
    CaptureResource, CapturedBlob, MAX_CAUSATION_DEPTH, ProjectionSpec, RecordedEvent, TenantId,
    order_blobs, order_rows, validate_capture_request, validate_captured_digest,
    validate_captured_event, validate_captured_order,
};
use serde_json::{Value, json};
use time::OffsetDateTime;

fn owner() -> TenantId {
    TenantId::new("review-one-owner").expect("valid tenant")
}

fn compact(value: &Value) -> u64 {
    let encoded = serde_json::to_vec(value).expect("encodable body");
    u64::try_from(encoded.len()).expect("a body shorter than the address space")
}

fn recorded(global_seq: u64, version: u64, stream_id: &str) -> RecordedEvent {
    RecordedEvent {
        global_seq,
        tenant: owner(),
        stream_type: "item".to_owned(),
        stream_id: stream_id.to_owned(),
        version,
        event_id: "event-1".to_owned(),
        name: "item.received".to_owned(),
        schema_version: 1,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        recorded_at: OffsetDateTime::UNIX_EPOCH,
        subject: "person-1".to_owned(),
        actor: "service-1".to_owned(),
        request_id: "request-1".to_owned(),
        trace_id: "trace-1".to_owned(),
        causation_id: None,
        causation_depth: 0,
        redacted_at: None,
        data: json!({ "value": 1 }),
        digest: None,
        parents: Vec::new(),
    }
}

/// "Duplicate projection names ... are invalid requests, never merged."
///
/// The existing case repeats one identical specification. Two entries under one name declaring
/// *different* fields is the request a merging reader would answer with something nobody asked for.
#[test]
fn a_repeated_projection_name_is_one_duplicate_request_whatever_its_fields() {
    const FIRST: ProjectionSpec = ProjectionSpec {
        name: "ledger",
        indexed: &["kind"],
    };
    const DRIFTED: ProjectionSpec = ProjectionSpec {
        name: "ledger",
        indexed: &["other"],
    };
    const DISTINCT: ProjectionSpec = ProjectionSpec {
        name: "sidecar",
        indexed: &[],
    };

    assert_eq!(
        validate_capture_request(&owner(), &[FIRST, DRIFTED]),
        Err(CaptureError::InvalidRequest {
            reason: CaptureRequestRefusal::DuplicateProjection
        }),
        "one name declared twice is a duplicate request, not two tables to merge"
    );
    assert!(
        validate_capture_request(&owner(), &[FIRST, DISTINCT]).is_ok(),
        "two names are two requests"
    );
    assert!(
        validate_capture_request(&owner(), &[]).is_ok(),
        "an empty projection request is valid"
    );
}

/// "It excludes coordinates, container overhead, JSON event-envelope fields."
///
/// An envelope as long as this kit will store, and a body of a handful of bytes: the payload cap
/// is the body alone, to the byte, and one byte short of it refuses.
#[test]
fn payload_accounting_counts_bodies_and_never_the_envelope() {
    let body = json!({ "value": 1 });
    let exact = compact(&body);
    let long = "x".repeat(400);
    let mut event = recorded(1, 1, "one");
    event.data = body;
    event.name.clone_from(&long);
    event.event_id.clone_from(&long);
    event.request_id.clone_from(&long);
    event.trace_id.clone_from(&long);
    event.causation_id = Some(long);
    assert!(
        validate_captured_event(&owner(), &event).is_ok(),
        "the fixture only means something while this envelope is one a writer here could store"
    );

    let mut fits = CaptureBudget::new(CaptureLimits {
        max_events: 1,
        max_blobs: 0,
        max_projection_rows: 0,
        max_payload_bytes: exact,
    });
    assert!(
        fits.admit_event(&event).is_ok(),
        "an envelope field is a coordinate, and a coordinate is not payload"
    );

    let mut short = CaptureBudget::new(CaptureLimits {
        max_events: 1,
        max_blobs: 0,
        max_projection_rows: 0,
        max_payload_bytes: exact - 1,
    });
    assert_eq!(
        short.admit_event(&event),
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::PayloadBytes,
            limit: exact - 1
        }),
        "the payload cap is the body's exact compact length"
    );
}

/// "Every reported limit must actually have been exceeded", and it reports its own cap.
#[test]
fn a_reported_cap_names_the_resource_that_crossed_it_and_its_own_limit() {
    let event = recorded(1, 1, "one");
    let mut counted = CaptureBudget::new(CaptureLimits {
        max_events: 0,
        max_blobs: 4,
        max_projection_rows: 4,
        max_payload_bytes: 0,
    });
    assert_eq!(
        counted.admit_event(&event),
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::Events,
            limit: 0
        }),
        "the count is charged before the body, and the refusal names which cap decided"
    );

    let budget = CaptureBudget::new(CaptureLimits {
        max_events: 9,
        max_blobs: 9,
        max_projection_rows: 3,
        max_payload_bytes: 9,
    });
    assert!(
        budget.proven(CaptureResource::ProjectionRows, 3).is_ok(),
        "a cap met exactly is met"
    );
    assert_eq!(
        budget.proven(CaptureResource::ProjectionRows, 4),
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::ProjectionRows,
            limit: 3
        })
    );
    assert_eq!(
        budget.proven(CaptureResource::PayloadBytes, u64::MAX),
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::PayloadBytes,
            limit: 9
        }),
        "a proven total reports the cap of the resource it was proven against"
    );
}

/// "a stream whose versions are not `1..n`" is corruption; a position gap is not.
#[test]
fn stream_versions_are_checked_per_stream_and_a_position_gap_is_ordinary() {
    let tenant = owner();
    let corrupt = Err(CaptureError::Corrupt {
        material: CaptureMaterial::Event,
    });

    assert!(
        validate_captured_order(&tenant, &[]).is_ok(),
        "a provisioned tenant with no history is a history"
    );
    assert!(
        validate_captured_order(
            &tenant,
            &[
                recorded(5, 1, "a"),
                recorded(6, 1, "b"),
                recorded(7, 2, "a"),
                recorded(8, 2, "b"),
            ]
        )
        .is_ok(),
        "two streams interleave, and neither has to start at the first position"
    );
    assert_eq!(
        validate_captured_order(&tenant, &[recorded(1, 1, "a"), recorded(2, 1, "a")]),
        corrupt,
        "one stream cannot record the same version twice"
    );

    let mut deepest = recorded(1, 1, "a");
    deepest.causation_depth = MAX_CAUSATION_DEPTH;
    assert!(
        validate_captured_order(&tenant, &[deepest]).is_ok(),
        "the documented maximum depth is admitted, not refused"
    );
    let mut deeper = recorded(1, 1, "a");
    deeper.causation_depth = MAX_CAUSATION_DEPTH + 1;
    assert_eq!(
        validate_captured_order(&tenant, &[deeper]),
        corrupt,
        "one past the maximum is a row no constructor here produced"
    );
}

/// A repeated coordinate is found by the ordering, so arrival order cannot hide one.
#[test]
fn a_repeated_coordinate_is_found_after_sorting_not_only_when_it_arrives_adjacent() {
    let mut rows = vec![
        ("b".to_owned(), json!(1)),
        ("a".to_owned(), json!(2)),
        ("b".to_owned(), json!(3)),
    ];
    assert_eq!(
        order_rows(&mut rows),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Projection
        }),
        "a key repeated out of arrival order is still one key bound twice"
    );

    let mut blobs = vec![
        CapturedBlob {
            digest: "c".to_owned(),
            bytes: vec![1],
        },
        CapturedBlob {
            digest: "a".to_owned(),
            bytes: vec![2],
        },
        CapturedBlob {
            digest: "c".to_owned(),
            bytes: vec![3],
        },
    ];
    assert_eq!(
        order_blobs(&mut blobs),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        })
    );

    // "Digest identity remains opaque": exactly what a writer here could bind, and nothing read
    // into the bytes beyond that.
    assert!(
        validate_captured_digest("a b").is_ok(),
        "no grammar is imposed on an opaque digest"
    );
    assert_eq!(
        validate_captured_digest("é"),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a digest carrying bytes no writer here accepts is corruption"
    );
}

/// "Object key order does not change that length."
#[test]
fn one_body_has_one_payload_length_whatever_order_its_keys_arrived_in() {
    let first: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).expect("valid stored body");
    let second: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).expect("valid stored body");
    let length = compact(&first);
    assert_eq!(length, compact(&second));

    let mut budget = CaptureBudget::new(CaptureLimits {
        max_events: 0,
        max_blobs: 0,
        max_projection_rows: 2,
        max_payload_bytes: length * 2,
    });
    assert!(budget.admit_projection_row(&first).is_ok());
    assert!(
        budget.admit_projection_row(&second).is_ok(),
        "the same body costs the same whichever order its keys were stored in"
    );
    assert_eq!(
        budget.admit_projection_row(&json!({})),
        Err(CaptureError::LimitExceeded {
            resource: CaptureResource::ProjectionRows,
            limit: 2
        }),
        "the row cap is the sum across every requested projection"
    );
}
