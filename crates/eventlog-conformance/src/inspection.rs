//! Shared history semantics, exercised against real provider fixtures.
use eventlog_core::{
    EventStore, Expected, InspectHistory, InspectionError, InspectionLimits, RecordedEvent,
    StreamId, TenantId,
};

/// Finite fixture limits, intentionally larger than the complete synthetic source.
pub const INSPECTION_LIMITS: InspectionLimits = InspectionLimits {
    source_bytes: 4 * 1024 * 1024,
    events: 64,
    envelope_bytes: 64 * 1024,
};

/// Append interleaved tenant/stream records without provisioning tenant identities.
///
/// # Panics
/// Panics if a provider cannot create the synthetic published-format fixture.
pub async fn prepare_inspection_history(store: &dyn EventStore) -> Vec<RecordedEvent> {
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let other = TenantId::new("inspection-other").unwrap();
    let mut expected = Vec::new();
    for (index, (owner, stream, version)) in [
        (tenant.clone(), "first", 0),
        (other, "first", 0),
        (tenant.clone(), "second", 0),
        (tenant.clone(), "first", 1),
    ]
    .into_iter()
    .enumerate()
    {
        let id = StreamId::new(owner.clone(), "inspection", stream).unwrap();
        let body = serde_json::json!({"ordinal": index});
        let mut metadata = crate::meta(&format!("inspection-{index}"), &body);
        metadata.causation_id = Some("inspection-cause".into());
        metadata.causation_depth = 1;
        let event = eventlog_core::NewEvent::new("Inspected", 7, body).unwrap();
        let result = store
            .append(&id, Expected::Exact(version), &[event], &metadata)
            .await
            .unwrap();
        if owner == tenant {
            expected.extend(result.events);
        }
    }
    expected
}

/// Assert complete envelopes, tenant isolation, optional identity and exact caps.
///
/// # Panics
/// Panics if the inspector violates the shared history contract.
pub async fn run_history_inspection(inspector: &dyn InspectHistory, expected: &[RecordedEvent]) {
    let tenant = TenantId::new("inspection-tenant").unwrap();
    let observed = inspector
        .inspect_history(&tenant, INSPECTION_LIMITS)
        .await
        .unwrap();
    assert_eq!(observed.tenant, tenant);
    assert_eq!(observed.stream_identity, None);
    assert_eq!(observed.events, expected);
    let empty = inspector
        .inspect_history(
            &TenantId::new("inspection-absent").unwrap(),
            INSPECTION_LIMITS,
        )
        .await
        .unwrap();
    assert!(empty.events.is_empty());
    assert_eq!(empty.stream_identity, None);
    let envelope_bytes = expected
        .iter()
        .map(|event| u64::try_from(serde_json::to_vec(event).unwrap().len()).unwrap())
        .sum();
    let exact = InspectionLimits {
        events: u64::try_from(expected.len()).unwrap(),
        envelope_bytes,
        ..INSPECTION_LIMITS
    };
    assert_eq!(
        inspector
            .inspect_history(&tenant, exact)
            .await
            .unwrap()
            .events,
        expected
    );
    for limits in [
        InspectionLimits {
            events: exact.events - 1,
            ..exact
        },
        InspectionLimits {
            envelope_bytes: envelope_bytes - 1,
            ..exact
        },
        InspectionLimits {
            source_bytes: 0,
            ..exact
        },
        InspectionLimits { events: 0, ..exact },
    ] {
        assert_eq!(
            inspector.inspect_history(&tenant, limits).await,
            Err(InspectionError::LimitExceeded)
        );
    }
}
