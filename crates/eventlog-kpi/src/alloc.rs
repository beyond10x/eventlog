//! Allocation bytes of two pure core calls (review L09, L10), counted by the global allocator.
//!
//! The workspace forbids `unsafe`, and a counting allocator is an `unsafe impl GlobalAlloc`, so
//! the count comes from `stats_alloc` (a small, dependency-free allocator wrapper), linked only
//! with the `counters` feature. Its counts are process-wide, so these scenarios run before any
//! runtime or worker thread exists.
use crate::{Measured, Outcome};

#[cfg(feature = "counters")]
#[global_allocator]
static GLOBAL: &stats_alloc::StatsAlloc<std::alloc::System> = &stats_alloc::INSTRUMENTED_SYSTEM;

const FINGERPRINT: &str = "core.fingerprint.alloc_bytes";
const CAPTURE_BUDGET: &str = "core.capture_budget.alloc_bytes";

#[cfg(not(feature = "counters"))]
#[allow(clippy::unnecessary_wraps)]
pub fn measure(measured: &mut Measured) -> Outcome<()> {
    for id in [FINGERPRINT, CAPTURE_BUDGET] {
        measured.skip(id, "built without the `counters` feature");
    }
    Ok(())
}

/// Bytes requested from the allocator, growth by reallocation included.
#[cfg(feature = "counters")]
fn requested<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let region = stats_alloc::Region::new(GLOBAL);
    let result = std::hint::black_box(work());
    let change = region.change();
    let grown = u64::try_from(change.bytes_reallocated.max(0)).unwrap_or(0);
    let allocated = u64::try_from(change.bytes_allocated).unwrap_or(u64::MAX);
    (result, allocated.saturating_add(grown))
}

#[cfg(feature = "counters")]
pub fn measure(measured: &mut Measured) -> Outcome<()> {
    use eventlog_conformance::meta;
    use eventlog_core::{
        AppendGroup, CaptureBudget, CaptureLimits, Expected, NewEvent, StreamAppend, StreamId,
        TenantId,
    };
    use serde_json::json;

    let tenant = TenantId::new("owner")?;
    let body = json!({ "value": "b".repeat(64 * 1024) });
    let appends = (0..8)
        .map(|index| {
            Ok(StreamAppend {
                stream: StreamId::new(tenant.clone(), "item", index.to_string())?,
                expected: Expected::Any,
                events: vec![NewEvent::new("seen", 1, body.clone())?],
            })
        })
        .collect::<Outcome<Vec<_>>>()?;
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: meta("fingerprint", &json!({})),
        appends,
    };
    let (fingerprint, bytes) = requested(|| group.fingerprint());
    fingerprint?;
    measured.put_count(FINGERPRINT, bytes);

    let row = json!({ "value": "r".repeat(16 * 1024) });
    let mut budget = CaptureBudget::new(CaptureLimits {
        max_events: 0,
        max_blobs: 0,
        max_projection_rows: 64,
        max_payload_bytes: u64::MAX,
    });
    let (admitted, bytes) = requested(|| {
        (0..64).try_for_each(|_| budget.admit_projection_row(&row).map_err(|e| e.to_string()))
    });
    admitted.map_err(crate::Failure)?;
    measured.put_count(CAPTURE_BUDGET, bytes);
    Ok(())
}
