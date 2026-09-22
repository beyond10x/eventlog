//! What a capture still promises once it stops reading blob content to take itself.
//!
//! `ConsistentTenantCapture::capture_tenant` reads every bound object before it returns, so the
//! bytes it hands over were hashed inside the provider's own consistency boundary and a damaged
//! object is refused there. `capture_tenant_deferred` moves that read to the moment a caller asks
//! for the content — which is the only way an observation of an authority with thousands of
//! bindings can cost less than reading all of them. **The check moves with the read; it does not
//! go away**, and that is what this exercise is.
//!
//! These assertions are the definition of the deferred read, and they are run against a provider
//! that has one. The default `capture_tenant_deferred` reads every object before it returns, as
//! `capture_tenant` does, so damage done afterwards cannot reach it — the exercise would be
//! asking such a provider to detect a change to content it is no longer looking at. That is not a
//! weaker promise, it is an earlier one, and it is the promise `run_consistent_capture` already
//! checks. Today `eventlog-file` is the provider with a deferred read and the only caller here.

use std::sync::Arc;

use eventlog_core::{
    CaptureError, CaptureLimits, CaptureMaterial, CaptureResource, ConsistentTenantCapture,
    EventStore, TenantId,
};

fn generous() -> CaptureLimits {
    CaptureLimits {
        max_events: 1024,
        max_blobs: 1024,
        max_projection_rows: 1024,
        max_payload_bytes: 1 << 20,
    }
}

const INTACT: &[u8] = b"deferred capture: the binding nobody touched";
const DAMAGED: &[u8] = b"deferred capture: the binding damaged after the observation";

/// Run the deferred read's assertions against one backend.
///
/// `damage` replaces the stored content behind one binding with something else, in place, without
/// going through the store port — the corruption a provider exists to catch rather than an
/// operation a caller performs. It is given the tenant, the digest and the exact bytes the
/// exercise bound, so a provider can find its own object however it stores it. A `damage` that
/// does nothing does not quietly pass: the exercise reads the binding back through the store port
/// first and requires that read to refuse.
///
/// # Panics
/// Panics with the failing assertion, naming the promise the provider broke.
pub async fn run_deferred_blob_bytes(
    store: &Arc<dyn EventStore>,
    capture: &dyn ConsistentTenantCapture,
    damage: &dyn Fn(&TenantId, &str, &[u8]),
) {
    let tenant = TenantId::new("deferred-blob-bytes").expect("valid tenant");
    store
        .stream_identity(&tenant)
        .await
        .expect("provisioned identity");
    store
        .put_blob(&tenant, "intact", INTACT)
        .await
        .expect("bound blob");
    store
        .put_blob(&tenant, "damaged", DAMAGED)
        .await
        .expect("bound blob");

    // Before anything is damaged: a deferred capture hands out the content it bound, every
    // binding of it, byte for byte. A capture that answered cheaply by answering with less would
    // pass every corruption assertion below and fail this one.
    let undamaged = capture
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    assert_eq!(
        undamaged
            .blobs
            .iter()
            .map(|blob| blob.digest.clone())
            .collect::<Vec<_>>(),
        vec!["damaged".to_owned(), "intact".to_owned()],
        "a deferred capture names every bound digest, in bytewise digest order"
    );
    for blob in &undamaged.blobs {
        let expected = if blob.digest == "intact" {
            INTACT
        } else {
            DAMAGED
        };
        assert_eq!(
            blob.bytes()
                .expect("an undamaged binding hands out its content"),
            expected,
            "a deferred binding handed out content that is not what it bound"
        );
    }

    // The caps are the caller's, and they are the same caps. A capture that stopped reading its
    // bindings in order to be cheap could just as easily stop *counting* them, and every
    // assertion about content would still pass while the explicit bound the caller chose had
    // quietly become no bound at all. The payload cap is charged in bytes, so it is the one that
    // says whether the sizes are still being counted without the content being read.
    let exact = undamaged
        .blobs
        .iter()
        .map(|blob| blob.bytes().expect("an undamaged binding").len() as u64)
        .sum::<u64>();
    assert_eq!(
        exact,
        (INTACT.len() + DAMAGED.len()) as u64,
        "the fixture's bindings are the bytes it bound"
    );
    let fits = CaptureLimits {
        max_payload_bytes: exact,
        ..generous()
    };
    capture
        .capture_tenant_deferred(&tenant, &[], fits)
        .await
        .expect("a payload cap that is exactly the content admits it");
    assert_eq!(
        capture
            .capture_tenant_deferred(
                &tenant,
                &[],
                CaptureLimits {
                    max_payload_bytes: exact - 1,
                    ..generous()
                },
            )
            .await
            .expect_err("a payload cap one byte short of the content refuses it"),
        CaptureError::LimitExceeded {
            resource: CaptureResource::PayloadBytes,
            limit: exact - 1,
        },
        "a deferred capture stopped charging its bindings against the caller's payload cap"
    );
    assert_eq!(
        capture
            .capture_tenant_deferred(
                &tenant,
                &[],
                CaptureLimits {
                    max_blobs: 1,
                    ..generous()
                },
            )
            .await
            .expect_err("a blob cap below the number bound refuses"),
        CaptureError::LimitExceeded {
            resource: CaptureResource::Blobs,
            limit: 1,
        },
        "a deferred capture stopped charging its bindings against the caller's blob cap"
    );

    // The observation under test is taken while the store is intact, exactly as a caller takes
    // one, and the damage happens afterwards — which is the whole question: the provider is no
    // longer holding these bytes, so the only place left to catch this is the read.
    let observed = capture
        .capture_tenant_deferred(&tenant, &[], generous())
        .await
        .expect("a deferred capture of a provisioned tenant");
    damage(&tenant, "damaged", DAMAGED);

    assert!(
        store.get_blob(&tenant, "damaged").await.is_err(),
        "the damage this exercise depends on did not reach the store: the provider's own read \
         still returns the binding, so nothing below is being tested"
    );

    let refused = observed
        .blobs
        .iter()
        .find(|blob| blob.digest == "damaged")
        .expect("the damaged binding is in the observation");
    assert_eq!(
        refused.bytes(),
        Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob
        }),
        "a binding whose stored content was damaged after the observation was handed out anyway"
    );

    let intact = observed
        .blobs
        .iter()
        .find(|blob| blob.digest == "intact")
        .expect("the untouched binding is in the observation");
    assert_eq!(
        intact
            .bytes()
            .expect("one damaged binding does not withdraw the others"),
        INTACT,
        "an untouched binding stopped handing out its content because a sibling was damaged"
    );

    assert!(
        observed.load().is_err(),
        "reading the whole observation is every binding read, so it refuses where one binding does"
    );
}
