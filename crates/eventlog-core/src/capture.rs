//! One tenant's complete owned observation, under a provider's own consistency boundary.
//!
//! A consumer that must compare complete history, bound content and materialized rows cannot prove
//! one observation by combining separately paginated reads: between two pages an append commits, a
//! blob is deleted and a projection is rebuilt, and the three answers it holds never existed
//! together. The provider supplies the whole thing under one native boundary instead, or it
//! supplies nothing.
//!
//! Nothing here is persisted. These are transient values at a read boundary: no wire contract, no
//! stored identity, no format version. The envelope a fact is recorded in remains [`RecordedEvent`]
//! and the projection vocabulary remains [`ProjectionSpec`].

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{
    BoxFuture, EventLogError, MAX_CAUSATION_DEPTH, ProjectionSpec, RecordedEvent, StreamId,
    TenantId, validate_field, validate_identity,
};

/// The explicit finite bounds one capture's returned content must fit.
///
/// Every count is supplied by the caller. There is no default and no clamping: a cap nobody chose
/// is a cap nobody sized, and zero is a real cap rather than "unset".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureLimits {
    pub max_events: u64,
    pub max_blobs: u64,
    pub max_projection_rows: u64,
    pub max_payload_bytes: u64,
}

/// One tenant's history, live bound content and requested materializations, observed together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantCapture {
    pub tenant: TenantId,
    /// The identity the provider already stored. Capture never mints one.
    pub stream_identity: String,
    /// Every event for this tenant, once, in ascending `global_seq`.
    pub events: Vec<RecordedEvent>,
    /// Every currently bound blob, once, in bytewise digest order, orphans included.
    pub blobs: Vec<CapturedBlob>,
    /// One entry per requested projection, in the request's order.
    pub projections: Vec<CapturedProjection>,
}

/// One bound blob, as the provider's own complete-content validator admitted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedBlob {
    pub digest: String,
    pub bytes: Vec<u8>,
}

/// One requested projection's rows, in bytewise key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedProjection {
    pub specification: ProjectionSpec,
    pub rows: Vec<(String, Value)>,
}

/// Why a capture request was refused before any store was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureRequestRefusal {
    InvalidTenant,
    InvalidProjectionSpecification,
    DuplicateProjection,
    DuplicateIndexedField,
}

/// Why one requested projection cannot be part of this observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionCaptureRefusal {
    /// No registry entry names it.
    Undeclared,
    /// The registry names it with different indexed fields, or in a different order.
    DeclarationMismatch,
    /// The actual table, columns or indexes are not the shape this kit writes.
    PhysicalShapeMismatch,
    /// A redaction left this view requiring a complete rebuild.
    Dirty,
}

/// Which explicit cap the content crossed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureResource {
    Events,
    Blobs,
    ProjectionRows,
    PayloadBytes,
}

/// Which stored material failed its integrity check.
///
/// The variant is the whole diagnostic. A stored value, a raw field or a database message would
/// carry the very bytes a capture refusal exists to withhold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureMaterial {
    Journal,
    Identity,
    Event,
    Blob,
    Projection,
}

/// Everything a capture can refuse with. On any refusal the caller receives no partial value.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CaptureError {
    #[error("capture request is invalid")]
    InvalidRequest { reason: CaptureRequestRefusal },
    /// Never provisioned, or forgotten. Distinguished from a provisioned tenant with no content.
    #[error("this tenant has no stored stream identity")]
    TenantIdentityMissing,
    /// Some event in this tenant's history has been erased, so no complete observation exists.
    #[error("this tenant's history contains redacted events")]
    RedactedHistory,
    /// A durable intent is pending. An authorized ordinary opener performs recovery, not this one.
    #[error("the store requires recovery by an authorized opener")]
    RecoveryRequired,
    #[error("projection {projection} cannot be captured")]
    ProjectionUnavailable {
        projection: String,
        reason: ProjectionCaptureRefusal,
    },
    #[error("capture content crossed an explicit limit of {limit}")]
    LimitExceeded {
        resource: CaptureResource,
        limit: u64,
    },
    #[error("stored capture material failed its integrity check")]
    Corrupt { material: CaptureMaterial },
    /// An existing operational failure, including Closed, Overloaded and Deadline.
    #[error(transparent)]
    Store(#[from] EventLogError),
}

/// One tenant's complete observation, from the provider that owns the boundary.
///
/// This is a separate capability, not a default on the store port: there is no emulation, no
/// pagination fallback and no callback under a lock. A consumer requires the trait explicitly, and
/// a provider that does not implement it refuses at capability selection rather than answering
/// with a plausible empty result.
pub trait ConsistentTenantCapture: Send + Sync + 'static {
    /// Observe one tenant's history, bound content and requested materializations together.
    ///
    /// # Errors
    /// Returns [`CaptureError::InvalidRequest`] for an unusable tenant or projection request,
    /// [`CaptureError::TenantIdentityMissing`] for a tenant with no stored identity,
    /// [`CaptureError::RedactedHistory`] when any event was erased,
    /// [`CaptureError::RecoveryRequired`] when the store has a pending durable intent,
    /// [`CaptureError::ProjectionUnavailable`] for a projection this store cannot serve,
    /// [`CaptureError::LimitExceeded`] when the content crosses an explicit cap,
    /// [`CaptureError::Corrupt`] for stored material that fails its integrity check, and
    /// [`CaptureError::Store`] for an operational failure.
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>>;
}

/// Check a capture request before any provider touches a connection or a lock.
///
/// # Errors
/// Returns [`CaptureError::InvalidRequest`] for an unusable tenant, an unusable projection
/// specification, a repeated projection name or a repeated indexed field. Duplicates are invalid
/// requests rather than something to merge: merging them would answer a question nobody asked.
pub fn validate_capture_request(
    tenant: &TenantId,
    projections: &[ProjectionSpec],
) -> Result<(), CaptureError> {
    let refuse = |reason| CaptureError::InvalidRequest { reason };
    validate_field("tenant", tenant.as_str())
        .map_err(|_| refuse(CaptureRequestRefusal::InvalidTenant))?;
    let mut names = BTreeSet::new();
    for specification in projections {
        specification
            .validate()
            .map_err(|_| refuse(CaptureRequestRefusal::InvalidProjectionSpecification))?;
        if !names.insert(specification.name) {
            return Err(refuse(CaptureRequestRefusal::DuplicateProjection));
        }
        let mut fields = BTreeSet::new();
        for field in specification.indexed {
            if !fields.insert(*field) {
                return Err(refuse(CaptureRequestRefusal::DuplicateIndexedField));
            }
        }
    }
    Ok(())
}

/// The exact, checked accounting every provider applies to one capture's content.
///
/// One implementation rather than three: a cap that is arithmetic in three places is three
/// arithmetics, and the one that saturates is the one nobody reads.
#[derive(Clone, Copy, Debug)]
pub struct CaptureBudget {
    limits: CaptureLimits,
    events: u64,
    blobs: u64,
    rows: u64,
    payload: u64,
}

impl CaptureBudget {
    #[must_use]
    pub const fn new(limits: CaptureLimits) -> Self {
        Self {
            limits,
            events: 0,
            blobs: 0,
            rows: 0,
            payload: 0,
        }
    }

    const fn cap(&self, resource: CaptureResource) -> u64 {
        match resource {
            CaptureResource::Events => self.limits.max_events,
            CaptureResource::Blobs => self.limits.max_blobs,
            CaptureResource::ProjectionRows => self.limits.max_projection_rows,
            CaptureResource::PayloadBytes => self.limits.max_payload_bytes,
        }
    }

    fn held(&mut self, resource: CaptureResource) -> &mut u64 {
        match resource {
            CaptureResource::Events => &mut self.events,
            CaptureResource::Blobs => &mut self.blobs,
            CaptureResource::ProjectionRows => &mut self.rows,
            CaptureResource::PayloadBytes => &mut self.payload,
        }
    }

    fn add(&mut self, resource: CaptureResource, amount: u64) -> Result<(), CaptureError> {
        let cap = self.cap(resource);
        let held = self.held(resource);
        // Overflow is not a value to admit: it is already past every u64 cap there can be.
        match held.checked_add(amount) {
            Some(value) if value <= cap => {
                *held = value;
                Ok(())
            }
            _ => Err(CaptureError::LimitExceeded {
                resource,
                limit: cap,
            }),
        }
    }

    /// Refuse a total the provider has already proven inside its own observation.
    ///
    /// The value must be a real lower bound on what the finished capture would hold, so that every
    /// reported limit is one the content actually crossed.
    ///
    /// # Errors
    /// Returns [`CaptureError::LimitExceeded`] when the proven total is over that cap.
    pub fn proven(&self, resource: CaptureResource, total: u64) -> Result<(), CaptureError> {
        if total > self.cap(resource) {
            return Err(CaptureError::LimitExceeded {
                resource,
                limit: self.cap(resource),
            });
        }
        Ok(())
    }

    /// Admit one event against the event cap and the payload cap.
    ///
    /// The payload contribution is the compact encoding of the event body alone. Envelope fields,
    /// coordinates and container overhead are not payload and are not counted.
    ///
    /// # Errors
    /// Returns [`CaptureError::LimitExceeded`] for a crossed cap and [`CaptureError::Corrupt`]
    /// when the stored body cannot be encoded.
    pub fn admit_event(&mut self, event: &RecordedEvent) -> Result<(), CaptureError> {
        self.add(CaptureResource::Events, 1)?;
        let bytes = compact_len(&event.data, CaptureMaterial::Event)?;
        self.add(CaptureResource::PayloadBytes, bytes)
    }

    /// Admit one blob's exact byte length against the blob cap and the payload cap.
    ///
    /// # Errors
    /// Returns [`CaptureError::LimitExceeded`] for a crossed cap.
    pub fn admit_blob(&mut self, bytes: u64) -> Result<(), CaptureError> {
        self.add(CaptureResource::Blobs, 1)?;
        self.add(CaptureResource::PayloadBytes, bytes)
    }

    /// Admit one projection row against the row cap and the payload cap.
    ///
    /// The row key and the projection declaration are coordinates, not payload.
    ///
    /// # Errors
    /// Returns [`CaptureError::LimitExceeded`] for a crossed cap and [`CaptureError::Corrupt`]
    /// when the stored body cannot be encoded.
    pub fn admit_projection_row(&mut self, body: &Value) -> Result<(), CaptureError> {
        self.add(CaptureResource::ProjectionRows, 1)?;
        let bytes = compact_len(body, CaptureMaterial::Projection)?;
        self.add(CaptureResource::PayloadBytes, bytes)
    }
}

/// The compact UTF-8 length of one stored body. Object key order does not change it.
fn compact_len(value: &Value, material: CaptureMaterial) -> Result<u64, CaptureError> {
    let encoded = serde_json::to_vec(value).map_err(|_| CaptureError::Corrupt { material })?;
    u64::try_from(encoded.len()).map_err(|_| CaptureError::Corrupt { material })
}

/// Check one stored event's constructor-level envelope invariants.
///
/// A row this kit did not write can still be selected by a query. Refusing it is the difference
/// between a capture and a transcription of whatever is in the table.
///
/// # Errors
/// Returns [`CaptureError::Corrupt`] for a stored event that no constructor here would produce.
pub fn validate_captured_event(
    tenant: &TenantId,
    event: &RecordedEvent,
) -> Result<(), CaptureError> {
    let corrupt = || CaptureError::Corrupt {
        material: CaptureMaterial::Event,
    };
    if &event.tenant != tenant || event.version == 0 || event.global_seq == 0 {
        return Err(corrupt());
    }
    StreamId::new(
        event.tenant.clone(),
        event.stream_type.clone(),
        event.stream_id.clone(),
    )
    .map_err(|_| corrupt())?;
    for (what, value) in [
        ("event name", &event.name),
        ("event id", &event.event_id),
        ("request id", &event.request_id),
        ("trace id", &event.trace_id),
    ] {
        validate_field(what, value).map_err(|_| corrupt())?;
    }
    for (what, value) in [("subject", &event.subject), ("actor", &event.actor)] {
        validate_identity(what, value).map_err(|_| corrupt())?;
    }
    if let Some(causation_id) = &event.causation_id {
        validate_field("causation id", causation_id).map_err(|_| corrupt())?;
    }
    if event.causation_depth > MAX_CAUSATION_DEPTH || !event.data.is_object() {
        return Err(corrupt());
    }
    Ok(())
}

/// Check the complete captured event set's position order and per-stream version contiguity.
///
/// Gaps in `global_seq` across tenants and rolled-back allocations are ordinary. A repeated
/// position, or a stream whose versions are not `1..n`, is not: one of them is a row this reader
/// has no business returning as history.
///
/// # Errors
/// Returns [`CaptureError::Corrupt`] for a repeated or unordered position, an inconsistent stream
/// version, or any event that fails [`validate_captured_event`].
pub fn validate_captured_order(
    tenant: &TenantId,
    events: &[RecordedEvent],
) -> Result<(), CaptureError> {
    let corrupt = || CaptureError::Corrupt {
        material: CaptureMaterial::Event,
    };
    let mut position = 0_u64;
    let mut heads: BTreeMap<(&str, &str), u64> = BTreeMap::new();
    for event in events {
        validate_captured_event(tenant, event)?;
        if event.global_seq <= position {
            return Err(corrupt());
        }
        position = event.global_seq;
        let head = heads
            .entry((event.stream_type.as_str(), event.stream_id.as_str()))
            .or_insert(0);
        if head.checked_add(1) != Some(event.version) {
            return Err(corrupt());
        }
        *head = event.version;
    }
    Ok(())
}

/// Check one stored blob digest before it is returned as a binding coordinate.
///
/// The digest stays opaque: this admits exactly what a writer here could have bound, and reads no
/// meaning into the bytes.
///
/// # Errors
/// Returns [`CaptureError::Corrupt`] for a stored digest no writer here would have accepted.
pub fn validate_captured_digest(digest: &str) -> Result<(), CaptureError> {
    validate_field("blob digest", digest).map_err(|_| CaptureError::Corrupt {
        material: CaptureMaterial::Blob,
    })
}

/// Put the captured blobs in bytewise digest order and refuse a repeated binding.
///
/// # Errors
/// Returns [`CaptureError::Corrupt`] when one digest is bound twice in one tenant.
pub fn order_blobs(blobs: &mut [CapturedBlob]) -> Result<(), CaptureError> {
    blobs.sort_by(|left, right| left.digest.as_bytes().cmp(right.digest.as_bytes()));
    if blobs
        .windows(2)
        .any(|pair| pair[0].digest == pair[1].digest)
    {
        return Err(CaptureError::Corrupt {
            material: CaptureMaterial::Blob,
        });
    }
    Ok(())
}

/// Put one projection's rows in bytewise key order and refuse a repeated key.
///
/// The key is returned exactly as it decoded: no trimming, no normalization, no grammar. Empty,
/// whitespace, Unicode and — where a provider admits it — an embedded NUL are all keys a writer
/// here could have produced, so all of them round-trip.
///
/// # Errors
/// Returns [`CaptureError::Corrupt`] when one key appears twice in one projection.
pub fn order_rows(rows: &mut [(String, Value)]) -> Result<(), CaptureError> {
    rows.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(CaptureError::Corrupt {
            material: CaptureMaterial::Projection,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::OffsetDateTime;

    fn limits(value: u64) -> CaptureLimits {
        CaptureLimits {
            max_events: value,
            max_blobs: value,
            max_projection_rows: value,
            max_payload_bytes: value,
        }
    }

    fn recorded(global_seq: u64, version: u64, stream_id: &str) -> RecordedEvent {
        RecordedEvent {
            global_seq,
            tenant: TenantId::new("tenant-1").unwrap(),
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
        }
    }

    #[test]
    fn every_cap_is_exact_including_zero_and_never_saturates() {
        let tenant = TenantId::new("tenant-1").unwrap();
        let event = recorded(1, 1, "one");
        let body = json!({ "value": 1 });
        let exact = compact_len(&body, CaptureMaterial::Event).unwrap();
        assert_eq!(exact, serde_json::to_vec(&body).unwrap().len() as u64);

        // Zero is a cap, not an unset default.
        let mut zero = CaptureBudget::new(limits(0));
        assert_eq!(
            zero.admit_event(&event),
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::Events,
                limit: 0
            })
        );
        assert!(
            CaptureBudget::new(limits(0))
                .proven(CaptureResource::Blobs, 0)
                .is_ok()
        );
        assert_eq!(
            CaptureBudget::new(limits(0)).proven(CaptureResource::Blobs, 1),
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::Blobs,
                limit: 0
            })
        );

        // The payload cap is the sum, to the byte, and one byte short of it refuses.
        let mut fits = CaptureBudget::new(CaptureLimits {
            max_events: 1,
            max_blobs: 1,
            max_projection_rows: 1,
            max_payload_bytes: exact * 2 + 3,
        });
        assert!(fits.admit_event(&event).is_ok());
        assert!(fits.admit_blob(3).is_ok());
        assert!(fits.admit_projection_row(&body).is_ok());
        let mut short = CaptureBudget::new(CaptureLimits {
            max_events: 1,
            max_blobs: 1,
            max_projection_rows: 1,
            max_payload_bytes: exact * 2 + 2,
        });
        assert!(short.admit_event(&event).is_ok());
        assert!(short.admit_blob(3).is_ok());
        assert_eq!(
            short.admit_projection_row(&body),
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::PayloadBytes,
                limit: exact * 2 + 2
            })
        );

        // Overflow is already past every cap there can be; it never becomes an admitted value.
        let mut overflowing = CaptureBudget::new(limits(u64::MAX));
        assert!(overflowing.admit_blob(u64::MAX).is_ok());
        assert_eq!(
            overflowing.admit_blob(1),
            Err(CaptureError::LimitExceeded {
                resource: CaptureResource::PayloadBytes,
                limit: u64::MAX
            })
        );
        assert!(validate_captured_order(&tenant, &[event]).is_ok());
    }

    #[test]
    fn stored_order_identity_and_request_shapes_are_checked() {
        const LEDGER: ProjectionSpec = ProjectionSpec {
            name: "ledger",
            indexed: &["kind"],
        };
        const REPEATED: ProjectionSpec = ProjectionSpec {
            name: "ledger",
            indexed: &["kind", "kind"],
        };
        const INVALID: ProjectionSpec = ProjectionSpec {
            name: "LEDGER",
            indexed: &[],
        };
        let tenant = TenantId::new("tenant-1").unwrap();
        let other = TenantId::new("tenant-2").unwrap();
        let corrupt = Err(CaptureError::Corrupt {
            material: CaptureMaterial::Event,
        });
        assert_eq!(
            validate_captured_order(&tenant, &[recorded(1, 1, "one"), recorded(1, 1, "two")]),
            corrupt,
            "a repeated position is not history"
        );
        assert_eq!(
            validate_captured_order(&tenant, &[recorded(1, 2, "one")]),
            corrupt,
            "a stream starts at version one"
        );
        assert_eq!(
            validate_captured_order(&tenant, &[recorded(1, 1, "one"), recorded(2, 3, "one")]),
            corrupt,
            "a stream is gapless"
        );
        assert!(
            validate_captured_order(
                &tenant,
                &[
                    recorded(1, 1, "one"),
                    recorded(7, 1, "two"),
                    recorded(9, 2, "one")
                ]
            )
            .is_ok(),
            "position gaps across streams are ordinary"
        );
        assert_eq!(
            validate_captured_order(&other, &[recorded(1, 1, "one")]),
            corrupt
        );
        let mut personal = recorded(1, 1, "one");
        personal.subject = "someone@example.com".to_owned();
        assert_eq!(validate_captured_order(&tenant, &[personal]), corrupt);
        let mut bare = recorded(1, 1, "one");
        bare.data = json!(7);
        assert_eq!(validate_captured_order(&tenant, &[bare]), corrupt);

        assert_eq!(
            validate_captured_digest(""),
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Blob
            })
        );
        assert!(validate_captured_digest("opaque").is_ok());

        assert!(validate_capture_request(&tenant, &[]).is_ok());
        assert!(validate_capture_request(&tenant, &[LEDGER]).is_ok());
        for (request, reason) in [
            (
                vec![LEDGER, LEDGER],
                CaptureRequestRefusal::DuplicateProjection,
            ),
            (vec![REPEATED], CaptureRequestRefusal::DuplicateIndexedField),
            (
                vec![INVALID],
                CaptureRequestRefusal::InvalidProjectionSpecification,
            ),
        ] {
            assert_eq!(
                validate_capture_request(&tenant, &request),
                Err(CaptureError::InvalidRequest { reason })
            );
        }
    }

    #[test]
    fn captured_content_is_ordered_bytewise_and_never_repeats_a_coordinate() {
        let mut blobs = vec![
            CapturedBlob {
                digest: "b".to_owned(),
                bytes: vec![2],
            },
            CapturedBlob {
                digest: "A".to_owned(),
                bytes: vec![1],
            },
        ];
        order_blobs(&mut blobs).unwrap();
        assert_eq!(
            blobs
                .iter()
                .map(|blob| blob.digest.as_str())
                .collect::<Vec<_>>(),
            ["A", "b"],
            "bytewise, not a locale's idea of order"
        );
        assert_eq!(
            order_blobs(&mut [
                CapturedBlob {
                    digest: "same".to_owned(),
                    bytes: Vec::new()
                },
                CapturedBlob {
                    digest: "same".to_owned(),
                    bytes: Vec::new()
                },
            ]),
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Blob
            })
        );

        let mut rows = vec![
            ("é".to_owned(), json!(1)),
            (String::new(), json!(2)),
            (" ".to_owned(), json!(3)),
            ("Z".to_owned(), json!(4)),
        ];
        order_rows(&mut rows).unwrap();
        assert_eq!(
            rows.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
            ["", " ", "Z", "é"]
        );
        assert_eq!(
            order_rows(&mut [(String::new(), json!(1)), (String::new(), json!(2))]),
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Projection
            })
        );
    }
}
