//! Ordered multi-stream appends, independently selectable from the legacy single-stream port.
use crate::{
    AppendResult, BoxFuture, CommandMeta, EventLogError, EventStore, Expected, Guard, NewEvent,
    NoGuard, StreamId, TenantId, request_hash, validate_append,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

/// One ordered entry; repeated streams observe earlier entries in the same transaction.
#[derive(Clone, Debug)]
pub struct StreamAppend {
    pub stream: StreamId,
    pub expected: Expected,
    pub events: Vec<NewEvent>,
}

/// One tenant-scoped command identity covers the complete ordered request.
#[derive(Clone, Debug)]
pub struct AppendGroup {
    pub tenant: TenantId,
    pub appends: Vec<StreamAppend>,
    pub meta: CommandMeta,
}

impl AppendGroup {
    /// Validate all entries and hash actual content, not just the caller's claimed digest.
    ///
    /// # Errors
    /// Refuses empty groups, cross-tenant entries, invalid events/metadata and stream claims.
    /// A stream claim names one result range and cannot represent an atomic group.
    pub fn fingerprint(&self) -> Result<String, EventLogError> {
        self.meta.validate()?;
        if self.appends.is_empty() {
            return Err(EventLogError::Invalid(
                "an append group must contain entries".into(),
            ));
        }
        if self.meta.claim.is_some() {
            return Err(EventLogError::Invalid(
                "stream claims cannot name an append group".into(),
            ));
        }
        let mut entries = Vec::with_capacity(self.appends.len());
        for append in &self.appends {
            if append.stream.tenant() != &self.tenant {
                return Err(EventLogError::Invalid(
                    "append group crosses tenants".into(),
                ));
            }
            validate_append(&append.events, &self.meta)?;
            let expected = match append.expected {
                Expected::Any => json!(["any"]),
                Expected::NoStream => json!(["no-stream"]),
                Expected::Exact(version) => json!(["exact", version]),
                // The head set is part of the request: two merges over different heads are
                // different requests, even under one idempotency key.
                Expected::Merge(heads) => json!(["merge", heads.to_hex()]),
            };
            entries
                .push(json!({"stream":append.stream,"expected":expected,"events":append.events}));
        }
        // Exhaustive destructuring makes an added metadata field require an explicit hash decision.
        let CommandMeta {
            idempotency_key,
            request_hash: digest,
            subject,
            actor,
            request_id,
            trace_id,
            causation_id,
            causation_depth,
            occurred_at,
            claim: _,
        } = &self.meta;
        request_hash(
            &json!({"format":"eventlog-append-group/1","tenant":self.tenant,
            "entries":entries,"meta":{"key":idempotency_key,"digest":digest,"subject":subject,
            "actor":actor,"request_id":request_id,"trace_id":trace_id,"causation_id":causation_id,
            "causation_depth":causation_depth,"occurred_at":occurred_at}}),
        )
    }
}

/// Ordered entry results. A retry has the original IDs, positions and version ranges.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendGroupResult {
    pub appends: Vec<AppendResult>,
    pub deduplicated: bool,
}

/// Durable bookkeeping stores coordinates, never a second copy of event bodies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRange {
    pub stream: StreamId,
    pub first_version: u64,
    pub last_version: u64,
}

/// What [`AtomicEventStore::append_group_guarded_with_blobs`]'s default refuses with.
///
/// A stable phrase rather than a new error variant: a provider that has not implemented the
/// single-barrier form is refusing a capability, which is what [`EventLogError::Invalid`] already
/// carries elsewhere in this kit (`ProjectionStore::get_blob`'s default says "transaction blob
/// reads are unavailable" in the same shape). A caller matches on it to decide whether to fall
/// back to a blob at a time; the conformance exercise matches on it to decide which half of the
/// contract applies to the provider in front of it.
pub const UNAVAILABLE: &str = "guarded blob-bearing append groups are unavailable";

/// A provider must implement one transaction; there is deliberately no append-loop fallback.
pub trait AtomicEventStore: EventStore {
    fn append_group<'a>(
        &'a self,
        group: &'a AppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        self.append_group_guarded(group, Arc::new(NoGuard))
    }

    /// Admission runs once, before entries, in the same transaction as their inline projectors.
    /// Retries return the original result without repeating admission or projections.
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;

    /// Commit a guarded group together with the blobs it binds, under the group's own barrier.
    ///
    /// This is [`AtomicEventStore::append_group_guarded`] and [`EventStore::put_blob`] in one
    /// transaction, for the caller that has both a guard and a batch of blobs — a migration
    /// importing many boundaries at once is the case it exists for, and the reason it is on the
    /// port rather than on one provider is that such a caller holds a trait object and cannot
    /// name a provider's inherent method.
    ///
    /// **The guarantee is the same on every provider, because a provider that cannot make it
    /// refuses.** Admission runs before a byte of the batch is written, so a refused guard
    /// publishes neither the group nor a blob. A caller holding a trait object cannot name the
    /// provider underneath it, so a default that published first and asked afterwards would make
    /// this method mean one thing on one provider and the opposite on another — and
    /// content-addressed storage has no way to take a published blob back. A blob row is a
    /// binding, not scratch: nothing reference-counts it and nothing sweeps it.
    ///
    /// **The default therefore fails closed.** It writes nothing, commits nothing and refuses
    /// with [`UNAVAILABLE`] — this provider does not implement guarded blob-bearing groups. A
    /// caller that wants the slow path can still write each blob with [`EventStore::put_blob`]
    /// and then call [`AtomicEventStore::append_group_guarded`], which is the same sequence the
    /// old default ran; what it cannot do is get that sequence under a name that promises the
    /// batch was not published when the guard refused.
    ///
    /// **A retry that carries a batch is admitted again, so the guard must tolerate its own
    /// commit.** The batch is part of what such a retry is asking about, and a store that
    /// answered it before running the guard would let anyone replaying a committed key learn
    /// whether a digest is bound and whether bytes match. So admission runs first, which means a
    /// guard can be presented a second time with a group it already admitted and committed: it
    /// has to be idempotent over that, and refuse only what is genuinely somebody else's. A guard
    /// that reads "this subject is occupied" without checking *by what* will refuse the retry
    /// that idempotency exists to serve. A retry carrying **no** batch — every caller of
    /// [`AtomicEventStore::append_group_guarded`] — is unaffected and does not repeat admission.
    ///
    /// # Errors
    /// [`EventLogError::Invalid`] carrying [`UNAVAILABLE`] from the default. From a provider that
    /// implements it: whatever the group and each blob refuse on their own paths, publishing
    /// nothing on any refusing path. From a guard re-run on a batch-bearing retry: whatever that
    /// guard refuses.
    fn append_group_guarded_with_blobs<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        let _ = (group, admission, blobs);
        Box::pin(async { Err(EventLogError::Invalid(UNAVAILABLE.into())) })
    }
}
