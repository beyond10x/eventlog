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
}
