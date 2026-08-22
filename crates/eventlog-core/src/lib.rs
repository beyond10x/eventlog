#![forbid(unsafe_code)]

//! What a durable fact is, and the port that stores one.
//!
//! Every mutation in a Daemonloom owner is a command that produces domain events, and every read
//! is a fold over those events. This crate holds the vocabulary that makes that one thing rather
//! than six: the envelope a fact is recorded in, the stream it belongs to, the concurrency check
//! that lets an aggregate hold an invariant, and the store port two backends implement.
//!
//! There is no "current tenant" here and no constructor that omits one, so reading another
//! tenant's history is not a permission that was withheld — it is a call that cannot be written.

mod aggregate;
mod projection;

pub use aggregate::{Aggregate, Applied, DomainEvent, Loaded, Outcome, Repository, SnapshotPolicy};
pub use projection::{
    CatchUpProgress, CatchUpRunner, Guard, MAX_INDEXED_FIELDS, NoGuard, ProjectionSpec,
    ProjectionStore, Projector, indexed_value, validate_identifier,
};

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

/// The longest any identity, key or name may be.
pub const MAX_FIELD_LEN: usize = 512;

/// The most events one command may produce.
///
/// A command that wants more than this is a batch job wearing a command's clothes, and it would
/// hold the stream's write lock for as long as it takes.
pub const MAX_EVENTS_PER_APPEND: usize = 1024;

/// How many events a reader gets when it does not say.
pub const DEFAULT_READ_LIMIT: usize = 100;

/// The most events any single read returns.
pub const MAX_READ_LIMIT: usize = 1000;

/// How far a chain of automation may go before the kit refuses the next append.
///
/// Automation A emits a command whose event triggers automation B whose event triggers A is not a
/// hypothetical; it is the first production incident of every rules engine. The depth is carried
/// on the envelope from the first release so that the guard does not require migrating every
/// owner's table later.
pub const MAX_CAUSATION_DEPTH: u32 = 16;

/// Whose history this is.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the value is empty, over-long, or carries bytes
    /// that cannot be stored and logged verbatim.
    pub fn new(value: impl Into<String>) -> Result<Self, EventLogError> {
        let value = value.into();
        validate_field("tenant", &value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One aggregate's history: whose it is, what kind of thing it is, and which one.
///
/// The three parts are separate fields rather than a joined string because a joined string is a
/// place for one owner to forget the tenant and for another to choose a different separator.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct StreamId {
    tenant: TenantId,
    stream_type: String,
    stream_id: String,
}

impl StreamId {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the type or id is empty, over-long, or carries
    /// bytes that cannot be stored and logged verbatim.
    pub fn new(
        tenant: TenantId,
        stream_type: impl Into<String>,
        stream_id: impl Into<String>,
    ) -> Result<Self, EventLogError> {
        let stream_type = stream_type.into();
        let stream_id = stream_id.into();
        validate_field("stream type", &stream_type)?;
        validate_field("stream id", &stream_id)?;
        Ok(Self {
            tenant,
            stream_type,
            stream_id,
        })
    }

    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    #[must_use]
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }
}

/// What the caller believes the stream's head is.
///
/// This is where an aggregate's invariants are actually enforced. A store that offered only
/// last-write-wins could not hold one, because two commands that each read a valid state would
/// both be allowed to write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expected {
    /// Append whatever the head is. Correct only when the events carry no decision.
    Any,
    /// The stream must not exist yet.
    NoStream,
    /// The stream's head must be exactly this version.
    Exact(u64),
}

/// A fact a command decided on, before the store gave it a place in history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEvent {
    pub name: String,
    pub schema_version: u32,
    pub data: Value,
}

impl NewEvent {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable event name, or for a body that is not a
    /// JSON object. A body that is a bare number or string cannot grow a field later without
    /// breaking every reader, and an event type is permanent.
    pub fn new(
        name: impl Into<String>,
        schema_version: u32,
        data: Value,
    ) -> Result<Self, EventLogError> {
        let name = name.into();
        validate_field("event name", &name)?;
        if !data.is_object() {
            return Err(EventLogError::Invalid(
                "an event body must be a JSON object".to_owned(),
            ));
        }
        Ok(Self {
            name,
            schema_version,
            data,
        })
    }
}

/// A caller's claim on an idempotency key, across every stream.
///
/// The kit's own idempotency is scoped to one stream, which is right when the caller names the
/// record it is writing to. It is not right when the *store* mints the identifier: two attempts at
/// one create then land in two different streams, and a per-stream check cannot see they are the
/// same request. Modules that mint identifiers rebuilt this by hand — twice, identically — before
/// it lived here.
///
/// `scope` is whatever the owner scopes a key to, usually the caller. `digest` is of what the
/// caller asked for, never of the record the store minted: the identifier is new on every attempt,
/// so hashing it would make every retry a different request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub scope: String,
    pub key: String,
    pub digest: String,
}

impl Claim {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when a part is empty, over-long, or carries bytes that
    /// cannot be stored and logged verbatim.
    pub fn new(
        scope: impl Into<String>,
        key: impl Into<String>,
        digest: impl Into<String>,
    ) -> Result<Self, EventLogError> {
        let value = Self {
            scope: scope.into(),
            key: key.into(),
            digest: digest.into(),
        };
        validate_field("claim scope", &value.scope)?;
        validate_field("claim key", &value.key)?;
        validate_field("claim digest", &value.digest)?;
        Ok(value)
    }
}

/// What a caller's earlier claim on a key produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedCommand {
    pub stream: StreamId,
    pub first_version: u64,
    pub last_version: u64,
}

/// Who issued a command, under which key, and what caused it.
///
/// Carried on every event the command produces, so that every stored fact can name a person and an
/// automated write is distinguishable from a person's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandMeta {
    /// Retrying an interrupted command must not write its events twice.
    pub idempotency_key: String,
    /// A digest of the command body. The same key with a different body is refused rather than
    /// answered with the earlier result, which is the difference between a safe retry and a
    /// silently wrong answer to a changed request.
    pub request_hash: String,
    /// The person the work is for.
    pub subject: String,
    /// The agent or service doing it, which is often not the same as `subject`.
    pub actor: String,
    pub request_id: String,
    pub trace_id: String,
    /// The event that caused this command, when something automated issued it.
    pub causation_id: Option<String>,
    pub causation_depth: u32,
    /// When the thing happened, as the caller understands it. The store stamps its own
    /// `recorded_at` and never orders by this one.
    pub occurred_at: OffsetDateTime,
    /// A caller-scoped claim on this key, for an owner that mints the record identifier itself.
    ///
    /// Recorded in the append transaction, so the claim and the facts it produced cannot disagree.
    pub claim: Option<Claim>,
}

impl CommandMeta {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when an identity or key is empty, over-long, or carries
    /// bytes that cannot be stored and logged verbatim.
    pub fn validate(&self) -> Result<(), EventLogError> {
        validate_field("idempotency key", &self.idempotency_key)?;
        validate_field("request hash", &self.request_hash)?;
        validate_identity("subject", &self.subject)?;
        validate_identity("actor", &self.actor)?;
        validate_field("request id", &self.request_id)?;
        validate_field("trace id", &self.trace_id)?;
        if let Some(causation_id) = &self.causation_id {
            validate_field("causation id", causation_id)?;
        }
        if self.causation_depth > MAX_CAUSATION_DEPTH {
            return Err(EventLogError::CausationDepthExceeded {
                depth: self.causation_depth,
                limit: MAX_CAUSATION_DEPTH,
            });
        }
        Ok(())
    }

    /// Derive the meta for a command an event caused.
    ///
    /// The idempotency key is the causing event's id, which is what makes at-least-once delivery
    /// safe: redelivering an event produces the same command result rather than a second one.
    #[must_use]
    pub fn caused_by(mut self, cause: &RecordedEvent) -> Self {
        self.idempotency_key.clone_from(&cause.event_id);
        self.causation_id = Some(cause.event_id.clone());
        self.causation_depth = cause.causation_depth.saturating_add(1);
        self
    }
}

/// A stable digest of a command body, for the idempotency check.
///
/// # Errors
/// Returns [`EventLogError::Invalid`] when the value cannot be serialised.
pub fn request_hash<T: Serialize>(value: &T) -> Result<String, EventLogError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| EventLogError::Invalid(format!("command is not serialisable: {error}")))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

/// A new event id. Time-ordered, so a debug listing sorts the way history happened.
#[must_use]
pub fn new_event_id() -> String {
    Uuid::now_v7().to_string()
}

/// One fact, as history holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedEvent {
    /// Position in this owner's table. Gaps are expected; see the reader's watermark.
    pub global_seq: u64,
    pub tenant: TenantId,
    pub stream_type: String,
    pub stream_id: String,
    /// Position in this stream, from 1, gapless.
    pub version: u64,
    pub event_id: String,
    pub name: String,
    pub schema_version: u32,
    pub occurred_at: OffsetDateTime,
    pub recorded_at: OffsetDateTime,
    pub subject: String,
    pub actor: String,
    pub request_id: String,
    pub trace_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: u32,
    pub redacted_at: Option<OffsetDateTime>,
    pub data: Value,
}

impl RecordedEvent {
    /// Whether this event's body has been erased.
    ///
    /// An aggregate's `apply` must stay total over one of these. A stream folds differently after a
    /// redaction, which is why the snapshots that summarised it are deleted with it.
    #[must_use]
    pub fn is_redacted(&self) -> bool {
        self.redacted_at.is_some()
    }

    /// The stream this event belongs to.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the stored identity is not a usable stream id,
    /// which means the row was written by something other than this kit.
    pub fn stream(&self) -> Result<StreamId, EventLogError> {
        StreamId::new(
            self.tenant.clone(),
            self.stream_type.clone(),
            self.stream_id.clone(),
        )
    }
}

/// The body a redacted event keeps, so that a reader can still say what is missing and why.
#[must_use]
pub fn redaction_tombstone(reason: &str) -> Value {
    json!({ "redacted": true, "reason": reason })
}

/// What an append did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub first_version: u64,
    pub last_version: u64,
    pub events: Vec<RecordedEvent>,
    /// True when this command had already been recorded and the stored result was returned.
    pub deduplicated: bool,
}

/// A window on one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamSlice {
    pub events: Vec<RecordedEvent>,
    /// The version a caller should ask from next.
    pub next_version: u64,
    pub end_of_stream: bool,
}

/// One page of a projection, with the cursor that continues it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionPage {
    pub rows: Vec<(String, Value)>,
    /// The key to resume from, or `None` when this page is the end.
    pub next_cursor: Option<String>,
}

/// A window on one tenant's whole history, in commit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedPage {
    pub events: Vec<RecordedEvent>,
    /// The position a reader should resume from. Opaque to consumers.
    pub next_position: u64,
    pub has_more: bool,
}

/// A fold, cached.
///
/// A snapshot is never the record. One that fails to deserialise is discarded and the fold
/// restarts from zero, because a snapshot that is repaired in place is a lie with a timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u64,
    pub state_schema_version: u32,
    pub state: Value,
    pub recorded_at: OffsetDateTime,
}

/// Everything that can go wrong that a caller must tell apart.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EventLogError {
    #[error("{0}")]
    Invalid(String),
    #[error("stream is at version {actual}, not {expected}")]
    Conflict { expected: u64, actual: u64 },
    #[error("idempotency key {key} was already used for a different request")]
    IdempotencyMismatch { key: String },
    #[error("causation depth {depth} exceeds the limit of {limit}")]
    CausationDepthExceeded { depth: u32, limit: u32 },
    #[error("no event at that version")]
    NotFound,
    #[error("event store is unavailable: {0}")]
    Backend(String),
}

/// Where facts are kept.
///
/// Two backends implement this and one exercise defines what they must agree on. In-memory is
/// SQLite `:memory:`, so there is no third implementation for the other two to diverge from.
pub trait EventStore: Send + Sync + 'static {
    /// Record what a command decided.
    ///
    /// # Errors
    /// Returns [`EventLogError::Conflict`] when the stream moved under the caller,
    /// [`EventLogError::IdempotencyMismatch`] when the key was used for a different body, and
    /// [`EventLogError::Invalid`] for an unusable command or an empty event list.
    fn append(
        &self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
    ) -> Result<AppendResult, EventLogError>;

    /// What a caller's earlier claim on this key produced, if anything.
    ///
    /// # Errors
    /// Returns [`EventLogError::IdempotencyMismatch`] when the key was claimed for a different
    /// request — the same key with a different body is a different request wearing the first
    /// one's name.
    fn recorded_claim(
        &self,
        tenant: &TenantId,
        claim: &Claim,
    ) -> Result<Option<ClaimedCommand>, EventLogError>;

    /// What a command with this key already produced, if it has run before.
    ///
    /// A retried command must be answered, not decided again. Deciding again would refuse the
    /// retry — "that corpus already exists" — which is the opposite of what a retry means and is
    /// exactly what a caller who lost the first response would see.
    ///
    /// # Errors
    /// Returns [`EventLogError::IdempotencyMismatch`] when the key was used for a different body.
    fn recorded_command(
        &self,
        stream: &StreamId,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<AppendResult>, EventLogError>;

    /// Read one stream forward, for a fold.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn read_stream(
        &self,
        stream: &StreamId,
        after_version: u64,
        limit: usize,
    ) -> Result<StreamSlice, EventLogError>;

    /// The stream's head, or `None` when it does not exist.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn stream_version(&self, stream: &StreamId) -> Result<Option<u64>, EventLogError>;

    /// Read a tenant's history in commit order, stopping short of anything still in flight.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn read_feed(
        &self,
        tenant: &TenantId,
        after_position: u64,
        limit: usize,
    ) -> Result<FeedPage, EventLogError>;

    /// Erase one event's body, keeping its id, version and place.
    ///
    /// # Errors
    /// Returns [`EventLogError::NotFound`] when the stream has no such version.
    fn redact(
        &self,
        stream: &StreamId,
        version: u64,
        reason: &str,
    ) -> Result<RecordedEvent, EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn save_snapshot(&self, stream: &StreamId, snapshot: &Snapshot) -> Result<(), EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn load_snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>, EventLogError>;

    /// Remove everything this kit holds for a tenant, in one transaction.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn forget_tenant(&self, tenant: &TenantId) -> Result<(), EventLogError>;

    /// Append with a check that runs inside the same transaction, against inline read models.
    ///
    /// # Errors
    /// Returns the guard's refusal, or whatever [`EventStore::append`] would have.
    fn append_guarded(
        &self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        guard: &dyn Guard,
    ) -> Result<AppendResult, EventLogError>;

    /// Create a projector's tables. Idempotent.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable projection declaration.
    fn create_projections(&self, projector: &dyn Projector) -> Result<(), EventLogError>;

    /// Drive this projector inside every append transaction from now on.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when a projection of that name is already registered.
    fn register_inline(&self, projector: Arc<dyn Projector>) -> Result<(), EventLogError>;

    /// Whether a projection of this name is driven inline.
    fn is_inline(&self, name: &str) -> bool;

    /// Apply one batch of a catch-up projection under its own lock.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn run_catch_up(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
        batch: usize,
    ) -> Result<CatchUpProgress, EventLogError>;

    /// Drop a projector's tables and replay the whole log into them.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn rebuild_projection(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
    ) -> Result<u64, EventLogError>;

    /// Read one projection row.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn projection_get(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError>;

    /// Read projection rows by a declared field.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the field was not declared indexed.
    fn projection_find(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        field: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<Value>, EventLogError>;

    /// Read a page of projection rows in key order.
    ///
    /// Every list a person sees is one of these. The cursor is the last key returned, so a list
    /// that grows while somebody is paging through it does not repeat or skip rows.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn projection_list(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Value)>, EventLogError>;

    /// One page of a projection, with the cursor that continues it.
    ///
    /// Five modules wrote this by hand and two of them got the cursor rule wrong until a test said
    /// so, which is the argument for having one. The rule: fetch one more row than asked for,
    /// return the asked-for number, and hand back the **last returned** key — not the last fetched
    /// one, and `None` rather than a cursor when the page was short. A cursor on a short page
    /// costs the caller an extra round trip that returns nothing; a cursor from the last fetched
    /// key skips a row.
    ///
    /// `prefix` bounds the page to one contiguous run of keys — one corpus's items, one project's
    /// entities, one owner's workspaces — because a projection is tenant-wide and a list is
    /// usually not. It is exclusive of the prefix itself, so a projection whose keys are
    /// `<owner>/<id>` passes `"<owner>/"` and gets every row under that owner and nothing else.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn projection_page(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ProjectionPage, EventLogError> {
        let limit = bounded_limit(limit);
        let prefix = prefix.unwrap_or("");
        let after = cursor.map_or_else(|| prefix.to_owned(), str::to_owned);
        let fetched = self.projection_list(projection, tenant, Some(&after), limit + 1)?;
        let mut rows = Vec::with_capacity(limit);
        let mut more = false;
        for (key, body) in fetched {
            if !key.starts_with(prefix) {
                break;
            }
            if rows.len() == limit {
                more = true;
                break;
            }
            rows.push((key, body));
        }
        let next_cursor = more
            .then(|| rows.last().map(|(key, _)| key.clone()))
            .flatten();
        Ok(ProjectionPage { rows, next_cursor })
    }

    /// A stable identifier for this tenant's stream in this owner.
    ///
    /// A reader that resumes from a position must first check the stream is the one it was
    /// reading. After a restore from backup the positions repeat, and a reader that did not notice
    /// would silently skip everything since.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn stream_identity(&self, tenant: &TenantId) -> Result<String, EventLogError>;

    /// Store bytes under their own digest.
    ///
    /// Content lives here rather than in the log, because an append-only record of somebody's
    /// upload is a record nobody can erase. An event names the digest; the bytes are erasable on
    /// their own, and erasing them leaves the fact that a file arrived intact.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the digest is unusable.
    fn put_blob(&self, tenant: &TenantId, digest: &str, bytes: &[u8]) -> Result<(), EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn get_blob(&self, tenant: &TenantId, digest: &str) -> Result<Option<Vec<u8>>, EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    fn delete_blob(&self, tenant: &TenantId, digest: &str) -> Result<(), EventLogError>;
}

/// What a caller asked for, clamped to what a page may be.
#[must_use]
pub fn bounded_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_READ_LIMIT)
}

/// # Errors
/// Returns [`EventLogError::Invalid`] when the value is empty, over-long, or carries bytes that
/// cannot be stored and logged verbatim.
pub fn validate_field(what: &str, value: &str) -> Result<(), EventLogError> {
    if value.is_empty() {
        return Err(EventLogError::Invalid(format!("{what} is required")));
    }
    if value.len() > MAX_FIELD_LEN {
        return Err(EventLogError::Invalid(format!("{what} is too long")));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(EventLogError::Invalid(format!(
            "{what} carries bytes that cannot be stored and logged verbatim"
        )));
    }
    Ok(())
}

/// An identity on the envelope is an opaque id, never a person's name, email or handle.
///
/// The log is append-only, so whatever goes in here outlives every request to erase it. Keeping
/// identities opaque is what lets a person be forgotten in the identity directory while the
/// attribution that makes the log auditable survives them. An address is refused rather than
/// stored, because by the time somebody notices, it is history.
///
/// # Errors
/// Returns [`EventLogError::Invalid`] when the value is not usable, or looks like an address or a
/// display name rather than an id.
pub fn validate_identity(what: &str, value: &str) -> Result<(), EventLogError> {
    validate_field(what, value)?;
    if value.contains('@') || value.contains(' ') {
        return Err(EventLogError::Invalid(format!(
            "{what} must be an opaque id: an address or display name in an append-only log \
             outlives every request to erase it"
        )));
    }
    Ok(())
}

/// Check an append request before any backend touches a connection.
///
/// # Errors
/// Returns [`EventLogError::Invalid`] for an empty or over-large event list, or an unusable
/// command meta.
pub fn validate_append(events: &[NewEvent], meta: &CommandMeta) -> Result<(), EventLogError> {
    if events.is_empty() {
        return Err(EventLogError::Invalid(
            "a command that decided nothing has nothing to append".to_owned(),
        ));
    }
    if events.len() > MAX_EVENTS_PER_APPEND {
        return Err(EventLogError::Invalid(format!(
            "a command may produce at most {MAX_EVENTS_PER_APPEND} events"
        )));
    }
    meta.validate()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> CommandMeta {
        CommandMeta {
            idempotency_key: "key-1".to_owned(),
            request_hash: "hash-1".to_owned(),
            subject: "person-1".to_owned(),
            actor: "service-1".to_owned(),
            request_id: "request-1".to_owned(),
            trace_id: "trace-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            occurred_at: OffsetDateTime::UNIX_EPOCH,
            claim: None,
        }
    }

    #[test]
    fn a_stream_cannot_be_named_without_a_tenant() {
        assert!(TenantId::new("").is_err());
        let tenant = TenantId::new("tenant-1").expect("valid tenant");
        assert!(StreamId::new(tenant, "", "id").is_err());
    }

    #[test]
    fn an_event_body_must_be_an_object() {
        assert!(NewEvent::new("item.received", 1, json!(7)).is_err());
        assert!(NewEvent::new("item.received", 1, json!({})).is_ok());
    }

    #[test]
    fn a_command_with_no_events_is_refused() {
        assert!(validate_append(&[], &meta()).is_err());
    }

    #[test]
    fn depth_beyond_the_limit_is_refused() {
        let mut value = meta();
        value.causation_depth = MAX_CAUSATION_DEPTH + 1;
        assert!(matches!(
            value.validate(),
            Err(EventLogError::CausationDepthExceeded { .. })
        ));
    }

    #[test]
    fn an_identity_that_names_a_person_is_refused() {
        let mut value = meta();
        value.subject = "someone@example.com".to_owned();
        assert!(
            matches!(value.validate(), Err(EventLogError::Invalid(_))),
            "an address in an append-only log outlives every request to erase it"
        );
        let mut value = meta();
        value.actor = "Ada Lovelace".to_owned();
        assert!(matches!(value.validate(), Err(EventLogError::Invalid(_))));
    }

    #[test]
    fn the_same_body_hashes_the_same_way() {
        let first = request_hash(&json!({"a": 1, "b": 2})).expect("hashable");
        let second = request_hash(&json!({"a": 1, "b": 2})).expect("hashable");
        let other = request_hash(&json!({"a": 1, "b": 3})).expect("hashable");
        assert_eq!(first, second);
        assert_ne!(first, other);
    }
}
