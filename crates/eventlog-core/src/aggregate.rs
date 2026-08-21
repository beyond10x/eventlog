//! Deciding, folding, and the cache that keeps a fold from starting at zero every time.
//!
//! An aggregate is the smallest thing with its own lifecycle — an item, not the corpus it is in.
//! It never touches a connection: `decide` reads the state it was folded into and returns facts,
//! and the repository is the only thing that knows a store exists.

use std::{marker::PhantomData, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use time::OffsetDateTime;

use crate::{
    AppendResult, CommandMeta, EventLogError, EventStore, Expected, MAX_READ_LIMIT, NewEvent,
    RecordedEvent, Snapshot, StreamId, TenantId,
};

/// A domain event that knows how it is written down and how it is read back.
///
/// `from_data` is where an upcaster runs: an event type is permanent, so a body written under an
/// older schema version must still become today's value rather than a parse error at three in the
/// morning.
pub trait DomainEvent: Sized {
    /// The stable key this event is stored and published under.
    fn name(&self) -> &'static str;

    /// The schema version this value writes.
    fn schema_version(&self) -> u32;

    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the value cannot be written as a JSON object.
    fn to_data(&self) -> Result<Value, EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when a stored body cannot be read as this type, which
    /// means an upcaster is missing rather than that the caller did anything wrong.
    fn from_data(name: &str, schema_version: u32, data: &Value) -> Result<Self, EventLogError>;
}

/// What `apply` receives.
///
/// A redacted event is a distinct case rather than an absent one. A stream folds differently after
/// an erasure, and an aggregate that could not be told would either crash or quietly pretend the
/// fact never happened.
#[derive(Debug)]
pub enum Applied<'a, E> {
    Happened {
        event: &'a E,
        recorded: &'a RecordedEvent,
    },
    Redacted {
        recorded: &'a RecordedEvent,
    },
}

impl<E> Applied<'_, E> {
    #[must_use]
    pub fn recorded(&self) -> &RecordedEvent {
        match self {
            Self::Happened { recorded, .. } | Self::Redacted { recorded } => recorded,
        }
    }
}

/// One lifecycle, folded from its own history.
pub trait Aggregate: Sized + Send + Sync + Serialize + DeserializeOwned {
    /// What a caller asks for.
    type Command;
    /// What this aggregate decided.
    type Event: DomainEvent;
    /// Why a decision was refused.
    type Error: From<EventLogError>;

    /// The stream type every instance of this aggregate is stored under.
    const TYPE: &'static str;

    /// Bumped when the snapshot shape changes. An older snapshot is discarded, never repaired.
    const STATE_SCHEMA_VERSION: u32;

    /// The state before anything happened.
    fn empty(id: &str) -> Self;

    /// Fold one fact in. Must be total: it is called for every event in history, including
    /// redacted ones, and it may not fail.
    fn apply(&mut self, applied: &Applied<'_, Self::Event>);

    /// Decide what a command means for this state, without touching anything.
    ///
    /// # Errors
    /// Returns the domain's own refusal. A command that changes nothing should return an empty
    /// list rather than an error, and the repository will not write.
    fn decide(&self, command: &Self::Command) -> Result<Vec<Self::Event>, Self::Error>;
}

/// A folded aggregate and where its history had reached.
#[derive(Debug, Clone)]
pub struct Loaded<A> {
    pub state: A,
    pub version: u64,
    /// True when nothing has ever been written to this stream.
    pub is_new: bool,
}

/// What handling a command did.
#[derive(Debug, Clone)]
pub struct Outcome<A> {
    pub state: A,
    pub version: u64,
    /// Empty when the command decided nothing, in which case nothing was written.
    pub events: Vec<RecordedEvent>,
    /// True when this command had already been recorded and the stored result was returned.
    pub deduplicated: bool,
}

/// How often a fold is cached.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotPolicy {
    /// Write a snapshot once this many events have accumulated since the last one.
    pub every: u64,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self { every: 100 }
    }
}

/// Loads aggregates, runs commands, and writes what they decided.
pub struct Repository<A: Aggregate> {
    store: Arc<dyn EventStore>,
    policy: SnapshotPolicy,
    aggregate: PhantomData<fn() -> A>,
}

impl<A: Aggregate> Repository<A> {
    #[must_use]
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self {
            store,
            policy: SnapshotPolicy::default(),
            aggregate: PhantomData,
        }
    }

    #[must_use]
    pub fn with_policy(mut self, policy: SnapshotPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The stream one instance of this aggregate lives in.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable id.
    pub fn stream(&self, tenant: &TenantId, id: &str) -> Result<StreamId, EventLogError> {
        StreamId::new(tenant.clone(), A::TYPE, id)
    }

    /// Fold one aggregate: newest snapshot, then everything after it.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable, or
    /// [`EventLogError::Invalid`] when a stored body cannot be read as this aggregate's event.
    pub fn load(&self, tenant: &TenantId, id: &str) -> Result<Loaded<A>, EventLogError> {
        let stream = self.stream(tenant, id)?;
        let (mut state, mut version) = match self.store.load_snapshot(&stream)? {
            Some(snapshot) if snapshot.state_schema_version == A::STATE_SCHEMA_VERSION => {
                match serde_json::from_value::<A>(snapshot.state) {
                    Ok(state) => (state, snapshot.version),
                    // A snapshot is a cache. One that cannot be read is discarded and the fold
                    // restarts from zero rather than being repaired into a plausible lie.
                    Err(_) => (A::empty(id), 0),
                }
            }
            _ => (A::empty(id), 0),
        };
        let mut seen_any = version > 0;
        loop {
            let slice = self.store.read_stream(&stream, version, MAX_READ_LIMIT)?;
            if slice.events.is_empty() {
                break;
            }
            seen_any = true;
            for recorded in &slice.events {
                apply_recorded(&mut state, recorded)?;
                version = recorded.version;
            }
            if slice.end_of_stream {
                break;
            }
        }
        Ok(Loaded {
            state,
            version,
            is_new: !seen_any,
        })
    }

    /// Load, decide, and write what was decided.
    ///
    /// A conflict is retried exactly once, because the common cause is two callers touching one
    /// aggregate at the same moment. A second conflict is refused rather than looped on: at that
    /// point something is contending that this aggregate boundary did not anticipate, and hiding
    /// it in a retry loop turns a design problem into a latency problem nobody can see.
    ///
    /// # Errors
    /// Returns the domain's refusal from `decide`, or a store error.
    pub fn handle(
        &self,
        tenant: &TenantId,
        id: &str,
        command: &A::Command,
        meta: &CommandMeta,
    ) -> Result<Outcome<A>, A::Error> {
        self.handle_guarded(tenant, id, command, meta, &crate::NoGuard)
    }

    /// Load, decide, check an invariant that spans streams, and write.
    ///
    /// The guard runs inside the append transaction against inline read models, which is what
    /// makes a limit like "this corpus is full" exact without serialising every write in the
    /// corpus onto one stream.
    ///
    /// # Errors
    /// Returns the guard's refusal, the domain's refusal from `decide`, or a store error.
    pub fn handle_guarded(
        &self,
        tenant: &TenantId,
        id: &str,
        command: &A::Command,
        meta: &CommandMeta,
        guard: &dyn crate::Guard,
    ) -> Result<Outcome<A>, A::Error> {
        let stream = self.stream(tenant, id)?;
        if let Some(recorded) =
            self.store
                .recorded_command(&stream, &meta.idempotency_key, &meta.request_hash)?
        {
            let loaded = self.load(tenant, id)?;
            return Ok(Outcome {
                state: loaded.state,
                version: loaded.version,
                events: recorded.events,
                deduplicated: true,
            });
        }
        let mut attempt = 0;
        loop {
            let loaded = self.load(tenant, id)?;
            let decided = loaded.state.decide(command)?;
            if decided.is_empty() {
                return Ok(Outcome {
                    state: loaded.state,
                    version: loaded.version,
                    events: Vec::new(),
                    deduplicated: false,
                });
            }
            let expected = if loaded.is_new {
                Expected::NoStream
            } else {
                Expected::Exact(loaded.version)
            };
            let new_events = to_new_events(&decided)?;
            match self
                .store
                .append_guarded(&stream, expected, &new_events, meta, guard)
            {
                Ok(result) => return Ok(self.finish(loaded, result, tenant, id)?),
                Err(EventLogError::Conflict { .. }) if attempt == 0 => {
                    attempt += 1;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn finish(
        &self,
        loaded: Loaded<A>,
        result: AppendResult,
        tenant: &TenantId,
        id: &str,
    ) -> Result<Outcome<A>, EventLogError> {
        let mut state = loaded.state;
        let mut version = loaded.version;
        for recorded in &result.events {
            if recorded.version > version {
                apply_recorded(&mut state, recorded)?;
                version = recorded.version;
            }
        }
        if self.policy.every > 0 && version.is_multiple_of(self.policy.every) {
            let stream = self.stream(tenant, id)?;
            let snapshot = Snapshot {
                version,
                state_schema_version: A::STATE_SCHEMA_VERSION,
                state: serde_json::to_value(&state).map_err(|error| {
                    EventLogError::Invalid(format!("aggregate is not serialisable: {error}"))
                })?,
                recorded_at: OffsetDateTime::now_utc(),
            };
            self.store.save_snapshot(&stream, &snapshot)?;
        }
        Ok(Outcome {
            state,
            version,
            events: result.events,
            deduplicated: result.deduplicated,
        })
    }

    /// Write a snapshot of this aggregate now, whatever the policy says.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    pub fn snapshot_now(&self, tenant: &TenantId, id: &str) -> Result<u64, EventLogError> {
        let loaded = self.load(tenant, id)?;
        let stream = self.stream(tenant, id)?;
        let snapshot = Snapshot {
            version: loaded.version,
            state_schema_version: A::STATE_SCHEMA_VERSION,
            state: serde_json::to_value(&loaded.state).map_err(|error| {
                EventLogError::Invalid(format!("aggregate is not serialisable: {error}"))
            })?,
            recorded_at: OffsetDateTime::now_utc(),
        };
        self.store.save_snapshot(&stream, &snapshot)?;
        Ok(loaded.version)
    }
}

fn apply_recorded<A: Aggregate>(
    state: &mut A,
    recorded: &RecordedEvent,
) -> Result<(), EventLogError> {
    if recorded.is_redacted() {
        state.apply(&Applied::Redacted { recorded });
        return Ok(());
    }
    let event = A::Event::from_data(&recorded.name, recorded.schema_version, &recorded.data)?;
    state.apply(&Applied::Happened {
        event: &event,
        recorded,
    });
    Ok(())
}

fn to_new_events<E: DomainEvent>(events: &[E]) -> Result<Vec<NewEvent>, EventLogError> {
    events
        .iter()
        .map(|event| NewEvent::new(event.name(), event.schema_version(), event.to_data()?))
        .collect()
}
