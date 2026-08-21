//! Read models, and the two ways they are driven.
//!
//! Event sourcing gives an owner no queries. Every list, filter and lookup is a projection over
//! the log, which means a wrong read model is a Tuesday rather than an incident: drop it and
//! rebuild it. What it must not become is a hand-maintained table the log cannot reproduce — the
//! moment one exists, the log has stopped being the record.

use std::sync::Arc;

use serde_json::Value;

use crate::{EventLogError, RecordedEvent, TenantId};

/// A read model's table and the fields it can be looked up by.
///
/// Indexed fields are declared here rather than written as SQL, so a projection author writes
/// none. A field that is not declared is not queryable, which is a decision somebody makes here
/// instead of an index that appears in production.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionSpec {
    pub name: &'static str,
    pub indexed: &'static [&'static str],
}

impl ProjectionSpec {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for a name or field that cannot be a table or column.
    pub fn validate(&self) -> Result<(), EventLogError> {
        validate_identifier("projection name", self.name)?;
        if self.indexed.len() > MAX_INDEXED_FIELDS {
            return Err(EventLogError::Invalid(format!(
                "a projection may declare at most {MAX_INDEXED_FIELDS} indexed fields"
            )));
        }
        for field in self.indexed {
            validate_identifier("indexed field", field)?;
        }
        Ok(())
    }

    /// The position of a declared field, for a backend to map onto its column.
    #[must_use]
    pub fn field_position(&self, field: &str) -> Option<usize> {
        self.indexed.iter().position(|name| *name == field)
    }
}

/// The most fields one read model may be looked up by.
pub const MAX_INDEXED_FIELDS: usize = 8;

/// Where a projection writes, without knowing which database it is in.
pub trait ProjectionStore {
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the write fails.
    fn upsert(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
        body: &Value,
    ) -> Result<(), EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the delete fails.
    fn delete(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<(), EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the read fails.
    fn get(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError>;

    /// Read a row and hold it for the rest of this transaction.
    ///
    /// This is how an invariant that spans streams is enforced: a guard reads a counter row here,
    /// inside the append transaction, so the limit is exact and contention is one row rather than
    /// one stream.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the projection is not driven inline — a guard over
    /// a lagging read model is a limit that is enforced late, which is not a limit.
    fn get_for_update(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError>;

    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the field was not declared indexed.
    fn find(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        field: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<Value>, EventLogError>;
}

/// Turns facts into a read model.
pub trait Projector: Send + Sync {
    /// The name this projection is registered, cursored and locked under.
    fn name(&self) -> &'static str;

    /// Every table this projector owns. They are created on registration and dropped on rebuild.
    fn projections(&self) -> &'static [ProjectionSpec];

    /// Fold one fact into the read model.
    ///
    /// Called at least once per event, so this must be an upsert or an otherwise repeatable
    /// write. It is never called twice concurrently for one projection.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when a write fails. An inline projector that errors
    /// aborts the append: a broken read model refuses the write rather than diverging quietly.
    fn apply(
        &self,
        event: &RecordedEvent,
        store: &mut dyn ProjectionStore,
    ) -> Result<(), EventLogError>;
}

/// A check run inside the append transaction, against inline read models.
pub trait Guard {
    /// # Errors
    /// Returns the refusal. The append is abandoned and nothing is written.
    fn check(&self, store: &mut dyn ProjectionStore) -> Result<(), EventLogError>;
}

/// A guard that allows everything, which is what an append without one uses.
pub struct NoGuard;

impl Guard for NoGuard {
    fn check(&self, _store: &mut dyn ProjectionStore) -> Result<(), EventLogError> {
        Ok(())
    }
}

impl<F> Guard for F
where
    F: Fn(&mut dyn ProjectionStore) -> Result<(), EventLogError>,
{
    fn check(&self, store: &mut dyn ProjectionStore) -> Result<(), EventLogError> {
        self(store)
    }
}

/// How far a catch-up projection has read, and what it does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatchUpProgress {
    pub applied: u64,
    pub position: u64,
    /// True when the feed had more waiting than this pass took.
    pub more_waiting: bool,
}

/// Drives one catch-up projection from a durable position.
///
/// It holds the projection's lock for the duration of a pass, so a second replica is safe by
/// construction rather than by configuration.
pub struct CatchUpRunner {
    store: Arc<dyn crate::EventStore>,
    projector: Arc<dyn Projector>,
    batch: usize,
}

impl CatchUpRunner {
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when this projector is registered inline. A projection
    /// is one or the other, never both, or every event lands in it twice.
    pub fn new(
        store: Arc<dyn crate::EventStore>,
        projector: Arc<dyn Projector>,
    ) -> Result<Self, EventLogError> {
        if store.is_inline(projector.name()) {
            return Err(EventLogError::Invalid(format!(
                "projection {} is already driven inline",
                projector.name()
            )));
        }
        store.create_projections(projector.as_ref())?;
        Ok(Self {
            store,
            projector,
            batch: 200,
        })
    }

    #[must_use]
    pub fn with_batch(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Apply everything waiting for one tenant, in one pass.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable, or whatever the projector
    /// returned.
    pub fn run_once(&self, tenant: &TenantId) -> Result<CatchUpProgress, EventLogError> {
        self.store
            .run_catch_up(self.projector.as_ref(), tenant, self.batch)
    }

    /// Apply everything waiting for one tenant, however many passes that takes.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    pub fn drain(&self, tenant: &TenantId) -> Result<u64, EventLogError> {
        let mut applied = 0;
        loop {
            let progress = self.run_once(tenant)?;
            applied += progress.applied;
            if !progress.more_waiting || progress.applied == 0 {
                return Ok(applied);
            }
        }
    }
}

/// # Errors
/// Returns [`EventLogError::Invalid`] when the value cannot be part of a table or column name.
pub fn validate_identifier(what: &str, value: &str) -> Result<(), EventLogError> {
    if value.is_empty()
        || value.len() > 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        || value.starts_with(|character: char| character.is_ascii_digit())
    {
        return Err(EventLogError::Invalid(format!(
            "{what} is lowercase ASCII, digits and underscores, not starting with a digit"
        )));
    }
    Ok(())
}

/// The value a declared field takes for one row, as the backend stores it.
#[must_use]
pub fn indexed_value(body: &Value, field: &str) -> Option<String> {
    match body.get(field) {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}
