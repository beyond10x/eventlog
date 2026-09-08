//! Read models, and the two ways they are driven.
//!
//! Event sourcing gives an owner no queries. Every list, filter and lookup is a projection over
//! the log, which means a wrong read model is a Tuesday rather than an incident: drop it and
//! rebuild it. What it must not become is a hand-maintained table the log cannot reproduce — the
//! moment one exists, the log has stopped being the record.

use std::sync::Arc;

use serde_json::Value;

use crate::{BoxFuture, EventLogError, RecordedEvent, TenantId};

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
///
/// `Send` is a supertrait so that a guard's or projector's future — which holds one of these
/// across its awaits — can itself be `Send`.
pub trait ProjectionStore: Send {
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the write fails.
    fn upsert<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
        body: &'a Value,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the delete fails.
    fn delete<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;

    /// # Errors
    /// Returns [`EventLogError::Backend`] when the read fails.
    fn get<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>>;

    /// Read a row and hold it for the rest of this transaction.
    ///
    /// This is how an invariant that spans streams is enforced: a guard reads a counter row here,
    /// inside the append transaction, so the limit is exact and contention is one row rather than
    /// one stream.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the projection is not driven inline — a guard over
    /// a lagging read model is a limit that is enforced late, which is not a limit.
    fn get_for_update<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>>;

    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the field was not declared indexed.
    fn find<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>>;
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
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;
}

/// A check run inside the append transaction, against inline read models.
///
/// `Send + Sync` are supertraits because a backend may run the check beside its connection, on
/// another thread than the caller's.
pub trait Guard: Send + Sync {
    /// # Errors
    /// Returns [`EventLogError::GuardRefused`] with the domain owner's stable refusal code when
    /// the command is rejected, or the relevant store error when the check cannot be completed.
    /// Either result abandons the append and rolls back every projection write the guard made.
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;
}

/// A guard that allows everything, which is what an append without one uses.
pub struct NoGuard;

impl Guard for NoGuard {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async { Ok(()) })
    }
}

impl<F> Guard for F
where
    F: for<'a> Fn(&'a mut dyn ProjectionStore) -> BoxFuture<'a, Result<(), EventLogError>>
        + Send
        + Sync,
{
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
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
    pub async fn new(
        store: Arc<dyn crate::EventStore>,
        projector: Arc<dyn Projector>,
    ) -> Result<Self, EventLogError> {
        if store.is_inline(projector.name()).await {
            return Err(EventLogError::Invalid(format!(
                "projection {} is already driven inline",
                projector.name()
            )));
        }
        store.create_projections(Arc::clone(&projector)).await?;
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
    pub async fn run_once(&self, tenant: &TenantId) -> Result<CatchUpProgress, EventLogError> {
        self.store
            .run_catch_up(Arc::clone(&self.projector), tenant, self.batch)
            .await
    }

    /// Apply everything waiting for one tenant, however many passes that takes.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the store is unavailable.
    pub async fn drain(&self, tenant: &TenantId) -> Result<u64, EventLogError> {
        let mut applied = 0;
        loop {
            let progress = self.run_once(tenant).await?;
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
