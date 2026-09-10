#![forbid(unsafe_code)]

//! The log on PostgreSQL, and the watermark that keeps a reader from skipping.
//!
//! A sequence hands out a position at insert time, but transactions commit out of order, so a
//! reader can see position 10 before 9 commits and will never come back for 9. What that costs is
//! a projection permanently missing one row, with no error anywhere and a cursor that looks
//! healthy. Readers here stop at the commit watermark instead, so an event stays invisible until
//! every transaction older than it has finished. One slow writer delays the tail; nothing is ever
//! skipped.
//!
//! The driver is `tokio-postgres`, natively async. The sync `postgres` wrapper this backend used
//! to ride starts a runtime of its own on every call, which panics the moment a caller is already
//! inside one — "Cannot start a runtime from within a runtime", at startup, in every module that
//! forgot a `spawn_blocking` wrap. There is no wrap to forget now; callers just `.await`.
//!
//! `xid8`, `pg_current_xact_id()` and `pg_snapshot_xmin()` require PostgreSQL 13 or later.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use eventlog_core::{
    AppendResult, BoxFuture, CatchUpProgress, Claim, ClaimedCommand, CommandMeta, EventLogError,
    EventStore, Expected, FeedPage, Guard, MAX_READ_LIMIT, NewEvent, NoGuard, ProjectionQuery,
    ProjectionSpec, ProjectionStore, Projector, RecordedEvent, Snapshot, SnapshotGeneration,
    StreamId, StreamSlice, TenantId, bounded_limit, indexed_value, new_event_id,
    redaction_tombstone, validate_append, validate_field,
};
use serde_json::Value;
use time::OffsetDateTime;
mod atomic_group;
mod documents;
mod pool;
mod schema;
use pool::Pool;
pub use pool::{PoolOptions, PoolStatus, PostgresConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_postgres::{GenericClient, Row, Transaction};

/// Every envelope column, in the order [`read_event`] expects them.
const COLUMNS: &str = "global_seq, tenant_id, stream_type, stream_id, version, event_id, \
     event_name, event_schema_version, occurred_at, recorded_at, subject, actor, request_id, \
     trace_id, causation_id, causation_depth, redacted_at, data";

/// The predicate that keeps a feed reader behind anything still in flight.
const WATERMARK: &str = "committed_xid < pg_snapshot_xmin(pg_current_snapshot())";

/// One owner's event tables in one PostgreSQL database.
pub struct PostgresEventStore {
    pool: Arc<Pool>,
    allow_schema_writes: bool,
    registration: tokio::sync::Mutex<()>,
    frozen: AtomicBool,
    admission_permit: eventlog_core::AdmissionPermit,
    prefix: String,
    inline: Mutex<Vec<Arc<dyn Projector>>>,
    inline_names: Mutex<BTreeSet<String>>,
}

impl PostgresEventStore {
    /// Connect and create this owner's tables if they are not there.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable prefix and [`EventLogError::Backend`]
    /// when the database cannot be reached or the tables cannot be created.
    pub async fn connect(url: &str, prefix: &str) -> Result<Self, EventLogError> {
        Self::connect_local(url, prefix, PoolOptions::default()).await
    }

    /// Explicit isolated-test constructor; hosted applications use `open` after `migrate`.
    /// # Errors
    /// Refuses nonlocal plaintext, unsupported schema and invalid resource bounds.
    pub async fn connect_local(
        url: &str,
        prefix: &str,
        options: PoolOptions,
    ) -> Result<Self, EventLogError> {
        Self::local(PostgresConfig::isolated(url, prefix)?, options).await
    }

    /// Open an isolated database, including an explicitly selected test-owned schema.
    /// # Errors
    /// Refuses hosted configurations or incompatible schemas.
    pub async fn local(
        config: PostgresConfig,
        options: PoolOptions,
    ) -> Result<Self, EventLogError> {
        if config.production {
            return Err(EventLogError::Invalid(
                "hosted configurations require explicit migration and budget admission".into(),
            ));
        }
        let prefix = config.prefix.clone();
        let pool = Pool::new(config, options)?;
        {
            let mut client = pool.acquire().await?;
            schema::migrate(&mut client, &prefix, &[]).await?;
            client.settled();
        }
        Ok(Self::from_pool(pool, prefix, true))
    }

    /// Apply the additive schema with the migration role, including declared projection shapes.
    /// # Errors
    /// Refuses unknown/partial schema and serializes concurrent migrators before any DDL.
    pub async fn migrate(
        config: PostgresConfig,
        options: PoolOptions,
        projections: &[ProjectionSpec],
    ) -> Result<(), EventLogError> {
        Self::migrate_with_document_projections(config, options, projections, &[]).await
    }

    /// Add document-query indexes to the named declared projections using the migration role.
    /// Existing scalar indexes and document bodies are preserved. Registration with a hosted
    /// DML role requires this explicit upgrade before serving a document-query projector.
    /// # Errors
    /// Refuses undeclared names, incompatible roster or physical schema, and failed migration.
    pub async fn migrate_with_document_projections(
        config: PostgresConfig,
        options: PoolOptions,
        projections: &[ProjectionSpec],
        documents: &[&str],
    ) -> Result<(), EventLogError> {
        let prefix = config.prefix.clone();
        let pool = Pool::new(config, options)?;
        let result = async {
            let mut client = pool.acquire().await?;
            schema::migrate_with_documents(&mut client, &prefix, projections, documents).await?;
            client.settled();
            Ok(())
        }
        .await;
        pool.shutdown().await?;
        result
    }

    /// Open a verified hosted database with a DML-only application role.
    /// `replicas * max_connections + reserved_connections` must fit the observed DB budget.
    /// # Errors
    /// Refuses unverified transport, missing/exceeded budgets, DDL-capable roles or schema drift.
    pub async fn open(
        config: PostgresConfig,
        options: PoolOptions,
        database_connections: usize,
        replicas: usize,
        reserved_connections: usize,
    ) -> Result<Self, EventLogError> {
        let required = replicas
            .checked_mul(options.max_connections)
            .and_then(|value| value.checked_add(reserved_connections));
        if !config.production
            || replicas == 0
            || reserved_connections == 0
            || required.is_none_or(|value| value > database_connections)
        {
            return Err(EventLogError::Invalid(
                "hosted PostgreSQL transport or deployment connection budget is not admitted"
                    .into(),
            ));
        }
        let prefix = config.prefix.clone();
        let pool = Pool::new(config, options)?;
        {
            let mut client = pool.acquire().await?;
            let can_ddl:bool=client.query_one("SELECT COALESCE(has_schema_privilege(current_user,current_schema(),'CREATE'),true) OR EXISTS(SELECT FROM pg_roles WHERE rolname=current_user AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)) OR EXISTS(SELECT FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)) OR EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND c.relkind IN ('r','p','S','v','m','f'))",&[]).await.map_err(backend)?.get(0);
            if can_ddl {
                return Err(EventLogError::Invalid(
                    "hosted application role must not own schema DDL".into(),
                ));
            }
            let role_limit: i32 = client
                .query_one(
                    "SELECT rolconnlimit FROM pg_roles WHERE rolname=current_user",
                    &[],
                )
                .await
                .map_err(backend)?
                .get(0);
            if role_limit <= 0
                || usize::try_from(role_limit)
                    .ok()
                    .is_none_or(|limit| limit > replicas * pool.options.max_connections)
            {
                return Err(EventLogError::Invalid("hosted application role requires a finite connection limit within its admitted replica pool share".into()));
            }
            schema::validate(&mut client, &prefix).await?;
            schema::permissions(&client, &prefix).await?;
            client.settled();
        }
        Ok(Self::from_pool(pool, prefix, false))
    }
    fn from_pool(pool: Arc<Pool>, prefix: String, allow_schema_writes: bool) -> Self {
        Self {
            pool,
            prefix,
            allow_schema_writes,
            registration: tokio::sync::Mutex::new(()),
            frozen: AtomicBool::new(false),
            admission_permit: eventlog_core::AdmissionPermit::default(),
            inline: Mutex::new(Vec::new()),
            inline_names: Mutex::new(BTreeSet::new()),
        }
    }
    /// Issue the owner host's transaction-admission grant before handing the store to domains.
    /// Keep this permit in trusted control guards, never in caller input or domain projectors.
    pub fn admission_permit(&self) -> eventlog_core::AdmissionPermit {
        self.admission_permit.clone()
    }
    /// Observe configured and currently held pool bounds.
    pub fn pool_status(&self) -> PoolStatus {
        self.pool.status()
    }
    /// Stop new acquisitions and wait a bounded interval for active leases to finish.
    /// # Errors
    /// A deadline refuses while outstanding work remains; new traffic stays closed.
    pub async fn shutdown(&self) -> Result<(), EventLogError> {
        self.pool.shutdown().await
    }
    /// Seal startup registration before exposing any service traffic.
    pub async fn seal(&self) {
        self.freeze().await;
    }
    async fn freeze(&self) {
        let _registration = self.registration.lock().await;
        self.frozen.store(true, Ordering::Release);
    }
    fn bounded<'a, T: Send + 'a>(
        &'a self,
        future: impl std::future::Future<Output = Result<T, EventLogError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, EventLogError>> {
        Box::pin(async move {
            tokio::time::timeout(self.pool.options.transaction_timeout, future)
                .await
                .map_err(|_| EventLogError::Deadline {
                    operation: "store operation",
                })?
        })
    }

    /// Drop this owner's tables, cursors included. For test setup only.
    ///
    /// Leaving the cursors behind is not a small omission: a cursor that outlives its events sits
    /// past the end of the new log, and the projection then reports nothing waiting, forever,
    /// with no error.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the database cannot be reached.
    pub async fn drop_tables(&self) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        self.pool
            .acquire()
            .await?
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {prefix}_events;
                 DROP TABLE IF EXISTS {prefix}_append_groups;
                 DROP TABLE IF EXISTS {prefix}_commands;
                 DROP TABLE IF EXISTS {prefix}_claims;
                 DROP TABLE IF EXISTS {prefix}_snapshots;
                 DROP TABLE IF EXISTS {prefix}_snapshot_generations;
                 DROP TABLE IF EXISTS {prefix}_projection_cursors;
                 DROP TABLE IF EXISTS {prefix}_blobs;
                 DROP TABLE IF EXISTS {prefix}_identity;
                 DROP TABLE IF EXISTS {prefix}_scope_counters;
                 DROP TABLE IF EXISTS {prefix}_schema_version;
                 DROP TABLE IF EXISTS {prefix}_projection_registry;"
            ))
            .await
            .map_err(backend)
    }
}

impl EventStore for PostgresEventStore {
    fn append<'a>(
        &'a self,
        stream: &'a StreamId,
        expected: Expected,
        events: &'a [NewEvent],
        meta: &'a CommandMeta,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        self.append_guarded(stream, expected, events, meta, Arc::new(NoGuard))
    }

    fn append_guarded<'a>(
        &'a self,
        stream: &'a StreamId,
        expected: Expected,
        events: &'a [NewEvent],
        meta: &'a CommandMeta,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        Box::pin(async move {
            tokio::time::timeout(self.pool.options.transaction_timeout, async move {
                validate_append(events, meta)?;
                self.freeze().await;
                let mut client = self.pool.acquire().await?;
                client.quarantine();
                let transaction = client.transaction().await.map_err(backend)?;
                publication_gate(&transaction, &self.prefix, false).await?;
                let result = self
                    .append_in_transaction(
                        &transaction,
                        stream,
                        expected,
                        events,
                        meta,
                        admission.as_ref(),
                        true,
                    )
                    .await;
                match result {
                    Ok(result) => {
                        transaction
                            .commit()
                            .await
                            .map_err(|_| EventLogError::UnknownCommit)?;
                        client.settled();
                        Ok(result)
                    }
                    Err(error) => {
                        transaction.rollback().await.map_err(backend)?;
                        client.settled();
                        Err(error)
                    }
                }
            })
            .await
            .map_err(|_| EventLogError::UnknownCommit)?
        })
    }

    fn recorded_claim<'a>(
        &'a self,
        tenant: &'a TenantId,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Option<ClaimedCommand>, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let row = client
                .query_opt(
                    &format!(
                        "SELECT request_digest, stream_type, stream_id, first_version, last_version
                         FROM {prefix}_claims
                         WHERE tenant_id = $1 AND scope = $2 AND claim_key = $3"
                    ),
                    &[&tenant.as_str(), &claim.scope, &claim.key],
                )
                .await
                .map_err(backend)?;
            client.settled();
            let Some(row) = row else {
                return Ok(None);
            };
            let digest: String = row.get(0);
            if digest != claim.digest {
                return Err(EventLogError::IdempotencyMismatch {
                    key: claim.key.clone(),
                });
            }
            let stream_type: String = row.get(1);
            let stream_id: String = row.get(2);
            let first_version: i64 = row.get(3);
            let last_version: i64 = row.get(4);
            Ok(Some(ClaimedCommand {
                stream: StreamId::new(tenant.clone(), stream_type, stream_id)?,
                first_version: to_u64(first_version)?,
                last_version: to_u64(last_version)?,
            }))
        })
    }

    fn recorded_command<'a>(
        &'a self,
        stream: &'a StreamId,
        idempotency_key: &'a str,
        request_hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<AppendResult>, EventLogError>> {
        self.bounded(async move {
            let prefix = self.prefix.clone();
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            let recorded = transaction
                .query_opt(
                    &format!(
                        "SELECT request_hash, first_version, last_version FROM {prefix}_commands
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                           AND idempotency_key = $4"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &idempotency_key,
                    ],
                )
                .await
                .map_err(backend)?;
            let Some(row) = recorded else {
                transaction.rollback().await.map_err(backend)?;
                client.settled();
                return Ok(None);
            };
            let stored_hash: String = row.get(0);
            let first_version: i64 = row.get(1);
            let last_version: i64 = row.get(2);
            if stored_hash != request_hash {
                return Err(EventLogError::IdempotencyMismatch {
                    key: idempotency_key.to_owned(),
                });
            }
            let events =
                select_versions(&transaction, &prefix, stream, first_version, last_version).await?;
            transaction.rollback().await.map_err(backend)?;
            client.settled();
            Ok(Some(AppendResult {
                first_version: to_u64(first_version)?,
                last_version: to_u64(last_version)?,
                events,
                deduplicated: true,
            }))
        })
    }

    fn read_stream<'a>(
        &'a self,
        stream: &'a StreamId,
        after_version: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        self.bounded(async move {
            let limit = bounded_limit(limit);
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let rows = client
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM {prefix}_events
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                           AND version > $4
                         ORDER BY version LIMIT $5"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &to_i64(after_version)?,
                        &to_i64(limit as u64 + 1)?,
                    ],
                )
                .await
                .map_err(backend)?;
            client.settled();
            let mut events = rows
                .iter()
                .map(read_event)
                .collect::<Result<Vec<_>, EventLogError>>()?;
            let end_of_stream = events.len() <= limit;
            events.truncate(limit);
            let next_version = events.last().map_or(after_version, |event| event.version);
            Ok(StreamSlice {
                events,
                next_version,
                end_of_stream,
            })
        })
    }

    fn stream_version<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let head: Option<i64> = client
                .query_one(
                    &format!(
                        "SELECT MAX(version) FROM {prefix}_events
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                    ],
                )
                .await
                .map_err(backend)?
                .get(0);
            client.settled();
            head.map(to_u64).transpose()
        })
    }

    fn list_streams<'a>(
        &'a self,
        tenant: &'a TenantId,
        stream_type: &'a str,
        after_id: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StreamId>, EventLogError>> {
        self.bounded(async move {
            StreamId::new(tenant.clone(), stream_type, after_id.unwrap_or("_"))?;
            let mut client = self.pool.acquire().await?;
            // Point inventory uses one READ COMMITTED statement and the existing stream index.
            // A feed watermark would hide successful writes behind unrelated transactions.
            let rows = client.query(
                &format!("SELECT DISTINCT stream_id FROM {}_events WHERE tenant_id=$1 AND stream_type=$2 AND stream_id > $3 ORDER BY stream_id LIMIT $4", self.prefix),
                &[&tenant.as_str(), &stream_type, &after_id.unwrap_or(""), &to_i64(bounded_limit(limit) as u64)?],
            ).await.map_err(backend)?;
            client.settled();
            rows.iter().map(|row| StreamId::new(tenant.clone(), stream_type, row.get::<_, String>(0))).collect()
        })
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after_position: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        self.bounded(async move {
            let limit = bounded_limit(limit);
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, &self.prefix, true).await?;
            let rows = transaction
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM {}
                         WHERE tenant_id = $1 AND global_seq > $2 AND {WATERMARK}
                           AND (first_unsettled IS NULL OR global_seq < first_unsettled)
                         ORDER BY global_seq LIMIT $3",
                        watermarked_source(prefix)
                    ),
                    &[
                        &tenant.as_str(),
                        &to_i64(after_position)?,
                        &to_i64(limit as u64 + 1)?,
                    ],
                )
                .await
                .map_err(backend)?;
            transaction.commit().await.map_err(backend)?;
            client.settled();
            let mut events = rows
                .iter()
                .map(read_event)
                .collect::<Result<Vec<_>, EventLogError>>()?;
            let has_more = events.len() > limit;
            events.truncate(limit);
            let next_position = events
                .last()
                .map_or(after_position, |event| event.global_seq);
            Ok(FeedPage {
                events,
                next_position,
                has_more,
            })
        })
    }

    fn redact<'a>(
        &'a self,
        stream: &'a StreamId,
        version: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<RecordedEvent, EventLogError>> {
        self.bounded(async move {
            validate_field("redaction reason", reason)?;
            let prefix = self.prefix.clone();
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, &self.prefix, false).await?;
            let now = OffsetDateTime::now_utc();
            transaction.execute(&format!("INSERT INTO {prefix}_snapshot_generations (tenant_id,stream_type,stream_id,generation) VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,stream_type,stream_id) DO UPDATE SET generation=EXCLUDED.generation"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id(), &uuid::Uuid::now_v7()]).await.map_err(backend)?;
            let changed = transaction
                .execute(
                    &format!(
                        "UPDATE {prefix}_events SET data = $5, redacted_at = $6
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                           AND version = $4"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &to_i64(version)?,
                        &redaction_tombstone(reason),
                        &now,
                    ],
                )
                .await
                .map_err(backend)?;
            if changed == 0 {
                return Err(EventLogError::NotFound);
            }
            transaction
                .execute(
                    &format!(
                        "DELETE FROM {prefix}_snapshots
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                           AND version >= $4"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &to_i64(version)?,
                    ],
                )
                .await
                .map_err(backend)?;
            let mut events = select_versions(
                &transaction,
                &prefix,
                stream,
                to_i64(version)?,
                to_i64(version)?,
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(|_| EventLogError::UnknownCommit)?;
            client.settled();
            events.pop().ok_or(EventLogError::NotFound)
        })
    }

    fn save_snapshot<'a>(
        &'a self,
        _stream: &'a StreamId,
        _snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async {
            Err(EventLogError::Invalid("snapshot provenance is required; capture snapshot_generation before folding and use save_snapshot_checked".into()))
        })
    }

    fn snapshot_generation<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<SnapshotGeneration>, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, prefix, false).await?;
            transaction.execute(&format!("INSERT INTO {prefix}_snapshot_generations (tenant_id,stream_type,stream_id,generation) VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,stream_type,stream_id) DO NOTHING"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id(), &uuid::Uuid::now_v7()]).await.map_err(backend)?;
            let generation: uuid::Uuid = transaction.query_one(&format!("SELECT generation FROM {prefix}_snapshot_generations WHERE tenant_id=$1 AND stream_type=$2 AND stream_id=$3"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id()]).await.map_err(backend)?.get(0);
            transaction.commit().await.map_err(|_| EventLogError::UnknownCommit)?;
            client.settled();
            Ok(Some(SnapshotGeneration::from_uuid(generation)))
        })
    }

    fn save_snapshot_checked<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
        generation: &'a SnapshotGeneration,
    ) -> BoxFuture<'a, Result<bool, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, prefix, false).await?;
            let accepted = transaction.execute(&format!("UPDATE {prefix}_snapshot_generations SET cached_generation=generation WHERE tenant_id=$1 AND stream_type=$2 AND stream_id=$3 AND generation=$4"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id(), generation.as_uuid()]).await.map_err(backend)?;
            if accepted == 0 {
                transaction.rollback().await.map_err(backend)?;
                client.settled();
                return Ok(false);
            }
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_snapshots (
                             tenant_id, stream_type, stream_id, version, state_schema_version,
                             state, recorded_at)
                         VALUES ($1, $2, $3, $4, $5, $6, $7)
                         ON CONFLICT (tenant_id, stream_type, stream_id) DO UPDATE SET
                             version = EXCLUDED.version,
                             state_schema_version = EXCLUDED.state_schema_version,
                             state = EXCLUDED.state,
                             recorded_at = EXCLUDED.recorded_at"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &to_i64(snapshot.version)?,
                        &to_i32(snapshot.state_schema_version)?,
                        &snapshot.state,
                        &snapshot.recorded_at,
                    ],
                )
                .await
                .map_err(backend)?;
            transaction.commit().await.map_err(|_| EventLogError::UnknownCommit)?;
            client.settled();
            Ok(true)
        })
    }

    fn load_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let row = client
                .query_opt(
                    &format!(
                        "SELECT version, state_schema_version, state, recorded_at
                         FROM {prefix}_snapshots JOIN {prefix}_snapshot_generations
                         USING (tenant_id, stream_type, stream_id)
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                         AND cached_generation = generation"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                    ],
                )
                .await
                .map_err(backend)?;
            client.settled();
            let Some(row) = row else {
                return Ok(None);
            };
            let version: i64 = row.get(0);
            let state_schema_version: i32 = row.get(1);
            let state: Value = row.get(2);
            let recorded_at: OffsetDateTime = row.get(3);
            Ok(Some(Snapshot {
                version: to_u64(version)?,
                state_schema_version: to_u32(state_schema_version)?,
                state,
                recorded_at,
            }))
        })
    }

    fn forget_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.bounded(async move {
            let prefix = self.prefix.clone();
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, &self.prefix, true).await?;
            for table in [
                "events",
                "append_groups",
                "commands",
                "claims",
                "identity",
                "snapshots",
                "snapshot_generations",
                "projection_cursors",
                "blobs",
            ] {
                transaction
                    .execute(
                        &format!("DELETE FROM {prefix}_{table} WHERE tenant_id = $1"),
                        &[&tenant.as_str()],
                    )
                    .await
                    .map_err(backend)?;
            }
            // Every read model this owner has ever created, not only the ones registered in this
            // process. A projection table left behind after an erasure is the erased tenant, still
            // readable, in a table nobody thought to name.
            let rows = transaction
                .query(
                    &format!("SELECT projection_name FROM {prefix}_projection_registry"),
                    &[],
                )
                .await
                .map_err(backend)?;
            for row in rows {
                let name: String = row.get(0);
                eventlog_core::validate_identifier("registered projection", &name)?;
                let table = projection_table(&prefix, &name);
                transaction
                    .execute(
                        &format!("DELETE FROM {table} WHERE tenant_id=$1"),
                        &[&tenant.as_str()],
                    )
                    .await
                    .map_err(backend)?;
            }
            let coordinate_prefix = format!("t{}:{}", tenant.as_str().len(), tenant.as_str());
            transaction
                .execute(
                    &format!(
                        "DELETE FROM {prefix}_scope_counters WHERE left(coordinate,length($1))=$1"
                    ),
                    &[&coordinate_prefix],
                )
                .await
                .map_err(backend)?;
            transaction
                .commit()
                .await
                .map_err(|_| EventLogError::UnknownCommit)?;
            client.settled();
            Ok(())
        })
    }

    fn projection_list<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after_key: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        self.bounded(async move {
            let limit = bounded_limit(limit);
            projection.validate()?;
            let table = projection_table(&self.prefix, projection.name);
            let mut client = self.pool.acquire().await?;
            let rows = client
                .query(
                    &format!(
                        "SELECT row_key, body FROM {table}
                         WHERE tenant_id = $1 AND row_key > $2
                         ORDER BY row_key LIMIT $3"
                    ),
                    &[
                        &tenant.as_str(),
                        &after_key.unwrap_or(""),
                        &to_i64(limit as u64)?,
                    ],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
        })
    }

    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let identity = new_event_id();
            let mut client = self.pool.acquire().await?;
            let row = client
                .query_one(
                    &format!(
                        "INSERT INTO {prefix}_identity (tenant_id, stream_identity)
                         VALUES ($1, $2)
                         ON CONFLICT (tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id
                         RETURNING stream_identity"
                    ),
                    &[&tenant.as_str(), &identity],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(row.get(0))
        })
    }

    fn put_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.bounded(async move {
            validate_field("digest", digest)?;
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_blobs (tenant_id, digest, bytes, byte_count,
                                                     recorded_at)
                         VALUES ($1, $2, $3, $4, $5)
                         ON CONFLICT (tenant_id, digest) DO NOTHING"
                    ),
                    &[
                        &tenant.as_str(),
                        &digest,
                        &bytes,
                        &to_i64(bytes.len() as u64)?,
                        &OffsetDateTime::now_utc(),
                    ],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(())
        })
    }

    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            let row = client
                .query_opt(
                    &format!(
                        "SELECT bytes FROM {prefix}_blobs WHERE tenant_id = $1 AND digest = $2"
                    ),
                    &[&tenant.as_str(), &digest],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn delete_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.bounded(async move {
            let prefix = &self.prefix;
            let mut client = self.pool.acquire().await?;
            client
                .execute(
                    &format!("DELETE FROM {prefix}_blobs WHERE tenant_id = $1 AND digest = $2"),
                    &[&tenant.as_str(), &digest],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(())
        })
    }

    fn create_projections(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        self.bounded(async move {
            schema::validate_document_names(
                projector.projections(),
                projector.document_projections(),
            )?;
            let mut client = self.pool.acquire().await?;
            if self.allow_schema_writes {
                schema::migrate_with_documents(
                    &mut client,
                    &self.prefix,
                    projector.projections(),
                    projector.document_projections(),
                )
                .await?;
            }
            for spec in projector.projections() {
                schema::validate_projection(&mut client, &self.prefix, spec).await?;
                if projector.document_projections().contains(&spec.name)
                    && !schema::projection_documents(&*client, &self.prefix, spec).await?
                {
                    return Err(EventLogError::Invalid(
                        "document projection requires explicit migration".into(),
                    ));
                }
            }
            client.settled();
            Ok(())
        })
    }

    fn register_inline(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        self.bounded(async move {
            let _registration = self.registration.lock().await;
            {
                let projectors = self.inline.lock().map_err(poisoned)?;
                if projectors
                    .iter()
                    .any(|existing| existing.name() == projector.name())
                {
                    return Err(EventLogError::Invalid("duplicate inline projector".into()));
                }
            }
            if self.frozen.load(Ordering::Acquire) {
                return Err(EventLogError::Invalid(
                    "inline registration is frozen after serving begins".into(),
                ));
            }
            {
                let names = self.inline_names.lock().map_err(poisoned)?;
                if projector
                    .projections()
                    .iter()
                    .any(|spec| names.contains(spec.name))
                {
                    return Err(EventLogError::Invalid(
                        "duplicate inline projection registration".into(),
                    ));
                }
            }
            let mut declared = BTreeSet::new();
            for spec in projector.projections() {
                spec.validate()?;
                if !declared.insert(spec.name) {
                    return Err(EventLogError::Invalid(
                        "duplicate projection in one registration".into(),
                    ));
                }
            }
            self.create_projections(Arc::clone(&projector)).await?;
            let mut names = self.inline_names.lock().map_err(poisoned)?;
            for spec in projector.projections() {
                if !names.insert(spec.name.to_owned()) {
                    return Err(EventLogError::Invalid(format!(
                        "projection {} is already registered",
                        spec.name
                    )));
                }
            }
            drop(names);
            self.inline.lock().map_err(poisoned)?.push(projector);
            Ok(())
        })
    }

    fn is_inline<'a>(&'a self, name: &'a str) -> BoxFuture<'a, bool> {
        Box::pin(std::future::ready(self.inline.lock().is_ok_and(
            |projectors| projectors.iter().any(|projector| projector.name() == name),
        )))
    }

    fn run_catch_up<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
        batch: usize,
    ) -> BoxFuture<'a, Result<CatchUpProgress, EventLogError>> {
        self.bounded(async move {
            let batch = bounded_limit(batch);
            let prefix = self.prefix.clone();
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction,&self.prefix,true).await?;

            // One runner per projection, whatever the replica count says.
            let locked: bool = transaction
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))",
                    &[&serde_json::to_string(&[self.prefix.as_str(),"projector",projector.name(),tenant.as_str()]).map_err(|_| EventLogError::Invalid("invalid lock coordinates".into()))?],
                )
                .await
                .map_err(backend)?
                .get(0);
            if !locked {
                transaction.rollback().await.map_err(backend)?;
                client.settled();
                return Ok(CatchUpProgress {
                    applied: 0,
                    position: 0,
                    more_waiting: true,
                });
            }

            let position: i64 = transaction
                .query_opt(
                    &format!(
                        "SELECT global_seq FROM {prefix}_projection_cursors
                         WHERE projection = $1 AND tenant_id = $2"
                    ),
                    &[&projector.name(), &tenant.as_str()],
                )
                .await
                .map_err(backend)?
                .map_or(0, |row| row.get(0));

            let rows = transaction
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM {}
                         WHERE tenant_id = $1 AND global_seq > $2 AND {WATERMARK}
                           AND (first_unsettled IS NULL OR global_seq < first_unsettled)
                         ORDER BY global_seq LIMIT $3",
                        watermarked_source(&prefix)
                    ),
                    &[&tenant.as_str(), &position, &to_i64(batch as u64 + 1)?],
                )
                .await
                .map_err(backend)?;
            let mut events = rows
                .iter()
                .map(read_event)
                .collect::<Result<Vec<_>, EventLogError>>()?;
            let more_waiting = events.len() > batch;
            events.truncate(batch);
            if events.is_empty() {
                transaction.rollback().await.map_err(backend)?;
                client.settled();
                return Ok(CatchUpProgress {
                    applied: 0,
                    position: to_u64(position)?,
                    more_waiting: false,
                });
            }
            let next_position = events.last().map_or(position, |event| {
                i64::try_from(event.global_seq).unwrap_or(position)
            });
            {
                let mut projections = PostgresProjections {
                    client: &transaction,
                    prefix: &prefix,
                    inline: &self.inline_names,
                    tenant,
                    admission: None,
                    reservation_pending: false,
                };
                for recorded in &events {
                    projector.apply(recorded, &mut projections).await?;
                }
            }
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_projection_cursors
                             (projection, tenant_id, global_seq, updated_at)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (projection, tenant_id) DO UPDATE SET
                             global_seq = EXCLUDED.global_seq,
                             updated_at = EXCLUDED.updated_at"
                    ),
                    &[
                        &projector.name(),
                        &tenant.as_str(),
                        &next_position,
                        &OffsetDateTime::now_utc(),
                    ],
                )
                .await
                .map_err(backend)?;
            let applied = events.len() as u64;
            transaction.commit().await.map_err(|_| EventLogError::UnknownCommit)?;
            client.settled();
            Ok(CatchUpProgress {
                applied,
                position: to_u64(next_position)?,
                more_waiting,
            })
        })
    }

    fn rebuild_projection<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<u64, EventLogError>> {
        self.bounded(async move {
            if self.is_inline(projector.name()).await { return Err(EventLogError::Invalid("rebuild requires a catch-up projection".into())); }
            let mut client=self.pool.acquire().await?; client.quarantine();
            let transaction=client.transaction().await.map_err(backend)?;
            publication_gate(&transaction,&self.prefix,true).await?;
            // Exactly the same owner/tenant/projector lock as ordinary catch-up workers.
            let identity=serde_json::to_string(&[self.prefix.as_str(),"projector",projector.name(),tenant.as_str()]).map_err(|_|EventLogError::Invalid("invalid lock coordinates".into()))?;
            transaction.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&identity]).await.map_err(backend)?;
            for spec in projector.projections() {
                spec.validate()?;
                let active=projection_table(&self.prefix,spec.name); let shadow=projection_table("eventlog_rebuild",spec.name);
                transaction.batch_execute(&format!("CREATE TEMP TABLE {shadow} (LIKE {active} INCLUDING ALL) ON COMMIT DROP")).await.map_err(backend)?;
            }
            // Rebuild the same contiguous eligible prefix used by feed and catch-up. An unrelated
            // transaction can hold xmin between two already-committed append XIDs.
            let target:i64=transaction.query_one(&format!("SELECT COALESCE(MAX(global_seq),0) FROM {} WHERE tenant_id=$1 AND {WATERMARK} AND (first_unsettled IS NULL OR global_seq < first_unsettled)",watermarked_source(&self.prefix)), &[&tenant.as_str()]).await.map_err(backend)?.get(0);
            let mut position=0_i64; let mut applied=0_u64;
            loop {
                let rows=transaction.query(&format!("SELECT {COLUMNS} FROM {}_events WHERE tenant_id=$1 AND global_seq>$2 AND global_seq<=$3 ORDER BY global_seq LIMIT $4",self.prefix), &[&tenant.as_str(),&position,&target,&to_i64(MAX_READ_LIMIT as u64)?]).await.map_err(backend)?;
                if rows.is_empty() {break;}
                let mut projections=PostgresProjections {client:&transaction,prefix:"eventlog_rebuild",inline:&self.inline_names,tenant,admission:None,reservation_pending:false};
                for row in &rows {let event=read_event(row)?; projector.apply(&event,&mut projections).await?; position=to_i64(event.global_seq)?; applied+=1;}
            }
            // MVCC keeps the original visible until this replacement and cursor commit together.
            for spec in projector.projections() {
                spec.validate()?;
                let active=projection_table(&self.prefix,spec.name); let shadow=projection_table("eventlog_rebuild",spec.name);
                transaction.execute(&format!("DELETE FROM {active} WHERE tenant_id=$1"), &[&tenant.as_str()]).await.map_err(backend)?;
                transaction.execute(&format!("INSERT INTO {active} SELECT * FROM {shadow} WHERE tenant_id=$1"), &[&tenant.as_str()]).await.map_err(backend)?;
            }
            transaction.execute(&format!("INSERT INTO {}_projection_cursors(projection,tenant_id,global_seq,updated_at) VALUES($1,$2,$3,$4) ON CONFLICT(projection,tenant_id) DO UPDATE SET global_seq=EXCLUDED.global_seq,updated_at=EXCLUDED.updated_at",self.prefix), &[&projector.name(),&tenant.as_str(),&position,&OffsetDateTime::now_utc()]).await.map_err(backend)?;
            transaction.commit().await.map_err(|_|EventLogError::UnknownCommit)?; client.settled(); Ok(applied)
        })
    }

    fn projection_get<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        self.bounded(async move {
            projection.validate()?;
            let table = projection_table(&self.prefix, projection.name);
            let mut client = self.pool.acquire().await?;
            let row = client
                .query_opt(
                    &format!("SELECT body FROM {table} WHERE tenant_id = $1 AND row_key = $2"),
                    &[&tenant.as_str(), &key],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn projection_query<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        query: &'a ProjectionQuery,
    ) -> BoxFuture<'a, Result<eventlog_core::ProjectionPage, EventLogError>> {
        self.bounded(async move {
            let mut client = self.pool.acquire().await?;
            let page = documents::query(&*client, &self.prefix, projection, tenant, query).await?;
            client.settled();
            Ok(page)
        })
    }

    fn projection_find<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        self.bounded(async move {
            let position = projection.field_position(field).ok_or_else(|| {
                EventLogError::Invalid(format!(
                    "{field} is not a declared indexed field of {}",
                    projection.name
                ))
            })?;
            projection.validate()?;
            let table = projection_table(&self.prefix, projection.name);
            let limit = bounded_limit(limit);
            let mut client = self.pool.acquire().await?;
            let rows = client
                .query(
                    &format!(
                        "SELECT body FROM {table}
                         WHERE tenant_id = $1 AND idx_{position} = $2
                         ORDER BY row_key LIMIT $3"
                    ),
                    &[&tenant.as_str(), &value, &to_i64(limit as u64)?],
                )
                .await
                .map_err(backend)?;
            client.settled();
            Ok(rows.iter().map(|row| row.get(0)).collect())
        })
    }
}

/// A projection's view of the transaction it is running in.
struct PostgresProjections<'a, 'b> {
    client: &'a Transaction<'b>,
    prefix: &'a str,
    inline: &'a Mutex<BTreeSet<String>>,
    tenant: &'a TenantId,
    admission: Option<(&'a eventlog_core::AdmissionPermit, &'a TenantId)>,
    reservation_pending: bool,
}

impl ProjectionStore for PostgresProjections<'_, '_> {
    fn query_documents<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        query: &'a ProjectionQuery,
    ) -> BoxFuture<'a, Result<eventlog_core::ProjectionPage, EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            if !self
                .inline
                .lock()
                .map_err(poisoned)?
                .contains(projection.name)
            {
                return Err(EventLogError::Invalid(
                    "transaction document queries require an inline projection".into(),
                ));
            }
            documents::query(self.client, self.prefix, projection, tenant, query).await
        })
    }

    fn get_blob<'a>(
        &'a mut self,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(async move {
            validate_field("digest", digest)?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT bytes FROM {}_blobs WHERE tenant_id=$1 AND digest=$2",
                        self.prefix
                    ),
                    &[&self.tenant.as_str(), &digest],
                )
                .await
                .map_err(backend)?;
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn reserve<'a>(
        &'a mut self,
        permit: &'a eventlog_core::AdmissionPermit,
        reservations: &'a [eventlog_core::Reservation],
    ) -> BoxFuture<'a, Result<Vec<i64>, EventLogError>> {
        Box::pin(async move {
            let Some((authority, tenant)) = self.admission else {
                return Err(EventLogError::Invalid(
                    "admission is confined to a trusted append guard".into(),
                ));
            };
            if !authority.same_authority(permit) {
                return Err(EventLogError::Invalid("foreign admission permit".into()));
            }
            let ordered = eventlog_core::ordered_reservations(reservations, tenant)?;
            if self.reservation_pending {
                return Err(EventLogError::Invalid(
                    "unfinished reservation poisons this append".into(),
                ));
            }
            self.reservation_pending = true;
            self.client
                .batch_execute("SAVEPOINT eventlog_reservation")
                .await
                .map_err(backend)?;
            let result=async {
                for (coordinate,_) in &ordered {lock_identity(self.client,self.prefix,"admission",&[coordinate]).await?;}
                let table=format!("{}_scope_counters",self.prefix);
                let mut next=Vec::with_capacity(ordered.len());
                // Validate every scope before any writes, including absent zero counters.
                for (coordinate,reservation) in &ordered {
                    let held=self.client.query_opt(&format!("SELECT held FROM {table} WHERE coordinate=$1 FOR UPDATE"), &[coordinate]).await.map_err(backend)?.map_or(0,|row|row.get::<_,i64>(0));
                    let updated=held.checked_add(reservation.delta).filter(|value|*value>=0 && *value<=reservation.ceiling).ok_or_else(||EventLogError::Invalid("admission ceiling or release bound refused".into()))?;
                    next.push(updated);
                }
                for ((coordinate,_),held) in ordered.iter().zip(&next) {self.client.execute(&format!("INSERT INTO {table}(coordinate,held) VALUES($1,$2) ON CONFLICT(coordinate) DO UPDATE SET held=EXCLUDED.held"), &[coordinate,held]).await.map_err(backend)?;}
                Ok(next)
            }.await;
            if result.is_err() {
                self.client
                    .batch_execute("ROLLBACK TO SAVEPOINT eventlog_reservation")
                    .await
                    .map_err(backend)?;
            }
            self.client
                .batch_execute("RELEASE SAVEPOINT eventlog_reservation")
                .await
                .map_err(backend)?;
            self.reservation_pending = false;
            result
        })
    }

    fn upsert<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
        body: &'a Value,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            projection.validate()?;
            let table = projection_table(self.prefix, projection.name);
            let columns: String = joined(projection.indexed.len(), |position| {
                format!(", idx_{position}")
            });
            let placeholders: String = joined(projection.indexed.len(), |position| {
                format!(", ${}", position + 4)
            });
            let updates: String = joined(projection.indexed.len(), |position| {
                format!(", idx_{position} = EXCLUDED.idx_{position}")
            });
            let indexed: Vec<Option<String>> = projection
                .indexed
                .iter()
                .map(|field| indexed_value(body, field))
                .collect();
            let statement = format!(
                "INSERT INTO {table} (tenant_id, row_key, body{columns})
                 VALUES ($1, $2, $3{placeholders})
                 ON CONFLICT (tenant_id, row_key) DO UPDATE SET body = EXCLUDED.body{updates}"
            );
            let tenant_value = tenant.as_str().to_owned();
            let key_value = key.to_owned();
            let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                vec![&tenant_value, &key_value, body];
            for value in &indexed {
                values.push(value);
            }
            self.client
                .execute(&statement, values.as_slice())
                .await
                .map(|_| ())
                .map_err(backend)
        })
    }

    fn delete<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            projection.validate()?;
            let table = projection_table(self.prefix, projection.name);
            self.client
                .execute(
                    &format!("DELETE FROM {table} WHERE tenant_id = $1 AND row_key = $2"),
                    &[&tenant.as_str(), &key],
                )
                .await
                .map(|_| ())
                .map_err(backend)
        })
    }

    fn get<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            projection.validate()?;
            let table = projection_table(self.prefix, projection.name);
            let row = self
                .client
                .query_opt(
                    &format!("SELECT body FROM {table} WHERE tenant_id = $1 AND row_key = $2"),
                    &[&tenant.as_str(), &key],
                )
                .await
                .map_err(backend)?;
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn get_for_update<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            {
                let inline = self.inline.lock().map_err(poisoned)?;
                if !inline.contains(projection.name) {
                    return Err(EventLogError::Invalid(format!(
                        "projection {} is not driven inline, so a guard over it would be \
                         enforced late",
                        projection.name
                    )));
                }
            }
            lock_identity(
                self.client,
                self.prefix,
                "projection-row",
                &[projection.name, tenant.as_str(), key],
            )
            .await?;
            projection.validate()?;
            let table = projection_table(self.prefix, projection.name);
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT body FROM {table} WHERE tenant_id = $1 AND row_key = $2 FOR UPDATE"
                    ),
                    &[&tenant.as_str(), &key],
                )
                .await
                .map_err(backend)?;
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn find<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        Box::pin(async move {
            if tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "projection context cannot cross tenant".into(),
                ));
            }
            let position = projection.field_position(field).ok_or_else(|| {
                EventLogError::Invalid(format!(
                    "{field} is not a declared indexed field of {}",
                    projection.name
                ))
            })?;
            projection.validate()?;
            let table = projection_table(self.prefix, projection.name);
            let limit = bounded_limit(limit);
            let rows = self
                .client
                .query(
                    &format!(
                        "SELECT body FROM {table}
                         WHERE tenant_id = $1 AND idx_{position} = $2
                         ORDER BY row_key LIMIT $3"
                    ),
                    &[&tenant.as_str(), &value, &to_i64(limit as u64)?],
                )
                .await
                .map_err(backend)?;
            Ok(rows.iter().map(|row| row.get(0)).collect())
        })
    }
}

/// Build one SQL fragment from a per-column fragment.
///
/// Written as a loop rather than `map(format!).collect()` so that neither the
/// `format_collect` nor the `format_push_string` lint has anything to say about the one place
/// this crate assembles column lists.
fn joined(count: usize, render: impl Fn(usize) -> String) -> String {
    let mut fragment = String::new();
    for position in 0..count {
        let piece = render(position);
        fragment.push_str(&piece);
    }
    fragment
}

fn projection_table(prefix: &str, name: &str) -> String {
    format!("{prefix}_p_{name}")
}

async fn select_versions<C: GenericClient>(
    client: &C,
    prefix: &str,
    stream: &StreamId,
    first_version: i64,
    last_version: i64,
) -> Result<Vec<RecordedEvent>, EventLogError> {
    let rows = client
        .query(
            &format!(
                "SELECT {COLUMNS} FROM {prefix}_events
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3
                   AND version >= $4 AND version <= $5
                 ORDER BY version"
            ),
            &[
                &stream.tenant().as_str(),
                &stream.stream_type(),
                &stream.stream_id(),
                &first_version,
                &last_version,
            ],
        )
        .await
        .map_err(backend)?;
    rows.iter().map(read_event).collect()
}

fn read_event(row: &Row) -> Result<RecordedEvent, EventLogError> {
    let global_seq: i64 = row.get(0);
    let tenant: String = row.get(1);
    let version: i64 = row.get(4);
    let event_id: uuid_shim::Uuid = row.get(5);
    let schema_version: i32 = row.get(7);
    let causation_depth: i32 = row.get(15);
    Ok(RecordedEvent {
        global_seq: to_u64(global_seq)?,
        tenant: TenantId::new(tenant)?,
        stream_type: row.get(2),
        stream_id: row.get(3),
        version: to_u64(version)?,
        event_id: event_id.to_string(),
        name: row.get(6),
        schema_version: to_u32(schema_version)?,
        occurred_at: row.get(8),
        recorded_at: row.get(9),
        subject: row.get(10),
        actor: row.get(11),
        request_id: row.get(12),
        trace_id: row.get(13),
        causation_id: row.get(14),
        causation_depth: to_u32(causation_depth)?,
        redacted_at: row.get(16),
        data: row.get(17),
    })
}

fn check_expected(expected: Expected, head: u64) -> Result<(), EventLogError> {
    match expected {
        Expected::Any => Ok(()),
        Expected::NoStream if head == 0 => Ok(()),
        Expected::NoStream => Err(EventLogError::Conflict {
            expected: 0,
            actual: head,
        }),
        Expected::Exact(version) if version == head => Ok(()),
        Expected::Exact(version) => Err(EventLogError::Conflict {
            expected: version,
            actual: head,
        }),
    }
}

fn validate_prefix(prefix: &str) -> Result<(), EventLogError> {
    if prefix.is_empty()
        || prefix == "eventlog_expected"
        || prefix == "eventlog_rebuild"
        || prefix.len() > 32
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
    {
        return Err(EventLogError::Invalid(
            "a table prefix is lowercase ASCII and underscores, up to 32 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn parse_uuid(value: &str) -> Result<uuid_shim::Uuid, EventLogError> {
    uuid_shim::Uuid::parse_str(value)
        .map_err(|error| EventLogError::Invalid(format!("event id is not a UUID: {error}")))
}

fn to_i64(value: u64) -> Result<i64, EventLogError> {
    i64::try_from(value).map_err(|_| EventLogError::Invalid("value is out of range".to_owned()))
}

fn to_i32(value: u32) -> Result<i32, EventLogError> {
    i32::try_from(value).map_err(|_| EventLogError::Invalid("value is out of range".to_owned()))
}

fn to_u64(value: i64) -> Result<u64, EventLogError> {
    u64::try_from(value).map_err(|_| EventLogError::Backend("stored value is negative".to_owned()))
}

fn to_u32(value: i32) -> Result<u32, EventLogError> {
    u32::try_from(value)
        .map_err(|_| EventLogError::Backend("stored value is out of range".to_owned()))
}

fn backend(error: tokio_postgres::Error) -> EventLogError {
    let result = match error.code().map(tokio_postgres::error::SqlState::code) {
        Some("53300") => EventLogError::Overloaded,
        Some("57014" | "55P03") => EventLogError::Deadline {
            operation: "database statement or lock",
        },
        Some(code) => EventLogError::Backend(format!(
            "PostgreSQL SQLSTATE {code}, position {:?}",
            error.as_db_error().and_then(|error| error.position())
        )),
        None => EventLogError::Backend("PostgreSQL connection unavailable".into()),
    };
    drop(error);
    result
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EventLogError {
    EventLogError::Backend("the store lock was poisoned by a panic".to_owned())
}

mod uuid_shim {
    pub use uuid::Uuid;
}

// JSON arrays encode string components injectively; hash collisions serialize unrelated work only.
async fn lock_identity(
    transaction: &Transaction<'_>,
    prefix: &str,
    kind: &str,
    fields: &[&str],
) -> Result<(), EventLogError> {
    let identity = serde_json::to_string(&(prefix, kind, fields))
        .map_err(|_| EventLogError::Invalid("invalid lock coordinates".into()))?;
    transaction.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&identity]).await.map_err(backend)?;
    Ok(())
}

// Evaluate both the row watermark and the first withheld position in one statement snapshot.
// The publication gate settles owner writers, but unrelated transactions can still hold xmin
// between committed XIDs. Never publish a higher position past such a withheld lower position.
fn watermarked_source(prefix: &str) -> String {
    format!(
        "{prefix}_events CROSS JOIN \
         (SELECT MIN(global_seq) AS first_unsettled FROM {prefix}_events \
          WHERE NOT ({WATERMARK})) AS publication_prefix"
    )
}

// Transaction publication gate: XID order can differ from global sequence allocation order.
// Take this before every other owner lock. Feed/fold readers query a fresh READ COMMITTED
// snapshot only after all in-flight publishers settle; writers remain mutually concurrent.
async fn publication_gate(
    transaction: &Transaction<'_>,
    prefix: &str,
    exclusive: bool,
) -> Result<(), EventLogError> {
    let identity = serde_json::to_string(&[prefix, "publication"])
        .map_err(|_| EventLogError::Invalid("publication coordinates".into()))?;
    let function = if exclusive {
        "pg_advisory_xact_lock"
    } else {
        "pg_advisory_xact_lock_shared"
    };
    transaction.query_one(&format!("SELECT {function}(hashtextextended(current_database() || ':' || current_schema() || $1,0))"),&[&identity]).await.map_err(backend)?;
    Ok(())
}
