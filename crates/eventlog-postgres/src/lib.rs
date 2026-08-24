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
    EventStore, Expected, FeedPage, Guard, MAX_READ_LIMIT, NewEvent, NoGuard, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, Snapshot, StreamId, StreamSlice, TenantId,
    bounded_limit, indexed_value, new_event_id, redaction_tombstone, validate_append,
    validate_field,
};
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::{Client, GenericClient, NoTls, Row, Transaction};

/// Every envelope column, in the order [`read_event`] expects them.
const COLUMNS: &str = "global_seq, tenant_id, stream_type, stream_id, version, event_id, \
     event_name, event_schema_version, occurred_at, recorded_at, subject, actor, request_id, \
     trace_id, causation_id, causation_depth, redacted_at, data";

/// The predicate that keeps a feed reader behind anything still in flight.
const WATERMARK: &str = "committed_xid < pg_snapshot_xmin(pg_current_snapshot())";

/// One owner's event tables in one PostgreSQL database.
pub struct PostgresEventStore {
    client: AsyncMutex<Client>,
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
        validate_prefix(prefix)?;
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(backend)?;
        // The half that talks to the socket. It runs as its own task and ends by itself when the
        // client is dropped.
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let store = Self {
            client: AsyncMutex::new(client),
            prefix: prefix.to_owned(),
            inline: Mutex::new(Vec::new()),
            inline_names: Mutex::new(BTreeSet::new()),
        };
        store.create_tables().await?;
        Ok(store)
    }

    /// Refuse a table of ours that somebody else made.
    ///
    /// `CREATE TABLE IF NOT EXISTS` **silently does nothing** when a table of that name already
    /// exists, whatever shape it has. This matters more here than on SQLite: a hosted deployment
    /// that moves a module onto the log points it at the database the module already had, and the
    /// old `<prefix>_events` is sitting there.
    async fn refuse_foreign_tables(&self, client: &Client) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let events = format!("{prefix}_events");
        let existing: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1",
                &[&events],
            )
            .await
            .map_err(backend)?
            .get(0);
        if existing == 0 {
            return Ok(());
        }
        let ours: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1
                   AND column_name = 'global_seq'",
                &[&events],
            )
            .await
            .map_err(backend)?
            .get(0);
        if ours > 0 {
            return Ok(());
        }
        Err(EventLogError::Backend(format!(
            "this database already has a table called {events} that this kit did not create, so \
             its own tables cannot be made. It is almost certainly {prefix}'s previous store. \
             Point this owner at a different schema, or rename the old tables out of the way once \
             you have decided what to do with what is in them."
        )))
    }

    async fn create_tables(&self) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let statements = format!(
            "CREATE TABLE IF NOT EXISTS {prefix}_events (
                 global_seq BIGSERIAL PRIMARY KEY,
                 committed_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version BIGINT NOT NULL,
                 event_id UUID NOT NULL,
                 event_name TEXT NOT NULL,
                 event_schema_version INTEGER NOT NULL,
                 occurred_at TIMESTAMPTZ NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 subject TEXT NOT NULL,
                 actor TEXT NOT NULL,
                 request_id TEXT NOT NULL,
                 trace_id TEXT NOT NULL,
                 causation_id TEXT,
                 causation_depth INTEGER NOT NULL DEFAULT 0,
                 redacted_at TIMESTAMPTZ,
                 data JSONB NOT NULL,
                 UNIQUE (tenant_id, stream_type, stream_id, version)
             );
             CREATE INDEX IF NOT EXISTS {prefix}_events_feed
                 ON {prefix}_events (tenant_id, global_seq);
             CREATE TABLE IF NOT EXISTS {prefix}_commands (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 request_hash TEXT NOT NULL,
                 first_version BIGINT NOT NULL,
                 last_version BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id, idempotency_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_claims (
                 tenant_id TEXT NOT NULL,
                 scope TEXT NOT NULL,
                 claim_key TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 first_version BIGINT NOT NULL,
                 last_version BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, scope, claim_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_identity (
                 tenant_id TEXT NOT NULL PRIMARY KEY,
                 stream_identity TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {prefix}_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL,
                 bytes BYTEA NOT NULL,
                 byte_count BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, digest)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_projection_cursors (
                 projection TEXT NOT NULL,
                 tenant_id TEXT NOT NULL,
                 global_seq BIGINT NOT NULL,
                 updated_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (projection, tenant_id)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_snapshots (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version BIGINT NOT NULL,
                 state_schema_version INTEGER NOT NULL,
                 state JSONB NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id)
             );"
        );
        let client = self.client.lock().await;
        self.refuse_foreign_tables(&client).await?;
        client.batch_execute(&statements).await.map_err(backend)
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
        self.client
            .lock()
            .await
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {prefix}_events;
                 DROP TABLE IF EXISTS {prefix}_commands;
                 DROP TABLE IF EXISTS {prefix}_claims;
                 DROP TABLE IF EXISTS {prefix}_snapshots;
                 DROP TABLE IF EXISTS {prefix}_projection_cursors;
                 DROP TABLE IF EXISTS {prefix}_blobs;
                 DROP TABLE IF EXISTS {prefix}_identity;"
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
            validate_append(events, meta)?;
            let prefix = self.prefix.clone();
            let mut client = self.client.lock().await;
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
                        &meta.idempotency_key,
                    ],
                )
                .await
                .map_err(backend)?;

            if let Some(row) = recorded {
                let request_hash: String = row.get(0);
                let first_version: i64 = row.get(1);
                let last_version: i64 = row.get(2);
                if request_hash != meta.request_hash {
                    return Err(EventLogError::IdempotencyMismatch {
                        key: meta.idempotency_key.clone(),
                    });
                }
                let stored =
                    select_versions(&transaction, &prefix, stream, first_version, last_version)
                        .await?;
                return Ok(AppendResult {
                    first_version: to_u64(first_version)?,
                    last_version: to_u64(last_version)?,
                    events: stored,
                    deduplicated: true,
                });
            }

            let head: Option<i64> = transaction
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
            let head = match head {
                Some(value) => to_u64(value)?,
                None => 0,
            };
            check_expected(expected, head)?;

            {
                let mut projections = PostgresProjections {
                    client: &transaction,
                    prefix: &prefix,
                    inline: &self.inline_names,
                };
                admission.check(&mut projections).await?;
            }

            let now = OffsetDateTime::now_utc();
            let mut written = Vec::with_capacity(events.len());
            for (offset, event) in events.iter().enumerate() {
                let version = head + 1 + u64::try_from(offset).unwrap_or(u64::MAX);
                let event_id = new_event_id();
                let uuid = parse_uuid(&event_id)?;
                let row = transaction
                    .query_one(
                        &format!(
                            "INSERT INTO {prefix}_events (
                                 tenant_id, stream_type, stream_id, version, event_id, event_name,
                                 event_schema_version, occurred_at, recorded_at, subject, actor,
                                 request_id, trace_id, causation_id, causation_depth, data)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                                     $15, $16)
                             RETURNING global_seq"
                        ),
                        &[
                            &stream.tenant().as_str(),
                            &stream.stream_type(),
                            &stream.stream_id(),
                            &to_i64(version)?,
                            &uuid,
                            &event.name,
                            &to_i32(event.schema_version)?,
                            &meta.occurred_at,
                            &now,
                            &meta.subject,
                            &meta.actor,
                            &meta.request_id,
                            &meta.trace_id,
                            &meta.causation_id,
                            &to_i32(meta.causation_depth)?,
                            &event.data,
                        ],
                    )
                    .await
                    .map_err(backend)?;
                let global_seq: i64 = row.get(0);
                written.push(RecordedEvent {
                    global_seq: to_u64(global_seq)?,
                    tenant: stream.tenant().clone(),
                    stream_type: stream.stream_type().to_owned(),
                    stream_id: stream.stream_id().to_owned(),
                    version,
                    event_id,
                    name: event.name.clone(),
                    schema_version: event.schema_version,
                    occurred_at: meta.occurred_at,
                    recorded_at: now,
                    subject: meta.subject.clone(),
                    actor: meta.actor.clone(),
                    request_id: meta.request_id.clone(),
                    trace_id: meta.trace_id.clone(),
                    causation_id: meta.causation_id.clone(),
                    causation_depth: meta.causation_depth,
                    redacted_at: None,
                    data: event.data.clone(),
                });
            }

            let first_version = head + 1;
            let last_version = head + u64::try_from(events.len()).unwrap_or(u64::MAX);
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_commands (
                             tenant_id, stream_type, stream_id, idempotency_key, request_hash,
                             first_version, last_version, recorded_at)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                        &meta.idempotency_key,
                        &meta.request_hash,
                        &to_i64(first_version)?,
                        &to_i64(last_version)?,
                        &now,
                    ],
                )
                .await
                .map_err(backend)?;

            let projectors: Vec<Arc<dyn Projector>> = self.inline.lock().map_err(poisoned)?.clone();
            for projector in &projectors {
                let mut projections = PostgresProjections {
                    client: &transaction,
                    prefix: &prefix,
                    inline: &self.inline_names,
                };
                for recorded in &written {
                    projector.apply(recorded, &mut projections).await?;
                }
            }

            if let Some(claim) = &meta.claim {
                transaction
                    .execute(
                        &format!(
                            "INSERT INTO {prefix}_claims (
                                 tenant_id, scope, claim_key, request_digest, stream_type,
                                 stream_id, first_version, last_version, recorded_at)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
                        ),
                        &[
                            &stream.tenant().as_str(),
                            &claim.scope,
                            &claim.key,
                            &claim.digest,
                            &stream.stream_type(),
                            &stream.stream_id(),
                            &to_i64(first_version)?,
                            &to_i64(last_version)?,
                            &now,
                        ],
                    )
                    .await
                    .map_err(backend)?;
            }

            transaction.commit().await.map_err(backend)?;

            Ok(AppendResult {
                first_version,
                last_version,
                events: written,
                deduplicated: false,
            })
        })
    }

    fn recorded_claim<'a>(
        &'a self,
        tenant: &'a TenantId,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Option<ClaimedCommand>, EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
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
        Box::pin(async move {
            let prefix = self.prefix.clone();
            let mut client = self.client.lock().await;
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
        Box::pin(async move {
            let limit = bounded_limit(limit);
            let prefix = &self.prefix;
            let client = self.client.lock().await;
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
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
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
            head.map(to_u64).transpose()
        })
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after_position: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        Box::pin(async move {
            let limit = bounded_limit(limit);
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            let rows = client
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM {prefix}_events
                         WHERE tenant_id = $1 AND global_seq > $2 AND {WATERMARK}
                         ORDER BY global_seq LIMIT $3"
                    ),
                    &[
                        &tenant.as_str(),
                        &to_i64(after_position)?,
                        &to_i64(limit as u64 + 1)?,
                    ],
                )
                .await
                .map_err(backend)?;
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
        Box::pin(async move {
            validate_field("redaction reason", reason)?;
            let prefix = self.prefix.clone();
            let mut client = self.client.lock().await;
            let transaction = client.transaction().await.map_err(backend)?;
            let now = OffsetDateTime::now_utc();
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
            transaction.commit().await.map_err(backend)?;
            events.pop().ok_or(EventLogError::NotFound)
        })
    }

    fn save_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            client
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
                .map(|_| ())
                .map_err(backend)
        })
    }

    fn load_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            let row = client
                .query_opt(
                    &format!(
                        "SELECT version, state_schema_version, state, recorded_at
                         FROM {prefix}_snapshots
                         WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3"
                    ),
                    &[
                        &stream.tenant().as_str(),
                        &stream.stream_type(),
                        &stream.stream_id(),
                    ],
                )
                .await
                .map_err(backend)?;
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
        Box::pin(async move {
            let prefix = self.prefix.clone();
            let mut client = self.client.lock().await;
            let transaction = client.transaction().await.map_err(backend)?;
            for table in [
                "events",
                "commands",
                "claims",
                "snapshots",
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
            let pattern = format!("{prefix}_p_%");
            let tables: Vec<String> = transaction
                .query(
                    "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()
                     AND tablename LIKE $1",
                    &[&pattern],
                )
                .await
                .map_err(backend)?
                .iter()
                .map(|row| row.get(0))
                .collect();
            for table in tables {
                transaction
                    .execute(
                        &format!("DELETE FROM {table} WHERE tenant_id = $1"),
                        &[&tenant.as_str()],
                    )
                    .await
                    .map_err(backend)?;
            }
            transaction.commit().await.map_err(backend)
        })
    }

    fn projection_list<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after_key: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        Box::pin(async move {
            let limit = bounded_limit(limit);
            let table = projection_table(&self.prefix, projection.name);
            let client = self.client.lock().await;
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
            Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
        })
    }

    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let identity = new_event_id();
            let client = self.client.lock().await;
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
            Ok(row.get(0))
        })
    }

    fn put_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            validate_field("digest", digest)?;
            let prefix = &self.prefix;
            let client = self.client.lock().await;
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
                .map(|_| ())
                .map_err(backend)
        })
    }

    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            let row = client
                .query_opt(
                    &format!(
                        "SELECT bytes FROM {prefix}_blobs WHERE tenant_id = $1 AND digest = $2"
                    ),
                    &[&tenant.as_str(), &digest],
                )
                .await
                .map_err(backend)?;
            Ok(row.map(|row| row.get(0)))
        })
    }

    fn delete_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            client
                .execute(
                    &format!("DELETE FROM {prefix}_blobs WHERE tenant_id = $1 AND digest = $2"),
                    &[&tenant.as_str(), &digest],
                )
                .await
                .map(|_| ())
                .map_err(backend)
        })
    }

    fn create_projections(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            let prefix = &self.prefix;
            let client = self.client.lock().await;
            for spec in projector.projections() {
                spec.validate()?;
                let table = projection_table(prefix, spec.name);
                let columns: String = joined(spec.indexed.len(), |position| {
                    format!(", idx_{position} TEXT")
                });
                let indexes: String = joined(spec.indexed.len(), |position| {
                    format!(
                        "CREATE INDEX IF NOT EXISTS {table}_idx_{position}
                             ON {table} (tenant_id, idx_{position});"
                    )
                });
                client
                    .batch_execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {table} (
                             tenant_id TEXT NOT NULL,
                             row_key TEXT NOT NULL,
                             body JSONB NOT NULL{columns},
                             PRIMARY KEY (tenant_id, row_key)
                         );
                         {indexes}"
                    ))
                    .await
                    .map_err(backend)?;
            }
            Ok(())
        })
    }

    fn register_inline(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
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
        // An in-memory set, not the database: nothing here can block.
        Box::pin(std::future::ready(
            self.inline_names
                .lock()
                .is_ok_and(|names| names.contains(name)),
        ))
    }

    fn run_catch_up<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
        batch: usize,
    ) -> BoxFuture<'a, Result<CatchUpProgress, EventLogError>> {
        Box::pin(async move {
            let batch = bounded_limit(batch);
            let prefix = self.prefix.clone();
            let mut client = self.client.lock().await;
            let transaction = client.transaction().await.map_err(backend)?;

            // One runner per projection, whatever the replica count says.
            let locked: bool = transaction
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)",
                    &[&projector.name()],
                )
                .await
                .map_err(backend)?
                .get(0);
            if !locked {
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
                        "SELECT {COLUMNS} FROM {prefix}_events
                         WHERE tenant_id = $1 AND global_seq > $2 AND {WATERMARK}
                         ORDER BY global_seq LIMIT $3"
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
            transaction.commit().await.map_err(backend)?;
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
        Box::pin(async move {
            let prefix = self.prefix.clone();
            {
                let client = self.client.lock().await;
                for spec in projector.projections() {
                    let table = projection_table(&prefix, spec.name);
                    client
                        .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                        .await
                        .map_err(backend)?;
                }
                client
                    .execute(
                        &format!(
                            "DELETE FROM {prefix}_projection_cursors
                             WHERE projection = $1 AND tenant_id = $2"
                        ),
                        &[&projector.name(), &tenant.as_str()],
                    )
                    .await
                    .map_err(backend)?;
            }
            self.create_projections(Arc::clone(&projector)).await?;
            let mut applied = 0;
            loop {
                let progress = self
                    .run_catch_up(Arc::clone(&projector), tenant, MAX_READ_LIMIT)
                    .await?;
                applied += progress.applied;
                if progress.applied == 0 || !progress.more_waiting {
                    return Ok(applied);
                }
            }
        })
    }

    fn projection_get<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            let table = projection_table(&self.prefix, projection.name);
            let client = self.client.lock().await;
            let row = client
                .query_opt(
                    &format!("SELECT body FROM {table} WHERE tenant_id = $1 AND row_key = $2"),
                    &[&tenant.as_str(), &key],
                )
                .await
                .map_err(backend)?;
            Ok(row.map(|row| row.get(0)))
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
        Box::pin(async move {
            let position = projection.field_position(field).ok_or_else(|| {
                EventLogError::Invalid(format!(
                    "{field} is not a declared indexed field of {}",
                    projection.name
                ))
            })?;
            let table = projection_table(&self.prefix, projection.name);
            let limit = bounded_limit(limit);
            let client = self.client.lock().await;
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
            Ok(rows.iter().map(|row| row.get(0)).collect())
        })
    }
}

/// A projection's view of the transaction it is running in.
struct PostgresProjections<'a, 'b> {
    client: &'a Transaction<'b>,
    prefix: &'a str,
    inline: &'a Mutex<BTreeSet<String>>,
}

impl ProjectionStore for PostgresProjections<'_, '_> {
    fn upsert<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
        body: &'a Value,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
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
            let position = projection.field_position(field).ok_or_else(|| {
                EventLogError::Invalid(format!(
                    "{field} is not a declared indexed field of {}",
                    projection.name
                ))
            })?;
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

fn backend(error: impl std::fmt::Display) -> EventLogError {
    EventLogError::Backend(error.to_string())
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EventLogError {
    EventLogError::Backend("the store lock was poisoned by a panic".to_owned())
}

mod uuid_shim {
    pub use uuid::Uuid;
}
