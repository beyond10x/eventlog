#![forbid(unsafe_code)]

//! The log on SQLite, which is also the log in memory.
//!
//! A file path gives an owner its local store; `:memory:` gives its tests one. They are the same
//! code, so a property proved in a test is proved for the deployment — which is exactly what a
//! separate hand-written memory backend cannot say.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use eventlog_core::{
    AppendResult, CatchUpProgress, Claim, ClaimedCommand, CommandMeta, EventLogError, EventStore,
    Expected, FeedPage, Guard, MAX_READ_LIMIT, NewEvent, NoGuard, ProjectionSpec, ProjectionStore,
    Projector, RecordedEvent, Snapshot, StreamId, StreamSlice, TenantId, bounded_limit,
    indexed_value, new_event_id, redaction_tombstone, validate_append, validate_field,
};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};
use serde_json::Value;

/// Every envelope column, in the order [`read_event`] expects them.
const COLUMNS: &str = "global_seq, tenant_id, stream_type, stream_id, version, event_id, \
     event_name, event_schema_version, occurred_at, recorded_at, subject, actor, request_id, \
     trace_id, causation_id, causation_depth, redacted_at, data";
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// One owner's event tables in one SQLite database.
pub struct SqliteEventStore {
    connection: Mutex<Connection>,
    prefix: String,
    inline: Mutex<Vec<Arc<dyn Projector>>>,
    inline_names: Mutex<BTreeSet<String>>,
}

impl SqliteEventStore {
    /// Open or create the store for one owner, identified by its table prefix.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable prefix and [`EventLogError::Backend`]
    /// when the database cannot be opened or its tables cannot be created.
    pub fn open(path: &str, prefix: &str) -> Result<Self, EventLogError> {
        let connection = Connection::open(path).map_err(backend)?;
        Self::from_connection(connection, prefix)
    }

    /// An empty store that lives only as long as the process.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the database cannot be created.
    pub fn in_memory(prefix: &str) -> Result<Self, EventLogError> {
        let connection = Connection::open_in_memory().map_err(backend)?;
        Self::from_connection(connection, prefix)
    }

    fn from_connection(connection: Connection, prefix: &str) -> Result<Self, EventLogError> {
        validate_prefix(prefix)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(backend)?;
        let store = Self {
            connection: Mutex::new(connection),
            prefix: prefix.to_owned(),
            inline: Mutex::new(Vec::new()),
            inline_names: Mutex::new(BTreeSet::new()),
        };
        store.create_tables()?;
        Ok(store)
    }

    fn create_tables(&self) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let statements = format!(
            "CREATE TABLE IF NOT EXISTS {prefix}_events (
                 global_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 committed_xid INTEGER NOT NULL DEFAULT 0,
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 event_id TEXT NOT NULL,
                 event_name TEXT NOT NULL,
                 event_schema_version INTEGER NOT NULL,
                 occurred_at TEXT NOT NULL,
                 recorded_at TEXT NOT NULL,
                 subject TEXT NOT NULL,
                 actor TEXT NOT NULL,
                 request_id TEXT NOT NULL,
                 trace_id TEXT NOT NULL,
                 causation_id TEXT,
                 causation_depth INTEGER NOT NULL DEFAULT 0,
                 redacted_at TEXT,
                 data TEXT NOT NULL,
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
                 first_version INTEGER NOT NULL,
                 last_version INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id, idempotency_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_claims (
                 tenant_id TEXT NOT NULL,
                 scope TEXT NOT NULL,
                 claim_key TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 first_version INTEGER NOT NULL,
                 last_version INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, scope, claim_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_identity (
                 tenant_id TEXT NOT NULL PRIMARY KEY,
                 stream_identity TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {prefix}_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, digest)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_projection_cursors (
                 projection TEXT NOT NULL,
                 tenant_id TEXT NOT NULL,
                 global_seq INTEGER NOT NULL,
                 updated_at TEXT NOT NULL,
                 PRIMARY KEY (projection, tenant_id)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_snapshots (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 state_schema_version INTEGER NOT NULL,
                 state TEXT NOT NULL,
                 recorded_at TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id)
             );"
        );
        self.connection
            .lock()
            .map_err(poisoned)?
            .execute_batch(&statements)
            .map_err(backend)
    }
}

impl EventStore for SqliteEventStore {
    fn append(
        &self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
    ) -> Result<AppendResult, EventLogError> {
        self.append_guarded(stream, expected, events, meta, &NoGuard)
    }

    fn append_guarded(
        &self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        admission: &dyn Guard,
    ) -> Result<AppendResult, EventLogError> {
        validate_append(events, meta)?;
        let prefix = self.prefix.clone();
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;

        let recorded: Option<(String, i64, i64)> = transaction
            .query_row(
                &format!(
                    "SELECT request_hash, first_version, last_version FROM {prefix}_commands
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3
                       AND idempotency_key = ?4"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    meta.idempotency_key
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(backend)?;

        if let Some((request_hash, first_version, last_version)) = recorded {
            if request_hash != meta.request_hash {
                return Err(EventLogError::IdempotencyMismatch {
                    key: meta.idempotency_key.clone(),
                });
            }
            let stored =
                select_versions(&transaction, &prefix, stream, first_version, last_version)?;
            return Ok(AppendResult {
                first_version: to_u64(first_version)?,
                last_version: to_u64(last_version)?,
                events: stored,
                deduplicated: true,
            });
        }

        let head: Option<i64> = transaction
            .query_row(
                &format!(
                    "SELECT MAX(version) FROM {prefix}_events
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?
            .flatten();
        let head = match head {
            Some(value) => to_u64(value)?,
            None => 0,
        };
        check_expected(expected, head)?;

        {
            let mut projections = SqliteProjections {
                connection: &transaction,
                prefix: &prefix,
                inline: &self.inline_names,
            };
            admission.check(&mut projections)?;
        }

        let now = OffsetDateTime::now_utc();
        let recorded_at = format_time(now)?;
        let occurred_at = format_time(meta.occurred_at)?;
        let mut written = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let version = head + 1 + offset as u64;
            let event_id = new_event_id();
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_events (
                             tenant_id, stream_type, stream_id, version, event_id, event_name,
                             event_schema_version, occurred_at, recorded_at, subject, actor,
                             request_id, trace_id, causation_id, causation_depth, data)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                                 ?15, ?16)"
                    ),
                    params![
                        stream.tenant().as_str(),
                        stream.stream_type(),
                        stream.stream_id(),
                        to_i64(version)?,
                        event_id,
                        event.name,
                        i64::from(event.schema_version),
                        occurred_at,
                        recorded_at,
                        meta.subject,
                        meta.actor,
                        meta.request_id,
                        meta.trace_id,
                        meta.causation_id,
                        i64::from(meta.causation_depth),
                        event.data.to_string(),
                    ],
                )
                .map_err(backend)?;
            let global_seq = to_u64(transaction.last_insert_rowid())?;
            written.push(RecordedEvent {
                global_seq,
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
        let last_version = head + events.len() as u64;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {prefix}_commands (
                         tenant_id, stream_type, stream_id, idempotency_key, request_hash,
                         first_version, last_version, recorded_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    meta.idempotency_key,
                    meta.request_hash,
                    to_i64(first_version)?,
                    to_i64(last_version)?,
                    recorded_at,
                ],
            )
            .map_err(backend)?;

        {
            let inline = self.inline.lock().map_err(poisoned)?;
            for projector in inline.iter() {
                let mut projections = SqliteProjections {
                    connection: &transaction,
                    prefix: &prefix,
                    inline: &self.inline_names,
                };
                for recorded in &written {
                    projector.apply(recorded, &mut projections)?;
                }
            }
        }

        if let Some(claim) = &meta.claim {
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {prefix}_claims (
                             tenant_id, scope, claim_key, request_digest, stream_type, stream_id,
                             first_version, last_version, recorded_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
                    ),
                    params![
                        stream.tenant().as_str(),
                        claim.scope,
                        claim.key,
                        claim.digest,
                        stream.stream_type(),
                        stream.stream_id(),
                        to_i64(first_version)?,
                        to_i64(last_version)?,
                        recorded_at,
                    ],
                )
                .map_err(backend)?;
        }

        transaction.commit().map_err(backend)?;

        Ok(AppendResult {
            first_version,
            last_version,
            events: written,
            deduplicated: false,
        })
    }

    fn recorded_claim(
        &self,
        tenant: &TenantId,
        claim: &Claim,
    ) -> Result<Option<ClaimedCommand>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let row: Option<(String, String, String, i64, i64)> = guard
            .query_row(
                &format!(
                    "SELECT request_digest, stream_type, stream_id, first_version, last_version
                     FROM {prefix}_claims
                     WHERE tenant_id = ?1 AND scope = ?2 AND claim_key = ?3"
                ),
                params![tenant.as_str(), claim.scope, claim.key],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(backend)?;
        let Some((digest, stream_type, stream_id, first_version, last_version)) = row else {
            return Ok(None);
        };
        if digest != claim.digest {
            return Err(EventLogError::IdempotencyMismatch {
                key: claim.key.clone(),
            });
        }
        Ok(Some(ClaimedCommand {
            stream: StreamId::new(tenant.clone(), stream_type, stream_id)?,
            first_version: to_u64(first_version)?,
            last_version: to_u64(last_version)?,
        }))
    }

    fn recorded_command(
        &self,
        stream: &StreamId,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<AppendResult>, EventLogError> {
        let prefix = self.prefix.clone();
        let guard = self.connection.lock().map_err(poisoned)?;
        let recorded: Option<(String, i64, i64)> = guard
            .query_row(
                &format!(
                    "SELECT request_hash, first_version, last_version FROM {prefix}_commands
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3
                       AND idempotency_key = ?4"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    idempotency_key
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(backend)?;
        let Some((stored_hash, first_version, last_version)) = recorded else {
            return Ok(None);
        };
        if stored_hash != request_hash {
            return Err(EventLogError::IdempotencyMismatch {
                key: idempotency_key.to_owned(),
            });
        }
        let events = select_versions(&guard, &prefix, stream, first_version, last_version)?;
        Ok(Some(AppendResult {
            first_version: to_u64(first_version)?,
            last_version: to_u64(last_version)?,
            events,
            deduplicated: true,
        }))
    }

    fn read_stream(
        &self,
        stream: &StreamId,
        after_version: u64,
        limit: usize,
    ) -> Result<StreamSlice, EventLogError> {
        let limit = bounded_limit(limit);
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let mut statement = guard
            .prepare(&format!(
                "SELECT {COLUMNS} FROM {prefix}_events
                 WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3 AND version > ?4
                 ORDER BY version LIMIT ?5"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    to_i64(after_version)?,
                    to_i64(limit as u64 + 1)?
                ],
                read_event,
            )
            .map_err(backend)?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(backend)??);
        }
        let end_of_stream = events.len() <= limit;
        events.truncate(limit);
        let next_version = events.last().map_or(after_version, |event| event.version);
        Ok(StreamSlice {
            events,
            next_version,
            end_of_stream,
        })
    }

    fn stream_version(&self, stream: &StreamId) -> Result<Option<u64>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let head: Option<i64> = guard
            .query_row(
                &format!(
                    "SELECT MAX(version) FROM {prefix}_events
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?
            .flatten();
        head.map(to_u64).transpose()
    }

    fn read_feed(
        &self,
        tenant: &TenantId,
        after_position: u64,
        limit: usize,
    ) -> Result<FeedPage, EventLogError> {
        let limit = bounded_limit(limit);
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let mut statement = guard
            .prepare(&format!(
                "SELECT {COLUMNS} FROM {prefix}_events
                 WHERE tenant_id = ?1 AND global_seq > ?2
                 ORDER BY global_seq LIMIT ?3"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![
                    tenant.as_str(),
                    to_i64(after_position)?,
                    to_i64(limit as u64 + 1)?
                ],
                read_event,
            )
            .map_err(backend)?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(backend)??);
        }
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
    }

    fn redact(
        &self,
        stream: &StreamId,
        version: u64,
        reason: &str,
    ) -> Result<RecordedEvent, EventLogError> {
        validate_field("redaction reason", reason)?;
        let prefix = self.prefix.clone();
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let now = format_time(OffsetDateTime::now_utc())?;
        let changed = transaction
            .execute(
                &format!(
                    "UPDATE {prefix}_events SET data = ?5, redacted_at = ?6
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3 AND version = ?4"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    to_i64(version)?,
                    redaction_tombstone(reason).to_string(),
                    now,
                ],
            )
            .map_err(backend)?;
        if changed == 0 {
            return Err(EventLogError::NotFound);
        }
        transaction
            .execute(
                &format!(
                    "DELETE FROM {prefix}_snapshots
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3 AND version >= ?4"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    to_i64(version)?
                ],
            )
            .map_err(backend)?;
        let mut events = select_versions(
            &transaction,
            &prefix,
            stream,
            to_i64(version)?,
            to_i64(version)?,
        )?;
        transaction.commit().map_err(backend)?;
        events.pop().ok_or(EventLogError::NotFound)
    }

    fn save_snapshot(&self, stream: &StreamId, snapshot: &Snapshot) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        guard
            .execute(
                &format!(
                    "INSERT INTO {prefix}_snapshots (
                         tenant_id, stream_type, stream_id, version, state_schema_version, state,
                         recorded_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT (tenant_id, stream_type, stream_id) DO UPDATE SET
                         version = excluded.version,
                         state_schema_version = excluded.state_schema_version,
                         state = excluded.state,
                         recorded_at = excluded.recorded_at"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                    to_i64(snapshot.version)?,
                    i64::from(snapshot.state_schema_version),
                    snapshot.state.to_string(),
                    format_time(snapshot.recorded_at)?,
                ],
            )
            .map(|_| ())
            .map_err(backend)
    }

    fn load_snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let row: Option<(i64, i64, String, String)> = guard
            .query_row(
                &format!(
                    "SELECT version, state_schema_version, state, recorded_at
                     FROM {prefix}_snapshots
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3"
                ),
                params![
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id()
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(backend)?;
        let Some((version, state_schema_version, state, recorded_at)) = row else {
            return Ok(None);
        };
        let Ok(state) = serde_json::from_str(&state) else {
            // A snapshot is a cache. One that cannot be read is discarded, never repaired, and
            // the fold restarts from zero.
            return Ok(None);
        };
        Ok(Some(Snapshot {
            version: to_u64(version)?,
            state_schema_version: to_u32(state_schema_version)?,
            state,
            recorded_at: parse_time(&recorded_at)?,
        }))
    }

    fn forget_tenant(&self, tenant: &TenantId) -> Result<(), EventLogError> {
        let prefix = self.prefix.clone();
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        for table in [
            "events",
            "commands",
            "snapshots",
            "projection_cursors",
            "blobs",
        ] {
            transaction
                .execute(
                    &format!("DELETE FROM {prefix}_{table} WHERE tenant_id = ?1"),
                    params![tenant.as_str()],
                )
                .map_err(backend)?;
        }
        // Every read model this owner has ever created, not only the ones registered in this
        // process. A projection table left behind after an erasure is the erased tenant, still
        // readable, in a table nobody thought to name.
        let tables: Vec<String> = {
            let mut statement = transaction
                .prepare(&format!(
                    "SELECT name FROM sqlite_master
                     WHERE type = 'table' AND name LIKE '{prefix}_p_%'"
                ))
                .map_err(backend)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let mut names = Vec::new();
            for row in rows {
                names.push(row.map_err(backend)?);
            }
            names
        };
        for table in tables {
            transaction
                .execute(
                    &format!("DELETE FROM {table} WHERE tenant_id = ?1"),
                    params![tenant.as_str()],
                )
                .map_err(backend)?;
        }
        transaction.commit().map_err(backend)
    }

    fn projection_list(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Value)>, EventLogError> {
        let limit = bounded_limit(limit);
        let table = projection_table(&self.prefix, projection.name);
        let guard = self.connection.lock().map_err(poisoned)?;
        let mut statement = guard
            .prepare(&format!(
                "SELECT row_key, body FROM {table}
                 WHERE tenant_id = ?1 AND row_key > ?2
                 ORDER BY row_key LIMIT ?3"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![
                    tenant.as_str(),
                    after_key.unwrap_or(""),
                    to_i64(limit as u64)?
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(backend)?;
        let mut found = Vec::new();
        for row in rows {
            let (key, body) = row.map_err(backend)?;
            found.push((
                key,
                serde_json::from_str(&body).map_err(|error| {
                    EventLogError::Backend(format!("stored projection row is not JSON: {error}"))
                })?,
            ));
        }
        Ok(found)
    }

    fn stream_identity(&self, tenant: &TenantId) -> Result<String, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let existing: Option<String> = guard
            .query_row(
                &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id = ?1"),
                params![tenant.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if let Some(identity) = existing {
            return Ok(identity);
        }
        let identity = new_event_id();
        guard
            .execute(
                &format!(
                    "INSERT INTO {prefix}_identity (tenant_id, stream_identity) VALUES (?1, ?2)
                     ON CONFLICT (tenant_id) DO NOTHING"
                ),
                params![tenant.as_str(), identity],
            )
            .map_err(backend)?;
        guard
            .query_row(
                &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id = ?1"),
                params![tenant.as_str()],
                |row| row.get(0),
            )
            .map_err(backend)
    }

    fn put_blob(&self, tenant: &TenantId, digest: &str, bytes: &[u8]) -> Result<(), EventLogError> {
        validate_field("digest", digest)?;
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        guard
            .execute(
                &format!(
                    "INSERT INTO {prefix}_blobs (tenant_id, digest, bytes, byte_count, recorded_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT (tenant_id, digest) DO NOTHING"
                ),
                params![
                    tenant.as_str(),
                    digest,
                    bytes,
                    to_i64(bytes.len() as u64)?,
                    format_time(OffsetDateTime::now_utc())?
                ],
            )
            .map(|_| ())
            .map_err(backend)
    }

    fn get_blob(&self, tenant: &TenantId, digest: &str) -> Result<Option<Vec<u8>>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        guard
            .query_row(
                &format!("SELECT bytes FROM {prefix}_blobs WHERE tenant_id = ?1 AND digest = ?2"),
                params![tenant.as_str(), digest],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)
    }

    fn delete_blob(&self, tenant: &TenantId, digest: &str) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        guard
            .execute(
                &format!("DELETE FROM {prefix}_blobs WHERE tenant_id = ?1 AND digest = ?2"),
                params![tenant.as_str(), digest],
            )
            .map(|_| ())
            .map_err(backend)
    }

    fn create_projections(&self, projector: &dyn Projector) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
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
            guard
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                         tenant_id TEXT NOT NULL,
                         row_key TEXT NOT NULL,
                         body TEXT NOT NULL{columns},
                         PRIMARY KEY (tenant_id, row_key)
                     );
                     {indexes}"
                ))
                .map_err(backend)?;
        }
        Ok(())
    }

    fn register_inline(&self, projector: Arc<dyn Projector>) -> Result<(), EventLogError> {
        self.create_projections(projector.as_ref())?;
        let mut names = self.inline_names.lock().map_err(poisoned)?;
        for spec in projector.projections() {
            if !names.insert(spec.name.to_owned()) {
                return Err(EventLogError::Invalid(format!(
                    "projection {} is already registered",
                    spec.name
                )));
            }
        }
        self.inline.lock().map_err(poisoned)?.push(projector);
        Ok(())
    }

    fn is_inline(&self, name: &str) -> bool {
        self.inline_names
            .lock()
            .is_ok_and(|names| names.contains(name))
    }

    fn run_catch_up(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
        batch: usize,
    ) -> Result<CatchUpProgress, EventLogError> {
        let batch = bounded_limit(batch);
        let position = self.cursor_position(projector.name(), tenant)?;
        let page = self.read_feed(tenant, position, batch)?;
        if page.events.is_empty() {
            return Ok(CatchUpProgress {
                applied: 0,
                position,
                more_waiting: false,
            });
        }
        let prefix = self.prefix.clone();
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        {
            let mut projections = SqliteProjections {
                connection: &transaction,
                prefix: &prefix,
                inline: &self.inline_names,
            };
            for recorded in &page.events {
                projector.apply(recorded, &mut projections)?;
            }
        }
        transaction
            .execute(
                &format!(
                    "INSERT INTO {prefix}_projection_cursors
                         (projection, tenant_id, global_seq, updated_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (projection, tenant_id) DO UPDATE SET
                         global_seq = excluded.global_seq,
                         updated_at = excluded.updated_at"
                ),
                params![
                    projector.name(),
                    tenant.as_str(),
                    to_i64(page.next_position)?,
                    format_time(OffsetDateTime::now_utc())?,
                ],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        Ok(CatchUpProgress {
            applied: page.events.len() as u64,
            position: page.next_position,
            more_waiting: page.has_more,
        })
    }

    fn rebuild_projection(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
    ) -> Result<u64, EventLogError> {
        let prefix = self.prefix.clone();
        {
            let guard = self.connection.lock().map_err(poisoned)?;
            for spec in projector.projections() {
                let table = projection_table(&prefix, spec.name);
                guard
                    .execute(&format!("DROP TABLE IF EXISTS {table}"), [])
                    .map_err(backend)?;
            }
            guard
                .execute(
                    &format!(
                        "DELETE FROM {prefix}_projection_cursors
                         WHERE projection = ?1 AND tenant_id = ?2"
                    ),
                    params![projector.name(), tenant.as_str()],
                )
                .map_err(backend)?;
        }
        self.create_projections(projector)?;
        let mut applied = 0;
        loop {
            let progress = self.run_catch_up(projector, tenant, MAX_READ_LIMIT)?;
            applied += progress.applied;
            if progress.applied == 0 || !progress.more_waiting {
                return Ok(applied);
            }
        }
    }

    fn projection_get(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        let prefix = &self.prefix;
        let table = projection_table(prefix, projection.name);
        let guard = self.connection.lock().map_err(poisoned)?;
        let body: Option<String> = guard
            .query_row(
                &format!("SELECT body FROM {table} WHERE tenant_id = ?1 AND row_key = ?2"),
                params![tenant.as_str(), key],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        body.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                EventLogError::Backend(format!("stored projection row is not JSON: {error}"))
            })
        })
        .transpose()
    }

    fn projection_find(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        field: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<Value>, EventLogError> {
        let position = projection.field_position(field).ok_or_else(|| {
            EventLogError::Invalid(format!(
                "{field} is not a declared indexed field of {}",
                projection.name
            ))
        })?;
        let limit = bounded_limit(limit);
        let table = projection_table(&self.prefix, projection.name);
        let guard = self.connection.lock().map_err(poisoned)?;
        let mut statement = guard
            .prepare(&format!(
                "SELECT body FROM {table}
                 WHERE tenant_id = ?1 AND idx_{position} = ?2
                 ORDER BY row_key LIMIT ?3"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![tenant.as_str(), value, to_i64(limit as u64)?],
                |row| row.get::<_, String>(0),
            )
            .map_err(backend)?;
        let mut found = Vec::new();
        for row in rows {
            let body = row.map_err(backend)?;
            found.push(serde_json::from_str(&body).map_err(|error| {
                EventLogError::Backend(format!("stored projection row is not JSON: {error}"))
            })?);
        }
        Ok(found)
    }
}

impl SqliteEventStore {
    fn cursor_position(&self, projection: &str, tenant: &TenantId) -> Result<u64, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let position: Option<i64> = guard
            .query_row(
                &format!(
                    "SELECT global_seq FROM {prefix}_projection_cursors
                     WHERE projection = ?1 AND tenant_id = ?2"
                ),
                params![projection, tenant.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        position.map_or(Ok(0), to_u64)
    }
}

/// A projection's view of the transaction it is running in.
struct SqliteProjections<'a> {
    connection: &'a Connection,
    prefix: &'a str,
    inline: &'a Mutex<BTreeSet<String>>,
}

impl ProjectionStore for SqliteProjections<'_> {
    fn upsert(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
        body: &Value,
    ) -> Result<(), EventLogError> {
        let table = projection_table(self.prefix, projection.name);
        let columns: String = joined(projection.indexed.len(), |position| {
            format!(", idx_{position}")
        });
        let placeholders: String = joined(projection.indexed.len(), |position| {
            format!(", ?{}", position + 4)
        });
        let updates: String = joined(projection.indexed.len(), |position| {
            format!(", idx_{position} = excluded.idx_{position}")
        });
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(tenant.as_str().to_owned()),
            Box::new(key.to_owned()),
            Box::new(body.to_string()),
        ];
        for field in projection.indexed {
            values.push(Box::new(indexed_value(body, field)));
        }
        let statement = format!(
            "INSERT INTO {table} (tenant_id, row_key, body{columns})
             VALUES (?1, ?2, ?3{placeholders})
             ON CONFLICT (tenant_id, row_key) DO UPDATE SET body = excluded.body{updates}"
        );
        let borrowed: Vec<&dyn rusqlite::ToSql> =
            values.iter().map(std::convert::AsRef::as_ref).collect();
        self.connection
            .execute(&statement, borrowed.as_slice())
            .map(|_| ())
            .map_err(backend)
    }

    fn delete(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<(), EventLogError> {
        let table = projection_table(self.prefix, projection.name);
        self.connection
            .execute(
                &format!("DELETE FROM {table} WHERE tenant_id = ?1 AND row_key = ?2"),
                params![tenant.as_str(), key],
            )
            .map(|_| ())
            .map_err(backend)
    }

    fn get(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        let table = projection_table(self.prefix, projection.name);
        let body: Option<String> = self
            .connection
            .query_row(
                &format!("SELECT body FROM {table} WHERE tenant_id = ?1 AND row_key = ?2"),
                params![tenant.as_str(), key],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        body.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                EventLogError::Backend(format!("stored projection row is not JSON: {error}"))
            })
        })
        .transpose()
    }

    fn get_for_update(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        // SQLite has one writer and this transaction is already IMMEDIATE, so the row is held.
        // The check that matters is the same one PostgreSQL makes: a guard may only read a read
        // model that is written in this transaction.
        let inline = self.inline.lock().map_err(poisoned)?;
        if !inline.contains(projection.name) {
            return Err(EventLogError::Invalid(format!(
                "projection {} is not driven inline, so a guard over it would be enforced late",
                projection.name
            )));
        }
        drop(inline);
        self.get(projection, tenant, key)
    }

    fn find(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        field: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<Value>, EventLogError> {
        let position = projection.field_position(field).ok_or_else(|| {
            EventLogError::Invalid(format!(
                "{field} is not a declared indexed field of {}",
                projection.name
            ))
        })?;
        let table = projection_table(self.prefix, projection.name);
        let limit = bounded_limit(limit);
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT body FROM {table}
                 WHERE tenant_id = ?1 AND idx_{position} = ?2
                 ORDER BY row_key LIMIT ?3"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![tenant.as_str(), value, to_i64(limit as u64)?],
                |row| row.get::<_, String>(0),
            )
            .map_err(backend)?;
        let mut found = Vec::new();
        for row in rows {
            let body = row.map_err(backend)?;
            found.push(serde_json::from_str(&body).map_err(|error| {
                EventLogError::Backend(format!("stored projection row is not JSON: {error}"))
            })?);
        }
        Ok(found)
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

fn select_versions(
    connection: &Connection,
    prefix: &str,
    stream: &StreamId,
    first_version: i64,
    last_version: i64,
) -> Result<Vec<RecordedEvent>, EventLogError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT {COLUMNS} FROM {prefix}_events
             WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3
               AND version >= ?4 AND version <= ?5
             ORDER BY version"
        ))
        .map_err(backend)?;
    let rows = statement
        .query_map(
            params![
                stream.tenant().as_str(),
                stream.stream_type(),
                stream.stream_id(),
                first_version,
                last_version
            ],
            read_event,
        )
        .map_err(backend)?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row.map_err(backend)??);
    }
    Ok(events)
}

fn read_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<RecordedEvent, EventLogError>> {
    let global_seq: i64 = row.get(0)?;
    let tenant: String = row.get(1)?;
    let stream_type: String = row.get(2)?;
    let stream_id: String = row.get(3)?;
    let version: i64 = row.get(4)?;
    let event_id: String = row.get(5)?;
    let name: String = row.get(6)?;
    let schema_version: i64 = row.get(7)?;
    let occurred_at: String = row.get(8)?;
    let recorded_at: String = row.get(9)?;
    let subject: String = row.get(10)?;
    let actor: String = row.get(11)?;
    let request_id: String = row.get(12)?;
    let trace_id: String = row.get(13)?;
    let causation_id: Option<String> = row.get(14)?;
    let causation_depth: i64 = row.get(15)?;
    let redacted_at: Option<String> = row.get(16)?;
    let data: String = row.get(17)?;
    Ok(build_event(
        global_seq,
        tenant,
        stream_type,
        stream_id,
        version,
        event_id,
        name,
        schema_version,
        &occurred_at,
        &recorded_at,
        subject,
        actor,
        request_id,
        trace_id,
        causation_id,
        causation_depth,
        redacted_at.as_deref(),
        &data,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_event(
    global_seq: i64,
    tenant: String,
    stream_type: String,
    stream_id: String,
    version: i64,
    event_id: String,
    name: String,
    schema_version: i64,
    occurred_at: &str,
    recorded_at: &str,
    subject: String,
    actor: String,
    request_id: String,
    trace_id: String,
    causation_id: Option<String>,
    causation_depth: i64,
    redacted_at: Option<&str>,
    data: &str,
) -> Result<RecordedEvent, EventLogError> {
    Ok(RecordedEvent {
        global_seq: to_u64(global_seq)?,
        tenant: TenantId::new(tenant)?,
        stream_type,
        stream_id,
        version: to_u64(version)?,
        event_id,
        name,
        schema_version: to_u32(schema_version)?,
        occurred_at: parse_time(occurred_at)?,
        recorded_at: parse_time(recorded_at)?,
        subject,
        actor,
        request_id,
        trace_id,
        causation_id,
        causation_depth: to_u32(causation_depth)?,
        redacted_at: redacted_at.map(parse_time).transpose()?,
        data: serde_json::from_str(data)
            .map_err(|error| EventLogError::Backend(format!("stored body is not JSON: {error}")))?,
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

fn format_time(value: OffsetDateTime) -> Result<String, EventLogError> {
    value
        .format(&Rfc3339)
        .map_err(|error| EventLogError::Backend(format!("time is not formattable: {error}")))
}

fn parse_time(value: &str) -> Result<OffsetDateTime, EventLogError> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|error| EventLogError::Backend(format!("stored time is not RFC 3339: {error}")))
}

fn to_i64(value: u64) -> Result<i64, EventLogError> {
    i64::try_from(value).map_err(|_| EventLogError::Invalid("value is out of range".to_owned()))
}

fn to_u64(value: i64) -> Result<u64, EventLogError> {
    u64::try_from(value).map_err(|_| EventLogError::Backend("stored value is negative".to_owned()))
}

fn to_u32(value: i64) -> Result<u32, EventLogError> {
    u32::try_from(value)
        .map_err(|_| EventLogError::Backend("stored value is out of range".to_owned()))
}

fn backend(error: impl std::fmt::Display) -> EventLogError {
    EventLogError::Backend(error.to_string())
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EventLogError {
    EventLogError::Backend("the store lock was poisoned by a panic".to_owned())
}
