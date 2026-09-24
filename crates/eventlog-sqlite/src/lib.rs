#![forbid(unsafe_code)]

//! The log on SQLite, which is also the log in memory.
//!
//! A file path gives an owner its local store; `:memory:` gives its tests one. They are the same
//! code, so a property proved in a test is proved for the deployment — which is exactly what a
//! separate hand-written memory backend cannot say.
//!
//! rusqlite is synchronous and has no async driver, so the sync/async bridge lives here rather
//! than in every caller: each store call runs on tokio's blocking pool and the caller just
//! `.await`s. Before this bridge was internal, every consuming module wrapped every call in
//! `spawn_blocking` by hand, and forgetting one wrap panicked a worker at startup.

mod atomic_group;
mod capture;
mod inline_admin;
mod inspection;
pub use inspection::SqliteHistoryInspector;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use eventlog_core::{
    AppendResult, BlobMigrationReport, BoxFuture, CatchUpProgress, Claim, ClaimedCommand,
    CommandMeta, EventLogError, EventStore, Expected, FeedPage, Guard, LegacyBlobMigration,
    MAX_READ_LIMIT, NewEvent, NoGuard, ProjectionSpec, ProjectionStore, Projector, RecordedEvent,
    Snapshot, SnapshotGeneration, StreamId, StreamSlice, TenantId, blob_integrity_sha256,
    bounded_limit, indexed_value, new_event_id, redaction_tombstone, validate_append,
    validate_field, validate_legacy_blob_count, validate_stored_blob,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, Row, TransactionBehavior, params};
use serde_json::Value;

/// Every envelope column, in the order [`read_event`] expects them.
const COLUMNS: &str = "global_seq, tenant_id, stream_type, stream_id, version, event_id, \
     event_name, event_schema_version, occurred_at, recorded_at, subject, actor, request_id, \
     trace_id, causation_id, causation_depth, redacted_at, data";
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// One owner's event tables in one SQLite database.
pub struct SqliteEventStore {
    inner: Arc<Inner>,
}

/// The synchronous store the blocking pool runs. Shared by `Arc` because a blocking task must
/// own what it touches: it may outlive a caller that gave up waiting.
struct Inner {
    connection: Mutex<Connection>,
    prefix: String,
    inline: Mutex<Vec<Arc<dyn Projector>>>,
    inline_names: Mutex<BTreeSet<String>>,
    admission_permit: eventlog_core::AdmissionPermit,
    registration: Mutex<bool>,
    /// Identities and instants the next appended events take instead of minting their own, in
    /// order. Empty except while [`SqliteEventStore::restore_events`] replays history.
    restored: Mutex<std::collections::VecDeque<RestoredEvent>>,
}

/// The identity and instant one replayed event had when another store first recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoredEvent {
    pub event_id: String,
    pub recorded_at: OffsetDateTime,
    /// Where the event stood in the branchable history it is copied from, when it is copied rather
    /// than replayed into a store of its own. Recorded beside the event, never in its body.
    pub origin: Option<EventOrigin>,
}

/// An event's place in a branchable history, kept when a linear store holds a copy of it: the
/// version it had there, its content digest, and the digests it followed on its stream.
///
/// Kept in its own table beside the event row rather than in `data`, so a redaction, which
/// replaces `data`, cannot erase it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventOrigin {
    pub version: u64,
    pub digest: String,
    pub parents: Vec<String>,
}

/// Each copied event's origin, by `(stream_type, stream_id, version)` in the linear store.
pub type OriginMap = BTreeMap<(String, String, u64), EventOrigin>;

impl SqliteEventStore {
    /// Give only the trusted host this grant; the domain's `EventStore` port cannot issue it.
    pub fn admission_permit(&self) -> eventlog_core::AdmissionPermit {
        self.inner.admission_permit.clone()
    }

    /// Open or create the store for one owner, identified by its table prefix.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for an unusable prefix and [`EventLogError::Backend`]
    /// when the database cannot be opened or its tables cannot be created.
    pub async fn open(path: &str, prefix: &str) -> Result<Self, EventLogError> {
        Self::open_with_blob_migration(path, prefix, LegacyBlobMigration::RefusePopulated)
            .await
            .map(|(store, _)| store)
    }

    /// Open an already provisioned file-backed owner without creating a database or tables.
    /// This path never performs a predecessor blob migration.
    ///
    /// # Errors
    /// Refuses a missing database, missing or incompatible owner schema, and inaccessible storage.
    pub async fn open_existing(path: &str, prefix: &str) -> Result<Self, EventLogError> {
        let path = path.to_owned();
        let prefix = prefix.to_owned();
        let inner = run_blocking(move || Inner::open_existing(&path, &prefix)).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Open or create the store with an explicit predecessor blob-import choice.
    ///
    /// # Errors
    /// Refuses populated predecessor bindings unless `TrustObservedBytes` is selected, and refuses
    /// malformed or foreign blob shapes before any persistent migration change.
    pub async fn open_with_blob_migration(
        path: &str,
        prefix: &str,
        migration: LegacyBlobMigration,
    ) -> Result<(Self, BlobMigrationReport), EventLogError> {
        let path = path.to_owned();
        let prefix = prefix.to_owned();
        let (inner, report) = run_blocking(move || Inner::open(&path, &prefix, migration)).await?;
        Ok((
            Self {
                inner: Arc::new(inner),
            },
            report,
        ))
    }

    /// Queue the identities and instants the next appended events take, in order.
    ///
    /// For a provider that keeps its history elsewhere and replays it into an in-memory store when
    /// it opens: the replayed events keep the ids and instants they were first recorded with,
    /// instead of receiving new ones on every open. Positions are still assigned here, in replay
    /// order, and while events are queued an append's expectation is not checked: it was checked
    /// when the event was first written. Never call this on a store other writers use; an append by
    /// another caller would take a queued identity.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when events from an earlier call are still queued, which
    /// means that call's append did not consume them.
    pub fn restore_events(&self, events: Vec<RestoredEvent>) -> Result<(), EventLogError> {
        let mut queue = self.inner.restored.lock().map_err(poisoned)?;
        if !queue.is_empty() {
            return Err(EventLogError::Invalid(
                "restored identities from an earlier replay were never used".into(),
            ));
        }
        queue.extend(events);
        Ok(())
    }

    /// How many queued restored identities no append has taken yet.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the queue's lock is poisoned.
    pub fn restored_pending(&self) -> Result<usize, EventLogError> {
        Ok(self.inner.restored.lock().map_err(poisoned)?.len())
    }

    /// Every origin a copy recorded for `tenant`'s events.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the table cannot be read or holds a row this
    /// reader did not write.
    pub async fn origins(&self, tenant: &TenantId) -> Result<OriginMap, EventLogError> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        run_blocking(move || inner.origins(&tenant)).await
    }

    /// The stream identity `tenant` already has, without minting one as
    /// [`EventStore::stream_identity`] does, so a caller can check before it writes.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the identity table cannot be read.
    pub async fn stored_stream_identity(
        &self,
        tenant: &TenantId,
    ) -> Result<Option<String>, EventLogError> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        run_blocking(move || {
            let prefix = &inner.prefix;
            let connection = inner.connection.lock().map_err(poisoned)?;
            connection
                .query_row(
                    &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id = ?1"),
                    params![tenant.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)
        })
        .await
    }

    /// Record the stream identity another store gave `tenant`, before anything asks for it.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] when the tenant already has a different identity.
    pub async fn restore_stream_identity(
        &self,
        tenant: &TenantId,
        identity: &str,
    ) -> Result<(), EventLogError> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let identity = identity.to_owned();
        run_blocking(move || inner.restore_stream_identity(&tenant, &identity)).await
    }

    /// An empty store that lives only as long as the process.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the database cannot be created.
    pub async fn in_memory(prefix: &str) -> Result<Self, EventLogError> {
        let prefix = prefix.to_owned();
        let (inner, _) = run_blocking(move || Inner::in_memory(&prefix)).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The whole database, as bytes [`SqliteEventStore::from_image`] reopens.
    ///
    /// An in-memory store that a caller rebuilds by replay can keep this instead and skip the
    /// replay next time. Registrations are not part of it: a reopened store's inline projectors
    /// are registered again.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] when the database cannot be serialized.
    pub async fn image(&self) -> Result<Vec<u8>, EventLogError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || {
            let connection = inner.connection.lock().map_err(poisoned)?;
            let data = connection.serialize("main").map_err(backend)?;
            Ok(data.to_vec())
        })
        .await
    }

    /// An in-memory store holding `image`, as [`SqliteEventStore::image`] wrote it.
    ///
    /// The image's tables are checked as any reopened database's are.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] or [`EventLogError::Invalid`] for bytes that are not
    /// such an image.
    pub async fn from_image(prefix: &str, image: Vec<u8>) -> Result<Self, EventLogError> {
        let prefix = prefix.to_owned();
        let (inner, _) = run_blocking(move || {
            let mut connection = Connection::open_in_memory().map_err(backend)?;
            connection
                .deserialize_read_exact("main", image.as_slice(), image.len(), false)
                .map_err(backend)?;
            let (inner, report) =
                Inner::from_connection(connection, &prefix, LegacyBlobMigration::RefusePopulated)?;
            inner.require_existing_schema()?;
            Ok((inner, report))
        })
        .await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

/// Run one store call where rusqlite is allowed to block.
///
/// # Errors
/// Returns whatever the call returned, or [`EventLogError::Backend`] when the blocking task
/// panicked — a panic on the pool must surface as an error here, not vanish.
async fn run_blocking<T, F>(work: F) -> Result<T, EventLogError>
where
    F: FnOnce() -> Result<T, EventLogError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(_) => Err(EventLogError::Backend(
            "the store's blocking worker panicked".to_owned(),
        )),
    }
}

impl Inner {
    fn open_existing(path: &str, prefix: &str) -> Result<Self, EventLogError> {
        validate_prefix(prefix)?;
        if path.is_empty() || path == ":memory:" {
            return Err(EventLogError::Invalid(
                "existing SQLite authority requires a file path".into(),
            ));
        }
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(backend)?;
        connection
            .execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(backend)?;
        let inner = Self::new(connection, prefix);
        inner.require_existing_schema()?;
        Ok(inner)
    }

    fn open(
        path: &str,
        prefix: &str,
        migration: LegacyBlobMigration,
    ) -> Result<(Self, BlobMigrationReport), EventLogError> {
        let connection = Connection::open(path).map_err(backend)?;
        Self::from_connection(connection, prefix, migration)
    }

    fn in_memory(prefix: &str) -> Result<(Self, BlobMigrationReport), EventLogError> {
        let connection = Connection::open_in_memory().map_err(backend)?;
        Self::from_connection(connection, prefix, LegacyBlobMigration::RefusePopulated)
    }

    fn from_connection(
        connection: Connection,
        prefix: &str,
        migration: LegacyBlobMigration,
    ) -> Result<(Self, BlobMigrationReport), EventLogError> {
        validate_prefix(prefix)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(backend)?;
        let store = Self::new(connection, prefix);
        let report = store.create_tables(migration)?;
        Ok((store, report))
    }

    fn new(connection: Connection, prefix: &str) -> Self {
        Self {
            connection: Mutex::new(connection),
            prefix: prefix.to_owned(),
            inline: Mutex::new(Vec::new()),
            inline_names: Mutex::new(BTreeSet::new()),
            admission_permit: eventlog_core::AdmissionPermit::default(),
            registration: Mutex::new(false),
            restored: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    fn require_existing_schema(&self) -> Result<(), EventLogError> {
        let connection = self.connection.lock().map_err(poisoned)?;
        self.refuse_foreign_tables(&connection)?;
        for suffix in [
            "events",
            "commands",
            "claims",
            "identity",
            "blobs",
            "projection_cursors",
            "snapshots",
            "snapshot_generations",
            "append_groups",
            "scope_counters",
            "projection_registry",
        ] {
            let name = format!("{}_{}", self.prefix, suffix);
            let present: Option<String> = connection
                .query_row(
                    "SELECT type FROM sqlite_master WHERE name=?1 AND type='table'",
                    params![name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if present.is_none() {
                return Err(EventLogError::Invalid(format!(
                    "SQLite owner table {name} is absent"
                )));
            }
        }
        if sqlite_blob_shape(&connection, &self.prefix)? != SqliteBlobShape::Current {
            return Err(EventLogError::Invalid(
                "existing SQLite owner has an unupgraded blob schema".into(),
            ));
        }
        self.validate_snapshot_generation_schema(&connection)
    }

    /// Refuse a table of ours that somebody else made.
    ///
    /// `CREATE TABLE IF NOT EXISTS` **silently does nothing** when a table of that name already
    /// exists, whatever shape it has. A module that kept its own `<prefix>_events` before moving
    /// onto the log therefore gets the old table, and the failure surfaces later as a missing
    /// column inside a wall of DDL — which names neither the file nor the collision.
    ///
    /// This is the same hazard
    /// `model/modules/docs/stories/M-022-one-replay-rule-behind-one-port.md`
    /// records for `PRIMARY KEY` changes, and no test could have caught it: every test starts with
    /// an empty database. A running deployment found it.
    fn refuse_foreign_tables(&self, connection: &Connection) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let events = format!("{prefix}_events");
        let existing: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![events],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        let Some(sql) = existing else {
            return Ok(());
        };
        if sql.contains("global_seq") {
            return Ok(());
        }
        Err(EventLogError::Backend(format!(
            "this database already has a table called {events} that this kit did not create, so \
             its own tables cannot be made. It is almost certainly {prefix}'s previous store. \
             Point this owner at a different database, or rename the old tables out of the way \
             once you have decided what to do with what is in them."
        )))
    }

    fn create_tables(
        &self,
        migration: LegacyBlobMigration,
    ) -> Result<BlobMigrationReport, EventLogError> {
        let prefix = &self.prefix;
        let blobs = sqlite_blob_table_body(&[
            SQLITE_BLOB_PREDECESSOR_COLUMNS,
            SQLITE_BLOB_HASH_COLUMN,
            SQLITE_BLOB_EDITION_COLUMN,
            SQLITE_BLOB_PRIMARY_KEY,
        ]);
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
             CREATE TABLE IF NOT EXISTS {prefix}_blobs ({blobs});
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
             );
             CREATE TABLE IF NOT EXISTS {prefix}_snapshot_generations (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 generation TEXT NOT NULL,
                 cached_generation TEXT,
                 PRIMARY KEY (tenant_id, stream_type, stream_id)
             );
             {}
             CREATE TABLE IF NOT EXISTS {prefix}_scope_counters (
                 coordinate TEXT PRIMARY KEY,
                 held INTEGER NOT NULL CHECK(held>=0)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_projection_registry(
                 projection_name TEXT PRIMARY KEY,
                 indexed_fields TEXT NOT NULL
             );
             {}",
            atomic_group::ddl(prefix),
            origins_ddl(prefix)
        );
        let mut connection = self.connection.lock().map_err(poisoned)?;
        self.refuse_foreign_tables(&connection)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let blob_shape = sqlite_blob_shape(&transaction, prefix)?;
        let mut report = BlobMigrationReport::default();
        match blob_shape {
            SqliteBlobShape::Missing => {
                report.upgraded = true;
            }
            SqliteBlobShape::Current => {}
            SqliteBlobShape::Legacy => {
                let table = format!("{prefix}_blobs");
                let rows: i64 = transaction
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .map_err(backend)?;
                let rows = u64::try_from(rows).map_err(|_| {
                    EventLogError::Backend("stored blob row count is invalid".into())
                })?;
                if rows != 0 && migration == LegacyBlobMigration::RefusePopulated {
                    return Err(EventLogError::Invalid(format!(
                        "populated predecessor blob table has {rows} rows; explicit TrustObservedBytes migration required"
                    )));
                }
                transaction
                    .execute_batch(&format!(
                        "ALTER TABLE {table} ADD COLUMN {SQLITE_BLOB_HASH_COLUMN}"
                    ))
                    .map_err(backend)?;
                let mut after: Option<(String, String)> = None;
                loop {
                    let read = |row: &Row<'_>| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    };
                    let row = if let Some((tenant, digest)) = &after {
                        transaction
                            .query_row(
                                &format!(
                            "SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {table}
                             WHERE tenant_id > ?1 OR (tenant_id = ?1 AND digest > ?2)
                             ORDER BY tenant_id,digest LIMIT 1"
                        ),
                                params![tenant, digest],
                                read,
                            )
                            .optional()
                    } else {
                        transaction
                            .query_row(
                                &format!(
                            "SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {table}
                             ORDER BY tenant_id,digest LIMIT 1"
                        ),
                                [],
                                read,
                            )
                            .optional()
                    }
                    .map_err(backend)?;
                    let Some((tenant, digest, bytes, count, recorded_at)) = row else {
                        break;
                    };
                    validate_field("stored blob tenant", &tenant).map_err(|_| {
                        EventLogError::Backend("stored blob identity is invalid".into())
                    })?;
                    validate_field("stored blob digest", &digest).map_err(|_| {
                        EventLogError::Backend("stored blob identity is invalid".into())
                    })?;
                    parse_time(&recorded_at)?;
                    validate_legacy_blob_count(&bytes, count)?;
                    let hash = blob_integrity_sha256(&bytes);
                    transaction
                        .execute(
                            &format!(
                                "UPDATE {table} SET integrity_sha256=?1
                                 WHERE tenant_id=?2 AND digest=?3"
                            ),
                            params![hash, tenant, digest],
                        )
                        .map_err(backend)?;
                    after = Some((tenant, digest));
                }
                transaction
                    .execute_batch(&format!(
                        "ALTER TABLE {table} ADD COLUMN {SQLITE_BLOB_EDITION_COLUMN}"
                    ))
                    .map_err(backend)?;
                report = BlobMigrationReport {
                    upgraded: true,
                    trusted_legacy_rows: rows,
                };
            }
        }
        transaction.execute_batch(&statements).map_err(backend)?;
        if sqlite_blob_shape(&transaction, prefix)? != SqliteBlobShape::Current {
            return Err(EventLogError::Invalid(
                "incompatible SQLite blob schema".into(),
            ));
        }
        if report.upgraded {
            validate_all_sqlite_blobs(&transaction, prefix)?;
        }
        self.validate_snapshot_generation_schema(&transaction)?;
        transaction.commit().map_err(backend)?;
        Ok(report)
    }

    fn validate_snapshot_generation_schema(
        &self,
        connection: &Connection,
    ) -> Result<(), EventLogError> {
        let mut query = connection
            .prepare(&format!(
                "PRAGMA table_info({}_snapshot_generations)",
                self.prefix
            ))
            .map_err(backend)?;
        let columns = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, u32>(5)?,
                ))
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        let expected = [
            ("tenant_id", true, 1),
            ("stream_type", true, 2),
            ("stream_id", true, 3),
            ("generation", true, 0),
            ("cached_generation", false, 0),
        ]
        .map(|(name, required, key)| (name.to_owned(), "TEXT".to_owned(), required, None, key));
        if columns != expected {
            return Err(EventLogError::Invalid(
                "incompatible SQLite snapshot generation schema".into(),
            ));
        }
        drop(query);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqliteBlobShape {
    Missing,
    Legacy,
    Current,
}

/// The predecessor's five blob columns, which the integrity edition keeps untouched.
const SQLITE_BLOB_PREDECESSOR_COLUMNS: &str = "tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL,
                 bytes BLOB NOT NULL,
                 byte_count INTEGER NOT NULL,
                 recorded_at TEXT NOT NULL";

/// The nullable hash column an admitted predecessor is backfilled through.
const SQLITE_BLOB_HASH_COLUMN: &str = "integrity_sha256 TEXT";

/// The edition tag whose closed check refuses an old five-column `INSERT`.
const SQLITE_BLOB_EDITION_COLUMN: &str = "integrity_v1 INTEGER NOT NULL DEFAULT 1
                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)";

/// The only key this table has.
const SQLITE_BLOB_PRIMARY_KEY: &str = "PRIMARY KEY (tenant_id, digest)";

/// One `CREATE TABLE` body, written the one way this kit writes it.
///
/// `create_tables` builds its own DDL through this function and admission builds what it expects
/// through the same one, so the shape that is written and the shape that is recognised cannot
/// drift apart.
fn sqlite_blob_table_body(parts: &[&str]) -> String {
    parts.join(",\n                 ")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqliteSchemaToken<'a> {
    Bare(&'a str),
    LeftParenthesis,
    RightParenthesis,
    Equals,
    Other,
}

fn sqlite_schema_tokens(sql: &str) -> Vec<SqliteSchemaToken<'_>> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        match bytes[position] {
            byte if byte.is_ascii_whitespace() => position += 1,
            b'-' if bytes.get(position + 1) == Some(&b'-') => {
                position += 2;
                while bytes.get(position).is_some_and(|byte| *byte != b'\n') {
                    position += 1;
                }
                tokens.push(SqliteSchemaToken::Other);
            }
            b'/' if bytes.get(position + 1) == Some(&b'*') => {
                position += 2;
                while position + 1 < bytes.len()
                    && (bytes[position] != b'*' || bytes[position + 1] != b'/')
                {
                    position += 1;
                }
                position = (position + 2).min(bytes.len());
                tokens.push(SqliteSchemaToken::Other);
            }
            quote @ (b'\'' | b'"' | b'`') => {
                position += 1;
                while position < bytes.len() {
                    if bytes[position] != quote {
                        position += 1;
                    } else if bytes.get(position + 1) == Some(&quote) {
                        position += 2;
                    } else {
                        position += 1;
                        break;
                    }
                }
                tokens.push(SqliteSchemaToken::Other);
            }
            b'[' => {
                position += 1;
                while bytes.get(position).is_some_and(|byte| *byte != b']') {
                    position += 1;
                }
                position = (position + 1).min(bytes.len());
                tokens.push(SqliteSchemaToken::Other);
            }
            b'(' => {
                tokens.push(SqliteSchemaToken::LeftParenthesis);
                position += 1;
            }
            b')' => {
                tokens.push(SqliteSchemaToken::RightParenthesis);
                position += 1;
            }
            b'=' => {
                tokens.push(SqliteSchemaToken::Equals);
                position += 1;
            }
            byte if byte.is_ascii_alphanumeric() || byte == b'_' => {
                let start = position;
                position += 1;
                while bytes
                    .get(position)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    position += 1;
                }
                tokens.push(SqliteSchemaToken::Bare(&sql[start..position]));
            }
            _ => {
                tokens.push(SqliteSchemaToken::Other);
                position += 1;
            }
        }
    }
    tokens
}

fn has_exact_sqlite_blob_integrity_check(sql: &str) -> bool {
    const EXPECTED: [SqliteSchemaToken<'static>; 8] = [
        SqliteSchemaToken::Bare("integrity_v1"),
        SqliteSchemaToken::Equals,
        SqliteSchemaToken::Bare("1"),
        SqliteSchemaToken::Bare("AND"),
        SqliteSchemaToken::Bare("integrity_sha256"),
        SqliteSchemaToken::Bare("IS"),
        SqliteSchemaToken::Bare("NOT"),
        SqliteSchemaToken::Bare("NULL"),
    ];
    let tokens = sqlite_schema_tokens(sql);
    let mut checks = 0;
    let mut expected = false;
    let mut position = 0;
    while position + 1 < tokens.len() {
        if tokens[position] != SqliteSchemaToken::Bare("CHECK")
            || tokens[position + 1] != SqliteSchemaToken::LeftParenthesis
        {
            position += 1;
            continue;
        }
        checks += 1;
        let expression_start = position + 2;
        let mut expression_end = expression_start;
        let mut depth = 1_u32;
        while expression_end < tokens.len() && depth != 0 {
            match tokens[expression_end] {
                SqliteSchemaToken::LeftParenthesis => depth += 1,
                SqliteSchemaToken::RightParenthesis => depth -= 1,
                _ => {}
            }
            expression_end += 1;
        }
        if depth != 0 {
            return false;
        }
        expected = tokens[expression_start..expression_end - 1] == EXPECTED;
        position = expression_end;
    }
    checks == 1 && expected
}

/// The tokens between a stored `CREATE TABLE` statement's outermost parentheses.
///
/// `None` when there is no balanced parenthesised body, or when any token follows the closing
/// parenthesis — table options such as `WITHOUT ROWID` and `STRICT` arrive there.
fn sqlite_table_body_tokens(sql: &str) -> Option<Vec<SqliteSchemaToken<'_>>> {
    let tokens = sqlite_schema_tokens(sql);
    let open = tokens
        .iter()
        .position(|token| *token == SqliteSchemaToken::LeftParenthesis)?;
    let mut depth = 0_u32;
    for (offset, token) in tokens[open..].iter().enumerate() {
        match token {
            SqliteSchemaToken::LeftParenthesis => depth += 1,
            SqliteSchemaToken::RightParenthesis => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    if open + offset + 1 != tokens.len() {
                        return None;
                    }
                    return Some(tokens[open + 1..open + offset].to_vec());
                }
            }
            _ => {}
        }
    }
    None
}

/// Recognise a stored blob table by its complete declared body.
///
/// `PRAGMA table_xinfo`, `PRAGMA index_list` and the check tokenizer each answer one question, and
/// a clause they were not asked about is admitted by all three: a `COLLATE` on `digest` makes one
/// opaque identity answer for another, a primary-key `ON CONFLICT` replaces the kit's own conflict
/// resolution, a `REFERENCES ... ON DELETE CASCADE` lets a foreign row delete an admitted binding.
/// Enumerating those three clauses would leave the next one, so this compares the whole body
/// instead: every token of an admitted table is a token this kit itself wrote. `GENERATED`,
/// `UNIQUE`, `AUTOINCREMENT`, a second `CHECK`, a different `DEFAULT` and every clause nobody has
/// thought of yet are refused by the same equality, without naming any of them.
fn recognized_sqlite_blob_shape(sql: &str) -> Option<SqliteBlobShape> {
    let body = sqlite_table_body_tokens(sql)?;
    let predecessor =
        sqlite_blob_table_body(&[SQLITE_BLOB_PREDECESSOR_COLUMNS, SQLITE_BLOB_PRIMARY_KEY]);
    if body == sqlite_schema_tokens(&predecessor) {
        return Some(SqliteBlobShape::Legacy);
    }
    let created = sqlite_blob_table_body(&[
        SQLITE_BLOB_PREDECESSOR_COLUMNS,
        SQLITE_BLOB_HASH_COLUMN,
        SQLITE_BLOB_EDITION_COLUMN,
        SQLITE_BLOB_PRIMARY_KEY,
    ]);
    // `ALTER TABLE ... ADD COLUMN` rewrites the stored statement by inserting the added column
    // after the last column definition, which leaves the key last again. The second ordering is
    // accepted so that a table whose statement SQLite rewrote some other way is still the same
    // table: identical columns in identical order, types, nullability, defaults, key and check.
    let migrated = sqlite_blob_table_body(&[
        SQLITE_BLOB_PREDECESSOR_COLUMNS,
        SQLITE_BLOB_PRIMARY_KEY,
        SQLITE_BLOB_HASH_COLUMN,
        SQLITE_BLOB_EDITION_COLUMN,
    ]);
    if body == sqlite_schema_tokens(&created) || body == sqlite_schema_tokens(&migrated) {
        return Some(SqliteBlobShape::Current);
    }
    None
}

fn sqlite_blob_shape(
    connection: &Connection,
    prefix: &str,
) -> Result<SqliteBlobShape, EventLogError> {
    let table = format!("{prefix}_blobs");
    let relation = connection
        .query_row(
            "SELECT type,sql FROM sqlite_master WHERE name=?1 AND type IN ('table','view')",
            params![table],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()
        .map_err(backend)?;
    let Some((kind, sql)) = relation else {
        return Ok(SqliteBlobShape::Missing);
    };
    let sql =
        sql.ok_or_else(|| EventLogError::Invalid("incompatible SQLite blob schema".into()))?;
    if kind != "table" {
        return Err(EventLogError::Invalid(
            "incompatible SQLite blob relation".into(),
        ));
    }
    let hidden_objects: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE tbl_name=?1 AND type='trigger'",
            params![table],
            |row| row.get(0),
        )
        .map_err(backend)?;
    if hidden_objects != 0 || sql.contains("WITHOUT ROWID") || sql.contains("STRICT") {
        return Err(EventLogError::Invalid(
            "unsupported SQLite blob trigger or table behavior".into(),
        ));
    }
    let recognized = recognized_sqlite_blob_shape(&sql)
        .ok_or_else(|| EventLogError::Invalid("unrecognized SQLite blob table semantics".into()))?;
    let columns = {
        let mut statement = connection
            .prepare(&format!("PRAGMA table_xinfo({table})"))
            .map_err(backend)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
    };
    let legacy = vec![
        ("tenant_id".into(), "TEXT".into(), true, None, 1, 0),
        ("digest".into(), "TEXT".into(), true, None, 2, 0),
        ("bytes".into(), "BLOB".into(), true, None, 0, 0),
        ("byte_count".into(), "INTEGER".into(), true, None, 0, 0),
        ("recorded_at".into(), "TEXT".into(), true, None, 0, 0),
    ];
    let mut current = legacy.clone();
    current.extend([
        ("integrity_sha256".into(), "TEXT".into(), false, None, 0, 0),
        (
            "integrity_v1".into(),
            "INTEGER".into(),
            true,
            Some("1".into()),
            0,
            0,
        ),
    ]);
    let shape = if columns == legacy {
        SqliteBlobShape::Legacy
    } else if columns == current && has_exact_sqlite_blob_integrity_check(&sql) {
        SqliteBlobShape::Current
    } else {
        return Err(EventLogError::Invalid(
            "incompatible SQLite blob schema".into(),
        ));
    };
    if shape != recognized {
        return Err(EventLogError::Invalid(
            "incompatible SQLite blob schema".into(),
        ));
    }
    let foreign_keys = {
        let mut statement = connection
            .prepare(&format!("PRAGMA foreign_key_list({table})"))
            .map_err(backend)?;
        statement
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
    };
    if !foreign_keys.is_empty() {
        return Err(EventLogError::Invalid(
            "unsupported SQLite blob foreign key behavior".into(),
        ));
    }
    let indexes = {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_list({table})"))
            .map_err(backend)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
    };
    if indexes.len() != 1 || !indexes[0].1 || indexes[0].2 != "pk" || indexes[0].3 {
        return Err(EventLogError::Invalid(
            "incompatible SQLite blob indexes".into(),
        ));
    }
    let index_columns = {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_info({})", indexes[0].0))
            .map_err(backend)?;
        statement
            .query_map([], |row| row.get::<_, String>(2))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
    };
    if index_columns != ["tenant_id", "digest"] {
        return Err(EventLogError::Invalid(
            "incompatible SQLite blob primary key".into(),
        ));
    }
    // The key's collation decides which spellings of an opaque digest are one identity, and no
    // column pragma reports it. The key index does.
    let key_collations = {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_xinfo({})", indexes[0].0))
            .map_err(backend)?;
        statement
            .query_map([], |row| row.get::<_, Option<String>>(4))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
    };
    if key_collations
        .iter()
        .flatten()
        .any(|collation| !collation.eq_ignore_ascii_case("BINARY"))
    {
        return Err(EventLogError::Invalid(
            "unsupported SQLite blob key collation".into(),
        ));
    }
    Ok(shape)
}

fn validate_all_sqlite_blobs(connection: &Connection, prefix: &str) -> Result<(), EventLogError> {
    let table = format!("{prefix}_blobs");
    let mut after: Option<(String, String)> = None;
    loop {
        let read = |row: &Row<'_>| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, i64>(6)?,
            ))
        };
        let row = if let Some((tenant, digest)) = &after {
            connection
                .query_row(
                    &format!(
                "SELECT tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1
                 FROM {table} WHERE tenant_id > ?1 OR (tenant_id = ?1 AND digest > ?2)
                 ORDER BY tenant_id,digest LIMIT 1"
            ),
                    params![tenant, digest],
                    read,
                )
                .optional()
        } else {
            connection
                .query_row(
                    &format!(
                "SELECT tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1
                 FROM {table} ORDER BY tenant_id,digest LIMIT 1"
            ),
                    [],
                    read,
                )
                .optional()
        }
        .map_err(backend)?;
        let Some((tenant, digest, bytes, count, recorded_at, hash, version)) = row else {
            break;
        };
        validate_field("stored blob tenant", &tenant)
            .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
        validate_field("stored blob digest", &digest)
            .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
        parse_time(&recorded_at)?;
        validate_stored_blob(bytes, count, hash, version)?;
        after = Some((tenant, digest));
    }
    Ok(())
}

impl EventStore for SqliteEventStore {
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
        guard: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendResult, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        let events = events.to_vec();
        let meta = meta.clone();
        Box::pin(run_blocking(move || {
            inner.append_guarded(&stream, expected, &events, &meta, guard.as_ref())
        }))
    }

    fn recorded_claim<'a>(
        &'a self,
        tenant: &'a TenantId,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Option<ClaimedCommand>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let claim = claim.clone();
        Box::pin(run_blocking(move || inner.recorded_claim(&tenant, &claim)))
    }

    fn recorded_command<'a>(
        &'a self,
        stream: &'a StreamId,
        idempotency_key: &'a str,
        request_hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<AppendResult>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        let idempotency_key = idempotency_key.to_owned();
        let request_hash = request_hash.to_owned();
        Box::pin(run_blocking(move || {
            inner.recorded_command(&stream, &idempotency_key, &request_hash)
        }))
    }

    fn read_stream<'a>(
        &'a self,
        stream: &'a StreamId,
        after_version: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        Box::pin(run_blocking(move || {
            inner.read_stream(&stream, after_version, limit)
        }))
    }

    fn stream_version<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        Box::pin(run_blocking(move || inner.stream_version(&stream)))
    }

    fn read_feed<'a>(
        &'a self,
        tenant: &'a TenantId,
        after_position: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<FeedPage, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || {
            inner.read_feed(&tenant, after_position, limit)
        }))
    }

    fn redact<'a>(
        &'a self,
        stream: &'a StreamId,
        version: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<RecordedEvent, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        let reason = reason.to_owned();
        Box::pin(run_blocking(move || {
            inner.redact(&stream, version, &reason)
        }))
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
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        Box::pin(run_blocking(move || {
            inner.snapshot_generation(&stream).map(Some)
        }))
    }

    fn save_snapshot_checked<'a>(
        &'a self,
        stream: &'a StreamId,
        snapshot: &'a Snapshot,
        generation: &'a SnapshotGeneration,
    ) -> BoxFuture<'a, Result<bool, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        let snapshot = snapshot.clone();
        let generation = *generation;
        Box::pin(run_blocking(move || {
            inner.save_snapshot_checked(&stream, &snapshot, generation)
        }))
    }

    fn load_snapshot<'a>(
        &'a self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let stream = stream.clone();
        Box::pin(run_blocking(move || inner.load_snapshot(&stream)))
    }

    fn forget_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || inner.forget_tenant(&tenant)))
    }

    fn create_projections(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(run_blocking(move || {
            inner.create_projections(projector.as_ref())
        }))
    }

    fn register_inline(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(run_blocking(move || inner.register_inline(projector)))
    }

    fn is_inline<'a>(&'a self, name: &'a str) -> BoxFuture<'a, bool> {
        // An in-memory set, not the database: nothing here can block.
        Box::pin(std::future::ready(self.inner.is_inline(name)))
    }

    fn run_catch_up<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
        batch: usize,
    ) -> BoxFuture<'a, Result<CatchUpProgress, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || {
            inner.run_catch_up(projector.as_ref(), &tenant, batch)
        }))
    }

    fn rebuild_projection<'a>(
        &'a self,
        projector: Arc<dyn Projector>,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<u64, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || {
            inner.rebuild_projection(projector.as_ref(), &tenant)
        }))
    }

    fn projection_get<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let projection = *projection;
        let tenant = tenant.clone();
        let key = key.to_owned();
        Box::pin(run_blocking(move || {
            inner.projection_get(&projection, &tenant, &key)
        }))
    }

    fn projection_find<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let projection = *projection;
        let tenant = tenant.clone();
        let field = field.to_owned();
        let value = value.to_owned();
        Box::pin(run_blocking(move || {
            inner.projection_find(&projection, &tenant, &field, &value, limit)
        }))
    }

    fn projection_list<'a>(
        &'a self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        after_key: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(String, Value)>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let projection = *projection;
        let tenant = tenant.clone();
        let after_key = after_key.map(str::to_owned);
        Box::pin(run_blocking(move || {
            inner.projection_list(&projection, &tenant, after_key.as_deref(), limit)
        }))
    }

    fn stream_identity<'a>(
        &'a self,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<String, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || inner.stream_identity(&tenant)))
    }

    fn put_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let digest = digest.to_owned();
        let bytes = bytes.to_vec();
        Box::pin(run_blocking(move || {
            inner.put_blob(&tenant, &digest, &bytes)
        }))
    }

    fn get_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let digest = digest.to_owned();
        Box::pin(run_blocking(move || inner.get_blob(&tenant, &digest)))
    }

    fn delete_blob<'a>(
        &'a self,
        tenant: &'a TenantId,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let digest = digest.to_owned();
        Box::pin(run_blocking(move || inner.delete_blob(&tenant, &digest)))
    }
}

impl Inner {
    fn append_guarded(
        &self,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        admission: &dyn Guard,
    ) -> Result<AppendResult, EventLogError> {
        validate_append(events, meta)?;
        *self.registration.lock().map_err(poisoned)? = true;
        self.keeping_restored_on_refusal(|| {
            let mut connection = self.connection.lock().map_err(poisoned)?;
            let connection = &mut *connection;
            begin_immediate(connection)?;
            let callback_failed = Arc::new(AtomicBool::new(false));
            let result = self.append_in_transaction(
                connection,
                stream,
                expected,
                events,
                meta,
                admission,
                true,
                &callback_failed,
            );
            finish_transaction(connection, result)
        })
    }

    /// Run one append transaction, and when it rolls back put back every restored identity it
    /// took. An append takes them as it inserts, before it knows it will commit; without this a
    /// refused append leaves the queue short, and the next append on the handle takes an identity
    /// and origin that belong to another event. An unknown commit may have taken effect, so its
    /// identities stay taken and the caller resolves the command by its key.
    pub(crate) fn keeping_restored_on_refusal<T>(
        &self,
        append: impl FnOnce() -> Result<T, EventLogError>,
    ) -> Result<T, EventLogError> {
        let before = self.restored.lock().map_err(poisoned)?.clone();
        let result = append();
        if let Err(error) = &result
            && !matches!(error, EventLogError::UnknownCommit)
        {
            *self.restored.lock().map_err(poisoned)? = before;
        }
        result
    }

    /// The append, between `BEGIN IMMEDIATE` and `COMMIT`.
    ///
    /// The transaction is managed by hand rather than through rusqlite's `Transaction`: a guard
    /// or an inline projector writes through a `&mut Connection`, which the borrowing
    /// `Transaction` type cannot hand out.
    // The flag selects legacy command bookkeeping; a group has only its group identity.
    #[allow(clippy::too_many_arguments)]
    fn append_in_transaction(
        &self,
        connection: &mut Connection,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        admission: &dyn Guard,
        record_command: bool,
        callback_failed: &Arc<AtomicBool>,
    ) -> Result<AppendResult, EventLogError> {
        let prefix = &self.prefix;

        if record_command {
            if let Some(claim) = &meta.claim {
                let prior:Option<(String,String,String,i64,i64)>=connection.query_row(&format!("SELECT request_digest,stream_type,stream_id,first_version,last_version FROM {prefix}_claims WHERE tenant_id=?1 AND scope=?2 AND claim_key=?3"),params![stream.tenant().as_str(),claim.scope,claim.key],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).optional().map_err(backend)?;
                if let Some((digest, kind, id, first, last)) = prior {
                    if digest != claim.digest {
                        return Err(EventLogError::IdempotencyMismatch {
                            key: claim.key.clone(),
                        });
                    }
                    let original = StreamId::new(stream.tenant().clone(), kind, id)?;
                    let events = select_versions(connection, prefix, &original, first, last)?;
                    return Ok(AppendResult {
                        first_version: to_u64(first)?,
                        last_version: to_u64(last)?,
                        events,
                        deduplicated: true,
                    });
                }
            }

            let recorded: Option<(String, i64, i64)> = connection
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
                    select_versions(connection, prefix, stream, first_version, last_version)?;
                return Ok(AppendResult {
                    first_version: to_u64(first_version)?,
                    last_version: to_u64(last_version)?,
                    events: stored,
                    deduplicated: true,
                });
            }
        }
        let head: Option<i64> = connection
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
        // A replayed append already passed its expectation when it was first written; history
        // merged since may have placed it after events its writer never saw.
        if self.restored.lock().map_err(poisoned)?.is_empty() {
            check_expected(expected, head)?;
        }

        {
            let mut projections = SqliteProjections {
                connection: &mut *connection,
                blob_prefix: prefix,
                projection_prefix: prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: Some((&self.admission_permit, stream.tenant())),
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            let result = drive(admission.check(&mut projections));
            ensure_callback_integrity(callback_failed)?;
            result?;
        }

        let minted = OffsetDateTime::now_utc();
        let occurred_at = format_time(meta.occurred_at)?;
        let mut written = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let version = head + 1 + offset as u64;
            let (event_id, now, origin) = match self.restored.lock().map_err(poisoned)?.pop_front()
            {
                Some(restored) => (restored.event_id, restored.recorded_at, restored.origin),
                None => (new_event_id(), minted, None),
            };
            let recorded_at = format_time(now)?;
            connection
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
            let global_seq = to_u64(connection.last_insert_rowid())?;
            // In the append's transaction, so a copied event and its origin commit together.
            if let Some(origin) = origin {
                connection
                    .execute_batch(&origins_ddl(prefix))
                    .map_err(backend)?;
                connection
                    .execute(
                        &format!(
                            "INSERT INTO {prefix}_{ORIGINS}
                             (tenant_id, stream_type, stream_id, version, origin)
                             VALUES (?1, ?2, ?3, ?4, ?5)"
                        ),
                        params![
                            stream.tenant().as_str(),
                            stream.stream_type(),
                            stream.stream_id(),
                            to_i64(version)?,
                            origin_value(&origin).to_string(),
                        ],
                    )
                    .map_err(backend)?;
            }
            #[cfg(test)]
            if !record_command && offset == 0 {
                atomic_group::checkpoint("group-event-1");
            }
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
                digest: None,
                parents: Vec::new(),
            });
        }

        let first_version = head + 1;
        let last_version = head + events.len() as u64;
        // The command is recorded at its last event's instant: the minted one normally, and the
        // restored one when history is replayed, so a replay leaves the receipts it found.
        let recorded_at = format_time(written.last().map_or(minted, |event| event.recorded_at))?;
        if record_command {
            connection
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
        }
        let projectors: Vec<Arc<dyn Projector>> = self.inline.lock().map_err(poisoned)?.clone();
        for projector in &projectors {
            let mut projections = SqliteProjections {
                connection: &mut *connection,
                blob_prefix: prefix,
                projection_prefix: prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: None,
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            for recorded in &written {
                let result = drive(projector.apply(recorded, &mut projections));
                ensure_callback_integrity(callback_failed)?;
                result?;
            }
        }

        if let Some(claim) = &meta.claim {
            connection
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

        ensure_callback_integrity(callback_failed)?;
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
        transaction.execute(&format!("INSERT INTO {prefix}_snapshot_generations (tenant_id,stream_type,stream_id,generation) VALUES (?1,?2,?3,?4) ON CONFLICT (tenant_id,stream_type,stream_id) DO UPDATE SET generation=excluded.generation"), params![stream.tenant().as_str(), stream.stream_type(), stream.stream_id(), eventlog_core::new_event_id()]).map_err(backend)?;
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

    fn snapshot_generation(&self, stream: &StreamId) -> Result<SnapshotGeneration, EventLogError> {
        let prefix = &self.prefix;
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        transaction.execute(&format!("INSERT INTO {prefix}_snapshot_generations (tenant_id,stream_type,stream_id,generation) VALUES (?1,?2,?3,?4) ON CONFLICT (tenant_id,stream_type,stream_id) DO NOTHING"), params![stream.tenant().as_str(),stream.stream_type(),stream.stream_id(),eventlog_core::new_event_id()]).map_err(backend)?;
        let generation: String = transaction.query_row(&format!("SELECT generation FROM {prefix}_snapshot_generations WHERE tenant_id=?1 AND stream_type=?2 AND stream_id=?3"), params![stream.tenant().as_str(),stream.stream_type(),stream.stream_id()], |row| row.get(0)).map_err(backend)?;
        let generation = generation.parse::<SnapshotGeneration>()?;
        transaction.commit().map_err(backend)?;
        Ok(generation)
    }

    fn save_snapshot_checked(
        &self,
        stream: &StreamId,
        snapshot: &Snapshot,
        generation: SnapshotGeneration,
    ) -> Result<bool, EventLogError> {
        let prefix = &self.prefix;
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let accepted = transaction.execute(&format!("UPDATE {prefix}_snapshot_generations SET cached_generation=generation WHERE tenant_id=?1 AND stream_type=?2 AND stream_id=?3 AND generation=?4"),params![stream.tenant().as_str(),stream.stream_type(),stream.stream_id(),generation.as_uuid().to_string()]).map_err(backend)?;
        if accepted == 0 {
            transaction.rollback().map_err(backend)?;
            return Ok(false);
        }
        transaction
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
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        Ok(true)
    }

    fn load_snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let row: Option<(i64, i64, String, String)> = guard
            .query_row(
                &format!(
                    "SELECT version, state_schema_version, state, recorded_at
                     FROM {prefix}_snapshots JOIN {prefix}_snapshot_generations
                     USING (tenant_id, stream_type, stream_id)
                     WHERE tenant_id = ?1 AND stream_type = ?2 AND stream_id = ?3
                     AND cached_generation = generation"
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
                    &format!("DELETE FROM {prefix}_{table} WHERE tenant_id = ?1"),
                    params![tenant.as_str()],
                )
                .map_err(backend)?;
        }
        // An owner provisioned before copies existed has no origin table, and nothing to erase
        // from it.
        if has_table(&transaction, &format!("{prefix}_{ORIGINS}"))? {
            transaction
                .execute(
                    &format!("DELETE FROM {prefix}_{ORIGINS} WHERE tenant_id = ?1"),
                    params![tenant.as_str()],
                )
                .map_err(backend)?;
        }
        // Every read model this owner has ever created, not only the ones registered in this
        // process. A projection table left behind after an erasure is the erased tenant, still
        // readable, in a table nobody thought to name.
        let mut tables: BTreeSet<String> = {
            let mut statement = transaction
                .prepare(&format!(
                    "SELECT projection_name FROM {prefix}_projection_registry"
                ))
                .map_err(backend)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let mut names = BTreeSet::new();
            for row in rows {
                let name = row.map_err(backend)?;
                eventlog_core::validate_identifier("registered projection", &name)?;
                names.insert(projection_table(&prefix, &name));
            }
            names
        };
        // Old files have projection tables but no persisted registry. Match a literal namespace
        // prefix (LIKE would treat its underscores as wildcards), and prove the legacy table
        // shape before erasing. A nested/ambiguous owner namespace refuses the whole transaction.
        let projection_prefix = format!("{prefix}_p_");
        {
            let mut statement = transaction
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND substr(name,1,length(?1))=?1")
                .map_err(backend)?;
            let rows = statement
                .query_map(params![projection_prefix], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            for row in rows {
                tables.insert(row.map_err(backend)?);
            }
        }
        for table in tables {
            let name = table.strip_prefix(&projection_prefix).ok_or_else(|| {
                EventLogError::Invalid("projection outside owner namespace".into())
            })?;
            eventlog_core::validate_identifier("stored projection", name)?;
            let mut statement = transaction
                .prepare(&format!("PRAGMA table_xinfo({table})"))
                .map_err(backend)?;
            let columns = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                })
                .map_err(backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(backend)?;
            let valid = columns.len() >= 3
                && columns.iter().enumerate().all(|(i, column)| {
                    let (expected_name, not_null, primary_key) = match i {
                        0 => ("tenant_id".to_owned(), 1, 1),
                        1 => ("row_key".to_owned(), 1, 2),
                        2 => ("body".to_owned(), 1, 0),
                        _ => (format!("idx_{}", i - 3), 0, 0),
                    };
                    column.0 == expected_name
                        && column.1.eq_ignore_ascii_case("TEXT")
                        && column.2 == not_null
                        && column.3 == primary_key
                        && column.4 == 0
                });
            if !valid {
                return Err(EventLogError::Invalid(format!(
                    "unsupported or ambiguous legacy projection: {table}"
                )));
            }
            drop(statement);
            transaction
                .execute(
                    &format!("DELETE FROM {table} WHERE tenant_id=?1"),
                    params![tenant.as_str()],
                )
                .map_err(backend)?;
        }
        let coordinate_prefix = format!("t{}:{}", tenant.as_str().len(), tenant.as_str());
        transaction
            .execute(
                &format!(
                    "DELETE FROM {prefix}_scope_counters WHERE substr(coordinate,1,length(?1))=?1"
                ),
                params![coordinate_prefix],
            )
            .map_err(backend)?;
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
        projection.validate()?;
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

    fn origins(&self, tenant: &TenantId) -> Result<OriginMap, EventLogError> {
        let prefix = &self.prefix;
        let connection = self.connection.lock().map_err(poisoned)?;
        let table = format!("{prefix}_{ORIGINS}");
        if !has_table(&connection, &table)? {
            return Ok(OriginMap::new());
        }
        let mut statement = connection
            .prepare(&format!(
                "SELECT stream_type, stream_id, version, origin FROM {table} WHERE tenant_id = ?1"
            ))
            .map_err(backend)?;
        let rows = statement
            .query_map(params![tenant.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(backend)?;
        let mut origins = OriginMap::new();
        for row in rows {
            let (stream_type, stream_id, version, origin) = row.map_err(backend)?;
            origins.insert(
                (stream_type, stream_id, to_u64(version)?),
                parse_origin(&origin)?,
            );
        }
        Ok(origins)
    }

    fn restore_stream_identity(
        &self,
        tenant: &TenantId,
        identity: &str,
    ) -> Result<(), EventLogError> {
        validate_field("stream identity", identity)?;
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        guard
            .execute(
                &format!(
                    "INSERT INTO {prefix}_identity (tenant_id, stream_identity) VALUES (?1, ?2)
                     ON CONFLICT (tenant_id) DO NOTHING"
                ),
                params![tenant.as_str(), identity],
            )
            .map_err(backend)?;
        let stored: String = guard
            .query_row(
                &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id = ?1"),
                params![tenant.as_str()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if stored != identity {
            return Err(EventLogError::Invalid(
                "tenant already has a different stream identity".into(),
            ));
        }
        Ok(())
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
        let integrity_sha256 = blob_integrity_sha256(bytes);
        let mut guard = self.connection.lock().map_err(poisoned)?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {prefix}_blobs (tenant_id, digest, bytes, byte_count, recorded_at,
                                                 integrity_sha256, integrity_v1)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                     ON CONFLICT (tenant_id, digest) DO NOTHING"
                ),
                params![
                    tenant.as_str(),
                    digest,
                    bytes,
                    to_i64(bytes.len() as u64)?,
                    format_time(OffsetDateTime::now_utc())?,
                    integrity_sha256,
                ],
            )
            .map_err(backend)?;
        let stored = transaction
            .query_row(
                &format!(
                    "SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {prefix}_blobs
                     WHERE tenant_id = ?1 AND digest = ?2"
                ),
                params![tenant.as_str(), digest],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(backend)?;
        let stored = checked_blob(&transaction, stored.0, stored.1, stored.2, stored.3)?;
        if stored != bytes {
            return Err(EventLogError::Invalid(
                "blob digest already names different content".into(),
            ));
        }
        transaction.commit().map_err(backend)
    }

    fn get_blob(&self, tenant: &TenantId, digest: &str) -> Result<Option<Vec<u8>>, EventLogError> {
        let prefix = &self.prefix;
        let guard = self.connection.lock().map_err(poisoned)?;
        let stored = guard
            .query_row(
                &format!(
                    "SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {prefix}_blobs
                     WHERE tenant_id = ?1 AND digest = ?2"
                ),
                params![tenant.as_str(), digest],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(backend)?;
        stored
            .map(|(bytes, count, hash, version)| checked_blob(&guard, bytes, count, hash, version))
            .transpose()
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
            let body = projection_table_body(spec.indexed.len());
            let indexes: String = joined(spec.indexed.len(), |position| {
                let index = projection_index(&table, position);
                format!(
                    "CREATE INDEX IF NOT EXISTS {index} ON {table} (tenant_id, idx_{position});"
                )
            });
            guard
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {table} ({body});{indexes}"
                ))
                .map_err(backend)?;
        }
        for spec in projector.projections() {
            let indexed = serde_json::to_string(spec.indexed)
                .map_err(|_| EventLogError::Invalid("invalid projection shape".into()))?;
            guard.execute(&format!("INSERT INTO {prefix}_projection_registry(projection_name,indexed_fields) VALUES(?1,?2) ON CONFLICT(projection_name) DO NOTHING"),params![spec.name,&indexed]).map_err(backend)?;
            let recorded:String=guard.query_row(&format!("SELECT indexed_fields FROM {prefix}_projection_registry WHERE projection_name=?1"),params![spec.name],|row|row.get(0)).map_err(backend)?;
            if recorded != indexed {
                return Err(EventLogError::Invalid(
                    "projection registration shape changed".into(),
                ));
            }
        }
        Ok(())
    }

    fn register_inline(&self, projector: Arc<dyn Projector>) -> Result<(), EventLogError> {
        let registration = self.registration.lock().map_err(poisoned)?;
        if *registration {
            return Err(EventLogError::Invalid(
                "inline registration is frozen after serving begins".into(),
            ));
        }
        {
            let projectors = self.inline.lock().map_err(poisoned)?;
            if projectors
                .iter()
                .any(|existing| existing.name() == projector.name())
            {
                return Err(EventLogError::Invalid("duplicate inline projector".into()));
            }
        }
        {
            let names = self.inline_names.lock().map_err(poisoned)?;
            if projector
                .projections()
                .iter()
                .any(|spec| names.contains(spec.name))
            {
                return Err(EventLogError::Invalid("duplicate inline projection".into()));
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
        self.inline
            .lock()
            .is_ok_and(|projectors| projectors.iter().any(|projector| projector.name() == name))
    }

    fn run_catch_up(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
        batch: usize,
    ) -> Result<CatchUpProgress, EventLogError> {
        let batch = bounded_limit(batch);
        let mut connection = self.connection.lock().map_err(poisoned)?;
        let connection = &mut *connection;
        begin_immediate(connection)?;
        let callback_failed = Arc::new(AtomicBool::new(false));
        let result = (|| {
            let position:i64=connection.query_row(&format!("SELECT global_seq FROM {}_projection_cursors WHERE projection=?1 AND tenant_id=?2",self.prefix),params![projector.name(),tenant.as_str()],|row|row.get(0)).optional().map_err(backend)?.unwrap_or(0);
            let mut events = {
                let mut statement=connection.prepare(&format!("SELECT {COLUMNS} FROM {}_events WHERE tenant_id=?1 AND global_seq>?2 ORDER BY global_seq LIMIT ?3",self.prefix)).map_err(backend)?;
                let rows = statement
                    .query_map(
                        params![tenant.as_str(), position, to_i64(batch as u64 + 1)?],
                        read_event,
                    )
                    .map_err(backend)?;
                let mut events = Vec::new();
                for row in rows {
                    events.push(row.map_err(backend)??);
                }
                events
            };
            let has_more = events.len() > batch;
            events.truncate(batch);
            let next_position = events
                .last()
                .map_or(to_u64(position)?, |event| event.global_seq);
            let page = FeedPage {
                events,
                next_position,
                has_more,
            };
            self.catch_up_in_transaction(connection, projector, tenant, &page, &callback_failed)?;
            Ok(CatchUpProgress {
                applied: page.events.len() as u64,
                position: next_position,
                more_waiting: has_more,
            })
        })();
        finish_transaction(connection, result)
    }

    fn catch_up_in_transaction(
        &self,
        connection: &mut Connection,
        projector: &dyn Projector,
        tenant: &TenantId,
        page: &FeedPage,
        callback_failed: &Arc<AtomicBool>,
    ) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        {
            let mut projections = SqliteProjections {
                connection: &mut *connection,
                blob_prefix: prefix,
                projection_prefix: prefix,
                inline: &self.inline_names,
                tenant,
                admission: None,
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            for recorded in &page.events {
                let result = drive(projector.apply(recorded, &mut projections));
                ensure_callback_integrity(callback_failed)?;
                result?;
            }
        }
        ensure_callback_integrity(callback_failed)?;
        connection
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
        Ok(())
    }

    fn rebuild_projection(
        &self,
        projector: &dyn Projector,
        tenant: &TenantId,
    ) -> Result<u64, EventLogError> {
        if self.is_inline(projector.name()) {
            return Err(EventLogError::Invalid(
                "rebuild requires a catch-up projection".into(),
            ));
        }
        let mut connection = self.connection.lock().map_err(poisoned)?;
        let connection = &mut *connection;
        begin_immediate(connection)?;
        let callback_failed = Arc::new(AtomicBool::new(false));
        let result = (|| {
            for spec in projector.projections() {
                let shadow = projection_table("eventlog_rebuild", spec.name);
                let columns = joined(spec.indexed.len(), |position| {
                    format!(",idx_{position} TEXT")
                });
                connection.execute_batch(&format!("CREATE TEMP TABLE {shadow}(tenant_id TEXT NOT NULL,row_key TEXT NOT NULL,body TEXT NOT NULL{columns},PRIMARY KEY(tenant_id,row_key))")).map_err(backend)?;
            }
            let mut position = 0_i64;
            let mut applied = 0_u64;
            loop {
                let events = {
                    let mut statement=connection.prepare(&format!("SELECT {COLUMNS} FROM {}_events WHERE tenant_id=?1 AND global_seq>?2 ORDER BY global_seq LIMIT ?3",self.prefix)).map_err(backend)?;
                    let rows = statement
                        .query_map(
                            params![tenant.as_str(), position, to_i64(MAX_READ_LIMIT as u64)?],
                            read_event,
                        )
                        .map_err(backend)?;
                    let mut events = Vec::new();
                    for row in rows {
                        events.push(row.map_err(backend)??);
                    }
                    events
                };
                if events.is_empty() {
                    break;
                }
                let mut projections = SqliteProjections {
                    connection: &mut *connection,
                    blob_prefix: &self.prefix,
                    projection_prefix: "eventlog_rebuild",
                    inline: &self.inline_names,
                    tenant,
                    admission: None,
                    callback_failed: Arc::clone(&callback_failed),
                    selected: None,
                };
                for event in &events {
                    let result = drive(projector.apply(event, &mut projections));
                    ensure_callback_integrity(&callback_failed)?;
                    result?;
                    position = to_i64(event.global_seq)?;
                    applied += 1;
                }
            }
            for spec in projector.projections() {
                let active = projection_table(&self.prefix, spec.name);
                let shadow = projection_table("eventlog_rebuild", spec.name);
                connection
                    .execute(
                        &format!("DELETE FROM {active} WHERE tenant_id=?1"),
                        params![tenant.as_str()],
                    )
                    .map_err(backend)?;
                connection
                    .execute(
                        &format!("INSERT INTO {active} SELECT * FROM {shadow} WHERE tenant_id=?1"),
                        params![tenant.as_str()],
                    )
                    .map_err(backend)?;
                connection
                    .execute_batch(&format!("DROP TABLE {shadow}"))
                    .map_err(backend)?;
            }
            ensure_callback_integrity(&callback_failed)?;
            connection.execute(&format!("INSERT INTO {}_projection_cursors(projection,tenant_id,global_seq,updated_at) VALUES(?1,?2,?3,?4) ON CONFLICT(projection,tenant_id) DO UPDATE SET global_seq=excluded.global_seq,updated_at=excluded.updated_at",self.prefix),params![projector.name(),tenant.as_str(),position,format_time(OffsetDateTime::now_utc())?]).map_err(backend)?;
            Ok(applied)
        })();
        finish_transaction(connection, result)
    }

    fn projection_get(
        &self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        let prefix = &self.prefix;
        projection.validate()?;
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
        projection.validate()?;
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

/// Open a hand-managed transaction. See [`Inner::append_in_transaction`] for why it is not
/// rusqlite's `Transaction`.
fn begin_immediate(connection: &Connection) -> Result<(), EventLogError> {
    connection.execute_batch("BEGIN IMMEDIATE").map_err(backend)
}

fn ensure_callback_integrity(callback_failed: &Arc<AtomicBool>) -> Result<(), EventLogError> {
    if callback_failed.load(Ordering::Acquire) {
        return Err(EventLogError::Backend(
            "transaction observed corrupt blob content".into(),
        ));
    }
    Ok(())
}

/// Commit on success, roll back on failure — the two ends of [`begin_immediate`].
fn finish_transaction<T>(
    connection: &Connection,
    result: Result<T, EventLogError>,
) -> Result<T, EventLogError> {
    match result {
        Ok(value) => match connection.execute_batch("COMMIT") {
            Ok(()) => Ok(value),
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(backend(error))
            }
        },
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// A waker that unparks the thread driving a future, so [`drive`] is a real executor rather
/// than a spin loop.
struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive one guard or projector future to completion on the blocking thread.
///
/// The futures a [`SqliteProjections`] hands out finish on their first poll — the rusqlite work
/// happens synchronously — so this normally never parks. It parks rather than panics when a
/// check awaits something else, so such a guard is slow here, never wrong.
fn drive<T>(mut future: BoxFuture<'_, T>) -> T {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// A future that already finished. What a [`SqliteProjections`] hands out: the rusqlite work
/// happens synchronously when the method is called, and the `.await` just unwraps it.
fn done<'a, T: Send + 'a>(value: T) -> BoxFuture<'a, T> {
    Box::pin(std::future::ready(value))
}

/// A projection's view of the transaction it is running in.
///
/// It holds the connection `&mut` — not through rusqlite's `Transaction` — because a guard's
/// future must be `Send`, and a `Transaction` borrows the connection in a way that is not.
struct SqliteProjections<'a> {
    connection: &'a mut Connection,
    blob_prefix: &'a str,
    projection_prefix: &'a str,
    inline: &'a Mutex<BTreeSet<String>>,
    tenant: &'a TenantId,
    admission: Option<(&'a eventlog_core::AdmissionPermit, &'a TenantId)>,
    callback_failed: Arc<AtomicBool>,
    selected: Option<&'a [ProjectionSpec]>,
}

impl ProjectionStore for SqliteProjections<'_> {
    fn get_blob<'a>(
        &'a mut self,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(async move {
            let result = (|| {
                validate_field("digest", digest)?;
                let stored = self
                    .connection
                    .query_row(
                        &format!(
                            "SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {}_blobs
                             WHERE tenant_id=?1 AND digest=?2",
                            self.blob_prefix
                        ),
                        params![self.tenant.as_str(), digest],
                        |row| {
                            Ok((
                                row.get::<_, Vec<u8>>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, i64>(3)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(backend)?;
                stored
                    .map(|(bytes, count, hash, version)| {
                        checked_blob(self.connection, bytes, count, hash, version)
                    })
                    .transpose()
            })();
            if matches!(result, Err(EventLogError::Backend(_))) {
                self.callback_failed.store(true, Ordering::Release);
            }
            result
        })
    }

    fn reserve<'a>(
        &'a mut self,
        permit: &'a eventlog_core::AdmissionPermit,
        reservations: &'a [eventlog_core::Reservation],
    ) -> BoxFuture<'a, Result<Vec<i64>, EventLogError>> {
        done(self.reserve_now(permit, reservations))
    }

    fn upsert<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
        body: &'a Value,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        done(self.upsert_now(projection, tenant, key, body))
    }

    fn delete<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        done(self.delete_now(projection, tenant, key))
    }

    fn get<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        done(self.get_now(projection, tenant, key))
    }

    fn get_for_update<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        done(self.get_for_update_now(projection, tenant, key))
    }

    fn find<'a>(
        &'a mut self,
        projection: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        done(self.find_now(projection, tenant, field, value, limit))
    }
}

impl SqliteProjections<'_> {
    fn validate_target(&self, projection: &ProjectionSpec) -> Result<(), EventLogError> {
        if self
            .selected
            .is_some_and(|selected| !selected.iter().any(|admitted| admitted == projection))
        {
            return Err(EventLogError::Invalid(
                "projection is outside this rebuild's selected tables".into(),
            ));
        }
        Ok(())
    }

    fn reserve_now(
        &mut self,
        permit: &eventlog_core::AdmissionPermit,
        reservations: &[eventlog_core::Reservation],
    ) -> Result<Vec<i64>, EventLogError> {
        let Some((authority, tenant)) = self.admission else {
            return Err(EventLogError::Invalid(
                "admission is confined to a trusted append guard".into(),
            ));
        };
        if !authority.same_authority(permit) {
            return Err(EventLogError::Invalid("foreign admission permit".into()));
        }
        let ordered = eventlog_core::ordered_reservations(reservations, tenant)?;
        self.connection
            .execute_batch("SAVEPOINT eventlog_reservation")
            .map_err(backend)?;
        let result = (|| {
            let table = format!("{}_scope_counters", self.projection_prefix);
            let mut next = Vec::with_capacity(ordered.len());
            // BEGIN IMMEDIATE already serializes independent connections, including absent keys.
            for (coordinate, reservation) in &ordered {
                let held = self
                    .connection
                    .query_row(
                        &format!("SELECT held FROM {table} WHERE coordinate=?1"),
                        params![coordinate],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()
                    .map_err(backend)?
                    .unwrap_or(0);
                next.push(
                    held.checked_add(reservation.delta)
                        .filter(|value| *value >= 0 && *value <= reservation.ceiling)
                        .ok_or_else(|| {
                            EventLogError::Invalid(
                                "admission ceiling or release bound refused".into(),
                            )
                        })?,
                );
            }
            for ((coordinate, _), held) in ordered.iter().zip(&next) {
                self.connection.execute(&format!("INSERT INTO {table}(coordinate,held) VALUES(?1,?2) ON CONFLICT(coordinate) DO UPDATE SET held=excluded.held"),params![coordinate,held]).map_err(backend)?;
            }
            Ok(next)
        })();
        if result.is_err() {
            self.connection
                .execute_batch("ROLLBACK TO SAVEPOINT eventlog_reservation")
                .map_err(backend)?;
        }
        self.connection
            .execute_batch("RELEASE SAVEPOINT eventlog_reservation")
            .map_err(backend)?;
        result
    }

    fn upsert_now(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
        body: &Value,
    ) -> Result<(), EventLogError> {
        self.validate_target(projection)?;
        if tenant != self.tenant {
            return Err(EventLogError::Invalid(
                "projection context cannot cross tenant".into(),
            ));
        }
        projection.validate()?;
        let table = projection_table(self.projection_prefix, projection.name);
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

    fn delete_now(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<(), EventLogError> {
        self.validate_target(projection)?;
        if tenant != self.tenant {
            return Err(EventLogError::Invalid(
                "projection context cannot cross tenant".into(),
            ));
        }
        projection.validate()?;
        let table = projection_table(self.projection_prefix, projection.name);
        self.connection
            .execute(
                &format!("DELETE FROM {table} WHERE tenant_id = ?1 AND row_key = ?2"),
                params![tenant.as_str(), key],
            )
            .map(|_| ())
            .map_err(backend)
    }

    fn get_now(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        self.validate_target(projection)?;
        if tenant != self.tenant {
            return Err(EventLogError::Invalid(
                "projection context cannot cross tenant".into(),
            ));
        }
        projection.validate()?;
        let table = projection_table(self.projection_prefix, projection.name);
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

    fn get_for_update_now(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Value>, EventLogError> {
        self.validate_target(projection)?;
        if tenant != self.tenant {
            return Err(EventLogError::Invalid(
                "projection context cannot cross tenant".into(),
            ));
        }
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
        self.get_now(projection, tenant, key)
    }

    fn find_now(
        &mut self,
        projection: &ProjectionSpec,
        tenant: &TenantId,
        field: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<Value>, EventLogError> {
        self.validate_target(projection)?;
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
        let table = projection_table(self.projection_prefix, projection.name);
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

/// The one `CREATE TABLE` body every projection table in this kit is written with.
///
/// Capture admits an actual stored table by comparing its whole declared body against this, so the
/// shape that is created and the shape that is recognised come from one place and cannot drift.
/// A `COLLATE` on `row_key`, a second `CHECK`, a `REFERENCES ... ON DELETE CASCADE` or a clause
/// nobody has thought of yet is refused by that equality without being named here.
fn projection_table_body(indexed: usize) -> String {
    let columns: String = joined(indexed, |position| format!(", idx_{position} TEXT"));
    format!(
        "tenant_id TEXT NOT NULL, row_key TEXT NOT NULL, body TEXT NOT NULL{columns}, \
         PRIMARY KEY (tenant_id, row_key)"
    )
}

/// The one name a declared field's index is created and recognised under.
fn projection_index(table: &str, position: usize) -> String {
    format!("{table}_idx_{position}")
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
        digest: None,
        parents: Vec::new(),
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
        // A linear store never forks, so there is no head set for a merge to join.
        _ => Err(EventLogError::Unsupported {
            capability: "merge expectations",
        }),
    }
}

/// Validate a stored blob row read through `connection`.
///
/// A database file can be damaged at rest, so its rows are hashed again on every read. An
/// in-memory database's rows change only through this store's own statements, and hashing them
/// again on each read cost `eventlog-tree` most of its open: a tree replays every blob into one,
/// then the replay, the projection and each capture read it back. Its metadata is still checked.
pub(crate) fn checked_blob(
    connection: &Connection,
    bytes: Vec<u8>,
    byte_count: i64,
    integrity_sha256: Option<String>,
    integrity_v1: i64,
) -> Result<Vec<u8>, EventLogError> {
    if connection.path().is_some_and(|path| !path.is_empty()) {
        return validate_stored_blob(bytes, byte_count, integrity_sha256, integrity_v1);
    }
    validate_legacy_blob_count(&bytes, byte_count)?;
    let well_formed = integrity_sha256.as_deref().is_some_and(|hash| {
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if integrity_v1 != 1 || !well_formed {
        return Err(EventLogError::Backend(
            "stored blob integrity metadata is invalid".into(),
        ));
    }
    Ok(bytes)
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

/// The suffix of the table that holds copied events' origins, beside the events table.
const ORIGINS: &str = "origins";

/// The origin table, created with every other table by `open`, and by the first origin-carrying
/// append into an owner provisioned before it existed: `open_existing` and `from_image` never
/// create a table, so an older owner gains this one only when a copy writes to it, inside that
/// append's transaction.
fn origins_ddl(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_{ORIGINS} (
             tenant_id TEXT NOT NULL,
             stream_type TEXT NOT NULL,
             stream_id TEXT NOT NULL,
             version INTEGER NOT NULL,
             origin TEXT NOT NULL,
             PRIMARY KEY (tenant_id, stream_type, stream_id, version)
         );"
    )
}

fn has_table(connection: &Connection, name: &str) -> Result<bool, EventLogError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            params![name],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(backend)
}

fn origin_value(origin: &EventOrigin) -> Value {
    serde_json::json!({
        "version": origin.version,
        "digest": origin.digest,
        "parents": origin.parents,
    })
}

fn parse_origin(text: &str) -> Result<EventOrigin, EventLogError> {
    let invalid = || EventLogError::Backend("stored origin is not one this kit wrote".to_owned());
    let value: Value = serde_json::from_str(text).map_err(|_| invalid())?;
    let map = value.as_object().ok_or_else(invalid)?;
    if map.len() != 3 {
        return Err(invalid());
    }
    Ok(EventOrigin {
        version: map
            .get("version")
            .and_then(Value::as_u64)
            .ok_or_else(invalid)?,
        digest: map
            .get("digest")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?
            .to_owned(),
        parents: map
            .get("parents")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?
            .iter()
            .map(|parent| parent.as_str().map(str::to_owned).ok_or_else(invalid))
            .collect::<Result<_, _>>()?,
    })
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

#[cfg(test)]
mod tests {
    use super::{
        SqliteBlobShape, has_exact_sqlite_blob_integrity_check, recognized_sqlite_blob_shape,
    };

    #[test]
    fn a_file_database_hashes_a_blob_again_and_an_in_memory_one_checks_its_metadata() {
        use eventlog_core::blob_integrity_sha256;
        let directory = tempfile::tempdir().expect("a scratch directory");
        let file = rusqlite::Connection::open(directory.path().join("store.db")).expect("a file");
        let memory = rusqlite::Connection::open_in_memory().expect("an in-memory database");
        let other = Some(blob_integrity_sha256(b"other"));

        let refused = super::checked_blob(&file, b"bytes".to_vec(), 5, other.clone(), 1);
        assert!(
            matches!(refused, Err(eventlog_core::EventLogError::Backend(_))),
            "{refused:?}"
        );
        assert_eq!(
            super::checked_blob(&memory, b"bytes".to_vec(), 5, other.clone(), 1).expect("trusted"),
            b"bytes"
        );
        for (count, hash, version) in [(4, other.clone(), 1), (5, None, 1), (5, other, 0)] {
            let refused = super::checked_blob(&memory, b"bytes".to_vec(), count, hash, version);
            assert!(
                matches!(refused, Err(eventlog_core::EventLogError::Backend(_))),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn sqlite_blob_admission_recognises_only_the_bodies_this_kit_writes() {
        let created = "CREATE TABLE owner_blobs (
             tenant_id TEXT NOT NULL,
             digest TEXT NOT NULL,
             bytes BLOB NOT NULL,
             byte_count INTEGER NOT NULL,
             recorded_at TEXT NOT NULL,
             integrity_sha256 TEXT,
             integrity_v1 INTEGER NOT NULL DEFAULT 1
                 CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),
             PRIMARY KEY (tenant_id, digest))";
        let migrated = "CREATE TABLE owner_blobs (
             tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BLOB NOT NULL,
             byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,
             PRIMARY KEY (tenant_id,digest), integrity_sha256 TEXT,
             integrity_v1 INTEGER NOT NULL DEFAULT 1
                 CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL))";
        let predecessor = "CREATE TABLE owner_blobs (
             tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BLOB NOT NULL,
             byte_count INTEGER NOT NULL,recorded_at TEXT NOT NULL,
             PRIMARY KEY (tenant_id,digest))";
        assert_eq!(
            recognized_sqlite_blob_shape(created),
            Some(SqliteBlobShape::Current)
        );
        assert_eq!(
            recognized_sqlite_blob_shape(migrated),
            Some(SqliteBlobShape::Current)
        );
        assert_eq!(
            recognized_sqlite_blob_shape(predecessor),
            Some(SqliteBlobShape::Legacy)
        );

        // One member per clause a column or index pragma cannot report, on both editions.
        for clause in [
            "digest TEXT NOT NULL",
            "digest TEXT NOT NULL COLLATE NOCASE",
            "digest TEXT NOT NULL COLLATE RTRIM",
            "digest TEXT NOT NULL REFERENCES elsewhere(id) ON DELETE CASCADE",
            "digest TEXT NOT NULL UNIQUE",
            "digest TEXT NOT NULL ON CONFLICT REPLACE",
            "digest TEXT NOT NULL GENERATED ALWAYS AS (tenant_id) VIRTUAL",
            "digest TEXT NOT NULL DEFAULT ''",
        ] {
            for body in [created, migrated, predecessor] {
                let altered = body.replace("digest TEXT NOT NULL", clause);
                assert_eq!(
                    recognized_sqlite_blob_shape(&altered).is_some(),
                    clause == "digest TEXT NOT NULL",
                    "{clause}"
                );
            }
        }
        for suffix in [
            "PRIMARY KEY (tenant_id, digest) ON CONFLICT REPLACE",
            "PRIMARY KEY (tenant_id, digest), CHECK (byte_count >= 0)",
            "PRIMARY KEY (tenant_id, digest), UNIQUE (digest)",
            "PRIMARY KEY (tenant_id, digest), FOREIGN KEY (tenant_id) REFERENCES elsewhere(id)",
        ] {
            let altered = created.replace("PRIMARY KEY (tenant_id, digest)", suffix);
            assert!(recognized_sqlite_blob_shape(&altered).is_none(), "{suffix}");
        }
        for option in ["WITHOUT ROWID", "STRICT"] {
            let altered = format!("{created} {option}");
            assert!(recognized_sqlite_blob_shape(&altered).is_none(), "{option}");
        }
    }

    #[test]
    fn sqlite_integrity_check_recognition_uses_sql_syntax() {
        let required = "CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)";
        assert!(has_exact_sqlite_blob_integrity_check(required));

        for sql in [
            "CONSTRAINT \"CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)\" CHECK (1)",
            "CHECK ('CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)' IS NOT NULL)",
            "/* CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL) */ CHECK (1)",
            "CHECK (integrity_v1 = 1 OR integrity_sha256 IS NOT NULL)",
            "CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL), CHECK (1)",
        ] {
            assert!(!has_exact_sqlite_blob_integrity_check(sql), "{sql}");
        }
    }
}
