//! Durable capture continuity: a checkpoint a later process continues from.
//!
//! Design: `docs/design/durable-capture-continuity.md`. The values are modelled in
//! `ess/capture/domains/capture.yaml`; the [`wire`] types are written by hand to its committed
//! JSON Schema projection under `ess/capture-generated/schema/types/`, and a test holds the
//! encoding to that schema.
//!
//! On an owner where [`SqliteEventStore::enable_durable_continuity`] ran, every write to captured
//! material through any connection runs a provider trigger that advances
//! `{prefix}_capture_continuity`'s epoch and replaces its random token in the same statement. An
//! acknowledged atomic group records, inside its own transaction, the mark it started from and
//! the mark it ended at. A later process proves continuity by walking that chain from its
//! checkpoint's mark to the current one: any write that is not an acknowledged group moves the
//! mark without an entry and breaks the chain, so the capture is complete.
//!
//! Out of scope: raw file edits that bypass SQLite, and a connection that disables triggers or
//! rewrites the continuity tables consistently.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use eventlog_core::{
    AppendGroupResult, CaptureError, CaptureLimits, CaptureUsage, CapturedBlob,
    CapturedProjectionDelta, CapturedRowChange, EventLogError, MAX_INDEXED_FIELDS, ProjectionSpec,
    RecordedEvent, TenantCaptureDelta, TenantId, validate_captured_digest, validate_captured_event,
    validate_identifier,
};
use rusqlite::{Connection, OptionalExtension as _, params};
use serde_json::value::RawValue;

use crate::{
    COLUMNS, Inner, SqliteEventStore, backend, begin_immediate, finish_transaction, has_table,
    poisoned, projection_table, read_event, run_blocking, sqlite_schema_tokens,
    sqlite_table_body_tokens, stored_json, to_i64, to_u64,
    tracked_capture::{Checkpoint, Draft, Scope, ScopedProjection, advanced_usage},
    validate_prefix,
};

/// The `format` of the SQLite encoding of a `DurableCaptureCheckpoint`.
const FORMAT: &str = "eventlog-sqlite/durable-capture-checkpoint";
/// The one encoding version this provider writes and reads.
const VERSION: u64 = 1;
/// The journal keeps whole entries, newest first, within both bounds.
const MAX_ENTRIES: i64 = 128;
const MAX_BYTES: i64 = 16 << 20;
const OPERATIONS: [&str; 3] = ["INSERT", "UPDATE", "DELETE"];

const CONTINUITY_BODY: &str = "instance TEXT NOT NULL, epoch INTEGER NOT NULL, token TEXT NOT NULL";
const JOURNAL_BODY: &str = "position INTEGER PRIMARY KEY, tenant_id TEXT NOT NULL, \
     from_epoch INTEGER NOT NULL, from_token TEXT NOT NULL, to_epoch INTEGER NOT NULL, \
     to_token TEXT NOT NULL, entry TEXT NOT NULL, bytes INTEGER NOT NULL";

fn continuity_table(prefix: &str) -> String {
    format!("{prefix}_capture_continuity")
}

fn journal_table(prefix: &str) -> String {
    format!("{prefix}_capture_journal")
}

/// The partial index of redacted events by tenant, which keeps the redacted-history check of
/// every journaled group to the redacted events of its tenant.
fn redacted_index(prefix: &str) -> (String, String) {
    let name = format!("{prefix}_capture_redacted");
    let sql =
        format!("CREATE INDEX {name} ON {prefix}_events (tenant_id) WHERE redacted_at IS NOT NULL");
    (name, sql)
}

fn operational(error: rusqlite::Error) -> CaptureError {
    CaptureError::Store(backend(error))
}

fn not_the_providers() -> EventLogError {
    EventLogError::Invalid("durable capture continuity state is not the provider's".into())
}

/// The provider's three triggers on one captured table, as `(name, SQL text)`.
///
/// The one place they are produced. Installation executes this text, which SQLite stores in
/// `sqlite_master.sql` as written, and every check compares a stored trigger's name and text
/// with this output exactly.
fn provider_triggers(prefix: &str, table: &str) -> [(String, String); 3] {
    OPERATIONS.map(|operation| {
        let name = format!("{table}_capture_{}", operation.to_ascii_lowercase());
        let sql = format!(
            "CREATE TRIGGER {name} AFTER {operation} ON {table} BEGIN \
             UPDATE {prefix}_capture_continuity SET epoch = epoch + 1, \
             token = lower(hex(randomblob(16))); END"
        );
        (name, sql)
    })
}

/// Whether `table` is one of `prefix`'s captured tables: events, blobs, identity or a projection.
fn captured(prefix: &str, table: &str) -> bool {
    ["events", "blobs", "identity"]
        .iter()
        .any(|suffix| table == format!("{prefix}_{suffix}"))
        || table
            .strip_prefix(&format!("{prefix}_p_"))
            .is_some_and(|name| validate_identifier("projection name", name).is_ok())
}

struct Trigger {
    name: String,
    table: String,
    sql: String,
}

fn main_triggers(connection: &Connection) -> rusqlite::Result<Vec<Trigger>> {
    let mut statement = connection.prepare(
        "SELECT name, tbl_name, sql FROM main.sqlite_schema WHERE type = 'trigger' ORDER BY name",
    )?;
    statement
        .query_map([], |row| {
            Ok(Trigger {
                name: row.get(0)?,
                table: row.get(1)?,
                sql: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            })
        })?
        .collect()
}

/// Whether `prefix`'s durable continuity installed `trigger`, by exact name and text.
fn owns(prefix: &str, trigger: &Trigger) -> bool {
    validate_prefix(prefix).is_ok()
        && captured(prefix, &trigger.table)
        && provider_triggers(prefix, &trigger.table)
            .iter()
            .any(|(name, sql)| *name == trigger.name && *sql == trigger.sql)
}

/// Whether some owner's durable continuity installed `trigger`: the owner its text names, checked
/// by regenerating that owner's trigger and comparing exactly. Such a trigger writes nothing but
/// its owner's continuity row, which no capture reads, so owners sharing one file stay apart.
fn any_owners(trigger: &Trigger) -> bool {
    trigger
        .sql
        .split_once(" BEGIN UPDATE ")
        .and_then(|(_, body)| body.split_once("_capture_continuity SET "))
        .is_some_and(|(prefix, _)| owns(prefix, trigger))
}

/// Whether `table` carries any trigger other than `prefix`'s provider triggers for it.
///
/// The blob table at open, a projection table at attach and capture, and strict inspection each
/// refused every trigger before durable continuity existed. They still refuse every other one.
/// SQLite resolves a trigger's target table without regard to case and records `tbl_name` as the
/// statement spelled it, so the target is matched without regard to case too.
pub(crate) fn foreign_triggers_on(
    connection: &Connection,
    prefix: &str,
    table: &str,
) -> rusqlite::Result<bool> {
    let expected = provider_triggers(prefix, table);
    let mut statement = connection.prepare(
        "SELECT name, sql FROM sqlite_master
         WHERE type = 'trigger' AND tbl_name = ?1 COLLATE NOCASE",
    )?;
    let found = statement.query_map([table], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    for row in found {
        let (name, sql) = row?;
        if !expected
            .iter()
            .any(|(held, text)| *held == name && Some(text) == sql.as_ref())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether anything in the schema can write captured rows outside the provider's own statements:
/// a trigger no owner's durable continuity installed, any temporary trigger, or a foreign key.
///
/// Such a schema stays capturable, but issues no continuity authority.
pub(crate) fn indirect_writers(connection: &Connection) -> rusqlite::Result<bool> {
    let structural: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM temp.sqlite_schema WHERE type='trigger')
         OR EXISTS(SELECT 1 FROM main.sqlite_schema AS s,
             pragma_foreign_key_list(s.name,'main') WHERE s.type='table')
         OR EXISTS(SELECT 1 FROM temp.sqlite_schema AS s,
             pragma_foreign_key_list(s.name,'temp') WHERE s.type='table')",
        [],
        |row| row.get(0),
    )?;
    Ok(structural || !main_triggers(connection)?.iter().all(any_owners))
}

/// The durable mutation epoch and the random token every write to captured material replaces.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureContinuityMark {
    epoch: u64,
    token: String,
}

impl CaptureContinuityMark {
    /// The same mark: equal epoch and equal token. An older copy of the file written as often as
    /// it lost repeats the epoch; the token makes that coincidence a 128-bit collision.
    fn same(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.token == other.token
    }
}

/// The durable values one checkpoint binds, all read inside one capture's transaction.
#[derive(Clone, Debug)]
pub(crate) struct DurableBinding {
    instance: String,
    prefix: String,
    schema_version: i64,
    mark: CaptureContinuityMark,
    position: u64,
}

fn hex32(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The store instance and the mark, when durable continuity is enabled and its one row is a row
/// the provider writes.
fn state(
    connection: &Connection,
    prefix: &str,
) -> Result<Option<(String, CaptureContinuityMark)>, EventLogError> {
    let table = continuity_table(prefix);
    if !has_table(connection, &table)? {
        return Ok(None);
    }
    let row: Option<(i64, String, i64, String)> = connection
        .query_row(
            &format!(
                "SELECT (SELECT count(*) FROM {table}), instance, epoch, token FROM {table}
                 WHERE typeof(instance) = 'text' AND typeof(epoch) = 'integer'
                   AND typeof(token) = 'text'
                 LIMIT 1"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(backend)?;
    let Some((1, instance, epoch, token)) = row else {
        return Ok(None);
    };
    let Ok(epoch) = u64::try_from(epoch) else {
        return Ok(None);
    };
    if !hex32(&instance) || !hex32(&token) {
        return Ok(None);
    }
    Ok(Some((instance, CaptureContinuityMark { epoch, token })))
}

/// The newest journal position, zero for an empty journal, `None` without one.
fn newest(connection: &Connection, prefix: &str) -> Result<Option<u64>, EventLogError> {
    let table = journal_table(prefix);
    if !has_table(connection, &table)? {
        return Ok(None);
    }
    let newest: i64 = connection
        .query_row(
            &format!("SELECT COALESCE(MAX(position), 0) FROM {table}"),
            [],
            |row| row.get(0),
        )
        .map_err(backend)?;
    Ok(u64::try_from(newest).ok())
}

/// Whether each table carries exactly the provider's three triggers for it, by name and text.
fn carry_provider_triggers(
    connection: &Connection,
    prefix: &str,
    tables: &[String],
) -> Result<bool, EventLogError> {
    let held = main_triggers(connection).map_err(backend)?;
    Ok(tables.iter().all(|table| {
        let mut found: Vec<(&str, &str)> = held
            .iter()
            .filter(|trigger| trigger.table.eq_ignore_ascii_case(table))
            .map(|trigger| (trigger.name.as_str(), trigger.sql.as_str()))
            .collect();
        found.sort_unstable();
        let expected = provider_triggers(prefix, table);
        let mut wanted: Vec<(&str, &str)> = expected
            .iter()
            .map(|(name, sql)| (name.as_str(), sql.as_str()))
            .collect();
        wanted.sort_unstable();
        found == wanted
    }))
}

/// Whether a stored table's declared body is exactly `body`.
fn shaped(connection: &Connection, table: &str, body: &str) -> Result<bool, EventLogError> {
    let sql: Option<Option<String>> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(backend)?;
    Ok(sql.flatten().is_some_and(|sql| {
        sqlite_table_body_tokens(&sql).as_deref() == Some(sqlite_schema_tokens(body).as_slice())
    }))
}

/// Install `prefix`'s provider triggers on `table` where they are missing.
fn install_triggers(
    connection: &Connection,
    prefix: &str,
    table: &str,
) -> Result<(), EventLogError> {
    for (name, sql) in provider_triggers(prefix, table) {
        let stored: Option<Option<String>> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1 COLLATE NOCASE",
                [&name],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        match stored {
            None => connection.execute_batch(&sql).map_err(backend)?,
            Some(Some(stored)) if stored == sql => {}
            Some(_) => {
                return Err(EventLogError::Invalid(format!(
                    "trigger {name} exists and is not the provider's"
                )));
            }
        }
    }
    Ok(())
}

/// Give a projection table `create_projections` just created the provider's triggers, in its
/// transaction, when the owner has durable continuity enabled.
pub(crate) fn cover_projection(
    connection: &Connection,
    prefix: &str,
    table: &str,
) -> Result<(), EventLogError> {
    if has_table(connection, &continuity_table(prefix))? {
        install_triggers(connection, prefix, table)?;
    }
    Ok(())
}

/// Delete every journal entry, inside a redaction's or an erasure's own transaction: entries hold
/// copies of projection row values.
pub(crate) fn clear_journal(connection: &Connection, prefix: &str) -> Result<(), EventLogError> {
    let journal = journal_table(prefix);
    if has_table(connection, &journal)? {
        connection
            .execute(&format!("DELETE FROM {journal}"), [])
            .map_err(backend)?;
    }
    Ok(())
}

/// The mark an acknowledged group starts from, read after `BEGIN IMMEDIATE` and before any
/// write. `None` when durable continuity is not enabled or its state is not the provider's:
/// the group then writes no entry, and continuity ends at it.
pub(crate) fn group_start(connection: &Connection, prefix: &str) -> Option<CaptureContinuityMark> {
    state(connection, prefix)
        .ok()
        .flatten()
        .map(|(_, mark)| mark)
}

/// Whether `tenant` has redacted history. Served by the partial index enable creates, so a group
/// pays for the redacted events of its tenant, not for its whole history.
pub(crate) fn redacted(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
) -> Result<bool, EventLogError> {
    connection
        .query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {prefix}_events
                 WHERE tenant_id = ?1 AND redacted_at IS NOT NULL)"
            ),
            params![tenant.as_str()],
            |row| row.get(0),
        )
        .map_err(backend)
}

/// The journal's copy of one stored row body: the exact text, never a re-serialized value.
fn stored_text(text: Option<String>) -> serde_json::Result<Option<Box<RawValue>>> {
    text.map(RawValue::from_string).transpose()
}

/// Record an acknowledged group's journal entry inside its transaction, then prune the journal.
///
/// A deduplicated group, a draft that missed something, and an entry over the byte bound write
/// nothing: the group's writes then move the mark without an entry, and the next restored
/// checkpoint gives a complete capture. A `withheld` group, whose tenant has redacted history,
/// writes an entry that links the chain and carries no event position, digest or row value.
pub(crate) fn journal_group(
    connection: &Connection,
    prefix: &str,
    from: CaptureContinuityMark,
    tenant: &TenantId,
    appended: &AppendGroupResult,
    draft: Option<Draft>,
    withheld: bool,
) -> Result<(), EventLogError> {
    let Some(draft) = draft else {
        return Ok(());
    };
    if appended.deduplicated || &draft.tenant != tenant || (draft.broken && !withheld) {
        return Ok(());
    }
    let Some((_, to)) = state(connection, prefix)? else {
        return Ok(());
    };
    let journal = journal_table(prefix);
    let position: i64 = connection
        .query_row(
            &format!("SELECT COALESCE(MAX(position), 0) + 1 FROM {journal}"),
            [],
            |row| row.get(0),
        )
        .map_err(backend)?;
    let mut entry = wire::DurableJournalEntry {
        position: to_u64(position)?,
        tenant_id: tenant.as_str().to_owned(),
        from,
        to,
        event_positions: Vec::new(),
        blobs: Vec::new(),
        rows: Vec::new(),
    };
    // A tenant with redacted history gets a link and nothing else: a row this group replaced may
    // hold a value derived from the redacted event, and the journal must not outlive it.
    if !withheld {
        entry.event_positions = appended
            .appends
            .iter()
            .flat_map(|append| &append.events)
            .map(|event| event.global_seq)
            .collect();
        entry.event_positions.sort_unstable();
        entry.blobs = draft.blobs.into_iter().collect();
        for (specification, row) in draft.rows.into_values() {
            let (Ok(before), Ok(after)) = (stored_text(row.before), stored_text(row.after)) else {
                return Ok(());
            };
            entry.rows.push(wire::JournaledRowChange {
                projection: wire::CapturedProjectionScope::of(&specification),
                key: row.key,
                before,
                after,
            });
        }
    }
    let text = serde_json::to_string(&entry).map_err(backend)?;
    let bytes = i64::try_from(text.len()).map_err(backend)?;
    if bytes > MAX_BYTES {
        return Ok(());
    }
    connection
        .execute(
            &format!(
                "INSERT INTO {journal}
                     (position, tenant_id, from_epoch, from_token, to_epoch, to_token, entry, bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
            ),
            params![
                position,
                tenant.as_str(),
                to_i64(entry.from.epoch)?,
                entry.from.token,
                to_i64(entry.to.epoch)?,
                entry.to.token,
                text,
                bytes
            ],
        )
        .map_err(backend)?;
    connection
        .execute(
            &format!(
                "DELETE FROM {journal} WHERE position IN (
                     SELECT position FROM (
                         SELECT position,
                                ROW_NUMBER() OVER (ORDER BY position DESC) AS place,
                                SUM(bytes) OVER (ORDER BY position DESC
                                                 ROWS UNBOUNDED PRECEDING) AS held
                         FROM {journal})
                     WHERE place > ?1 OR held > ?2)"
            ),
            params![MAX_ENTRIES, MAX_BYTES],
        )
        .map_err(backend)?;
    Ok(())
}

/// The SQLite encoding of `checkpoint`'s durable values, or `None` when it has none.
pub(crate) fn encode(checkpoint: &Checkpoint, prefix: &str) -> Option<Vec<u8>> {
    let binding = checkpoint.durable.as_ref()?;
    if binding.prefix != prefix {
        return None;
    }
    let scope = &checkpoint.scope;
    serde_json::to_vec(&wire::SqliteDurableCheckpoint {
        format: FORMAT.to_owned(),
        version: VERSION,
        store_instance: binding.instance.clone(),
        prefix: binding.prefix.clone(),
        scope: wire::CaptureCheckpointScope {
            capture: wire::CaptureScope {
                tenant_id: scope.tenant.as_str().to_owned(),
                stream_identity: scope.identity.clone(),
            },
            projections: scope
                .projections
                .iter()
                .map(|projection| wire::CapturedProjectionScope {
                    name: projection.name.clone(),
                    indexed_fields: projection.indexed.clone(),
                })
                .collect(),
            limits: scope.limits.into(),
        },
        usage: checkpoint.usage.into(),
        position: binding.position,
        schema_version: binding.schema_version,
        mark: binding.mark.clone(),
    })
    .ok()
}

/// A checkpoint restored from bytes `encode` wrote for this prefix, or `None`. `instance` is the
/// store instance the restoring handle last read, when it has read one.
pub(crate) fn decode(bytes: &[u8], prefix: &str, instance: Option<&str>) -> Option<Checkpoint> {
    let wire: wire::SqliteDurableCheckpoint = serde_json::from_slice(bytes).ok()?;
    if wire.format != FORMAT
        || wire.version != VERSION
        || wire.prefix != prefix
        || instance.is_some_and(|instance| wire.store_instance != instance)
        || !hex32(&wire.store_instance)
        || !hex32(&wire.mark.token)
    {
        return None;
    }
    let capture = wire.scope.capture;
    let tenant = TenantId::new(capture.tenant_id).ok()?;
    if capture.stream_identity.is_empty() {
        return None;
    }
    let mut names = BTreeSet::new();
    let mut projections = Vec::with_capacity(wire.scope.projections.len());
    for projection in wire.scope.projections {
        validate_identifier("projection name", &projection.name).ok()?;
        if !names.insert(projection.name.clone())
            || projection.indexed_fields.len() > MAX_INDEXED_FIELDS
        {
            return None;
        }
        let mut fields = BTreeSet::new();
        for field in &projection.indexed_fields {
            validate_identifier("indexed field", field).ok()?;
            if !fields.insert(field.as_str()) {
                return None;
            }
        }
        projections.push(ScopedProjection {
            name: projection.name,
            indexed: projection.indexed_fields,
        });
    }
    Some(Checkpoint::restored(
        Scope {
            tenant,
            identity: capture.stream_identity,
            projections,
            limits: wire.scope.limits.into(),
        },
        wire.usage.into(),
        DurableBinding {
            instance: wire.store_instance,
            prefix: wire.prefix,
            schema_version: wire.schema_version,
            mark: wire.mark,
            position: wire.position,
        },
    ))
}

/// Whether `usage` fits every cap of `limits`.
fn within(usage: CaptureUsage, limits: CaptureLimits) -> bool {
    usage.events <= limits.max_events
        && usage.blobs <= limits.max_blobs
        && usage.projection_rows <= limits.max_projection_rows
        && usage.payload_bytes <= limits.max_payload_bytes
}

/// How a restored checkpoint continues.
pub(crate) enum Continued {
    Unchanged,
    Delta(TenantCaptureDelta),
}

/// The journal entries after a checkpoint, walked as one chain of marks.
struct Walk {
    last: u64,
    end: CaptureContinuityMark,
    positions: Vec<u64>,
    blobs: BTreeSet<String>,
    rows: BTreeMap<(String, String), CapturedRowChange>,
}

type JournalRow = (i64, String, i64, String, i64, String, String);

/// Walk every entry after `then.position`: each must start at the mark the previous one ended
/// at, from `then.mark` on. Collect `tenant`'s events, blobs and requested projection rows;
/// other tenants' entries are links and contribute nothing. `None` where any link does not hold.
fn walk(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    then: &DurableBinding,
) -> Option<Walk> {
    let journal = journal_table(prefix);
    let entries: Vec<JournalRow> = {
        let mut statement = connection
            .prepare(&format!(
                "SELECT position, tenant_id, from_epoch, from_token, to_epoch, to_token, entry
                 FROM {journal} WHERE position > ?1 ORDER BY position"
            ))
            .ok()?;
        statement
            .query_map([i64::try_from(then.position).ok()?], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            })
            .ok()?
            .collect::<Result<_, _>>()
            .ok()?
    };
    let mut walk = Walk {
        last: then.position,
        end: then.mark.clone(),
        positions: Vec::new(),
        blobs: BTreeSet::new(),
        rows: BTreeMap::new(),
    };
    for (position, owner, from_epoch, from_token, to_epoch, to_token, text) in entries {
        let from = CaptureContinuityMark {
            epoch: u64::try_from(from_epoch).ok()?,
            token: from_token,
        };
        if !from.same(&walk.end) {
            return None;
        }
        walk.end = CaptureContinuityMark {
            epoch: u64::try_from(to_epoch).ok()?,
            token: to_token,
        };
        walk.last = u64::try_from(position).ok()?;
        if owner != tenant.as_str() {
            continue;
        }
        let entry: wire::DurableJournalEntry = serde_json::from_str(&text).ok()?;
        if entry.position != walk.last
            || entry.tenant_id != owner
            || !entry.from.same(&from)
            || !entry.to.same(&walk.end)
            || entry.event_positions.is_empty()
        {
            // Every acknowledged group appends an event: an entry of this tenant without one is
            // withheld after redaction, and nothing of this tenant continues past it.
            return None;
        }
        walk.positions.extend(entry.event_positions);
        walk.blobs.extend(entry.blobs);
        for change in entry.rows {
            // The value a complete capture reads: the stored text, parsed by its own parser.
            let parse = |text: Option<Box<RawValue>>| {
                text.map(|text| stored_json(text.get().as_bytes()))
                    .transpose()
            };
            let (Ok(before), Ok(after)) = (parse(change.before), parse(change.after)) else {
                return None;
            };
            let Some(specification) = projections
                .iter()
                .find(|requested| requested.name == change.projection.name)
            else {
                continue;
            };
            // Writes address the physical table by name: the same table under another
            // declaration is not an unrelated projection whose change may be left out.
            if !change.projection.is(specification) {
                return None;
            }
            match walk
                .rows
                .get_mut(&(change.projection.name.clone(), change.key.clone()))
            {
                Some(held) => {
                    if held.after != before {
                        return None;
                    }
                    held.after = after;
                }
                None => {
                    walk.rows.insert(
                        (change.projection.name, change.key.clone()),
                        CapturedRowChange {
                            key: change.key,
                            before,
                            after,
                        },
                    );
                }
            }
        }
    }
    Some(walk)
}

/// The events at `positions`, read back; `None` unless each is `tenant`'s, unredacted, valid and
/// later than the one before.
fn read_events(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    positions: &[u64],
) -> Result<Option<Vec<RecordedEvent>>, CaptureError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT {COLUMNS} FROM {prefix}_events WHERE global_seq = ?1"
        ))
        .map_err(operational)?;
    let mut events = Vec::with_capacity(positions.len());
    let mut previous = 0_u64;
    for position in positions {
        if *position <= previous {
            return Ok(None);
        }
        previous = *position;
        let row = statement
            .query_row([to_i64(*position)?], read_event)
            .optional()
            .map_err(operational)?;
        let Some(Ok(event)) = row else {
            return Ok(None);
        };
        if event.redacted_at.is_some() || validate_captured_event(tenant, &event).is_err() {
            return Ok(None);
        }
        events.push(event);
    }
    Ok(Some(events))
}

/// The bytes bound to each digest, through the complete-content validator; `None` unless every
/// one is bound and valid.
fn read_blobs(
    connection: &Connection,
    verified: &crate::verified::VerifiedBlobs,
    prefix: &str,
    tenant: &TenantId,
    digests: &BTreeSet<String>,
) -> Result<Option<Vec<CapturedBlob>>, CaptureError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT typeof(digest), bytes, byte_count, CAST(integrity_sha256 AS BLOB), integrity_v1
             FROM {prefix}_blobs WHERE tenant_id = ?1 AND digest = ?2"
        ))
        .map_err(operational)?;
    let mut blobs = Vec::with_capacity(digests.len());
    for digest in digests {
        if validate_captured_digest(digest).is_err() {
            return Ok(None);
        }
        let row = statement
            .query_row(params![tenant.as_str(), digest], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .optional()
            .map_err(operational)?;
        let Some((class, bytes, count, hash, edition)) = row else {
            return Ok(None);
        };
        let Ok(hash) = hash.map(String::from_utf8).transpose() else {
            return Ok(None);
        };
        if class != "text" {
            return Ok(None);
        }
        let Ok(bytes) = verified.check(connection, bytes, count, hash, edition) else {
            return Ok(None);
        };
        blobs.push(CapturedBlob {
            digest: digest.clone(),
            bytes,
        });
    }
    Ok(Some(blobs))
}

impl Inner {
    /// The durable values a checkpoint issued in this transaction binds: `None` unless durable
    /// continuity is enabled and every captured table of the request carries exactly the
    /// provider's triggers, so that no write to it can leave the mark where it was.
    pub(crate) fn durable_binding(
        &self,
        connection: &Connection,
        projections: &[ProjectionSpec],
        schema_version: i64,
    ) -> Result<Option<DurableBinding>, CaptureError> {
        let prefix = &self.prefix;
        let state = state(connection, prefix)?;
        self.tracked
            .note_instance(state.as_ref().map(|(instance, _)| instance.clone()));
        let Some((instance, mark)) = state else {
            return Ok(None);
        };
        let Some(position) = newest(connection, prefix)? else {
            return Ok(None);
        };
        let mut tables = vec![
            format!("{prefix}_events"),
            format!("{prefix}_blobs"),
            format!("{prefix}_identity"),
        ];
        tables.extend(
            projections
                .iter()
                .map(|spec| projection_table(prefix, spec.name)),
        );
        if !carry_provider_triggers(connection, prefix, &tables)? {
            return Ok(None);
        }
        Ok(Some(DurableBinding {
            instance,
            prefix: prefix.clone(),
            schema_version,
            mark,
            position,
        }))
    }

    /// Continue from `old`'s durable values to `now`, inside the capture's transaction.
    ///
    /// Unchanged when instance, schema version, mark and position all equal. An append delta when
    /// the entries after `old`'s position chain its mark to the current one. `None`, and so a
    /// complete capture, otherwise.
    pub(crate) fn continue_durably(
        &self,
        connection: &Connection,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
        old: &Checkpoint,
        now: &DurableBinding,
    ) -> Result<Option<Continued>, CaptureError> {
        let Some(then) = old.durable.as_ref() else {
            return Ok(None);
        };
        if then.prefix != now.prefix
            || then.instance != now.instance
            || then.schema_version != now.schema_version
        {
            return Ok(None);
        }
        if then.mark.same(&now.mark) && then.position == now.position {
            // The usage is decoded, not counted: one over the request's caps is no proof of a
            // crossed cap, so the complete capture decides instead.
            return Ok(within(old.usage, limits).then_some(Continued::Unchanged));
        }
        let Some(walk) = walk(connection, &self.prefix, tenant, projections, then) else {
            return Ok(None);
        };
        if walk.last == then.position || !walk.end.same(&now.mark) || walk.last != now.position {
            return Ok(None);
        }
        let Some(events) = read_events(connection, &self.prefix, tenant, &walk.positions)? else {
            return Ok(None);
        };
        let Some(blobs) = read_blobs(
            connection,
            &self.verified,
            &self.prefix,
            tenant,
            &walk.blobs,
        )?
        else {
            return Ok(None);
        };
        let mut delta = TenantCaptureDelta {
            tenant: tenant.clone(),
            stream_identity: old.scope.identity.clone(),
            events,
            blobs,
            projections: projections
                .iter()
                .map(|specification| CapturedProjectionDelta {
                    specification: *specification,
                    rows: walk
                        .rows
                        .iter()
                        .filter(|((name, _), _)| name == specification.name)
                        .map(|(_, row)| row.clone())
                        .collect(),
                })
                .collect(),
            resulting_usage: old.usage,
        };
        // The base usage is decoded from bytes, not counted. A cap it crosses, or a delta it cannot
        // be applied to, is no proof about the observation: the complete capture decides, and
        // reports a cap only when the real content crosses it.
        let Ok(usage) = advanced_usage(old.usage, &delta, limits) else {
            return Ok(None);
        };
        delta.resulting_usage = usage;
        Ok(Some(Continued::Delta(delta)))
    }

    /// Read the durable store instance into this handle, for `restore_checkpoint`.
    pub(crate) fn refresh_durable_instance(&self) {
        let instance = self
            .connection
            .lock()
            .ok()
            .and_then(|connection| state(&connection, &self.prefix).ok().flatten())
            .map(|(instance, _)| instance);
        self.tracked.note_instance(instance);
    }

    fn enable_durable_continuity(&self) -> Result<(), EventLogError> {
        let connection = self.connection.lock().map_err(poisoned)?;
        begin_immediate(&connection)?;
        let result = self.enable_in_transaction(&connection);
        let instance = finish_transaction(&connection, result)?;
        self.tracked.note_instance(Some(instance));
        Ok(())
    }

    fn enable_in_transaction(&self, connection: &Connection) -> Result<String, EventLogError> {
        let prefix = &self.prefix;
        let temporary: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM temp.sqlite_schema WHERE type = 'trigger')",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if temporary
            || !main_triggers(connection)
                .map_err(backend)?
                .iter()
                .all(any_owners)
        {
            return Err(EventLogError::Invalid(
                "durable capture continuity refuses a store carrying a trigger the provider does \
                 not own"
                    .into(),
            ));
        }
        let continuity = continuity_table(prefix);
        let journal = journal_table(prefix);
        match (
            has_table(connection, &continuity)?,
            has_table(connection, &journal)?,
        ) {
            (false, false) => connection
                .execute_batch(&format!(
                    "CREATE TABLE {continuity} ({CONTINUITY_BODY});
                     CREATE TABLE {journal} ({JOURNAL_BODY});
                     INSERT INTO {continuity} (instance, epoch, token)
                     VALUES (lower(hex(randomblob(16))), 0, lower(hex(randomblob(16))));"
                ))
                .map_err(backend)?,
            (true, true)
                if shaped(connection, &continuity, CONTINUITY_BODY)?
                    && shaped(connection, &journal, JOURNAL_BODY)? => {}
            _ => return Err(not_the_providers()),
        }
        for table in self.captured_tables(connection)? {
            install_triggers(connection, prefix, &table)?;
        }
        let (index, sql) = redacted_index(prefix);
        let stored: Option<Option<String>> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1 COLLATE NOCASE",
                [&index],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        match stored {
            None => connection.execute_batch(&sql).map_err(backend)?,
            Some(Some(stored)) if stored == sql => {}
            Some(_) => return Err(not_the_providers()),
        }
        let (instance, _) = state(connection, prefix)?.ok_or_else(not_the_providers)?;
        Ok(instance)
    }

    /// Events, blobs, identity and every registered projection whose table exists.
    fn captured_tables(&self, connection: &Connection) -> Result<Vec<String>, EventLogError> {
        let prefix = &self.prefix;
        let mut tables = vec![
            format!("{prefix}_events"),
            format!("{prefix}_blobs"),
            format!("{prefix}_identity"),
        ];
        let names: Vec<String> = {
            let mut statement = connection
                .prepare(&format!(
                    "SELECT projection_name FROM {prefix}_projection_registry
                     ORDER BY projection_name"
                ))
                .map_err(backend)?;
            statement
                .query_map([], |row| row.get(0))
                .map_err(backend)?
                .collect::<Result<_, _>>()
                .map_err(backend)?
        };
        for name in names {
            validate_identifier("registered projection", &name)?;
            let table = projection_table(prefix, &name);
            if has_table(connection, &table)? {
                tables.push(table);
            }
        }
        Ok(tables)
    }

    fn disable_durable_continuity(&self) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let connection = self.connection.lock().map_err(poisoned)?;
        begin_immediate(&connection)?;
        let result = (|| {
            for trigger in main_triggers(&connection).map_err(backend)? {
                if owns(prefix, &trigger) {
                    connection
                        .execute_batch(&format!("DROP TRIGGER {}", trigger.name))
                        .map_err(backend)?;
                }
            }
            connection
                .execute_batch(&format!(
                    "DROP INDEX IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};",
                    redacted_index(prefix).0,
                    journal_table(prefix),
                    continuity_table(prefix)
                ))
                .map_err(backend)
        })();
        finish_transaction(&connection, result)?;
        self.tracked.note_instance(None);
        Ok(())
    }
}

impl SqliteEventStore {
    /// Install the provider-owned change recording that lets a later process continue capture
    /// from a [`eventlog_core::DurableCaptureCheckpoint`], in one transaction.
    ///
    /// Creates `{prefix}_capture_continuity`, which mints this store's instance, the bounded
    /// `{prefix}_capture_journal`, and an `AFTER INSERT`, `AFTER UPDATE` and `AFTER DELETE`
    /// trigger on the events, blobs and identity tables and on every registered projection
    /// table, and a partial index of redacted events by tenant, which keeps the redacted-history
    /// check of every journaled group cheap. Idempotent: a second call on an enabled store
    /// changes nothing, the instance included. **Eventlog 0.7.0 and earlier refuse a store carrying these triggers**;
    /// [`Self::disable_durable_continuity`] makes it theirs again.
    ///
    /// # Errors
    /// Returns [`EventLogError::Invalid`] for a store carrying a trigger the provider does not
    /// own, or continuity tables the provider did not create, and [`EventLogError::Backend`] for
    /// an operational failure. Nothing is changed on any error.
    pub async fn enable_durable_continuity(&self) -> Result<(), EventLogError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.enable_durable_continuity()).await
    }

    /// Drop every provider trigger of this owner and both continuity tables, in one transaction,
    /// after which earlier Eventlog versions open and attach the store as before. Every durable
    /// checkpoint issued before then restores to nothing.
    ///
    /// # Errors
    /// Returns [`EventLogError::Backend`] for an operational failure; nothing is changed then.
    pub async fn disable_durable_continuity(&self) -> Result<(), EventLogError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.disable_durable_continuity()).await
    }
}

/// The JSON encodings, by the names of the ESS types they encode.
mod wire {
    use serde::{Deserialize, Deserializer, Serialize};
    use serde_json::value::RawValue;

    use super::CaptureContinuityMark;

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct CaptureScope {
        pub(super) tenant_id: String,
        pub(super) stream_identity: String,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct CapturedProjectionScope {
        pub(super) name: String,
        pub(super) indexed_fields: Vec<String>,
    }

    impl CapturedProjectionScope {
        pub(super) fn of(specification: &eventlog_core::ProjectionSpec) -> Self {
            Self {
                name: specification.name.to_owned(),
                indexed_fields: specification
                    .indexed
                    .iter()
                    .map(|field| (*field).to_owned())
                    .collect(),
            }
        }

        pub(super) fn is(&self, specification: &eventlog_core::ProjectionSpec) -> bool {
            self.name == specification.name
                && self
                    .indexed_fields
                    .iter()
                    .map(String::as_str)
                    .eq(specification.indexed.iter().copied())
        }
    }

    // The field names are the committed schema's property names, `max_` prefix included.
    #[allow(clippy::struct_field_names)]
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct CaptureLimits {
        max_events: u64,
        max_blobs: u64,
        max_projection_rows: u64,
        max_payload_bytes: u64,
    }

    impl From<eventlog_core::CaptureLimits> for CaptureLimits {
        fn from(limits: eventlog_core::CaptureLimits) -> Self {
            Self {
                max_events: limits.max_events,
                max_blobs: limits.max_blobs,
                max_projection_rows: limits.max_projection_rows,
                max_payload_bytes: limits.max_payload_bytes,
            }
        }
    }

    impl From<CaptureLimits> for eventlog_core::CaptureLimits {
        fn from(limits: CaptureLimits) -> Self {
            Self {
                max_events: limits.max_events,
                max_blobs: limits.max_blobs,
                max_projection_rows: limits.max_projection_rows,
                max_payload_bytes: limits.max_payload_bytes,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct CaptureUsage {
        events: u64,
        blobs: u64,
        projection_rows: u64,
        payload_bytes: u64,
    }

    impl From<eventlog_core::CaptureUsage> for CaptureUsage {
        fn from(usage: eventlog_core::CaptureUsage) -> Self {
            Self {
                events: usage.events,
                blobs: usage.blobs,
                projection_rows: usage.projection_rows,
                payload_bytes: usage.payload_bytes,
            }
        }
    }

    impl From<CaptureUsage> for eventlog_core::CaptureUsage {
        fn from(usage: CaptureUsage) -> Self {
            Self {
                events: usage.events,
                blobs: usage.blobs,
                projection_rows: usage.projection_rows,
                payload_bytes: usage.payload_bytes,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct CaptureCheckpointScope {
        pub(super) capture: CaptureScope,
        pub(super) projections: Vec<CapturedProjectionScope>,
        pub(super) limits: CaptureLimits,
    }

    /// Version 1 of the SQLite encoding of `DurableCaptureCheckpoint`.
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct SqliteDurableCheckpoint {
        pub(super) format: String,
        pub(super) version: u64,
        pub(super) store_instance: String,
        pub(super) prefix: String,
        pub(super) scope: CaptureCheckpointScope,
        pub(super) usage: CaptureUsage,
        pub(super) position: u64,
        pub(super) schema_version: i64,
        pub(super) mark: CaptureContinuityMark,
    }

    /// Absent is no row; a present JSON `null` is a row whose value is null. A present value is
    /// the exact text the row held, embedded as JSON: re-serializing a parsed value would hand a
    /// later reader a number the row never held.
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct JournaledRowChange {
        pub(super) projection: CapturedProjectionScope,
        pub(super) key: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "present"
        )]
        pub(super) before: Option<Box<RawValue>>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "present"
        )]
        pub(super) after: Option<Box<RawValue>>,
    }

    /// One acknowledged atomic group, written in the group's own transaction.
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct DurableJournalEntry {
        pub(super) position: u64,
        pub(super) tenant_id: String,
        pub(super) from: CaptureContinuityMark,
        pub(super) to: CaptureContinuityMark,
        pub(super) event_positions: Vec<u64>,
        pub(super) blobs: Vec<String>,
        pub(super) rows: Vec<JournaledRowChange>,
    }

    /// A member that is present, JSON `null` included, is `Some`: only absence is `None`.
    fn present<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Box<RawValue>>, D::Error> {
        Box::<RawValue>::deserialize(deserializer).map(Some)
    }
}
