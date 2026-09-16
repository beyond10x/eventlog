//! One tenant's complete observation, inside one `BEGIN IMMEDIATE` transaction.
//!
//! SQLite has one writer. Taking the write transaction before the first stored read is what makes
//! this an observation rather than a sequence of guesses: it waits behind a writer that started
//! earlier and excludes every writer that starts later, across separately opened handles and
//! including the autocommit identity and blob statements that take no transaction of their own.
//! A deferred reader that fixes its snapshot first would still be reading a tenant an erasure is
//! in the middle of removing.

use std::sync::Arc;

use eventlog_core::{
    BoxFuture, CaptureBudget, CaptureError, CaptureLimits, CaptureMaterial, CaptureResource,
    CapturedBlob, CapturedProjection, ConsistentTenantCapture, EventLogError,
    ProjectionCaptureRefusal, ProjectionSpec, RecordedEvent, TenantCapture, TenantId, order_blobs,
    order_rows, validate_capture_request, validate_captured_digest, validate_captured_order,
    validate_stored_blob,
};
use rusqlite::{Connection, OptionalExtension as _, Row, params};
use serde_json::Value;

use crate::{
    COLUMNS, Inner, SqliteEventStore, backend, begin_immediate, poisoned, projection_index,
    projection_table, projection_table_body, read_event, sqlite_schema_tokens,
    sqlite_table_body_tokens, to_i64, to_u64,
};

/// How many rows one bounded read retains at a time inside the capture transaction.
///
/// The caps bound what a capture returns, not what a driver materializes on the way there, so the
/// reads are chunked and each item is checked against the accumulating payload before it is kept.
const CHUNK: i64 = 1_000;

fn operational(error: rusqlite::Error) -> CaptureError {
    CaptureError::Store(backend(error))
}

fn corrupt(material: CaptureMaterial) -> CaptureError {
    CaptureError::Corrupt { material }
}

impl ConsistentTenantCapture for SqliteEventStore {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>> {
        let inner = Arc::clone(&self.inner);
        let tenant = tenant.clone();
        let requested = projections.to_vec();
        Box::pin(async move {
            validate_capture_request(&tenant, &requested)?;
            match tokio::task::spawn_blocking(move || {
                inner.capture_tenant(&tenant, &requested, limits)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(CaptureError::Store(EventLogError::Backend(
                    "the store's blocking worker panicked".to_owned(),
                ))),
            }
        })
    }
}

impl Inner {
    fn capture_tenant(
        &self,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Result<TenantCapture, CaptureError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|error| CaptureError::Store(poisoned(error)))?;
        let connection = &mut *connection;
        begin_immediate(connection)?;
        let result = self.observe(connection, tenant, projections, limits);
        // There is no DML and no DDL in this transaction; it ends only to release the writer.
        match result {
            Ok(value) => match connection.execute_batch("COMMIT") {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = connection.execute_batch("ROLLBACK");
                    Err(operational(error))
                }
            },
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn observe(
        &self,
        connection: &Connection,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Result<TenantCapture, CaptureError> {
        let prefix = &self.prefix;
        let identity = stored_identity(connection, prefix, tenant)?;
        let redacted: bool = connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {prefix}_events
                     WHERE tenant_id = ?1 AND redacted_at IS NOT NULL)"
                ),
                params![tenant.as_str()],
                |row| row.get(0),
            )
            .map_err(operational)?;
        if redacted {
            // Redaction leaves bound blobs and materialized rows exactly where they were, and no
            // checksum here proves those rows are the erased history rather than a copy of it.
            return Err(CaptureError::RedactedHistory);
        }

        let mut admitted = Vec::with_capacity(projections.len());
        for specification in projections {
            admit_projection(connection, prefix, specification)?;
            admitted.push(*specification);
        }

        let mut budget = CaptureBudget::new(limits);
        preflight(connection, prefix, tenant, &admitted, &budget)?;
        let events = read_events(connection, prefix, tenant, &mut budget)?;
        validate_captured_order(tenant, &events)?;
        let blobs = read_blobs(connection, prefix, tenant, &mut budget)?;
        let mut captured = Vec::with_capacity(admitted.len());
        for specification in admitted {
            let rows = read_rows(connection, prefix, tenant, specification, &mut budget)?;
            captured.push(CapturedProjection {
                specification,
                rows,
            });
        }
        Ok(TenantCapture {
            tenant: tenant.clone(),
            stream_identity: identity,
            events,
            blobs,
            projections: captured,
        })
    }
}

/// Read the identity the store already holds. This never mints one.
///
/// `stream_identity` would: it inserts on absence, in every provider. A capture that called it
/// would answer "this tenant exists" by making it exist.
fn stored_identity(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
) -> Result<String, CaptureError> {
    let stored: Option<(String, Vec<u8>)> = connection
        .query_row(
            &format!(
                "SELECT typeof(stream_identity), CAST(stream_identity AS BLOB)
                 FROM {prefix}_identity WHERE tenant_id = ?1"
            ),
            params![tenant.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(operational)?;
    let Some((kind, bytes)) = stored else {
        return Err(CaptureError::TenantIdentityMissing);
    };
    if kind != "text" {
        return Err(corrupt(CaptureMaterial::Identity));
    }
    // Valid exactly when it decodes as a nonempty string, and preserved byte for byte: a legacy
    // value that is not a UUID is somebody's real identity, not something to replace.
    let identity = String::from_utf8(bytes).map_err(|_| corrupt(CaptureMaterial::Identity))?;
    if identity.is_empty() {
        return Err(corrupt(CaptureMaterial::Identity));
    }
    Ok(identity)
}

/// Refuse a cap the stored counts and lengths already prove crossed, before decoding anything.
fn preflight(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    admitted: &[ProjectionSpec],
    budget: &CaptureBudget,
) -> Result<(), CaptureError> {
    let events: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM {prefix}_events WHERE tenant_id = ?1"),
            params![tenant.as_str()],
            |row| row.get(0),
        )
        .map_err(operational)?;
    budget.proven(CaptureResource::Events, to_u64(events)?)?;
    // The stored length is what a payload cap would be proven from, so it is checked against the
    // bytes first — a column is not evidence about content it merely claims to describe, and a
    // length that is a lie would otherwise report a cap the tenant's content never crossed.
    // SQLite measures the bytes inside this same transaction: nothing is decoded here, and an
    // oversized blob is never loaded, let alone allocated, to find out how long it is.
    let (blobs, bytes, malformed): (i64, i64, i64) = connection
        .query_row(
            &format!(
                "SELECT COUNT(*),
                        COALESCE(SUM(length(CAST(bytes AS BLOB))),0),
                        COALESCE(SUM(CASE WHEN byte_count IS NOT length(CAST(bytes AS BLOB))
                                          THEN 1 ELSE 0 END),0)
                 FROM {prefix}_blobs WHERE tenant_id = ?1"
            ),
            params![tenant.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(operational)?;
    budget.proven(CaptureResource::Blobs, to_u64(blobs)?)?;
    if malformed != 0 {
        return Err(corrupt(CaptureMaterial::Blob));
    }
    // Now a real lower bound on the payload: the bytes this tenant actually holds. Each blob is
    // still validated and charged one at a time as it is decoded; this only refuses early.
    budget.proven(
        CaptureResource::PayloadBytes,
        u64::try_from(bytes).map_err(|_| corrupt(CaptureMaterial::Blob))?,
    )?;
    let mut rows = 0_u64;
    for specification in admitted {
        let table = projection_table(prefix, specification.name);
        let count: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = ?1"),
                params![tenant.as_str()],
                |row| row.get(0),
            )
            .map_err(operational)?;
        rows = rows
            .checked_add(to_u64(count)?)
            .ok_or_else(|| corrupt(CaptureMaterial::Projection))?;
    }
    budget.proven(CaptureResource::ProjectionRows, rows)
}

fn read_events(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    budget: &mut CaptureBudget,
) -> Result<Vec<RecordedEvent>, CaptureError> {
    let mut events = Vec::new();
    let mut after = 0_i64;
    loop {
        let page = {
            let mut statement = connection
                .prepare(&format!(
                    "SELECT {COLUMNS} FROM {prefix}_events
                     WHERE tenant_id = ?1 AND global_seq > ?2
                     ORDER BY global_seq LIMIT ?3"
                ))
                .map_err(operational)?;
            let rows = statement
                .query_map(params![tenant.as_str(), after, CHUNK], read_event)
                .map_err(operational)?;
            let mut page = Vec::new();
            for row in rows {
                page.push(
                    row.map_err(operational)?
                        .map_err(|_| corrupt(CaptureMaterial::Event))?,
                );
            }
            page
        };
        if page.is_empty() {
            return Ok(events);
        }
        for event in page {
            budget.admit_event(&event)?;
            after = to_i64(event.global_seq)?;
            events.push(event);
        }
    }
}

fn read_blobs(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    budget: &mut CaptureBudget,
) -> Result<Vec<CapturedBlob>, CaptureError> {
    let columns = "typeof(digest), CAST(digest AS BLOB), bytes, byte_count, \
                   CAST(integrity_sha256 AS BLOB), integrity_v1";
    let read = |row: &Row<'_>| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<Vec<u8>>>(4)?,
            row.get::<_, i64>(5)?,
        ))
    };
    let owner = tenant.as_str();
    let chunk = CHUNK;
    let mut blobs = Vec::new();
    let mut after: Option<String> = None;
    loop {
        // The first page carries no key predicate, so a digest that sorts before every other one
        // is read rather than silently filtered out of the binding set.
        let page = {
            let (statement, bound): (String, Vec<&dyn rusqlite::ToSql>) = match &after {
                None => (
                    format!(
                        "SELECT {columns} FROM {prefix}_blobs WHERE tenant_id = ?1
                         ORDER BY digest LIMIT ?2"
                    ),
                    vec![&owner, &chunk],
                ),
                Some(after) => (
                    format!(
                        "SELECT {columns} FROM {prefix}_blobs
                         WHERE tenant_id = ?1 AND digest > ?2 ORDER BY digest LIMIT ?3"
                    ),
                    vec![&owner, after, &chunk],
                ),
            };
            let mut prepared = connection.prepare(&statement).map_err(operational)?;
            let rows = prepared
                .query_map(bound.as_slice(), read)
                .map_err(operational)?;
            let mut page = Vec::new();
            for row in rows {
                page.push(row.map_err(operational)?);
            }
            page
        };
        if page.is_empty() {
            order_blobs(&mut blobs)?;
            return Ok(blobs);
        }
        for (class, digest, bytes, count, hash, edition) in page {
            // A resume point is a value *and* a storage class. SQLite orders classes before
            // values, so a BLOB digest outranks every text digest and every text bound as the
            // resume point: it would be re-selected on every page, `after` would never pass it,
            // and the caller would be handed a cap this tenant's bindings never crossed. No writer
            // here can store one — `put_blob` binds a Rust `&str` — so it is corruption, refused
            // before it can become a resume bound.
            if class != "text" {
                return Err(corrupt(CaptureMaterial::Blob));
            }
            let digest = String::from_utf8(digest).map_err(|_| corrupt(CaptureMaterial::Blob))?;
            validate_captured_digest(&digest)?;
            let hash = hash
                .map(String::from_utf8)
                .transpose()
                .map_err(|_| corrupt(CaptureMaterial::Blob))?;
            // The accepted SQL complete-content validator, not a checksum column read back raw.
            let bytes = validate_stored_blob(bytes, count, hash, edition)
                .map_err(|_| corrupt(CaptureMaterial::Blob))?;
            budget.admit_blob(bytes.len() as u64)?;
            after = Some(digest.clone());
            blobs.push(CapturedBlob { digest, bytes });
        }
    }
}

fn read_rows(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    specification: ProjectionSpec,
    budget: &mut CaptureBudget,
) -> Result<Vec<(String, Value)>, CaptureError> {
    let table = projection_table(prefix, specification.name);
    let read = |row: &Row<'_>| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    };
    let owner = tenant.as_str();
    let chunk = CHUNK;
    let mut rows: Vec<(String, Value)> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        // An empty key is a key a writer here can produce, so the first page has no predicate.
        let page = {
            let (statement, bound): (String, Vec<&dyn rusqlite::ToSql>) = match &after {
                None => (
                    format!(
                        "SELECT typeof(row_key), CAST(row_key AS BLOB), CAST(body AS BLOB)
                         FROM {table}
                         WHERE tenant_id = ?1 ORDER BY row_key LIMIT ?2"
                    ),
                    vec![&owner, &chunk],
                ),
                Some(after) => (
                    format!(
                        "SELECT typeof(row_key), CAST(row_key AS BLOB), CAST(body AS BLOB)
                         FROM {table}
                         WHERE tenant_id = ?1 AND row_key > ?2 ORDER BY row_key LIMIT ?3"
                    ),
                    vec![&owner, after, &chunk],
                ),
            };
            let mut prepared = connection.prepare(&statement).map_err(operational)?;
            let fetched = prepared
                .query_map(bound.as_slice(), read)
                .map_err(operational)?;
            let mut page = Vec::new();
            for row in fetched {
                page.push(row.map_err(operational)?);
            }
            page
        };
        if page.is_empty() {
            order_rows(&mut rows)?;
            return Ok(rows);
        }
        for (class, key, body) in page {
            // The same reason as the digest above: a BLOB key outranks every text key and every
            // text resume bound, so the page would never advance past it. `ProjectionStore::upsert`
            // takes `&str`, so no row this kit wrote can be here.
            if class != "text" {
                return Err(corrupt(CaptureMaterial::Projection));
            }
            // Every byte a writer here admitted comes back: no grammar, no normalization.
            let key = String::from_utf8(key).map_err(|_| corrupt(CaptureMaterial::Projection))?;
            let body: Value =
                serde_json::from_slice(&body).map_err(|_| corrupt(CaptureMaterial::Projection))?;
            budget.admit_projection_row(&body)?;
            after = Some(key.clone());
            rows.push((key, body));
        }
    }
}

/// Admit one requested projection against the registry *and* the actual stored schema.
///
/// A name in the registry is a declaration, not a table. `CREATE TABLE IF NOT EXISTS` does nothing
/// when a table of that name already exists, whatever shape it has, so the registry alone would
/// admit somebody else's table under this kit's name. Nothing here creates an expected temporary
/// table: a read-only capture that writes to prove it is read-only has proved the opposite.
fn admit_projection(
    connection: &Connection,
    prefix: &str,
    specification: &ProjectionSpec,
) -> Result<(), CaptureError> {
    let refuse = |reason| CaptureError::ProjectionUnavailable {
        projection: specification.name.to_owned(),
        reason,
    };
    let declared: Option<String> = connection
        .query_row(
            &format!(
                "SELECT indexed_fields FROM {prefix}_projection_registry
                 WHERE projection_name = ?1"
            ),
            params![specification.name],
            |row| row.get(0),
        )
        .optional()
        .map_err(operational)?;
    let Some(declared) = declared else {
        return Err(refuse(ProjectionCaptureRefusal::Undeclared));
    };
    let expected = serde_json::to_string(specification.indexed)
        .map_err(|_| refuse(ProjectionCaptureRefusal::DeclarationMismatch))?;
    if declared != expected {
        return Err(refuse(ProjectionCaptureRefusal::DeclarationMismatch));
    }

    let table = projection_table(prefix, specification.name);
    let relation: Option<(String, Option<String>)> = connection
        .query_row(
            "SELECT type,sql FROM sqlite_master WHERE name=?1 AND type IN ('table','view')",
            params![table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(operational)?;
    let Some((kind, Some(sql))) = relation else {
        return Err(refuse(ProjectionCaptureRefusal::PhysicalShapeMismatch));
    };
    let triggers: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE tbl_name=?1 AND type='trigger'",
            params![table],
            |row| row.get(0),
        )
        .map_err(operational)?;
    if kind != "table" || triggers != 0 || sql.contains("WITHOUT ROWID") || sql.contains("STRICT") {
        return Err(refuse(ProjectionCaptureRefusal::PhysicalShapeMismatch));
    }
    let body = projection_table_body(specification.indexed.len());
    if sqlite_table_body_tokens(&sql).as_deref() != Some(sqlite_schema_tokens(&body).as_slice()) {
        return Err(refuse(ProjectionCaptureRefusal::PhysicalShapeMismatch));
    }
    let foreign: Vec<i64> = {
        let mut statement = connection
            .prepare(&format!("PRAGMA foreign_key_list({table})"))
            .map_err(operational)?;
        let rows = statement
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(operational)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(operational)?
    };
    if !foreign.is_empty() {
        return Err(refuse(ProjectionCaptureRefusal::PhysicalShapeMismatch));
    }
    admit_indexes(connection, &table, specification)
}

fn admit_indexes(
    connection: &Connection,
    table: &str,
    specification: &ProjectionSpec,
) -> Result<(), CaptureError> {
    let refuse = || CaptureError::ProjectionUnavailable {
        projection: specification.name.to_owned(),
        reason: ProjectionCaptureRefusal::PhysicalShapeMismatch,
    };
    let indexes: Vec<(String, bool, String, bool)> = {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_list({table})"))
            .map_err(operational)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })
            .map_err(operational)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(operational)?
    };
    if indexes.len() != specification.indexed.len() + 1 {
        return Err(refuse());
    }
    let mut primary = 0_usize;
    for (name, unique, origin, partial) in &indexes {
        if *partial {
            return Err(refuse());
        }
        let columns = index_columns(connection, name)?;
        // The key's collation decides which spellings of a key are one row, and no column pragma
        // reports it. The index does.
        if index_collations(connection, name)?
            .iter()
            .flatten()
            .any(|collation| !collation.eq_ignore_ascii_case("BINARY"))
        {
            return Err(refuse());
        }
        match origin.as_str() {
            "pk" => {
                primary += 1;
                if !*unique || columns != ["tenant_id", "row_key"] {
                    return Err(refuse());
                }
            }
            "c" => {
                let position = (0..specification.indexed.len())
                    .find(|position| &projection_index(table, *position) == name)
                    .ok_or_else(refuse)?;
                let expected = [String::from("tenant_id"), format!("idx_{position}")];
                if *unique || columns != expected {
                    return Err(refuse());
                }
            }
            _ => return Err(refuse()),
        }
    }
    if primary != 1 {
        return Err(refuse());
    }
    Ok(())
}

fn index_columns(connection: &Connection, index: &str) -> Result<Vec<String>, CaptureError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA index_info({index})"))
        .map_err(operational)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(2))
        .map_err(operational)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(operational)
}

fn index_collations(
    connection: &Connection,
    index: &str,
) -> Result<Vec<Option<String>>, CaptureError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA index_xinfo({index})"))
        .map_err(operational)?;
    let rows = statement
        .query_map([], |row| row.get::<_, Option<String>>(4))
        .map_err(operational)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(operational)
}
