//! Which owner a physical projection table belongs to, for every owner in one database file.
//!
//! A projection table is named `<prefix>_p_<projection>`, and both parts may contain `_p_`.
//! Owner `a` with projection `p_b` and owner `a_p` with projection `b` therefore name the same
//! table, `a_p_p_b`. `CREATE TABLE IF NOT EXISTS` reuses whatever table has that name, and each
//! owner's own registry records only its own projection names, so before this roster the second
//! owner silently read, overwrote and erased the first owner's rows.
//!
//! The roster is one table for the whole file. Registration claims each table name for exactly one
//! (owner prefix, projection) pair inside the registering transaction, before the table is created
//! or reused, and refuses a name that is claimed, or can belong, to another owner. The table is
//! additive DDL: no existing table or column changes, and existing names keep their spelling.
//!
//! An unclaimed name is checked against what the file already holds, because two owners may have
//! collided before this roster existed:
//!
//! - an owner whose prefix is shorter and whose persisted registry maps a projection to this name;
//! - an owner whose prefix is longer and whose namespace holds this name: either the name is in
//!   that owner's projection space (`<other>_p_...`), or a table of that name already exists (that
//!   owner's own tables, or a projection table it made).
//!
//! An owner is recognised by its `<prefix>_events` or `<prefix>_projection_registry` table.
use eventlog_core::EventLogError;
use rusqlite::{Connection, OptionalExtension, params};

use crate::{backend, has_table, projection_table, validate_prefix};

/// The database-wide roster. Its name contains no `_p_` and ends in no owner table suffix, so no
/// owner prefix or projection name can produce it.
pub(crate) const TABLE_OWNERS: &str = "eventlog_table_owners";

/// Claim `<prefix>_p_<projection>` for this owner, or refuse with the owner that has it.
///
/// Runs inside the caller's write transaction, so a refusal rolls the whole registration back.
pub(crate) fn claim_projection(
    connection: &Connection,
    prefix: &str,
    projection: &str,
) -> Result<(), EventLogError> {
    let table = projection_table(prefix, projection);
    connection
        .execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {TABLE_OWNERS} (
                 table_name TEXT NOT NULL PRIMARY KEY,
                 owner_prefix TEXT NOT NULL,
                 projection_name TEXT NOT NULL
             );"
        ))
        .map_err(backend)?;
    let claimed: Option<(String, String)> = connection
        .query_row(
            &format!(
                "SELECT owner_prefix, projection_name FROM {TABLE_OWNERS} WHERE table_name = ?1"
            ),
            params![table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(backend)?;
    match claimed {
        Some((owner, name)) if owner == prefix && name == projection => return Ok(()),
        Some((owner, name)) => {
            return Err(collision(&table, &owner, Some(&name)));
        }
        None => {}
    }
    if let Some((owner, name)) = existing_owner(connection, prefix, &table)? {
        return Err(collision(&table, &owner, name.as_deref()));
    }
    connection
        .execute(
            &format!(
                "INSERT INTO {TABLE_OWNERS} (table_name, owner_prefix, projection_name)
                 VALUES (?1, ?2, ?3)"
            ),
            params![table, prefix, projection],
        )
        .map_err(backend)?;
    Ok(())
}

fn collision(table: &str, owner: &str, projection: Option<&str>) -> EventLogError {
    let what = projection.map_or_else(
        || format!("the namespace of owner {owner}"),
        |projection| format!("owner {owner}'s projection {projection}"),
    );
    EventLogError::Invalid(format!(
        "projection table {table} would be shared with {what}; choose another owner prefix or \
         projection name"
    ))
}

/// Another owner that the file already says this name belongs to.
fn existing_owner(
    connection: &Connection,
    prefix: &str,
    table: &str,
) -> Result<Option<(String, Option<String>)>, EventLogError> {
    for (boundary, _) in table.match_indices('_') {
        let other = &table[..boundary];
        if other == prefix || validate_prefix(other).is_err() || !is_owner(connection, other)? {
            continue;
        }
        let rest = &table[boundary + 1..];
        if other.len() < prefix.len() {
            // A shorter owner can only name this table as one of its projections.
            let Some(name) = rest.strip_prefix("p_") else {
                continue;
            };
            if registers(connection, other, name)? {
                return Ok(Some((other.to_owned(), Some(name.to_owned()))));
            }
        } else if let Some(name) = rest.strip_prefix("p_") {
            let registered = registers(connection, other, name)?;
            return Ok(Some((
                other.to_owned(),
                registered.then(|| name.to_owned()),
            )));
        } else if has_table(connection, table)? {
            return Ok(Some((other.to_owned(), None)));
        }
    }
    Ok(None)
}

fn is_owner(connection: &Connection, prefix: &str) -> Result<bool, EventLogError> {
    Ok(has_table(connection, &format!("{prefix}_events"))?
        || has_table(connection, &format!("{prefix}_projection_registry"))?)
}

fn registers(
    connection: &Connection,
    prefix: &str,
    projection: &str,
) -> Result<bool, EventLogError> {
    let registry = format!("{prefix}_projection_registry");
    if !has_table(connection, &registry)? {
        return Ok(false);
    }
    connection
        .query_row(
            &format!("SELECT 1 FROM {registry} WHERE projection_name = ?1"),
            params![projection],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(backend)
}
