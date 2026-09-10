//! One BEGIN IMMEDIATE owns the complete group, including inline projectors and admission.
use super::{
    AppendResult, Arc, BoxFuture, Connection, EventLogError, Guard, Inner, NoGuard,
    SqliteEventStore, SqliteProjections, backend, begin_immediate, drive, finish_transaction,
    params, poisoned, run_blocking, select_versions, to_i64,
};
use eventlog_core::{AppendGroup, AppendGroupResult, AtomicEventStore, GroupRange};
use rusqlite::OptionalExtension as _;

pub(super) fn ddl(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_append_groups (
        tenant_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL,
        ranges TEXT NOT NULL, PRIMARY KEY(tenant_id,idempotency_key));"
    )
}

impl AtomicEventStore for SqliteEventStore {
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let group = group.clone();
        Box::pin(run_blocking(move || {
            let fingerprint = group.fingerprint()?;
            *inner.registration.lock().map_err(poisoned)? = true;
            let mut connection = inner.connection.lock().map_err(poisoned)?;
            begin_immediate(&connection)?;
            let result = inner.group_in_transaction(
                &mut connection,
                &group,
                &fingerprint,
                admission.as_ref(),
            );
            finish_transaction(&connection, result)
        }))
    }
}

impl Inner {
    fn group_in_transaction(
        &self,
        connection: &mut Connection,
        group: &AppendGroup,
        fingerprint: &str,
        admission: &dyn Guard,
    ) -> Result<AppendGroupResult, EventLogError> {
        let prefix = &self.prefix;
        let prior: Option<(String,String)> = connection.query_row(
            &format!("SELECT request_hash,ranges FROM {prefix}_append_groups WHERE tenant_id=?1 AND idempotency_key=?2"),
            params![group.tenant.as_str(),group.meta.idempotency_key],
            |row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(backend)?;
        if let Some((digest, ranges)) = prior {
            if digest != fingerprint {
                return Err(EventLogError::IdempotencyMismatch {
                    key: group.meta.idempotency_key.clone(),
                });
            }
            let ranges: Vec<GroupRange> = serde_json::from_str(&ranges).map_err(backend)?;
            let mut appends = Vec::with_capacity(ranges.len());
            for range in ranges {
                if range.stream.tenant() != &group.tenant {
                    return Err(EventLogError::Invalid(
                        "stored group crosses tenants".into(),
                    ));
                }
                let events = select_versions(
                    connection,
                    prefix,
                    &range.stream,
                    to_i64(range.first_version)?,
                    to_i64(range.last_version)?,
                )?;
                appends.push(AppendResult {
                    first_version: range.first_version,
                    last_version: range.last_version,
                    events,
                    deduplicated: true,
                });
            }
            return Ok(AppendGroupResult {
                appends,
                deduplicated: true,
            });
        }
        {
            let mut projections = SqliteProjections {
                connection: &mut *connection,
                prefix,
                inline: &self.inline_names,
                tenant: &group.tenant,
                admission: Some((&self.admission_permit, &group.tenant)),
            };
            drive(admission.check(&mut projections))?;
        }
        let mut appends = Vec::with_capacity(group.appends.len());
        let mut ranges = Vec::with_capacity(group.appends.len());
        for entry in &group.appends {
            let result = self.append_in_transaction(
                connection,
                &entry.stream,
                entry.expected,
                &entry.events,
                &group.meta,
                &NoGuard,
                false,
            )?;
            ranges.push(GroupRange {
                stream: entry.stream.clone(),
                first_version: result.first_version,
                last_version: result.last_version,
            });
            appends.push(result);
        }
        connection.execute(&format!("INSERT INTO {prefix}_append_groups (tenant_id,idempotency_key,request_hash,ranges) VALUES (?1,?2,?3,?4)"),
            params![group.tenant.as_str(),group.meta.idempotency_key,fingerprint,serde_json::to_string(&ranges).map_err(backend)?]).map_err(backend)?;
        Ok(AppendGroupResult {
            appends,
            deduplicated: false,
        })
    }
}
