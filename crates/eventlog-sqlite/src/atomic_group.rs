//! One BEGIN IMMEDIATE owns the complete group, including inline projectors and admission.
use super::{
    AppendResult, Arc, BoxFuture, Connection, EventLogError, Guard, Inner, NoGuard,
    SqliteEventStore, SqliteProjections, backend, begin_immediate, drive, finish_transaction,
    params, poisoned, run_blocking, select_versions, to_i64,
};
use eventlog_core::{
    AppendGroup, AppendGroupResult, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup,
    BlobWrite, GroupRange,
};
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
                &[],
                admission.as_ref(),
            );
            finish_transaction(&connection, result)
        }))
    }
}

impl AtomicBlobEventStore for SqliteEventStore {
    fn append_group_with_blobs_guarded<'a>(
        &'a self,
        request: &'a BlobAppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let mut request = request.clone();
        Box::pin(run_blocking(move || {
            let fingerprint = request.fingerprint()?;
            request.blobs.sort_by(|a, b| a.digest.cmp(&b.digest));
            *inner.registration.lock().map_err(poisoned)? = true;
            let mut connection = inner.connection.lock().map_err(poisoned)?;
            begin_immediate(&connection)?;
            let result = inner.group_in_transaction(
                &mut connection,
                &request.group,
                &fingerprint,
                &request.blobs,
                admission.as_ref(),
            );
            finish_blob_transaction(&connection, result)
        }))
    }
}

fn finish_blob_transaction<T>(
    connection: &Connection,
    result: Result<T, EventLogError>,
) -> Result<T, EventLogError> {
    match result {
        Ok(value) => {
            if connection.execute_batch("COMMIT").is_ok() {
                Ok(value)
            } else {
                // COMMIT may have taken effect before its response failed. Do not
                // compensate blobs; resolve the original durable command identity.
                let _ = connection.execute_batch("ROLLBACK");
                Err(EventLogError::UnknownCommit)
            }
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

impl Inner {
    fn group_in_transaction(
        &self,
        connection: &mut Connection,
        group: &AppendGroup,
        fingerprint: &str,
        blobs: &[BlobWrite],
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
        for blob in blobs {
            let prior: Option<Vec<u8>> = connection
                .query_row(
                    &format!("SELECT bytes FROM {prefix}_blobs WHERE tenant_id=?1 AND digest=?2"),
                    params![group.tenant.as_str(), blob.digest],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if let Some(bytes) = prior {
                if bytes != blob.bytes {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
            } else {
                let byte_count = u64::try_from(blob.bytes.len()).map_err(backend)?;
                connection.execute(
                    &format!("INSERT INTO {prefix}_blobs (tenant_id,digest,bytes,byte_count,recorded_at) VALUES (?1,?2,?3,?4,?5)"),
                    params![group.tenant.as_str(),blob.digest,blob.bytes,to_i64(byte_count)?,super::format_time(time::OffsetDateTime::now_utc())?],
                ).map_err(backend)?;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use eventlog_core::EventStore;

    #[tokio::test]
    async fn atomic_blob_native_commit_failure_is_unknown_and_retry_resolves() {
        let store = SqliteEventStore::in_memory("atomic_unknown").await.unwrap();
        let request = eventlog_conformance::atomic_blob_request("same-key", "one", "content");
        {
            let mut connection = store.inner.connection.lock().unwrap();
            connection.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);").unwrap();
            begin_immediate(&connection).unwrap();
            let staged = store
                .inner
                .group_in_transaction(
                    &mut connection,
                    &request.group,
                    &request.fingerprint().unwrap(),
                    &request.blobs,
                    &NoGuard,
                )
                .unwrap();
            assert!(!staged.deduplicated);
            connection
                .execute("INSERT INTO fault_child VALUES (1)", [])
                .unwrap();
            assert_eq!(
                finish_blob_transaction(&connection, Ok(staged)),
                Err(EventLogError::UnknownCommit)
            );
            for table in [
                "atomic_unknown_blobs",
                "atomic_unknown_events",
                "atomic_unknown_append_groups",
            ] {
                let count: i64 = connection
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 0);
            }
        }
        let resolved = store.append_group_with_blobs(&request).await.unwrap();
        assert!(!resolved.deduplicated);
        assert!(
            store
                .append_group_with_blobs(&request)
                .await
                .unwrap()
                .deduplicated
        );
        assert_eq!(
            store
                .get_blob(&request.group.tenant, "content")
                .await
                .unwrap(),
            Some(request.blobs[0].bytes.clone())
        );
    }
}
