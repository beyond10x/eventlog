//! One BEGIN IMMEDIATE owns the complete group, including inline projectors and admission.
use super::{
    AppendResult, Arc, BoxFuture, Connection, EventLogError, Guard, Inner, NoGuard,
    SqliteEventStore, SqliteProjections, backend, begin_immediate, drive,
    ensure_callback_integrity, finish_transaction, params, poisoned, run_blocking, select_versions,
    to_i64,
};
use eventlog_core::{AppendGroup, AppendGroupResult, AtomicEventStore, GroupRange};
use rusqlite::OptionalExtension as _;

#[cfg(test)]
pub(super) fn checkpoint(point: &str) {
    if std::env::var("EVENTLOG_SQLITE_GROUP_CRASH_AT").as_deref() == Ok(point) {
        std::process::exit(73);
    }
}

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
            let callback_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let result = inner.group_in_transaction(
                &mut connection,
                &group,
                &fingerprint,
                admission.as_ref(),
                &callback_failed,
            );
            #[cfg(test)]
            if result.is_ok() {
                checkpoint("group-precommit");
            }
            let result = finish_transaction(&connection, result);
            #[cfg(test)]
            if result.is_ok() {
                checkpoint("group-postcommit");
            }
            result
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
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
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
                blob_prefix: prefix,
                projection_prefix: prefix,
                inline: &self.inline_names,
                tenant: &group.tenant,
                admission: Some((&self.admission_permit, &group.tenant)),
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            let result = drive(admission.check(&mut projections));
            ensure_callback_integrity(callback_failed)?;
            result?;
        }
        let mut appends = Vec::with_capacity(group.appends.len());
        let mut ranges = Vec::with_capacity(group.appends.len());
        #[cfg(test)]
        let mut member = 0;
        for entry in &group.appends {
            let result = self.append_in_transaction(
                connection,
                &entry.stream,
                entry.expected,
                &entry.events,
                &group.meta,
                &NoGuard,
                false,
                callback_failed,
            )?;
            #[cfg(test)]
            {
                member += 1;
                checkpoint(&format!("group-member-{member}"));
            }
            ranges.push(GroupRange {
                stream: entry.stream.clone(),
                first_version: result.first_version,
                last_version: result.last_version,
            });
            appends.push(result);
        }
        connection.execute(&format!("INSERT INTO {prefix}_append_groups (tenant_id,idempotency_key,request_hash,ranges) VALUES (?1,?2,?3,?4)"),
            params![group.tenant.as_str(),group.meta.idempotency_key,fingerprint,serde_json::to_string(&ranges).map_err(backend)?]).map_err(backend)?;
        #[cfg(test)]
        checkpoint("group-bookkeeping");
        ensure_callback_integrity(callback_failed)?;
        Ok(AppendGroupResult {
            appends,
            deduplicated: false,
        })
    }
}

#[cfg(test)]
mod native_group_crash {
    use super::*;
    use eventlog_conformance::{TALLY, Tally, event, meta};
    use eventlog_core::{EventStore, Expected, StreamAppend, StreamId, TenantId};
    use std::{process::Command, sync::Arc};

    fn tenant() -> TenantId {
        TenantId::new("native-crash").unwrap()
    }

    fn stream(id: &str) -> StreamId {
        StreamId::new(tenant(), "item", id).unwrap()
    }

    fn group() -> AppendGroup {
        AppendGroup {
            tenant: tenant(),
            meta: meta("native-group", &serde_json::json!({})),
            appends: ["a", "z", "a"]
                .into_iter()
                .map(|id| StreamAppend {
                    stream: stream(id),
                    expected: Expected::Any,
                    events: vec![event("item.changed", 1)],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn child() {
        let Ok(path) = std::env::var("EVENTLOG_SQLITE_GROUP_CHILD_PATH") else {
            return;
        };
        let store = SqliteEventStore::open(&path, "native_group").await.unwrap();
        store.register_inline(Arc::new(Tally)).await.unwrap();
        store.append_group(&group()).await.unwrap();
    }

    #[tokio::test]
    async fn every_native_group_boundary_recovers_one_complete_outcome() {
        for (point, committed) in [
            ("group-event-1", false),
            ("group-member-1", false),
            ("group-member-2", false),
            ("group-member-3", false),
            ("group-bookkeeping", false),
            ("group-precommit", false),
            ("group-postcommit", true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("native.sqlite");
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "atomic_group::native_group_crash::child",
                    "--nocapture",
                ])
                .env("EVENTLOG_SQLITE_GROUP_CHILD_PATH", &path)
                .env("EVENTLOG_SQLITE_GROUP_CRASH_AT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let store = SqliteEventStore::open(path.to_str().unwrap(), "native_group")
                .await
                .unwrap();
            let before = store.read_feed(&tenant(), 0, 10).await.unwrap().events;
            assert_eq!(before.len(), if committed { 3 } else { 0 }, "{point}");
            for (id, count) in [("a", 2), ("z", 1)] {
                assert_eq!(
                    store.stream_version(&stream(id)).await.unwrap(),
                    committed.then_some(count),
                    "{point}"
                );
                let row = store
                    .projection_get(&TALLY, &tenant(), &format!("item/{id}"))
                    .await
                    .unwrap();
                assert_eq!(
                    row.and_then(|value| value["count"].as_u64()),
                    committed.then_some(count),
                    "{point}"
                );
            }

            store.register_inline(Arc::new(Tally)).await.unwrap();
            let retry = store
                .append_group(&group())
                .await
                .unwrap_or_else(|error| panic!("{point}: retry refused after reopen: {error}"));
            assert_eq!(retry.deduplicated, committed, "{point}");
            assert_eq!(retry.appends.len(), 3, "{point}");
            assert_eq!(
                retry
                    .appends
                    .iter()
                    .map(|append| (append.first_version, append.last_version))
                    .collect::<Vec<_>>(),
                [(1, 1), (1, 1), (2, 2)],
                "{point}: original repeated-stream ranges"
            );
            let received: Vec<_> = retry
                .appends
                .iter()
                .flat_map(|append| append.events.iter().cloned())
                .collect();
            assert_eq!(received.len(), 3, "{point}");
            if committed {
                assert_eq!(received, before, "{point}: original event coordinates");
            } else {
                assert_eq!(received[0].global_seq, 1, "{point}: fresh retry");
            }
            assert_eq!(
                store.read_feed(&tenant(), 0, 10).await.unwrap().events,
                received,
                "{point}: no duplicate or missing group member"
            );
            for (id, count) in [("a", 2), ("z", 1)] {
                let row = store
                    .projection_get(&TALLY, &tenant(), &format!("item/{id}"))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(row["count"].as_u64(), Some(count), "{point}");
            }
            drop(store);
            let reopened = SqliteEventStore::open(path.to_str().unwrap(), "native_group")
                .await
                .unwrap();
            let second_retry = reopened.append_group(&group()).await.unwrap();
            assert!(
                second_retry.deduplicated,
                "{point}: retry after recovery reopen"
            );
            assert_eq!(second_retry.appends.len(), retry.appends.len(), "{point}");
            for (original, again) in retry.appends.iter().zip(&second_retry.appends) {
                assert_eq!(again.first_version, original.first_version, "{point}");
                assert_eq!(again.last_version, original.last_version, "{point}");
                assert_eq!(again.events, original.events, "{point}");
                assert!(again.deduplicated, "{point}");
            }
        }
    }
}
