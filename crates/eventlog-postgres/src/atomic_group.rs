//! Group locking precedes ordered semantic application in one PostgreSQL transaction.
use super::{
    AppendResult, Arc, BTreeSet, BoxFuture, COLUMNS, CommandMeta, EventLogError, Expected, Guard,
    NewEvent, NoGuard, OffsetDateTime, PostgresEventStore, PostgresProjections, Projector,
    StreamId, backend, check_expected, ensure_callback_integrity, lock_identity, new_event_id,
    parse_uuid, poisoned, publication_gate, read_event, select_versions, to_i32, to_i64, to_u64,
};
use eventlog_core::{
    AppendGroup, AppendGroupResult, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup,
    BlobWrite, GroupRange, blob_integrity_sha256, validate_stored_blob,
};

#[cfg(test)]
fn checkpoint(point: &str) {
    if std::env::var("EVENTLOG_POSTGRES_GROUP_CRASH_AT").as_deref() == Ok(point) {
        std::process::exit(73);
    }
}

impl PostgresEventStore {
    // Both ports share event writing, guards and projectors; only command bookkeeping differs.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn append_in_transaction(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        admission: &dyn Guard,
        record_command: bool,
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<AppendResult, EventLogError> {
        let prefix = self.prefix.clone();
        if record_command {
            if let Some(claim) = &meta.claim {
                lock_identity(
                    transaction,
                    &prefix,
                    "claim",
                    &[stream.tenant().as_str(), &claim.scope, &claim.key],
                )
                .await?;
                let prior=transaction.query_opt(&format!("SELECT request_digest,stream_type,stream_id,first_version,last_version FROM {prefix}_claims WHERE tenant_id=$1 AND scope=$2 AND claim_key=$3"), &[&stream.tenant().as_str(),&claim.scope,&claim.key]).await.map_err(backend)?;
                if let Some(row) = prior {
                    if row.get::<_, String>(0) != claim.digest {
                        return Err(EventLogError::IdempotencyMismatch {
                            key: claim.key.clone(),
                        });
                    }
                    let original = StreamId::new(
                        stream.tenant().clone(),
                        row.get::<_, String>(1),
                        row.get::<_, String>(2),
                    )?;
                    let first: i64 = row.get(3);
                    let last: i64 = row.get(4);
                    let events =
                        select_versions(transaction, &prefix, &original, first, last).await?;

                    return Ok(AppendResult {
                        first_version: to_u64(first)?,
                        last_version: to_u64(last)?,
                        events,
                        deduplicated: true,
                    });
                }
            }
            lock_identity(
                transaction,
                &prefix,
                "stream",
                &[
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                ],
            )
            .await?;

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
                    select_versions(transaction, &prefix, stream, first_version, last_version)
                        .await?;

                return Ok(AppendResult {
                    first_version: to_u64(first_version)?,
                    last_version: to_u64(last_version)?,
                    events: stored,
                    deduplicated: true,
                });
            }
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
                client: transaction,
                blob_prefix: &prefix,
                projection_prefix: &prefix,
                lock_prefix: &prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: Some((&self.admission_permit, stream.tenant())),
                reservation_pending: false,
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            let result = admission.check(&mut projections).await;
            ensure_callback_integrity(callback_failed)?;
            result?;
            if projections.reservation_pending {
                return Err(EventLogError::Invalid(
                    "unfinished reservation poisons this append".into(),
                ));
            }
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
                             RETURNING {COLUMNS}"
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
            #[cfg(test)]
            if !record_command && offset == 0 {
                checkpoint("group-event-1");
            }
            written.push(read_event(&row)?);
        }

        let first_version = head + 1;
        let last_version = head + u64::try_from(events.len()).unwrap_or(u64::MAX);
        if record_command {
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
        }
        let projectors: Vec<Arc<dyn Projector>> = self.inline.lock().map_err(poisoned)?.clone();
        for projector in &projectors {
            let mut projections = PostgresProjections {
                client: transaction,
                blob_prefix: &prefix,
                projection_prefix: &prefix,
                lock_prefix: &prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: None,
                reservation_pending: false,
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            for recorded in &written {
                let result = projector.apply(recorded, &mut projections).await;
                ensure_callback_integrity(callback_failed)?;
                result?;
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

        ensure_callback_integrity(callback_failed)?;
        Ok(AppendResult {
            first_version,
            last_version,
            events: written,
            deduplicated: false,
        })
    }

    async fn group_in_transaction(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        group: &AppendGroup,
        fingerprint: &str,
        blobs: &[BlobWrite],
        admission: &dyn Guard,
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<AppendGroupResult, EventLogError> {
        let prefix = &self.prefix;
        lock_identity(
            transaction,
            prefix,
            "append-group",
            &[group.tenant.as_str(), &group.meta.idempotency_key],
        )
        .await?;
        let prior = transaction.query_opt(
            &format!("SELECT request_hash,ranges FROM {prefix}_append_groups WHERE tenant_id=$1 AND idempotency_key=$2"),
            &[&group.tenant.as_str(),&group.meta.idempotency_key]).await.map_err(backend)?;
        if let Some(row) = prior {
            if row.get::<_, String>(0) != fingerprint {
                return Err(EventLogError::IdempotencyMismatch {
                    key: group.meta.idempotency_key.clone(),
                });
            }
            let ranges: Vec<GroupRange> = serde_json::from_value(row.get(1))
                .map_err(|_| EventLogError::Invalid("invalid stored group ranges".into()))?;
            let mut appends = Vec::with_capacity(ranges.len());
            for range in ranges {
                if range.stream.tenant() != &group.tenant {
                    return Err(EventLogError::Invalid(
                        "stored group crosses tenants".into(),
                    ));
                }
                let events = select_versions(
                    transaction,
                    prefix,
                    &range.stream,
                    to_i64(range.first_version)?,
                    to_i64(range.last_version)?,
                )
                .await?;
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
        let streams: BTreeSet<_> = group.appends.iter().map(|a| &a.stream).collect();
        for stream in streams {
            lock_identity(
                transaction,
                prefix,
                "stream",
                &[
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                ],
            )
            .await?;
        }
        for blob in blobs {
            // Row/unique locking also serializes the existing standalone blob ports.
            // Advisory locks alone cannot protect against writers that do not take them.
            let byte_count = to_i64(
                u64::try_from(blob.bytes.len())
                    .map_err(|_| EventLogError::Invalid("blob length exceeds u64".into()))?,
            )?;
            // The integrity columns are the same contract the standalone port writes; a binding
            // this path published without them would fail the table's own check.
            let integrity_sha256 = blob_integrity_sha256(&blob.bytes);
            transaction.execute(
                &format!("INSERT INTO {prefix}_blobs (tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1) VALUES ($1,$2,$3,$4,$5,$6,1) ON CONFLICT (tenant_id,digest) DO NOTHING"),
                &[&group.tenant.as_str(),&blob.digest,&blob.bytes,&byte_count,&OffsetDateTime::now_utc(),&integrity_sha256],
            ).await.map_err(backend)?;
            let row = transaction.query_opt(
                &format!("SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {prefix}_blobs WHERE tenant_id=$1 AND digest=$2 FOR UPDATE"),
                &[&group.tenant.as_str(),&blob.digest],
            ).await.map_err(backend)?;
            let stored = match row {
                Some(row) => validate_stored_blob(
                    row.get(0),
                    row.get(1),
                    row.get(2),
                    i64::from(row.get::<_, i32>(3)),
                )?,
                None => {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
            };
            if stored != blob.bytes {
                return Err(EventLogError::Invalid(
                    "blob digest already names different content".into(),
                ));
            }
        }
        {
            let mut projections = PostgresProjections {
                client: transaction,
                blob_prefix: prefix,
                projection_prefix: prefix,
                lock_prefix: prefix,
                inline: &self.inline_names,
                tenant: &group.tenant,
                admission: Some((&self.admission_permit, &group.tenant)),
                reservation_pending: false,
                callback_failed: Arc::clone(callback_failed),
                selected: None,
            };
            let result = admission.check(&mut projections).await;
            ensure_callback_integrity(callback_failed)?;
            result?;
            if projections.reservation_pending {
                return Err(EventLogError::Invalid(
                    "unfinished reservation poisons this append".into(),
                ));
            }
        }
        let mut appends = Vec::with_capacity(group.appends.len());
        let mut ranges = Vec::with_capacity(group.appends.len());
        #[cfg(test)]
        let mut member = 0;
        for entry in &group.appends {
            let result = self
                .append_in_transaction(
                    transaction,
                    &entry.stream,
                    entry.expected,
                    &entry.events,
                    &group.meta,
                    &NoGuard,
                    false,
                    callback_failed,
                )
                .await?;
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
        transaction.execute(&format!("INSERT INTO {prefix}_append_groups (tenant_id,idempotency_key,request_hash,ranges) VALUES ($1,$2,$3,$4)"),
            &[&group.tenant.as_str(),&group.meta.idempotency_key,&fingerprint,&serde_json::to_value(&ranges).map_err(|_| EventLogError::Invalid("invalid group ranges".into()))?]).await.map_err(backend)?;
        #[cfg(test)]
        checkpoint("group-bookkeeping");
        ensure_callback_integrity(callback_failed)?;
        Ok(AppendGroupResult {
            appends,
            deduplicated: false,
        })
    }
}

impl AtomicEventStore for PostgresEventStore {
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            let fingerprint = group.fingerprint()?;
            self.freeze().await;
            tokio::time::timeout(self.pool.options.transaction_timeout, async {
                let mut client = self.pool.acquire().await?;
                client.quarantine();
                let transaction = client.transaction().await.map_err(backend)?;
                publication_gate(&transaction, &self.prefix, false).await?;
                let callback_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let result = self
                    .group_in_transaction(
                        &transaction,
                        group,
                        &fingerprint,
                        &[],
                        admission.as_ref(),
                        &callback_failed,
                    )
                    .await;
                match result {
                    Ok(result) => {
                        #[cfg(test)]
                        checkpoint("group-precommit");
                        transaction
                            .commit()
                            .await
                            .map_err(|_| EventLogError::UnknownCommit)?;
                        client.settled();
                        #[cfg(test)]
                        checkpoint("group-postcommit");
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
}

impl AtomicBlobEventStore for PostgresEventStore {
    fn append_group_with_blobs_guarded<'a>(
        &'a self,
        request: &'a BlobAppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            let fingerprint = request.fingerprint()?;
            let mut request = request.clone();
            request.blobs.sort_by(|a, b| a.digest.cmp(&b.digest));
            self.freeze().await;
            tokio::time::timeout(self.pool.options.transaction_timeout, async {
                let mut client = self.pool.acquire().await?;
                client.quarantine();
                let transaction = client.transaction().await.map_err(backend)?;
                publication_gate(&transaction, &self.prefix, false).await?;
                let callback_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let result = self
                    .group_in_transaction(
                        &transaction,
                        &request.group,
                        &fingerprint,
                        &request.blobs,
                        admission.as_ref(),
                        &callback_failed,
                    )
                    .await;
                match result {
                    Ok(result) => {
                        #[cfg(test)]
                        checkpoint("group-precommit");
                        transaction
                            .commit()
                            .await
                            .map_err(|_| EventLogError::UnknownCommit)?;
                        client.settled();
                        #[cfg(test)]
                        checkpoint("group-postcommit");
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
}

#[cfg(test)]
mod native_group_crash {
    use super::*;
    use eventlog_conformance::{TALLY, Tally, event, meta};
    use eventlog_core::{EventStore, StreamAppend, TenantId};
    use std::process::Command;

    fn tenant() -> TenantId {
        TenantId::new("native-crash").unwrap()
    }

    /// Read the feed until it has delivered `expected` events, or give up saying so.
    ///
    /// The commit watermark is cluster-wide: a feed reader stops before
    /// `pg_snapshot_xmin(pg_current_snapshot())`, and a transaction open anywhere in the same
    /// instance — including a sibling case in this same lane — holds that back. Nothing is
    /// skipped and nothing is lost; the events are committed and simply not yet visible to a
    /// feed. Asking once is therefore asking whether this suite happened to be idle, which is a
    /// question about the machine and not about the group. `eventlog_conformance::drain_at_least`
    /// applies the same bounded retry to the catch-up runner for this reason, and the reason is in
    /// `AGENTS.md` under *The watermark couples feed latency across owners*.
    async fn feed_at_least(
        store: &PostgresEventStore,
        expected: usize,
    ) -> Vec<eventlog_core::RecordedEvent> {
        for _ in 0..200 {
            let events = store.read_feed(&tenant(), 0, 10).await.unwrap().events;
            if events.len() >= expected {
                return events;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("the feed never delivered {expected} events");
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
        let Ok(prefix) = std::env::var("EVENTLOG_POSTGRES_GROUP_CHILD_PREFIX") else {
            return;
        };
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").unwrap();
        let store = PostgresEventStore::connect(&url, &prefix).await.unwrap();
        store.register_inline(Arc::new(Tally)).await.unwrap();
        store.append_group(&group()).await.unwrap();
    }

    #[tokio::test]
    async fn every_native_group_boundary_recovers_one_complete_outcome() {
        let url =
            std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned real PostgreSQL URL");
        let run: String = OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .to_string()
            .bytes()
            .map(|digit| char::from(b'a' + digit - b'0'))
            .collect();
        for (index, (point, committed)) in [
            ("group-event-1", false),
            ("group-member-1", false),
            ("group-member-2", false),
            ("group-member-3", false),
            ("group-bookkeeping", false),
            ("group-precommit", false),
            ("group-postcommit", true),
        ]
        .into_iter()
        .enumerate()
        {
            let prefix = format!(
                "ng_{run}_{}",
                char::from(b'a' + u8::try_from(index).unwrap())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "atomic_group::native_group_crash::child",
                    "--nocapture",
                ])
                .env("EVENTLOG_POSTGRES_GROUP_CHILD_PREFIX", &prefix)
                .env("EVENTLOG_POSTGRES_GROUP_CRASH_AT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let store = PostgresEventStore::connect(&url, &prefix).await.unwrap();
            let before = feed_at_least(&store, usize::from(committed) * 3).await;
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
                assert_eq!(received[0].version, 1, "{point}: fresh retry");
            }
            assert_eq!(
                feed_at_least(&store, received.len()).await,
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
            store.shutdown().await.unwrap();
            let reopened = PostgresEventStore::connect(&url, &prefix).await.unwrap();
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
            reopened.drop_tables().await.unwrap();
            reopened.shutdown().await.unwrap();
        }
    }
}
