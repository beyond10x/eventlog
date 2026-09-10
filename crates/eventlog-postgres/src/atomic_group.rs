//! Group locking precedes ordered semantic application in one PostgreSQL transaction.
use super::{
    AppendResult, Arc, BTreeSet, BoxFuture, COLUMNS, CommandMeta, EventLogError, Expected, Guard,
    NewEvent, NoGuard, OffsetDateTime, PostgresEventStore, PostgresProjections, Projector,
    StreamId, backend, check_expected, lock_identity, new_event_id, parse_uuid, poisoned,
    publication_gate, read_event, select_versions, to_i32, to_i64, to_u64,
};
use eventlog_core::{AppendGroup, AppendGroupResult, AtomicEventStore, GroupRange};

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
                prefix: &prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: Some((&self.admission_permit, stream.tenant())),
                reservation_pending: false,
            };
            admission.check(&mut projections).await?;
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
                prefix: &prefix,
                inline: &self.inline_names,
                tenant: stream.tenant(),
                admission: None,
                reservation_pending: false,
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
        admission: &dyn Guard,
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
        {
            let mut projections = PostgresProjections {
                client: transaction,
                prefix,
                inline: &self.inline_names,
                tenant: &group.tenant,
                admission: Some((&self.admission_permit, &group.tenant)),
                reservation_pending: false,
            };
            admission.check(&mut projections).await?;
            if projections.reservation_pending {
                return Err(EventLogError::Invalid(
                    "unfinished reservation poisons this append".into(),
                ));
            }
        }
        let mut appends = Vec::with_capacity(group.appends.len());
        let mut ranges = Vec::with_capacity(group.appends.len());
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
                )
                .await?;
            ranges.push(GroupRange {
                stream: entry.stream.clone(),
                first_version: result.first_version,
                last_version: result.last_version,
            });
            appends.push(result);
        }
        transaction.execute(&format!("INSERT INTO {prefix}_append_groups (tenant_id,idempotency_key,request_hash,ranges) VALUES ($1,$2,$3,$4)"),
            &[&group.tenant.as_str(),&group.meta.idempotency_key,&fingerprint,&serde_json::to_value(&ranges).map_err(|_| EventLogError::Invalid("invalid group ranges".into()))?]).await.map_err(backend)?;
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
                let result = self
                    .group_in_transaction(&transaction, group, &fingerprint, admission.as_ref())
                    .await;
                match result {
                    Ok(result) => {
                        transaction
                            .commit()
                            .await
                            .map_err(|_| EventLogError::UnknownCommit)?;
                        client.settled();
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
