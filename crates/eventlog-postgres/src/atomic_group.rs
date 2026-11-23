//! Group locking precedes ordered semantic application in one PostgreSQL transaction.
//!
//! Both ports write events in batched statements: a group's stream locks, stream heads and event
//! rows each take one statement per [`STATEMENT_BATCH`] items, never one per member or event.
use std::collections::{BTreeMap, HashMap};

use super::{
    ADVISORY_KEY, AppendResult, Arc, BTreeSet, BoxFuture, COLUMNS, CommandMeta, EventLogError,
    Expected, Guard, NewEvent, OffsetDateTime, PostgresEventStore, PostgresProjections, Projector,
    RecordedEvent, STATEMENT_BATCH, StreamId, Window, backend, check_expected,
    ensure_callback_integrity, lock_coordinates, lock_identity, new_event_id, ordinal, parse_uuid,
    poisoned, publication_gate, read_event, select_versions, select_windows, set_statement_timeout,
    timed_out_write, to_i32, to_i64, to_u64,
};
use eventlog_core::{
    AppendGroup, AppendGroupResult, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup,
    GroupRange,
};

#[cfg(test)]
fn checkpoint(point: &str) {
    if std::env::var("EVENTLOG_POSTGRES_GROUP_CRASH_AT").as_deref() == Ok(point) {
        std::process::exit(73);
    }
}

/// One blob a group binds, with the SHA-256 of its content computed once for the whole request.
pub(crate) struct GroupBlob<'a> {
    digest: &'a str,
    bytes: &'a [u8],
    integrity_sha256: &'a str,
}

impl<'a> GroupBlob<'a> {
    /// The blobs in digest order, each with its hash from `hashes`, borrowing the caller's bytes.
    fn sorted(
        blobs: impl IntoIterator<Item = (&'a str, &'a [u8])>,
        hashes: &'a BTreeMap<String, String>,
    ) -> Result<Vec<Self>, EventLogError> {
        let mut sorted = blobs
            .into_iter()
            .map(|(digest, bytes)| {
                let integrity_sha256 = hashes.get(digest).ok_or_else(|| {
                    EventLogError::Invalid("blob content hash was not computed".into())
                })?;
                Ok(Self {
                    digest,
                    bytes,
                    integrity_sha256,
                })
            })
            .collect::<Result<Vec<_>, EventLogError>>()?;
        sorted.sort_by(|a, b| a.digest.cmp(b.digest));
        Ok(sorted)
    }
}

/// The blobs a group binds, and how they stand to its admission and its receipt.
#[derive(Clone, Copy)]
enum GroupBlobs<'a> {
    /// `AtomicBlobEventStore`: bound before admission, under a receipt whose fingerprint binds
    /// their bytes, so a retry runs no admission. Empty for a plain group.
    Bound(&'a [GroupBlob<'a>]),
    /// `AtomicEventStore::append_group_guarded_with_blobs`: admitted before a byte is bound,
    /// under the plain group receipt, with the sorted digests recorded beside it. A retry is
    /// admitted again and then answered against that record. `blob_fingerprint` is the receipt
    /// `AtomicBlobEventStore` would have recorded for the same group and batch.
    Guarded {
        blobs: &'a [GroupBlob<'a>],
        blob_fingerprint: &'a str,
    },
}

impl<'a> GroupBlobs<'a> {
    fn blobs(self) -> &'a [GroupBlob<'a>] {
        match self {
            Self::Bound(blobs) | Self::Guarded { blobs, .. } => blobs,
        }
    }

    fn is_empty(self) -> bool {
        self.blobs().is_empty()
    }
}

/// One event with every coordinate fixed before the statement that writes it.
struct Planned<'a> {
    stream: &'a StreamId,
    version: i64,
    event_id: uuid::Uuid,
    event: &'a NewEvent,
    schema_version: i32,
    recorded_at: OffsetDateTime,
}

/// Plan `events` at versions `head + 1 ..`, refusing what one insert per event refused.
fn plan<'a>(
    stream: &'a StreamId,
    head: u64,
    events: &'a [NewEvent],
    recorded_at: OffsetDateTime,
) -> Result<Vec<Planned<'a>>, EventLogError> {
    let mut planned = Vec::with_capacity(events.len());
    for (offset, event) in events.iter().enumerate() {
        let version = head + 1 + u64::try_from(offset).unwrap_or(u64::MAX);
        planned.push(Planned {
            stream,
            version: to_i64(version)?,
            event_id: parse_uuid(&new_event_id())?,
            event,
            schema_version: to_i32(event.schema_version)?,
            recorded_at,
        });
    }
    Ok(planned)
}

/// Insert planned events, [`STATEMENT_BATCH`] rows per statement, and return what was stored in
/// the planned order.
///
/// The rows leave the ordered subquery one at a time, so `global_seq` is drawn in planned order,
/// as it was with one statement per event. Each returned row is placed by its event id.
async fn insert_events(
    transaction: &tokio_postgres::Transaction<'_>,
    prefix: &str,
    meta: &CommandMeta,
    planned: &[Planned<'_>],
) -> Result<Vec<RecordedEvent>, EventLogError> {
    if planned.is_empty() {
        return Ok(Vec::new());
    }
    let causation_depth = to_i32(meta.causation_depth)?;
    let statement = transaction
        .prepare(&format!(
            "INSERT INTO {prefix}_events (
                 tenant_id, stream_type, stream_id, version, event_id, event_name,
                 event_schema_version, occurred_at, recorded_at, subject, actor, request_id,
                 trace_id, causation_id, causation_depth, data)
             SELECT r_tenant, r_type, r_id, r_version, r_event_id, r_name, r_schema,
                    $10::timestamptz, r_recorded, $11::text, $12::text, $13::text, $14::text,
                    $15::text, $16::int4, r_data
             FROM (SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[],
                                        $5::uuid[], $6::text[], $7::int4[],
                                        $8::timestamptz[], $9::jsonb[])
                       WITH ORDINALITY AS u(r_tenant, r_type, r_id, r_version, r_event_id,
                                            r_name, r_schema, r_recorded, r_data, ord)
                   ORDER BY ord) AS r
             RETURNING {COLUMNS}"
        ))
        .await
        .map_err(backend)?;
    let mut written = Vec::with_capacity(planned.len());
    for chunk in planned.chunks(STATEMENT_BATCH) {
        let tenants: Vec<&str> = chunk.iter().map(|p| p.stream.tenant().as_str()).collect();
        let types: Vec<&str> = chunk.iter().map(|p| p.stream.stream_type()).collect();
        let ids: Vec<&str> = chunk.iter().map(|p| p.stream.stream_id()).collect();
        let versions: Vec<i64> = chunk.iter().map(|p| p.version).collect();
        let event_ids: Vec<uuid::Uuid> = chunk.iter().map(|p| p.event_id).collect();
        let names: Vec<&str> = chunk.iter().map(|p| p.event.name.as_str()).collect();
        let schemas: Vec<i32> = chunk.iter().map(|p| p.schema_version).collect();
        let recorded: Vec<OffsetDateTime> = chunk.iter().map(|p| p.recorded_at).collect();
        let data: Vec<&serde_json::Value> = chunk.iter().map(|p| &p.event.data).collect();
        let rows = transaction
            .query(
                &statement,
                &[
                    &tenants,
                    &types,
                    &ids,
                    &versions,
                    &event_ids,
                    &names,
                    &schemas,
                    &recorded,
                    &data,
                    &meta.occurred_at,
                    &meta.subject,
                    &meta.actor,
                    &meta.request_id,
                    &meta.trace_id,
                    &meta.causation_id,
                    &causation_depth,
                ],
            )
            .await
            .map_err(backend)?;
        let positions: HashMap<uuid::Uuid, usize> = event_ids
            .iter()
            .enumerate()
            .map(|(position, id)| (*id, position))
            .collect();
        let mut slots: Vec<Option<RecordedEvent>> = chunk.iter().map(|_| None).collect();
        for row in &rows {
            let id: uuid::Uuid = row.get(5);
            let slot = positions
                .get(&id)
                .and_then(|position| slots.get_mut(*position))
                .ok_or_else(|| {
                    EventLogError::Backend("batched insert returned a stranger".into())
                })?;
            *slot = Some(read_event(row)?);
        }
        for slot in slots {
            written.push(
                slot.ok_or_else(|| EventLogError::Backend("batched insert lost a row".into()))?,
            );
        }
    }
    Ok(written)
}

/// Take every stream's append lock, in the set's order, [`STATEMENT_BATCH`] per statement.
///
/// The coordinates are [`lock_identity`]'s and the key expression is [`ADVISORY_KEY`] itself, so
/// these are the very locks a single append takes; the ordered subquery keeps the acquisition
/// order that prevents two groups from deadlocking.
///
/// One statement waits for up to 1,000 locks in turn, so `statement_timeout` would add up waits
/// that one statement per lock bounded each on its own. Each lock statement may therefore run
/// for the rest of the group's `deadline`; `lock_timeout` still bounds every single wait. The
/// session's `statement_timeout` is restored for the statements after it.
async fn lock_streams(
    transaction: &tokio_postgres::Transaction<'_>,
    prefix: &str,
    streams: &BTreeSet<&StreamId>,
    deadline: tokio::time::Instant,
    statement_timeout: std::time::Duration,
) -> Result<(), EventLogError> {
    let identities = streams
        .iter()
        .map(|stream| {
            lock_coordinates(
                prefix,
                "stream",
                &[
                    stream.tenant().as_str(),
                    stream.stream_type(),
                    stream.stream_id(),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key = ADVISORY_KEY.replace("$1", "r.identity");
    let statement = transaction
        .prepare(&format!(
            "SELECT pg_advisory_xact_lock({key})
             FROM (SELECT identity FROM unnest($1::text[]) WITH ORDINALITY AS u(identity, ord)
                   ORDER BY ord) AS r"
        ))
        .await
        .map_err(backend)?;
    for chunk in identities.chunks(STATEMENT_BATCH) {
        set_statement_timeout(
            transaction,
            deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await?;
        transaction
            .execute(&statement, &[&chunk])
            .await
            .map_err(backend)?;
    }
    set_statement_timeout(transaction, statement_timeout).await
}

/// The coordinate of one projector's lock for one tenant: the very lock catch-up, catch-up
/// rebuild and inline rebuild take (`[prefix, "projector", name, tenant]`).
fn projector_lock_identity(
    prefix: &str,
    name: &str,
    tenant: &eventlog_core::TenantId,
) -> Result<String, EventLogError> {
    serde_json::to_string(&[prefix, "projector", name, tenant.as_str()])
        .map_err(|_| EventLogError::Invalid("invalid lock coordinates".into()))
}

/// Take every inline projector's lock for `tenant`, ordered by name, in one statement, held to
/// the end of the transaction.
///
/// [`Projector::apply`] promises that one projection is never folded concurrently. Appends to
/// distinct streams share the publication gate and hold different stream locks, so without this
/// two of them — through one handle or several — would run one projector side by side, and a
/// get/compute/upsert of a shared row would lose an update. The lock lasts until commit because
/// a later call must also see the earlier call's writes.
///
/// Order: after the publication gate, the claim or group lock and every stream lock, and before
/// the guard and the first projector callback, whose admission and projection-row locks come
/// after it. Every writer takes the locks in that order and by name, so a write driving several
/// projectors cannot deadlock against another. A projector is folded by every append of its
/// owner, so writes of one tenant serialize from here to commit while an inline projector is
/// attached; other tenants are not affected.
async fn lock_projectors(
    transaction: &tokio_postgres::Transaction<'_>,
    prefix: &str,
    projectors: &[Arc<dyn Projector>],
    tenant: &eventlog_core::TenantId,
) -> Result<(), EventLogError> {
    if projectors.is_empty() {
        return Ok(());
    }
    let mut names: Vec<&str> = projectors
        .iter()
        .map(|projector| projector.name())
        .collect();
    names.sort_unstable();
    names.dedup();
    let identities = names
        .iter()
        .map(|name| projector_lock_identity(prefix, name, tenant))
        .collect::<Result<Vec<_>, _>>()?;
    let key = ADVISORY_KEY.replace("$1", "r.identity");
    transaction
        .execute(
            &format!(
                "SELECT pg_advisory_xact_lock({key})
                 FROM (SELECT identity FROM unnest($1::text[]) WITH ORDINALITY AS u(identity, ord)
                       ORDER BY ord) AS r"
            ),
            &[&identities],
        )
        .await
        .map_err(backend)?;
    Ok(())
}

/// Every stream's current head, 0 for a stream not yet written, [`STATEMENT_BATCH`] per statement.
async fn stream_heads<'s>(
    transaction: &tokio_postgres::Transaction<'_>,
    prefix: &str,
    streams: &BTreeSet<&'s StreamId>,
) -> Result<BTreeMap<&'s StreamId, u64>, EventLogError> {
    let statement = transaction
        .prepare(&format!(
            "SELECT r.ord, (SELECT MAX(version) FROM {prefix}_events
                            WHERE tenant_id = r.r_tenant AND stream_type = r.r_type
                              AND stream_id = r.r_id)
             FROM unnest($1::text[], $2::text[], $3::text[])
                 WITH ORDINALITY AS r(r_tenant, r_type, r_id, ord)"
        ))
        .await
        .map_err(backend)?;
    let ordered: Vec<&StreamId> = streams.iter().copied().collect();
    let mut heads = BTreeMap::new();
    for chunk in ordered.chunks(STATEMENT_BATCH) {
        let tenants: Vec<&str> = chunk.iter().map(|s| s.tenant().as_str()).collect();
        let types: Vec<&str> = chunk.iter().map(|s| s.stream_type()).collect();
        let ids: Vec<&str> = chunk.iter().map(|s| s.stream_id()).collect();
        let rows = transaction
            .query(&statement, &[&tenants, &types, &ids])
            .await
            .map_err(backend)?;
        for row in rows {
            let stream = chunk
                .get(ordinal(row.get(0))?)
                .ok_or_else(|| EventLogError::Backend("batched head out of range".into()))?;
            let head: Option<i64> = row.get(1);
            heads.insert(*stream, head.map(to_u64).transpose()?.unwrap_or(0));
        }
    }
    Ok(heads)
}

impl PostgresEventStore {
    /// A single append: command and claim bookkeeping, then its events in batched statements.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn append_in_transaction(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        stream: &StreamId,
        expected: Expected,
        events: &[NewEvent],
        meta: &CommandMeta,
        admission: &dyn Guard,
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<AppendResult, EventLogError> {
        let prefix = self.prefix.clone();
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
                let events = select_versions(transaction, &prefix, &original, first, last).await?;

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
                select_versions(transaction, &prefix, stream, first_version, last_version).await?;

            return Ok(AppendResult {
                first_version: to_u64(first_version)?,
                last_version: to_u64(last_version)?,
                events: stored,
                deduplicated: true,
            });
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

        let projectors: Vec<Arc<dyn Projector>> = self.inline.lock().map_err(poisoned)?.clone();
        lock_projectors(transaction, &prefix, &projectors, stream.tenant()).await?;
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
                verified: &self.verified,
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
        let planned = plan(stream, head, events, now)?;
        let written = insert_events(transaction, &prefix, meta, &planned).await?;

        let first_version = head + 1;
        let last_version = head + u64::try_from(events.len()).unwrap_or(u64::MAX);
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
                verified: &self.verified,
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

    /// Bind each blob in digest order under the group's transaction.
    ///
    /// A row this statement inserted holds exactly the bytes it was given, and the insert's row
    /// lock keeps it so until the group ends: nothing is read back. Only a digest already bound
    /// is read, locked and checked, because only then are the stored bytes somebody else's.
    async fn bind_blobs(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        tenant: &eventlog_core::TenantId,
        blobs: &[GroupBlob<'_>],
    ) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        for blob in blobs {
            // Row/unique locking also serializes the existing standalone blob ports.
            // Advisory locks alone cannot protect against writers that do not take them.
            let byte_count = to_i64(
                u64::try_from(blob.bytes.len())
                    .map_err(|_| EventLogError::Invalid("blob length exceeds u64".into()))?,
            )?;
            // The integrity columns are the same contract the standalone port writes; a binding
            // this path published without them would fail the table's own check. The hash is the
            // one the request's fingerprint was built from.
            // This handle hashed these exact bytes; a group that returns an error forgets them.
            self.verified.remember(blob.integrity_sha256, blob.bytes);
            let inserted = transaction.execute(
                &format!("INSERT INTO {prefix}_blobs (tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1) VALUES ($1,$2,$3,$4,$5,$6,1) ON CONFLICT (tenant_id,digest) DO NOTHING"),
                &[&tenant.as_str(),&blob.digest,&blob.bytes,&byte_count,&OffsetDateTime::now_utc(),&blob.integrity_sha256],
            ).await.map_err(backend)?;
            if inserted == 1 {
                continue;
            }
            #[cfg(test)]
            self.blob_read_backs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let row = transaction.query_opt(
                &format!("SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {prefix}_blobs WHERE tenant_id=$1 AND digest=$2 FOR UPDATE"),
                &[&tenant.as_str(),&blob.digest],
            ).await.map_err(backend)?;
            let stored = match row {
                Some(row) => self.verified.check_stored(
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
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn group_in_transaction(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        group: &AppendGroup,
        fingerprint: &str,
        blobs: GroupBlobs<'_>,
        admission: &dyn Guard,
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
        deadline: tokio::time::Instant,
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
            let recorded: String = row.get(0);
            let mismatch = || EventLogError::IdempotencyMismatch {
                key: group.meta.idempotency_key.clone(),
            };
            match blobs {
                GroupBlobs::Bound(_) if recorded != fingerprint => return Err(mismatch()),
                GroupBlobs::Bound(_) => {}
                GroupBlobs::Guarded {
                    blobs,
                    blob_fingerprint,
                } => {
                    // A retry carrying a batch is answering for it, so admission runs before
                    // anything is said about the batch, including which fingerprint the receipt
                    // matches; otherwise a replayed key would tell its caller whether a digest is
                    // bound and whether bytes match.
                    self.admit(transaction, group, admission, callback_failed)
                        .await?;
                    // Committed through `AtomicBlobEventStore`: that receipt already binds this
                    // group, these sorted digests and the SHA-256 of these bytes, so it is this
                    // request. Its content is not read, as that method's own retry does not.
                    if recorded != blob_fingerprint {
                        if recorded != fingerprint {
                            return Err(mismatch());
                        }
                        // Decided by what the commit recorded, not by what is bound now: a
                        // deletion is not a different request.
                        let digests: Option<serde_json::Value> = transaction
                            .query_opt(
                                &format!("SELECT digests FROM {prefix}_group_batches WHERE tenant_id=$1 AND idempotency_key=$2"),
                                &[&group.tenant.as_str(), &group.meta.idempotency_key],
                            )
                            .await
                            .map_err(backend)?
                            .map(|row| row.get(0));
                        let digests: Vec<String> = match digests {
                            Some(digests) => serde_json::from_value(digests).map_err(|_| {
                                EventLogError::Backend("invalid stored group batch".into())
                            })?,
                            None => Vec::new(),
                        };
                        if digests
                            .iter()
                            .map(String::as_str)
                            .ne(blobs.iter().map(|blob| blob.digest))
                        {
                            return Err(mismatch());
                        }
                        for blob in blobs {
                            self.check_bound(transaction, &group.tenant, blob).await?;
                        }
                    }
                }
            }
            let ranges: Vec<GroupRange> = serde_json::from_value(row.get(1))
                .map_err(|_| EventLogError::Invalid("invalid stored group ranges".into()))?;
            let mut windows = Vec::with_capacity(ranges.len());
            for range in &ranges {
                if range.stream.tenant() != &group.tenant {
                    return Err(EventLogError::Invalid(
                        "stored group crosses tenants".into(),
                    ));
                }
                // `select_versions`' inclusive range, as a window.
                let after = to_i64(range.first_version)?.saturating_sub(1);
                let through = to_i64(range.last_version)?;
                windows.push(Window {
                    stream: &range.stream,
                    after,
                    through,
                    fetch: through.saturating_sub(after).max(0),
                });
            }
            let stored = select_windows(
                transaction,
                prefix,
                &windows.iter().collect::<Vec<_>>(),
                None,
            )
            .await?;
            let mut appends = Vec::with_capacity(ranges.len());
            for (range, rows) in ranges.iter().zip(stored) {
                appends.push(AppendResult {
                    first_version: range.first_version,
                    last_version: range.last_version,
                    events: rows.iter().map(read_event).collect::<Result<_, _>>()?,
                    deduplicated: true,
                });
            }
            return Ok(AppendGroupResult {
                appends,
                deduplicated: true,
            });
        }
        let streams: BTreeSet<_> = group.appends.iter().map(|a| &a.stream).collect();
        lock_streams(
            transaction,
            prefix,
            &streams,
            deadline,
            self.pool.options.statement_timeout,
        )
        .await?;
        let projectors: Vec<Arc<dyn Projector>> = self.inline.lock().map_err(poisoned)?.clone();
        lock_projectors(transaction, prefix, &projectors, &group.tenant).await?;
        match blobs {
            GroupBlobs::Bound(blobs) => {
                self.bind_blobs(transaction, &group.tenant, blobs).await?;
                self.admit(transaction, group, admission, callback_failed)
                    .await?;
            }
            // Admission before a byte of the batch is bound: a refused guard publishes no blob.
            GroupBlobs::Guarded { blobs, .. } => {
                self.admit(transaction, group, admission, callback_failed)
                    .await?;
                self.bind_blobs(transaction, &group.tenant, blobs).await?;
            }
        }
        // Every member is planned against the heads read under the stream locks, each later
        // member of a stream starting where the earlier one ended. The first member the store
        // refuses ends the plan; the members before it are still written and projected, as they
        // were one member at a time, before the refusal returns and rolls the group back.
        let mut heads = stream_heads(transaction, prefix, &streams).await?;
        let mut planned = Vec::new();
        let mut members = Vec::with_capacity(group.appends.len());
        let mut refusal = None;
        for entry in &group.appends {
            let head = heads.get(&entry.stream).copied().unwrap_or(0);
            let member = check_expected(entry.expected, head).and_then(|()| {
                plan(
                    &entry.stream,
                    head,
                    &entry.events,
                    OffsetDateTime::now_utc(),
                )
            });
            match member {
                Ok(events) => {
                    let last = head + u64::try_from(events.len()).unwrap_or(u64::MAX);
                    heads.insert(&entry.stream, last);
                    members.push((entry, head + 1, last, events.len()));
                    planned.extend(events);
                }
                Err(error) => {
                    refusal = Some(error);
                    break;
                }
            }
        }
        let mut written = insert_events(transaction, prefix, &group.meta, &planned)
            .await?
            .into_iter();
        #[cfg(test)]
        if !planned.is_empty() {
            checkpoint("group-event-1");
        }
        let mut appends = Vec::with_capacity(members.len());
        let mut ranges = Vec::with_capacity(members.len());
        #[cfg(test)]
        let mut member = 0;
        for (entry, first_version, last_version, count) in members {
            let events: Vec<RecordedEvent> = written.by_ref().take(count).collect();
            for projector in &projectors {
                let mut projections = PostgresProjections {
                    client: transaction,
                    blob_prefix: prefix,
                    projection_prefix: prefix,
                    lock_prefix: prefix,
                    inline: &self.inline_names,
                    tenant: entry.stream.tenant(),
                    admission: None,
                    reservation_pending: false,
                    callback_failed: Arc::clone(callback_failed),
                    verified: &self.verified,
                    selected: None,
                };
                for recorded in &events {
                    let result = projector.apply(recorded, &mut projections).await;
                    ensure_callback_integrity(callback_failed)?;
                    result?;
                }
            }
            ensure_callback_integrity(callback_failed)?;
            #[cfg(test)]
            {
                member += 1;
                checkpoint(&format!("group-member-{member}"));
            }
            ranges.push(GroupRange {
                stream: entry.stream.clone(),
                first_version,
                last_version,
            });
            appends.push(AppendResult {
                first_version,
                last_version,
                events,
                deduplicated: false,
            });
        }
        if let Some(refusal) = refusal {
            return Err(refusal);
        }
        transaction.execute(&format!("INSERT INTO {prefix}_append_groups (tenant_id,idempotency_key,request_hash,ranges) VALUES ($1,$2,$3,$4)"),
            &[&group.tenant.as_str(),&group.meta.idempotency_key,&fingerprint,&serde_json::to_value(&ranges).map_err(|_| EventLogError::Invalid("invalid group ranges".into()))?]).await.map_err(backend)?;
        if let GroupBlobs::Guarded { blobs, .. } = blobs {
            let digests: Vec<&str> = blobs.iter().map(|blob| blob.digest).collect();
            transaction
                .execute(
                    &format!("INSERT INTO {prefix}_group_batches (tenant_id,idempotency_key,digests) VALUES ($1,$2,$3)"),
                    &[
                        &group.tenant.as_str(),
                        &group.meta.idempotency_key,
                        &serde_json::json!(digests),
                    ],
                )
                .await
                .map_err(backend)?;
        }
        #[cfg(test)]
        checkpoint("group-bookkeeping");
        ensure_callback_integrity(callback_failed)?;
        Ok(AppendGroupResult {
            appends,
            deduplicated: false,
        })
    }

    /// Run the group's admission guard inside its transaction, with the trusted reservation
    /// authority, refusing an unfinished reservation.
    async fn admit(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        group: &AppendGroup,
        admission: &dyn Guard,
        callback_failed: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
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
            verified: &self.verified,
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
        Ok(())
    }

    /// Refuse a retried batch blob whose digest is bound to other bytes; an unbound digest (one
    /// deleted since the commit) is no refusal, and nothing is bound again.
    async fn check_bound(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        tenant: &eventlog_core::TenantId,
        blob: &GroupBlob<'_>,
    ) -> Result<(), EventLogError> {
        let prefix = &self.prefix;
        let row = transaction.query_opt(
            &format!("SELECT bytes,byte_count,integrity_sha256,integrity_v1 FROM {prefix}_blobs WHERE tenant_id=$1 AND digest=$2"),
            &[&tenant.as_str(), &blob.digest],
        ).await.map_err(backend)?;
        if let Some(row) = row {
            let stored = self.verified.check_stored(
                row.get(0),
                row.get(1),
                row.get(2),
                i64::from(row.get::<_, i32>(3)),
            )?;
            if stored != blob.bytes {
                return Err(EventLogError::Invalid(
                    "blob digest already names different content".into(),
                ));
            }
        }
        Ok(())
    }
}

impl PostgresEventStore {
    /// Run one group in its own transaction under the store-operation deadline: the driver every
    /// group port shares.
    ///
    /// A deduplicated answer to a batch-bearing guarded retry rolls back rather than commits: it
    /// wrote nothing, and its re-run admission may have staged reservations it must not keep.
    ///
    /// `deadline` is taken at the public entry: one budget, from which the wait for registration
    /// to freeze (an inline rebuild holds it while it replays) is spent too, as on the
    /// single-append path.
    async fn run_group(
        &self,
        group: &AppendGroup,
        fingerprint: &str,
        blobs: GroupBlobs<'_>,
        admission: Arc<dyn Guard>,
        deadline: tokio::time::Instant,
    ) -> Result<AppendGroupResult, EventLogError> {
        let commit_started = std::sync::atomic::AtomicBool::new(false);
        let outcome = tokio::time::timeout_at(deadline, async {
            self.freeze().await;
            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client.transaction().await.map_err(backend)?;
            publication_gate(&transaction, &self.prefix, false).await?;
            let callback_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let result = self
                .group_in_transaction(
                    &transaction,
                    group,
                    fingerprint,
                    blobs,
                    admission.as_ref(),
                    &callback_failed,
                    deadline,
                )
                .await;
            match result {
                Ok(result)
                    if result.deduplicated && matches!(blobs, GroupBlobs::Guarded { .. }) =>
                {
                    transaction.rollback().await.map_err(backend)?;
                    client.settled();
                    Ok(result)
                }
                Ok(result) => {
                    #[cfg(test)]
                    checkpoint("group-precommit");
                    commit_started.store(true, std::sync::atomic::Ordering::Release);
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
        .map_err(|_| timed_out_write(&commit_started))
        .and_then(|outcome| outcome);
        // Content of a blob-bearing group that returned an error is not kept.
        if outcome.is_err() && !blobs.is_empty() {
            self.verified.forget();
        }
        outcome
    }
}

impl AtomicEventStore for PostgresEventStore {
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            // One budget from the public entry: the wait for registration to freeze (an inline
            // rebuild holds it while it replays) is spent from it, as on the single-append path.
            let deadline = tokio::time::Instant::now() + self.pool.options.transaction_timeout;
            let fingerprint = group.fingerprint()?;
            self.run_group(
                group,
                &fingerprint,
                GroupBlobs::Bound(&[]),
                admission,
                deadline,
            )
            .await
        })
    }

    /// The group and its batch in the group's one transaction, so the batch costs the one commit
    /// the group already takes where a `put_blob` per blob costs one each.
    ///
    /// Admission runs before a byte of the batch is bound, and a refusal anywhere rolls back the
    /// bindings with the events, so no blob of a group that did not commit is reachable. The
    /// sorted digests the commit bound are recorded beside its receipt: a retry carrying a batch
    /// is admitted again and must carry exactly those digests, whether or not each is still bound;
    /// a retry carrying none is the ordinary group retry and repeats no admission.
    fn append_group_guarded_with_blobs<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        if blobs.is_empty() {
            return self.append_group_guarded(group, admission);
        }
        Box::pin(async move {
            // One budget from the public entry, registration wait included; see
            // `run_group`.
            let deadline = tokio::time::Instant::now() + self.pool.options.transaction_timeout;
            let fingerprint = group.fingerprint()?;
            // Equal repetitions of a digest collapse to one binding; other bytes under it refuse.
            let mut unique: BTreeMap<&str, &[u8]> = BTreeMap::new();
            for (digest, bytes) in blobs {
                eventlog_core::validate_field("blob digest", digest)?;
                if let Some(earlier) = unique.insert(digest, bytes)
                    && earlier != bytes.as_slice()
                {
                    return Err(EventLogError::Invalid(
                        "blob digest already names different content".into(),
                    ));
                }
            }
            // One SHA-256 per content serves the fingerprint `AtomicBlobEventStore` would have
            // recorded for this request and the integrity column alike.
            let (blob_fingerprint, hashes) =
                BlobAppendGroup::fingerprint_parts(group, unique.iter().map(|(d, b)| (*d, *b)))?;
            let blobs = GroupBlob::sorted(unique, &hashes)?;
            self.run_group(
                group,
                &fingerprint,
                GroupBlobs::Guarded {
                    blobs: &blobs,
                    blob_fingerprint: &blob_fingerprint,
                },
                admission,
                deadline,
            )
            .await
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
            // One budget from the public entry, registration wait included; see
            // `run_group`.
            let deadline = tokio::time::Instant::now() + self.pool.options.transaction_timeout;
            // One SHA-256 per content serves the fingerprint and the integrity column alike, and
            // the blobs are bound from the caller's own bytes.
            let (fingerprint, hashes) = request.fingerprint_and_hashes()?;
            let blobs = GroupBlob::sorted(
                request
                    .blobs
                    .iter()
                    .map(|blob| (blob.digest.as_str(), blob.bytes.as_slice())),
                &hashes,
            )?;
            self.run_group(
                &request.group,
                &fingerprint,
                GroupBlobs::Bound(&blobs),
                admission,
                deadline,
            )
            .await
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

    /// The tenant's committed events and group receipts, counted in the tables, not through the
    /// feed.
    ///
    /// A withheld feed reads as empty, so `feed_at_least(_, 0)` returns on its first read and
    /// proves nothing about a crash point that is expected to commit nothing; and because it
    /// returns on `>=`, a duplicate member committed behind the watermark would not show in a feed
    /// that already holds the expected count. A plain count in a fresh statement snapshot is not
    /// bounded by the watermark: it sees every committed row and no uncommitted one. It is the
    /// positive check that a crash point committed nothing, and what makes the counts after a
    /// retry exact.
    async fn committed_rows(store: &PostgresEventStore) -> (i64, i64) {
        let prefix = &store.prefix;
        let mut client = store.pool.acquire().await.unwrap();
        let row = client
            .query_one(
                &format!(
                    "SELECT (SELECT count(*) FROM {prefix}_events WHERE tenant_id = $1),
                            (SELECT count(*) FROM {prefix}_append_groups WHERE tenant_id = $1)"
                ),
                &[&tenant().as_str()],
            )
            .await
            .unwrap();
        client.settled();
        (row.get(0), row.get(1))
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
            assert_eq!(
                committed_rows(&store).await,
                if committed { (3, 1) } else { (0, 0) },
                "{point}: committed events and group receipts after the crash"
            );
            // With the table count exact, the feed only has to deliver those rows: for the six
            // points that committed nothing an empty read is now consistent with, not the proof of,
            // an empty log.
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
            // Exact, not a lower bound: the table count refuses a duplicate member or receipt the
            // watermark still withholds, and the feed must then deliver exactly those members.
            assert_eq!(
                committed_rows(&store).await,
                (3, 1),
                "{point}: no duplicate or missing group member or receipt"
            );
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
