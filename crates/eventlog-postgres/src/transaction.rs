//! Caller transactions compose the existing append protocol without lending raw SQL authority.
use std::future::Future;

use super::{
    BoxFuture, COLUMNS, EventLogError, GenericClient, NoGuard, PostgresEventStore,
    PostgresProjections, ProjectionSpec, ProjectionStore, SnapshotGeneration, StreamId,
    StreamSlice, TenantId, Transaction, Value, backend, bounded_limit, lock_identity,
    publication_gate, read_event, to_i64, to_u64, validate_field,
};
use eventlog_core::{
    AppendGroup, AppendGroupResult, ProjectionPage, ProjectionQuery, TransactionSession,
    TransactionalEventStore,
};

fn unfinished() -> EventLogError {
    EventLogError::Invalid("unfinished session operation poisons this transaction".into())
}
fn begin(pending: &mut bool) -> Result<(), EventLogError> {
    if *pending {
        return Err(unfinished());
    }
    *pending = true;
    Ok(())
}
async fn operation<T>(
    pending: &mut bool,
    future: impl Future<Output = Result<T, EventLogError>>,
) -> Result<T, EventLogError> {
    begin(pending)?;
    let result = future.await;
    // Cancellation leaves this set. A caught SQL error cannot turn COMMIT's rollback response
    // into apparent success either; only a successfully settled operation clears it.
    if result.is_ok() {
        *pending = false;
    }
    result
}
async fn saved<T>(
    pending: &mut bool,
    transaction: &Transaction<'_>,
    future: impl Future<Output = Result<T, EventLogError>>,
) -> Result<T, EventLogError> {
    begin(pending)?;
    transaction
        .batch_execute("SAVEPOINT eventlog_session_step")
        .await
        .map_err(backend)?;
    let result = future.await;
    if result.is_err() {
        transaction
            .batch_execute("ROLLBACK TO SAVEPOINT eventlog_session_step")
            .await
            .map_err(backend)?;
    }
    transaction
        .batch_execute("RELEASE SAVEPOINT eventlog_session_step")
        .await
        .map_err(backend)?;
    *pending = false;
    result
}

struct Session<'a, 'b> {
    owner: &'a PostgresEventStore,
    transaction: &'a Transaction<'b>,
    tenant: &'a TenantId,
    projections: PostgresProjections<'a, 'b>,
    pending: bool,
}
impl Session<'_, '_> {
    fn scope(&self, stream: &StreamId) -> Result<(), EventLogError> {
        if stream.tenant() != self.tenant {
            return Err(EventLogError::Invalid(
                "transaction session cannot cross tenant".into(),
            ));
        }
        Ok(())
    }
}

impl TransactionalEventStore for PostgresEventStore {
    fn with_transaction<'a, T, F>(
        &'a self,
        tenant: &'a TenantId,
        work: F,
    ) -> BoxFuture<'a, Result<T, EventLogError>>
    where
        T: Send + 'a,
        F: for<'session> FnOnce(
                &'session mut dyn TransactionSession,
            ) -> BoxFuture<'session, Result<T, EventLogError>>
            + Send
            + 'a,
    {
        Box::pin(async move {
            self.freeze().await;
            tokio::time::timeout(self.pool.options.transaction_timeout, async {
                let mut client = self.pool.acquire().await?;
                client.quarantine();
                let transaction = client.transaction().await.map_err(backend)?;
                publication_gate(&transaction, &self.prefix, false).await?;
                let mut session = Session {
                    owner: self,
                    transaction: &transaction,
                    tenant,
                    pending: false,
                    projections: PostgresProjections {
                        client: &transaction,
                        prefix: &self.prefix,
                        inline: &self.inline_names,
                        tenant,
                        admission: None,
                        reservation_pending: false,
                    },
                };
                let result = work(&mut session).await;
                let result = match result {
                    Ok(_) if session.pending || session.projections.reservation_pending => {
                        Err(unfinished())
                    }
                    settled => settled,
                };
                match result {
                    Ok(value) => {
                        transaction
                            .commit()
                            .await
                            .map_err(|_| EventLogError::UnknownCommit)?;
                        client.settled();
                        Ok(value)
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

impl TransactionSession for Session<'_, '_> {
    fn tenant(&self) -> &TenantId {
        self.tenant
    }
    fn projections(&mut self) -> &mut dyn ProjectionStore {
        self
    }
    fn read_stream<'a>(
        &'a mut self,
        stream: &'a StreamId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>> {
        Box::pin(async move {
            self.scope(stream)?;
            operation(
                &mut self.pending,
                read_stream(self.transaction, &self.owner.prefix, stream, after, limit),
            )
            .await
        })
    }
    fn stream_version<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>> {
        Box::pin(async move {
            self.scope(stream)?;
            operation(
                &mut self.pending,
                stream_version(self.transaction, &self.owner.prefix, stream),
            )
            .await
        })
    }
    fn list_streams<'a>(
        &'a mut self,
        kind: &'a str,
        after: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StreamId>, EventLogError>> {
        Box::pin(async move {
            operation(
                &mut self.pending,
                list_streams(
                    self.transaction,
                    &self.owner.prefix,
                    self.tenant,
                    kind,
                    after,
                    limit,
                ),
            )
            .await
        })
    }
    fn lock_stream<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.scope(stream)?;
            operation(
                &mut self.pending,
                lock_identity(
                    self.transaction,
                    &self.owner.prefix,
                    "stream",
                    &[
                        self.tenant.as_str(),
                        stream.stream_type(),
                        stream.stream_id(),
                    ],
                ),
            )
            .await
        })
    }
    fn lock_identity<'a>(
        &'a mut self,
        namespace: &'a str,
        identity: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            validate_field("identity namespace", namespace)?;
            validate_field("logical identity", identity)?;
            operation(
                &mut self.pending,
                lock_identity(
                    self.transaction,
                    &self.owner.prefix,
                    "session-identity",
                    &[self.tenant.as_str(), namespace, identity],
                ),
            )
            .await
        })
    }
    fn snapshot_generation<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<SnapshotGeneration>, EventLogError>> {
        Box::pin(async move {
            self.scope(stream)?;
            let transaction = self.transaction;
            let prefix = &self.owner.prefix;
            operation(&mut self.pending, async move {
                transaction.execute(&format!("INSERT INTO {prefix}_snapshot_generations (tenant_id,stream_type,stream_id,generation) VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,stream_type,stream_id) DO NOTHING"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id(), &uuid::Uuid::now_v7()]).await.map_err(backend)?;
                let generation: uuid::Uuid = transaction.query_one(&format!("SELECT generation FROM {prefix}_snapshot_generations WHERE tenant_id=$1 AND stream_type=$2 AND stream_id=$3 FOR SHARE"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id()]).await.map_err(backend)?.get(0);
                Ok(Some(SnapshotGeneration::from_uuid(generation)))
            }).await
        })
    }
    fn reserve_sequence<'a>(
        &'a mut self,
        namespace: &'a str,
        count: u64,
    ) -> BoxFuture<'a, Result<u64, EventLogError>> {
        Box::pin(async move {
            validate_field("sequence namespace", namespace)?;
            if count == 0 {
                return Err(EventLogError::Invalid(
                    "sequence reservation must be positive".into(),
                ));
            }
            let count = to_i64(count)?;
            let coordinate = format!(
                "q{}:{}{}:{namespace}",
                self.tenant.as_str().len(),
                self.tenant.as_str(),
                namespace.len()
            );
            let table = format!("{}_scope_counters", self.owner.prefix);
            let transaction = self.transaction;
            saved(&mut self.pending, transaction, async move {
                let row = transaction.query_opt(&format!("INSERT INTO {table} (coordinate,held) VALUES ($1,$2) ON CONFLICT(coordinate) DO UPDATE SET held={table}.held+EXCLUDED.held WHERE {table}.held <= 9223372036854775807-$2 RETURNING held-$2"), &[&coordinate, &count]).await.map_err(backend)?
                    .ok_or_else(|| EventLogError::Invalid("sequence reservation exceeds its integer range".into()))?;
                to_u64(row.get(0))
            }).await
        })
    }
    fn append_group<'a>(
        &'a mut self,
        group: &'a AppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        Box::pin(async move {
            if &group.tenant != self.tenant {
                return Err(EventLogError::Invalid(
                    "transaction group cannot cross tenant".into(),
                ));
            }
            let fingerprint = group.fingerprint()?;
            saved(
                &mut self.pending,
                self.transaction,
                self.owner
                    .group_in_transaction(self.transaction, group, &fingerprint, &NoGuard),
            )
            .await
        })
    }
}

// A returned projection view must retain the session's cancellation marker too. Returning the
// underlying PostgresProjections directly would let a dropped upsert future escape that marker.
macro_rules! projection_operation {
    ($name:ident($($arg:ident: $type:ty),*) -> $output:ty) => {
        fn $name<'a>(&'a mut self, $($arg: $type),*) -> BoxFuture<'a, Result<$output, EventLogError>> {
            Box::pin(async move { operation(&mut self.pending, self.projections.$name($($arg),*)).await })
        }
    };
}
impl ProjectionStore for Session<'_, '_> {
    projection_operation!(get_blob(digest: &'a str) -> Option<Vec<u8>>);
    projection_operation!(reserve(permit: &'a eventlog_core::AdmissionPermit, reservations: &'a [eventlog_core::Reservation]) -> Vec<i64>);
    projection_operation!(upsert(projection: &'a ProjectionSpec, tenant: &'a TenantId, key: &'a str, body: &'a Value) -> ());
    projection_operation!(delete(projection: &'a ProjectionSpec, tenant: &'a TenantId, key: &'a str) -> ());
    projection_operation!(get(projection: &'a ProjectionSpec, tenant: &'a TenantId, key: &'a str) -> Option<Value>);
    projection_operation!(get_for_update(projection: &'a ProjectionSpec, tenant: &'a TenantId, key: &'a str) -> Option<Value>);
    projection_operation!(find(projection: &'a ProjectionSpec, tenant: &'a TenantId, field: &'a str, value: &'a str, limit: usize) -> Vec<Value>);
    projection_operation!(query_documents(projection: &'a ProjectionSpec, tenant: &'a TenantId, query: &'a ProjectionQuery) -> ProjectionPage);
}

pub(super) async fn read_stream<C: GenericClient>(
    client: &C,
    prefix: &str,
    stream: &StreamId,
    after: u64,
    limit: usize,
) -> Result<StreamSlice, EventLogError> {
    let limit = bounded_limit(limit);
    let rows = client.query(&format!("SELECT {COLUMNS} FROM {prefix}_events WHERE tenant_id=$1 AND stream_type=$2 AND stream_id=$3 AND version>$4 ORDER BY version LIMIT $5"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id(), &to_i64(after)?, &to_i64(limit as u64 + 1)?]).await.map_err(backend)?;
    let mut events = rows.iter().map(read_event).collect::<Result<Vec<_>, _>>()?;
    let end_of_stream = events.len() <= limit;
    events.truncate(limit);
    let next_version = events.last().map_or(after, |event| event.version);
    Ok(StreamSlice {
        events,
        next_version,
        end_of_stream,
    })
}
pub(super) async fn stream_version<C: GenericClient>(
    client: &C,
    prefix: &str,
    stream: &StreamId,
) -> Result<Option<u64>, EventLogError> {
    let head: Option<i64> = client.query_one(&format!("SELECT MAX(version) FROM {prefix}_events WHERE tenant_id=$1 AND stream_type=$2 AND stream_id=$3"), &[&stream.tenant().as_str(), &stream.stream_type(), &stream.stream_id()]).await.map_err(backend)?.get(0);
    head.map(to_u64).transpose()
}
pub(super) async fn list_streams<C: GenericClient>(
    client: &C,
    prefix: &str,
    tenant: &TenantId,
    kind: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<StreamId>, EventLogError> {
    StreamId::new(tenant.clone(), kind, after.unwrap_or("_"))?;
    let rows = client.query(&format!("SELECT DISTINCT stream_id FROM {prefix}_events WHERE tenant_id=$1 AND stream_type=$2 AND stream_id>$3 ORDER BY stream_id LIMIT $4"), &[&tenant.as_str(), &kind, &after.unwrap_or(""), &to_i64(bounded_limit(limit) as u64)?]).await.map_err(backend)?;
    rows.iter()
        .map(|row| StreamId::new(tenant.clone(), kind, row.get::<_, String>(0)))
        .collect()
}
