//! One tenant's complete observation, behind the publication gate and inside one fresh snapshot.
//!
//! The order of the two is the whole design. Taking a repeatable-read snapshot first and *then*
//! waiting for the gate fixes a view of the database before the wait, so an erasure that completes
//! during that wait is invisible and the capture hands back a tenant that no longer exists. The
//! lock comes first, at session level, and the snapshot is established after it is held.
//!
//! The feed's watermark has no place here either. It exists to keep a resumable cursor from
//! skipping a late commit, and it works by hiding rows an unrelated transaction's `xmin` still
//! covers — rows that are committed, belong to this tenant, and are part of its history. A capture
//! that applied it would report a complete observation with a hole in it.

use eventlog_core::{
    BoxFuture, CaptureBudget, CaptureError, CaptureLimits, CaptureMaterial, CaptureResource,
    CapturedBlob, CapturedProjection, ConsistentTenantCapture, EventLogError,
    ProjectionCaptureRefusal, ProjectionSpec, RecordedEvent, TenantCapture, TenantId, order_blobs,
    order_rows, validate_capture_request, validate_captured_digest, validate_captured_order,
    validate_stored_blob,
};
use serde_json::Value;
use tokio_postgres::{IsolationLevel, Transaction};

use crate::{
    ADVISORY_KEY, COLUMNS, PostgresEventStore, backend, pool::Lease, projection_table,
    publication_identity, read_event, schema, to_i64, to_u64,
};

/// How many rows one bounded read retains at a time inside the capture snapshot.
///
/// Chunked reads inside one repeatable-read snapshot are one observation; the prohibition is on
/// the feed watermark and on handing back a partial result, not on bounded accumulation. Loading
/// every blob through one unbounded query and only then testing the cap is the thing this avoids.
const CHUNK: i64 = 1_000;

fn operational(error: tokio_postgres::Error) -> CaptureError {
    CaptureError::Store(backend(error))
}

fn corrupt(material: CaptureMaterial) -> CaptureError {
    CaptureError::Corrupt { material }
}

impl ConsistentTenantCapture for PostgresEventStore {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>> {
        Box::pin(async move {
            validate_capture_request(tenant, projections)?;
            // The existing transaction deadline bounds lock acquisition, the read and the cleanup
            // together. A timeout drops this future, which retires the quarantined lease.
            tokio::time::timeout(
                self.pool.options.transaction_timeout,
                self.capture_bounded(tenant, projections, limits),
            )
            .await
            .map_err(|_| {
                CaptureError::Store(EventLogError::Deadline {
                    operation: "store operation",
                })
            })?
        })
    }
}

impl PostgresEventStore {
    async fn capture_bounded(
        &self,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Result<TenantCapture, CaptureError> {
        let identity = publication_identity(&self.prefix)?;
        let mut client = self.pool.acquire().await?;
        // Quarantined from the first moment. Cancellation while awaiting the lock, while holding
        // it, or mid-read must retire this session; a session-level lock survives its transaction,
        // so recycling the connection would hand the next caller a lock nobody knows it holds.
        client.quarantine();
        client
            .execute(
                &format!("SELECT pg_advisory_lock({ADVISORY_KEY})"),
                &[&identity],
            )
            .await
            .map_err(operational)?;
        let value = self
            .capture_locked(&mut client, tenant, projections, limits)
            .await;
        let release = client
            .query_one(
                &format!("SELECT pg_advisory_unlock({ADVISORY_KEY})"),
                &[&identity],
            )
            .await;
        let released = match release {
            Ok(row) => row.get::<_, bool>(0),
            // A cleanup failure prevents returning a value, and never converts an earlier one.
            Err(error) => return Err(value.err().unwrap_or_else(|| operational(error))),
        };
        let value = value?;
        if !released {
            return Err(CaptureError::Store(EventLogError::Backend(
                "publication lock release was not confirmed".to_owned(),
            )));
        }
        // Only now: released, verified, and with a complete value in hand.
        client.settled();
        Ok(value)
    }

    async fn capture_locked(
        &self,
        client: &mut Lease,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Result<TenantCapture, CaptureError> {
        // Every compliant publisher takes this gate before allocating a sequence position and
        // holds it to commit, so once it is held exclusively the earlier ones have settled and the
        // later ones cannot start. The snapshot below is established after that is true.
        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(operational)?;
        let value = self
            .read_capture(&transaction, tenant, projections, limits)
            .await;
        match value {
            Ok(value) => {
                transaction.commit().await.map_err(operational)?;
                Ok(value)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn read_capture(
        &self,
        transaction: &Transaction<'_>,
        tenant: &TenantId,
        projections: &[ProjectionSpec],
        limits: CaptureLimits,
    ) -> Result<TenantCapture, CaptureError> {
        let prefix = &self.prefix;
        let identity = stored_identity(transaction, prefix, tenant).await?;
        let redacted: bool = transaction
            .query_one(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {prefix}_events
                     WHERE tenant_id = $1 AND redacted_at IS NOT NULL)"
                ),
                &[&tenant.as_str()],
            )
            .await
            .map_err(operational)?
            .get(0);
        if redacted {
            return Err(CaptureError::RedactedHistory);
        }

        let mut admitted = Vec::with_capacity(projections.len());
        for specification in projections {
            admit_projection(transaction, prefix, specification).await?;
            admitted.push(*specification);
        }

        let mut budget = CaptureBudget::new(limits);
        preflight(transaction, prefix, tenant, &admitted, &budget).await?;
        let events = read_events(transaction, prefix, tenant, &mut budget).await?;
        validate_captured_order(tenant, &events)?;
        let blobs = read_blobs(transaction, prefix, tenant, &mut budget).await?;
        let mut captured = Vec::with_capacity(admitted.len());
        for specification in admitted {
            let rows = read_rows(transaction, prefix, tenant, specification, &mut budget).await?;
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

/// Read the identity the store already holds; `stream_identity` would insert one on absence.
async fn stored_identity(
    transaction: &Transaction<'_>,
    prefix: &str,
    tenant: &TenantId,
) -> Result<String, CaptureError> {
    let stored = transaction
        .query_opt(
            &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id = $1"),
            &[&tenant.as_str()],
        )
        .await
        .map_err(operational)?;
    let Some(row) = stored else {
        return Err(CaptureError::TenantIdentityMissing);
    };
    let identity: String = row
        .try_get(0)
        .map_err(|_| corrupt(CaptureMaterial::Identity))?;
    if identity.is_empty() {
        return Err(corrupt(CaptureMaterial::Identity));
    }
    Ok(identity)
}

/// Refuse a cap the stored counts and lengths already prove crossed, inside this same snapshot.
async fn preflight(
    transaction: &Transaction<'_>,
    prefix: &str,
    tenant: &TenantId,
    admitted: &[ProjectionSpec],
    budget: &CaptureBudget,
) -> Result<(), CaptureError> {
    // The stored length is what a payload cap would be proven from, so it is checked against the
    // bytes first — a column is not evidence about content it merely claims to describe, and a
    // length that is a lie would otherwise report a cap the tenant's content never crossed. The
    // server measures the bytes inside this same snapshot: no blob crosses the wire here, and an
    // oversized one is never allocated on this side to find out how long it is.
    let totals = transaction
        .query_one(
            &format!(
                "SELECT (SELECT COUNT(*) FROM {prefix}_events WHERE tenant_id = $1),
                        (SELECT COUNT(*) FROM {prefix}_blobs WHERE tenant_id = $1),
                        (SELECT COALESCE(SUM(octet_length(bytes)),0)::bigint FROM {prefix}_blobs
                         WHERE tenant_id = $1),
                        (SELECT COUNT(*) FROM {prefix}_blobs WHERE tenant_id = $1
                         AND byte_count IS DISTINCT FROM octet_length(bytes)::bigint)"
            ),
            &[&tenant.as_str()],
        )
        .await
        .map_err(operational)?;
    budget.proven(CaptureResource::Events, to_u64(totals.get(0))?)?;
    budget.proven(CaptureResource::Blobs, to_u64(totals.get(1))?)?;
    if totals.get::<_, i64>(3) != 0 {
        return Err(corrupt(CaptureMaterial::Blob));
    }
    // Now a real lower bound on the payload: the bytes this tenant actually holds. Each blob is
    // still validated and charged one at a time as it is decoded; this only refuses early.
    budget.proven(
        CaptureResource::PayloadBytes,
        u64::try_from(totals.get::<_, i64>(2)).map_err(|_| corrupt(CaptureMaterial::Blob))?,
    )?;
    let mut rows = 0_u64;
    for specification in admitted {
        let table = projection_table(prefix, specification.name);
        let count: i64 = transaction
            .query_one(
                &format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = $1"),
                &[&tenant.as_str()],
            )
            .await
            .map_err(operational)?
            .get(0);
        rows = rows
            .checked_add(to_u64(count)?)
            .ok_or_else(|| corrupt(CaptureMaterial::Projection))?;
    }
    budget.proven(CaptureResource::ProjectionRows, rows)
}

async fn read_events(
    transaction: &Transaction<'_>,
    prefix: &str,
    tenant: &TenantId,
    budget: &mut CaptureBudget,
) -> Result<Vec<RecordedEvent>, CaptureError> {
    let owner = tenant.as_str();
    let chunk = CHUNK;
    let statement = format!(
        "SELECT {COLUMNS} FROM {prefix}_events
         WHERE tenant_id = $1 AND ($2::bigint IS NULL OR global_seq > $2) ORDER BY global_seq LIMIT $3"
    );
    let mut events = Vec::new();
    let mut after: Option<i64> = None;
    loop {
        let page = transaction
            .query(&statement, &[&owner, &after, &chunk])
            .await
            .map_err(operational)?;
        if page.is_empty() {
            return Ok(events);
        }
        for row in &page {
            let event = read_event(row).map_err(|_| corrupt(CaptureMaterial::Event))?;
            budget.admit_event(&event)?;
            after = Some(to_i64(event.global_seq)?);
            events.push(event);
        }
    }
}

async fn read_blobs(
    transaction: &Transaction<'_>,
    prefix: &str,
    tenant: &TenantId,
    budget: &mut CaptureBudget,
) -> Result<Vec<CapturedBlob>, CaptureError> {
    let owner = tenant.as_str();
    let chunk = CHUNK;
    let columns = "digest,bytes,byte_count,integrity_sha256,integrity_v1";
    let first = format!(
        "SELECT {columns} FROM {prefix}_blobs WHERE tenant_id = $1 ORDER BY digest LIMIT $2"
    );
    let next = format!(
        "SELECT {columns} FROM {prefix}_blobs
         WHERE tenant_id = $1 AND digest > $2 ORDER BY digest LIMIT $3"
    );
    let mut blobs = Vec::new();
    let mut after: Option<String> = None;
    loop {
        // The first page carries no key predicate: a binding is either in the set or it is not,
        // and one that sorts before every other must not disappear into a resume condition.
        let page = match &after {
            None => transaction.query(&first, &[&owner, &chunk]).await,
            Some(after) => transaction.query(&next, &[&owner, after, &chunk]).await,
        }
        .map_err(operational)?;
        if page.is_empty() {
            order_blobs(&mut blobs)?;
            return Ok(blobs);
        }
        for row in &page {
            let material = CaptureMaterial::Blob;
            let digest: String = row.try_get(0).map_err(|_| corrupt(material))?;
            validate_captured_digest(&digest)?;
            let stored: Vec<u8> = row.try_get(1).map_err(|_| corrupt(material))?;
            let count: i64 = row.try_get(2).map_err(|_| corrupt(material))?;
            let hash: Option<String> = row.try_get(3).map_err(|_| corrupt(material))?;
            let edition: i32 = row.try_get(4).map_err(|_| corrupt(material))?;
            // The accepted SQL complete-content validator, not a checksum column read back raw.
            let bytes = validate_stored_blob(stored, count, hash, i64::from(edition))
                .map_err(|_| corrupt(material))?;
            budget.admit_blob(bytes.len() as u64)?;
            after = Some(digest.clone());
            blobs.push(CapturedBlob { digest, bytes });
        }
    }
}

async fn read_rows(
    transaction: &Transaction<'_>,
    prefix: &str,
    tenant: &TenantId,
    specification: ProjectionSpec,
    budget: &mut CaptureBudget,
) -> Result<Vec<(String, Value)>, CaptureError> {
    let owner = tenant.as_str();
    let chunk = CHUNK;
    let table = projection_table(prefix, specification.name);
    let first =
        format!("SELECT row_key, body FROM {table} WHERE tenant_id = $1 ORDER BY row_key LIMIT $2");
    let next = format!(
        "SELECT row_key, body FROM {table}
         WHERE tenant_id = $1 AND row_key > $2 ORDER BY row_key LIMIT $3"
    );
    let mut rows: Vec<(String, Value)> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        // An empty key is a key a writer here can produce, so the first page has no predicate.
        let page = match &after {
            None => transaction.query(&first, &[&owner, &chunk]).await,
            Some(after) => transaction.query(&next, &[&owner, after, &chunk]).await,
        }
        .map_err(operational)?;
        if page.is_empty() {
            // Bytewise, in Rust: a database collation is a total order, but not this one.
            order_rows(&mut rows)?;
            return Ok(rows);
        }
        for row in &page {
            let material = CaptureMaterial::Projection;
            let key: String = row.try_get(0).map_err(|_| corrupt(material))?;
            let body: Value = row.try_get(1).map_err(|_| corrupt(material))?;
            budget.admit_projection_row(&body)?;
            after = Some(key.clone());
            rows.push((key, body));
        }
    }
}

/// Admit one requested projection against the registry *and* the actual stored schema.
///
/// `validate_projection` proves the same thing by creating a temporary expected table and
/// comparing catalogs against it. A read-only capture cannot: writing something to prove it does
/// not write is the contradiction, and the hosted application role is not given schema authority
/// anyway. So the expected shape is described in Rust — from the same list the create path builds
/// its DDL from — and compared against what the catalog actually reports.
pub(crate) async fn admit_projection(
    transaction: &Transaction<'_>,
    prefix: &str,
    specification: &ProjectionSpec,
) -> Result<(), CaptureError> {
    let refuse = |reason| CaptureError::ProjectionUnavailable {
        projection: specification.name.to_owned(),
        reason,
    };
    let declared = transaction
        .query_opt(
            &format!(
                "SELECT indexed_fields FROM {prefix}_projection_registry
                 WHERE projection_name = $1"
            ),
            &[&specification.name],
        )
        .await
        .map_err(operational)?;
    let Some(declared) = declared else {
        return Err(refuse(ProjectionCaptureRefusal::Undeclared));
    };
    let declared: Value = declared
        .try_get(0)
        .map_err(|_| refuse(ProjectionCaptureRefusal::DeclarationMismatch))?;
    if declared != serde_json::json!(specification.indexed) {
        return Err(refuse(ProjectionCaptureRefusal::DeclarationMismatch));
    }

    let table = projection_table(prefix, specification.name);
    let mismatch = || refuse(ProjectionCaptureRefusal::PhysicalShapeMismatch);
    let relation = transaction
        .query_opt(
            "SELECT c.relkind::text, c.relrowsecurity, c.relforcerowsecurity,
                    c.relpersistence::text, c.relispartition,
                    EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid),
                    EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
                      OR EXISTS(SELECT FROM pg_policy WHERE polrelid=c.oid)
                      OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=c.oid
                                AND (attgenerated<>'' OR attidentity<>''))
                      OR EXISTS(SELECT FROM pg_attribute a JOIN pg_collation l
                                ON l.oid=a.attcollation
                                WHERE a.attrelid=c.oid AND NOT l.collisdeterministic),
                    EXISTS(SELECT FROM pg_inherits
                           WHERE inhrelid=c.oid OR inhparent=c.oid)
             FROM pg_class c WHERE c.oid=to_regclass($1)",
            &[&table],
        )
        .await
        .map_err(operational)?;
    let Some(relation) = relation else {
        return Err(mismatch());
    };
    if relation.get::<_, String>(0) != "r"
        || relation.get::<_, bool>(1)
        || relation.get::<_, bool>(2)
        || relation.get::<_, String>(3) != "p"
        || relation.get::<_, bool>(4)
        || relation.get::<_, bool>(5)
        || relation.get::<_, bool>(6)
        || relation.get::<_, bool>(7)
    {
        return Err(mismatch());
    }

    // The column's collation decides which spellings of a key are one row. Anything but this
    // database's own default is a different table wearing this one's name.
    let columns = transaction
        .query(
            "SELECT a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull,
                    COALESCE(pg_get_expr(d.adbin,d.adrelid),''),
                    a.attcollation = 0 OR a.attcollation = (SELECT oid FROM pg_collation
                        WHERE collname='default' AND collnamespace='pg_catalog'::regnamespace)
             FROM pg_attribute a LEFT JOIN pg_attrdef d
                 ON a.attrelid=d.adrelid AND a.attnum=d.adnum
             WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&table],
        )
        .await
        .map_err(operational)?;
    let expected = schema::projection_columns(specification);
    if columns.len() != expected.len() {
        return Err(mismatch());
    }
    for (row, (name, kind, required)) in columns.iter().zip(&expected) {
        if row.get::<_, String>(0) != *name
            || row.get::<_, String>(1) != *kind
            || row.get::<_, bool>(2) != *required
            || !row.get::<_, String>(3).is_empty()
            || !row.get::<_, bool>(4)
        {
            return Err(mismatch());
        }
    }

    let constraints = transaction
        .query(
            "SELECT contype::text, pg_get_constraintdef(oid, true), convalidated
             FROM pg_constraint WHERE conrelid=to_regclass($1)",
            &[&table],
        )
        .await
        .map_err(operational)?;
    let [constraint] = constraints.as_slice() else {
        return Err(mismatch());
    };
    if constraint.get::<_, String>(0) != "p"
        || constraint.get::<_, String>(1) != "PRIMARY KEY (tenant_id, row_key)"
        || !constraint.get::<_, bool>(2)
    {
        return Err(mismatch());
    }

    let indexes = transaction
        .query(
            "SELECT pg_get_indexdef(indexrelid), indisvalid, indisready
             FROM pg_index WHERE indrelid=to_regclass($1)",
            &[&table],
        )
        .await
        .map_err(operational)?;
    let mut observed = Vec::with_capacity(indexes.len());
    for row in &indexes {
        let definition: String = row.get(0);
        let columns = definition
            .split_once(" USING ")
            .ok_or_else(mismatch)?
            .1
            .to_owned();
        observed.push((
            definition.starts_with("CREATE UNIQUE INDEX"),
            columns,
            row.get::<_, bool>(1),
            row.get::<_, bool>(2),
        ));
    }
    observed.sort();
    let mut wanted = vec![(true, "btree (tenant_id, row_key)".to_owned(), true, true)];
    for position in 0..specification.indexed.len() {
        wanted.push((
            false,
            format!("btree (tenant_id, idx_{position})"),
            true,
            true,
        ));
    }
    wanted.sort();
    if observed != wanted {
        return Err(mismatch());
    }

    // The same role profile ordinary projection admission requires; capture does not widen it.
    let allowed: bool = transaction
        .query_one(
            "SELECT COALESCE(has_table_privilege(current_user,to_regclass($1),'SELECT')
                    AND has_table_privilege(current_user,to_regclass($1),'INSERT')
                    AND has_table_privilege(current_user,to_regclass($1),'UPDATE')
                    AND has_table_privilege(current_user,to_regclass($1),'DELETE'),false)",
            &[&table],
        )
        .await
        .map_err(operational)?
        .get(0);
    if !allowed {
        return Err(mismatch());
    }
    Ok(())
}
