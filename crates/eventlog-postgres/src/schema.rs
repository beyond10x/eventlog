//! Additive, serialized schema admission; checks every column, default, constraint and index.
use super::backend;
use eventlog_core::{
    BlobMigrationReport, EventLogError, LegacyBlobMigration, ProjectionSpec, blob_integrity_sha256,
    validate_field, validate_legacy_blob_count, validate_stored_blob,
};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, GenericClient, Transaction};

const BASE: &[&str] = &[
    "events",
    "commands",
    "claims",
    "identity",
    "blobs",
    "projection_cursors",
    "snapshots",
];
const ADDED: &[&str] = &["scope_counters", "schema_version", "projection_registry"];
const SNAPSHOT_ADDED: &[&str] = &["snapshot_generations"];
const GROUP_ADDED: &[&str] = &["append_groups"];
const BATCH_ADDED: &[&str] = &["group_batches"];
const FLOOR_ADDED: &[&str] = &["settlement_floor"];
const LEGACY_CHECKSUM: &str = "68bf4ddeee1655b94ea9b30bab82641b898bdaaa413ce46e1e384697e6e24841";
const SNAPSHOT_CHECKSUM: &str = "f91f4eadc46ca90544177b2a3fdb8bd7b727400601effb8d274bc9ce2a8e6358";
const ATOMIC_GROUP_CHECKSUM: &str =
    "9d31a95af97bbdb3f675197383851f73758e1459166837171ef367c46530aa1f";
const BLOB_INTEGRITY_CHECKSUM: &str =
    "3d101321a8cc49dbca39a7d2305fb82dafe0550789fc87612f95dd232359ad0d";
const BATCH_CHECKSUM: &str = "f5989b20d7de8babbd77d3427015b92a70f5f6275982f740616476873d09a7fa";
const MD5_DIGEST_CHECKSUM: &str =
    "20d998c6771a5b4a4e03df9034ac4bab9479ae72ab908f2bae5ef045c7a6881c";
const HASH_EXPRESSION_CHECKSUM: &str =
    "41ca55d5a4e879d0928e0834e06f4503a01f5becde9712b891bde2c1b0eb282f";

fn legacy_base_ddl(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_events (
                 global_seq BIGSERIAL PRIMARY KEY,
                 committed_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version BIGINT NOT NULL,
                 event_id UUID NOT NULL,
                 event_name TEXT NOT NULL,
                 event_schema_version INTEGER NOT NULL,
                 occurred_at TIMESTAMPTZ NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 subject TEXT NOT NULL,
                 actor TEXT NOT NULL,
                 request_id TEXT NOT NULL,
                 trace_id TEXT NOT NULL,
                 causation_id TEXT,
                 causation_depth INTEGER NOT NULL DEFAULT 0,
                 redacted_at TIMESTAMPTZ,
                 data JSONB NOT NULL,
                 UNIQUE (tenant_id, stream_type, stream_id, version)
             );
             CREATE INDEX IF NOT EXISTS {prefix}_events_feed
                 ON {prefix}_events (tenant_id, global_seq);
             CREATE TABLE IF NOT EXISTS {prefix}_commands (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 request_hash TEXT NOT NULL,
                 first_version BIGINT NOT NULL,
                 last_version BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id, idempotency_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_claims (
                 tenant_id TEXT NOT NULL,
                 scope TEXT NOT NULL,
                 claim_key TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 first_version BIGINT NOT NULL,
                 last_version BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, scope, claim_key)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_identity (
                 tenant_id TEXT NOT NULL PRIMARY KEY,
                 stream_identity TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {prefix}_blobs (
                 tenant_id TEXT NOT NULL,
                 digest TEXT NOT NULL,
                 bytes BYTEA NOT NULL,
                 byte_count BIGINT NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, digest)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_projection_cursors (
                 projection TEXT NOT NULL,
                 tenant_id TEXT NOT NULL,
                 global_seq BIGINT NOT NULL,
                 updated_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (projection, tenant_id)
             );
             CREATE TABLE IF NOT EXISTS {prefix}_snapshots (
                 tenant_id TEXT NOT NULL,
                 stream_type TEXT NOT NULL,
                 stream_id TEXT NOT NULL,
                 version BIGINT NOT NULL,
                 state_schema_version INTEGER NOT NULL,
                 state JSONB NOT NULL,
                 recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY (tenant_id, stream_type, stream_id)
             );"
    )
}

fn base_ddl(prefix: &str) -> String {
    legacy_base_ddl(prefix).replace(
        "                 recorded_at TIMESTAMPTZ NOT NULL,\n                 PRIMARY KEY (tenant_id, digest)\n             );",
        "                 recorded_at TIMESTAMPTZ NOT NULL,\n                 integrity_sha256 TEXT,\n                 integrity_v1 INTEGER NOT NULL DEFAULT 1\n                     CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL),\n                 PRIMARY KEY (tenant_id, digest)\n             );",
    )
}

fn legacy_blob_ddl(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_blobs (
             tenant_id TEXT NOT NULL,
             digest TEXT NOT NULL,
             bytes BYTEA NOT NULL,
             byte_count BIGINT NOT NULL,
             recorded_at TIMESTAMPTZ NOT NULL,
             PRIMARY KEY (tenant_id, digest)
         );"
    )
}
fn additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_scope_counters (coordinate TEXT PRIMARY KEY, held BIGINT NOT NULL CHECK (held >= 0)); CREATE TABLE IF NOT EXISTS {prefix}_schema_version (version BIGINT PRIMARY KEY CHECK (version = 1), checksum TEXT NOT NULL); CREATE TABLE IF NOT EXISTS {prefix}_projection_registry(projection_name TEXT PRIMARY KEY,indexed_fields JSONB NOT NULL);"
    )
}
// Frozen original schema edition: existing ledger rows must remain migratable.
fn legacy_checksum() -> String {
    LEGACY_CHECKSUM.into()
}

fn snapshot_additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_snapshot_generations (tenant_id TEXT NOT NULL,stream_type TEXT NOT NULL,stream_id TEXT NOT NULL,generation UUID NOT NULL,cached_generation UUID,PRIMARY KEY(tenant_id,stream_type,stream_id));"
    )
}

fn snapshot_checksum() -> String {
    SNAPSHOT_CHECKSUM.into()
}

fn group_additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_append_groups (tenant_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,request_hash TEXT NOT NULL,ranges JSONB NOT NULL,PRIMARY KEY(tenant_id,idempotency_key));"
    )
}

/// The sorted digests a guarded blob-bearing group bound, beside its receipt: a retry carrying a
/// batch is the committed request only when it carries exactly these, whether or not each is
/// still bound.
fn batch_additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_group_batches (tenant_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,digests JSONB NOT NULL,PRIMARY KEY(tenant_id,idempotency_key));"
    )
}

/// The settlement floor: the highest position `migrate` recorded as settled after it found a
/// restored history. A logical restore copies each event's `committed_xid` from the source
/// cluster, and the feed would compare that foreign transaction id with this cluster's clock and
/// withhold the restored events, and every later one, until the clock passed it. Positions at or
/// below the highest floor count as settled; the watermark applies above it. One row per restore.
fn floor_additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_settlement_floor (settled_through BIGINT PRIMARY KEY CHECK (settled_through > 0),recorded_at TIMESTAMPTZ NOT NULL);"
    )
}

/// The feed's publication boundary — the first position whose transaction is not yet visible —
/// reads `committed_xid >= xmin`. Without this index that is a scan of the owner's whole history
/// for every feed page; with it, a range scan over the transactions still in flight.
fn watermark_index(prefix: &str) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {prefix}_events_publication ON {prefix}_events (committed_xid, global_seq);"
    )
}

/// Tenant erasure removes the counters whose coordinate starts with the tenant's prefix. The
/// primary key orders coordinates by the database collation, in which one prefix's strings need
/// not be contiguous; in byte order they are, so this index lets the erasure read only that range
/// instead of every counter of the owner while it holds the exclusive publication gate.
fn counter_index(prefix: &str) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {prefix}_scope_counters_bytes ON {prefix}_scope_counters ((coordinate COLLATE \"C\"));"
    )
}

/// Every table of the current edition, in admission order.
pub(crate) fn every_table() -> Vec<&'static str> {
    BASE.iter()
        .chain(ADDED)
        .chain(SNAPSHOT_ADDED)
        .chain(GROUP_ADDED)
        .chain(BATCH_ADDED)
        .chain(FLOOR_ADDED)
        .copied()
        .collect()
}

/// The DDL of the owner tables, without the projection tables' shape.
fn owner_ddl() -> String {
    format!(
        "{}{}{}{}{}{}{}{}",
        base_ddl("owner"),
        additions("owner"),
        snapshot_additions("owner"),
        group_additions("owner"),
        batch_additions("owner"),
        floor_additions("owner"),
        watermark_index("owner"),
        counter_index("owner")
    )
}

/// The current edition: the owner tables and the columns and indexes of a projection table. A
/// change to either is a new checksum, which every earlier binary refuses at `open` and `migrate`: the
/// cutover to a new projection shape is fenced by the ledger, not only by the per-table shape
/// check a binary meets when it registers a projection.
fn checksum() -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{}{}", owner_ddl(), projection_edition()))
    )
}

/// The batch edition, released in 0.10.0: the current owner tables, projection tables indexed
/// only by `(tenant_id, idx_N)`.
fn batch_checksum() -> String {
    BATCH_CHECKSUM.into()
}

/// The edition `main` wrote from pull request 70 until the digest became FIPS-safe, never
/// released: the current owner tables, the bytewise page index, and each field indexed by the
/// first 8 bytes of `md5(idx_N)`, which a PostgreSQL server in FIPS mode refuses to compute.
fn md5_digest_checksum() -> String {
    MD5_DIGEST_CHECKSUM.into()
}

/// The edition pull request 72 wrote, never released: the current owner tables, the bytewise
/// page index, and each field indexed by the expression `hashtextextended(idx_N, 0)`, with no
/// stored digest column.
fn hash_expression_checksum() -> String {
    HASH_EXPRESSION_CHECKSUM.into()
}

/// The edition with blob integrity columns and atomic groups, before the batch record.
fn blob_integrity_checksum() -> String {
    BLOB_INTEGRITY_CHECKSUM.into()
}

fn atomic_group_checksum() -> String {
    ATOMIC_GROUP_CHECKSUM.into()
}

async fn lock(transaction: &Transaction<'_>, prefix: &str) -> Result<(), EventLogError> {
    transaction.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':schema:' || $1, 0))", &[&prefix]).await.map_err(backend)?;
    Ok(())
}
async fn exists<C: GenericClient>(client: &C, table: &str) -> Result<bool, EventLogError> {
    Ok(client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
        .await
        .map_err(backend)?
        .get(0))
}
/// Each relation's physical shape, in the order asked, gathered with four catalog queries for the
/// whole list rather than seven per relation.
///
/// Every relation is judged exactly as one at a time would be — a missing relation has the empty
/// shape; then a rewrite rule, the relation kind and security, a trigger, policy, identity
/// column, generated column (unless `generated` admits it) or nondeterministic collation, and
/// inheritance each refuse in that order — and its refusal is returned in its own slot, so a
/// caller walking the list meets the same first error it met when each relation cost seven round
/// trips.
///
/// `Generated::Compared` admits a generated column, and the column's description then carries
/// its generation kind beside its expression, so the shape equals another only when both
/// generate the same column from the same expression. Only a projection table, compared with a
/// temporary table built from its declaration, has one (its stored value digests).
async fn shapes<C: GenericClient>(
    client: &C,
    tables: &[(String, &str)],
    generated: Generated,
) -> Result<Vec<Result<Vec<String>, EventLogError>>, EventLogError> {
    let names: Vec<&str> = tables.iter().map(|(table, _)| table.as_str()).collect();
    let mut relations: Vec<Option<tokio_postgres::Row>> = (0..tables.len()).map(|_| None).collect();
    for row in client.query("SELECT n.ord, c.relkind::text, c.relrowsecurity, c.relforcerowsecurity, c.relpersistence::text, c.relispartition, \
            EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid), \
            EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) OR EXISTS(SELECT FROM pg_policy WHERE polrelid=c.oid) OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=c.oid AND attidentity<>'') OR EXISTS(SELECT FROM pg_attribute a JOIN pg_collation co ON co.oid=a.attcollation WHERE a.attrelid=c.oid AND NOT co.collisdeterministic), \
            EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid), \
            EXISTS(SELECT FROM pg_attribute WHERE attrelid=c.oid AND attgenerated<>'') \
            FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) JOIN pg_class c ON c.oid=to_regclass(n.name)", &[&names]).await.map_err(backend)? {
        let position = ordinal_slot(row.get(0), tables.len())?;
        relations[position] = Some(row);
    }
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); tables.len()];
    for row in client.query("SELECT n.ord, a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull, COALESCE(pg_get_expr(d.adbin,d.adrelid),''), a.attcollation::text, a.attgenerated::text \
            FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) \
            JOIN pg_attribute a ON a.attrelid=to_regclass(n.name) \
            LEFT JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum \
            WHERE a.attnum>0 AND NOT a.attisdropped ORDER BY n.ord, a.attnum", &[&names]).await.map_err(backend)? {
        let position = ordinal_slot(row.get(0), tables.len())?;
        let prefix = tables[position].1;
        let default: String = row.get(4);
        let kind: String = row.get(6);
        // PostgreSQL qualifies sequences when the temporary expected schema shadows search_path.
        // A generation expression names only columns of its own row, and the owner prefix may be
        // spelled inside one of them, so it is kept as written.
        let default = if kind.is_empty() { default.replace(prefix, "OWNER").replace("pg_temp_", "") } else { default };
        let default = if default.starts_with("nextval(") { "owned-sequence".to_owned() } else { default };
        columns[position].push(format!("column:{}:{}:{}:{}:{}:{kind}", row.get::<_, String>(1), row.get::<_, String>(2), row.get::<_, bool>(3), default, row.get::<_, String>(5)));
    }
    let mut constraints: Vec<Vec<String>> = vec![Vec::new(); tables.len()];
    for row in client
        .query(
            "SELECT n.ord, co.contype::text, pg_get_constraintdef(co.oid, true), co.convalidated \
            FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) \
            JOIN pg_constraint co ON co.conrelid=to_regclass(n.name) \
            ORDER BY n.ord, co.contype, pg_get_constraintdef(co.oid, true)",
            &[&names],
        )
        .await
        .map_err(backend)?
    {
        let position = ordinal_slot(row.get(0), tables.len())?;
        constraints[position].push(format!(
            "constraint:{}:{}:{}",
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, bool>(3)
        ));
    }
    let mut indexes: Vec<Vec<Result<String, EventLogError>>> =
        (0..tables.len()).map(|_| Vec::new()).collect();
    for row in client
        .query(
            "SELECT n.ord, pg_get_indexdef(i.indexrelid), i.indisvalid, i.indisready \
            FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) \
            JOIN pg_index i ON i.indrelid=to_regclass(n.name)",
            &[&names],
        )
        .await
        .map_err(backend)?
    {
        let position = ordinal_slot(row.get(0), tables.len())?;
        let definition: String = row.get(1);
        let unique = definition.starts_with("CREATE UNIQUE INDEX");
        indexes[position].push(
            definition
                .split_once(" USING ")
                .map(|(_, columns)| {
                    format!(
                        "index:{unique}:{columns}:{}:{}",
                        row.get::<_, bool>(2),
                        row.get::<_, bool>(3)
                    )
                })
                .ok_or_else(|| {
                    EventLogError::Invalid("unrecognized PostgreSQL index definition".into())
                }),
        );
    }
    let mut answers = Vec::with_capacity(tables.len());
    for (((((table, prefix), relation), columns), constraints), indexes) in tables
        .iter()
        .zip(relations)
        .zip(columns)
        .zip(constraints)
        .zip(indexes)
    {
        answers.push(judge(
            table,
            prefix,
            relation.as_ref(),
            generated,
            columns,
            constraints,
            indexes,
        ));
    }
    Ok(answers)
}

/// Whether a relation may have a generated column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Generated {
    /// An owner table: a generated column is refused like a trigger.
    Refused,
    /// A projection table: a generated column is part of the compared shape.
    Compared,
}

/// One relation's admission, in the order a single relation was always judged.
fn judge(
    table: &str,
    prefix: &str,
    relation: Option<&tokio_postgres::Row>,
    generated: Generated,
    columns: Vec<String>,
    constraints: Vec<String>,
    indexes: Vec<Result<String, EventLogError>>,
) -> Result<Vec<String>, EventLogError> {
    let Some(relation) = relation else {
        return Ok(Vec::new());
    };
    if relation.get::<_, bool>(6) {
        return Err(EventLogError::Invalid(format!(
            "unadmitted rewrite rule: {table}"
        )));
    }
    if relation.get::<_, String>(1) != "r"
        || relation.get::<_, bool>(2)
        || relation.get::<_, bool>(3)
        || relation.get::<_, bool>(5)
        || relation.get::<_, String>(4)
            != if prefix == "eventlog_expected" {
                "t"
            } else {
                "p"
            }
    {
        return Err(EventLogError::Invalid(format!(
            "unsupported relation/security shape: {table}"
        )));
    }
    if relation.get::<_, bool>(7) || (generated == Generated::Refused && relation.get::<_, bool>(9))
    {
        return Err(EventLogError::Invalid(format!(
            "unadmitted trigger, policy or generated column: {table}"
        )));
    }
    if relation.get::<_, bool>(8) {
        return Err(EventLogError::Invalid(format!(
            "unsupported inherited relation: {table}"
        )));
    }
    let mut shape = columns;
    shape.extend(constraints);
    let mut indexes = indexes.into_iter().collect::<Result<Vec<_>, _>>()?;
    indexes.sort();
    shape.extend(indexes);
    Ok(shape)
}

/// The zero-based slot of a 1-based `WITH ORDINALITY` position.
fn ordinal_slot(ordinal: i64, len: usize) -> Result<usize, EventLogError> {
    usize::try_from(ordinal)
        .ok()
        .and_then(|ordinal| ordinal.checked_sub(1))
        .filter(|slot| *slot < len)
        .ok_or_else(|| EventLogError::Backend("catalog answered an unknown relation".into()))
}

async fn compare(
    transaction: &Transaction<'_>,
    prefix: &str,
    suffixes: &[&str],
) -> Result<(), EventLogError> {
    let mut tables = Vec::with_capacity(suffixes.len() * 2);
    for suffix in suffixes {
        tables.push((format!("{prefix}_{suffix}"), prefix));
        tables.push((format!("eventlog_expected_{suffix}"), "eventlog_expected"));
    }
    let mut shapes = shapes(transaction, &tables, Generated::Refused)
        .await?
        .into_iter();
    for suffix in suffixes {
        let table = format!("{prefix}_{suffix}");
        let (Some(actual), Some(expected)) = (shapes.next(), shapes.next()) else {
            return Err(EventLogError::Backend(
                "catalog answered too few relations".into(),
            ));
        };
        if actual? != expected? {
            return Err(EventLogError::Invalid(format!(
                "incompatible PostgreSQL schema: {table}; explicit supported migration required"
            )));
        }
        // A replaced sequence/default or user trigger is not part of the admitted durable shape.
        if *suffix == "events" {
            let row = transaction.query_one("SELECT EXISTS(SELECT FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum JOIN pg_depend dependency ON dependency.classid='pg_attrdef'::regclass AND dependency.objid=d.oid AND dependency.refclassid='pg_class'::regclass WHERE d.adrelid=to_regclass($1) AND a.attname='global_seq' AND dependency.refobjid=to_regclass(pg_get_serial_sequence($1,'global_seq')) AND pg_get_expr(d.adbin,d.adrelid)=format('nextval(%L::regclass)',to_regclass(pg_get_serial_sequence($1,'global_seq'))::text)) AND (SELECT count(*) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum JOIN pg_depend dependency ON dependency.classid='pg_attrdef'::regclass AND dependency.objid=d.oid AND dependency.refclassid='pg_class'::regclass WHERE d.adrelid=to_regclass($1) AND a.attname='global_seq' AND dependency.refobjid IN (SELECT oid FROM pg_class WHERE relkind='S'))=1, (SELECT COALESCE(bool_and(s.seqtypid='bigint'::regtype AND s.seqstart=1 AND s.seqincrement=1 AND s.seqmin=1 AND s.seqmax=9223372036854775807 AND s.seqcache=1 AND NOT s.seqcycle AND c.relpersistence='p'),false) FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid WHERE s.seqrelid=to_regclass(pg_get_serial_sequence($1,'global_seq')))", &[&table]).await.map_err(backend)?;
            let (owns, sequence_shape): (bool, bool) = (row.get(0), row.get(1));
            if !owns || !sequence_shape {
                return Err(EventLogError::Invalid(
                    "events sequence must retain its owned persistent BIGSERIAL shape with CACHE 1"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}
async fn expected(transaction: &Transaction<'_>) -> Result<(), EventLogError> {
    transaction
        .batch_execute(
            &format!(
                "{}{}{}{}{}{}{}{}",
                base_ddl("eventlog_expected"),
                additions("eventlog_expected"),
                snapshot_additions("eventlog_expected"),
                group_additions("eventlog_expected"),
                batch_additions("eventlog_expected"),
                floor_additions("eventlog_expected"),
                watermark_index("eventlog_expected"),
                counter_index("eventlog_expected")
            )
            .replace("CREATE TABLE IF NOT EXISTS", "CREATE TEMP TABLE"),
        )
        .await
        .map_err(backend)?;
    transaction
        .batch_execute(
            &legacy_blob_ddl("eventlog_expected_legacy")
                .replace("CREATE TABLE IF NOT EXISTS", "CREATE TEMP TABLE"),
        )
        .await
        .map_err(backend)
}
async fn drop_expected(transaction: &Transaction<'_>) -> Result<(), EventLogError> {
    let drops: String = BASE
        .iter()
        .chain(ADDED)
        .chain(SNAPSHOT_ADDED)
        .chain(GROUP_ADDED)
        .chain(BATCH_ADDED)
        .chain(FLOOR_ADDED)
        .chain(["legacy_blobs"].iter())
        .map(|suffix| format!("DROP TABLE pg_temp.eventlog_expected_{suffix};"))
        .collect::<Vec<_>>()
        .concat();
    transaction.batch_execute(&drops).await.map_err(backend)
}
async fn version(
    transaction: &Transaction<'_>,
    prefix: &str,
    expected_checksum: &str,
) -> Result<(), EventLogError> {
    version_among(transaction, prefix, &[expected_checksum]).await?;
    Ok(())
}

/// Which of `accepted` the ledger records; any other ledger is refused.
async fn version_among(
    transaction: &Transaction<'_>,
    prefix: &str,
    accepted: &[&str],
) -> Result<usize, EventLogError> {
    let rows = transaction
        .query(
            &format!("SELECT version,checksum FROM {prefix}_schema_version"),
            &[],
        )
        .await
        .map_err(backend)?;
    let recorded = match rows.as_slice() {
        [row] if row.get::<_, i64>(0) == 1 => row.get::<_, String>(1),
        _ => String::new(),
    };
    accepted
        .iter()
        .position(|checksum| *checksum == recorded)
        .ok_or_else(|| EventLogError::Invalid("unknown PostgreSQL schema version/checksum".into()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostgresBlobShape {
    Legacy,
    Current,
}

async fn blob_shape(
    transaction: &Transaction<'_>,
    prefix: &str,
) -> Result<PostgresBlobShape, EventLogError> {
    let mut shapes = shapes(
        transaction,
        &[
            (format!("{prefix}_blobs"), prefix),
            ("eventlog_expected_blobs".to_owned(), "eventlog_expected"),
            (
                "eventlog_expected_legacy_blobs".to_owned(),
                "eventlog_expected",
            ),
        ],
        Generated::Refused,
    )
    .await?
    .into_iter();
    let (Some(actual), Some(current), Some(legacy)) = (shapes.next(), shapes.next(), shapes.next())
    else {
        return Err(EventLogError::Backend(
            "catalog answered too few relations".into(),
        ));
    };
    let actual = actual?;
    if actual == current? {
        return Ok(PostgresBlobShape::Current);
    }
    if actual == legacy? {
        return Ok(PostgresBlobShape::Legacy);
    }
    Err(EventLogError::Invalid(format!(
        "incompatible PostgreSQL schema: {prefix}_blobs; explicit supported migration required"
    )))
}

async fn validate_all_postgres_blobs(
    transaction: &Transaction<'_>,
    prefix: &str,
) -> Result<(), EventLogError> {
    let table = format!("{prefix}_blobs");
    let mut after: Option<(String, String)> = None;
    loop {
        let row = if let Some((tenant, digest)) = &after {
            transaction
                .query_opt(
                    &format!(
                "SELECT tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1
                 FROM {table} WHERE tenant_id > $1 OR (tenant_id = $1 AND digest > $2)
                 ORDER BY tenant_id,digest LIMIT 1"
            ),
                    &[tenant, digest],
                )
                .await
        } else {
            transaction
                .query_opt(
                    &format!(
                "SELECT tenant_id,digest,bytes,byte_count,recorded_at,integrity_sha256,integrity_v1
                 FROM {table} ORDER BY tenant_id,digest LIMIT 1"
            ),
                    &[],
                )
                .await
        }
        .map_err(backend)?;
        let Some(row) = row else { break };
        let tenant: String = row.get(0);
        let digest: String = row.get(1);
        validate_field("stored blob tenant", &tenant)
            .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
        validate_field("stored blob digest", &digest)
            .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
        let _: time::OffsetDateTime = row.get(4);
        validate_stored_blob(
            row.get(2),
            row.get(3),
            row.get(5),
            i64::from(row.get::<_, i32>(6)),
        )?;
        after = Some((tenant, digest));
    }
    Ok(())
}

pub(crate) async fn migrate(
    client: &mut Client,
    prefix: &str,
    projections: &[ProjectionSpec],
) -> Result<(), EventLogError> {
    migrate_with_blob_migration(
        client,
        prefix,
        projections,
        LegacyBlobMigration::RefusePopulated,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn migrate_with_blob_migration(
    client: &mut Client,
    prefix: &str,
    projections: &[ProjectionSpec],
    migration: LegacyBlobMigration,
) -> Result<BlobMigrationReport, EventLogError> {
    let transaction = client.transaction().await.map_err(backend)?;
    lock(&transaction, prefix).await?;
    expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    let mut existing = 0;
    for suffix in BASE {
        existing += usize::from(exists(&transaction, &format!("{prefix}_{suffix}")).await?);
    }
    if existing != 0 && existing != BASE.len() {
        return Err(EventLogError::Invalid(format!(
            "partial or foreign PostgreSQL owner schema: {prefix}_events"
        )));
    }
    if existing != 0 {
        // The publication-boundary index is the one addition to an existing table; every edition
        // before it has the same events table without it. It is created here, in the migration
        // transaction, so the existing owner compares against the current shape and a refusal
        // below rolls the index back with everything else.
        transaction
            .batch_execute(&watermark_index(prefix))
            .await
            .map_err(backend)?;
        let unblobbed: Vec<&str> = BASE
            .iter()
            .copied()
            .filter(|suffix| *suffix != "blobs")
            .collect();
        compare(&transaction, prefix, &unblobbed).await?;
    }
    let ledger = exists(&transaction, &format!("{prefix}_schema_version")).await?;
    let generations = exists(&transaction, &format!("{prefix}_snapshot_generations")).await?;
    let groups = exists(&transaction, &format!("{prefix}_append_groups")).await?;
    let counters = exists(&transaction, &format!("{prefix}_scope_counters")).await?;
    let registry = exists(&transaction, &format!("{prefix}_projection_registry")).await?;
    let batches = exists(&transaction, &format!("{prefix}_group_batches")).await?;
    let floor = exists(&transaction, &format!("{prefix}_settlement_floor")).await?;
    if existing != 0 && counters {
        // As the publication index above: the counter table of every earlier edition is this
        // one without the index, so it gains it before it is compared.
        transaction
            .batch_execute(&counter_index(prefix))
            .await
            .map_err(backend)?;
    }
    if existing == 0
        && (ledger || generations || groups || counters || registry || batches || floor)
    {
        return Err(EventLogError::Invalid(
            "partial PostgreSQL admission migration".into(),
        ));
    }
    if groups && (!ledger || !generations) {
        return Err(EventLogError::Invalid(
            "partial PostgreSQL group migration".into(),
        ));
    }
    // A batch record without the receipts it sits beside would answer for groups nobody holds.
    if batches && !groups {
        return Err(EventLogError::Invalid(
            "partial PostgreSQL group batch migration".into(),
        ));
    }
    let prior_checksum = if groups {
        atomic_group_checksum()
    } else if generations {
        snapshot_checksum()
    } else {
        legacy_checksum()
    };
    let prior_blob_shape = if existing == 0 {
        None
    } else {
        Some(blob_shape(&transaction, prefix).await?)
    };
    // The recorded checksum of an earlier edition that differs from the current one only in its
    // projection tables' indexes, when the ledger holds one.
    let mut index_edition: Option<String> = None;
    if prior_blob_shape == Some(PostgresBlobShape::Current) {
        if !ledger || !generations || !groups {
            return Err(EventLogError::Invalid(
                "current PostgreSQL blob schema has an inconsistent ledger edition".into(),
            ));
        }
        compare(&transaction, prefix, ADDED).await?;
        compare(&transaction, prefix, SNAPSHOT_ADDED).await?;
        compare(&transaction, prefix, GROUP_ADDED).await?;
        if batches {
            compare(&transaction, prefix, BATCH_ADDED).await?;
            // The 0.10.0 edition, `main`'s MD5 digest edition and pull request 72's expression
            // edition differ from the current one only in their projection tables' indexes and
            // digest columns, which `upgrade_registered_projections` moves below.
            let editions = [
                checksum(),
                batch_checksum(),
                md5_digest_checksum(),
                hash_expression_checksum(),
            ];
            let recorded = version_among(
                &transaction,
                prefix,
                &editions.iter().map(String::as_str).collect::<Vec<_>>(),
            )
            .await?;
            index_edition = (recorded != 0).then(|| editions[recorded].clone());
        } else {
            version(&transaction, prefix, &blob_integrity_checksum()).await?;
        }
    } else if ledger {
        compare(&transaction, prefix, ADDED).await?;
        if generations {
            compare(&transaction, prefix, SNAPSHOT_ADDED).await?;
            if groups {
                compare(&transaction, prefix, GROUP_ADDED).await?;
            }
            if batches {
                compare(&transaction, prefix, BATCH_ADDED).await?;
            }
            version(&transaction, prefix, &prior_checksum).await?;
        } else {
            version(&transaction, prefix, &legacy_checksum()).await?;
        }
    } else if generations || counters || registry || groups {
        return Err(EventLogError::Invalid(
            "partial PostgreSQL admission migration".into(),
        ));
    }
    let mut report = BlobMigrationReport::default();
    if prior_blob_shape == Some(PostgresBlobShape::Legacy) {
        let table = format!("{prefix}_blobs");
        transaction
            .batch_execute(&format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"))
            .await
            .map_err(backend)?;
        let rows: i64 = transaction
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .map_err(backend)?
            .get(0);
        let rows = u64::try_from(rows)
            .map_err(|_| EventLogError::Backend("stored blob row count is invalid".into()))?;
        if rows != 0 && migration == LegacyBlobMigration::RefusePopulated {
            return Err(EventLogError::Invalid(format!(
                "populated predecessor blob table has {rows} rows; explicit TrustObservedBytes migration required"
            )));
        }
        transaction
            .batch_execute(&format!(
                "ALTER TABLE {table} ADD COLUMN integrity_sha256 TEXT"
            ))
            .await
            .map_err(backend)?;
        let mut after: Option<(String, String)> = None;
        loop {
            let row = if let Some((tenant, digest)) = &after {
                transaction
                    .query_opt(
                        &format!(
                            "SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {table}
                     WHERE tenant_id > $1 OR (tenant_id = $1 AND digest > $2)
                     ORDER BY tenant_id,digest LIMIT 1"
                        ),
                        &[tenant, digest],
                    )
                    .await
            } else {
                transaction
                    .query_opt(
                        &format!(
                            "SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {table}
                     ORDER BY tenant_id,digest LIMIT 1"
                        ),
                        &[],
                    )
                    .await
            }
            .map_err(backend)?;
            let Some(row) = row else { break };
            let tenant: String = row.get(0);
            let digest: String = row.get(1);
            let bytes: Vec<u8> = row.get(2);
            let count: i64 = row.get(3);
            let _: time::OffsetDateTime = row.get(4);
            validate_field("stored blob tenant", &tenant)
                .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
            validate_field("stored blob digest", &digest)
                .map_err(|_| EventLogError::Backend("stored blob identity is invalid".into()))?;
            validate_legacy_blob_count(&bytes, count)?;
            let hash = blob_integrity_sha256(&bytes);
            transaction
                .execute(
                    &format!(
                        "UPDATE {table} SET integrity_sha256=$1 WHERE tenant_id=$2 AND digest=$3"
                    ),
                    &[&hash, &tenant, &digest],
                )
                .await
                .map_err(backend)?;
            after = Some((tenant, digest));
        }
        transaction
            .batch_execute(&format!(
                "ALTER TABLE {table} ADD COLUMN integrity_v1 INTEGER NOT NULL DEFAULT 1
                 CHECK (integrity_v1 = 1 AND integrity_sha256 IS NOT NULL)"
            ))
            .await
            .map_err(backend)?;
        report = BlobMigrationReport {
            upgraded: true,
            trusted_legacy_rows: rows,
        };
    } else if prior_blob_shape.is_none() {
        report.upgraded = true;
    }

    transaction
        .batch_execute(&format!(
            "{}{}{}{}{}{}{}{}",
            base_ddl(prefix),
            additions(prefix),
            snapshot_additions(prefix),
            group_additions(prefix),
            batch_additions(prefix),
            floor_additions(prefix),
            watermark_index(prefix),
            counter_index(prefix)
        ))
        .await
        .map_err(backend)?;
    if !ledger {
        transaction
            .execute(
                &format!("INSERT INTO {prefix}_schema_version (version,checksum) VALUES (1,$1)"),
                &[&checksum()],
            )
            .await
            .map_err(backend)?;
    } else if prior_blob_shape == Some(PostgresBlobShape::Legacy)
        || (prior_blob_shape == Some(PostgresBlobShape::Current)
            && (!batches || index_edition.is_some()))
    {
        let prior_checksum = if let Some(recorded) = index_edition {
            recorded
        } else if prior_blob_shape == Some(PostgresBlobShape::Current) {
            blob_integrity_checksum()
        } else {
            prior_checksum
        };
        upgrade_registered_projections(&transaction, prefix).await?;
        transaction
            .execute(
                &format!(
                    "UPDATE {prefix}_schema_version SET checksum=$1 WHERE version=1 AND checksum=$2"
                ),
                &[&checksum(), &prior_checksum],
            )
            .await
            .map_err(backend)?;
    }
    for spec in projections {
        create_projection(&transaction, prefix, spec).await?;
        let indexed = serde_json::to_value(spec.indexed)
            .map_err(|_| EventLogError::Invalid("invalid projection shape".into()))?;
        transaction.execute(&format!("INSERT INTO {prefix}_projection_registry(projection_name,indexed_fields) VALUES($1,$2) ON CONFLICT(projection_name) DO NOTHING"), &[&spec.name,&indexed]).await.map_err(backend)?;
        let recorded:serde_json::Value=transaction.query_one(&format!("SELECT indexed_fields FROM {prefix}_projection_registry WHERE projection_name=$1"), &[&spec.name]).await.map_err(backend)?.get(0);
        if recorded != indexed {
            return Err(EventLogError::Invalid(
                "projection registration shape changed without a supported migration".into(),
            ));
        }
    }
    compare(&transaction, prefix, &every_table()).await?;
    version(&transaction, prefix, &checksum()).await?;
    if report.upgraded {
        validate_all_postgres_blobs(&transaction, prefix).await?;
    }
    drop_expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    transaction
        .commit()
        .await
        .map_err(|_| EventLogError::BlobMigrationCommitUnknown)?;
    Ok(report)
}
pub(crate) async fn validate(client: &mut Client, prefix: &str) -> Result<(), EventLogError> {
    let transaction = client.transaction().await.map_err(backend)?;
    lock(&transaction, prefix).await?;
    expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    compare(&transaction, prefix, &every_table()).await?;
    version(&transaction, prefix, &checksum()).await?;
    drop_expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    transaction.commit().await.map_err(backend)
}
/// The migration metadata: the schema ledger, the projection roster and the settlement floor.
/// Only the migration role writes them; hosted traffic reads the ledger at admission, the roster
/// at registration and erasure, and the floor in every feed. An application role that could write
/// the roster could remove a projection from it, and erasure, which deletes a tenant from every
/// registered projection, would then leave that tenant's rows behind in the unregistered table.
/// One that could raise the floor could declare an event still in flight settled, and a feed
/// cursor would pass it.
const MIGRATION_METADATA: &[&str] = &["schema_version", "projection_registry", "settlement_floor"];

pub(crate) async fn permissions(client: &Client, prefix: &str) -> Result<(), EventLogError> {
    let durable: Vec<_> = BASE
        .iter()
        .chain(ADDED)
        .chain(SNAPSHOT_ADDED)
        .chain(GROUP_ADDED)
        .chain(BATCH_ADDED)
        .chain(FLOOR_ADDED)
        .filter(|suffix| !MIGRATION_METADATA.contains(suffix))
        .map(|suffix| format!("{prefix}_{suffix}"))
        .collect();
    let metadata: Vec<_> = MIGRATION_METADATA
        .iter()
        .map(|suffix| format!("{prefix}_{suffix}"))
        .collect();
    let admitted:bool=client.query_one("SELECT COALESCE(bool_and(has_table_privilege(current_user,to_regclass(name),'SELECT') AND has_table_privilege(current_user,to_regclass(name),'INSERT') AND has_table_privilege(current_user,to_regclass(name),'UPDATE') AND has_table_privilege(current_user,to_regclass(name),'DELETE')),false) FROM unnest($1::text[]) AS name",&[&durable]).await.map_err(backend)?.get(0);
    let sequence = format!("{prefix}_events");
    let sequence_admitted:bool=client.query_one("SELECT COALESCE(has_sequence_privilege(current_user,to_regclass(pg_get_serial_sequence($1,'global_seq')),'USAGE'),false) AND has_database_privilege(current_user,current_database(),'TEMP')",&[&sequence]).await.map_err(backend)?.get(0);
    if !admitted || !sequence_admitted {
        return Err(EventLogError::Invalid("hosted application role lacks required durable DML, sequence usage or temporary-table rights".into()));
    }
    let readable:bool=client.query_one("SELECT COALESCE(bool_and(has_table_privilege(current_user,to_regclass(name),'SELECT')),false) FROM unnest($1::text[]) AS name",&[&metadata]).await.map_err(backend)?.get(0);
    if !readable {
        return Err(EventLogError::Invalid(
            "hosted application role cannot read the migration metadata".into(),
        ));
    }
    let writable:bool=client.query_one("SELECT COALESCE(bool_or(has_table_privilege(current_user,to_regclass(name),'INSERT') OR has_table_privilege(current_user,to_regclass(name),'UPDATE') OR has_table_privilege(current_user,to_regclass(name),'DELETE') OR has_table_privilege(current_user,to_regclass(name),'TRUNCATE')),true) FROM unnest($1::text[]) AS name",&[&metadata]).await.map_err(backend)?.get(0);
    if writable {
        return Err(EventLogError::Invalid(
            "hosted application role can write migration metadata reserved for the migration role"
                .into(),
        ));
    }
    Ok(())
}
/// One column of a projection table, named and typed the way the catalog reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionColumn {
    pub(crate) name: String,
    pub(crate) kind: &'static str,
    pub(crate) required: bool,
    /// The expression a stored generated column is computed from, as `pg_get_expr` prints it.
    pub(crate) generated: Option<String>,
}

impl ProjectionColumn {
    fn plain(name: String, kind: &'static str, required: bool) -> Self {
        Self {
            name,
            kind,
            required,
            generated: None,
        }
    }

    /// The column's declaration in `CREATE TABLE` and `ALTER TABLE ... ADD COLUMN`.
    fn declaration(&self) -> String {
        let required = if self.required { " NOT NULL" } else { "" };
        let generated = self
            .generated
            .as_ref()
            .map(|expression| format!(" GENERATED ALWAYS AS ({expression}) STORED"))
            .unwrap_or_default();
        format!("{} {}{required}{generated}", self.name, self.kind)
    }
}

/// Every column a projection table with `fields` declared fields has, in order: the key, the
/// body, each field's value, then each field's stored digest.
///
/// The DDL below is built from this, projection admission compares a stored table against a
/// temporary one built from it, read-only capture admission compares the catalog against the
/// same list, and the ledger checksum covers it. One description, so the shape that is created and
/// the shape that is recognised cannot drift apart without a compiler error between them. The
/// digests follow every value, so a table `migrate` extends with `ADD COLUMN` has the column order
/// of a fresh one.
pub(crate) fn projection_columns(fields: usize) -> Vec<ProjectionColumn> {
    let mut columns = vec![
        ProjectionColumn::plain("tenant_id".to_owned(), "text", true),
        ProjectionColumn::plain("row_key".to_owned(), "text", true),
        ProjectionColumn::plain("body".to_owned(), "jsonb", true),
    ];
    for position in 0..fields {
        columns.push(ProjectionColumn::plain(
            format!("idx_{position}"),
            "text",
            false,
        ));
    }
    for position in 0..fields {
        columns.push(ProjectionColumn {
            name: digest_column(position),
            kind: "bigint",
            required: false,
            generated: Some(indexed_digest(&format!("idx_{position}"))),
        });
    }
    columns
}

/// The stored digest of the `position`-th declared field's value.
pub(crate) fn digest_column(position: usize) -> String {
    format!("idx_{position}_digest")
}

/// The columns a projection row's writer and a rebuild's copy name: every column but the
/// generated digests, which the server computes and refuses to be given.
pub(crate) fn projection_value_columns(fields: usize) -> String {
    projection_columns(fields)
        .into_iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The one name a declared field's index is created under.
///
/// A name that fits PostgreSQL's 63-byte identifier limit is the table name and the field's
/// position, as it always was. One that would not fit keeps as much of the table name as fits
/// beside a digest of the whole table name and the position: PostgreSQL would otherwise truncate
/// the position away, so every field of a long table would share one name and `IF NOT EXISTS`
/// would create only the first index.
pub(crate) fn projection_index(table: &str, position: usize) -> String {
    bounded_index_name(table, &format!("idx_{position}"))
}

/// `{table}_{role}`, or, past 63 bytes, as much of the table name as fits beside a digest of it.
fn bounded_index_name(table: &str, role: &str) -> String {
    let plain = format!("{table}_{role}");
    if plain.len() <= 63 {
        return plain;
    }
    let digest = format!("{:x}", Sha256::digest(table.as_bytes()));
    let suffix = format!("_{}_{role}", &digest[..8]);
    let kept = table
        .char_indices()
        .map(|(at, _)| at)
        .take_while(|at| at + suffix.len() <= 63)
        .last()
        .unwrap_or(0);
    format!("{}{suffix}", &table[..kept])
}

/// Every secondary index of a projection table with `fields` indexed fields: its name and its
/// columns, written exactly as `pg_get_indexdef` prints the part after `USING`.
///
/// One list: `migrate` creates these, projection admission compares a stored table against a
/// temporary one built from them, read-only capture admission compares the catalog against the
/// same strings, and the ledger checksum covers them, so none of the four can change alone.
///
/// - `(tenant_id, row_key COLLATE "C")` walks a page in byte order (`projection_page_sql`). The
///   primary key orders keys under the database collation, which on a non-C database is not
///   the order the shared pager reads; without this index each page sorted the tenant's rows.
/// - `(tenant_id, idx_N_digest)` finds the candidates for a declared field's value
///   (`projection_find_filter`).
pub(crate) fn projection_indexes(table: &str, fields: usize) -> Vec<(String, String)> {
    let mut indexes = vec![(
        bounded_index_name(table, "key_c"),
        "btree (tenant_id, row_key COLLATE \"C\")".to_owned(),
    )];
    for position in 0..fields {
        indexes.push((
            projection_index(table, position),
            format!("btree (tenant_id, {})", digest_column(position)),
        ));
    }
    indexes
}

/// The indexes of earlier editions that `migrate` drops: 0.10.0 and before indexed each field's
/// value itself, `(tenant_id, idx_N)`, and a value too long for a B-tree entry aborted the append
/// that wrote it; `main` after pull request 70 indexed the first 8 bytes of `md5(idx_N)`, which a
/// server in FIPS mode refuses to compute; pull request 72 indexed the expression
/// `hashtextextended(idx_N, 0)`, which the planner matched on every find.
fn retired_projection_indexes(fields: usize) -> Vec<String> {
    (0..fields)
        .flat_map(|position| {
            [
                format!("btree (tenant_id, idx_{position})"),
                format!(
                    "btree (tenant_id, substr(decode(md5(idx_{position}), 'hex'::text), 1, 8))"
                ),
                format!("btree (tenant_id, hashtextextended(idx_{position}, '0'::bigint))"),
            ]
        })
        .collect()
}

/// A fixed-size digest of an indexed value, as SQL over `value`: its 64-bit
/// `hashtextextended(value, 0)`, written the way `pg_get_expr` prints a generation expression.
///
/// Each declared field's stored digest column is generated from this, and its index keys on
/// that column instead of the value, so every entry has one size whatever the value's length:
/// PostgreSQL refuses a B-tree entry over about a third of a page, and a long indexed value
/// aborted the append that wrote it (review L78). The server computes it from the stored value,
/// so no writer can store a digest that does not match. Two values can share the digest, so
/// every row a lookup finds through it is checked against the value itself
/// (`projection_find_filter`).
///
/// `hashtextextended` is PostgreSQL's own text hash, the one hash partitioning stores: immutable,
/// as a generation expression must be, and kept stable across releases for that reason. Under a
/// deterministic collation, which admission requires of every column, it hashes the value's
/// bytes. It runs no OpenSSL digest, so a server in FIPS mode computes it; that server refuses
/// `md5()`, which an earlier digest called.
pub(crate) fn indexed_digest(value: &str) -> String {
    format!("hashtextextended({value}, (0)::bigint)")
}

/// The candidate rows of tenant `$1` for the value `$2` of the `position`-th declared field, as a
/// `WHERE` clause: the rows whose stored digest equals the value's.
///
/// A candidate can hold another value with the same digest, so the caller compares each row's
/// `idx_N` with the value. When one differs, the find runs again with
/// `projection_find_checked_filter`, whose rows all hold the value: the first answer may be short,
/// since a colliding row took one of the `LIMIT` places. The planner reads the parameter's digest
/// as a constant and the column as a plain `bigint`: no expression to match while planning, and no
/// value comparison while scanning (KPI `postgres.projection_find.latency`).
pub(crate) fn projection_find_filter(position: usize) -> String {
    format!(
        "tenant_id = $1 AND {} = {}",
        digest_column(position),
        indexed_digest("$2::text")
    )
}

/// `projection_find_filter` with the value comparison in SQL: the rows that hold `$2`.
pub(crate) fn projection_find_checked_filter(position: usize) -> String {
    format!(
        "{} AND idx_{position} = $2",
        projection_find_filter(position)
    )
}

/// The projection shape the ledger checksum covers: the columns and the index columns of a
/// one-field table.
fn projection_edition() -> String {
    projection_columns(1)
        .iter()
        .map(ProjectionColumn::declaration)
        .chain(
            projection_indexes("owner_p_projection", 1)
                .into_iter()
                .map(|(_, columns)| columns),
        )
        .collect::<Vec<_>>()
        .join(";")
}

pub(crate) async fn create_projection(
    transaction: &Transaction<'_>,
    prefix: &str,
    spec: &ProjectionSpec,
) -> Result<(), EventLogError> {
    spec.validate()?;
    let table = super::projection_table(prefix, spec.name);
    if table.len() > 63 {
        return Err(EventLogError::Invalid(
            "projection SQL identifier exceeds PostgreSQL's exact length limit".into(),
        ));
    }
    let columns = projection_columns(spec.indexed.len())
        .iter()
        .map(ProjectionColumn::declaration)
        .collect::<Vec<_>>()
        .join(",");
    transaction
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {table} ({columns},PRIMARY KEY(tenant_id,row_key));"
        ))
        .await
        .map_err(backend)?;
    install_projection_indexes(transaction, &table, spec.indexed.len()).await?;
    // Migration admits existing projection tables too: the stored table must have exactly the
    // shape a fresh one would, and IF NOT EXISTS alone does not refuse hidden behavior that can
    // discard projection writes before registration.
    compare_projection(transaction, prefix, &table, spec.indexed.len()).await
}

/// Drop every index of `retired_projection_indexes` the stored table has, add every stored
/// digest column it lacks, then create every index of `projection_indexes` it lacks.
///
/// An index is matched by its definition, whatever its name: an existing table may hold one
/// created under an older, truncated name, and creating it again would leave two where the
/// admitted shape has one. The retired indexes go first, since a current one takes the same
/// name. The digest columns are added in one `ALTER TABLE`, one rewrite of the table, and only
/// beside a value column that exists: a table without it is refused by the comparison that
/// follows. All of it runs inside the migration transaction, which holds the table against
/// reads and writes until it commits.
async fn install_projection_indexes(
    transaction: &Transaction<'_>,
    table: &str,
    fields: usize,
) -> Result<(), EventLogError> {
    let installed: Vec<(String, String)> = transaction
        .query(
            "SELECT indexrelid::regclass::text, pg_get_indexdef(indexrelid)
             FROM pg_index WHERE indrelid=to_regclass($1)",
            &[&table],
        )
        .await
        .map_err(backend)?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let has = |columns: &str| {
        let definition = format!(" USING {columns}");
        installed
            .iter()
            .find(|(_, stored)| stored.ends_with(&definition))
            .map(|(name, _)| name.clone())
    };
    let present: Vec<String> = transaction
        .query(
            "SELECT attname::text FROM pg_attribute
             WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped",
            &[&table],
        )
        .await
        .map_err(backend)?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let retired = retired_projection_indexes(fields)
        .into_iter()
        .filter_map(|columns| has(&columns))
        .map(|index| format!("DROP INDEX {index};"));
    let added: Vec<String> = (0..fields)
        .filter(|position| {
            present.contains(&format!("idx_{position}"))
                && !present.contains(&digest_column(*position))
        })
        .filter_map(|position| {
            projection_columns(fields)
                .into_iter()
                .find(|column| column.name == digest_column(position))
        })
        .map(|column| format!("ADD COLUMN {}", column.declaration()))
        .collect();
    let added = (!added.is_empty())
        .then(|| format!("ALTER TABLE {table} {};", added.join(", ")))
        .into_iter();
    let missing = projection_indexes(table, fields)
        .into_iter()
        .filter(|(_, columns)| has(columns).is_none())
        .map(|(index, columns)| format!("CREATE INDEX {index} ON {table} USING {columns};"));
    let statements = retired
        .chain(added)
        .chain(missing)
        .collect::<Vec<_>>()
        .concat();
    if !statements.is_empty() {
        transaction
            .batch_execute(&statements)
            .await
            .map_err(backend)?;
    }
    Ok(())
}

/// Move every registered projection table to the current edition's indexes.
///
/// Runs when `migrate` rewrites the ledger from an earlier edition: the new checksum admits only
/// projection tables of the new shape, and a table `migrate` was not given this time is still
/// registered and still read. A registered table that does not exist yet is created by its own
/// registration, in the current shape.
async fn upgrade_registered_projections(
    transaction: &Transaction<'_>,
    prefix: &str,
) -> Result<(), EventLogError> {
    let registered = transaction
        .query(
            &format!(
                "SELECT projection_name, indexed_fields FROM {prefix}_projection_registry
                 ORDER BY projection_name"
            ),
            &[],
        )
        .await
        .map_err(backend)?;
    for row in registered {
        let name: String = row.get(0);
        let fields: serde_json::Value = row.get(1);
        // The name is spliced into DDL: it must be what a declaration could have registered.
        eventlog_core::validate_identifier("registered projection name", &name)?;
        let Some(fields) = fields.as_array() else {
            return Err(EventLogError::Invalid(format!(
                "projection registry holds no field list for {name}"
            )));
        };
        let table = super::projection_table(prefix, &name);
        if !exists(transaction, &table).await? {
            continue;
        }
        install_projection_indexes(transaction, &table, fields.len()).await?;
        compare_projection(transaction, prefix, &table, fields.len()).await?;
    }
    Ok(())
}

/// Refuse a stored projection table whose physical shape differs from the declared one, compared
/// against a temporary table made from the same declaration: the application role needs TEMP, not
/// owner-schema CREATE.
async fn compare_projection(
    transaction: &Transaction<'_>,
    prefix: &str,
    table: &str,
    fields: usize,
) -> Result<(), EventLogError> {
    let columns = projection_columns(fields)
        .iter()
        .map(ProjectionColumn::declaration)
        .collect::<Vec<_>>()
        .join(",");
    let indexes = projection_indexes("eventlog_expected_projection", fields)
        .into_iter()
        .map(|(_, columns)| {
            format!("CREATE INDEX ON eventlog_expected_projection USING {columns};")
        })
        .collect::<Vec<_>>()
        .concat();
    transaction.batch_execute(&format!("CREATE TEMP TABLE eventlog_expected_projection ({columns},PRIMARY KEY(tenant_id,row_key));{indexes}")).await.map_err(backend)?;
    let answered = shapes(
        transaction,
        &[
            (table.to_owned(), prefix),
            (
                "eventlog_expected_projection".to_owned(),
                "eventlog_expected",
            ),
        ],
        Generated::Compared,
    )
    .await;
    transaction
        .batch_execute("DROP TABLE pg_temp.eventlog_expected_projection")
        .await
        .map_err(backend)?;
    let mut answered = answered?.into_iter();
    let (Some(actual), Some(expected)) = (answered.next(), answered.next()) else {
        return Err(EventLogError::Backend(
            "catalog answered too few relations".into(),
        ));
    };
    if actual? != expected? {
        return Err(EventLogError::Invalid(format!(
            "incompatible PostgreSQL projection: {table}"
        )));
    }
    Ok(())
}

pub(crate) async fn validate_projection(
    client: &mut Client,
    prefix: &str,
    spec: &ProjectionSpec,
) -> Result<(), EventLogError> {
    spec.validate()?;
    let transaction = client.transaction().await.map_err(backend)?;
    let recorded = transaction
        .query_opt(
            &format!(
                "SELECT indexed_fields FROM {prefix}_projection_registry WHERE projection_name=$1"
            ),
            &[&spec.name],
        )
        .await
        .map_err(backend)?;
    if recorded.map(|row| row.get::<_, serde_json::Value>(0))
        != Some(serde_json::json!(spec.indexed))
    {
        return Err(EventLogError::Invalid(
            "projection is absent from the explicit migration roster or indexed paths changed"
                .into(),
        ));
    }
    let table = super::projection_table(prefix, spec.name);
    let allowed:bool=transaction.query_one("SELECT COALESCE(has_table_privilege(current_user,to_regclass($1),'SELECT') AND has_table_privilege(current_user,to_regclass($1),'INSERT') AND has_table_privilege(current_user,to_regclass($1),'UPDATE') AND has_table_privilege(current_user,to_regclass($1),'DELETE'),false)",&[&table]).await.map_err(backend)?.get(0);
    if !allowed {
        return Err(EventLogError::Invalid(
            "declared projection lacks required application DML permissions".into(),
        ));
    }
    compare_projection(&transaction, prefix, &table, spec.indexed.len()).await?;
    transaction.commit().await.map_err(backend)
}

#[cfg(test)]
async fn downgrade_blob_fixture(client: &Client, prefix: &str) {
    client
        .batch_execute(&format!(
            "BEGIN;
             ALTER TABLE {prefix}_blobs RENAME TO {prefix}_blobs_current;
             CREATE TABLE {prefix}_blobs (
                 tenant_id TEXT NOT NULL,digest TEXT NOT NULL,bytes BYTEA NOT NULL,
                 byte_count BIGINT NOT NULL,recorded_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY(tenant_id,digest));
             INSERT INTO {prefix}_blobs(tenant_id,digest,bytes,byte_count,recorded_at)
                 SELECT tenant_id,digest,bytes,byte_count,recorded_at FROM {prefix}_blobs_current;
             DROP TABLE {prefix}_blobs_current;
             COMMIT;"
        ))
        .await
        .unwrap();
}

#[cfg(test)]
mod blob_integrity_tests {
    use super::{
        ATOMIC_GROUP_CHECKSUM, BATCH_CHECKSUM, BLOB_INTEGRITY_CHECKSUM, HASH_EXPRESSION_CHECKSUM,
        LEGACY_CHECKSUM, MD5_DIGEST_CHECKSUM, SNAPSHOT_CHECKSUM, additions, atomic_group_checksum,
        base_ddl, batch_checksum, blob_integrity_checksum, checksum, group_additions,
        hash_expression_checksum, legacy_checksum, md5_digest_checksum, owner_ddl,
        snapshot_additions, snapshot_checksum,
    };
    use sha2::{Digest, Sha256};

    #[test]
    fn historical_editions_and_old_reader_are_frozen() {
        assert_eq!(legacy_checksum(), LEGACY_CHECKSUM);
        assert_eq!(snapshot_checksum(), SNAPSHOT_CHECKSUM);
        assert_eq!(atomic_group_checksum(), ATOMIC_GROUP_CHECKSUM);
        assert_eq!(blob_integrity_checksum(), BLOB_INTEGRITY_CHECKSUM);
        // The blob integrity edition is the current DDL without the batch record.
        let blob_integrity = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}{}{}{}",
                base_ddl("owner"),
                additions("owner"),
                snapshot_additions("owner"),
                group_additions("owner")
            ))
        );
        assert_eq!(blob_integrity, BLOB_INTEGRITY_CHECKSUM);
        assert_ne!(checksum(), ATOMIC_GROUP_CHECKSUM);
        assert_ne!(checksum(), BLOB_INTEGRITY_CHECKSUM);
        // The batch edition (0.10.0) is the current owner DDL without the projection edition.
        assert_eq!(batch_checksum(), BATCH_CHECKSUM);
        assert_eq!(format!("{:x}", Sha256::digest(owner_ddl())), BATCH_CHECKSUM);
        assert_ne!(checksum(), BATCH_CHECKSUM);
        // `main`'s unreleased MD5 digest edition: the current owner DDL and the MD5 projection index.
        assert_eq!(md5_digest_checksum(), MD5_DIGEST_CHECKSUM);
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(format!(
                    "{}btree (tenant_id, row_key COLLATE \"C\");btree (tenant_id, substr(decode(md5(idx_0), 'hex'::text), 1, 8))",
                    owner_ddl()
                ))
            ),
            MD5_DIGEST_CHECKSUM
        );
        assert_ne!(checksum(), MD5_DIGEST_CHECKSUM);
        // Pull request 72's unreleased edition: the current owner DDL, the page index and the
        // `hashtextextended(idx_0, 0)` expression index, no stored digest column.
        assert_eq!(hash_expression_checksum(), HASH_EXPRESSION_CHECKSUM);
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(format!(
                    "{}btree (tenant_id, row_key COLLATE \"C\");btree (tenant_id, hashtextextended(idx_0, '0'::bigint))",
                    owner_ddl()
                ))
            ),
            HASH_EXPRESSION_CHECKSUM
        );
        assert_ne!(checksum(), HASH_EXPRESSION_CHECKSUM);
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::PostgresEventStore;
    use eventlog_core::{EventStore, Expected, Snapshot, StreamId, TenantId};

    #[tokio::test]
    async fn snapshot_schema_upgrade_retains_history_and_refuses_partial_metadata() {
        let Ok(url) = std::env::var("EVENTLOG_TEST_POSTGRES_URL") else {
            eprintln!("skipped: snapshot upgrade requires EVENTLOG_TEST_POSTGRES_URL");
            return;
        };
        let (mut sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let prefix = "snapshot_upgrade";
        for suffix in BASE
            .iter()
            .chain(ADDED)
            .chain(SNAPSHOT_ADDED)
            .chain(GROUP_ADDED)
            .chain(BATCH_ADDED)
            .chain(FLOOR_ADDED)
        {
            sql.batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_{suffix}"))
                .await
                .unwrap();
        }
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        let stream = StreamId::new(TenantId::new("tenant").unwrap(), "item", "one").unwrap();
        store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta("one", &serde_json::json!({})),
            )
            .await
            .unwrap();
        store.shutdown().await.unwrap();
        downgrade_blob_fixture(&sql, prefix).await;
        sql.batch_execute("INSERT INTO snapshot_upgrade_snapshots VALUES ('tenant','item','one',1,1,'{\"total\":999}',now()); DROP TABLE snapshot_upgrade_snapshot_generations; DROP TABLE snapshot_upgrade_append_groups; DROP TABLE snapshot_upgrade_group_batches").await.unwrap();
        sql.execute(
            "UPDATE snapshot_upgrade_schema_version SET checksum=$1",
            &[&legacy_checksum()],
        )
        .await
        .unwrap();
        migrate(&mut sql, prefix, &[]).await.unwrap();
        assert_eq!(
            sql.query_one("SELECT checksum FROM snapshot_upgrade_schema_version", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            checksum()
        );
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        assert!(
            store.load_snapshot(&stream).await.unwrap().is_none(),
            "preupgrade cache has no provenance"
        );
        let generation = store.snapshot_generation(&stream).await.unwrap().unwrap();
        assert!(
            store.load_snapshot(&stream).await.unwrap().is_none(),
            "capture must not certify old bytes"
        );
        let events = store.read_stream(&stream, 0, 100).await.unwrap();
        assert_eq!(events.events.len(), 1);
        assert!(
            store
                .recorded_command(
                    &stream,
                    "one",
                    &eventlog_core::request_hash(&serde_json::json!({})).unwrap()
                )
                .await
                .unwrap()
                .is_some()
        );
        let snapshot = Snapshot {
            version: 1,
            state_schema_version: 1,
            state: serde_json::json!({"total":1}),
            recorded_at: time::OffsetDateTime::now_utc(),
        };
        assert!(
            store
                .save_snapshot_checked(&stream, &snapshot, &generation)
                .await
                .unwrap()
        );
        assert_eq!(
            store.load_snapshot(&stream).await.unwrap().unwrap().state,
            snapshot.state
        );
        store.shutdown().await.unwrap();
        sql.execute(
            "UPDATE snapshot_upgrade_schema_version SET checksum=$1",
            &[&legacy_checksum()],
        )
        .await
        .unwrap();
        assert!(
            migrate(&mut sql, prefix, &[]).await.is_err(),
            "old checksum with new table is partial"
        );
        sql.execute(
            "UPDATE snapshot_upgrade_schema_version SET checksum=$1",
            &[&checksum()],
        )
        .await
        .unwrap();
        sql.batch_execute("DROP TABLE snapshot_upgrade_snapshot_generations")
            .await
            .unwrap();
        assert!(
            migrate(&mut sql, prefix, &[]).await.is_err(),
            "new checksum without table is partial"
        );
        sql.batch_execute(
            "DROP TABLE snapshot_upgrade_append_groups; DROP TABLE snapshot_upgrade_group_batches",
        )
        .await
        .unwrap();
        downgrade_blob_fixture(&sql, prefix).await;
        sql.execute(
            "UPDATE snapshot_upgrade_schema_version SET checksum=$1",
            &[&legacy_checksum()],
        )
        .await
        .unwrap();
        migrate(&mut sql, prefix, &[]).await.unwrap();
        sql.batch_execute("ALTER TABLE snapshot_upgrade_snapshot_generations ALTER COLUMN generation TYPE TEXT USING generation::text").await.unwrap();
        assert!(
            migrate(&mut sql, prefix, &[]).await.is_err(),
            "foreign metadata shape is refused"
        );
        assert!(validate(&mut sql, prefix).await.is_err());
        for suffix in BASE
            .iter()
            .chain(ADDED)
            .chain(SNAPSHOT_ADDED)
            .chain(GROUP_ADDED)
            .chain(BATCH_ADDED)
            .chain(FLOOR_ADDED)
        {
            sql.batch_execute(&format!("DROP TABLE {prefix}_{suffix}"))
                .await
                .unwrap();
        }
    }
}

#[cfg(test)]
mod group_migration_tests {
    use super::*;
    use crate::PostgresEventStore;
    use eventlog_core::{EventStore, Expected, StreamId, TenantId};

    #[tokio::test]
    async fn snapshot_edition_migrates_to_groups_and_foreign_group_shape_refuses() {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
        let (mut sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let prefix = "group_upgrade";
        for suffix in BASE
            .iter()
            .chain(ADDED)
            .chain(SNAPSHOT_ADDED)
            .chain(GROUP_ADDED)
            .chain(BATCH_ADDED)
            .chain(FLOOR_ADDED)
        {
            sql.batch_execute(&format!("DROP TABLE IF EXISTS {prefix}_{suffix}"))
                .await
                .unwrap();
        }
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        let stream = StreamId::new(TenantId::new("tenant").unwrap(), "item", "one").unwrap();
        let original = store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.created", 1)],
                &eventlog_conformance::meta("created", &serde_json::json!({})),
            )
            .await
            .unwrap();
        store.shutdown().await.unwrap();
        downgrade_blob_fixture(&sql, prefix).await;
        sql.batch_execute(
            "DROP TABLE group_upgrade_append_groups; DROP TABLE group_upgrade_group_batches",
        )
        .await
        .unwrap();
        sql.execute(
            "UPDATE group_upgrade_schema_version SET checksum=$1",
            &[&snapshot_checksum()],
        )
        .await
        .unwrap();
        assert!(matches!(
            validate(&mut sql, prefix).await,
            Err(EventLogError::Invalid(_))
        ));
        migrate(&mut sql, prefix, &[]).await.unwrap();
        validate(&mut sql, prefix).await.unwrap();
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        assert_eq!(
            store.read_stream(&stream, 0, 100).await.unwrap().events,
            original.events
        );
        store.shutdown().await.unwrap();
        sql.batch_execute(
            "ALTER TABLE group_upgrade_append_groups ALTER COLUMN request_hash DROP NOT NULL",
        )
        .await
        .unwrap();
        assert!(matches!(
            migrate(&mut sql, prefix, &[]).await,
            Err(EventLogError::Invalid(_))
        ));
        assert!(matches!(
            validate(&mut sql, prefix).await,
            Err(EventLogError::Invalid(_))
        ));
        for suffix in BASE
            .iter()
            .chain(ADDED)
            .chain(SNAPSHOT_ADDED)
            .chain(GROUP_ADDED)
            .chain(BATCH_ADDED)
            .chain(FLOOR_ADDED)
        {
            sql.batch_execute(&format!("DROP TABLE {prefix}_{suffix}"))
                .await
                .unwrap();
        }
    }

    /// The blob-integrity edition gains the batch record through `migrate` alone: the hosted
    /// application role cannot create it, so `open` refuses that edition until the migration role
    /// has run, and a ledger naming the batch edition without its table is refused as partial.
    #[tokio::test]
    async fn blob_integrity_edition_migrates_to_the_batch_record_and_refuses_it_half_done() {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
        let (mut sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let prefix = "batch_upgrade";
        let drop_all: String = BASE
            .iter()
            .chain(ADDED)
            .chain(SNAPSHOT_ADDED)
            .chain(GROUP_ADDED)
            .chain(BATCH_ADDED)
            .chain(FLOOR_ADDED)
            .map(|suffix| format!("DROP TABLE IF EXISTS {prefix}_{suffix};"))
            .collect::<Vec<_>>()
            .concat();
        sql.batch_execute(&drop_all).await.unwrap();
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        let stream = StreamId::new(TenantId::new("tenant").unwrap(), "item", "one").unwrap();
        let original = store
            .append(
                &stream,
                Expected::NoStream,
                &[eventlog_conformance::event("item.created", 1)],
                &eventlog_conformance::meta("created", &serde_json::json!({})),
            )
            .await
            .unwrap();
        store.shutdown().await.unwrap();

        sql.batch_execute(
            "DROP TABLE batch_upgrade_group_batches; DROP TABLE batch_upgrade_settlement_floor; DROP INDEX batch_upgrade_events_publication",
        )
        .await
        .unwrap();
        assert_eq!(
            migrate(&mut sql, prefix, &[]).await,
            Err(EventLogError::Invalid(
                "unknown PostgreSQL schema version/checksum".into()
            )),
            "the batch edition's checksum without its table is partial"
        );

        sql.execute(
            "UPDATE batch_upgrade_schema_version SET checksum=$1",
            &[&blob_integrity_checksum()],
        )
        .await
        .unwrap();
        assert!(
            matches!(
                validate(&mut sql, prefix).await,
                Err(EventLogError::Invalid(_))
            ),
            "an application open refuses the previous edition"
        );
        migrate(&mut sql, prefix, &[]).await.unwrap();
        validate(&mut sql, prefix).await.unwrap();
        assert_eq!(
            sql.query_one("SELECT checksum FROM batch_upgrade_schema_version", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            checksum()
        );
        let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
        assert_eq!(
            store.read_stream(&stream, 0, 100).await.unwrap().events,
            original.events
        );
        store.shutdown().await.unwrap();

        sql.batch_execute("ALTER TABLE batch_upgrade_group_batches ALTER COLUMN digests TYPE TEXT")
            .await
            .unwrap();
        assert!(
            matches!(
                migrate(&mut sql, prefix, &[]).await,
                Err(EventLogError::Invalid(_))
            ),
            "a foreign batch record shape is refused"
        );
        sql.batch_execute(
            "ALTER TABLE batch_upgrade_group_batches ALTER COLUMN digests TYPE JSONB USING digests::jsonb; \
             DROP TABLE batch_upgrade_append_groups",
        )
        .await
        .unwrap();
        assert_eq!(
            migrate(&mut sql, prefix, &[]).await,
            Err(EventLogError::Invalid(
                "partial PostgreSQL group batch migration".into()
            )),
            "a batch record without the receipts it sits beside is refused"
        );
        sql.batch_execute(&drop_all).await.unwrap();
    }
}

#[cfg(test)]
mod projection_index_tests {
    use super::*;

    const PREFIX: &str = "projidx_upgrade";
    const LONG: ProjectionSpec = ProjectionSpec {
        name: "assignments_by_queue_and_skill_and_agent",
        indexed: &["queue", "skill"],
    };

    #[test]
    fn a_long_table_names_each_field_index_apart_within_the_identifier_limit() {
        let short = "owner_p_items";
        assert_eq!(projection_index(short, 3), "owner_p_items_idx_3");
        let table = crate::projection_table(PREFIX, LONG.name);
        assert_eq!(table.len(), 58, "the fixture is the long case");
        let names: Vec<String> = projection_indexes(&table, 8)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        for name in &names {
            assert!(
                name.len() <= 63,
                "{name} exceeds PostgreSQL's identifier limit"
            );
        }
        let distinct: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            distinct.len(),
            9,
            "every index, the page index and each field's, has its own name: {names:?}"
        );
    }

    async fn secondary_indexes(sql: &Client, table: &str) -> Vec<String> {
        sql.query(
            "SELECT pg_get_indexdef(indexrelid) FROM pg_index
             WHERE indrelid=to_regclass($1) AND NOT indisprimary ORDER BY 1",
            &[&table],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
    }

    /// A projection whose `{table}_idx_{p}` name would pass 63 bytes gets one index per declared
    /// field, and migration repairs a table that an older release left with only the first one
    /// (PostgreSQL truncated every name to the same identifier and `IF NOT EXISTS` skipped the
    /// rest), which admission now refuses.
    #[tokio::test]
    async fn a_long_projection_gets_every_index_and_migration_repairs_the_missing_ones() {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture");
        let (mut sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let table = crate::projection_table(PREFIX, LONG.name);
        let drop_all: String = every_table()
            .iter()
            .map(|suffix| format!("DROP TABLE IF EXISTS {PREFIX}_{suffix};"))
            .chain(std::iter::once(format!("DROP TABLE IF EXISTS {table};")))
            .collect::<Vec<_>>()
            .concat();
        sql.batch_execute(&drop_all).await.unwrap();

        migrate(&mut sql, PREFIX, &[LONG]).await.unwrap();
        let fresh = secondary_indexes(&sql, &table).await;
        assert_eq!(
            fresh.len(),
            3,
            "one index per declared field and the page index: {fresh:?}"
        );
        validate_projection(&mut sql, PREFIX, &LONG).await.unwrap();

        // The state an older release left: every field's name truncated to one identifier, so
        // only the first field's index exists.
        let names: String = fresh
            .iter()
            .map(|definition| {
                let name = definition
                    .trim_start_matches("CREATE INDEX ")
                    .split(' ')
                    .next()
                    .unwrap();
                format!("DROP INDEX {name};")
            })
            .collect::<Vec<_>>()
            .concat();
        sql.batch_execute(&format!(
            "{names}CREATE INDEX {table}_idx_0 ON {table} (tenant_id, idx_0);"
        ))
        .await
        .unwrap();
        assert_eq!(secondary_indexes(&sql, &table).await.len(), 1);
        let refused = validate_projection(&mut sql, PREFIX, &LONG)
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, EventLogError::Invalid(message)
                if message == &format!("incompatible PostgreSQL projection: {table}")),
            "{refused:?}"
        );

        migrate(&mut sql, PREFIX, &[LONG]).await.unwrap();
        let repaired = secondary_indexes(&sql, &table).await;
        assert_eq!(
            repaired.len(),
            3,
            "migration adds the missing indexes: {repaired:?}"
        );
        assert!(
            repaired
                .iter()
                .any(|index| index
                    .ends_with(&format!(" USING btree (tenant_id, {})", digest_column(1)))),
            "{repaired:?}"
        );
        validate_projection(&mut sql, PREFIX, &LONG).await.unwrap();
        sql.batch_execute(&drop_all).await.unwrap();
    }
}
