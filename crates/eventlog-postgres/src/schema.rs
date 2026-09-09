//! Additive, serialized schema admission; checks every column, default, constraint and index.
use super::backend;
use eventlog_core::{EventLogError, ProjectionSpec};
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
fn base_ddl(prefix: &str) -> String {
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
fn additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_scope_counters (coordinate TEXT PRIMARY KEY, held BIGINT NOT NULL CHECK (held >= 0)); CREATE TABLE IF NOT EXISTS {prefix}_schema_version (version BIGINT PRIMARY KEY CHECK (version = 1), checksum TEXT NOT NULL); CREATE TABLE IF NOT EXISTS {prefix}_projection_registry(projection_name TEXT PRIMARY KEY,indexed_fields JSONB NOT NULL);"
    )
}
// Frozen original schema edition: existing ledger rows must remain migratable.
fn legacy_checksum() -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{}{}", base_ddl("owner"), additions("owner")))
    )
}

fn snapshot_additions(prefix: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {prefix}_snapshot_generations (tenant_id TEXT NOT NULL,stream_type TEXT NOT NULL,stream_id TEXT NOT NULL,generation UUID NOT NULL,cached_generation UUID,PRIMARY KEY(tenant_id,stream_type,stream_id));"
    )
}

fn checksum() -> String {
    format!(
        "{:x}",
        Sha256::digest(format!(
            "{}{}{}",
            base_ddl("owner"),
            additions("owner"),
            snapshot_additions("owner")
        ))
    )
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
async fn shape<C: GenericClient>(
    client: &C,
    table: &str,
    prefix: &str,
) -> Result<Vec<String>, EventLogError> {
    let relation=client.query_opt("SELECT c.relkind::text,c.relrowsecurity,c.relforcerowsecurity,c.relpersistence::text,c.relispartition FROM pg_class c WHERE c.oid=to_regclass($1)",&[&table]).await.map_err(backend)?;
    let Some(relation) = relation else {
        return Ok(Vec::new());
    };
    let rewritten: bool = client
        .query_one(
            "SELECT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=to_regclass($1))",
            &[&table],
        )
        .await
        .map_err(backend)?
        .get(0);
    if rewritten {
        return Err(EventLogError::Invalid(format!(
            "unadmitted rewrite rule: {table}"
        )));
    }
    if relation.get::<_, String>(0) != "r"
        || relation.get::<_, bool>(1)
        || relation.get::<_, bool>(2)
        || relation.get::<_, bool>(4)
        || relation.get::<_, String>(3)
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
    let hidden:bool=client.query_one("SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal) OR EXISTS(SELECT FROM pg_policy WHERE polrelid=to_regclass($1)) OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass($1) AND (attgenerated<>'' OR attidentity<>'')) OR EXISTS(SELECT FROM pg_attribute a JOIN pg_collation c ON c.oid=a.attcollation WHERE a.attrelid=to_regclass($1) AND NOT c.collisdeterministic)",&[&table]).await.map_err(backend)?.get(0);
    if hidden {
        return Err(EventLogError::Invalid(format!(
            "unadmitted trigger, policy or generated column: {table}"
        )));
    }
    let inherited: bool = client
        .query_one(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=to_regclass($1) OR inhparent=to_regclass($1))",
            &[&table],
        )
        .await
        .map_err(backend)?
        .get(0);
    if inherited {
        return Err(EventLogError::Invalid(format!(
            "unsupported inherited relation: {table}"
        )));
    }
    let mut shape = Vec::new();
    for row in client.query("SELECT a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull, COALESCE(pg_get_expr(d.adbin,d.adrelid),''),a.attcollation::text FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", &[&table]).await.map_err(backend)? {
        let default: String = row.get(3);
        // PostgreSQL qualifies sequences when the temporary expected schema shadows search_path.
        let default = default.replace(prefix,"OWNER").replace("pg_temp_", "");
        let default = if default.starts_with("nextval(") { "owned-sequence".to_owned() } else { default };
        shape.push(format!("column:{}:{}:{}:{}:{}",row.get::<_,String>(0),row.get::<_,String>(1),row.get::<_,bool>(2),default,row.get::<_,String>(4)));
    }
    for row in client.query("SELECT contype::text, pg_get_constraintdef(oid, true), convalidated FROM pg_constraint WHERE conrelid=to_regclass($1) ORDER BY contype, pg_get_constraintdef(oid,true)", &[&table]).await.map_err(backend)? { shape.push(format!("constraint:{}:{}:{}",row.get::<_,String>(0),row.get::<_,String>(1),row.get::<_,bool>(2))); }
    let mut indexes = Vec::new();
    for row in client.query("SELECT pg_get_indexdef(indexrelid), indisvalid, indisready FROM pg_index WHERE indrelid=to_regclass($1)", &[&table]).await.map_err(backend)? {
        let definition: String = row.get(0);
        let unique = definition.starts_with("CREATE UNIQUE INDEX");
        let columns = definition.split_once(" USING ").ok_or_else(|| EventLogError::Invalid("unrecognized PostgreSQL index definition".into()))?.1;
        indexes.push(format!("index:{unique}:{columns}:{}:{}",row.get::<_,bool>(1),row.get::<_,bool>(2)));
    }
    indexes.sort();
    shape.extend(indexes);
    Ok(shape)
}
async fn compare(
    transaction: &Transaction<'_>,
    prefix: &str,
    suffixes: &[&str],
) -> Result<(), EventLogError> {
    for suffix in suffixes {
        let table = format!("{prefix}_{suffix}");
        if shape(transaction, &table, prefix).await?
            != shape(
                transaction,
                &format!("eventlog_expected_{suffix}"),
                "eventlog_expected",
            )
            .await?
        {
            return Err(EventLogError::Invalid(format!(
                "incompatible PostgreSQL schema: {table}; explicit supported migration required"
            )));
        }
        // A replaced sequence/default or user trigger is not part of the admitted durable shape.
        if *suffix == "events" {
            let owns: bool = transaction.query_one("SELECT EXISTS(SELECT FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum JOIN pg_depend dependency ON dependency.classid='pg_attrdef'::regclass AND dependency.objid=d.oid AND dependency.refclassid='pg_class'::regclass WHERE d.adrelid=to_regclass($1) AND a.attname='global_seq' AND dependency.refobjid=to_regclass(pg_get_serial_sequence($1,'global_seq')) AND pg_get_expr(d.adbin,d.adrelid)=format('nextval(%L::regclass)',to_regclass(pg_get_serial_sequence($1,'global_seq'))::text)) AND (SELECT count(*) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum JOIN pg_depend dependency ON dependency.classid='pg_attrdef'::regclass AND dependency.objid=d.oid AND dependency.refclassid='pg_class'::regclass WHERE d.adrelid=to_regclass($1) AND a.attname='global_seq' AND dependency.refobjid IN (SELECT oid FROM pg_class WHERE relkind='S'))=1", &[&table]).await.map_err(backend)?.get(0);
            let sequence_shape:bool=transaction.query_one("SELECT COALESCE(bool_and(s.seqtypid='bigint'::regtype AND s.seqstart=1 AND s.seqincrement=1 AND s.seqmin=1 AND s.seqmax=9223372036854775807 AND s.seqcache=1 AND NOT s.seqcycle AND c.relpersistence='p'),false) FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid WHERE s.seqrelid=to_regclass(pg_get_serial_sequence($1,'global_seq'))",&[&table]).await.map_err(backend)?.get(0);
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
                "{}{}{}",
                base_ddl("eventlog_expected"),
                additions("eventlog_expected"),
                snapshot_additions("eventlog_expected")
            )
            .replace("CREATE TABLE IF NOT EXISTS", "CREATE TEMP TABLE"),
        )
        .await
        .map_err(backend)
}
async fn drop_expected(transaction: &Transaction<'_>) -> Result<(), EventLogError> {
    for suffix in BASE.iter().chain(ADDED).chain(SNAPSHOT_ADDED) {
        transaction
            .batch_execute(&format!("DROP TABLE pg_temp.eventlog_expected_{suffix}"))
            .await
            .map_err(backend)?;
    }
    Ok(())
}
async fn version(
    transaction: &Transaction<'_>,
    prefix: &str,
    expected_checksum: &str,
) -> Result<(), EventLogError> {
    let rows = transaction
        .query(
            &format!("SELECT version,checksum FROM {prefix}_schema_version"),
            &[],
        )
        .await
        .map_err(backend)?;
    if rows.len() != 1
        || rows[0].get::<_, i64>(0) != 1
        || rows[0].get::<_, String>(1) != expected_checksum
    {
        return Err(EventLogError::Invalid(
            "unknown PostgreSQL schema version/checksum".into(),
        ));
    }
    Ok(())
}
pub(crate) async fn migrate(
    client: &mut Client,
    prefix: &str,
    projections: &[ProjectionSpec],
) -> Result<(), EventLogError> {
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
        compare(&transaction, prefix, BASE).await?;
    }
    let ledger = exists(&transaction, &format!("{prefix}_schema_version")).await?;
    let generations = exists(&transaction, &format!("{prefix}_snapshot_generations")).await?;
    if ledger {
        compare(&transaction, prefix, ADDED).await?;
        if generations {
            compare(&transaction, prefix, SNAPSHOT_ADDED).await?;
            version(&transaction, prefix, &checksum()).await?;
        } else {
            version(&transaction, prefix, &legacy_checksum()).await?;
        }
    } else if generations
        || exists(&transaction, &format!("{prefix}_scope_counters")).await?
        || exists(&transaction, &format!("{prefix}_projection_registry")).await?
    {
        return Err(EventLogError::Invalid(
            "partial PostgreSQL admission migration".into(),
        ));
    }
    transaction
        .batch_execute(&format!(
            "{}{}{}",
            base_ddl(prefix),
            additions(prefix),
            snapshot_additions(prefix)
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
    } else if !generations {
        transaction
            .execute(
                &format!(
                    "UPDATE {prefix}_schema_version SET checksum=$1 WHERE version=1 AND checksum=$2"
                ),
                &[&checksum(), &legacy_checksum()],
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
    drop_expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    transaction.commit().await.map_err(backend)
}
pub(crate) async fn validate(client: &mut Client, prefix: &str) -> Result<(), EventLogError> {
    let transaction = client.transaction().await.map_err(backend)?;
    lock(&transaction, prefix).await?;
    expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    compare(&transaction, prefix, BASE).await?;
    compare(&transaction, prefix, ADDED).await?;
    compare(&transaction, prefix, SNAPSHOT_ADDED).await?;
    version(&transaction, prefix, &checksum()).await?;
    drop_expected(&transaction)
        .await
        .map_err(|error| EventLogError::Invalid(format!("expected schema: {error}")))?;
    transaction.commit().await.map_err(backend)
}
pub(crate) async fn permissions(client: &Client, prefix: &str) -> Result<(), EventLogError> {
    let tables: Vec<_> = BASE
        .iter()
        .chain(ADDED)
        .chain(SNAPSHOT_ADDED)
        .map(|suffix| format!("{prefix}_{suffix}"))
        .collect();
    let admitted:bool=client.query_one("SELECT COALESCE(bool_and(has_table_privilege(current_user,to_regclass(name),'SELECT') AND has_table_privilege(current_user,to_regclass(name),'INSERT') AND has_table_privilege(current_user,to_regclass(name),'UPDATE') AND has_table_privilege(current_user,to_regclass(name),'DELETE')),false) FROM unnest($1::text[]) AS name",&[&tables]).await.map_err(backend)?.get(0);
    let sequence = format!("{prefix}_events");
    let sequence_admitted:bool=client.query_one("SELECT COALESCE(has_sequence_privilege(current_user,to_regclass(pg_get_serial_sequence($1,'global_seq')),'USAGE'),false) AND has_database_privilege(current_user,current_database(),'TEMP')",&[&sequence]).await.map_err(backend)?.get(0);
    if !admitted || !sequence_admitted {
        return Err(EventLogError::Invalid("hosted application role lacks required durable DML, sequence usage or temporary-table rights".into()));
    }
    Ok(())
}
pub(crate) async fn create_projection<C: GenericClient>(
    client: &C,
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
    let columns = super::joined(spec.indexed.len(), |position| {
        format!(", idx_{position} TEXT")
    });
    let indexes = super::joined(spec.indexed.len(), |position| {
        format!(
            "CREATE INDEX IF NOT EXISTS {table}_idx_{position} ON {table} (tenant_id, idx_{position});"
        )
    });
    client.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {table} (tenant_id TEXT NOT NULL,row_key TEXT NOT NULL,body JSONB NOT NULL{columns},PRIMARY KEY(tenant_id,row_key));{indexes}")).await.map_err(backend)?;
    // Migration admits existing projection tables too; IF NOT EXISTS alone does not
    // reject hidden behavior that can discard projection writes before registration.
    shape(client, &table, prefix).await?;
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
    // The expected projection is temporary: an application role needs TEMP, not owner-schema CREATE.
    let allowed:bool=transaction.query_one("SELECT COALESCE(has_table_privilege(current_user,to_regclass($1),'SELECT') AND has_table_privilege(current_user,to_regclass($1),'INSERT') AND has_table_privilege(current_user,to_regclass($1),'UPDATE') AND has_table_privilege(current_user,to_regclass($1),'DELETE'),false)",&[&table]).await.map_err(backend)?.get(0);
    if !allowed {
        return Err(EventLogError::Invalid(
            "declared projection lacks required application DML permissions".into(),
        ));
    }
    let columns = super::joined(spec.indexed.len(), |position| {
        format!(", idx_{position} TEXT")
    });
    transaction.batch_execute(&format!("CREATE TEMP TABLE eventlog_expected_projection (tenant_id TEXT NOT NULL,row_key TEXT NOT NULL,body JSONB NOT NULL{columns},PRIMARY KEY(tenant_id,row_key)) ON COMMIT DROP;")).await.map_err(backend)?;
    for position in 0..spec.indexed.len() {
        transaction
            .batch_execute(&format!(
                "CREATE INDEX ON eventlog_expected_projection (tenant_id,idx_{position})"
            ))
            .await
            .map_err(backend)?;
    }
    if shape(&transaction, &table, prefix).await?
        != shape(
            &transaction,
            "eventlog_expected_projection",
            "eventlog_expected",
        )
        .await?
    {
        return Err(EventLogError::Invalid(format!(
            "incompatible PostgreSQL projection: {table}"
        )));
    }
    transaction.commit().await.map_err(backend)
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
        for suffix in BASE.iter().chain(ADDED).chain(SNAPSHOT_ADDED) {
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
        sql.batch_execute("INSERT INTO snapshot_upgrade_snapshots VALUES ('tenant','item','one',1,1,'{\"total\":999}',now()); DROP TABLE snapshot_upgrade_snapshot_generations").await.unwrap();
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
        for suffix in BASE.iter().chain(ADDED).chain(SNAPSHOT_ADDED) {
            sql.batch_execute(&format!("DROP TABLE {prefix}_{suffix}"))
                .await
                .unwrap();
        }
    }
}
