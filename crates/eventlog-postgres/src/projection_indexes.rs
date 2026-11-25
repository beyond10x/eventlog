//! The physical indexes of projection tables: the bytewise page index, its admission and the
//! fenced migration from the 0.10.0 edition.
use super::{PostgresEventStore, projection_page_sql, projection_table, schema};
use eventlog_core::{EventLogError, EventStore, ProjectionSpec, TenantId};
use serde_json::Value;
use tokio_postgres::{Client, NoTls};

/// The ledger checksum the 0.10.0 release writes and requires: its `open` and `migrate` refuse
/// any other value.
const RELEASE_0_10_0_CHECKSUM: &str =
    "f5989b20d7de8babbd77d3427015b92a70f5f6275982f740616476873d09a7fa";

/// The ledger checksum `main` wrote from 0fe56b5c (pull request 70) until the digest became
/// FIPS-safe: projection tables indexed by the first 8 bytes of `md5(idx_N)`. Never released, but a
/// store `main` migrated holds it.
const MD5_DIGEST_CHECKSUM: &str =
    "20d998c6771a5b4a4e03df9034ac4bab9479ae72ab908f2bae5ef045c7a6881c";

/// The digest index of `idx_0` in the `MD5_DIGEST_CHECKSUM` edition, as `pg_get_indexdef` prints
/// its columns.
const MD5_DIGEST_INDEX: &str = "btree (tenant_id, substr(decode(md5(idx_0), 'hex'::text), 1, 8))";

/// The ledger checksum pull request 72 wrote: projection tables indexed by the expression
/// `hashtextextended(idx_N, 0)`, with no stored digest column. Never released.
const HASH_EXPRESSION_CHECKSUM: &str =
    "41ca55d5a4e879d0928e0834e06f4503a01f5becde9712b891bde2c1b0eb282f";

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "keyed_rows",
    indexed: &["kind"],
};

fn fixture() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture")
}

pub(super) async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The fixture URL with its database replaced by `database`.
fn with_database(url: &str, database: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let authority = base.find("://").map_or(0, |at| at + 3);
    let base = match base[authority..].find('/') {
        Some(slash) => &base[..authority + slash],
        None => base,
    };
    match query {
        Some(query) => format!("{base}/{database}?{query}"),
        None => format!("{base}/{database}"),
    }
}

pub(super) async fn drop_owner(sql: &Client, prefix: &str, projections: &[&ProjectionSpec]) {
    let statement = schema::every_table()
        .iter()
        .map(|suffix| format!("{prefix}_{suffix}"))
        .chain(
            projections
                .iter()
                .map(|spec| projection_table(prefix, spec.name)),
        )
        .map(|table| format!("DROP TABLE IF EXISTS {table};"))
        .collect::<Vec<_>>()
        .concat();
    sql.batch_execute(&statement).await.unwrap();
}

/// Every secondary index definition of `table`, after `USING`, sorted.
async fn secondary_indexes(sql: &Client, table: &str) -> Vec<String> {
    let mut found: Vec<String> = sql
        .query(
            "SELECT pg_get_indexdef(indexrelid) FROM pg_index
             WHERE indrelid=to_regclass($1) AND NOT indisprimary",
            &[&table],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| {
            let definition: String = row.get(0);
            definition.split_once(" USING ").unwrap().1.to_owned()
        })
        .collect();
    found.sort();
    found
}

fn nodes<'a>(plan: &'a Value, found: &mut Vec<&'a Value>) {
    found.push(plan);
    for child in plan["Plans"].as_array().into_iter().flatten() {
        nodes(child, found);
    }
}

/// Review L22: on a database created with ICU `und`, the page query walks the bytewise
/// `(tenant_id, row_key COLLATE "C")` index `migrate` builds, with no Sort node, and the
/// mixed-case keys still page completely, in byte order.
#[tokio::test]
async fn an_icu_database_walks_the_bytewise_page_index_without_a_sort() {
    let url = fixture();
    let icu_url = with_database(&url, &icu_database(&url).await);
    let prefix = "icu_page";
    let table = projection_table(prefix, ROWS.name);
    {
        let mut sql = connect(&icu_url).await;
        drop_owner(&sql, prefix, &[&ROWS]).await;
        let collation: String = sql
            .query_one(
                "SELECT datlocprovider::text || ':' || COALESCE(datlocale, '')
                 FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(collation, "i:und", "the database under test is ICU und");
        schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
        sql.batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, row_key, body)
                 VALUES ('tenant-icu','desk/1','{{}}'),('tenant-icu','Desk/2','{{}}'),
                        ('tenant-icu','desk/3','{{}}');
             INSERT INTO {table} (tenant_id, row_key, body, idx_0)
                 SELECT 'tenant-' || (n % 20), 'row/' || n, '{{}}', 'k' || (n % 7)
                 FROM generate_series(1, 4000) AS n;
             ANALYZE {table};"
        ))
        .await
        .unwrap();
        for after_cursor in [false, true] {
            let plan: Value = sql
                .query_one(
                    &format!(
                        "EXPLAIN (FORMAT JSON) {}",
                        projection_page_sql(&table, after_cursor)
                    ),
                    &[&"tenant-3", &"row/1", &10_i64],
                )
                .await
                .unwrap()
                .get(0);
            let mut found = Vec::new();
            nodes(&plan[0]["Plan"], &mut found);
            assert!(
                found.iter().all(|node| {
                    !node["Node Type"]
                        .as_str()
                        .is_some_and(|kind| kind.contains("Sort"))
                }),
                "the page is walked in index order, not sorted: {plan}"
            );
            let index = found
                .iter()
                .find_map(|node| node["Index Name"].as_str())
                .unwrap_or_else(|| panic!("the page reads an index: {plan}"));
            let definition: String = sql
                .query_one(
                    "SELECT pg_get_indexdef(to_regclass($1))",
                    &[&index.to_owned()],
                )
                .await
                .unwrap()
                .get(0);
            assert!(
                definition.ends_with(" USING btree (tenant_id, row_key COLLATE \"C\")"),
                "{definition}"
            );
        }
    }
    let store = PostgresEventStore::connect(&icu_url, prefix).await.unwrap();
    let tenant = TenantId::new("tenant-icu").unwrap();
    let keys = |page: eventlog_core::ProjectionPage| -> Vec<String> {
        page.rows.into_iter().map(|(key, _)| key).collect()
    };
    assert_eq!(
        keys(
            store
                .projection_page(&ROWS, &tenant, None, None, 10)
                .await
                .unwrap()
        ),
        ["Desk/2", "desk/1", "desk/3"],
        "all three rows, in byte order"
    );
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = store
            .projection_page(&ROWS, &tenant, Some("desk/"), cursor.as_deref(), 1)
            .await
            .unwrap();
        cursor = page.next_cursor.clone();
        seen.extend(keys(page));
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, ["desk/1", "desk/3"], "the prefix pages by bytes");
    store.shutdown().await.unwrap();
    drop_owner(&connect(&icu_url).await, prefix, &[&ROWS]).await;
}

/// The fixture's ICU `und` database, created on first use and then kept.
///
/// It is never dropped: `DROP DATABASE` waits for a checkpoint while it holds a transaction id,
/// and that holds back every feed in the instance (AGENTS.md, *The watermark couples feed latency
/// across owners*). Dropped per run, it made a sibling case's bounded feed retry run out.
async fn icu_database(url: &str) -> String {
    const NAME: &str = "eventlog_test_icu_und";
    let admin = connect(url).await;
    let present: bool = admin
        .query_one(
            "SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)",
            &[&NAME],
        )
        .await
        .unwrap()
        .get(0);
    if !present {
        let created = admin
            .batch_execute(&format!(
                "CREATE DATABASE {NAME} TEMPLATE template0 ENCODING 'UTF8'
                     LOCALE_PROVIDER icu ICU_LOCALE 'und'"
            ))
            .await;
        // Another test process may have created it in between.
        if let Err(error) = created {
            assert_eq!(
                error.code(),
                Some(&tokio_postgres::error::SqlState::DUPLICATE_DATABASE),
                "{error}"
            );
        }
    }
    NAME.to_owned()
}

/// The digest index of `idx_0`, as `pg_get_indexdef` prints its columns: the stored digest column.
const DIGEST_INDEX: &str = "btree (tenant_id, idx_0_digest)";

/// The digest index of `idx_0` in the `HASH_EXPRESSION_CHECKSUM` edition: the expression itself.
const HASH_EXPRESSION_INDEX: &str = "btree (tenant_id, hashtextextended(idx_0, '0'::bigint))";

/// The digest columns of `table` as the catalog reports them: name, type, generation kind and
/// generation expression.
async fn digest_columns(sql: &Client, table: &str) -> Vec<String> {
    sql.query(
        "SELECT a.attname || ':' || format_type(a.atttypid, a.atttypmod) || ':' || a.attgenerated::text
                || ':' || COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
         FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
         WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attname LIKE '%_digest'
         ORDER BY a.attnum",
        &[&table],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| row.get(0))
    .collect()
}

/// The one digest column of a one-field table, as `digest_columns` prints it.
const DIGEST_COLUMN: &str = "idx_0_digest:bigint:s:hashtextextended(idx_0, (0)::bigint)";

/// Every SQL function the text `sql` calls: each identifier directly followed by `(`.
fn called_functions(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut word = String::new();
    for character in sql.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            word.push(character);
        } else {
            if character == '(' && !word.is_empty() {
                found.push(word.clone());
            }
            word.clear();
        }
    }
    found
}

/// Story pg-projection-find-back-within-1-05: a PostgreSQL server in FIPS mode refuses `md5()`,
/// and an index whose expression calls it can be neither built nor read there. The value digest
/// calls `hashtextextended`, PostgreSQL's own non-cryptographic text hash (the one hash
/// partitioning uses), which runs no OpenSSL digest and so stays available in FIPS mode.
#[test]
fn the_value_digest_calls_only_a_function_fips_mode_allows() {
    let generated: Vec<String> = schema::projection_columns(2)
        .into_iter()
        .filter_map(|column| column.generated)
        .collect();
    assert_eq!(
        generated.len(),
        2,
        "each declared field has a stored digest"
    );
    let shape: Vec<String> = schema::projection_indexes("owner_p_rows", 2)
        .into_iter()
        .map(|(_, columns)| columns)
        .chain(generated)
        .chain((0..2).flat_map(|position| {
            [
                schema::projection_find_filter(position),
                schema::projection_find_checked_filter(position),
            ]
        }))
        .collect();
    let mut called: Vec<String> = shape
        .iter()
        .map(String::as_str)
        .flat_map(called_functions)
        .collect();
    called.sort();
    called.dedup();
    assert_eq!(
        called,
        ["hashtextextended"],
        "the digest columns, the indexes and the lookups call only the FIPS-safe hash: {shape:?}"
    );
}

/// `length` printable ASCII bytes with no repetition a compressor could use.
fn incompressible(length: usize, mut state: u64) -> String {
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'!' + u8::try_from(state % 94).unwrap())
        })
        .collect()
}

/// A store in the 0.10.0 edition — its ledger checksum, projection tables indexed by
/// `(tenant_id, idx_N)` and without the bytewise page index — is refused by `open` until
/// `migrate` runs. `migrate` alone, given no projection, moves every registered projection table
/// to the current indexes and the ledger to a checksum the 0.10.0 binary refuses: the cutover is
/// fenced. The migrated table answers the same lookups and now takes an indexed value the value
/// index refused.
#[tokio::test]
async fn the_previous_edition_migrates_its_projection_tables_and_fences_the_previous_binary() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_upgrade";
    let table = projection_table(prefix, ROWS.name);
    let tenant = TenantId::new("tenant").unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let current = secondary_indexes(&sql, &table).await;
    assert_eq!(
        current,
        [DIGEST_INDEX, "btree (tenant_id, row_key COLLATE \"C\")"],
        "a fresh projection table has the digest index and the bytewise page index"
    );
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
             SELECT 'tenant', 'row/' || n, jsonb_build_object('n', n), 'kind'
             FROM generate_series(1, 50) AS n"
    ))
    .await
    .unwrap();
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    let before = store
        .projection_find(&ROWS, &tenant, "kind", "kind", 100)
        .await
        .unwrap();
    assert_eq!(before.len(), 50);
    store.shutdown().await.unwrap();

    // The 0.10.0 state: its ledger checksum, the value itself indexed, no bytewise page index.
    let ours: Vec<String> = sql
        .query(
            "SELECT indexrelid::regclass::text FROM pg_index
             WHERE indrelid=to_regclass($1) AND NOT indisprimary",
            &[&table],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    for index in ours {
        sql.batch_execute(&format!("DROP INDEX {index}"))
            .await
            .unwrap();
    }
    sql.batch_execute(&format!(
        "ALTER TABLE {table} DROP COLUMN IF EXISTS idx_0_digest;
         CREATE INDEX {table}_idx_0 ON {table} (tenant_id, idx_0)"
    ))
    .await
    .unwrap();
    assert_eq!(
        digest_columns(&sql, &table).await,
        Vec::<String>::new(),
        "the fixture holds 0.10.0's columns"
    );
    sql.execute(
        &format!("UPDATE {prefix}_schema_version SET checksum=$1"),
        &[&RELEASE_0_10_0_CHECKSUM],
    )
    .await
    .unwrap();
    let large = incompressible(8_000, 0x2545_f491_4f6c_dd1d);
    let insert_large = format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0) VALUES ('tenant', 'large', '{{}}', $1)"
    );
    let refused = sql.execute(&insert_large, &[&large]).await.unwrap_err();
    assert_eq!(
        refused.code(),
        Some(&tokio_postgres::error::SqlState::PROGRAM_LIMIT_EXCEEDED),
        "the value index of 0.10.0 cannot hold the value: {refused}"
    );
    assert_eq!(
        schema::validate(&mut sql, prefix).await,
        Err(EventLogError::Invalid(
            "unknown PostgreSQL schema version/checksum".into()
        )),
        "an application open refuses the previous edition until migrate runs"
    );

    schema::migrate(&mut sql, prefix, &[]).await.unwrap();
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        current,
        "migrate moved a registered projection it was not given to the current indexes"
    );
    assert_eq!(
        digest_columns(&sql, &table).await,
        [DIGEST_COLUMN],
        "migrate added the stored digest column"
    );
    schema::validate(&mut sql, prefix).await.unwrap();
    schema::validate_projection(&mut sql, prefix, &ROWS)
        .await
        .unwrap();
    let ledger: String = sql
        .query_one(
            &format!("SELECT checksum FROM {prefix}_schema_version"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        ledger, RELEASE_0_10_0_CHECKSUM,
        "the 0.10.0 binary requires its own checksum, so it is refused on the migrated schema"
    );
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", "kind", 100)
            .await
            .unwrap(),
        before,
        "the migrated table answers the same lookup"
    );
    sql.execute(&insert_large, &[&large]).await.unwrap();
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", &large, 10)
            .await
            .unwrap(),
        [serde_json::json!({})],
        "the 8,000-byte value is indexed and found"
    );
    store.shutdown().await.unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// A store `main` migrated before the digest became FIPS-safe — its ledger checksum and each
/// projection table's MD5 digest index — is refused by `open` until `migrate` runs. `migrate`
/// alone moves every registered projection table to the `hashtextextended` digest under the same
/// index name and the ledger to a checksum that `main`'s binary refuses.
#[tokio::test]
async fn the_md5_digest_edition_migrates_to_the_fips_safe_digest_and_is_fenced() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_digest_edition";
    let table = projection_table(prefix, ROWS.name);
    let tenant = TenantId::new("tenant").unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let current = secondary_indexes(&sql, &table).await;
    assert_eq!(
        current,
        [DIGEST_INDEX, "btree (tenant_id, row_key COLLATE \"C\")"],
        "a fresh projection table has the FIPS-safe digest index"
    );
    let large = incompressible(8_000, 0x1d87_2b41_9c3e_5a07);
    sql.execute(
        &format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
                 SELECT 'tenant', 'row/' || n, jsonb_build_object('n', n), 'kind'
                 FROM generate_series(1, 50) AS n
             UNION ALL SELECT 'tenant', 'large', '{{}}', $1"
        ),
        &[&large],
    )
    .await
    .unwrap();

    // `main`'s state: its ledger checksum and the MD5 digest index under the field's index name.
    let index = schema::projection_index(&table, 0);
    sql.batch_execute(&format!(
        "DROP INDEX IF EXISTS {index}; ALTER TABLE {table} DROP COLUMN IF EXISTS idx_0_digest;
         CREATE INDEX {index} ON {table} USING {MD5_DIGEST_INDEX}"
    ))
    .await
    .unwrap();
    sql.execute(
        &format!("UPDATE {prefix}_schema_version SET checksum=$1"),
        &[&MD5_DIGEST_CHECKSUM],
    )
    .await
    .unwrap();
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        ["btree (tenant_id, row_key COLLATE \"C\")", MD5_DIGEST_INDEX],
        "the fixture holds main's edition"
    );
    assert_eq!(
        schema::validate(&mut sql, prefix).await,
        Err(EventLogError::Invalid(
            "unknown PostgreSQL schema version/checksum".into()
        )),
        "an application open refuses main's edition until migrate runs"
    );

    schema::migrate(&mut sql, prefix, &[]).await.unwrap();
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        current,
        "migrate moved a registered projection it was not given to the FIPS-safe digest"
    );
    assert_eq!(
        digest_columns(&sql, &table).await,
        [DIGEST_COLUMN],
        "migrate added the stored digest column"
    );
    schema::validate(&mut sql, prefix).await.unwrap();
    schema::validate_projection(&mut sql, prefix, &ROWS)
        .await
        .unwrap();
    let ledger: String = sql
        .query_one(
            &format!("SELECT checksum FROM {prefix}_schema_version"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        ledger, MD5_DIGEST_CHECKSUM,
        "main's binary requires its own checksum, so it is refused on the migrated schema"
    );
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", "kind", 100)
            .await
            .unwrap()
            .len(),
        50,
        "the migrated table answers the same lookup"
    );
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", &large, 10)
            .await
            .unwrap(),
        [serde_json::json!({})],
        "the 8,000-byte value is found through the new digest"
    );
    store.shutdown().await.unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// Two values with one `hashtextextended(value, 0)`, the 64-bit digest the indexed-value index
/// keys on (5613642215003777566). Found offline by a parallel Pollard rho search over
/// 16-hex-digit strings (about 2^32 evaluations of PostgreSQL's `hash_bytes_extended`); the test
/// below asserts in PostgreSQL that they collide.
const DIGEST_TWINS: [&str; 2] = ["2b4484988e527c47", "aac8b6a1b2ffc8a6"];

/// A guard that looks one value up inside the append's transaction and keeps what it found.
struct FindsKind {
    tenant: TenantId,
    value: &'static str,
    found: std::sync::Mutex<Vec<Value>>,
}

impl eventlog_core::Guard for FindsKind {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let rows = store
                .find(&ROWS, &self.tenant, "kind", self.value, 10)
                .await?;
            *self.found.lock().unwrap() = rows;
            Ok(())
        })
    }
}

/// Review L78: the index keys on a fixed-size digest of the value, so two values sharing it are
/// both candidates for either lookup. The comparison with the value itself keeps only the one
/// asked for, in `projection_find` and in a guard's `find` inside the append's transaction.
#[tokio::test]
async fn values_sharing_a_digest_are_told_apart_by_the_candidate_check() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "digest_twins";
    let table = projection_table(prefix, ROWS.name);
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let [first, second] = DIGEST_TWINS;
    let collide: bool = sql
        .query_one(
            &format!(
                "SELECT $1::text <> $2::text AND {} = {}",
                schema::indexed_digest("$1::text"),
                schema::indexed_digest("$2::text")
            ),
            &[&first, &second],
        )
        .await
        .unwrap()
        .get(0);
    assert!(collide, "the twins are distinct values with one digest");
    sql.execute(
        &format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0) VALUES
                 ('twins', 'first', jsonb_build_object('kind', $1::text), $1),
                 ('twins', 'second', jsonb_build_object('kind', $2::text), $2)"
        ),
        &[&first, &second],
    )
    .await
    .unwrap();
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
             SELECT 'twins', 'row/' || n, '{{}}', 'value/' || n FROM generate_series(1, 4000) AS n;
         ANALYZE {table};"
    ))
    .await
    .unwrap();
    // The lookup reads the digest index, not the table.
    let plan: Value = sql
        .query_one(
            &format!(
                "EXPLAIN (FORMAT JSON) SELECT body FROM {table} WHERE {} ORDER BY row_key LIMIT $3",
                schema::projection_find_filter(0)
            ),
            &[&"twins", &first, &10_i64],
        )
        .await
        .unwrap()
        .get(0);
    let mut found = Vec::new();
    nodes(&plan[0]["Plan"], &mut found);
    let index = found
        .iter()
        .find_map(|node| node["Index Name"].as_str())
        .unwrap_or_else(|| panic!("the lookup reads an index: {plan}"));
    let definition: String = sql
        .query_one(
            "SELECT pg_get_indexdef(to_regclass($1))",
            &[&index.to_owned()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        definition.ends_with(&format!(" USING {DIGEST_INDEX}")),
        "{definition}"
    );
    let tenant = TenantId::new("twins").unwrap();
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    for (index, value) in DIGEST_TWINS.into_iter().enumerate() {
        assert_eq!(
            store
                .projection_find(&ROWS, &tenant, "kind", value, 10)
                .await
                .unwrap(),
            [serde_json::json!({ "kind": value })],
            "projection_find returns only the row holding {value}"
        );
        let guard = std::sync::Arc::new(FindsKind {
            tenant: tenant.clone(),
            value,
            found: std::sync::Mutex::default(),
        });
        let stream =
            eventlog_core::StreamId::new(tenant.clone(), "item", format!("twin-{index}")).unwrap();
        store
            .append_guarded(
                &stream,
                eventlog_core::Expected::NoStream,
                &[eventlog_conformance::event("item.received", 1)],
                &eventlog_conformance::meta(&format!("twin-{index}"), &serde_json::json!({})),
                guard.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            *guard.found.lock().unwrap(),
            [serde_json::json!({ "kind": value })],
            "a guard's find returns only the row holding {value}"
        );
    }
    store.shutdown().await.unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// Story pg-projection-find-back-within-1-05, option B: a fresh projection table stores each
/// declared field's digest in a column the server generates from the value, and indexes that
/// column. No writer can store a digest that does not match its value: PostgreSQL refuses an
/// explicit value for the column, in an insert and in an update.
#[tokio::test]
async fn a_projection_table_stores_each_value_digest_as_a_generated_column() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_generated";
    let table = projection_table(prefix, ROWS.name);
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    assert_eq!(digest_columns(&sql, &table).await, [DIGEST_COLUMN]);
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        [DIGEST_INDEX, "btree (tenant_id, row_key COLLATE \"C\")"]
    );
    for statement in [
        format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0, idx_0_digest)
             VALUES ('tenant', 'forged', '{{}}', 'kind', 7)"
        ),
        format!("UPDATE {table} SET idx_0_digest = 7"),
    ] {
        let refused = sql.execute(&statement, &[]).await.unwrap_err();
        assert_eq!(
            refused.code(),
            Some(&tokio_postgres::error::SqlState::GENERATED_ALWAYS),
            "{statement}: {refused}"
        );
    }
    sql.execute(
        &format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0) VALUES ('tenant', 'row', '{{}}', $1)"
        ),
        &[&"kind"],
    )
    .await
    .unwrap();
    let stored: bool = sql
        .query_one(
            &format!(
                "SELECT idx_0_digest = hashtextextended('kind', 0) FROM {table} WHERE row_key = 'row'"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(stored, "the server stored the value's own digest");
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// A store pull request 72 migrated — its ledger checksum and each projection table indexed by
/// the expression `hashtextextended(idx_N, 0)`, without the stored digest column — is refused by
/// `open` until `migrate` runs. `migrate` alone adds the generated column to every registered
/// projection table, replaces the expression index under the same name, and moves the ledger to
/// a checksum pull request 72's binary refuses.
#[tokio::test]
async fn the_hash_expression_edition_migrates_to_the_stored_digest_and_is_fenced() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_hash_edition";
    let table = projection_table(prefix, ROWS.name);
    let tenant = TenantId::new("tenant").unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let current = secondary_indexes(&sql, &table).await;
    assert_eq!(
        current,
        [DIGEST_INDEX, "btree (tenant_id, row_key COLLATE \"C\")"]
    );
    let large = incompressible(8_000, 0x3c6e_f372_fe94_f82b);
    sql.execute(
        &format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
                 SELECT 'tenant', 'row/' || n, jsonb_build_object('n', n), 'kind'
                 FROM generate_series(1, 50) AS n
             UNION ALL SELECT 'tenant', 'large', '{{}}', $1"
        ),
        &[&large],
    )
    .await
    .unwrap();

    // Pull request 72's state: no digest column, the expression index under the field's index
    // name, its ledger checksum.
    let index = schema::projection_index(&table, 0);
    sql.batch_execute(&format!(
        "DROP INDEX IF EXISTS {index}; ALTER TABLE {table} DROP COLUMN IF EXISTS idx_0_digest;
         CREATE INDEX {index} ON {table} USING {HASH_EXPRESSION_INDEX}"
    ))
    .await
    .unwrap();
    sql.execute(
        &format!("UPDATE {prefix}_schema_version SET checksum=$1"),
        &[&HASH_EXPRESSION_CHECKSUM],
    )
    .await
    .unwrap();
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        [
            HASH_EXPRESSION_INDEX,
            "btree (tenant_id, row_key COLLATE \"C\")"
        ],
        "the fixture holds pull request 72's edition"
    );
    assert_eq!(digest_columns(&sql, &table).await, Vec::<String>::new());
    assert_eq!(
        schema::validate(&mut sql, prefix).await,
        Err(EventLogError::Invalid(
            "unknown PostgreSQL schema version/checksum".into()
        )),
        "an application open refuses pull request 72's edition until migrate runs"
    );

    schema::migrate(&mut sql, prefix, &[]).await.unwrap();
    assert_eq!(
        secondary_indexes(&sql, &table).await,
        current,
        "migrate moved a registered projection it was not given to the stored digest index"
    );
    assert_eq!(
        digest_columns(&sql, &table).await,
        [DIGEST_COLUMN],
        "migrate added the stored digest column"
    );
    schema::validate(&mut sql, prefix).await.unwrap();
    schema::validate_projection(&mut sql, prefix, &ROWS)
        .await
        .unwrap();
    let ledger: String = sql
        .query_one(
            &format!("SELECT checksum FROM {prefix}_schema_version"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        ledger, HASH_EXPRESSION_CHECKSUM,
        "pull request 72's binary requires its own checksum, so it is refused on the migrated schema"
    );
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", "kind", 100)
            .await
            .unwrap()
            .len(),
        50,
        "the migrated table answers the same lookup"
    );
    assert_eq!(
        store
            .projection_find(&ROWS, &tenant, "kind", &large, 10)
            .await
            .unwrap(),
        [serde_json::json!({})],
        "the 8,000-byte value is found through the stored digest"
    );
    store.shutdown().await.unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// A digest column that is not the generated one is a different table: a plain column a writer
/// fills, or a generated column over another expression, can hold a digest that does not match
/// the value, and the lookup would then miss the row with no candidate to check. Projection
/// admission (`validate_projection`, run by every `open` that attaches a projection) and
/// read-only capture admission both refuse it.
#[tokio::test]
async fn a_digest_column_the_server_does_not_generate_from_the_value_is_refused() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_forged_digest";
    let table = projection_table(prefix, ROWS.name);
    let index = schema::projection_index(&table, 0);
    for (case, column) in [
        ("a plain column", "idx_0_digest BIGINT"),
        (
            "another expression",
            "idx_0_digest BIGINT GENERATED ALWAYS AS (hashtextextended(idx_0, 1)) STORED",
        ),
    ] {
        drop_owner(&sql, prefix, &[&ROWS]).await;
        schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
        // The control: the shape `migrate` built is admitted by both.
        schema::validate_projection(&mut sql, prefix, &ROWS)
            .await
            .unwrap();
        {
            let transaction = sql.transaction().await.unwrap();
            crate::capture::admit_projection(&transaction, prefix, &ROWS)
                .await
                .unwrap();
        }
        sql.batch_execute(&format!(
            "ALTER TABLE {table} DROP COLUMN idx_0_digest;
             ALTER TABLE {table} ADD COLUMN {column};
             CREATE INDEX {index} ON {table} (tenant_id, idx_0_digest)"
        ))
        .await
        .unwrap();
        assert_eq!(
            secondary_indexes(&sql, &table).await,
            [DIGEST_INDEX, "btree (tenant_id, row_key COLLATE \"C\")"],
            "{case}: the indexes are the admitted ones"
        );
        assert_eq!(
            schema::validate_projection(&mut sql, prefix, &ROWS).await,
            Err(EventLogError::Invalid(format!(
                "incompatible PostgreSQL projection: {table}"
            ))),
            "{case}: projection admission refuses it"
        );
        let transaction = sql.transaction().await.unwrap();
        assert_eq!(
            crate::capture::admit_projection(&transaction, prefix, &ROWS).await,
            Err(eventlog_core::CaptureError::ProjectionUnavailable {
                projection: ROWS.name.to_owned(),
                reason: eventlog_core::ProjectionCaptureRefusal::PhysicalShapeMismatch,
            }),
            "{case}: capture admission refuses it"
        );
        drop(transaction);
    }
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// Review L78 and option B: the find checks each candidate's value after the digest index
/// returned it. When rows holding a colliding value sort first and fill the limit, the checked
/// candidates alone would be short or empty; the find must still return the rows holding the
/// value asked for, in `projection_find` and in a guard's `find`.
#[tokio::test]
async fn a_digest_collision_that_fills_the_limit_still_returns_the_value_asked_for() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "digest_crowd";
    let table = projection_table(prefix, ROWS.name);
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let [asked, colliding] = DIGEST_TWINS;
    sql.execute(
        &format!(
            "INSERT INTO {table} (tenant_id, row_key, body, idx_0) VALUES
                 ('crowd', 'a/1', jsonb_build_object('kind', $2::text), $2),
                 ('crowd', 'a/2', jsonb_build_object('kind', $2::text), $2),
                 ('crowd', 'b/1', jsonb_build_object('kind', $1::text, 'n', 1), $1),
                 ('crowd', 'b/2', jsonb_build_object('kind', $1::text, 'n', 2), $1)"
        ),
        &[&asked, &colliding],
    )
    .await
    .unwrap();
    let tenant = TenantId::new("crowd").unwrap();
    let store = PostgresEventStore::connect(&url, prefix).await.unwrap();
    let first = serde_json::json!({ "kind": asked, "n": 1 });
    let second = serde_json::json!({ "kind": asked, "n": 2 });
    for (limit, expected) in [
        (1, vec![first.clone()]),
        (2, vec![first.clone(), second.clone()]),
        (3, vec![first.clone(), second.clone()]),
    ] {
        assert_eq!(
            store
                .projection_find(&ROWS, &tenant, "kind", asked, limit)
                .await
                .unwrap(),
            expected,
            "projection_find with limit {limit}"
        );
    }
    let guard = std::sync::Arc::new(FindsKind {
        tenant: tenant.clone(),
        value: asked,
        found: std::sync::Mutex::default(),
    });
    let crowded = std::sync::Arc::new(FindsFirst(guard.clone()));
    let stream = eventlog_core::StreamId::new(tenant.clone(), "item", "crowd").unwrap();
    store
        .append_guarded(
            &stream,
            eventlog_core::Expected::NoStream,
            &[eventlog_conformance::event("item.received", 1)],
            &eventlog_conformance::meta("crowd", &serde_json::json!({})),
            crowded,
        )
        .await
        .unwrap();
    assert_eq!(
        *guard.found.lock().unwrap(),
        [first],
        "a guard's find with limit 1"
    );
    store.shutdown().await.unwrap();
    drop_owner(&sql, prefix, &[&ROWS]).await;
}

/// A guard that looks `FindsKind`'s value up with a limit of one.
struct FindsFirst(std::sync::Arc<FindsKind>);

impl eventlog_core::Guard for FindsFirst {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn eventlog_core::ProjectionStore,
    ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let rows = store
                .find(&ROWS, &self.0.tenant, "kind", self.0.value, 1)
                .await?;
            *self.0.found.lock().unwrap() = rows;
            Ok(())
        })
    }
}

#[test]
fn a_fixture_url_takes_another_database() {
    assert_eq!(
        with_database("postgres://u@127.0.0.1:5432/postgres", "x"),
        "postgres://u@127.0.0.1:5432/x"
    );
    assert_eq!(
        with_database("postgresql://u@h:1/db?sslmode=disable", "x"),
        "postgresql://u@h:1/x?sslmode=disable"
    );
    assert_eq!(with_database("postgres://u@h:1", "x"), "postgres://u@h:1/x");
}
