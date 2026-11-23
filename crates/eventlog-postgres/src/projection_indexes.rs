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

const ROWS: ProjectionSpec = ProjectionSpec {
    name: "keyed_rows",
    indexed: &["kind"],
};

fn fixture() -> String {
    std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("assigned PostgreSQL fixture")
}

async fn connect(url: &str) -> Client {
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

async fn drop_owner(sql: &Client, prefix: &str, projections: &[&ProjectionSpec]) {
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

/// The digest index of `idx_0`, as `pg_get_indexdef` prints its columns.
const DIGEST_INDEX: &str = "btree (tenant_id, substr(decode(md5(idx_0), 'hex'::text), 1, 8))";

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
        ["btree (tenant_id, row_key COLLATE \"C\")", DIGEST_INDEX],
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
        "CREATE INDEX {table}_idx_0 ON {table} (tenant_id, idx_0)"
    ))
    .await
    .unwrap();
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

/// Two values whose MD5 digests share their first 8 bytes, the digest the indexed-value index
/// keys on. Found offline by a parallel Pollard rho search over 16-hex-digit strings (about
/// 2^32 MD5 evaluations); the test below asserts in PostgreSQL that they collide.
const DIGEST_TWINS: [&str; 2] = ["8ea434a5e947b6ef", "0dec2c651fc63026"];

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
            "SELECT $1::text <> $2::text
                AND substr(decode(md5($1::text), 'hex'), 1, 8)
                  = substr(decode(md5($2::text), 'hex'), 1, 8)",
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
