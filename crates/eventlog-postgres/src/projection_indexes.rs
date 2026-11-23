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

/// A store in the 0.10.0 edition — its ledger checksum, projection tables without the bytewise
/// page index — is refused by `open` until `migrate` runs. `migrate` alone, given no
/// projection, builds the index on every registered projection table and moves the ledger to a
/// checksum the 0.10.0 binary refuses: the cutover is fenced.
#[tokio::test]
async fn the_previous_edition_migrates_its_projection_tables_and_fences_the_previous_binary() {
    let url = fixture();
    let mut sql = connect(&url).await;
    let prefix = "projkey_upgrade";
    let table = projection_table(prefix, ROWS.name);
    drop_owner(&sql, prefix, &[&ROWS]).await;
    schema::migrate(&mut sql, prefix, &[ROWS]).await.unwrap();
    let current = secondary_indexes(&sql, &table).await;
    assert!(
        current.contains(&"btree (tenant_id, row_key COLLATE \"C\")".to_owned()),
        "a fresh projection table has the bytewise page index: {current:?}"
    );
    sql.batch_execute(&format!(
        "INSERT INTO {table} (tenant_id, row_key, body, idx_0)
             SELECT 'tenant', 'row/' || n, '{{}}', 'kind' FROM generate_series(1, 50) AS n"
    ))
    .await
    .unwrap();

    // The 0.10.0 state: its ledger checksum, and no bytewise index on the projection table.
    let index: String = sql
        .query_one(
            "SELECT indexrelid::regclass::text FROM pg_index
             WHERE indrelid=to_regclass($1)
               AND pg_get_indexdef(indexrelid) LIKE '%(tenant_id, row_key COLLATE \"C\")'",
            &[&table],
        )
        .await
        .unwrap()
        .get(0);
    sql.batch_execute(&format!("DROP INDEX {index}"))
        .await
        .unwrap();
    sql.execute(
        &format!("UPDATE {prefix}_schema_version SET checksum=$1"),
        &[&RELEASE_0_10_0_CHECKSUM],
    )
    .await
    .unwrap();
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
        "migrate rebuilt the index on a registered projection it was not given"
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
    let rows: i64 = sql
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 50, "the migration kept every row");
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
